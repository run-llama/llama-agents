# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextvars import ContextVar
from pathlib import Path
from typing import cast

import httpx
import pytest
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from llama_agents.server import MemoryWorkflowStore, SqliteWorkflowStore, WorkflowServer
from llama_agents.server._store.abstract_workflow_store import (
    PersistentHandler,
    StoredEvent,
)
from server_test_fixtures import async_yield, live_server
from starlette.requests import ClientDisconnect, Request
from starlette.types import Message
from workflows.events import Event, StopEvent


@pytest.fixture(params=["memory", "sqlite"])
async def stream_server(
    request: pytest.FixtureRequest, tmp_path: Path
) -> WorkflowServer:
    store = (
        MemoryWorkflowStore()
        if request.param == "memory"
        else SqliteWorkflowStore(str(tmp_path / "events.db"))
    )
    await store.update(
        PersistentHandler(
            handler_id="stream-handler",
            workflow_name="stream-workflow",
            run_id="stream-run",
            status="running",
        )
    )
    await store.append_event(
        "stream-run", EventEnvelopeWithMetadata.from_event(Event(index=0))
    )
    return WorkflowServer(workflow_store=store, sse_heartbeat_interval=0.01)


def _stream_request(*, sse: bool, spec_version: str = "2.4") -> Request:
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": spec_version},
            "http_version": "1.1",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 12345),
            "root_path": "",
            "method": "GET",
            "path": "/events/stream-handler",
            "path_params": {"handler_id": "stream-handler"},
            "query_string": f"after_sequence=-1&sse={str(sse).lower()}".encode(),
            "headers": [],
        }
    )


async def _response_body(
    server: WorkflowServer, *, sse: bool = True
) -> AsyncGenerator[str, None]:
    request = _stream_request(sse=sse)
    response = await server._api._stream_events(request)
    return cast(AsyncGenerator[str, None], response.body_iterator)


@pytest.mark.parametrize("sse", [False, True])
async def test_subscription_failure_terminates_response(
    stream_server: WorkflowServer, monkeypatch: pytest.MonkeyPatch, sse: bool
) -> None:
    store = stream_server._service.store
    events = await store.query_events("stream-run")

    async def broken_subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        yield events[0]
        raise OSError("journal read failed")

    monkeypatch.setattr(store, "subscribe_events", broken_subscription)
    body = await _response_body(stream_server, sse=sse)
    first = await anext(body)
    assert '"index":0' in first

    async def consume_remainder() -> None:
        async for _ in body:
            pass

    with pytest.raises(OSError, match="journal read failed"):
        await asyncio.wait_for(consume_remainder(), timeout=0.3)


@pytest.mark.parametrize("sse", [False, True])
async def test_slow_response_does_not_prefetch_subscription(
    stream_server: WorkflowServer, monkeypatch: pytest.MonkeyPatch, sse: bool
) -> None:
    store = stream_server._service.store
    for index in range(1, 1000):
        await store.append_event(
            "stream-run", EventEnvelopeWithMetadata.from_event(Event(index=index))
        )
    subscribe = store.subscribe_events
    reads = 0

    async def counted_subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        nonlocal reads
        async for event in subscribe(run_id, after_sequence):
            reads += 1
            yield event

    monkeypatch.setattr(store, "subscribe_events", counted_subscription)
    body = await _response_body(stream_server, sse=sse)
    try:
        first = await anext(body)
        payload = first.split("data: ")[-1].strip()
        assert json.loads(payload)["value"] == {"_data": {"index": 0}}
        await async_yield()
        # The sent item, one queued item, and one blocked producer item are bounded.
        assert reads <= 3, "a stalled response must apply backpressure to its source"
    finally:
        await body.aclose()


@pytest.mark.parametrize("sse", [False, True])
async def test_response_close_waits_for_subscription_cleanup(
    stream_server: WorkflowServer, monkeypatch: pytest.MonkeyPatch, sse: bool
) -> None:
    store = stream_server._service.store
    events = await store.query_events("stream-run")
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    closed = asyncio.Event()

    async def subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        try:
            yield events[0]
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await allow_cleanup.wait()
            closed.set()

    monkeypatch.setattr(store, "subscribe_events", subscription)
    body = await _response_body(stream_server, sse=sse)
    await anext(body)
    close = asyncio.create_task(body.aclose())
    try:
        await asyncio.wait_for(cleanup_started.wait(), timeout=0.3)
        assert not close.done(), "response close returned before subscription cleanup"
    finally:
        allow_cleanup.set()
        await close
    assert closed.is_set()


@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
@pytest.mark.parametrize("sse", [False, True])
async def test_disconnect_during_send_closes_subscription(
    stream_server: WorkflowServer,
    monkeypatch: pytest.MonkeyPatch,
    sse: bool,
    spec_version: str,
) -> None:
    store = stream_server._service.store
    events = await store.query_events("stream-run")
    sending = asyncio.Event()
    closed = asyncio.Event()

    async def subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        try:
            yield events[0]
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            closed.set()

    async def send(message: Message) -> None:
        if message["type"] == "http.response.body":
            sending.set()
            if spec_version == "2.4":
                raise OSError("client went away")
            await asyncio.Event().wait()

    async def receive() -> Message:
        await sending.wait()
        return {"type": "http.disconnect"}

    monkeypatch.setattr(store, "subscribe_events", subscription)
    request = _stream_request(sse=sse, spec_version=spec_version)
    # Exercise the real response and routing, retaining it until cleanup is checked.
    response = await stream_server._api._stream_events(request)
    try:
        if spec_version == "2.4":
            with pytest.raises(ClientDisconnect):
                await response(request.scope, receive, send)
        else:
            await response(request.scope, receive, send)
        assert closed.is_set(), "disconnect returned with a live store subscription"
    finally:
        body = cast(AsyncGenerator[str, None], response.body_iterator)
        await body.aclose()


@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
@pytest.mark.parametrize("sse", [False, True])
async def test_disconnect_during_read_waits_for_cleanup(
    stream_server: WorkflowServer,
    monkeypatch: pytest.MonkeyPatch,
    sse: bool,
    spec_version: str,
) -> None:
    store = stream_server._service.store
    events = await store.query_events("stream-run")
    reading = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    closed = asyncio.Event()

    async def subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        try:
            yield events[0]
            reading.set()
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await allow_cleanup.wait()
            closed.set()

    async def send(message: Message) -> None:
        pass

    async def receive() -> Message:
        await reading.wait()
        return {"type": "http.disconnect"}

    monkeypatch.setattr(store, "subscribe_events", subscription)
    request = _stream_request(sse=sse, spec_version=spec_version)
    response = await stream_server._api._stream_events(request)
    response_task = asyncio.create_task(response(request.scope, receive, send))
    try:
        await asyncio.wait_for(reading.wait(), timeout=1)
        if spec_version == "2.4":
            response_task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        await async_yield()
        assert not response_task.done(), (
            "disconnect abandoned subscription finalization"
        )
    finally:
        allow_cleanup.set()
        await asyncio.gather(response_task, return_exceptions=True)
    assert closed.is_set()


async def test_heartbeats_preserve_pending_store_read(
    stream_server: WorkflowServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = stream_server._service.store
    subscribe = store.subscribe_events
    closed = asyncio.Event()

    async def subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        try:
            async for event in subscribe(run_id, after_sequence):
                yield event
        finally:
            closed.set()

    monkeypatch.setattr(store, "subscribe_events", subscription)
    body = await _response_body(stream_server)
    try:
        assert (await anext(body)).startswith("id: 0\n")
        for _ in range(3):
            assert await anext(body) == ": heartbeat\n\n"
            assert not closed.is_set()
        await store.append_event(
            "stream-run", EventEnvelopeWithMetadata.from_event(Event(index=1))
        )
        assert (await anext(body)).startswith("id: 1\n")
    finally:
        await body.aclose()
    assert closed.is_set()


@pytest.mark.parametrize("sse", [False, True])
async def test_closing_one_response_preserves_other_consumer(
    stream_server: WorkflowServer, sse: bool
) -> None:
    store = stream_server._service.store
    slow = await _response_body(stream_server, sse=sse)
    fast = await _response_body(stream_server, sse=sse)
    try:
        await anext(slow)
        await anext(fast)
        for index in range(1, 11):
            await store.append_event(
                "stream-run", EventEnvelopeWithMetadata.from_event(Event(index=index))
            )
            assert f'"index":{index}' in await anext(fast)
        await slow.aclose()
        await store.append_event(
            "stream-run", EventEnvelopeWithMetadata.from_event(StopEvent(result="done"))
        )
        assert '"type":"StopEvent"' in await anext(fast)
        with pytest.raises(StopAsyncIteration):
            await anext(fast)
    finally:
        await slow.aclose()
        await fast.aclose()


@pytest.mark.parametrize("sse", [False, True])
async def test_live_http_subscription_failure_closes_connection(
    stream_server: WorkflowServer, monkeypatch: pytest.MonkeyPatch, sse: bool
) -> None:
    store = stream_server._service.store
    events = await store.query_events("stream-run")
    closed = asyncio.Event()

    async def broken_subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        try:
            yield events[0]
            raise RuntimeError("journal read failed")
        finally:
            closed.set()

    monkeypatch.setattr(store, "subscribe_events", broken_subscription)
    async with live_server(lambda: stream_server) as (base_url, _):
        async with httpx.AsyncClient(base_url=base_url, timeout=2) as client:
            with pytest.raises(httpx.RemoteProtocolError):
                await client.get(
                    f"/events/stream-handler?after_sequence=-1&sse={str(sse).lower()}"
                )
            assert closed.is_set()
            assert (await client.get("/health")).status_code == 200


@pytest.mark.parametrize("close_early", [False, True])
@pytest.mark.parametrize("sse", [False, True])
async def test_subscription_context_survives_reads_and_close(
    stream_server: WorkflowServer,
    monkeypatch: pytest.MonkeyPatch,
    sse: bool,
    close_early: bool,
) -> None:
    store = stream_server._service.store
    await store.append_event(
        "stream-run", EventEnvelopeWithMetadata.from_event(Event(index=1))
    )
    events = await store.query_events("stream-run")
    span: ContextVar[str | None] = ContextVar("subscription_span", default=None)
    closed = asyncio.Event()

    async def subscription(
        run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        token = span.set("store-span")
        try:
            for event in events:
                yield event
        finally:
            span.reset(token)
            closed.set()

    monkeypatch.setattr(store, "subscribe_events", subscription)
    body = await _response_body(stream_server, sse=sse)
    try:
        assert '"index":0' in await anext(body)
        if not close_early:
            assert '"index":1' in await anext(body)
            with pytest.raises(StopAsyncIteration):
                await anext(body)
    finally:
        await body.aclose()
    assert closed.is_set()
    assert span.get() is None
