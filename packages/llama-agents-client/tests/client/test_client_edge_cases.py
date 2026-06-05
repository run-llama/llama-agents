# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
# ty: ignore[invalid-argument-type, no-matching-overload]
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable
from unittest.mock import AsyncMock

import httpx
import pytest
from client_test_workflows import (  # type: ignore[import]
    CrashingWorkflow,
    GreetEvent,
    GreetingWorkflow,
    InputEvent,
)
from httpx import ASGITransport, AsyncClient
from llama_agents.client import WorkflowClient
from llama_agents.client.protocol.serializable_events import (
    EventEnvelopeWithMetadata,
)
from llama_agents.server import MemoryWorkflowStore
from llama_agents.server.server import WorkflowServer
from workflows import Context
from workflows.events import Event, StartEvent


@pytest.fixture()
def server() -> WorkflowServer:
    ws = WorkflowServer(workflow_store=MemoryWorkflowStore())
    ws.add_workflow(name="greeting", workflow=GreetingWorkflow())
    ws.add_workflow(name="crashing", workflow=CrashingWorkflow())
    return ws


@pytest.fixture()
def client(server: WorkflowServer) -> WorkflowClient:
    transport = ASGITransport(server.app)
    httpx_client = AsyncClient(transport=transport, base_url="http://test")
    return WorkflowClient(httpx_client=httpx_client)


def _mock_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> AsyncClient:
    """An httpx.AsyncClient backed by a MockTransport for synthetic responses."""
    return AsyncClient(transport=httpx.MockTransport(handler), base_url="http://test")


class UnserializableEvent(Event):
    """Event whose model_dump raises, used to drive serialization-error wrappers."""

    def model_dump(self, **_: Any) -> dict[str, Any]:  # type: ignore[override]
        raise RuntimeError("boom")


class FakeStatusOnlyStreamClient:
    """Mock httpx client whose stream() yields a response with a given status code.

    Sibling of FakeStreamClient in test_client.py — used to drive the 404 and
    204 branches in get_workflow_events().
    """

    def __init__(self, status: int) -> None:
        self._status = status

    @asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        params: dict[str, str] | None = None,
        **kwargs: object,
    ) -> AsyncIterator[AsyncMock]:
        resp = AsyncMock()
        resp.status_code = self._status

        async def aiter_lines() -> AsyncIterator[str]:
            if False:
                yield ""

        resp.aiter_lines = aiter_lines
        yield resp


# --- Constructor validation -------------------------------------------------


def test_init_without_either_raises_value_error() -> None:
    with pytest.raises(
        ValueError, match="Either httpx_client or base_url must be provided"
    ):
        WorkflowClient()  # pyright: ignore[reportCallIssue]


def test_init_with_both_raises_value_error() -> None:
    with pytest.raises(
        ValueError, match="Only one of httpx_client or base_url must be provided"
    ):
        WorkflowClient(  # pyright: ignore[reportCallIssue]
            httpx_client=AsyncClient(),
            base_url="http://test",
        )


@pytest.mark.asyncio
async def test_init_with_base_url_only_builds_transient_client() -> None:
    """The base_url branch of _get_client constructs a transient AsyncClient."""
    wf_client = WorkflowClient(base_url="http://does-not-matter")
    assert wf_client.httpx_client is None
    assert wf_client.base_url == "http://does-not-matter"

    async with wf_client._get_client() as inner:
        assert isinstance(inner, httpx.AsyncClient)
        # Each entry to the context manager should yield a fresh transient
        # client — confirm by checking it's not the (None) stored attribute.
        assert inner is not wf_client.httpx_client


# --- 5xx error handling -----------------------------------------------------


@pytest.mark.asyncio
async def test_5xx_error_message_includes_body_preview() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream failed: backend unreachable")

    wf_client = WorkflowClient(httpx_client=_mock_client(handler))
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await wf_client.is_healthy()

    msg = str(exc_info.value)
    assert "503" in msg
    assert "Service Unavailable" in msg
    assert "GET http://test/health" in msg
    assert "upstream failed: backend unreachable" in msg


@pytest.mark.asyncio
async def test_5xx_error_body_truncated_at_200_chars() -> None:
    long_body = "X" * 500 + "Y" * 500
    expected_prefix = "X" * 200

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=long_body)

    wf_client = WorkflowClient(httpx_client=_mock_client(handler))
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await wf_client.is_healthy()

    msg = str(exc_info.value)
    assert expected_prefix in msg
    # Past the 200-char window is dropped.
    assert "Y" not in msg


# --- Dict event serialization pass-through ----------------------------------


@pytest.mark.asyncio
async def test_run_workflow_accepts_dict_start_event(client: WorkflowClient) -> None:
    """A bare dict start_event should pass through _serialize_event unchanged."""
    result = await client.run_workflow(
        "greeting", start_event={"greeting": "hello", "name": "Ada"}
    )
    assert result.status == "completed"


@pytest.mark.asyncio
async def test_run_workflow_nowait_accepts_dict_start_event(
    client: WorkflowClient,
) -> None:
    handler = await client.run_workflow_nowait(
        "greeting", start_event={"greeting": "hi", "name": "Bo"}
    )
    assert handler.handler_id


@pytest.mark.asyncio
async def test_send_event_accepts_dict_event(client: WorkflowClient) -> None:
    """send_event with a dict should pass through without raising."""
    handler = await client.run_workflow_nowait(
        "greeting", start_event=InputEvent(greeting="hi", name="C")
    )
    response = await client.send_event(
        handler_id=handler.handler_id,
        event={
            "qualified_name": "client_test_workflows.GreetEvent",
            "value": {"greeting": "hi", "exclamation_marks": 1},
        },
    )
    assert response.status == "sent"


# --- Serialization error wrappers -------------------------------------------


@pytest.mark.asyncio
async def test_run_workflow_wraps_serialize_failure(client: WorkflowClient) -> None:
    with pytest.raises(
        ValueError, match="Impossible to serialize the start event because of:"
    ):
        await client.run_workflow(
            "greeting",
            start_event=UnserializableEvent(),  # pyright: ignore[reportArgumentType]
        )


@pytest.mark.asyncio
async def test_run_workflow_nowait_wraps_serialize_failure(
    client: WorkflowClient,
) -> None:
    with pytest.raises(
        ValueError, match="Impossible to serialize the start event because of:"
    ):
        await client.run_workflow_nowait(
            "greeting",
            start_event=UnserializableEvent(),  # pyright: ignore[reportArgumentType]
        )


@pytest.mark.asyncio
async def test_send_event_wraps_serialize_failure(client: WorkflowClient) -> None:
    with pytest.raises(
        ValueError, match="Error while serializing the provided event:"
    ):
        await client.send_event(handler_id="h", event=UnserializableEvent())


class _ExplodingContext(Context):  # type: ignore[misc]
    """Context subclass whose to_dict() raises, to drive the context wrapper."""

    def __init__(self) -> None:
        # Skip Context.__init__ — we only need to_dict to be called and to raise.
        pass

    def to_dict(self, *args: object, **kwargs: object) -> dict[str, Any]:  # type: ignore[override]
        raise RuntimeError("ctx boom")


@pytest.mark.asyncio
async def test_run_workflow_wraps_context_to_dict_failure(
    client: WorkflowClient,
) -> None:
    with pytest.raises(
        ValueError, match="Impossible to serialize the context because of:"
    ):
        await client.run_workflow(
            "greeting",
            start_event=InputEvent(greeting="x", name="y"),
            context=_ExplodingContext(),
        )


@pytest.mark.asyncio
async def test_run_workflow_nowait_wraps_context_to_dict_failure(
    client: WorkflowClient,
) -> None:
    with pytest.raises(
        ValueError, match="Impossible to serialize the context because of:"
    ):
        await client.run_workflow_nowait(
            "greeting",
            start_event=InputEvent(greeting="x", name="y"),
            context=_ExplodingContext(),
        )


# --- EventStream lifecycle --------------------------------------------------


@pytest.mark.asyncio
async def test_event_stream_double_iteration_raises_runtime_error(
    client: WorkflowClient,
) -> None:
    handler = await client.run_workflow_nowait(
        "greeting", start_event=InputEvent(greeting="hi", name="J")
    )
    stream = client.get_workflow_events(handler_id=handler.handler_id)

    async for _ in stream:
        pass

    with pytest.raises(RuntimeError, match="EventStream can only be iterated once"):
        async for _ in stream:
            pass


@pytest.mark.asyncio
async def test_event_stream_aclose_is_idempotent(client: WorkflowClient) -> None:
    """A double aclose() should be a no-op — the second call returns immediately."""
    handler = await client.run_workflow_nowait(
        "greeting", start_event=InputEvent(greeting="hi", name="K")
    )
    stream = client.get_workflow_events(handler_id=handler.handler_id)

    async for _ in stream:
        pass

    # Stream auto-aclosed via the finally in _iterate(); _task should now be None.
    assert stream._task is None
    # Calling aclose() again is allowed and is a no-op.
    await stream.aclose()
    assert stream._task is None


# --- 404 / 204 edge cases in get_workflow_events ----------------------------


@pytest.mark.asyncio
async def test_get_workflow_events_404_raises_value_error() -> None:
    fake = FakeStatusOnlyStreamClient(404)
    wf_client = WorkflowClient(httpx_client=fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Handler not found"):
        async for _ in wf_client.get_workflow_events(handler_id="missing"):
            pass


@pytest.mark.asyncio
async def test_get_workflow_events_204_terminates_cleanly() -> None:
    fake = FakeStatusOnlyStreamClient(204)
    wf_client = WorkflowClient(httpx_client=fake)  # type: ignore[arg-type]

    events: list[EventEnvelopeWithMetadata] = []
    async for event in wf_client.get_workflow_events(handler_id="h"):
        events.append(event)
    assert events == []


# --- cancel_handler purge query param wire format ---------------------------


@pytest.mark.asyncio
async def test_cancel_handler_purge_query_param_wire_format() -> None:
    """The purge flag must serialize as the literal strings "true"/"false"."""
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["purge"] = request.url.params.get("purge", "")
        return httpx.Response(200, json={"status": "cancelled"})

    wf_client = WorkflowClient(httpx_client=_mock_client(handler))

    await wf_client.cancel_handler("h", purge=True)
    assert captured["purge"] == "true"

    await wf_client.cancel_handler("h", purge=False)
    assert captured["purge"] == "false"


# Keep an unused import quiet under aggressive lint.
_ = (GreetEvent, StartEvent)
