# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Literal

import httpx
import pytest
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from llama_agents.server._store.agent_data_store import AgentDataStore
from llama_agents_integration_tests.fake_agent_data import FakeAgentDataBackend
from server_test_fixtures import async_yield
from workflows.events import Event, StopEvent

Journal = Literal["event", "tick"]


@dataclass
class JournalAPI:
    backend: FakeAgentDataBackend = field(default_factory=FakeAgentDataBackend)
    failures: set[tuple[str, int]] = field(default_factory=set)
    gates: dict[tuple[str, int], tuple[asyncio.Event, asyncio.Event]] = field(
        default_factory=dict
    )
    cancelled: set[tuple[str, int]] = field(default_factory=set)

    async def handle(self, request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/v1/beta/agent-data":
            body = json.loads(request.content)
            key = (body["data"]["run_id"], body["data"]["sequence"])
            if key in self.gates:
                started, release = self.gates[key]
                started.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    self.cancelled.add(key)
                    raise
            if key in self.failures:
                return httpx.Response(503, json={"error": "journal unavailable"})
        return self.backend.handle_request(request)

    def block(self, run_id: str, sequence: int) -> tuple[asyncio.Event, asyncio.Event]:
        gate = (asyncio.Event(), asyncio.Event())
        self.gates[run_id, sequence] = gate
        return gate


@pytest.fixture
async def journal_store(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[AgentDataStore, JournalAPI]]:
    api = JournalAPI()
    store = AgentDataStore(
        base_url="https://agent-data.test",
        api_key="test-key",
        project_id="test-project",
        deployment_name="test-deployment",
    )
    async with httpx.AsyncClient(
        base_url="https://agent-data.test", transport=httpx.MockTransport(api.handle)
    ) as client:
        monkeypatch.setattr(store._client, "http_client", lambda: client)
        try:
            yield store, api
        finally:
            for _, release in api.gates.values():
                release.set()
            tasks = [
                task
                for pending in (store._pending_events, store._pending_ticks)
                for batch in pending.values()
                for task in batch
            ]
            await asyncio.gather(*tasks, return_exceptions=True)


async def append(
    store: AgentDataStore, journal: Journal, sequence: int, run_id: str = "run"
) -> None:
    if journal == "event":
        await store.append_event(
            run_id, EventEnvelopeWithMetadata.from_event(Event(index=sequence))
        )
    else:
        await store.append_tick(run_id, {"step": sequence})


def stored_sequences(
    api: JournalAPI, journal: Journal, run_id: str = "run"
) -> list[int]:
    collection = "workflow_contexts_" + ("events" if journal == "event" else "ticks")
    return [
        item["data"]["sequence"]
        for item in api.backend.search(
            "test-deployment",
            collection,
            {"run_id": {"eq": run_id}},
            order_by="sequence",
        )
    ]


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_completed_write_error_survives_task_compaction(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    api.failures.add(("run", 0))
    await append(store, journal, 0)
    await async_yield()
    for sequence in range(1, 52):
        await append(store, journal, sequence)
    with pytest.raises(httpx.HTTPStatusError, match="503"):
        await store.after_tick("run", {})
    assert stored_sequences(api, journal) == list(range(1, 52))


@pytest.mark.parametrize("journal", ["event", "tick"])
@pytest.mark.parametrize("waiter", ["barrier", "query", "stream"])
async def test_cancelled_barrier_does_not_cancel_or_forget_writes(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal, waiter: str
) -> None:
    store, api = journal_store
    started, release = api.block("run", 0)
    await append(store, journal, 0)
    await started.wait()

    async def wait_for_writes() -> None:
        if waiter == "barrier":
            await store.after_tick("run", {})
        elif waiter == "query":
            if journal == "event":
                await store.query_events("run")
            else:
                await store.get_ticks("run")
        else:
            stream = (
                store.subscribe_events("run")
                if journal == "event"
                else store.stream_ticks("run")
            )
            try:
                await anext(stream)
            finally:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    await close()

    barrier = asyncio.create_task(wait_for_writes())
    await async_yield()
    barrier.cancel()
    with pytest.raises(asyncio.CancelledError):
        await barrier
    release.set()
    await store.after_tick("run", {})
    assert not api.cancelled, (
        "cancelling a waiter must not cancel an HTTP journal write"
    )
    assert stored_sequences(api, journal) == [0]


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_concurrent_barriers_both_wait_for_same_write(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    started, release = api.block("run", 0)
    await append(store, journal, 0)
    await started.wait()
    first = asyncio.create_task(store.after_tick("run", {}))
    await async_yield()
    second = asyncio.create_task(store.after_tick("run", {}))
    try:
        await async_yield()
        assert not second.done(), "a concurrent barrier returned before persistence"
    finally:
        release.set()
        await asyncio.gather(first, second)
    assert stored_sequences(api, journal) == [0]


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_barrier_preserves_writes_added_while_it_waits(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    started0, release0 = api.block("run", 0)
    started1, release1 = api.block("run", 1)
    await append(store, journal, 0)
    await started0.wait()
    first = asyncio.create_task(store.after_tick("run", {}))
    await async_yield()
    await append(store, journal, 1)
    await started1.wait()
    release0.set()
    await first
    second = asyncio.create_task(store.after_tick("run", {}))
    try:
        await async_yield()
        assert not second.done(), "a completed barrier forgot a newer write"
    finally:
        release1.set()
        await second
    assert stored_sequences(api, journal) == [0, 1]


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_read_waits_for_write_already_observed_by_another_barrier(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    started, release = api.block("run", 0)
    await append(store, journal, 0)
    await started.wait()
    barrier = asyncio.create_task(store.after_tick("run", {}))
    await async_yield()
    reader = asyncio.create_task(
        store.query_events("run") if journal == "event" else store.get_ticks("run")
    )
    try:
        await async_yield()
        assert not reader.done(), "a journal read raced past an uncommitted write"
    finally:
        release.set()
        await barrier
        records = await reader
    assert [record.sequence for record in records] == [0]


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_concurrent_barriers_both_observe_write_failure(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    api.failures.add(("run", 0))
    started, release = api.block("run", 0)
    await append(store, journal, 0)
    await started.wait()
    first = asyncio.create_task(store.after_tick("run", {}))
    await async_yield()
    second = asyncio.create_task(store.after_tick("run", {}))
    await async_yield()
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert all(isinstance(result, httpx.HTTPStatusError) for result in results)
    assert stored_sequences(api, journal) == []


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_cancelled_barrier_leaves_later_write_failure_observable(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    api.failures.add(("run", 0))
    started, release = api.block("run", 0)
    await append(store, journal, 0)
    await started.wait()
    barrier = asyncio.create_task(store.after_tick("run", {}))
    await async_yield()
    barrier.cancel()
    with pytest.raises(asyncio.CancelledError):
        await barrier
    release.set()
    with pytest.raises(httpx.HTTPStatusError, match="503"):
        await store.after_tick("run", {})


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_observed_error_does_not_poison_later_writes(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    api.failures.add(("run", 0))
    await append(store, journal, 0)
    with pytest.raises(httpx.HTTPStatusError):
        await store.after_tick("run", {})
    await append(store, journal, 1)
    await store.after_tick("run", {})
    assert stored_sequences(api, journal) == [1]


@pytest.mark.parametrize("journal", ["event", "tick"])
async def test_barrier_for_one_run_does_not_block_other_run(
    journal_store: tuple[AgentDataStore, JournalAPI], journal: Journal
) -> None:
    store, api = journal_store
    started, release = api.block("run", 0)
    await append(store, journal, 0)
    await started.wait()
    barrier = asyncio.create_task(store.after_tick("run", {}))
    try:
        await append(store, journal, 0, run_id="other")
        await store.after_tick("other", {})
        assert stored_sequences(api, journal, "other") == [0]
        assert not barrier.done()
    finally:
        release.set()
        await barrier


async def test_terminal_cleanup_waits_for_event_already_being_flushed(
    journal_store: tuple[AgentDataStore, JournalAPI],
) -> None:
    store, api = journal_store
    started, release = api.block("run", 0)
    await append(store, "event", 0)
    await started.wait()
    barrier = asyncio.create_task(store.after_tick("run", {}))
    await async_yield()
    terminal = asyncio.create_task(
        store.append_event(
            "run", EventEnvelopeWithMetadata.from_event(StopEvent(result="done"))
        )
    )
    try:
        await async_yield()
        assert not terminal.done(), (
            "terminal cleanup skipped an in-flight journal write"
        )
    finally:
        release.set()
        await asyncio.gather(barrier, terminal)
    assert stored_sequences(api, "event") == [0, 1]
