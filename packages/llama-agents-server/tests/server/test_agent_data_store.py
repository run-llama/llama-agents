# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from llama_agents.server import (
    HandlerQuery,
    PersistentHandler,
)
from llama_agents.server._store.abstract_workflow_store import Status, StoredEvent
from llama_agents.server._store.agent_data_state_store import AgentDataStateStore
from llama_agents.server._store.agent_data_store import AgentDataStore
from llama_agents_integration_tests.fake_agent_data import (
    FakeAgentDataBackend,
    create_agent_data_store,
)
from pydantic import BaseModel
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import DictState, InMemoryStateStore
from workflows.events import Event, StopEvent

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class AgentDataCounterState(BaseModel):
    count: int = 0
    label: str = "default"


@pytest.fixture()
def backend() -> FakeAgentDataBackend:
    return FakeAgentDataBackend()


@pytest.fixture()
def store(
    backend: FakeAgentDataBackend, monkeypatch: pytest.MonkeyPatch
) -> AgentDataStore:
    return create_agent_data_store(backend, monkeypatch)


def make_handler(
    handler_id: str = "h1",
    workflow_name: str = "wf",
    status: Status = "running",
    run_id: str | None = None,
    idle_since: datetime | None = None,
) -> PersistentHandler:
    return PersistentHandler(
        handler_id=handler_id,
        workflow_name=workflow_name,
        status=status,
        run_id=run_id,
        idle_since=idle_since,
    )


def make_envelope(
    event: Event | None = None,
    seq_label: int = 0,
) -> EventEnvelopeWithMetadata:
    if event is None:
        event = Event(data=f"seq-{seq_label}")
    return EventEnvelopeWithMetadata.from_event(event, include_qualified_name=False)


def create_agent_data_store_with_collection(
    backend: FakeAgentDataBackend,
    monkeypatch: pytest.MonkeyPatch,
    collection: str,
) -> AgentDataStore:
    store = AgentDataStore(
        base_url="https://fake-api.example.com",
        api_key="test-key",
        project_id="test-project",
        deployment_name="test-deploy",
        collection=collection,
    )
    mock_http = httpx.AsyncClient(
        base_url=store._client._base_url,
        headers=store._client._headers(),
        params={"project_id": store._client._project_id},
        transport=httpx.MockTransport(backend.handle_request),
    )
    monkeypatch.setattr(store._client, "_shared_client", mock_http)
    return store


async def _subscribe_and_collect(
    store: AgentDataStore,
    run_id: str,
    after_sequence: int = -1,
) -> tuple[list[StoredEvent], asyncio.Task[None]]:
    collected: list[StoredEvent] = []
    existing_queue_count = len(store._subscriber_queues.get(run_id, ()))

    async def consumer() -> None:
        async for event in store.subscribe_events(
            run_id, after_sequence=after_sequence
        ):
            collected.append(event)

    task = asyncio.create_task(consumer())

    async def registered() -> None:
        assert len(store._subscriber_queues.get(run_id, ())) > existing_queue_count

    await wait_for_passing(registered, max_duration=2.0, interval=0.01)
    return collected, task


async def _wait_collected_count(
    collected: list[StoredEvent],
    expected: int,
) -> None:
    async def check() -> None:
        assert len(collected) == expected

    await wait_for_passing(check, max_duration=2.0, interval=0.01)


# ---------------------------------------------------------------------------
# Handler CRUD tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_and_query_returns_handler(store: AgentDataStore) -> None:
    handler = make_handler(handler_id="h1", run_id="run-1")
    await store.update(handler)

    result = await store.query(HandlerQuery(handler_id_in=["h1"]))
    assert len(result) == 1
    assert result[0].handler_id == "h1"
    assert result[0].run_id == "run-1"


@pytest.mark.asyncio
async def test_update_overwrites_existing(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1", status="running"))
    await store.update(make_handler(handler_id="h1", status="completed"))

    result = await store.query(HandlerQuery(handler_id_in=["h1"]))
    assert len(result) == 1
    assert result[0].status == "completed"


@pytest.mark.asyncio
async def test_query_filters_by_run_id(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1", run_id="run-1"))
    await store.update(make_handler(handler_id="h2", run_id="run-2"))

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert len(result) == 1
    assert result[0].handler_id == "h1"


@pytest.mark.asyncio
async def test_query_filters_by_workflow_name(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1", workflow_name="wf-a"))
    await store.update(make_handler(handler_id="h2", workflow_name="wf-b"))

    result = await store.query(HandlerQuery(workflow_name_in=["wf-a"]))
    assert len(result) == 1
    assert result[0].handler_id == "h1"


@pytest.mark.asyncio
async def test_query_filters_by_status(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1", status="running"))
    await store.update(make_handler(handler_id="h2", status="completed"))

    result = await store.query(HandlerQuery(status_in=["completed"]))
    assert len(result) == 1
    assert result[0].handler_id == "h2"


@pytest.mark.asyncio
async def test_query_with_empty_filter_returns_nothing(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1"))
    result = await store.query(HandlerQuery(handler_id_in=[]))
    assert result == []


@pytest.mark.asyncio
async def test_query_no_filters_returns_all(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1"))
    await store.update(make_handler(handler_id="h2"))

    result = await store.query(HandlerQuery())
    assert len(result) == 2


@pytest.mark.asyncio
async def test_query_filters_by_is_idle(store: AgentDataStore) -> None:
    now = datetime.now(timezone.utc)
    await store.update(make_handler(handler_id="h1", idle_since=now))
    await store.update(make_handler(handler_id="h2", idle_since=None))

    idle = await store.query(HandlerQuery(is_idle=True))
    assert len(idle) == 1
    assert idle[0].handler_id == "h1"

    not_idle = await store.query(HandlerQuery(is_idle=False))
    assert len(not_idle) == 1
    assert not_idle[0].handler_id == "h2"


@pytest.mark.asyncio
async def test_delete_removes_matching_handlers(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1", workflow_name="wf-a"))
    await store.update(make_handler(handler_id="h2", workflow_name="wf-b"))

    count = await store.delete(HandlerQuery(workflow_name_in=["wf-a"]))
    assert count == 1

    remaining = await store.query(HandlerQuery())
    assert len(remaining) == 1
    assert remaining[0].handler_id == "h2"


@pytest.mark.asyncio
async def test_delete_invalidates_cache(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1"))
    # Verify it's cached
    assert store._id_cache.get("h1") is not None

    await store.delete(HandlerQuery(handler_id_in=["h1"]))
    assert store._id_cache.get("h1") is None


def _seed_raw_handler(
    backend: FakeAgentDataBackend,
    *,
    handler_id: str,
    run_id: str | None,
    status: Status = "running",
    workflow_name: str = "wf",
) -> str:
    """Insert a raw handler row into the fake backend, bypassing update()."""
    handler = PersistentHandler(
        handler_id=handler_id,
        workflow_name=workflow_name,
        status=status,
        run_id=run_id,
    )
    item = backend.create("test-deploy", "handlers", handler.model_dump(mode="json"))
    return item["id"]


@pytest.mark.asyncio
async def test_update_collapses_duplicates_with_matching_run_id(
    store: AgentDataStore, backend: FakeAgentDataBackend
) -> None:
    """Duplicate rows for the same handler_id/run_id collapse to one row.

    The survivor is the oldest row, so its created_at is preserved.
    """
    # Seed two rows for the same handler with identical run_id. The first
    # insert gets the smaller row-level created_at and should be the survivor.
    oldest_id = _seed_raw_handler(
        backend, handler_id="dup-1", run_id="run-dup", status="running"
    )
    _seed_raw_handler(backend, handler_id="dup-1", run_id="run-dup", status="failed")

    await store.update(
        make_handler(handler_id="dup-1", run_id="run-dup", status="completed")
    )

    rows = backend._get_items("test-deploy", "handlers")
    handler_rows = [r for r in rows if r["data"].get("handler_id") == "dup-1"]
    assert len(handler_rows) == 1
    assert handler_rows[0]["id"] == oldest_id
    assert handler_rows[0]["data"]["status"] == "completed"


@pytest.mark.asyncio
async def test_update_collapses_duplicates_with_mismatched_run_id(
    store: AgentDataStore, backend: FakeAgentDataBackend
) -> None:
    """Duplicates collapse regardless of run_id — one row per handler_id.

    The survivor is the oldest row; the latest write's run_id and status are
    what ends up persisted on it.
    """
    oldest_id = _seed_raw_handler(
        backend, handler_id="dup-2", run_id="run-a", status="running"
    )
    _seed_raw_handler(backend, handler_id="dup-2", run_id="run-b", status="running")

    await store.update(
        make_handler(handler_id="dup-2", run_id="run-b", status="completed")
    )

    rows = backend._get_items("test-deploy", "handlers")
    handler_rows = [r for r in rows if r["data"].get("handler_id") == "dup-2"]
    assert len(handler_rows) == 1
    assert handler_rows[0]["id"] == oldest_id
    assert handler_rows[0]["data"]["run_id"] == "run-b"
    assert handler_rows[0]["data"]["status"] == "completed"


@pytest.mark.asyncio
async def test_query_multiple_run_ids(store: AgentDataStore) -> None:
    await store.update(make_handler(handler_id="h1", run_id="run-1"))
    await store.update(make_handler(handler_id="h2", run_id="run-2"))
    await store.update(make_handler(handler_id="h3", run_id="run-3"))

    result = await store.query(HandlerQuery(run_id_in=["run-1", "run-3"]))
    ids = {h.handler_id for h in result}
    assert ids == {"h1", "h3"}


# ---------------------------------------------------------------------------
# Event journal tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_append_event_and_query(store: AgentDataStore) -> None:
    await store.append_event("run-1", make_envelope(seq_label=0))

    result = await store.query_events("run-1")
    assert len(result) == 1
    assert result[0].run_id == "run-1"
    assert result[0].sequence == 0
    assert result[0].event.type == "Event"


@pytest.mark.asyncio
async def test_append_multiple_events(store: AgentDataStore) -> None:
    for i in range(5):
        await store.append_event("run-1", make_envelope(seq_label=i))

    result = await store.query_events("run-1")
    assert len(result) == 5
    assert [e.sequence for e in result] == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_query_events_after_sequence(store: AgentDataStore) -> None:
    for i in range(5):
        await store.append_event("run-1", make_envelope(seq_label=i))

    result = await store.query_events("run-1", after_sequence=2)
    assert len(result) == 2
    assert [e.sequence for e in result] == [3, 4]


@pytest.mark.asyncio
async def test_query_events_with_limit(store: AgentDataStore) -> None:
    for i in range(5):
        await store.append_event("run-1", make_envelope(seq_label=i))

    result = await store.query_events("run-1", limit=3)
    assert len(result) == 3
    assert [e.sequence for e in result] == [0, 1, 2]


@pytest.mark.asyncio
async def test_query_events_nonexistent_run(store: AgentDataStore) -> None:
    result = await store.query_events("nonexistent")
    assert result == []


@pytest.mark.asyncio
async def test_events_isolated_by_run_id(store: AgentDataStore) -> None:
    for i in range(3):
        await store.append_event("run-a", make_envelope(seq_label=i))
    for i in range(2):
        await store.append_event("run-b", make_envelope(seq_label=i))

    result_a = await store.query_events("run-a")
    result_b = await store.query_events("run-b")

    assert len(result_a) == 3
    assert len(result_b) == 2


# ---------------------------------------------------------------------------
# Event subscription tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_events_receives_appended(store: AgentDataStore) -> None:
    collected, task = await _subscribe_and_collect(store, "run-1")

    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    await asyncio.wait_for(task, timeout=2.0)
    assert len(collected) == 3
    assert [e.sequence for e in collected] == [0, 1, 2]


@pytest.mark.asyncio
async def test_subscribe_events_terminates_on_stop(store: AgentDataStore) -> None:
    collected, task = await _subscribe_and_collect(store, "run-1")

    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    await asyncio.wait_for(task, timeout=2.0)
    assert len(collected) == 2
    assert collected[-1].event.type == "StopEvent"


@pytest.mark.asyncio
async def test_subscribe_events_already_terminated(store: AgentDataStore) -> None:
    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    collected: list[StoredEvent] = []
    async for event in store.subscribe_events("run-1"):
        collected.append(event)

    assert len(collected) == 2
    assert collected[-1].event.type == "StopEvent"


@pytest.mark.asyncio
async def test_subscribe_events_with_after_sequence(store: AgentDataStore) -> None:
    for i in range(3):
        await store.append_event("run-1", make_envelope(seq_label=i))

    collected, task = await _subscribe_and_collect(store, "run-1", after_sequence=1)

    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    await asyncio.wait_for(task, timeout=2.0)
    assert [e.sequence for e in collected] == [2, 3]


# ---------------------------------------------------------------------------
# Tick journal tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_append_tick_and_get(store: AgentDataStore) -> None:
    await store.append_tick("run-1", {"step": "a", "state": {}})
    await store.append_tick("run-1", {"step": "b", "state": {}})

    ticks = await store.get_ticks("run-1")
    assert len(ticks) == 2
    assert ticks[0].sequence == 0
    assert ticks[1].sequence == 1
    assert ticks[0].tick_data["step"] == "a"
    assert ticks[1].tick_data["step"] == "b"


@pytest.mark.asyncio
async def test_get_ticks_empty(store: AgentDataStore) -> None:
    ticks = await store.get_ticks("nonexistent")
    assert ticks == []


@pytest.mark.asyncio
async def test_ticks_isolated_by_run_id(store: AgentDataStore) -> None:
    await store.append_tick("run-a", {"step": "a1"})
    await store.append_tick("run-b", {"step": "b1"})
    await store.append_tick("run-b", {"step": "b2"})

    assert len(await store.get_ticks("run-a")) == 1
    assert len(await store.get_ticks("run-b")) == 2


# ---------------------------------------------------------------------------
# State store tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_state_store_returns_in_memory(store: AgentDataStore) -> None:
    state_store = store.create_state_store("run-1")
    assert isinstance(state_store, AgentDataStateStore)


@pytest.mark.asyncio
async def test_create_state_store_with_type(store: AgentDataStore) -> None:
    state_store = store.create_state_store("run-1", state_type=DictState)
    assert isinstance(state_store, AgentDataStateStore)
    assert state_store.state_type is DictState


@pytest.mark.asyncio
async def test_create_state_store_seeds_from_in_memory_serialized_state(
    backend: FakeAgentDataBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = JsonSerializer()
    seed = InMemoryStateStore(DictState())
    await seed.set("token", "persisted")
    serialized_state = seed.to_dict(serializer)
    store = create_agent_data_store_with_collection(
        backend, monkeypatch, collection="handlers"
    )

    state_store = store.create_state_store(
        "run-seeded",
        serialized_state=serialized_state,
        serializer=serializer,
    )

    assert await state_store.get("token") == "persisted"

    restored = create_agent_data_store_with_collection(
        backend, monkeypatch, collection="handlers"
    )
    reconnected = restored.create_state_store(
        "run-seeded",
        serialized_state=state_store.to_dict(serializer),
        serializer=serializer,
    )
    assert await reconnected.get("token") == "persisted"


@pytest.mark.asyncio
async def test_create_state_store_reconnects_agent_data_handle_collection(
    backend: FakeAgentDataBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = JsonSerializer()
    original = create_agent_data_store_with_collection(
        backend, monkeypatch, collection="handlers"
    )
    original_state = original.create_state_store("run-agent-data-handle")
    await original_state.set("token", "persisted")
    serialized_state = original_state.to_dict(serializer)

    restored = create_agent_data_store_with_collection(
        backend, monkeypatch, collection="other_handlers"
    )
    restored_state = restored.create_state_store(
        "run-agent-data-handle",
        serialized_state=serialized_state,
        serializer=serializer,
    )

    assert await restored_state.get("token") == "persisted"


@pytest.mark.asyncio
async def test_legacy_agent_data_typed_state_decodes_without_metadata(
    backend: FakeAgentDataBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = JsonSerializer()
    store = create_agent_data_store_with_collection(
        backend, monkeypatch, collection="handlers"
    )
    backend.create(
        "test-deploy",
        "handlers_state",
        {
            "run_id": "run-legacy-typed",
            "data": serializer.serialize(
                AgentDataCounterState(count=7, label="legacy")
            ),
        },
    )

    state_store = store.create_state_store(
        "run-legacy-typed",
        state_type=AgentDataCounterState,
        serializer=serializer,
    )

    state = await state_store.get_state()
    assert isinstance(state, AgentDataCounterState)
    assert state.count == 7
    assert state.label == "legacy"


# ---------------------------------------------------------------------------
# LRU cache behavior tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_uses_cache_on_second_call(store: AgentDataStore) -> None:
    """After the first update caches the ID, subsequent updates use it."""
    await store.update(make_handler(handler_id="h1", status="running"))
    cached = store._id_cache.get("h1")
    assert cached is not None

    # Second update should use cached ID (no search)
    await store.update(make_handler(handler_id="h1", status="completed"))
    result = await store.query(HandlerQuery(handler_id_in=["h1"]))
    assert result[0].status == "completed"


# ---------------------------------------------------------------------------
# Bug: sequence counters reset across store instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sequence_continues_after_new_store_instance(
    backend: FakeAgentDataBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A new store instance should continue sequences from existing data."""
    store1 = create_agent_data_store(backend, monkeypatch)
    await store1.append_event("run-1", make_envelope(seq_label=0))
    await store1.append_event("run-1", make_envelope(seq_label=1))

    # Gather in-flight events so they're visible to the next store instance
    await store1._regroup_events("run-1")

    # Simulate server restart: new store instance, same backend
    store2 = create_agent_data_store(backend, monkeypatch)
    await store2.append_event("run-1", make_envelope(seq_label=2))

    events = await store2.query_events("run-1")
    sequences = [e.sequence for e in events]
    # Should be [0, 1, 2] with no duplicates
    assert sequences == [0, 1, 2], (
        f"Expected unique sequences [0, 1, 2], got {sequences}"
    )


@pytest.mark.asyncio
async def test_tick_sequence_continues_after_new_store_instance(
    backend: FakeAgentDataBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same as above but for ticks."""
    store1 = create_agent_data_store(backend, monkeypatch)
    await store1.append_tick("run-1", {"step": 0})
    await store1.append_tick("run-1", {"step": 1})
    # Ensure in-flight tick writes land before the new instance queries _max_sequence
    await store1._regroup_ticks("run-1")

    store2 = create_agent_data_store(backend, monkeypatch)
    await store2.append_tick("run-1", {"step": 2})

    ticks = await store2.get_ticks("run-1")
    sequences = [t.sequence for t in ticks]
    assert sequences == [0, 1, 2], (
        f"Expected unique sequences [0, 1, 2], got {sequences}"
    )


# ---------------------------------------------------------------------------
# Bug: from_dict loses collection name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_state_store_from_dict_preserves_collection(
    store: AgentDataStore,
) -> None:
    """from_dict should produce a store pointing at the same collection."""
    state_store = store.create_state_store("run-1")
    await state_store.set(path="key", value="hello")

    serialized = state_store.to_dict(JsonSerializer())
    restored = AgentDataStateStore.from_dict(
        serialized,
        JsonSerializer(),
        client=store._client,
        run_id="run-1",
    )

    # Should be able to read data written by the original store
    val = await restored.get("key")
    assert val == "hello"


# ---------------------------------------------------------------------------
# HTTP client reuse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_data_client_reuses_http_client(store: AgentDataStore) -> None:
    client = store._client
    c1 = client.http_client()
    c2 = client.http_client()
    assert c1 is c2


# ---------------------------------------------------------------------------
# In-memory fan-out queues
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_events_multiple_concurrent_subscribers(
    store: AgentDataStore,
) -> None:
    collected_a, task_a = await _subscribe_and_collect(store, "run-1")
    collected_b, task_b = await _subscribe_and_collect(store, "run-1")

    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    await asyncio.wait_for(task_a, timeout=2.0)
    await asyncio.wait_for(task_b, timeout=2.0)

    assert len(collected_a) == 3
    assert len(collected_b) == 3
    assert [e.sequence for e in collected_a] == [0, 1, 2]
    assert [e.sequence for e in collected_b] == [0, 1, 2]


@pytest.mark.asyncio
async def test_subscribe_events_backfill_and_live(store: AgentDataStore) -> None:
    # Append some events before subscribing (these will be backfilled)
    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))

    collected, task = await _subscribe_and_collect(store, "run-1", after_sequence=-1)

    # Append live events after subscriber is listening
    await store.append_event("run-1", make_envelope(seq_label=2))
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    await asyncio.wait_for(task, timeout=2.0)

    assert len(collected) == 4
    assert [e.sequence for e in collected] == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# Fire-and-forget persistence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_events_fire_and_forget_during_streaming(
    store: AgentDataStore, backend: FakeAgentDataBackend
) -> None:
    collected, task = await _subscribe_and_collect(store, "run-1")

    # Append several non-terminal events (fire-and-forget)
    for i in range(5):
        await store.append_event("run-1", make_envelope(seq_label=i))

    # Subscriber receives events immediately via in-memory queue
    await _wait_collected_count(collected, 5)

    # Terminal event gathers all pending writes
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))
    await asyncio.wait_for(task, timeout=2.0)

    events_key = ("test-deploy", store._events_collection)
    persisted_after = len(backend._items.get(events_key, []))
    assert persisted_after == 6
    assert len(collected) == 6


@pytest.mark.asyncio
async def test_terminal_event_gathers_all_pending(
    store: AgentDataStore, backend: FakeAgentDataBackend
) -> None:
    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    # After terminal append returns, all events should be persisted
    events_key = ("test-deploy", store._events_collection)
    persisted = len(backend._items.get(events_key, []))
    assert persisted == 3


@pytest.mark.asyncio
async def test_events_not_persisted_until_gathered(
    store: AgentDataStore, backend: FakeAgentDataBackend
) -> None:
    """Events use fire-and-forget tasks; regroup_events gathers them."""
    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))

    # Pending tasks exist but may not have completed yet
    assert "run-1" in store._pending_events
    assert len(store._pending_events["run-1"]) == 2

    # After regrouping, events are persisted
    await store._regroup_events("run-1")

    events_key = ("test-deploy", store._events_collection)
    persisted = len(backend._items.get(events_key, []))
    assert persisted == 2


@pytest.mark.asyncio
async def test_regroup_events_surfaces_errors(
    store: AgentDataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When an event create fails, _regroup_events raises the error."""
    original_create = store._client.create

    async def failing_create(collection: str, data: dict[str, Any]) -> dict[str, Any]:
        if collection == store._events_collection:
            raise RuntimeError("simulated failure")
        return await original_create(collection, data)

    monkeypatch.setattr(store._client, "create", failing_create)

    await store.append_event("run-1", make_envelope(seq_label=0))

    with pytest.raises(RuntimeError, match="simulated failure"):
        await store._regroup_events("run-1")


@pytest.mark.asyncio
async def test_cleanup_run_removes_subscriber_queues(store: AgentDataStore) -> None:
    """_cleanup_run should remove the run_id key from _subscriber_queues."""
    store._add_subscriber_queue("run-1")
    assert "run-1" in store._subscriber_queues

    await store._cleanup_run("run-1")
    assert "run-1" not in store._subscriber_queues


@pytest.mark.asyncio
async def test_cleanup_run_removes_sequence_counters(store: AgentDataStore) -> None:
    """_cleanup_run should remove sequence counters for the completed run."""
    # Populate sequence counters by appending events and ticks
    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_tick("run-1", {"step": "a"})

    assert "run-1" in store._event_sequences
    assert "run-1" in store._tick_sequences

    # Trigger cleanup via terminal event
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))

    assert "run-1" not in store._event_sequences
    assert "run-1" not in store._tick_sequences


@pytest.mark.asyncio
async def test_persist_error_does_not_block_in_memory_delivery(
    store: AgentDataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In-memory subscribers receive events even when HTTP persistence fails."""
    collected, task = await _subscribe_and_collect(store, "run-1")

    # Make client.create raise to simulate persistence failure
    original_create = store._client.create

    async def broken_create(collection: str, data: dict[str, Any]) -> dict[str, Any]:
        if collection == store._events_collection:
            raise RuntimeError("simulated API failure")
        return await original_create(collection, data)

    monkeypatch.setattr(store._client, "create", broken_create)

    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))

    # Subscriber should still receive events via in-memory queue
    await _wait_collected_count(collected, 2)

    # The failed tasks are still pending — _regroup_events would raise
    with pytest.raises(RuntimeError, match="simulated API failure"):
        await store._regroup_events("run-1")

    # Restore create so terminal event can persist and clean up
    monkeypatch.setattr(store._client, "create", original_create)
    await store.append_event("run-1", make_envelope(event=StopEvent(data="done")))
    await asyncio.wait_for(task, timeout=2.0)
    assert len(collected) == 3
