# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from llama_agents.server import (
    AbstractWorkflowStore,
    MemoryWorkflowStore,
    SqliteWorkflowStore,
)
from llama_agents.server._store.abstract_workflow_store import StoredEvent
from llama_agents_integration_tests.fake_agent_data import (
    FakeAgentDataBackend,
    create_agent_data_store,
)
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows.events import (
    Event,
    StopEvent,
    WorkflowCancelledEvent,
    WorkflowFailedEvent,
    _set_event_origin_namespace,
)


def make_envelope(
    event: Event | None = None,
    seq_label: int = 0,
) -> EventEnvelopeWithMetadata:
    """Create an EventEnvelopeWithMetadata by serializing a real Event."""
    if event is None:
        event = Event(data=f"seq-{seq_label}")
    return EventEnvelopeWithMetadata.from_event(event, include_qualified_name=False)


def _stop(seq_label: int = 0) -> StopEvent:
    return StopEvent(data=f"seq-{seq_label}")


def _failed() -> WorkflowFailedEvent:
    return WorkflowFailedEvent(
        step_name="test_step",
        exception=ValueError("boom"),
        attempts=1,
        elapsed_seconds=0.0,
    )


def _cancelled() -> WorkflowCancelledEvent:
    return WorkflowCancelledEvent()


async def _subscribe_and_collect(
    store: AbstractWorkflowStore,
    run_id: str,
    after_sequence: int | None = None,
) -> tuple[list[StoredEvent], asyncio.Task[None]]:
    """Subscribe to events, returning the collected list and the consumer task."""
    collected: list[StoredEvent] = []

    async def consumer() -> None:
        kwargs = {} if after_sequence is None else {"after_sequence": after_sequence}
        async for event in store.subscribe_events(run_id, **kwargs):
            collected.append(event)

    subscriber_queues = getattr(store, "_subscriber_queues", None)
    existing_queue_count = (
        len(subscriber_queues.get(run_id, ()))
        if subscriber_queues is not None
        else None
    )
    task = asyncio.create_task(consumer())

    async def registered() -> None:
        conditions = getattr(store, "_conditions", None)
        if conditions is not None:
            assert run_id in conditions
            return

        if subscriber_queues is not None:
            assert existing_queue_count is not None
            assert len(subscriber_queues.get(run_id, ())) > existing_queue_count
            return

        raise AssertionError("store does not expose subscription registration state")

    await wait_for_passing(registered, max_duration=2.0, interval=0.01)
    return collected, task


@pytest.fixture(params=["memory", "sqlite", "agent_data"])
def store(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AbstractWorkflowStore:
    if request.param == "memory":
        return MemoryWorkflowStore()
    elif request.param == "sqlite":
        return SqliteWorkflowStore(str(tmp_path / "test.sqlite"), poll_interval=0.05)
    else:
        return create_agent_data_store(FakeAgentDataBackend(), monkeypatch)


@pytest.mark.asyncio
async def test_append_single_event_and_query_it_back(
    store: AbstractWorkflowStore,
) -> None:
    await store.append_event("run-1", make_envelope(seq_label=0))

    result = await store.query_events("run-1")
    assert len(result) == 1
    assert result[0].run_id == "run-1"
    assert result[0].sequence == 0
    assert result[0].event.type == "Event"


@pytest.mark.asyncio
async def test_append_multiple_events_and_query_all(
    store: AbstractWorkflowStore,
) -> None:
    for i in range(5):
        await store.append_event("run-1", make_envelope(seq_label=i))

    result = await store.query_events("run-1")
    assert len(result) == 5
    assert [e.sequence for e in result] == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seed_count, after_sequence, limit, expected_sequences",
    [
        pytest.param(5, 2, None, [3, 4], id="after_sequence_only"),
        pytest.param(5, None, 3, [0, 1, 2], id="limit_only"),
        pytest.param(10, 3, 2, [4, 5], id="after_sequence_and_limit"),
    ],
)
async def test_query_events_with_filters(
    store: AbstractWorkflowStore,
    seed_count: int,
    after_sequence: int | None,
    limit: int | None,
    expected_sequences: list[int],
) -> None:
    for i in range(seed_count):
        await store.append_event("run-1", make_envelope(seq_label=i))

    result = await store.query_events(
        "run-1", after_sequence=after_sequence, limit=limit
    )
    assert len(result) == len(expected_sequences)
    assert [e.sequence for e in result] == expected_sequences


@pytest.mark.asyncio
async def test_query_events_for_nonexistent_run_id_returns_empty(
    store: AbstractWorkflowStore,
) -> None:
    result = await store.query_events("nonexistent-run")
    assert result == []


@pytest.mark.asyncio
async def test_events_from_different_run_ids_are_isolated(
    store: AbstractWorkflowStore,
) -> None:
    for i in range(3):
        await store.append_event("run-a", make_envelope(seq_label=i))
    for i in range(2):
        await store.append_event("run-b", make_envelope(seq_label=i))

    result_a = await store.query_events("run-a")
    result_b = await store.query_events("run-b")

    assert len(result_a) == 3
    assert all(e.run_id == "run-a" for e in result_a)

    assert len(result_b) == 2
    assert all(e.run_id == "run-b" for e in result_b)


@pytest.mark.asyncio
async def test_subscribe_events_receives_appended_events(
    store: AbstractWorkflowStore,
) -> None:
    """Appending events wakes a waiting subscriber."""
    collected, task = await _subscribe_and_collect(store, "run-1")

    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))
    await store.append_event("run-1", make_envelope(event=_stop(seq_label=2)))

    await asyncio.wait_for(task, timeout=5.0)

    assert len(collected) == 3
    assert [e.sequence for e in collected] == [0, 1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_event, expected_type",
    [
        pytest.param(_failed(), "WorkflowFailedEvent", id="failed"),
        pytest.param(_cancelled(), "WorkflowCancelledEvent", id="cancelled"),
    ],
)
async def test_subscribe_events_terminates_on_terminal_event(
    store: AbstractWorkflowStore,
    terminal_event: Event,
    expected_type: str,
) -> None:
    """Subscriber terminates after receiving a terminal event."""
    collected, task = await _subscribe_and_collect(store, "run-1")

    if expected_type == "WorkflowFailedEvent":
        await store.append_event("run-1", make_envelope())
    await store.append_event("run-1", make_envelope(event=terminal_event))

    await asyncio.wait_for(task, timeout=5.0)

    assert collected[-1].event.type == expected_type


@pytest.mark.asyncio
async def test_subscribe_events_multiple_concurrent_subscribers(
    store: AbstractWorkflowStore,
) -> None:
    """Multiple concurrent subscribers on the same run each receive all events."""
    collected_a, task_a = await _subscribe_and_collect(store, "run-1")
    collected_b, task_b = await _subscribe_and_collect(store, "run-1")

    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))
    await store.append_event("run-1", make_envelope(event=_stop(seq_label=2)))

    await asyncio.wait_for(asyncio.gather(task_a, task_b), timeout=5.0)

    assert len(collected_a) == 3
    assert len(collected_b) == 3
    assert [e.sequence for e in collected_a] == [0, 1, 2]
    assert [e.sequence for e in collected_b] == [0, 1, 2]


@pytest.mark.asyncio
async def test_subscribe_events_with_after_sequence(
    store: AbstractWorkflowStore,
) -> None:
    """Subscriber can resume from a specific sequence position."""
    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(seq_label=1))
    await store.append_event("run-1", make_envelope(seq_label=2))

    collected, task = await _subscribe_and_collect(store, "run-1", after_sequence=1)

    await store.append_event("run-1", make_envelope(event=_stop(seq_label=3)))

    await asyncio.wait_for(task, timeout=5.0)

    # Should have events 2, 3 (skipping 0 and 1)
    assert len(collected) == 2
    assert [e.sequence for e in collected] == [2, 3]


@pytest.mark.asyncio
async def test_subscribe_events_already_terminated(
    store: AbstractWorkflowStore,
) -> None:
    """If events already contain a terminal event, subscriber terminates immediately."""
    await store.append_event("run-1", make_envelope(seq_label=0))
    await store.append_event("run-1", make_envelope(event=_stop(seq_label=1)))

    collected: list[StoredEvent] = []
    async for event in store.subscribe_events("run-1"):
        collected.append(event)

    assert len(collected) == 2
    assert collected[-1].event.type == "StopEvent"


@pytest.mark.asyncio
async def test_stream_ticks_yields_all_rows_in_order(
    store: AbstractWorkflowStore,
) -> None:
    for i in range(13):
        await store.append_tick("run-1", {"type": "TickSendEvent", "i": i})

    yielded: list[int] = []
    async for tick in store.stream_ticks("run-1"):
        yielded.append(tick.sequence)

    assert yielded == list(range(13))


@pytest.mark.asyncio
async def test_stream_ticks_empty_history(
    store: AbstractWorkflowStore,
) -> None:
    yielded: list[object] = []
    async for tick in store.stream_ticks("empty-run"):
        yielded.append(tick)
    assert yielded == []


def _stored(event: Event) -> StoredEvent:
    return StoredEvent(
        run_id="run-1",
        sequence=0,
        timestamp=datetime.now(timezone.utc),
        event=EventEnvelopeWithMetadata.from_event(event),
    )


def test_child_origin_stop_event_is_not_terminal() -> None:
    """A StopEvent carrying a child origin namespace is a boundary event a child
    wrote to the stream; it must not terminate the run's event stream."""
    root_stop = _stop()
    child_stop = _stop()
    _set_event_origin_namespace(child_stop, ("child#0",))

    assert AbstractWorkflowStore._is_terminal_event(_stored(root_stop)) is True
    assert AbstractWorkflowStore._is_terminal_event(_stored(child_stop)) is False
