# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from llama_agents.server import (
    AbstractWorkflowStore,
    HandlerQuery,
    MemoryWorkflowStore,
    PersistentHandler,
)
from llama_agents.server._store.abstract_workflow_store import Status, StoredEvent
from workflows.events import (
    Event,
    StopEvent,
    WorkflowCancelledEvent,
    WorkflowFailedEvent,
)

T0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _ts(seconds: int) -> datetime:
    return datetime(2024, 1, 1, 0, 0, seconds, tzinfo=timezone.utc)


def _handler(
    handler_id: str = "h1",
    workflow_name: str = "wf",
    status: Status = "running",
    **kwargs: Any,
) -> PersistentHandler:
    return PersistentHandler(
        handler_id=handler_id,
        workflow_name=workflow_name,
        status=status,
        **kwargs,
    )


async def _insert(
    store: MemoryWorkflowStore,
    handler_id: str = "h1",
    workflow_name: str = "wf",
    status: Status = "running",
    **kwargs: Any,
) -> PersistentHandler:
    h = _handler(
        handler_id=handler_id, workflow_name=workflow_name, status=status, **kwargs
    )
    await store.update(h)
    return h


async def _query_ids(store: MemoryWorkflowStore, **kwargs: Any) -> set[str]:
    result = await store.query(HandlerQuery(**kwargs))
    return {h.handler_id for h in result}


def _make_stored_event(event: Event, run_id: str = "run-1") -> StoredEvent:
    return StoredEvent(
        run_id=run_id,
        sequence=0,
        timestamp=datetime.now(timezone.utc),
        event=EventEnvelopeWithMetadata.from_event(event),
    )


@pytest.fixture
def store() -> MemoryWorkflowStore:
    return MemoryWorkflowStore()


@pytest.mark.asyncio
async def test_update_and_query_returns_inserted_handler(
    store: MemoryWorkflowStore,
) -> None:
    await _insert(store, handler_id="h1", workflow_name="wf_a")

    result = await store.query(
        HandlerQuery(workflow_name_in=["wf_a"], status_in=["running"])
    )
    assert len(result) == 1
    found = result[0]
    assert found.handler_id == "h1"
    assert found.workflow_name == "wf_a"
    assert found.status == "running"


@pytest.mark.asyncio
async def test_update_on_conflict_overwrites_existing_row(
    store: MemoryWorkflowStore,
) -> None:
    await _insert(store, handler_id="h2", workflow_name="wf_b")
    await _insert(store, handler_id="h2", workflow_name="wf_b", status="completed")

    assert (
        await _query_ids(store, workflow_name_in=["wf_b"], status_in=["running"])
        == set()
    )

    result_completed = await store.query(
        HandlerQuery(workflow_name_in=["wf_b"], status_in=["completed"])
    )
    assert len(result_completed) == 1
    found = result_completed[0]
    assert found.handler_id == "h2"
    assert found.workflow_name == "wf_b"
    assert found.status == "completed"


@pytest.mark.asyncio
async def test_delete_filters_by_query(store: MemoryWorkflowStore) -> None:
    await _insert(
        store, handler_id="delete-me", workflow_name="wf_delete", status="completed"
    )
    await _insert(store, handler_id="keep-me", workflow_name="wf_keep")

    deleted = await store.delete(HandlerQuery(handler_id_in=["delete-me"]))
    assert deleted == 1
    assert await _query_ids(store) == {"keep-me"}


@pytest.mark.asyncio
async def test_delete_noop_on_empty_filter(store: MemoryWorkflowStore) -> None:
    await _insert(
        store, handler_id="delete-me", workflow_name="wf_delete", status="completed"
    )

    deleted = await store.delete(HandlerQuery(handler_id_in=[]))
    assert deleted == 0
    assert await _query_ids(store) == {"delete-me"}


@pytest.mark.asyncio
async def test_query_filters_by_handler_id_and_empty_lists(
    store: MemoryWorkflowStore,
) -> None:
    for hid, wf in [("h1", "wf_a"), ("h2", "wf_a"), ("h3", "wf_b")]:
        await _insert(store, handler_id=hid, workflow_name=wf)

    assert await _query_ids(store, handler_id_in=["h1", "h3"]) == {"h1", "h3"}
    assert await store.query(HandlerQuery(handler_id_in=[])) == []
    assert await store.query(HandlerQuery(workflow_name_in=[])) == []
    assert await _query_ids(store) == {"h1", "h2", "h3"}


@pytest.mark.asyncio
async def test_query_filters_by_multiple_statuses(store: MemoryWorkflowStore) -> None:
    statuses: list[tuple[str, Status]] = [
        ("h1", "running"),
        ("h2", "completed"),
        ("h3", "failed"),
        ("h4", "cancelled"),
    ]
    for hid, status in statuses:
        await _insert(store, handler_id=hid, status=status)

    assert await _query_ids(store, status_in=["completed", "failed"]) == {"h2", "h3"}
    assert await store.query(HandlerQuery(status_in=[])) == []


@pytest.mark.asyncio
async def test_query_filters_by_workflow_name(store: MemoryWorkflowStore) -> None:
    await _insert(store, handler_id="h1", workflow_name="wf_a")
    await _insert(store, handler_id="h2", workflow_name="wf_b")
    await _insert(store, handler_id="h3", workflow_name="wf_a", status="completed")

    assert await _query_ids(store, workflow_name_in=["wf_a"]) == {"h1", "h3"}
    assert await _query_ids(store, workflow_name_in=["wf_a", "wf_b"]) == {
        "h1",
        "h2",
        "h3",
    }


@pytest.mark.asyncio
async def test_query_combines_multiple_filters(store: MemoryWorkflowStore) -> None:
    await _insert(store, handler_id="h1", workflow_name="wf_a")
    await _insert(store, handler_id="h2", workflow_name="wf_a", status="completed")
    await _insert(store, handler_id="h3", workflow_name="wf_b")
    await _insert(store, handler_id="h4", workflow_name="wf_b", status="completed")

    assert await _query_ids(
        store, workflow_name_in=["wf_a"], status_in=["running"]
    ) == {"h1"}
    assert await _query_ids(
        store,
        handler_id_in=["h2", "h4"],
        workflow_name_in=["wf_a"],
        status_in=["completed"],
    ) == {"h2"}


@pytest.mark.asyncio
async def test_delete_removes_multiple_matching_handlers(
    store: MemoryWorkflowStore,
) -> None:
    for i in range(5):
        await _insert(
            store, handler_id=f"h{i}", status="completed" if i % 2 == 0 else "running"
        )

    deleted = await store.delete(HandlerQuery(status_in=["completed"]))
    assert deleted == 3
    assert await _query_ids(store) == {"h1", "h3"}


@pytest.mark.asyncio
async def test_store_handles_all_datetime_fields(store: MemoryWorkflowStore) -> None:
    now = datetime.now(timezone.utc)
    stop = StopEvent(result={"output": "success"})
    await _insert(
        store,
        status="completed",
        run_id="run123",
        error=None,
        result=stop,
        started_at=now,
        updated_at=now,
        completed_at=now,
    )

    result = await store.query(HandlerQuery(handler_id_in=["h1"]))
    assert len(result) == 1
    found = result[0]
    assert found.run_id == "run123"
    assert found.result == stop
    assert found.started_at == now
    assert found.updated_at == now
    assert found.completed_at == now


@pytest.mark.asyncio
async def test_store_handles_error_field(store: MemoryWorkflowStore) -> None:
    await _insert(store, status="failed", error="Something went wrong")

    result = await store.query(HandlerQuery(handler_id_in=["h1"]))
    assert len(result) == 1
    assert result[0].error == "Something went wrong"


@pytest.mark.asyncio
async def test_empty_store_returns_empty_results(store: MemoryWorkflowStore) -> None:
    assert await store.query(HandlerQuery()) == []
    assert await store.delete(HandlerQuery(handler_id_in=["nonexistent"])) == 0


@pytest.mark.asyncio
async def test_update_handler_status_with_nonexistent_run_id(
    store: MemoryWorkflowStore,
) -> None:
    await store.update_handler_status("nonexistent-run-id", status="completed")


@pytest.mark.asyncio
async def test_update_handler_status_sets_status_and_completed_at(
    store: MemoryWorkflowStore,
) -> None:
    await _insert(store, run_id="run-1")

    await store.update_handler_status("run-1", status="completed")

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert len(result) == 1
    assert result[0].status == "completed"
    assert result[0].updated_at is not None
    assert result[0].completed_at is not None


@pytest.mark.asyncio
async def test_update_handler_status_with_result(store: MemoryWorkflowStore) -> None:
    await _insert(store, run_id="run-1")

    stop = StopEvent(result={"answer": 42})
    await store.update_handler_status("run-1", status="completed", result=stop)

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert result[0].status == "completed"
    assert result[0].result == stop


@pytest.mark.asyncio
async def test_update_handler_status_with_error(store: MemoryWorkflowStore) -> None:
    await _insert(store, run_id="run-1")

    await store.update_handler_status("run-1", status="failed", error="boom")

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert result[0].status == "failed"
    assert result[0].error == "boom"
    assert result[0].completed_at is not None


@pytest.mark.asyncio
async def test_update_handler_status_idle_since_explicit_none_clears(
    store: MemoryWorkflowStore,
) -> None:
    now = datetime.now(timezone.utc)
    await _insert(store, run_id="run-1", idle_since=now)

    await store.update_handler_status("run-1", idle_since=None)

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert result[0].idle_since is None


@pytest.mark.asyncio
async def test_update_handler_status_idle_since_unset_preserves(
    store: MemoryWorkflowStore,
) -> None:
    now = datetime.now(timezone.utc)
    await _insert(store, run_id="run-1", idle_since=now)

    await store.update_handler_status("run-1", status="running")

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert result[0].idle_since == now


@pytest.mark.asyncio
async def test_update_handler_status_non_terminal_does_not_set_completed_at(
    store: MemoryWorkflowStore,
) -> None:
    await _insert(store, run_id="run-1")

    await store.update_handler_status("run-1", status="running")

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert result[0].completed_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["completed", "failed", "cancelled"])
async def test_update_handler_status_terminal_sets_completed_at(
    store: MemoryWorkflowStore,
    terminal_status: Status,
) -> None:
    await _insert(store, run_id="run-1")

    await store.update_handler_status("run-1", status=terminal_status)

    result = await store.query(HandlerQuery(run_id_in=["run-1"]))
    assert result[0].completed_at is not None


def test_is_terminal_event_stop_event() -> None:
    stored = _make_stored_event(StopEvent(result="done"))
    assert AbstractWorkflowStore._is_terminal_event(stored) is True


def test_is_terminal_event_regular_event() -> None:
    stored = _make_stored_event(Event())
    assert AbstractWorkflowStore._is_terminal_event(stored) is False


def test_is_terminal_event_workflow_failed_event() -> None:
    event = WorkflowFailedEvent(
        step_name="my_step",
        exception=ValueError("bad value"),
        attempts=1,
        elapsed_seconds=0.1,
    )
    stored = _make_stored_event(event)
    assert AbstractWorkflowStore._is_terminal_event(stored) is True


def test_is_terminal_event_workflow_cancelled_event() -> None:
    stored = _make_stored_event(WorkflowCancelledEvent())
    assert AbstractWorkflowStore._is_terminal_event(stored) is True


# --- max_completed history cap tests ---


@pytest.mark.asyncio
async def test_max_completed_default_is_1000() -> None:
    assert MemoryWorkflowStore().max_completed == 1000


@pytest.mark.asyncio
async def test_max_completed_none_means_unlimited() -> None:
    store = MemoryWorkflowStore(max_completed=None)
    assert store.max_completed is None

    for i in range(50):
        await _insert(
            store,
            handler_id=f"h{i}",
            status="completed",
            run_id=f"run-{i}",
            completed_at=_ts(i),
        )

    assert len(await store.query(HandlerQuery())) == 50


def test_max_completed_negative_raises_value_error() -> None:
    with pytest.raises(ValueError, match="max_completed must be >= 0 or None"):
        MemoryWorkflowStore(max_completed=-1)


@pytest.mark.asyncio
async def test_max_completed_evicts_oldest_when_exceeded() -> None:
    store = MemoryWorkflowStore(max_completed=3)

    for i in range(5):
        await _insert(
            store,
            handler_id=f"h{i}",
            status="completed",
            run_id=f"run-{i}",
            completed_at=_ts(i),
        )

    assert await _query_ids(store) == {"h2", "h3", "h4"}


@pytest.mark.asyncio
async def test_max_completed_does_not_evict_running_handlers() -> None:
    store = MemoryWorkflowStore(max_completed=2)

    for i in range(3):
        await _insert(store, handler_id=f"running-{i}", run_id=f"run-r{i}")

    for i in range(3):
        await _insert(
            store,
            handler_id=f"done-{i}",
            status="completed",
            run_id=f"run-d{i}",
            completed_at=_ts(i),
        )

    assert await _query_ids(store) == {
        "running-0",
        "running-1",
        "running-2",
        "done-1",
        "done-2",
    }


@pytest.mark.asyncio
async def test_max_completed_applies_to_all_terminal_statuses() -> None:
    store = MemoryWorkflowStore(max_completed=2)

    await _insert(
        store, handler_id="h-completed", status="completed", completed_at=_ts(0)
    )
    await _insert(store, handler_id="h-failed", status="failed", completed_at=_ts(1))
    await _insert(
        store, handler_id="h-cancelled", status="cancelled", completed_at=_ts(2)
    )

    assert await _query_ids(store) == {"h-failed", "h-cancelled"}


@pytest.mark.asyncio
async def test_max_completed_cleans_up_events_ticks_and_state() -> None:
    store = MemoryWorkflowStore(max_completed=1)

    store.events["run-old"] = []
    store.ticks["run-old"] = []
    store.create_state_store("run-old")

    await _insert(
        store,
        handler_id="h-old",
        status="completed",
        run_id="run-old",
        completed_at=_ts(0),
    )
    assert "run-old" in store.events
    assert "run-old" in store.ticks
    assert ("run-old", ()) in store.state_stores

    await _insert(
        store,
        handler_id="h-new",
        status="completed",
        run_id="run-new",
        completed_at=_ts(1),
    )

    assert "run-old" not in store.events
    assert "run-old" not in store.ticks
    assert ("run-old", ()) not in store.state_stores
    remaining = await store.query(HandlerQuery())
    assert len(remaining) == 1
    assert remaining[0].handler_id == "h-new"


@pytest.mark.asyncio
async def test_max_completed_eviction_via_update_handler_status() -> None:
    """Eviction triggers when status changes to terminal via update_handler_status."""
    store = MemoryWorkflowStore(max_completed=2)

    for i in range(3):
        await _insert(store, handler_id=f"h{i}", run_id=f"run-{i}")

    await store.update_handler_status("run-0", status="completed")
    await store.update_handler_status("run-1", status="completed")
    await store.update_handler_status("run-2", status="completed")

    ids = await _query_ids(store)
    assert len(ids) == 2
    assert "h0" not in ids
    assert "h1" in ids
    assert "h2" in ids


@pytest.mark.asyncio
async def test_max_completed_ignores_stale_terminal_queue_entries() -> None:
    store = MemoryWorkflowStore(max_completed=1)

    await _insert(
        store,
        handler_id="shared",
        status="completed",
        run_id="run-old",
        completed_at=_ts(0),
    )
    await _insert(store, handler_id="shared", status="running", run_id="run-active")

    await _insert(
        store,
        handler_id="done-2",
        status="completed",
        run_id="run-2",
        completed_at=_ts(1),
    )

    handlers = await store.query(HandlerQuery())
    by_id = {handler.handler_id: handler for handler in handlers}
    assert set(by_id) == {"shared", "done-2"}
    assert by_id["shared"].status == "running"
