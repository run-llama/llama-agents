from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from llama_agents.server import (
    HandlerQuery,
    PersistentHandler,
    SqliteWorkflowStore,
)
from pydantic import BaseModel
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import (
    DictState,
    InMemoryStateStore,
    StateStore,
)
from workflows.events import StopEvent


@pytest.mark.asyncio
async def test_update_and_query_returns_inserted_handler(tmp_path: Path) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    handler = PersistentHandler(
        handler_id="h1",
        workflow_name="wf_a",
        status="running",
    )

    await store.update(handler)

    # Filter by workflow_name list
    result = await store.query(
        HandlerQuery(workflow_name_in=["wf_a"], status_in=["running"])
    )

    assert len(result) == 1
    found = result[0]
    assert found.handler_id == "h1"
    assert found.workflow_name == "wf_a"
    assert found.status == "running"


@pytest.mark.asyncio
async def test_update_on_conflict_overwrites_existing_row(tmp_path: Path) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    # Initial insert (in-progress)
    await store.update(
        PersistentHandler(
            handler_id="h2",
            workflow_name="wf_b",
            status="running",
        )
    )

    # Update same handler_id (completed)
    await store.update(
        PersistentHandler(
            handler_id="h2",
            workflow_name="wf_b",
            status="completed",
        )
    )

    # Should not be returned for completed=False
    result_in_progress = await store.query(
        HandlerQuery(workflow_name_in=["wf_b"], status_in=["running"])
    )
    assert result_in_progress == []

    # Should be returned for completed=True with latest values
    result_completed = await store.query(
        HandlerQuery(workflow_name_in=["wf_b"], status_in=["completed"])
    )
    assert len(result_completed) == 1
    found = result_completed[0]
    assert found.handler_id == "h2"
    assert found.workflow_name == "wf_b"
    assert found.status == "completed"


@pytest.mark.asyncio
async def test_delete_filters_by_query(tmp_path: Path) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    await store.update(
        PersistentHandler(
            handler_id="delete-me",
            workflow_name="wf_delete",
            status="completed",
        )
    )
    await store.update(
        PersistentHandler(
            handler_id="keep-me",
            workflow_name="wf_keep",
            status="running",
        )
    )

    deleted = await store.delete(HandlerQuery(handler_id_in=["delete-me"]))

    assert deleted == 1
    remaining = await store.query(HandlerQuery())
    ids = {handler.handler_id for handler in remaining}
    assert ids == {"keep-me"}


@pytest.mark.asyncio
async def test_delete_noop_on_empty_filter(tmp_path: Path) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    await store.update(
        PersistentHandler(
            handler_id="delete-me",
            workflow_name="wf_delete",
            status="completed",
        )
    )

    deleted = await store.delete(HandlerQuery(handler_id_in=[]))

    assert deleted == 0
    remaining = await store.query(HandlerQuery())
    assert len(remaining) == 1
    assert remaining[0].handler_id == "delete-me"


@pytest.mark.asyncio
async def test_query_filters_by_handler_id_and_empty_lists(tmp_path: Path) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    # Seed three handlers
    for hid, wf in [("h1", "wf_a"), ("h2", "wf_a"), ("h3", "wf_b")]:
        await store.update(
            PersistentHandler(
                handler_id=hid,
                workflow_name=wf,
                status="running",
            )
        )

    # Filter by specific handler ids
    result = await store.query(HandlerQuery(handler_id_in=["h1", "h3"]))
    ids = {h.handler_id for h in result}
    assert ids == {"h1", "h3"}

    # Empty handler_id list short-circuits to []
    result_empty_ids = await store.query(HandlerQuery(handler_id_in=[]))
    assert result_empty_ids == []

    # Empty workflow_name list short-circuits to []
    result_empty_wf = await store.query(HandlerQuery(workflow_name_in=[]))
    assert result_empty_wf == []

    # No filters returns all
    all_rows = await store.query(HandlerQuery())
    assert {h.handler_id for h in all_rows} == {"h1", "h2", "h3"}


class CustomStopEvent(StopEvent):
    x: int
    y: list[int]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event",
    [StopEvent(result={"meta": {"x": 1, "y": [2, 3]}}), CustomStopEvent(x=1, y=[2, 3])],
)
async def test_update_pydantic_result_serialization(
    tmp_path: Path, event: StopEvent
) -> None:
    """
    Ensures that a Pydantic BaseModel (StopEvent) stored in `result` is properly
    serialized using model_dump_json() and does not raise TypeError as with json.dumps.
    Also validates round-trip deserialization shape.
    """
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    handler = PersistentHandler(
        handler_id="pydantic-result",
        workflow_name="wf_pyd",
        status="completed",
        result=event,
    )

    # This would raise TypeError if the store used json.dumps(handler.result)
    await store.update(handler)

    rows = await store.query(HandlerQuery(handler_id_in=["pydantic-result"]))
    assert len(rows) == 1
    found = rows[0]
    assert found.handler_id == "pydantic-result"

    # The row's result should deserialize to a StopEvent
    assert found.result == event


class _MemoCounterState(BaseModel):
    count: int = 0


@pytest.mark.asyncio
async def test_create_state_store_memoizes_per_run(tmp_path: Path) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    first = store.create_state_store("run-1")
    second = store.create_state_store("run-1")
    other = store.create_state_store("run-2")

    assert first is second
    assert other is not first


@pytest.mark.asyncio
async def test_create_state_store_upgrades_default_state_type(tmp_path: Path) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    # A type-less caller (e.g. handler continuation) comes first...
    first = store.create_state_store("run-1")
    assert first.state_type is DictState

    # ...and must not shadow the workflow's concrete state type.
    second = store.create_state_store("run-1", state_type=_MemoCounterState)

    assert second is first
    assert first.state_type is _MemoCounterState


@pytest.mark.asyncio
async def test_create_state_store_restore_reuses_cached_facade(tmp_path: Path) -> None:
    """A restore call must seed the already-handed-out facade, not rebuild it."""
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)
    serializer = JsonSerializer()

    first = store.create_state_store("run-1")
    seed = InMemoryStateStore(DictState(token="restored"))

    second = store.create_state_store(
        "run-1",
        serialized_state=seed.to_dict(serializer),
        serializer=serializer,
    )

    assert second is first
    assert await asyncio.wait_for(first.get("token"), timeout=2.0) == "restored"


@pytest.mark.asyncio
async def test_create_state_store_concurrent_writers_share_lock(
    tmp_path: Path,
) -> None:
    db_path: str = str(tmp_path / "handlers.db")
    store = SqliteWorkflowStore(db_path)

    first = store.create_state_store("run-1")
    second = store.create_state_store("run-1")
    await first.set("count", 0)

    async def increment(state_store: StateStore[Any]) -> None:
        for _ in range(10):
            async with state_store.edit_state() as state:
                state["count"] = state["count"] + 1

    await asyncio.gather(increment(first), increment(second))

    assert await first.get("count") == 20
