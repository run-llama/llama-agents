# ty: ignore[invalid-argument-type]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from llama_agents.server import SqliteWorkflowStore
from llama_agents.server._store.sqlite.migrate import run_migrations
from llama_agents.server._store.sqlite.sqlite_state_store import (
    SqliteStateStore,
)
from pydantic import BaseModel
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import DictState, InMemoryStateStore

# -- Typed state models for testing --


class CounterState(BaseModel):
    count: int = 0
    label: str = "default"


class ExtendedCounterState(CounterState):
    extra: str = "extra_default"


# -- Fixtures --


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    path = str(tmp_path / "test_state.db")
    conn = sqlite3.connect(path)
    try:
        run_migrations(conn)
        conn.commit()
    finally:
        conn.close()
    return path


@pytest.fixture
def store(db_path: str) -> SqliteStateStore[DictState]:
    return SqliteStateStore(db_path=db_path, run_id="run-1")


@pytest.fixture
def typed_store(db_path: str) -> SqliteStateStore[CounterState]:
    return SqliteStateStore(
        db_path=db_path,
        run_id="run-typed",
        state_type=CounterState,
    )


# -- Basic get/set tests --


@pytest.mark.asyncio
async def test_get_returns_default_dict_state(
    store: SqliteStateStore[DictState],
) -> None:
    state = await store.get_state()
    assert isinstance(state, DictState)
    assert dict(state) == {}


@pytest.mark.asyncio
async def test_set_and_get_path(store: SqliteStateStore[DictState]) -> None:
    await store.set("foo", 42)
    value = await store.get("foo")
    assert value == 42


@pytest.mark.asyncio
async def test_set_nested_path(store: SqliteStateStore[DictState]) -> None:
    await store.set("a.b.c", "deep")
    value = await store.get("a.b.c")
    assert value == "deep"


@pytest.mark.asyncio
async def test_get_missing_path_raises(store: SqliteStateStore[DictState]) -> None:
    with pytest.raises(ValueError, match="not found"):
        await store.get("nonexistent")


@pytest.mark.asyncio
async def test_get_missing_path_returns_default(
    store: SqliteStateStore[DictState],
) -> None:
    value = await store.get("nonexistent", default="fallback")
    assert value == "fallback"


@pytest.mark.asyncio
async def test_set_empty_path_raises(store: SqliteStateStore[DictState]) -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        await store.set("", 42)


# -- get_state / set_state --


@pytest.mark.asyncio
async def test_set_state_replaces_dict_state(
    store: SqliteStateStore[DictState],
) -> None:
    await store.set("x", 1)
    new_state = DictState(y=2)
    await store.set_state(new_state)
    state = await store.get_state()
    assert "y" in state
    assert "x" not in state


@pytest.mark.asyncio
async def test_typed_state_get_returns_default(
    typed_store: SqliteStateStore[CounterState],
) -> None:
    state = await typed_store.get_state()
    assert isinstance(state, CounterState)
    assert state.count == 0
    assert state.label == "default"


@pytest.mark.asyncio
async def test_typed_state_set_and_get(
    typed_store: SqliteStateStore[CounterState],
) -> None:
    await typed_store.set_state(CounterState(count=5, label="updated"))
    state = await typed_store.get_state()
    assert state.count == 5
    assert state.label == "updated"


@pytest.mark.asyncio
async def test_set_state_parent_type_merge(db_path: str) -> None:
    """Setting a parent type state merges fields, preserving child-specific fields."""
    store: SqliteStateStore[ExtendedCounterState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-merge",
        state_type=ExtendedCounterState,
    )
    await store.set_state(ExtendedCounterState(count=1, label="init", extra="mine"))

    # Set parent type — should merge
    parent = CounterState(count=10, label="merged")
    await store.set_state(parent)  # type: ignore[arg-type]

    state = await store.get_state()
    assert state.count == 10
    assert state.label == "merged"
    assert state.extra == "mine"  # child field preserved


# -- edit_state --


@pytest.mark.asyncio
async def test_edit_state_dict(store: SqliteStateStore[DictState]) -> None:
    await store.set("counter", 0)
    async with store.edit_state() as state:
        state["counter"] = state["counter"] + 1
    value = await store.get("counter")
    assert value == 1


@pytest.mark.asyncio
async def test_edit_state_typed(typed_store: SqliteStateStore[CounterState]) -> None:
    async with typed_store.edit_state() as state:
        state.count += 10
    result = await typed_store.get_state()
    assert result.count == 10


# -- clear --


@pytest.mark.asyncio
async def test_clear_resets_state(store: SqliteStateStore[DictState]) -> None:
    await store.set("x", 99)
    await store.clear()
    state = await store.get_state()
    assert dict(state) == {}


@pytest.mark.asyncio
async def test_clear_resets_typed_state(
    typed_store: SqliteStateStore[CounterState],
) -> None:
    await typed_store.set_state(CounterState(count=100, label="dirty"))
    await typed_store.clear()
    state = await typed_store.get_state()
    assert state.count == 0
    assert state.label == "default"


# -- Persistence across instances --


@pytest.mark.asyncio
async def test_state_persists_across_instances(db_path: str) -> None:
    """State set by one store instance is readable by a new instance pointing at the same DB."""
    store1: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-persist"
    )
    await store1.set("key", "value")

    store2: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-persist"
    )
    value = await store2.get("key")
    assert value == "value"


@pytest.mark.asyncio
async def test_typed_state_persists_across_instances(db_path: str) -> None:
    store1: SqliteStateStore[CounterState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-typed-persist",
        state_type=CounterState,
    )
    await store1.set_state(CounterState(count=42, label="persisted"))

    store2: SqliteStateStore[CounterState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-typed-persist",
        state_type=CounterState,
    )
    state = await store2.get_state()
    assert state.count == 42
    assert state.label == "persisted"


@pytest.mark.asyncio
async def test_typed_state_decodes_from_row_metadata_when_store_type_changes(
    db_path: str,
) -> None:
    store1: SqliteStateStore[CounterState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-typed-metadata",
        state_type=CounterState,
    )
    await store1.set_state(CounterState(count=7, label="typed-row"))

    store2: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-typed-metadata",
        state_type=DictState,
    )

    state = await store2.get_state()

    assert isinstance(state, CounterState)
    assert state.count == 7
    assert state.label == "typed-row"


@pytest.mark.asyncio
async def test_different_run_ids_are_isolated(db_path: str) -> None:
    store_a: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-a"
    )
    store_b: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-b"
    )
    await store_a.set("x", "from-a")
    await store_b.set("x", "from-b")

    assert await store_a.get("x") == "from-a"
    assert await store_b.get("x") == "from-b"


# -- to_dict / from_dict --


@pytest.mark.asyncio
async def test_to_dict_returns_metadata_only(
    store: SqliteStateStore[DictState],
) -> None:
    await store.set("key", "value")
    serializer = JsonSerializer()
    d = store.to_dict(serializer)
    assert d["store_type"] == "sqlite"
    assert d["run_id"] == "run-1"
    assert "state_data" not in d


@pytest.mark.asyncio
async def test_from_dict_sqlite_format(db_path: str) -> None:
    """from_dict with sqlite format reconnects to existing row."""
    store1: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-fromdict"
    )
    await store1.set("saved", True)

    serializer = JsonSerializer()
    payload = store1.to_dict(serializer)

    store2 = SqliteStateStore.from_dict(
        payload, serializer, db_path=db_path, state_type=DictState
    )
    value = await store2.get("saved")
    assert value is True


@pytest.mark.asyncio
async def test_from_dict_in_memory_format_migrates(db_path: str) -> None:
    """from_dict with InMemorySerializedState format stores data on first DB access."""
    serializer = JsonSerializer()
    in_memory_store = InMemoryStateStore(DictState(migrated_key="migrated_value"))
    payload = in_memory_store.to_dict(serializer)

    store = SqliteStateStore.from_dict(
        payload,
        serializer,
        db_path=db_path,
        state_type=DictState,
        run_id="run-migrate",
    )
    value = await store.get("migrated_key")
    assert value == "migrated_value"


@pytest.mark.asyncio
async def test_from_dict_empty_raises() -> None:
    with pytest.raises(ValueError, match="Cannot restore"):
        SqliteStateStore.from_dict({}, JsonSerializer())


# -- Migration applies cleanly --


@pytest.mark.asyncio
async def test_migration_applies_on_existing_db(tmp_path: Path) -> None:
    """Verify the state table can be created on a DB that already has other tables."""
    db_path = str(tmp_path / "existing.db")
    # Create DB with workflow store tables first
    SqliteWorkflowStore(db_path)

    # Now create state store on same DB — should work
    store: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-coexist"
    )
    await store.set("coexist", True)
    value = await store.get("coexist")
    assert value is True
