# ty: ignore[invalid-argument-type]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from llama_agents.server import HandlerQuery, SqliteWorkflowStore
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


@pytest.mark.asyncio
async def test_clear_resets_subclass_fields(db_path: str) -> None:
    """Clear resets to the stored state's type; child-only fields don't survive."""
    store: SqliteStateStore[CounterState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-clear-subclass",
        state_type=CounterState,
    )
    await store.set_state(ExtendedCounterState(count=1, extra="dirty"))

    await store.clear()

    state = await store.get_state()
    assert isinstance(state, ExtendedCounterState)
    assert state.count == 0
    assert state.extra == "extra_default"


@pytest.mark.asyncio
async def test_typeless_clear_over_typed_row(db_path: str) -> None:
    """A DictState-typed facade over a row holding a typed model can still clear."""
    typed: SqliteStateStore[CounterState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-typeless-clear",
        state_type=CounterState,
    )
    await typed.set_state(CounterState(count=9, label="dirty"))

    typeless: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path,
        run_id="run-typeless-clear",
    )
    await typeless.clear()

    state = await typed.get_state()
    assert state == CounterState()


@pytest.mark.asyncio
async def test_get_inside_edit_state_does_not_deadlock(
    store: SqliteStateStore[DictState],
) -> None:
    """`get` must work inside an `edit_state` block on a durable backend."""
    await store.set("counter", 1)

    async def nested_read() -> int:
        async with store.edit_state() as state:
            state["other"] = 2
            return int(await store.get("counter"))

    assert await asyncio.wait_for(nested_read(), timeout=2.0) == 1


# -- Writer-scoped connection sessions --


@pytest.mark.asyncio
async def test_set_state_opens_exactly_one_connection(
    store: SqliteStateStore[DictState], monkeypatch: pytest.MonkeyPatch
) -> None:
    """In default multi-connection mode, a write's load+save share one connection."""
    await store.set("x", 1)  # materialize the row before counting

    connect_count = 0
    real_connect = sqlite3.connect

    def counting_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        nonlocal connect_count
        connect_count += 1
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", counting_connect)

    await store.set_state(DictState(y=2))
    assert connect_count == 1

    connect_count = 0
    await store.clear()
    assert connect_count == 1


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
async def test_typed_state_decodes_when_store_type_changes(
    db_path: str,
) -> None:
    """The self-describing payload drives decode even if the reader's declared type differs."""
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
async def test_from_dict_sqlite_format_with_new_run_copies_state(
    db_path: str,
) -> None:
    store1: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-fromdict-source"
    )
    await store1.set("saved", True)

    serializer = JsonSerializer()
    payload = store1.to_dict(serializer)

    store2 = SqliteStateStore.from_dict(
        payload,
        serializer,
        db_path=db_path,
        state_type=DictState,
        run_id="run-fromdict-target",
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


@pytest.mark.asyncio
async def test_sqlite_workflow_store_single_connection_keeps_state_connection_open(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "single-connection.db")
    workflow_store = SqliteWorkflowStore(db_path, single_connection=True)
    state_store = workflow_store.create_state_store("run-single")

    await state_store.set("x", 1)
    assert await state_store.get("x") == 1

    await workflow_store.query(HandlerQuery())


@pytest.mark.asyncio
async def test_sqlite_workflow_store_single_connection_opens_existing_regular_db(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "existing-regular.db")
    conn = sqlite3.connect(db_path)
    try:
        run_migrations(conn)
        conn.commit()
    finally:
        conn.close()

    workflow_store = SqliteWorkflowStore(db_path, single_connection=True)
    state_store = workflow_store.create_state_store("run-existing-single")

    await state_store.set("x", 1)
    assert await state_store.get("x") == 1


# -- Per-namespace records --


@pytest.mark.asyncio
async def test_namespace_round_trip_and_isolation(db_path: str) -> None:
    """Root and child namespaces persist as independent rows under one run."""
    root: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-ns"
    )
    child: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-ns", namespace=("child",)
    )

    await root.set("k", "root-val")
    await child.set("k", "child-val")

    # Fresh facades read straight from their own row — no cross-namespace bleed.
    root2: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-ns"
    )
    child2: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=db_path, run_id="run-ns", namespace=("child",)
    )
    assert await root2.get("k") == "root-val"
    assert await child2.get("k") == "child-val"


@pytest.mark.asyncio
async def test_pre_migration_root_row_reads_as_root(tmp_path: Path) -> None:
    """A row written under the pre-namespace schema reads back as root."""
    path = str(tmp_path / "legacy.db")
    serializer = JsonSerializer()
    legacy_state_json = json.dumps({"_data": {"k": serializer.serialize("legacy")}})

    conn = sqlite3.connect(path)
    try:
        # Recreate the schema as it stood at migration 4 (single-column PK,
        # no namespace) and mark migrations 1-4 applied.
        conn.executescript(
            """
            CREATE TABLE schema_migrations (
                package TEXT NOT NULL,
                version INTEGER NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (package, version)
            );
            CREATE TABLE workflow_state (
                run_id TEXT PRIMARY KEY,
                state_json TEXT NOT NULL DEFAULT '{}',
                state_type TEXT NOT NULL DEFAULT 'DictState',
                state_module TEXT NOT NULL DEFAULT 'workflows.context.state_store',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        for version in range(1, 5):
            conn.execute(
                "INSERT INTO schema_migrations (package, version) VALUES ('server', ?)",
                (version,),
            )
        conn.execute(
            "INSERT INTO workflow_state "
            "(run_id, state_json, state_type, state_module, created_at, updated_at) "
            "VALUES (?, ?, 'DictState', 'workflows.context.state_store', '', '')",
            ("legacy-run", legacy_state_json),
        )
        conn.commit()
        # Apply the pending namespace migration (table rebuild).
        run_migrations(conn)
        conn.commit()
    finally:
        conn.close()

    store: SqliteStateStore[DictState] = SqliteStateStore(
        db_path=path, run_id="legacy-run"
    )
    assert await store.get("k") == "legacy"
