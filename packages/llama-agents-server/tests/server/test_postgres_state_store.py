# ty: ignore[invalid-argument-type]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from typing import AsyncGenerator

import asyncpg
import pytest
from llama_agents.server._store.postgres_state_store import (
    PostgresStateStore,
)
from pydantic import BaseModel
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import DictState, InMemoryStateStore
from workflows.context.state_store_integration import state_store_handoff

SCHEMA = "test_pg_state"


class CounterState(BaseModel):
    count: int = 0
    label: str = "default"


class ExtendedCounterState(CounterState):
    extra: str = "extra_default"


@pytest.fixture
async def pool(postgres_dsn: str) -> AsyncGenerator[asyncpg.Pool, None]:
    p = await asyncpg.create_pool(postgres_dsn, min_size=1, max_size=5)
    async with p.acquire() as conn:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.workflow_state (
                run_id VARCHAR(255) PRIMARY KEY,
                state_json TEXT NOT NULL,
                state_type VARCHAR(255),
                state_module VARCHAR(255),
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            )
        """)
        await conn.execute(f"DELETE FROM {SCHEMA}.workflow_state")
    yield p
    await p.close()


@pytest.mark.docker
async def test_get_returns_default_dict_state(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-1", schema=SCHEMA
    )
    state = await store.get_state()
    assert isinstance(state, DictState)
    assert dict(state) == {}


@pytest.mark.docker
async def test_set_and_get_path(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-path", schema=SCHEMA
    )
    await store.set("foo", 42)
    value = await store.get("foo")
    assert value == 42


@pytest.mark.docker
async def test_set_nested_path(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-nested", schema=SCHEMA
    )
    await store.set("a.b.c", "deep")
    value = await store.get("a.b.c")
    assert value == "deep"


@pytest.mark.docker
async def test_get_missing_path_raises(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-missing", schema=SCHEMA
    )
    with pytest.raises(ValueError, match="not found"):
        await store.get("nonexistent")


@pytest.mark.docker
async def test_get_missing_path_returns_default(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-default", schema=SCHEMA
    )
    value = await store.get("nonexistent", default="fallback")
    assert value == "fallback"


@pytest.mark.docker
async def test_set_state_replaces_dict_state(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-replace", schema=SCHEMA
    )
    await store.set("x", 1)
    new_state = DictState(y=2)
    await store.set_state(new_state)
    state = await store.get_state()
    assert "y" in state
    assert "x" not in state


@pytest.mark.docker
async def test_typed_state_get_returns_default(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[CounterState] = PostgresStateStore(
        pool=pool, run_id="run-typed", state_type=CounterState, schema=SCHEMA
    )
    state = await store.get_state()
    assert isinstance(state, CounterState)
    assert state.count == 0
    assert state.label == "default"


@pytest.mark.docker
async def test_typed_state_set_and_get(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[CounterState] = PostgresStateStore(
        pool=pool, run_id="run-typed-set", state_type=CounterState, schema=SCHEMA
    )
    await store.set_state(CounterState(count=5, label="updated"))
    state = await store.get_state()
    assert state.count == 5
    assert state.label == "updated"


@pytest.mark.docker
async def test_set_state_parent_type_merge(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[ExtendedCounterState] = PostgresStateStore(
        pool=pool, run_id="run-merge", state_type=ExtendedCounterState, schema=SCHEMA
    )
    await store.set_state(ExtendedCounterState(count=1, label="init", extra="mine"))
    parent = CounterState(count=10, label="merged")
    await store.set_state(parent)  # type: ignore[arg-type]
    state = await store.get_state()
    assert state.count == 10
    assert state.label == "merged"
    assert state.extra == "mine"


@pytest.mark.docker
async def test_edit_state_dict(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-edit", schema=SCHEMA
    )
    await store.set("counter", 0)
    async with store.edit_state() as state:
        state["counter"] = state["counter"] + 1
    value = await store.get("counter")
    assert value == 1


@pytest.mark.docker
async def test_edit_state_typed(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[CounterState] = PostgresStateStore(
        pool=pool, run_id="run-edit-typed", state_type=CounterState, schema=SCHEMA
    )
    async with store.edit_state() as state:
        state.count += 10
    result = await store.get_state()
    assert result.count == 10


@pytest.mark.docker
async def test_clear_resets_state(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-clear", schema=SCHEMA
    )
    await store.set("x", 99)
    await store.clear()
    state = await store.get_state()
    assert dict(state) == {}


@pytest.mark.docker
async def test_clear_resets_typed_state(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[CounterState] = PostgresStateStore(
        pool=pool, run_id="run-clear-typed", state_type=CounterState, schema=SCHEMA
    )
    await store.set_state(CounterState(count=100, label="dirty"))
    await store.clear()
    state = await store.get_state()
    assert state.count == 0
    assert state.label == "default"


@pytest.mark.docker
async def test_different_run_ids_are_isolated(pool: asyncpg.Pool) -> None:
    store_a: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-a", schema=SCHEMA
    )
    store_b: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-b", schema=SCHEMA
    )
    await store_a.set("x", "from-a")
    await store_b.set("x", "from-b")
    assert await store_a.get("x") == "from-a"
    assert await store_b.get("x") == "from-b"


@pytest.mark.docker
async def test_to_dict_returns_metadata_only(pool: asyncpg.Pool) -> None:
    store: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-todict", schema=SCHEMA
    )
    await store.set("key", "value")
    serializer = JsonSerializer()
    d = store.to_dict(serializer)
    assert d["store_type"] == "postgres"
    assert d["run_id"] == "run-todict"
    assert "state_data" not in d


@pytest.mark.docker
async def test_from_dict_postgres_format(pool: asyncpg.Pool) -> None:
    store1: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-fromdict", schema=SCHEMA
    )
    await store1.set("saved", True)

    serializer = JsonSerializer()
    payload = store1.to_dict(serializer)

    store2 = PostgresStateStore.from_dict(
        payload, serializer, pool=pool, state_type=DictState, schema=SCHEMA
    )
    value = await store2.get("saved")
    assert value is True


@pytest.mark.docker
async def test_from_dict_postgres_format_with_new_run_copies_state(
    pool: asyncpg.Pool,
) -> None:
    store1: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-fromdict-source", schema=SCHEMA
    )
    await store1.set("saved", True)

    serializer = JsonSerializer()
    payload = store1.to_dict(serializer)

    store2 = PostgresStateStore.from_dict(
        payload,
        serializer,
        pool=pool,
        state_type=DictState,
        run_id="run-fromdict-target",
        schema=SCHEMA,
    )
    value = await store2.get("saved")
    assert value is True


@pytest.mark.docker
async def test_handoff_materializes_new_run_copy(pool: asyncpg.Pool) -> None:
    store1: PostgresStateStore[DictState] = PostgresStateStore(
        pool=pool, run_id="run-handoff-source", schema=SCHEMA
    )
    await store1.set("saved", True)

    serializer = JsonSerializer()
    store2 = PostgresStateStore.from_dict(
        store1.to_dict(serializer),
        serializer,
        pool=pool,
        state_type=DictState,
        run_id="run-handoff-target",
        schema=SCHEMA,
    )

    payload = await state_store_handoff(store2, serializer)

    assert payload["store_type"] == "postgres"
    assert payload["run_id"] == "run-handoff-target"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT state_json FROM {SCHEMA}.workflow_state WHERE run_id = $1",
            "run-handoff-target",
        )
    assert row is not None
    assert '"saved": "true"' in row["state_json"]


@pytest.mark.docker
async def test_from_dict_in_memory_format_migrates(pool: asyncpg.Pool) -> None:
    serializer = JsonSerializer()
    in_memory_store = InMemoryStateStore(DictState(migrated_key="migrated_value"))
    payload = in_memory_store.to_dict(serializer)

    store = PostgresStateStore.from_dict(
        payload,
        serializer,
        pool=pool,
        state_type=DictState,
        run_id="run-migrate",
        schema=SCHEMA,
    )
    value = await store.get("migrated_key")
    assert value == "migrated_value"


@pytest.mark.docker
async def test_from_dict_rejects_wrong_provider_handle(pool: asyncpg.Pool) -> None:
    with pytest.raises(ValueError, match="store_type 'agent_data'"):
        PostgresStateStore.from_dict(
            {"store_type": "agent_data", "run_id": "run-1"},
            JsonSerializer(),
            pool=pool,
            schema=SCHEMA,
        )


async def test_from_dict_empty_raises() -> None:
    with pytest.raises(ValueError, match="Cannot restore"):
        PostgresStateStore.from_dict({}, JsonSerializer())


async def test_from_dict_no_pool_raises() -> None:
    with pytest.raises(ValueError, match="pool is required"):
        PostgresStateStore.from_dict(
            {"store_type": "postgres", "run_id": "x"}, JsonSerializer()
        )
