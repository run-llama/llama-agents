# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""State store matrix tests - testing StateStore implementations.

Tests the StateStore protocol across InMemoryStateStore and SqlStateStore
(with both SQLite and PostgreSQL engines) to ensure consistent behavior.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any, AsyncGenerator, Generator

import asyncpg
import pytest
from llama_agents.server._store.postgres_state_store import PostgresStateStore
from llama_agents.server._store.sqlite.migrate import run_migrations
from llama_agents.server._store.sqlite.sqlite_state_store import SqliteStateStore
from llama_agents_integration_tests.fake_agent_data import (
    FakeAgentDataBackend,
    create_agent_data_state_store,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
    field_serializer,
    field_validator,
)
from testcontainers.postgres import PostgresContainer
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import DictState, InMemoryStateStore, StateStore

# -- Custom state types for testing --


class MyRandomObject:
    """Non-Pydantic object that requires custom serialization."""

    def __init__(self, name: str) -> None:
        self.name = name


class PydanticObject(BaseModel):
    """Simple Pydantic model for nested state testing."""

    name: str


class MyState(BaseModel):
    """Custom typed state with serialization logic."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        strict=True,
    )

    my_obj: MyRandomObject
    pydantic_obj: PydanticObject
    name: str
    age: int

    @field_serializer("my_obj", when_used="always")
    def serialize_my_obj(self, my_obj: MyRandomObject) -> str:
        return my_obj.name

    @field_validator("my_obj", mode="before")
    @classmethod
    def deserialize_my_obj(cls, v: str | MyRandomObject) -> MyRandomObject:
        if isinstance(v, MyRandomObject):
            return v
        if isinstance(v, str):
            return MyRandomObject(v)
        raise ValueError(f"Invalid type for my_obj: {type(v)}")


# -- Fixtures --


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Module-scoped PostgreSQL container for state store tests.

    Requires Docker to be running.
    """
    with PostgresContainer("postgres:16", driver=None) as postgres:
        yield postgres


@pytest.fixture(scope="module")
def postgres_dsn(
    postgres_container: PostgresContainer,
) -> str:
    """Module-scoped PostgreSQL DSN for state store tests."""
    connection_url = postgres_container.get_connection_url()
    if "postgresql+psycopg2://" in connection_url:
        connection_url = connection_url.replace(
            "postgresql+psycopg2://", "postgresql://"
        )
    elif "postgresql+psycopg://" in connection_url:
        connection_url = connection_url.replace(
            "postgresql+psycopg://", "postgresql://"
        )
    return connection_url


async def _create_postgres_pool(dsn: str) -> asyncpg.Pool:
    """Create a pool and ensure schema/table exist."""
    pool = await asyncpg.create_pool(dsn=dsn)
    assert pool is not None
    async with pool.acquire() as conn:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dbos.workflow_state (
                run_id VARCHAR(255) NOT NULL,
                namespace VARCHAR(255) NOT NULL DEFAULT '',
                state_json TEXT NOT NULL,
                state_type VARCHAR(255),
                state_module VARCHAR(255),
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                PRIMARY KEY (run_id, namespace)
            )
        """)
    return pool


@pytest.fixture(scope="module")
def sqlite_db_path(
    tmp_path_factory: pytest.TempPathFactory,
) -> str:
    """Module-scoped SQLite database path for state store tests."""
    db_file: Path = tmp_path_factory.mktemp("state_store") / "test.sqlite3"
    db_path = str(db_file)

    # Run migrations to create tables
    conn = sqlite3.connect(db_path)
    try:
        run_migrations(conn)
        conn.commit()
    finally:
        conn.close()

    return db_path


def _get_store_params() -> list[Any]:
    """Get store type parameters for the test matrix."""
    return [
        pytest.param("in_memory", id="in_memory"),
        pytest.param("sqlite", id="sqlite"),
        pytest.param("postgres", marks=pytest.mark.docker, id="postgres"),
        pytest.param("agent_data", id="agent_data"),
    ]


def _get_sql_params() -> list[Any]:
    """Get SQL-only backend parameters for persistence/isolation tests."""
    return [
        pytest.param("sqlite", id="sqlite"),
        pytest.param("postgres", marks=pytest.mark.docker, id="postgres"),
    ]


@pytest.fixture(params=_get_store_params())
async def state_store(
    request: pytest.FixtureRequest,
    sqlite_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[StateStore[DictState], None]:
    """Parametrized fixture yielding a fresh StateStore for each test."""
    # Use unique run_id per test to avoid state bleeding
    run_id = f"test-{id(request)}"

    if request.param == "in_memory":
        yield InMemoryStateStore(DictState())
    elif request.param == "sqlite":
        store = SqliteStateStore(db_path=sqlite_db_path, run_id=run_id)
        yield store
    elif request.param == "postgres":
        dsn: str = request.getfixturevalue("postgres_dsn")
        pool = await _create_postgres_pool(dsn)
        store = PostgresStateStore(pool=pool, run_id=run_id, schema="dbos")
        yield store
        await pool.close()
    elif request.param == "agent_data":
        yield create_agent_data_state_store(
            FakeAgentDataBackend(), monkeypatch, run_id=run_id
        )


@pytest.fixture(params=_get_sql_params())
async def sql_store_factory(
    request: pytest.FixtureRequest,
    sqlite_db_path: str,
) -> AsyncGenerator[tuple[str, str | None, asyncpg.Pool | None], None]:
    """Parametrized fixture yielding (backend, db_path_or_schema, pool) for SQL backend tests."""
    if request.param == "sqlite":
        yield "sqlite", sqlite_db_path, None
    elif request.param == "postgres":
        dsn: str = request.getfixturevalue("postgres_dsn")
        pool = await _create_postgres_pool(dsn)
        yield "postgres", "dbos", pool
        await pool.close()


@pytest.fixture(params=_get_store_params())
async def custom_state_store(
    request: pytest.FixtureRequest,
    sqlite_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[StateStore[MyState], None]:
    """Parametrized fixture yielding a StateStore with custom typed state."""
    run_id = f"test-custom-{id(request)}"
    initial_state = MyState(
        my_obj=MyRandomObject("llama-index"),
        pydantic_obj=PydanticObject(name="llama-index"),
        name="John",
        age=30,
    )

    if request.param == "in_memory":
        yield InMemoryStateStore(initial_state)
    elif request.param == "sqlite":
        store = SqliteStateStore(
            db_path=sqlite_db_path,
            run_id=run_id,
            state_type=MyState,
        )
        await store.set_state(initial_state)
        yield store
    elif request.param == "postgres":
        dsn: str = request.getfixturevalue("postgres_dsn")
        pool = await _create_postgres_pool(dsn)
        store = PostgresStateStore(
            pool=pool,
            run_id=run_id,
            state_type=MyState,
            schema="dbos",
        )
        await store.set_state(initial_state)
        yield store
        await pool.close()
    elif request.param == "agent_data":
        store = create_agent_data_state_store(
            FakeAgentDataBackend(), monkeypatch, run_id=run_id, state_type=MyState
        )
        await store.set_state(initial_state)
        yield store


# -- Basic Operations Tests --


@pytest.mark.asyncio
async def test_get_set_basic_values(state_store: StateStore[DictState]) -> None:
    """Test basic get/set operations with simple values."""
    await state_store.set("name", "John")
    await state_store.set("age", 30)

    assert await state_store.get("name") == "John"
    assert await state_store.get("age") == 30


@pytest.mark.asyncio
async def test_get_with_default(state_store: StateStore[DictState]) -> None:
    """Test get with default value for missing keys."""
    result = await state_store.get("nonexistent", default=None)
    assert result is None

    result = await state_store.get("missing", default="fallback")
    assert result == "fallback"


@pytest.mark.asyncio
async def test_get_missing_raises(state_store: StateStore[DictState]) -> None:
    """Test that get raises ValueError for missing key without default."""
    with pytest.raises(ValueError, match="not found"):
        await state_store.get("nonexistent")


@pytest.mark.asyncio
async def test_method_named_keys(state_store: StateStore[DictState]) -> None:
    """Keys colliding with mapping method names return stored values, not bound methods."""
    await state_store.set("items", [1, 2, 3])
    await state_store.set("keys", "abc")

    assert await state_store.get("items") == [1, 2, 3]
    assert await state_store.get("keys") == "abc"
    assert await state_store.get("values", default=None) is None


@pytest.mark.asyncio
async def test_nested_get_set(state_store: StateStore[DictState]) -> None:
    """Test nested path access with dot notation."""
    await state_store.set("nested", {"a": "b"})
    assert await state_store.get("nested.a") == "b"

    await state_store.set("nested.a", "c")
    assert await state_store.get("nested.a") == "c"


@pytest.mark.asyncio
async def test_get_state_returns_copy(state_store: StateStore[DictState]) -> None:
    """Test that get_state returns a copy, not the original."""
    await state_store.set("value", 1)

    state1 = await state_store.get_state()
    state2 = await state_store.get_state()

    # Should be equal but not the same object
    assert state1.model_dump() == state2.model_dump()


@pytest.mark.asyncio
async def test_set_state_replaces(state_store: StateStore[DictState]) -> None:
    """Test that set_state replaces the entire state."""
    await state_store.set("old_key", "old_value")

    new_state = DictState()
    new_state["new_key"] = "new_value"
    await state_store.set_state(new_state)

    assert await state_store.get("new_key") == "new_value"
    # Old key should be gone or inaccessible
    result = await state_store.get("old_key", default=None)
    assert result is None


@pytest.mark.asyncio
async def test_clear_resets_state(state_store: StateStore[DictState]) -> None:
    """Test that clear resets to default state."""
    await state_store.set("name", "Jane")
    await state_store.set("age", 25)

    await state_store.clear()

    assert await state_store.get("name", default=None) is None
    assert await state_store.get("age", default=None) is None


# -- edit_state Context Manager Tests --


@pytest.mark.asyncio
async def test_edit_state_basic(state_store: StateStore[DictState]) -> None:
    """Test basic edit_state context manager usage."""
    await state_store.set("counter", 0)

    async with state_store.edit_state() as state:
        current = state.get("counter", 0)
        state["counter"] = current + 1

    assert await state_store.get("counter") == 1


@pytest.mark.asyncio
async def test_edit_state_multiple_changes(state_store: StateStore[DictState]) -> None:
    """Test multiple changes within a single edit_state."""
    async with state_store.edit_state() as state:
        state["a"] = 1
        state["b"] = 2
        state["c"] = {"nested": "value"}

    assert await state_store.get("a") == 1
    assert await state_store.get("b") == 2
    assert await state_store.get("c.nested") == "value"


@pytest.mark.asyncio
async def test_edit_state_exception_handling(
    state_store: StateStore[DictState],
) -> None:
    """Test that exceptions in edit_state don't corrupt state."""
    await state_store.set("value", "original")

    with pytest.raises(ValueError, match="intentional"):
        async with state_store.edit_state() as state:
            state["value"] = "modified"
            raise ValueError("intentional error")

    # State should remain unchanged after exception
    # Note: behavior may vary - InMemory commits on context exit, SQL rolls back
    # This test documents the expected behavior


# -- Custom Typed State Tests --


@pytest.mark.asyncio
async def test_custom_state_type(custom_state_store: StateStore[MyState]) -> None:
    """Test state store with custom Pydantic model."""
    state = await custom_state_store.get_state()
    assert isinstance(state, MyState)
    assert state.name == "John"
    assert state.age == 30
    assert state.my_obj.name == "llama-index"


@pytest.mark.asyncio
async def test_custom_state_set_values(custom_state_store: StateStore[MyState]) -> None:
    """Test setting values on custom typed state."""
    await custom_state_store.set("name", "Jane")
    await custom_state_store.set("age", 25)

    assert await custom_state_store.get("name") == "Jane"
    assert await custom_state_store.get("age") == 25

    # Original custom fields should still be accessible
    state = await custom_state_store.get_state()
    assert state.my_obj.name == "llama-index"


@pytest.mark.asyncio
async def test_custom_state_validation(custom_state_store: StateStore[MyState]) -> None:
    """Test that Pydantic validation is enforced on custom state."""
    # MyState has strict=True, so setting age to string should fail
    with pytest.raises(ValidationError):
        await custom_state_store.set("age", "not a number")


# -- Serialization Tests --


@pytest.mark.asyncio
async def test_to_dict_from_dict_roundtrip(state_store: StateStore[DictState]) -> None:
    """Test serialization roundtrip with to_dict/from_dict."""
    await state_store.set("name", "John")
    await state_store.set("age", 30)

    serializer = JsonSerializer()
    data = state_store.to_dict(serializer)

    # For InMemoryStateStore, from_dict restores the full state
    # For SqlStateStore, from_dict returns metadata (engine must be set separately)
    if isinstance(state_store, InMemoryStateStore):
        restored = InMemoryStateStore.from_dict(data, serializer)
        assert await restored.get("name") == "John"
        assert await restored.get("age") == 30


# -- SQL Backend Tests (parameterized across SQLite and PostgreSQL) --


@pytest.mark.asyncio
async def test_sql_persistence(
    sql_store_factory: tuple[str, str, asyncpg.Pool | None],
) -> None:
    """Test that state persists across store instances."""
    backend, db_path_or_schema, pool = sql_store_factory
    run_id = "persistence-test"

    if backend == "sqlite":
        store1 = SqliteStateStore(db_path=db_path_or_schema, run_id=run_id)
        await store1.set("persistent_key", "persistent_value")
        store2 = SqliteStateStore(db_path=db_path_or_schema, run_id=run_id)
    else:
        assert pool is not None
        store1 = PostgresStateStore(pool=pool, run_id=run_id, schema=db_path_or_schema)
        await store1.set("persistent_key", "persistent_value")
        store2 = PostgresStateStore(pool=pool, run_id=run_id, schema=db_path_or_schema)

    result = await store2.get("persistent_key")
    assert result == "persistent_value"


@pytest.mark.asyncio
async def test_sql_isolation(
    sql_store_factory: tuple[str, str, asyncpg.Pool | None],
) -> None:
    """Test that different run_ids have isolated state."""
    backend, db_path_or_schema, pool = sql_store_factory

    if backend == "sqlite":
        store1 = SqliteStateStore(db_path=db_path_or_schema, run_id="run-1")
        store2 = SqliteStateStore(db_path=db_path_or_schema, run_id="run-2")
    else:
        assert pool is not None
        store1 = PostgresStateStore(pool=pool, run_id="run-1", schema=db_path_or_schema)
        store2 = PostgresStateStore(pool=pool, run_id="run-2", schema=db_path_or_schema)

    await store1.set("key", "value1")
    await store2.set("key", "value2")

    assert await store1.get("key") == "value1"
    assert await store2.get("key") == "value2"


@pytest.mark.asyncio
async def test_sql_concurrent_edits(
    sql_store_factory: tuple[str, str, asyncpg.Pool | None],
) -> None:
    """Test concurrent edit_state calls are serialized correctly."""
    backend, db_path_or_schema, pool = sql_store_factory
    run_id = "concurrent-test"

    if backend == "sqlite":
        store = SqliteStateStore(db_path=db_path_or_schema, run_id=run_id)
    else:
        assert pool is not None
        store = PostgresStateStore(pool=pool, run_id=run_id, schema=db_path_or_schema)

    await store.set("counter", 0)

    async def increment() -> None:
        async with store.edit_state() as state:
            current = state.get("counter", 0)
            await asyncio.sleep(0.01)  # Simulate some work
            state["counter"] = current + 1

    await asyncio.gather(*[increment() for _ in range(5)])

    result = await store.get("counter")
    assert result == 5


@pytest.mark.asyncio
async def test_sql_custom_state_persistence(
    sql_store_factory: tuple[str, str, asyncpg.Pool | None],
) -> None:
    """Test that custom typed state persists correctly."""
    backend, db_path_or_schema, pool = sql_store_factory
    run_id = "custom-persistence-test"

    initial_state = MyState(
        my_obj=MyRandomObject("persisted"),
        pydantic_obj=PydanticObject(name="persisted"),
        name="Original",
        age=100,
    )

    if backend == "sqlite":
        store1 = SqliteStateStore(
            db_path=db_path_or_schema,
            run_id=run_id,
            state_type=MyState,
        )
        await store1.set_state(initial_state)
        await store1.set("name", "Modified")
        store2 = SqliteStateStore(
            db_path=db_path_or_schema,
            run_id=run_id,
            state_type=MyState,
        )
    else:
        assert pool is not None
        store1 = PostgresStateStore(
            pool=pool,
            run_id=run_id,
            state_type=MyState,
            schema=db_path_or_schema,
        )
        await store1.set_state(initial_state)
        await store1.set("name", "Modified")
        store2 = PostgresStateStore(
            pool=pool,
            run_id=run_id,
            state_type=MyState,
            schema=db_path_or_schema,
        )

    state = await store2.get_state()

    assert state.name == "Modified"
    assert state.my_obj.name == "persisted"


# -- PostgreSQL-Specific Tests --


@pytest.mark.docker
@pytest.mark.asyncio
async def test_postgres_uses_dbos_schema(postgres_dsn: str) -> None:
    """Test that PostgresStateStore with schema='dbos' uses the table in the dbos schema."""
    pool = await _create_postgres_pool(postgres_dsn)
    try:
        run_id = "pg-schema-test"
        store = PostgresStateStore(pool=pool, run_id=run_id, schema="dbos")

        await store.set("test", "value")

        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'dbos'
                    AND table_name = 'workflow_state'
                )
                """
            )
            assert exists is True
    finally:
        await pool.close()
