# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Generic, Literal

import asyncpg
from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    DictState,
    create_cleared_state,
    decode_state,
    deserialize_state_from_dict,
    encode_state,
    get_by_path,
    merge_state,
    parse_in_memory_state,
    set_by_path,
)

logger = logging.getLogger(__name__)

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]


class PostgresSerializedState(BaseModel):
    """Serialized state referencing a postgres database row."""

    store_type: Literal["postgres"] = "postgres"
    run_id: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PostgresStateStore(Generic[MODEL_T]):
    """Asyncpg-backed StateStore implementation.

    Every get() reads from the database, every set() writes through.
    No in-memory cache — the database is the source of truth.
    """

    state_type: type[MODEL_T]

    def __init__(
        self,
        pool: asyncpg.Pool,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
        schema: str | None = None,
    ) -> None:
        self._pool = pool
        self._run_id = run_id
        self.state_type = state_type or DictState  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        self._serializer = serializer or JsonSerializer()
        self._schema = schema
        self._pending_seed: tuple[dict[str, Any], BaseSerializer] | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def _table_ref(self) -> str:
        if self._schema:
            return f"{self._schema}.workflow_state"
        return "workflow_state"

    @functools.cached_property
    def _lock(self) -> asyncio.Lock:
        """Lazy lock initialization for Python 3.14+ compatibility."""
        return asyncio.Lock()

    def _deserialize_state(
        self,
        state_json: str,
        state_type_name: str | None,
        state_module: str | None,
    ) -> MODEL_T:
        """Deserialize state from JSON string."""
        return decode_state(state_json, state_type_name, state_module, self._serializer)  # type: ignore[return-value]

    def _create_default_state(self) -> MODEL_T:
        return self.state_type()

    async def _write_in_memory_state(self, serialized_state: dict[str, Any]) -> None:
        """Migrate InMemory-format state into the database."""
        state = deserialize_state_from_dict(serialized_state, self._serializer)
        await self._save_state(state)  # type: ignore[arg-type]

    async def _flush_pending_seed(self) -> None:
        """Flush pending seed data to the database if present."""
        if self._pending_seed is None:
            return
        serialized_state, serializer = self._pending_seed
        self._pending_seed = None
        self._serializer = serializer
        store_type = serialized_state.get("store_type")
        if store_type == "postgres":
            source_run_id = serialized_state.get("run_id")
            if source_run_id and source_run_id != self._run_id:
                await self._copy_state_from_run(source_run_id)
        else:
            await self._write_in_memory_state(serialized_state)

    async def _copy_state_from_run(self, source_run_id: str) -> None:
        """Copy state from another run_id using SQL INSERT...SELECT."""
        async with self._pool.acquire() as conn:
            now = _utc_now()
            await conn.execute(
                f"""
                INSERT INTO {self._table_ref} (run_id, state_json, state_type, state_module, created_at, updated_at)
                SELECT $1, state_json, state_type, state_module, $2, $3
                FROM {self._table_ref} WHERE run_id = $4
                ON CONFLICT(run_id) DO UPDATE SET
                    state_json = EXCLUDED.state_json,
                    state_type = EXCLUDED.state_type,
                    state_module = EXCLUDED.state_module,
                    updated_at = EXCLUDED.updated_at
                """,
                self._run_id,
                now,
                now,
                source_run_id,
            )

    async def _load_state(
        self,
        conn: asyncpg.Connection | None = None,
    ) -> MODEL_T:
        """Load state from database. Creates default if row doesn't exist."""
        await self._flush_pending_seed()
        should_release = conn is None
        if conn is None:
            conn = await self._pool.acquire()  # type: ignore[assignment]
        try:
            row = await conn.fetchrow(  # type: ignore[union-attr]
                f"SELECT state_json, state_type, state_module FROM {self._table_ref} WHERE run_id = $1",
                self._run_id,
            )
            if row is None:
                state = self._create_default_state()
                await self._save_state(state, conn)
                return state
            return self._deserialize_state(
                row["state_json"], row["state_type"], row["state_module"]
            )
        finally:
            if should_release:
                await self._pool.release(conn)  # type: ignore[arg-type]

    async def _save_state(
        self,
        state: MODEL_T,
        conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy | None = None,
    ) -> None:
        """Save state to database via upsert."""
        should_release = conn is None
        if conn is None:
            conn = await self._pool.acquire()  # type: ignore[assignment]
        try:
            now = _utc_now()
            state_data, state_type_name, state_module = encode_state(
                state, self._serializer
            )
            state_json = (
                state_data if isinstance(state_data, str) else json.dumps(state_data)
            )
            await conn.execute(  # type: ignore[union-attr]
                f"""
                INSERT INTO {self._table_ref} (run_id, state_json, state_type, state_module, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT(run_id) DO UPDATE SET
                    state_json = EXCLUDED.state_json,
                    state_type = EXCLUDED.state_type,
                    state_module = EXCLUDED.state_module,
                    updated_at = EXCLUDED.updated_at
                """,
                self._run_id,
                state_json,
                state_type_name,
                state_module,
                now,
                now,
            )
        finally:
            if should_release:
                await self._pool.release(conn)  # type: ignore[arg-type]

    async def get_state(self) -> MODEL_T:
        """Return a copy of the current state model."""
        state = await self._load_state()
        return state.model_copy()

    async def set_state(self, state: MODEL_T) -> None:
        """Replace or merge into the current state model."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT state_json, state_type, state_module FROM {self._table_ref} WHERE run_id = $1",
                self._run_id,
            )
            if row is None:
                await self._save_state(state, conn)
                return

            current_state = self._deserialize_state(
                row["state_json"], row["state_type"], row["state_module"]
            )
            merged = merge_state(current_state, state)
            await self._save_state(merged, conn)  # type: ignore[arg-type]

    async def get(self, path: str, default: Any = ...) -> Any:
        """Get a nested value using dot-separated paths."""
        state = await self._load_state()
        return get_by_path(state, path, default)

    async def set(self, path: str, value: Any) -> None:
        """Set a nested value using dot-separated paths."""
        async with self.edit_state() as state:
            set_by_path(state, path, value)

    async def clear(self) -> None:
        """Reset the state to its type defaults."""
        await self.set_state(create_cleared_state(self.state_type))

    @asynccontextmanager
    async def edit_state(self) -> AsyncGenerator[MODEL_T, None]:
        """Edit state transactionally under a lock."""
        async with self._lock:
            state = await self._load_state()
            yield state
            await self._save_state(state)

    def to_dict(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize state store metadata for persistence.

        Returns metadata only — actual state lives in the database.
        """
        payload = PostgresSerializedState(run_id=self._run_id)
        return payload.model_dump()

    @classmethod
    def from_dict(
        cls,
        serialized_state: dict[str, Any],
        serializer: BaseSerializer,
        pool: asyncpg.Pool | None = None,
        state_type: type[BaseModel] | None = None,
        run_id: str | None = None,
        schema: str | None = None,
    ) -> PostgresStateStore[Any]:
        """Restore a state store from serialized payload.

        Handles both InMemorySerializedState (migrates data to DB on first use)
        and PostgresSerializedState (reconnects to existing row).
        """
        if not serialized_state:
            raise ValueError("Cannot restore PostgresStateStore from empty dict")
        if pool is None:
            raise ValueError("pool is required for PostgresStateStore.from_dict()")

        store_type = serialized_state.get("store_type")

        if store_type == "postgres":
            parsed = PostgresSerializedState.model_validate(serialized_state)
            effective_run_id = run_id or parsed.run_id
            return cls(
                pool=pool,
                run_id=effective_run_id,
                state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
                serializer=serializer,
                schema=schema,
            )

        # InMemory format — will need async migration
        parse_in_memory_state(serialized_state)

        effective_run_id = run_id or str(uuid.uuid4())
        store = cls(
            pool=pool,
            run_id=effective_run_id,
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            serializer=serializer,
            schema=schema,
        )
        # Note: caller must await store._write_in_memory_state(serialized_state)
        # since from_dict is synchronous but migration requires async DB access
        return store
