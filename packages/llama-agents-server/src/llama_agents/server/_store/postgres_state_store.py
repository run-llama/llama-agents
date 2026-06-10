# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Generic, Literal

import asyncpg
from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import DictState
from workflows.context.state_store_integration import (
    StateRecord,
    StateStoreFacade,
    decode_seed_state,
    string_record_from_state,
)

logger = logging.getLogger(__name__)

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]


class PostgresSerializedState(BaseModel):
    """Serialized state referencing a postgres database row."""

    store_type: Literal["postgres"] = "postgres"
    run_id: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class _PostgresStateStorage:
    """Asyncpg-backed raw state storage."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        run_id: str,
        schema: str | None = None,
        pending_seed: tuple[dict[str, Any], BaseSerializer] | None = None,
    ) -> None:
        self._pool = pool
        self._run_id = run_id
        self._schema = schema
        self._pending_seed = pending_seed
        self._seed_lock = asyncio.Lock()

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def _table_ref(self) -> str:
        if self._schema:
            return f"{self._schema}.workflow_state"
        return "workflow_state"

    async def ensure_seeded(self) -> None:
        if self._pending_seed is None:
            return
        async with self._seed_lock:
            if self._pending_seed is None:
                return
            serialized_state, serializer = self._pending_seed
            self._pending_seed = None
            store_type = serialized_state.get("store_type")
            if store_type == "postgres":
                source_run_id = serialized_state.get("run_id")
                if source_run_id and source_run_id != self._run_id:
                    await self._copy_state_from_run(source_run_id)
                return
            state = decode_seed_state(serialized_state, serializer)
            await self._save_record(string_record_from_state(state, serializer))

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

    async def load(
        self,
        conn: asyncpg.Connection | None = None,
    ) -> StateRecord | None:
        """Load raw state from database."""
        await self.ensure_seeded()
        return await self._load_without_seed(conn)

    async def _load_without_seed(
        self,
        conn: asyncpg.Connection | None = None,
    ) -> StateRecord | None:
        should_release = conn is None
        if conn is None:
            conn = await self._pool.acquire()  # type: ignore[assignment]
        try:
            row = await conn.fetchrow(  # type: ignore[union-attr]
                f"SELECT state_json, state_type, state_module FROM {self._table_ref} WHERE run_id = $1",
                self._run_id,
            )
            if row is None:
                return None
            return StateRecord(
                data=row["state_json"],
                state_type=row["state_type"],
                state_module=row["state_module"],
            )
        finally:
            if should_release:
                await self._pool.release(conn)  # type: ignore[arg-type]

    async def save(self, record: StateRecord) -> None:
        await self.ensure_seeded()
        await self._save_record(record)

    async def _save_record(
        self,
        record: StateRecord,
        conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy | None = None,
    ) -> None:
        """Save raw state to database via upsert."""
        should_release = conn is None
        if conn is None:
            conn = await self._pool.acquire()  # type: ignore[assignment]
        try:
            now = _utc_now()
            # json.dumps raises TypeError for non-JSON data rather than
            # silently writing a Python repr into the JSON column.
            data = record.data if isinstance(record.data, str) else json.dumps(record.data)
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
                data,
                record.state_type,
                record.state_module,
                now,
                now,
            )
        finally:
            if should_release:
                await self._pool.release(conn)  # type: ignore[arg-type]

    def to_handle(self) -> dict[str, Any]:
        payload = PostgresSerializedState(run_id=self._run_id)
        return payload.model_dump()


class PostgresStateStore(StateStoreFacade[MODEL_T], Generic[MODEL_T]):
    """Compatibility StateStore facade backed by postgres storage."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
        schema: str | None = None,
        pending_seed: tuple[dict[str, Any], BaseSerializer] | None = None,
    ) -> None:
        self._pool = pool
        self._postgres_storage = _PostgresStateStorage(
            pool, run_id, schema, pending_seed
        )
        super().__init__(
            self._postgres_storage,
            state_type or DictState,  # type: ignore[arg-type]
            serializer or JsonSerializer(),
            to_dict_mode="handle",
        )

    @property
    def run_id(self) -> str:
        return self._postgres_storage.run_id

    async def _save_state(self, state: BaseModel) -> None:
        await self.ensure_seeded()
        await self._postgres_storage.save(
            string_record_from_state(state, self._serializer)
        )

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
                pending_seed=(
                    (serialized_state, serializer)
                    if parsed.run_id != effective_run_id
                    else None
                ),
            )

        if store_type not in (None, "in_memory"):
            raise ValueError(
                f"Cannot restore store_type '{store_type}' with PostgresStateStore.from_dict()"
            )

        # InMemory format — migrate on first DB access
        effective_run_id = run_id or str(uuid.uuid4())
        return cls(
            pool=pool,
            run_id=effective_run_id,
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            serializer=serializer,
            schema=schema,
            pending_seed=(serialized_state, serializer),
        )
