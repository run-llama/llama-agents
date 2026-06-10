# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

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
)

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
    ) -> None:
        self._pool = pool
        self._run_id = run_id
        self._schema = schema

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def _table_ref(self) -> str:
        if self._schema:
            return f"{self._schema}.workflow_state"
        return "workflow_state"

    async def load(self) -> StateRecord | None:
        """Load raw state from the database."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT state_json FROM {self._table_ref} WHERE run_id = $1",
                self._run_id,
            )
        if row is None:
            return None
        return StateRecord(data=row["state_json"])

    async def save(self, record: StateRecord) -> None:
        """Save raw state to the database via upsert."""
        now = _utc_now()
        async with self._pool.acquire() as conn:
            await conn.execute(
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
                record.data,
                record.state_type,
                record.state_module,
                now,
                now,
            )

    def to_handle(self) -> dict[str, Any]:
        payload = PostgresSerializedState(run_id=self._run_id)
        return payload.model_dump()

    def parse_own_handle(
        self, payload: dict[str, Any]
    ) -> PostgresSerializedState | None:
        if payload.get("store_type") != "postgres":
            return None
        return PostgresSerializedState.model_validate(payload)

    async def copy_from_handle(self, handle: PostgresSerializedState) -> None:
        """Copy state from another run's row using SQL INSERT...SELECT."""
        now = _utc_now()
        async with self._pool.acquire() as conn:
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
                handle.run_id,
            )


class PostgresStateStore(StateStoreFacade[MODEL_T], Generic[MODEL_T]):
    """StateStore facade backed by postgres storage."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
        schema: str | None = None,
    ) -> None:
        self._postgres_storage = _PostgresStateStorage(pool, run_id, schema)
        super().__init__(
            self._postgres_storage,
            state_type or DictState,  # type: ignore[arg-type]
            serializer or JsonSerializer(),
        )

    @property
    def run_id(self) -> str:
        return self._postgres_storage.run_id

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
        """Restore a state store from a serialized payload.

        Construct + seed: ``add_seed`` validates the payload eagerly
        (foreign durable handles raise) and materializes it lazily.
        """
        if not serialized_state:
            raise ValueError("Cannot restore PostgresStateStore from empty dict")
        if pool is None:
            raise ValueError("pool is required for PostgresStateStore.from_dict()")

        effective_run_id = run_id or serialized_state.get("run_id") or str(uuid.uuid4())
        store: PostgresStateStore[Any] = cls(
            pool=pool,
            run_id=effective_run_id,
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            serializer=serializer,
            schema=schema,
        )
        store.add_seed(serialized_state, serializer)
        return store
