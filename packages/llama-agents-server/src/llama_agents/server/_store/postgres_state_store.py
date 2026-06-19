# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Generic, Literal

import asyncpg
from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import DictState
from workflows.context.state_store_integration import (
    StateRecord,
    StateStoreFacade,
    restored_run_id,
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
        namespace: tuple[str, ...] = (),
        schema: str | None = None,
        connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy | None = None,
    ) -> None:
        self._pool = pool
        self._run_id = run_id
        self._namespace = namespace
        # Persisted key: () -> "" (today's single root row), ("child",) -> "child".
        self._namespace_key = "/".join(namespace)
        self._schema = schema
        self._shared_conn = connection

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def _table_ref(self) -> str:
        if self._schema:
            return f"{self._schema}.workflow_state"
        return "workflow_state"

    @asynccontextmanager
    async def _acquire(
        self,
    ) -> AsyncIterator[asyncpg.Connection | asyncpg.pool.PoolConnectionProxy]:
        """One query-API surface: the bound connection or a pool checkout."""
        if self._shared_conn is not None:
            yield self._shared_conn
            return
        async with self._pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_PostgresStateStorage]:
        """Scope a load+save pair to one pool connection.

        Yields a separate conn-bound storage so concurrent readers on this
        storage keep acquiring their own connections.
        """
        if self._shared_conn is not None:
            yield self
            return
        async with self._pool.acquire() as conn:
            yield _PostgresStateStorage(
                self._pool, self._run_id, self._namespace, self._schema, connection=conn
            )

    async def load(self) -> StateRecord | None:
        """Load raw state from the database."""
        async with self._acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT state_json, state_type, state_module FROM {self._table_ref} "
                "WHERE run_id = $1 AND namespace = $2",
                self._run_id,
                self._namespace_key,
            )
        if row is None:
            return None
        return StateRecord(
            data=row["state_json"],
            state_type=row["state_type"],
            state_module=row["state_module"],
        )

    async def save(self, record: StateRecord) -> None:
        """Save raw state to the database via upsert."""
        now = _utc_now()
        async with self._acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._table_ref} (run_id, namespace, state_json, state_type, state_module, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT(run_id, namespace) DO UPDATE SET
                    state_json = EXCLUDED.state_json,
                    state_type = EXCLUDED.state_type,
                    state_module = EXCLUDED.state_module,
                    updated_at = EXCLUDED.updated_at
                """,
                self._run_id,
                self._namespace_key,
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
        """Copy every namespace row from another run using INSERT...SELECT.

        The ``run_id`` filter spans all namespaces, so a single statement
        copies the source run's root and every child namespace; ``namespace``
        is carried through unchanged.
        """
        now = _utc_now()
        async with self._acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._table_ref} (run_id, namespace, state_json, state_type, state_module, created_at, updated_at)
                SELECT $1, namespace, state_json, state_type, state_module, $2, $3
                FROM {self._table_ref} WHERE run_id = $4
                ON CONFLICT(run_id, namespace) DO UPDATE SET
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
        namespace: tuple[str, ...] = (),
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
        schema: str | None = None,
    ) -> None:
        super().__init__(
            _PostgresStateStorage(pool, run_id, namespace, schema),
            state_type,
            serializer,
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
        """Restore a state store from a serialized payload.

        Construct + seed: ``add_seed`` validates the payload eagerly
        (foreign durable handles raise) and materializes it lazily.
        """
        if not serialized_state:
            raise ValueError("Cannot restore PostgresStateStore from empty dict")
        if pool is None:
            raise ValueError("pool is required for PostgresStateStore.from_dict()")

        store: PostgresStateStore[Any] = cls(
            pool=pool,
            run_id=restored_run_id(run_id, serialized_state),
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            serializer=serializer,
            schema=schema,
        )
        store.add_seed(serialized_state, serializer)
        return store
