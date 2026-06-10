# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generic, Literal

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


class SqliteSerializedState(BaseModel):
    """Serialized state referencing a sqlite database row."""

    store_type: Literal["sqlite"] = "sqlite"
    run_id: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class _SqliteStateStorage:
    """Sqlite-backed raw state storage."""

    def __init__(
        self,
        db_path: str,
        run_id: str,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        self._db_path = db_path
        self._run_id = run_id
        self._shared_conn = connection

    @property
    def run_id(self) -> str:
        return self._run_id

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        if self._shared_conn is not None:
            yield self._shared_conn
            return
        conn = sqlite3.connect(self._db_path, timeout=30.0)
        try:
            yield conn
        finally:
            conn.close()

    async def load(self) -> StateRecord | None:
        """Load raw state from the database."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT state_json FROM workflow_state WHERE run_id = ?",
                (self._run_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return StateRecord(data=row[0])

    async def save(self, record: StateRecord) -> None:
        """Save raw state to the database via upsert."""
        with self._connect() as conn:
            now = _utc_now().isoformat()
            conn.execute(
                """
                INSERT INTO workflow_state (run_id, state_json, state_type, state_module, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    state_json = excluded.state_json,
                    state_type = excluded.state_type,
                    state_module = excluded.state_module,
                    updated_at = excluded.updated_at
                """,
                (
                    self._run_id,
                    record.data,
                    record.state_type,
                    record.state_module,
                    now,
                    now,
                ),
            )
            conn.commit()

    def to_handle(self) -> dict[str, Any]:
        payload = SqliteSerializedState(run_id=self._run_id)
        return payload.model_dump()

    def parse_own_handle(self, payload: dict[str, Any]) -> SqliteSerializedState | None:
        if payload.get("store_type") != "sqlite":
            return None
        return SqliteSerializedState.model_validate(payload)

    async def copy_from_handle(self, handle: SqliteSerializedState) -> None:
        """Copy state from another run's row using SQL INSERT...SELECT."""
        with self._connect() as conn:
            now = _utc_now().isoformat()
            conn.execute(
                """
                INSERT OR REPLACE INTO workflow_state (run_id, state_json, state_type, state_module, created_at, updated_at)
                SELECT ?, state_json, state_type, state_module, ?, ?
                FROM workflow_state WHERE run_id = ?
                """,
                (self._run_id, now, now, handle.run_id),
            )
            conn.commit()


class SqliteStateStore(StateStoreFacade[MODEL_T], Generic[MODEL_T]):
    """StateStore facade backed by sqlite storage."""

    def __init__(
        self,
        db_path: str,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        self._db_path = db_path
        super().__init__(
            _SqliteStateStorage(db_path, run_id, connection), state_type, serializer
        )

    @classmethod
    def from_dict(
        cls,
        serialized_state: dict[str, Any],
        serializer: BaseSerializer,
        db_path: str | None = None,
        state_type: type[BaseModel] | None = None,
        run_id: str | None = None,
    ) -> SqliteStateStore[Any]:
        """Restore a state store from a serialized payload.

        Construct + seed: ``add_seed`` validates the payload eagerly
        (foreign durable handles raise) and materializes it lazily.
        """
        if not serialized_state:
            raise ValueError("Cannot restore SqliteStateStore from empty dict")
        if db_path is None:
            raise ValueError("db_path is required for SqliteStateStore.from_dict()")

        store: SqliteStateStore[Any] = cls(
            db_path=db_path,
            run_id=restored_run_id(run_id, serialized_state),
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            serializer=serializer,
        )
        store.add_seed(serialized_state, serializer)
        return store
