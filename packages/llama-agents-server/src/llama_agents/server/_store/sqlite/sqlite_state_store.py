# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generic, Literal

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

    def _copy_state_from_run(self, source_run_id: str) -> None:
        """Copy state from another run_id using SQL INSERT...SELECT."""
        with self._connect() as conn:
            now = _utc_now().isoformat()
            conn.execute(
                """
                INSERT OR REPLACE INTO workflow_state (run_id, state_json, state_type, state_module, created_at, updated_at)
                SELECT ?, state_json, state_type, state_module, ?, ?
                FROM workflow_state WHERE run_id = ?
                """,
                (self._run_id, now, now, source_run_id),
            )
            conn.commit()

    async def load(self) -> StateRecord | None:
        """Load raw state from database."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT state_json, state_type, state_module FROM workflow_state WHERE run_id = ?",
                (self._run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return StateRecord(data=row[0], state_type=row[1], state_module=row[2])

    async def save(self, record: StateRecord) -> None:
        """Save raw state to database."""
        self._save_record(record)

    def _save_record(
        self, record: StateRecord, conn: sqlite3.Connection | None = None
    ) -> None:
        should_commit = conn is None
        if conn is None:
            context = self._connect()
            conn = context.__enter__()
        else:
            context = None
        try:
            now = _utc_now().isoformat()
            # json.dumps raises TypeError for non-JSON data rather than
            # silently writing a Python repr into the JSON column.
            data = (
                record.data if isinstance(record.data, str) else json.dumps(record.data)
            )
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
                    data,
                    record.state_type,
                    record.state_module,
                    now,
                    now,
                ),
            )
            if should_commit:
                conn.commit()
        finally:
            if context is not None:
                context.__exit__(None, None, None)

    def to_handle(self) -> dict[str, Any]:
        payload = SqliteSerializedState(run_id=self._run_id)
        return payload.model_dump()


class SqliteStateStore(StateStoreFacade[MODEL_T], Generic[MODEL_T]):
    """Compatibility StateStore facade backed by sqlite storage."""

    def __init__(
        self,
        db_path: str,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        self._db_path = db_path
        self._sqlite_storage = _SqliteStateStorage(db_path, run_id, connection)
        super().__init__(
            self._sqlite_storage,
            state_type or DictState,  # type: ignore[arg-type]
            serializer or JsonSerializer(),
            to_dict_mode="handle",
        )

    @property
    def run_id(self) -> str:
        return self._sqlite_storage.run_id

    def _seed_from_serialized(
        self, serialized_state: dict[str, Any], serializer: BaseSerializer
    ) -> None:
        """Seed this store from serialized state data."""
        self._serializer = serializer
        store_type = serialized_state.get("store_type")
        if store_type == "sqlite":
            source_run_id = serialized_state.get("run_id")
            if source_run_id and source_run_id != self.run_id:
                self._sqlite_storage._copy_state_from_run(source_run_id)
            return
        self._write_in_memory_state(serialized_state)

    def _write_in_memory_state(self, serialized_state: dict[str, Any]) -> None:
        """Migrate InMemory-format state into the database."""
        state = decode_seed_state(serialized_state, self._serializer)
        self._sqlite_storage._save_record(
            string_record_from_state(state, self._serializer)
        )

    async def _save_state(self, state: BaseModel) -> None:
        await self._sqlite_storage.save(
            string_record_from_state(state, self._serializer)
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
        """Restore a state store from serialized payload.

        Handles both InMemorySerializedState (migrates data to DB on first use)
        and SqliteSerializedState (reconnects to existing row).
        """
        if not serialized_state:
            raise ValueError("Cannot restore SqliteStateStore from empty dict")

        store_type = serialized_state.get("store_type")

        if store_type == "sqlite":
            parsed = SqliteSerializedState.model_validate(serialized_state)
            effective_run_id = run_id or parsed.run_id
            if db_path is None:
                raise ValueError("db_path is required for SqliteStateStore.from_dict()")
            store = cls(
                db_path=db_path,
                run_id=effective_run_id,
                state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
                serializer=serializer,
            )
            if parsed.run_id != effective_run_id:
                store._seed_from_serialized(serialized_state, serializer)
            return store

        # InMemory format — migrate data to DB immediately
        effective_run_id = run_id or str(uuid.uuid4())
        if db_path is None:
            raise ValueError("db_path is required for SqliteStateStore.from_dict()")
        store = cls(
            db_path=db_path,
            run_id=effective_run_id,
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            serializer=serializer,
        )
        store._write_in_memory_state(serialized_state)
        return store
