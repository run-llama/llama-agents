# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import logging
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Generic, Literal, cast

from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    DictState,
    create_cleared_state,
    decode_seed_state,
    decode_state,
    encode_state_to_str,
    get_by_path,
    merge_state,
    set_by_path,
)

logger = logging.getLogger(__name__)

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]


class SqliteSerializedState(BaseModel):
    """Serialized state referencing a sqlite database row."""

    store_type: Literal["sqlite"] = "sqlite"
    run_id: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SqliteStateStore(Generic[MODEL_T]):
    """Sqlite-backed StateStore implementation.

    Every get() reads from the database, every set() writes through.
    No in-memory cache — the database is the source of truth.
    """

    state_type: type[MODEL_T]

    def __init__(
        self,
        db_path: str,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        self._db_path = db_path
        self._run_id = run_id
        self.state_type = state_type or DictState  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        self._serializer = serializer or JsonSerializer()
        self._shared_conn = connection

    @property
    def run_id(self) -> str:
        return self._run_id

    @functools.cached_property
    def _lock(self) -> asyncio.Lock:
        """Lazy lock initialization for Python 3.14+ compatibility."""
        return asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        if self._shared_conn is not None:
            return self._shared_conn
        return sqlite3.connect(self._db_path, timeout=30.0)

    def _write_in_memory_state(self, serialized_state: dict[str, Any]) -> None:
        """Migrate InMemory-format state into the database."""
        state = decode_seed_state(serialized_state, self._serializer)
        self._save_state(state)  # type: ignore[arg-type]

    def _seed_from_serialized(
        self, serialized_state: dict[str, Any], serializer: BaseSerializer
    ) -> None:
        """Seed this store from serialized state data.

        Handles both sqlite references (SQL-level copy) and InMemory format.
        """
        self._serializer = serializer
        store_type = serialized_state.get("store_type")
        if store_type == "sqlite":
            source_run_id = serialized_state.get("run_id")
            if source_run_id and source_run_id != self._run_id:
                self._copy_state_from_run(source_run_id)
        else:
            self._write_in_memory_state(serialized_state)

    def _copy_state_from_run(self, source_run_id: str) -> None:
        """Copy state from another run_id using SQL INSERT...SELECT."""
        conn = self._connect()
        try:
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
        finally:
            conn.close()

    def _create_default_state(self) -> MODEL_T:
        return self.state_type()

    def _load_state(self) -> MODEL_T:
        """Load state from database. Creates default if row doesn't exist."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT state_json, state_type, state_module FROM workflow_state WHERE run_id = ?",
                (self._run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                state = self._create_default_state()
                self._save_state(state, conn)
                conn.commit()
                return state
            return cast(MODEL_T, decode_state(row[0], row[1], row[2], self._serializer))
        finally:
            conn.close()

    def _save_state(
        self, state: MODEL_T, conn: sqlite3.Connection | None = None
    ) -> None:
        """Save state to database."""
        should_close = conn is None
        if conn is None:
            conn = self._connect()
        try:
            now = _utc_now().isoformat()
            state_json, state_type_name, state_module = encode_state_to_str(
                state, self._serializer
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
                    state_json,
                    state_type_name,
                    state_module,
                    now,
                    now,
                ),
            )
            if should_close:
                conn.commit()
        finally:
            if should_close:
                conn.close()

    async def get_state(self) -> MODEL_T:
        """Return a copy of the current state model."""
        state = self._load_state()
        return state.model_copy()

    async def set_state(self, state: MODEL_T) -> None:
        """Replace or merge into the current state model."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT state_json, state_type, state_module FROM workflow_state WHERE run_id = ?",
                (self._run_id,),
            )
            row = cursor.fetchone()

            if row is None:
                self._save_state(state, conn)
                conn.commit()
                return

            current_state = decode_state(row[0], row[1], row[2], self._serializer)
            merged = merge_state(current_state, state)
            self._save_state(merged, conn)  # type: ignore[arg-type]
            conn.commit()
        finally:
            conn.close()

    async def get(self, path: str, default: Any = ...) -> Any:
        """Get a nested value using dot-separated paths."""
        state = self._load_state()
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
            state = self._load_state()
            yield state
            self._save_state(state)

    def to_dict(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize state store metadata for persistence.

        Returns metadata only — actual state lives in the database.
        """
        payload = SqliteSerializedState(run_id=self._run_id)
        return payload.model_dump()

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
            return cls(
                db_path=db_path,
                run_id=effective_run_id,
                state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
                serializer=serializer,
            )

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
