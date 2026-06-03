# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import weakref
from collections.abc import AsyncIterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator, Sequence

from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from workflows.context import JsonSerializer
from workflows.context.serializers import BaseSerializer

from ..abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    StoredEvent,
    StoredTick,
)
from .migrate import run_migrations as _run_migrations
from .sqlite_state_store import SqliteStateStore

_TICK_PAGE_SIZE = 100


class SqliteWorkflowStore(AbstractWorkflowStore):
    def __init__(
        self,
        db_path: str,
        poll_interval: float = 1.0,
        auto_migrate: bool = True,
        single_connection: bool = False,
    ) -> None:
        self.db_path = db_path
        self.poll_interval = poll_interval
        self._single_connection = single_connection
        self._persistent_conn: sqlite3.Connection | None = None
        self._conditions: weakref.WeakValueDictionary[str, asyncio.Condition] = (
            weakref.WeakValueDictionary()
        )
        if single_connection:
            self._persistent_conn = self._open_nolock(db_path)
        if auto_migrate:
            self._run_migrations()

    @staticmethod
    def _open_nolock(db_path: str) -> sqlite3.Connection:
        """Open a SQLite connection with file locking disabled.

        Uses the ``unix-none`` VFS so that SQLite never issues ``fcntl``
        lock calls.  Safe when the database is only accessed by a single
        process (e.g. AgentCore session storage).
        """
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(f"file:{db_path}?vfs=unix-none", uri=True)
            conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            return conn
        except sqlite3.OperationalError:
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.close()
            return sqlite3.connect(db_path, timeout=30.0)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        if self._single_connection:
            assert self._persistent_conn is not None
            yield self._persistent_conn
        else:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            try:
                yield conn
            finally:
                conn.close()

    def create_state_store(
        self,
        run_id: str,
        state_type: type[Any] | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> SqliteStateStore[Any]:
        store = SqliteStateStore(
            db_path=self.db_path,
            run_id=run_id,
            state_type=state_type,
            serializer=serializer,
            connection=self._persistent_conn,
        )
        if serialized_state is not None and serializer is not None:
            store._seed_from_serialized(serialized_state, serializer)
        return store

    def _get_or_create_condition(self, run_id: str) -> asyncio.Condition:
        cond = self._conditions.get(run_id)
        if cond is None:
            cond = asyncio.Condition()
            self._conditions[run_id] = cond
        return cond

    def _run_migrations(self) -> None:
        if self._persistent_conn is not None:
            _run_migrations(self._persistent_conn)
            self._persistent_conn.commit()
        else:
            self.run_migrations(self.db_path)

    @staticmethod
    def run_migrations(db_path: str) -> None:
        """Run all pending SQLite schema migrations.

        Safe to call multiple times — only applies migrations not yet applied.
        """
        conn = sqlite3.connect(db_path, timeout=30.0)
        try:
            _run_migrations(conn)
            conn.commit()
        finally:
            conn.close()

    async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
        filter_spec = self._build_filters(query)
        if filter_spec is None:
            return []

        clauses, params = filter_spec
        sql = """SELECT handler_id, workflow_name, status, run_id, error, result,
                        started_at, updated_at, completed_at, idle_since FROM handlers"""
        if clauses:
            sql = f"{sql} WHERE {' AND '.join(clauses)}"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()

        return [_row_to_persistent_handler(row) for row in rows]

    async def update(self, handler: PersistentHandler) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO handlers (handler_id, workflow_name, status, run_id, error, result,
                                      started_at, updated_at, completed_at, idle_since)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(handler_id) DO UPDATE SET
                    workflow_name = excluded.workflow_name,
                    status = excluded.status,
                    run_id = excluded.run_id,
                    error = excluded.error,
                    result = excluded.result,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at,
                    completed_at = excluded.completed_at,
                    idle_since = excluded.idle_since
                """,
                (
                    handler.handler_id,
                    handler.workflow_name,
                    handler.status,
                    handler.run_id,
                    handler.error,
                    JsonSerializer().serialize(handler.result)
                    if handler.result is not None
                    else None,
                    handler.started_at.isoformat() if handler.started_at else None,
                    handler.updated_at.isoformat() if handler.updated_at else None,
                    handler.completed_at.isoformat() if handler.completed_at else None,
                    handler.idle_since.isoformat() if handler.idle_since else None,
                ),
            )
            conn.commit()

    async def delete(self, query: HandlerQuery) -> int:
        filter_spec = self._build_filters(query)
        if filter_spec is None:
            return 0

        clauses, params = filter_spec
        if not clauses:
            return 0

        sql = f"DELETE FROM handlers WHERE {' AND '.join(clauses)}"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            deleted = cursor.rowcount
            conn.commit()

        return int(deleted)

    async def append_event(self, run_id: str, event: EventEnvelopeWithMetadata) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO events (run_id, sequence, timestamp, event_json)
                VALUES (?, COALESCE((SELECT MAX(sequence) FROM events WHERE run_id = ?), -1) + 1, CURRENT_TIMESTAMP, ?)""",
                (
                    run_id,
                    run_id,
                    event.model_dump_json(),
                ),
            )
            conn.commit()
        condition = self._conditions.get(run_id)
        if condition is not None:
            async with condition:
                condition.notify_all()

    async def query_events(
        self,
        run_id: str,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        sql = "SELECT run_id, sequence, timestamp, event_json FROM events WHERE run_id = ?"
        params: list[Any] = [run_id]
        if after_sequence is not None:
            sql += " AND sequence > ?"
            params.append(after_sequence)
        sql += " ORDER BY sequence"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return [
            StoredEvent(
                run_id=row[0],
                sequence=row[1],
                timestamp=datetime.fromisoformat(row[2]),
                event=EventEnvelopeWithMetadata.model_validate_json(row[3]),
            )
            for row in rows
        ]

    async def subscribe_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        condition = self._get_or_create_condition(run_id)
        cursor = after_sequence

        while True:
            async with condition:
                batch = await self.query_events(run_id, after_sequence=cursor)
                if not batch:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(
                            condition.wait(), timeout=self.poll_interval
                        )
                    continue

            for event in batch:
                yield event
                cursor = event.sequence
                if self._is_terminal_event(event):
                    return

    async def append_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO ticks (run_id, sequence, timestamp, tick_data)
                VALUES (?, COALESCE((SELECT MAX(sequence) FROM ticks WHERE run_id = ?), -1) + 1, CURRENT_TIMESTAMP, ?)""",
                (
                    run_id,
                    run_id,
                    json.dumps(tick_data),
                ),
            )
            conn.commit()

    async def get_ticks(self, run_id: str) -> list[StoredTick]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT run_id, sequence, timestamp, tick_data FROM ticks WHERE run_id = ? ORDER BY sequence",
                (run_id,),
            )
            rows = cursor.fetchall()
        return [
            StoredTick(
                run_id=row[0],
                sequence=row[1],
                timestamp=datetime.fromisoformat(row[2]),
                tick_data=json.loads(row[3]),
            )
            for row in rows
        ]

    async def stream_ticks(self, run_id: str) -> AsyncIterator[StoredTick]:
        seq_cursor: int | None = None
        while True:
            if seq_cursor is None:
                sql = (
                    "SELECT run_id, sequence, timestamp, tick_data FROM ticks "
                    "WHERE run_id = ? ORDER BY sequence LIMIT ?"
                )
                params: list[Any] = [run_id, _TICK_PAGE_SIZE]
            else:
                sql = (
                    "SELECT run_id, sequence, timestamp, tick_data FROM ticks "
                    "WHERE run_id = ? AND sequence > ? ORDER BY sequence LIMIT ?"
                )
                params = [run_id, seq_cursor, _TICK_PAGE_SIZE]
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            for row in rows:
                tick = StoredTick(
                    run_id=row[0],
                    sequence=row[1],
                    timestamp=datetime.fromisoformat(row[2]),
                    tick_data=json.loads(row[3]),
                )
                yield tick
                seq_cursor = tick.sequence
            if len(rows) < _TICK_PAGE_SIZE:
                return

    def get_legacy_ctx(self, run_id: str) -> dict[str, Any] | None:
        """Read the old ctx column for a run_id, if present."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT ctx FROM handlers WHERE run_id = ?",
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None or row[0] is None:
                return None
            try:
                data = json.loads(row[0])
                if not isinstance(data, dict) or not data:
                    return None
                return data
            except (json.JSONDecodeError, TypeError):
                return None

    def _build_filters(self, query: HandlerQuery) -> tuple[list[str], list[str]] | None:
        clauses: list[str] = []
        params: list[str] = []

        def add_in_clause(column: str, values: Sequence[str]) -> None:
            placeholders = ",".join(["?"] * len(values))
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(values)

        if query.workflow_name_in is not None:
            if len(query.workflow_name_in) == 0:
                return None
            add_in_clause("workflow_name", query.workflow_name_in)

        if query.handler_id_in is not None:
            if len(query.handler_id_in) == 0:
                return None
            add_in_clause("handler_id", query.handler_id_in)

        if query.run_id_in is not None:
            if len(query.run_id_in) == 0:
                return None
            add_in_clause("run_id", query.run_id_in)

        if query.status_in is not None:
            if len(query.status_in) == 0:
                return None
            add_in_clause("status", query.status_in)

        if query.is_idle is not None:
            if query.is_idle:
                clauses.append("idle_since IS NOT NULL")
            else:
                clauses.append("idle_since IS NULL")

        if not clauses:
            return clauses, params

        return clauses, params


def _row_to_persistent_handler(row: tuple) -> PersistentHandler:
    return PersistentHandler(
        handler_id=row[0],
        workflow_name=row[1],
        status=row[2],
        run_id=row[3],
        error=row[4],
        result=json.loads(row[5]) if row[5] else None,
        started_at=datetime.fromisoformat(row[6]) if row[6] else None,
        updated_at=datetime.fromisoformat(row[7]) if row[7] else None,
        completed_at=datetime.fromisoformat(row[8]) if row[8] else None,
        idle_since=datetime.fromisoformat(row[9]) if row[9] else None,
    )
