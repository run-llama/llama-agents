# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Distributed lifecycle lock for coordinating idle release/resume across replicas."""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Iterator
from uuid import uuid4

import asyncpg
from llama_agents.dbos.journal.crud import _qualified_table_ref, _quote_identifier
from llama_agents.server._keyed_lock import KeyedLock

LIFECYCLE_TABLE_NAME = "run_lifecycle"


class RunLifecycleState(str, Enum):
    active = "active"
    releasing = "releasing"
    released = "released"
    resuming = "resuming"


@dataclass(frozen=True)
class ResumeClaim:
    token: str
    previous_state: RunLifecycleState


class RunLifecycleLock(ABC):
    """Abstract base for the run lifecycle lock.

    State machine: active -> releasing -> released -> resuming -> active
    """

    @abstractmethod
    async def create(self, run_id: str) -> None:
        """Insert row with state='active'. Called when workflow starts."""
        ...

    @abstractmethod
    async def begin_release(self, run_id: str) -> bool:
        """CAS: active -> releasing. Returns True on success."""
        ...

    @abstractmethod
    async def complete_release(self, run_id: str) -> bool:
        """CAS: releasing -> released. Returns True on success."""
        ...

    @abstractmethod
    async def try_begin_resume(
        self, run_id: str, crash_timeout_seconds: float | None = None
    ) -> ResumeClaim | RunLifecycleState | None:
        """Attempt to claim resume.

        Returns:
            None: no row or 'active' - send normally
            ResumeClaim: transitioned to 'resuming', caller owns resume
            releasing/resuming: in progress, caller should wait and retry

        If crash_timeout_seconds is set and the current state is 'releasing'
        or 'resuming' with an updated_at older than the timeout, force-transitions
        to 'resuming' and returns a ResumeClaim.
        """
        ...

    @abstractmethod
    async def is_resume_owner(self, run_id: str, token: str) -> bool:
        """Return whether token still owns the resuming row."""
        ...

    @abstractmethod
    async def refresh_resume_owner(self, run_id: str, token: str) -> bool:
        """Refresh resume ownership timestamp. Returns True if token still owns it."""
        ...

    @abstractmethod
    async def complete_resume(self, run_id: str, token: str) -> bool:
        """CAS: resuming with token -> active. Returns True on success."""
        ...


class PostgresRunLifecycleLock(RunLifecycleLock):
    """Lifecycle lock using asyncpg with SELECT FOR UPDATE."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        table_name: str = LIFECYCLE_TABLE_NAME,
        schema: str | None = None,
    ) -> None:
        self._pool = pool
        self._table_ref = _qualified_table_ref(table_name, schema)

    async def create(self, run_id: str) -> None:
        await self._pool.execute(
            f"INSERT INTO {self._table_ref} (run_id, state, updated_at) "
            f"VALUES ($1, $2, $3) "
            f"ON CONFLICT (run_id) DO UPDATE SET state = $2, resume_token = NULL, updated_at = $3",
            run_id,
            RunLifecycleState.active.value,
            datetime.now(timezone.utc),
        )

    async def begin_release(self, run_id: str) -> bool:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET state = $1, resume_token = NULL, updated_at = $2 "
            f"WHERE run_id = $3 AND state = $4 RETURNING run_id",
            RunLifecycleState.releasing.value,
            datetime.now(timezone.utc),
            run_id,
            RunLifecycleState.active.value,
        )
        return row is not None

    async def complete_release(self, run_id: str) -> bool:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET state = $1, resume_token = NULL, updated_at = $2 "
            f"WHERE run_id = $3 AND state = $4 RETURNING run_id",
            RunLifecycleState.released.value,
            datetime.now(timezone.utc),
            run_id,
            RunLifecycleState.releasing.value,
        )
        return row is not None

    async def try_begin_resume(
        self, run_id: str, crash_timeout_seconds: float | None = None
    ) -> ResumeClaim | RunLifecycleState | None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"SELECT state, updated_at FROM {self._table_ref} "
                    f"WHERE run_id = $1 FOR UPDATE",
                    run_id,
                )
                if row is None:
                    return None
                state = RunLifecycleState(row["state"])
                if state == RunLifecycleState.active:
                    return None
                if state == RunLifecycleState.released or (
                    state in (RunLifecycleState.releasing, RunLifecycleState.resuming)
                    and crash_timeout_seconds is not None
                    and (datetime.now(timezone.utc) - row["updated_at"]).total_seconds()
                    > crash_timeout_seconds
                ):
                    token = uuid4().hex
                    await conn.execute(
                        f"UPDATE {self._table_ref} SET state = $1, resume_token = $2, updated_at = $3 "
                        f"WHERE run_id = $4",
                        RunLifecycleState.resuming.value,
                        token,
                        datetime.now(timezone.utc),
                        run_id,
                    )
                    return ResumeClaim(token=token, previous_state=state)
                return state

    async def is_resume_owner(self, run_id: str, token: str) -> bool:
        row = await self._pool.fetchrow(
            f"SELECT run_id FROM {self._table_ref} "
            f"WHERE run_id = $1 AND state = $2 AND resume_token = $3",
            run_id,
            RunLifecycleState.resuming.value,
            token,
        )
        return row is not None

    async def refresh_resume_owner(self, run_id: str, token: str) -> bool:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET updated_at = $1 "
            f"WHERE run_id = $2 AND state = $3 AND resume_token = $4 RETURNING run_id",
            datetime.now(timezone.utc),
            run_id,
            RunLifecycleState.resuming.value,
            token,
        )
        return row is not None

    async def complete_resume(self, run_id: str, token: str) -> bool:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET state = $1, resume_token = NULL, updated_at = $2 "
            f"WHERE run_id = $3 AND state = $4 AND resume_token = $5 RETURNING run_id",
            RunLifecycleState.active.value,
            datetime.now(timezone.utc),
            run_id,
            RunLifecycleState.resuming.value,
            token,
        )
        return row is not None


class SqliteRunLifecycleLock(RunLifecycleLock):
    """Lifecycle lock using sqlite3 with process-local KeyedLock for serialization."""

    def __init__(
        self,
        db_path: str,
        table_name: str = LIFECYCLE_TABLE_NAME,
    ) -> None:
        self._db_path = db_path
        self._table_ref = _quote_identifier(table_name)
        self._lock = KeyedLock()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    async def create(self, run_id: str) -> None:
        async with self._lock(run_id):
            with self._connect() as conn:
                conn.execute(
                    f"INSERT OR REPLACE INTO {self._table_ref} (run_id, state, updated_at) "
                    f"VALUES (?, ?, ?)",
                    (
                        run_id,
                        RunLifecycleState.active.value,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                conn.commit()

    async def begin_release(self, run_id: str) -> bool:
        async with self._lock(run_id):
            with self._connect() as conn:
                cursor = conn.execute(
                    f"UPDATE {self._table_ref} SET state = ?, resume_token = NULL, updated_at = ? "
                    f"WHERE run_id = ? AND state = ?",
                    (
                        RunLifecycleState.releasing.value,
                        datetime.now(timezone.utc).isoformat(),
                        run_id,
                        RunLifecycleState.active.value,
                    ),
                )
                conn.commit()
                return cursor.rowcount > 0

    async def complete_release(self, run_id: str) -> bool:
        async with self._lock(run_id):
            with self._connect() as conn:
                cursor = conn.execute(
                    f"UPDATE {self._table_ref} SET state = ?, resume_token = NULL, updated_at = ? "
                    f"WHERE run_id = ? AND state = ?",
                    (
                        RunLifecycleState.released.value,
                        datetime.now(timezone.utc).isoformat(),
                        run_id,
                        RunLifecycleState.releasing.value,
                    ),
                )
                conn.commit()
                return cursor.rowcount > 0

    async def try_begin_resume(
        self, run_id: str, crash_timeout_seconds: float | None = None
    ) -> ResumeClaim | RunLifecycleState | None:
        async with self._lock(run_id):
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    row = conn.execute(
                        f"SELECT state, updated_at FROM {self._table_ref} WHERE run_id = ?",
                        (run_id,),
                    ).fetchone()
                    if row is None:
                        result: ResumeClaim | RunLifecycleState | None = None
                    else:
                        state = RunLifecycleState(row["state"])
                        result = state
                        if state == RunLifecycleState.active:
                            result = None
                        elif state == RunLifecycleState.released or (
                            state
                            in (
                                RunLifecycleState.releasing,
                                RunLifecycleState.resuming,
                            )
                            and crash_timeout_seconds is not None
                            and (
                                datetime.now(timezone.utc)
                                - datetime.fromisoformat(row["updated_at"])
                            ).total_seconds()
                            > crash_timeout_seconds
                        ):
                            token = uuid4().hex
                            conn.execute(
                                f"UPDATE {self._table_ref} SET state = ?, resume_token = ?, updated_at = ? WHERE run_id = ?",
                                (
                                    RunLifecycleState.resuming.value,
                                    token,
                                    datetime.now(timezone.utc).isoformat(),
                                    run_id,
                                ),
                            )
                            result = ResumeClaim(token=token, previous_state=state)
                    conn.commit()
                    return result
                except Exception:
                    conn.rollback()
                    raise

    async def is_resume_owner(self, run_id: str, token: str) -> bool:
        async with self._lock(run_id):
            with self._connect() as conn:
                row = conn.execute(
                    f"SELECT run_id FROM {self._table_ref} "
                    f"WHERE run_id = ? AND state = ? AND resume_token = ?",
                    (run_id, RunLifecycleState.resuming.value, token),
                ).fetchone()
                return row is not None

    async def refresh_resume_owner(self, run_id: str, token: str) -> bool:
        async with self._lock(run_id):
            with self._connect() as conn:
                cursor = conn.execute(
                    f"UPDATE {self._table_ref} SET updated_at = ? "
                    f"WHERE run_id = ? AND state = ? AND resume_token = ?",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        run_id,
                        RunLifecycleState.resuming.value,
                        token,
                    ),
                )
                conn.commit()
                return cursor.rowcount > 0

    async def complete_resume(self, run_id: str, token: str) -> bool:
        async with self._lock(run_id):
            with self._connect() as conn:
                cursor = conn.execute(
                    f"UPDATE {self._table_ref} SET state = ?, resume_token = NULL, updated_at = ? "
                    f"WHERE run_id = ? AND state = ? AND resume_token = ?",
                    (
                        RunLifecycleState.active.value,
                        datetime.now(timezone.utc).isoformat(),
                        run_id,
                        RunLifecycleState.resuming.value,
                        token,
                    ),
                )
                conn.commit()
                return cursor.rowcount > 0
