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
    version: datetime
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
    async def refresh_resume_owner(
        self, run_id: str, version: datetime
    ) -> ResumeClaim | None:
        """Refresh resume ownership timestamp and return the new owner claim."""
        ...

    @abstractmethod
    async def complete_resume(self, run_id: str, version: datetime) -> bool:
        """CAS: resuming with version -> active. Returns True on success."""
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
            f"ON CONFLICT (run_id) DO UPDATE SET state = $2, updated_at = $3",
            run_id,
            RunLifecycleState.active.value,
            datetime.now(timezone.utc),
        )

    async def begin_release(self, run_id: str) -> bool:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET state = $1, updated_at = $2 "
            f"WHERE run_id = $3 AND state = $4 RETURNING run_id",
            RunLifecycleState.releasing.value,
            datetime.now(timezone.utc),
            run_id,
            RunLifecycleState.active.value,
        )
        return row is not None

    async def complete_release(self, run_id: str) -> bool:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET state = $1, updated_at = $2 "
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
                    claim_row = await conn.fetchrow(
                        f"UPDATE {self._table_ref} SET state = $1, updated_at = $2 "
                        f"WHERE run_id = $3 RETURNING updated_at",
                        RunLifecycleState.resuming.value,
                        datetime.now(timezone.utc),
                        run_id,
                    )
                    return ResumeClaim(
                        version=claim_row["updated_at"],
                        previous_state=state,
                    )
                return state

    async def refresh_resume_owner(
        self, run_id: str, version: datetime
    ) -> ResumeClaim | None:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET updated_at = $1 "
            f"WHERE run_id = $2 AND state = $3 AND updated_at = $4 RETURNING updated_at",
            datetime.now(timezone.utc),
            run_id,
            RunLifecycleState.resuming.value,
            version,
        )
        if row is None:
            return None
        return ResumeClaim(
            version=row["updated_at"],
            previous_state=RunLifecycleState.resuming,
        )

    async def complete_resume(self, run_id: str, version: datetime) -> bool:
        row = await self._pool.fetchrow(
            f"UPDATE {self._table_ref} SET state = $1, updated_at = $2 "
            f"WHERE run_id = $3 AND state = $4 AND updated_at = $5 RETURNING run_id",
            RunLifecycleState.active.value,
            datetime.now(timezone.utc),
            run_id,
            RunLifecycleState.resuming.value,
            version,
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

    @staticmethod
    def _datetime_text(value: datetime) -> str:
        return value.isoformat()

    async def create(self, run_id: str) -> None:
        async with self._lock(run_id):
            with self._connect() as conn:
                conn.execute(
                    f"INSERT OR REPLACE INTO {self._table_ref} (run_id, state, updated_at) "
                    f"VALUES (?, ?, ?)",
                    (
                        run_id,
                        RunLifecycleState.active.value,
                        self._datetime_text(datetime.now(timezone.utc)),
                    ),
                )
                conn.commit()

    async def begin_release(self, run_id: str) -> bool:
        async with self._lock(run_id):
            with self._connect() as conn:
                cursor = conn.execute(
                    f"UPDATE {self._table_ref} SET state = ?, updated_at = ? "
                    f"WHERE run_id = ? AND state = ?",
                    (
                        RunLifecycleState.releasing.value,
                        self._datetime_text(datetime.now(timezone.utc)),
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
                    f"UPDATE {self._table_ref} SET state = ?, updated_at = ? "
                    f"WHERE run_id = ? AND state = ?",
                    (
                        RunLifecycleState.released.value,
                        self._datetime_text(datetime.now(timezone.utc)),
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
                            version = datetime.now(timezone.utc)
                            conn.execute(
                                f"UPDATE {self._table_ref} SET state = ?, updated_at = ? WHERE run_id = ?",
                                (
                                    RunLifecycleState.resuming.value,
                                    self._datetime_text(version),
                                    run_id,
                                ),
                            )
                            result = ResumeClaim(version=version, previous_state=state)
                    conn.commit()
                    return result
                except Exception:
                    conn.rollback()
                    raise

    async def refresh_resume_owner(
        self, run_id: str, version: datetime
    ) -> ResumeClaim | None:
        async with self._lock(run_id):
            with self._connect() as conn:
                new_version = datetime.now(timezone.utc)
                cursor = conn.execute(
                    f"UPDATE {self._table_ref} SET updated_at = ? "
                    f"WHERE run_id = ? AND state = ? AND updated_at = ?",
                    (
                        self._datetime_text(new_version),
                        run_id,
                        RunLifecycleState.resuming.value,
                        self._datetime_text(version),
                    ),
                )
                conn.commit()
                if cursor.rowcount == 0:
                    return None
                return ResumeClaim(
                    version=new_version,
                    previous_state=RunLifecycleState.resuming,
                )

    async def complete_resume(self, run_id: str, version: datetime) -> bool:
        async with self._lock(run_id):
            with self._connect() as conn:
                cursor = conn.execute(
                    f"UPDATE {self._table_ref} SET state = ?, updated_at = ? "
                    f"WHERE run_id = ? AND state = ? AND updated_at = ?",
                    (
                        RunLifecycleState.active.value,
                        self._datetime_text(datetime.now(timezone.utc)),
                        run_id,
                        RunLifecycleState.resuming.value,
                        self._datetime_text(version),
                    ),
                )
                conn.commit()
                return cursor.rowcount > 0
