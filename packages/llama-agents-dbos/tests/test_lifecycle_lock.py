# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Unit tests for RunLifecycleLock implementations (SQLite + PostgreSQL)."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Protocol

import asyncpg
import pytest
from llama_agents.dbos._store import POSTGRES_MIGRATION_SOURCE
from llama_agents.dbos.journal.lifecycle import (
    LIFECYCLE_TABLE_NAME,
    PostgresRunLifecycleLock,
    ResumeClaim,
    RunLifecycleLock,
    RunLifecycleState,
    SqliteRunLifecycleLock,
)
from llama_agents.server._store import (
    POSTGRES_MIGRATION_SOURCE as SERVER_POSTGRES_MIGRATION_SOURCE,
)
from llama_agents.server._store.postgres.migrate import run_migrations as pg_migrations


class UpdatedAtSetter(Protocol):
    async def set_updated_at(self, run_id: str, updated_at: datetime) -> None: ...


class SqliteUpdatedAtSetter:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    async def set_updated_at(self, run_id: str, updated_at: datetime) -> None:
        conn = sqlite3.connect(self._db_path)
        conn.execute(
            f"UPDATE {LIFECYCLE_TABLE_NAME} SET updated_at = ? WHERE run_id = ?",
            (updated_at.isoformat(), run_id),
        )
        conn.commit()
        conn.close()


class PostgresUpdatedAtSetter:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def set_updated_at(self, run_id: str, updated_at: datetime) -> None:
        await self._pool.execute(
            f"UPDATE {LIFECYCLE_TABLE_NAME} SET updated_at = $1 WHERE run_id = $2",
            updated_at,
            run_id,
        )


LockFixture = tuple[RunLifecycleLock, UpdatedAtSetter]

sqlite_param = pytest.param("sqlite", id="sqlite")
postgres_param = pytest.param("postgres", marks=pytest.mark.docker, id="postgres")


@pytest.fixture
async def lock_fixture(
    request: pytest.FixtureRequest,
    journal_db_path: str,
) -> AsyncGenerator[LockFixture]:
    backend = request.param
    if backend == "sqlite":
        yield (
            SqliteRunLifecycleLock(db_path=journal_db_path),
            SqliteUpdatedAtSetter(journal_db_path),
        )
    else:
        dsn = request.getfixturevalue("postgres_dsn")
        conn = await asyncpg.connect(dsn)
        schema = "test_lifecycle_lock"
        try:
            await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            await pg_migrations(
                conn,
                schema=schema,
                sources=[
                    SERVER_POSTGRES_MIGRATION_SOURCE,
                    POSTGRES_MIGRATION_SOURCE,
                ],
            )
        finally:
            await conn.close()
        pool = await asyncpg.create_pool(dsn, server_settings={"search_path": schema})
        assert pool is not None
        try:
            yield (
                PostgresRunLifecycleLock(pool, schema=schema),
                PostgresUpdatedAtSetter(pool),
            )
        finally:
            await pool.close()


both = pytest.mark.parametrize(
    "lock_fixture", [sqlite_param, postgres_param], indirect=True
)
sqlite_only = pytest.mark.parametrize("lock_fixture", [sqlite_param], indirect=True)


@both
@pytest.mark.asyncio
async def test_create_sets_active(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    assert await lock.try_begin_resume("run-1") is None


@both
@pytest.mark.asyncio
async def test_begin_release_active_to_releasing(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    assert await lock.begin_release("run-1") is True
    assert await lock.try_begin_resume("run-1") == RunLifecycleState.releasing


@both
@pytest.mark.asyncio
async def test_begin_release_not_active_returns_false(
    lock_fixture: LockFixture,
) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")
    assert await lock.begin_release("run-1") is False


@both
@pytest.mark.asyncio
async def test_complete_release(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")
    await lock.complete_release("run-1")
    claim = await lock.try_begin_resume("run-1")
    assert isinstance(claim, ResumeClaim)
    assert claim.previous_state == RunLifecycleState.released


@both
@pytest.mark.asyncio
async def test_try_begin_resume_no_row(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    assert await lock.try_begin_resume("nonexistent") is None


@both
@pytest.mark.asyncio
async def test_try_begin_resume_active_returns_none(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    assert await lock.try_begin_resume("run-1") is None


@both
@pytest.mark.asyncio
async def test_try_begin_resume_released_transitions_to_resuming(
    lock_fixture: LockFixture,
) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")
    await lock.complete_release("run-1")

    result = await lock.try_begin_resume("run-1")
    assert isinstance(result, ResumeClaim)
    assert result.previous_state == RunLifecycleState.released
    assert await lock.try_begin_resume("run-1") == RunLifecycleState.resuming


@both
@pytest.mark.asyncio
async def test_try_begin_resume_releasing_returns_releasing(
    lock_fixture: LockFixture,
) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")
    assert await lock.try_begin_resume("run-1") == RunLifecycleState.releasing


@both
@pytest.mark.asyncio
async def test_try_begin_resume_force_resumes_on_crash_timeout(
    lock_fixture: LockFixture,
) -> None:
    lock, setter = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")

    stale_time = datetime.now(timezone.utc) - timedelta(seconds=200)
    await setter.set_updated_at("run-1", stale_time)

    result = await lock.try_begin_resume("run-1", crash_timeout_seconds=120.0)
    assert isinstance(result, ResumeClaim)
    assert result.previous_state == RunLifecycleState.releasing
    assert await lock.try_begin_resume("run-1") == RunLifecycleState.resuming


@both
@pytest.mark.asyncio
async def test_try_begin_resume_releasing_no_force_without_timeout(
    lock_fixture: LockFixture,
) -> None:
    lock, setter = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")

    stale_time = datetime.now(timezone.utc) - timedelta(seconds=200)
    await setter.set_updated_at("run-1", stale_time)

    assert await lock.try_begin_resume("run-1") == RunLifecycleState.releasing


@both
@pytest.mark.asyncio
async def test_create_is_idempotent(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")
    await lock.complete_release("run-1")
    await lock.create("run-1")
    assert await lock.try_begin_resume("run-1") is None


@both
@pytest.mark.asyncio
async def test_full_lifecycle(lock_fixture: LockFixture) -> None:
    """Test the full active -> releasing -> released -> resuming -> active cycle."""
    lock, _ = lock_fixture
    await lock.create("run-1")

    assert await lock.begin_release("run-1") is True
    assert await lock.try_begin_resume("run-1") == RunLifecycleState.releasing

    await lock.complete_release("run-1")
    claim = await lock.try_begin_resume("run-1")
    assert isinstance(claim, ResumeClaim)
    assert claim.previous_state == RunLifecycleState.released
    assert await lock.try_begin_resume("run-1") == RunLifecycleState.resuming
    assert await lock.complete_resume("run-1", claim.version) is True
    assert await lock.try_begin_resume("run-1") is None


@both
@pytest.mark.asyncio
async def test_resume_claim_fences_completion(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")
    await lock.complete_release("run-1")

    claim = await lock.try_begin_resume("run-1")
    assert isinstance(claim, ResumeClaim)

    wrong_version = datetime.now(timezone.utc) - timedelta(days=1)
    assert await lock.complete_resume("run-1", wrong_version) is False
    assert await lock.try_begin_resume("run-1") == RunLifecycleState.resuming
    assert await lock.refresh_resume_owner("run-1", wrong_version) is None
    refreshed_claim = await lock.refresh_resume_owner("run-1", claim.version)
    assert isinstance(refreshed_claim, ResumeClaim)
    assert await lock.complete_resume("run-1", claim.version) is False
    assert await lock.complete_resume("run-1", refreshed_claim.version) is True
    assert await lock.try_begin_resume("run-1") is None


@both
@pytest.mark.asyncio
async def test_stale_resuming_takeover_invalidates_old_owner(
    lock_fixture: LockFixture,
) -> None:
    lock, setter = lock_fixture
    await lock.create("run-1")
    await lock.begin_release("run-1")
    await lock.complete_release("run-1")

    old_claim = await lock.try_begin_resume("run-1")
    assert isinstance(old_claim, ResumeClaim)

    stale_time = datetime.now(timezone.utc) - timedelta(seconds=200)
    await setter.set_updated_at("run-1", stale_time)

    new_claim = await lock.try_begin_resume("run-1", crash_timeout_seconds=120.0)
    assert isinstance(new_claim, ResumeClaim)
    assert new_claim.previous_state == RunLifecycleState.resuming
    assert await lock.complete_resume("run-1", old_claim.version) is False
    assert await lock.complete_resume("run-1", new_claim.version) is True


@sqlite_only
@pytest.mark.asyncio
async def test_sqlite_concurrent_resume_claims_are_serialized(
    lock_fixture: LockFixture,
    journal_db_path: str,
) -> None:
    lock, _ = lock_fixture
    other_lock = SqliteRunLifecycleLock(db_path=journal_db_path)
    await lock.create("run-1")
    await lock.begin_release("run-1")
    await lock.complete_release("run-1")

    results = await asyncio.gather(
        lock.try_begin_resume("run-1"),
        other_lock.try_begin_resume("run-1"),
    )

    claims = [result for result in results if isinstance(result, ResumeClaim)]
    states = [result for result in results if result == RunLifecycleState.resuming]
    assert len(claims) == 1
    assert len(states) == 1


@both
@pytest.mark.asyncio
async def test_run_id_isolation(lock_fixture: LockFixture) -> None:
    lock, _ = lock_fixture
    await lock.create("run-1")
    await lock.create("run-2")
    await lock.begin_release("run-1")

    assert await lock.try_begin_resume("run-1") == RunLifecycleState.releasing
    assert await lock.try_begin_resume("run-2") is None
