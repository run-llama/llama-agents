# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from llama_agents.dbos._store import SQLITE_MIGRATION_SOURCE
from llama_agents.server._store import (
    SQLITE_MIGRATION_SOURCE as SERVER_SQLITE_MIGRATION_SOURCE,
)
from llama_agents.server._store.sqlite.migrate import (
    run_migrations as sqlite_run_migrations,
)
from llama_agents_integration_tests.postgres import (
    get_asyncpg_dsn,
)
from llama_agents_integration_tests.postgres import (
    postgres_container as _postgres_container,
)
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool
from testcontainers.postgres import PostgresContainer

_SQLITE_SOURCES = [
    SERVER_SQLITE_MIGRATION_SOURCE,
    SQLITE_MIGRATION_SOURCE,
]


@pytest.fixture
def journal_db_path() -> Generator[str]:
    """Create a temporary SQLite database with both server and DBOS migrations."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test.db")
        conn = sqlite3.connect(db_path)
        try:
            sqlite_run_migrations(conn, sources=_SQLITE_SOURCES)
        finally:
            conn.close()
        yield db_path


@pytest.fixture
def sqlite_engine(journal_db_path: str) -> Engine:
    """Create a SQLAlchemy engine pointing at the migrated test database."""
    engine = create_engine(
        f"sqlite:///{journal_db_path}",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    return engine


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Module-scoped PostgreSQL container for docker-marked tests."""
    with _postgres_container() as pg:
        yield pg


@pytest.fixture(scope="module")
def postgres_dsn(postgres_container: PostgresContainer) -> str:
    """Return a plain postgresql:// DSN suitable for asyncpg."""
    return get_asyncpg_dsn(postgres_container)


RUNNER_PATH = str(Path(__file__).parent / "fixtures" / "runner.py")


def run_scenario(
    workflow: str,
    db_url: str,
    run_id: str,
    config: dict[str, Any] | None = None,
    call_close: bool = False,
    timeout: float = 45.0,
) -> subprocess.CompletedProcess[str]:
    """Run a workflow scenario in a subprocess via runner.py."""
    cmd = [
        sys.executable,
        RUNNER_PATH,
        "--workflow",
        workflow,
        "--db-url",
        db_url,
        "--run-id",
        run_id,
    ]
    if config:
        cmd.extend(["--config", json.dumps(config)])
    if call_close:
        cmd.append("--call-close")
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        stderr = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        msg = f"Subprocess timed out after {timeout}s\nstdout:\n{stdout}\nstderr:\n{stderr}"
        pytest.fail(msg)
        raise AssertionError("unreachable")  # noqa: B904


def assert_no_determinism_errors(result: subprocess.CompletedProcess[str]) -> None:
    """Check subprocess result for crashes and DBOS determinism errors."""
    combined = result.stdout + result.stderr

    if result.returncode != 0:
        msg = f"Subprocess exited with code {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        pytest.fail(msg)

    if "Traceback (most recent call last)" in result.stdout:
        msg = f"Subprocess exception!\nstdout: {result.stdout}\nstderr: {result.stderr}"
        pytest.fail(msg)

    if "DBOSUnexpectedStepError" in combined or "Error 11" in combined:
        msg = f"DBOS determinism error on resume!\nstdout: {result.stdout}\nstderr: {result.stderr}"
        pytest.fail(msg)


def log_on_failure(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode != 0:
        print(f"\n=== {label} FAILED ===")
        print(f"stdout: {result.stdout}")
        print(f"stderr: {result.stderr}")
