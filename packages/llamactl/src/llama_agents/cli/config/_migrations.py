"""Ad-hoc SQLite schema migrations using PRAGMA user_version.

Inspired by https://eskerda.com/sqlite-schema-migrations-python/
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from importlib import import_module, resources
from pathlib import Path
from typing import Any, Generator

logger = logging.getLogger(__name__)


_MIGRATIONS_PKG = "llama_agents.cli.config.migrations"
_USER_VERSION_PATTERN = re.compile(r"pragma\s+user_version\s*=\s*(\d+)", re.IGNORECASE)


def _lock_file_unix(fd: int) -> None:
    """Acquire exclusive lock on Unix using fcntl."""
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_EX)


def _unlock_file_unix(fd: int) -> None:
    """Release lock on Unix using fcntl."""
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_UN)


@contextmanager
def _file_lock(lock_path: Path) -> Generator[None, None, None]:
    """File lock to serialize migrations across processes.

    Uses fcntl.flock on Unix. On Windows, SQLite's built-in locking provides
    sufficient protection for typical CLI usage patterns.
    """
    if os.name == "nt":
        # On Windows, rely on SQLite's own file locking
        yield
        return

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "w")  # noqa: SIM115
    try:
        _lock_file_unix(lock_file.fileno())
        yield
    finally:
        _unlock_file_unix(lock_file.fileno())
        lock_file.close()


def _iter_migration_files() -> list[Any]:
    """Yield packaged SQL migration files in lexicographic order."""
    pkg = import_module(_MIGRATIONS_PKG)
    root = resources.files(pkg)
    files = [p for p in root.iterdir() if p.name.endswith(".sql")]
    if not files:
        raise ValueError("No migration files found")
    return sorted(files, key=lambda p: p.name)


def _parse_target_version(sql_text: str) -> int | None:
    """Return target schema version declared in the first PRAGMA line, if any."""
    first_line = sql_text.splitlines()[0] if sql_text else ""
    match = _USER_VERSION_PATTERN.search(first_line)
    return int(match.group(1)) if match else None


def _apply_pending_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending migrations (internal, assumes lock is held)."""
    cur = conn.cursor()
    current_version_row = cur.execute("PRAGMA user_version").fetchone()
    current_version = int(current_version_row[0]) if current_version_row else 0

    for path in _iter_migration_files():
        sql_text = path.read_text()
        target_version = _parse_target_version(sql_text) or 0
        if target_version <= current_version:
            continue

        try:
            logger.debug(
                "Applying migration %s → target version %s", path.name, target_version
            )
            cur.executescript("BEGIN;\n" + sql_text)
        except Exception as exc:  # noqa: BLE001 – we surface the exact error
            logger.error("Failed migration %s: %s", path.name, exc)
            cur.execute("ROLLBACK")
            raise
        else:
            cur.execute("COMMIT")
            current_version = target_version


def run_migrations(conn: sqlite3.Connection, db_path: Path | None = None) -> None:
    """Apply pending migrations found under the migrations package.

    Each migration file should start with a `PRAGMA user_version=N;` line.
    Files are applied in lexicographic order and only when N > current_version.

    Uses a file lock to prevent concurrent migrations across processes when
    db_path is provided.
    """
    if db_path is not None:
        lock_path = db_path.with_suffix(".db.lock")
        with _file_lock(lock_path):
            _apply_pending_migrations(conn)
    else:
        _apply_pending_migrations(conn)
