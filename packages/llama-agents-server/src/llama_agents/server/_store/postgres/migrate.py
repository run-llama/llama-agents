# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import logging
import re

import asyncpg
from llama_agents.server._store import POSTGRES_MIGRATION_SOURCE
from llama_agents.server._store.migration_utils import (
    iter_migration_files,
    parse_target_version,
)

logger = logging.getLogger(__name__)

_MIGRATIONS_PKG = POSTGRES_MIGRATION_SOURCE[1]
# Arbitrary but fixed int64 used as a pg_advisory_lock key so that
# concurrent replicas serialize their migration runs.
_LOCK_ID = 7_201_407_233_458_173

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier, raising on invalid names."""
    if len(name.encode("utf-8")) > 63 or not _VALID_IDENTIFIER.fullmatch(name):
        msg = f"Invalid SQL identifier: {name!r}"
        raise ValueError(msg)
    return f'"{name}"'


async def run_migrations(
    conn: asyncpg.Connection,
    schema: str | None = None,
    sources: list[tuple[str, str]] | None = None,
) -> None:
    """Apply pending migrations found under the migrations package(s).

    Each migration file should start with a ``-- migration: N`` line.
    Files are applied in lexicographic order and only when N > current_version
    for the corresponding package.

    *sources* is a list of ``(package_name, importable_pkg)`` pairs.  When
    ``None`` it defaults to ``[("server", _MIGRATIONS_PKG)]``.

    A session-level advisory lock ensures that concurrent replicas serialize
    their migration runs so DDL and version bookkeeping never race.
    """
    if sources is None:
        sources = [("server", _MIGRATIONS_PKG)]

    await conn.execute("SELECT pg_advisory_lock($1)", _LOCK_ID)
    try:
        original_search_path = await conn.fetchval("SHOW search_path")
        try:
            await _run_migrations_locked(conn, schema, sources)
        finally:
            await conn.execute(
                "SELECT set_config('search_path', $1, false)", original_search_path
            )
    finally:
        await conn.execute("SELECT pg_advisory_unlock($1)", _LOCK_ID)


async def _run_migrations_locked(
    conn: asyncpg.Connection,
    schema: str | None,
    sources: list[tuple[str, str]],
) -> None:
    if schema:
        quoted = _quote_identifier(schema)
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted}")
        await conn.execute(f"SET search_path TO {quoted}")

    # Create the schema_migrations table with a composite PK on (package, version).
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            package TEXT NOT NULL DEFAULT 'server',
            version INTEGER NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (package, version)
        )
    """)

    # Defensive upgrade: if table exists but lacks the package column, add it.
    has_package = await conn.fetchval("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'schema_migrations' AND column_name = 'package'
        )
    """)
    if not has_package:
        async with conn.transaction():
            await conn.execute(
                "ALTER TABLE schema_migrations ADD COLUMN package TEXT NOT NULL DEFAULT 'server'"
            )
            await conn.execute(
                "ALTER TABLE schema_migrations DROP CONSTRAINT IF EXISTS schema_migrations_pkey"
            )
            await conn.execute(
                "ALTER TABLE schema_migrations ADD PRIMARY KEY (package, version)"
            )

    for package_name, source_pkg in sources:
        row = await conn.fetchval(
            "SELECT MAX(version) FROM schema_migrations WHERE package = $1",
            package_name,
        )
        current_version = row if row is not None else 0

        for path in iter_migration_files(source_pkg):
            sql_text = path.read_text()
            target_version = parse_target_version(sql_text) or 0
            if target_version <= current_version:
                continue

            try:
                logger.debug(
                    "Applying migration %s [%s] -> target version %s",
                    path.name,
                    package_name,
                    target_version,
                )
                async with conn.transaction():
                    await conn.execute(sql_text)
                    await conn.execute(
                        "INSERT INTO schema_migrations (package, version) VALUES ($1, $2)",
                        package_name,
                        target_version,
                    )
            except Exception:
                logger.error("Failed migration %s [%s]", path.name, package_name)
                raise

            current_version = target_version
