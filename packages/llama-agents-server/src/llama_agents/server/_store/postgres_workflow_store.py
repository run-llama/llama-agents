# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import functools
import json
import logging
import weakref
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any, Sequence, cast

import asyncpg
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from workflows.context import JsonSerializer
from workflows.context.serializers import BaseSerializer

from .._pool import PoolProvider
from .abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    StoredEvent,
    StoredTick,
)
from .postgres.migrate import run_migrations as _run_migrations
from .postgres_state_store import PostgresStateStore

logger = logging.getLogger(__name__)

_TICK_PAGE_SIZE = 100

# Bounds for the LISTEN-connection reconnect backoff.
_LISTEN_RECONNECT_INITIAL_DELAY = 0.5
_LISTEN_RECONNECT_MAX_DELAY = 30.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PostgresWorkflowStore(AbstractWorkflowStore):
    """Async Postgres workflow store using asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: str,
        schema: str | None = None,
        poll_interval: float = 1.0,
        handlers_table_name: str = "wf_handlers",
        events_table_name: str = "wf_events",
        pool_min_size: int = 2,
        pool_max_size: int = 10,
        auto_migrate: bool = True,
        pool: PoolProvider | None = None,
    ) -> None:
        """Construct a PostgresWorkflowStore.

        When ``pool`` is provided, the provider controls ownership semantics.
        When it is omitted, the store owns a lazily-created asyncpg pool using
        the provided DSN and pool size settings.
        """
        super().__init__()
        self._dsn = dsn
        self._schema = schema
        self.poll_interval = poll_interval
        self._handlers_table_name = handlers_table_name
        self._events_table_name = events_table_name
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._auto_migrate = auto_migrate
        self._pool_provider = pool or PoolProvider.create(
            dsn,
            min_size=pool_min_size,
            max_size=pool_max_size,
        )
        self._pool: asyncpg.Pool | None = None
        self._listen_conn: asyncpg.Connection | None = None
        self._conditions: weakref.WeakValueDictionary[str, asyncio.Condition] = (
            weakref.WeakValueDictionary()
        )
        # LISTEN reconnect bookkeeping.
        self._closing = False
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task[None] | None = None

    @property
    def _handlers_ref(self) -> str:
        if self._schema:
            return f"{self._schema}.{self._handlers_table_name}"
        return self._handlers_table_name

    @property
    def _events_ref(self) -> str:
        if self._schema:
            return f"{self._schema}.{self._events_table_name}"
        return self._events_table_name

    @property
    def _ticks_ref(self) -> str:
        if self._schema:
            return f"{self._schema}.wf_ticks"
        return "wf_ticks"

    @property
    def _notify_channel(self) -> str:
        return self._events_table_name

    @functools.cached_property
    def _start_lock(self) -> asyncio.Lock:
        """Lazy lock initialization for Python 3.14+ compatibility."""
        return asyncio.Lock()

    async def start(self) -> None:
        """Resolve the connection pool, run migrations if enabled, and set up LISTEN.

        Safe to call concurrently: the lock plus re-check ensures exactly one
        caller resolves the pool, runs migrations, and acquires the LISTEN
        connection. ``self._pool`` is published only after everything
        succeeded, so a late joiner's fast path never observes a half-started
        store. Re-checking inside the lock (instead of latching a "started"
        flag) keeps re-start after ``close()`` working.
        """
        if self._pool is not None:
            return
        async with self._start_lock:
            if self._pool is not None:
                return
            # Reset for re-start after a close().
            self._closing = False
            pool = await self._pool_provider.get()
            if self._auto_migrate:
                await self._run_migrations_on(pool)
            await self._setup_listener(pool)
            self._pool = pool

    async def _setup_listener(self, pool: asyncpg.Pool) -> None:
        """Set up a dedicated connection for LISTEN/NOTIFY."""
        conn = cast(asyncpg.Connection, await pool.acquire())
        try:
            await conn.add_listener(self._notify_channel, self._on_notify)
            # Recover from network blips / Postgres restarts.
            try:
                conn.add_termination_listener(self._on_listen_termination)
            except AttributeError:
                # asyncpg < 0.27 (or a stub during tests) may not expose this.
                logger.debug(
                    "Connection.add_termination_listener unavailable; LISTEN reconnect disabled"
                )
        except Exception:
            await pool.release(conn)
            raise
        self._listen_conn = conn

    def _on_notify(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """Handle NOTIFY callback — schedule condition notification."""
        run_id = payload
        condition = self._conditions.get(run_id)
        if condition is not None:
            asyncio.ensure_future(self._notify_condition(condition))

    @staticmethod
    async def _notify_condition(condition: asyncio.Condition) -> None:
        async with condition:
            condition.notify_all()

    def _wake_all_subscribers(self) -> None:
        """Wake every active condition so subscribe_events re-queries.

        Used after a LISTEN reconnect — we may have missed NOTIFYs while the
        listener connection was down. The polling fallback in
        ``subscribe_events`` would catch them within ``poll_interval``; this
        just makes recovery immediate.
        """
        # Snapshot to avoid mutation-during-iteration on the WeakValueDictionary.
        for cond in list(self._conditions.values()):
            asyncio.ensure_future(self._notify_condition(cond))

    def _on_listen_termination(self, connection: asyncpg.Connection) -> None:
        """asyncpg termination callback — schedule a reconnect.

        Fires for normal close too, so we guard with ``self._closing``. The
        callback runs on asyncpg's internal task; do real work via
        ``ensure_future``.
        """
        if self._closing:
            return
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return  # already reconnecting
        try:
            self._reconnect_task = asyncio.ensure_future(self._reconnect_listener())
        except RuntimeError:
            # No running loop (e.g. termination during shutdown). Nothing to do.
            logger.debug("No running loop to schedule LISTEN reconnect")

    async def _reconnect_listener(self) -> None:
        """Reconnect the LISTEN connection with bounded exponential backoff.

        On success, wakes every subscribe_events consumer so they re-query
        immediately rather than waiting for the polling fallback.
        """
        async with self._reconnect_lock:
            if self._closing or self._pool is None:
                return
            pool = self._pool

            logger.warning("LISTEN connection dropped; reconnecting")

            # Release the dead conn back to the pool. asyncpg handles already-
            # closed connections gracefully on release.
            if self._listen_conn is not None:
                try:
                    await pool.release(self._listen_conn)
                except Exception:
                    logger.debug(
                        "Failed to release dead listen connection", exc_info=True
                    )
                self._listen_conn = None

            delay = _LISTEN_RECONNECT_INITIAL_DELAY
            while not self._closing:
                try:
                    await self._setup_listener(pool)
                except Exception:
                    logger.warning(
                        "LISTEN reconnect attempt failed; retrying in %.1fs",
                        delay,
                        exc_info=True,
                    )
                    try:
                        await asyncio.sleep(delay)
                    except asyncio.CancelledError:
                        return
                    delay = min(delay * 2, _LISTEN_RECONNECT_MAX_DELAY)
                    continue

                logger.info("LISTEN connection re-established")
                self._wake_all_subscribers()
                return

    async def close(self) -> None:
        """Tear down the LISTEN connection and close the pool (if owned)."""
        # Block any in-flight reconnect coroutine from re-installing the listener.
        self._closing = True
        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except (asyncio.CancelledError, Exception):
                pass
        self._reconnect_task = None
        if self._listen_conn is not None:
            try:
                await self._listen_conn.remove_listener(
                    self._notify_channel, self._on_notify
                )
            except Exception:
                logger.debug("Failed to remove listener during close", exc_info=True)
            try:
                await self._pool.release(self._listen_conn)  # type: ignore[union-attr]  # ty: ignore[unresolved-attribute]
            except Exception:
                logger.debug(
                    "Failed to release listen connection during close", exc_info=True
                )
            self._listen_conn = None
        # Close the provider unconditionally: if start() failed after the
        # provider resolved a pool but before self._pool was published (e.g.
        # migrations or LISTEN setup raised), the pool would otherwise leak.
        # Idempotent, and a no-op for borrowed pools.
        await self._pool_provider.close()
        self._pool = None
        # Cached facades hold the closed pool; drop them so a re-start()
        # builds fresh stores against the new pool.
        self._state_store_cache.clear()

    async def _ensure_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            await self.start()
        assert self._pool is not None
        return self._pool

    def _get_or_create_condition(self, run_id: str) -> asyncio.Condition:
        cond = self._conditions.get(run_id)
        if cond is None:
            cond = asyncio.Condition()
            self._conditions[run_id] = cond
        return cond

    def _build_state_store(
        self,
        run_id: str,
        namespace: tuple[str, ...],
        state_type: type[Any] | None,
        serializer: BaseSerializer | None,
    ) -> PostgresStateStore[Any]:
        if self._pool is None:
            raise RuntimeError(
                "PostgresWorkflowStore pool not initialized. Call start() first."
            )
        return PostgresStateStore(
            pool=self._pool,
            run_id=run_id,
            namespace=namespace,
            state_type=state_type,
            serializer=serializer,
            schema=self._schema,
        )

    # ── Migrations ──────────────────────────────────────────────────────

    async def run_migrations(self) -> None:
        """Apply file-based migrations to create/update schema."""
        pool = await self._ensure_pool()
        await self._run_migrations_on(pool)

    async def _run_migrations_on(self, pool: asyncpg.Pool) -> None:
        """Run migrations against an explicit pool (usable mid-start)."""
        async with pool.acquire() as conn:
            await _run_migrations(cast(asyncpg.Connection, conn), schema=self._schema)

    @staticmethod
    def run_migrations_sync(dsn: str, schema: str | None = None) -> None:
        """Run migrations synchronously, handling event loop detection.

        Safe to call from both sync and async contexts. When called from
        within a running event loop, runs migrations in a background thread.
        """

        async def _migrate() -> None:
            store = PostgresWorkflowStore(dsn=dsn, schema=schema)
            await store.start()
            try:
                await store.run_migrations()
            finally:
                await store.close()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(lambda: asyncio.run(_migrate())).result()
        else:
            asyncio.run(_migrate())

    # ── Handlers ────────────────────────────────────────────────────────

    async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
        filter_spec = self._build_filters(query)
        if filter_spec is None:
            return []

        clauses, params = filter_spec
        sql = f"""
            SELECT handler_id, workflow_name, status, run_id, error, result,
                   started_at, updated_at, completed_at, idle_since
            FROM {self._handlers_ref}
        """
        if clauses:
            sql = f"{sql} WHERE {' AND '.join(clauses)}"

        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        return [self._row_to_handler(row) for row in rows]

    async def update(self, handler: PersistentHandler) -> None:
        result_json = None
        if handler.result is not None:
            result_json = JsonSerializer().serialize(handler.result)

        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._handlers_ref}
                    (handler_id, workflow_name, status, run_id, error, result,
                     started_at, updated_at, completed_at, idle_since)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (handler_id) DO UPDATE SET
                    workflow_name = EXCLUDED.workflow_name,
                    status = EXCLUDED.status,
                    run_id = EXCLUDED.run_id,
                    error = EXCLUDED.error,
                    result = EXCLUDED.result,
                    started_at = EXCLUDED.started_at,
                    updated_at = EXCLUDED.updated_at,
                    completed_at = EXCLUDED.completed_at,
                    idle_since = EXCLUDED.idle_since
                """,
                handler.handler_id,
                handler.workflow_name,
                handler.status,
                handler.run_id,
                handler.error,
                result_json,
                handler.started_at,
                handler.updated_at,
                handler.completed_at,
                handler.idle_since,
            )

    async def delete(self, query: HandlerQuery) -> int:
        filter_spec = self._build_filters(query)
        if filter_spec is None:
            return 0

        clauses, params = filter_spec
        if not clauses:
            return 0

        sql = f"DELETE FROM {self._handlers_ref} WHERE {' AND '.join(clauses)}"
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(sql, *params)
            # asyncpg returns "DELETE N"
            return int(result.split()[-1])

    # ── Events ──────────────────────────────────────────────────────────

    _MAX_SEQUENCE_RETRIES = 5

    async def append_event(self, run_id: str, event: EventEnvelopeWithMetadata) -> None:
        now = _utc_now()
        event_json = event.model_dump_json()

        pool = await self._ensure_pool()
        insert_sql = f"""
            INSERT INTO {self._events_ref} (run_id, sequence, timestamp, event_json)
            VALUES (
                $1,
                COALESCE((SELECT MAX(sequence) FROM {self._events_ref} WHERE run_id = $1::varchar), -1) + 1,
                $2,
                $3
            )
        """
        # Retry on unique constraint violation from concurrent sequence assignment
        for attempt in range(self._MAX_SEQUENCE_RETRIES):
            try:
                async with pool.acquire() as conn:
                    await conn.execute(insert_sql, run_id, now, event_json)
                    await conn.execute(
                        "SELECT pg_notify($1, $2)",
                        self._notify_channel,
                        run_id,
                    )
                    return
            except asyncpg.UniqueViolationError:
                if attempt == self._MAX_SEQUENCE_RETRIES - 1:
                    raise
                logger.debug(
                    "Sequence conflict for run_id=%s, retrying (attempt %d)",
                    run_id,
                    attempt + 1,
                )

    async def query_events(
        self,
        run_id: str,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        sql = f"""
            SELECT run_id, sequence, timestamp, event_json
            FROM {self._events_ref}
            WHERE run_id = $1
        """
        params: list[Any] = [run_id]
        param_idx = 2

        if after_sequence is not None:
            sql += f" AND sequence > ${param_idx}"
            params.append(after_sequence)
            param_idx += 1

        sql += " ORDER BY sequence"

        if limit is not None:
            sql += f" LIMIT ${param_idx}"
            params.append(limit)

        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        return [
            StoredEvent(
                run_id=row["run_id"],
                sequence=row["sequence"],
                timestamp=row["timestamp"],
                event=EventEnvelopeWithMetadata.model_validate_json(row["event_json"]),
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

    # ── Ticks ──────────────────────────────────────────────────────────

    _MAX_TICK_SEQUENCE_RETRIES = 5

    async def append_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        now = _utc_now()
        tick_json = json.dumps(tick_data)

        pool = await self._ensure_pool()
        insert_sql = f"""
            INSERT INTO {self._ticks_ref} (run_id, sequence, timestamp, tick_data)
            VALUES (
                $1,
                COALESCE((SELECT MAX(sequence) FROM {self._ticks_ref} WHERE run_id = $1::varchar), -1) + 1,
                $2,
                $3::jsonb
            )
        """
        for attempt in range(self._MAX_TICK_SEQUENCE_RETRIES):
            try:
                async with pool.acquire() as conn:
                    await conn.execute(insert_sql, run_id, now, tick_json)
                    return
            except asyncpg.UniqueViolationError:
                if attempt == self._MAX_TICK_SEQUENCE_RETRIES - 1:
                    raise
                logger.debug(
                    "Tick sequence conflict for run_id=%s, retrying (attempt %d)",
                    run_id,
                    attempt + 1,
                )

    async def get_ticks(self, run_id: str) -> list[StoredTick]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id, sequence, timestamp, tick_data
                FROM {self._ticks_ref}
                WHERE run_id = $1
                ORDER BY sequence
                """,
                run_id,
            )

        return [
            StoredTick(
                run_id=row["run_id"],
                sequence=row["sequence"],
                timestamp=row["timestamp"],
                tick_data=json.loads(row["tick_data"])
                if isinstance(row["tick_data"], str)
                else row["tick_data"],
            )
            for row in rows
        ]

    async def stream_ticks(self, run_id: str) -> AsyncIterator[StoredTick]:
        pool = await self._ensure_pool()
        cursor: int | None = None
        while True:
            if cursor is None:
                sql = (
                    f"SELECT run_id, sequence, timestamp, tick_data "
                    f"FROM {self._ticks_ref} WHERE run_id = $1 "
                    f"ORDER BY sequence LIMIT $2"
                )
                params: list[Any] = [run_id, _TICK_PAGE_SIZE]
            else:
                sql = (
                    f"SELECT run_id, sequence, timestamp, tick_data "
                    f"FROM {self._ticks_ref} WHERE run_id = $1 AND sequence > $2 "
                    f"ORDER BY sequence LIMIT $3"
                )
                params = [run_id, cursor, _TICK_PAGE_SIZE]
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
            for row in rows:
                tick = StoredTick(
                    run_id=row["run_id"],
                    sequence=row["sequence"],
                    timestamp=row["timestamp"],
                    tick_data=json.loads(row["tick_data"])
                    if isinstance(row["tick_data"], str)
                    else row["tick_data"],
                )
                yield tick
                cursor = tick.sequence
            if len(rows) < _TICK_PAGE_SIZE:
                return

    # ── Helpers ─────────────────────────────────────────────────────────

    def _build_filters(self, query: HandlerQuery) -> tuple[list[str], list[Any]] | None:
        clauses: list[str] = []
        params: list[Any] = []
        param_idx = 1

        def add_in_clause(column: str, values: Sequence[str]) -> None:
            nonlocal param_idx
            placeholders = ", ".join([f"${param_idx + i}" for i in range(len(values))])
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(values)
            param_idx += len(values)

        for field, column in [
            (query.workflow_name_in, "workflow_name"),
            (query.handler_id_in, "handler_id"),
            (query.run_id_in, "run_id"),
            (query.status_in, "status"),
        ]:
            if field is not None:
                if len(field) == 0:
                    return None
                add_in_clause(column, field)

        if query.is_idle is not None:
            if query.is_idle:
                clauses.append("idle_since IS NOT NULL")
            else:
                clauses.append("idle_since IS NULL")

        return clauses, params

    @staticmethod
    def _row_to_handler(row: asyncpg.Record) -> PersistentHandler:
        return PersistentHandler(
            handler_id=row["handler_id"],
            workflow_name=row["workflow_name"],
            status=row["status"],
            run_id=row["run_id"],
            error=row["error"],
            result=json.loads(row["result"]) if row["result"] else None,
            started_at=row["started_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            idle_since=row["idle_since"],
        )
