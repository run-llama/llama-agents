# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from llama_agents.server import WorkflowServer
from llama_agents.server._store.postgres.migrate import run_migrations
from llama_agents.server._store.postgres_workflow_store import PostgresWorkflowStore
from server_test_fixtures import SimpleTestWorkflow
from workflows.context.serializers import JsonSerializer
from workflows.events import Event
from workflows.runtime.types.ticks import TickStepResult


@pytest.fixture
async def pg_conn() -> AsyncIterator[asyncpg.Connection]:
    dsn = os.environ.get("LLAMA_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("LLAMA_TEST_POSTGRES_DSN is not set")
    conn = await asyncpg.connect(dsn)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
def schema_names() -> tuple[str, str]:
    token = uuid.uuid4().hex
    return f"test_a_{token}", f"test_b_{token}"


async def test_each_schema_has_own_sequence_constraints(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    first, second = schema_names
    try:
        await run_migrations(pg_conn, schema=first)
        await run_migrations(pg_conn, schema=second)
        constraints = await pg_conn.fetch(
            """SELECT n.nspname, c.conname
               FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
               JOIN pg_namespace n ON n.oid = t.relnamespace
               WHERE n.nspname = ANY($1::text[]) AND c.contype = 'u'""",
            [first, second],
        )
        found = {(row["nspname"], row["conname"]) for row in constraints}
        for schema in schema_names:
            assert (schema, "uq_wf_events_run_id_sequence") in found
            assert (schema, "uq_wf_ticks_run_id_sequence") in found
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{first}" CASCADE')
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{second}" CASCADE')


async def test_legacy_migration_table_in_second_schema_upgrades(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    first, second = schema_names
    try:
        await run_migrations(pg_conn, schema=first)
        await pg_conn.execute(f'CREATE SCHEMA "{second}"')
        await pg_conn.execute(
            f'CREATE TABLE "{second}".schema_migrations (version INTEGER PRIMARY KEY)'
        )
        await run_migrations(pg_conn, schema=second)
        columns = await pg_conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name='schema_migrations'",
            second,
        )
        assert "package" in {row["column_name"] for row in columns}
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{first}" CASCADE')
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{second}" CASCADE')


async def test_migration_restores_connection_search_path(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    first, _ = schema_names
    previous = await pg_conn.fetchval("SHOW search_path")
    try:
        await run_migrations(pg_conn, schema=first)
        assert await pg_conn.fetchval("SHOW search_path") == previous
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{first}" CASCADE')


async def test_many_concurrent_event_and_tick_writers_preserve_every_sequence(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    first, _ = schema_names
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    store = PostgresWorkflowStore(dsn, schema=first, pool_min_size=2, pool_max_size=20)
    try:
        await store.start()
        count = 80
        await asyncio.gather(
            *(store.append_tick("run", {"index": i}) for i in range(count)),
            *(
                store.append_event(
                    "run", EventEnvelopeWithMetadata.from_event(Event(index=i))
                )
                for i in range(count)
            ),
        )
        ticks = await store.get_ticks("run")
        events = await store.query_events("run")
        assert [tick.sequence for tick in ticks] == list(range(count))
        assert [event.sequence for event in events] == list(range(count))
        assert {tick.tick_data["index"] for tick in ticks} == set(range(count))
    finally:
        await store.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{first}" CASCADE')


async def test_store_handles_mixed_case_schema_for_events_ticks_and_state(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    schema = schema_names[0].upper()
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    store = PostgresWorkflowStore(dsn, schema=schema)
    try:
        await store.start()
        await store.append_tick("run", {"part": "one"})
        await store.append_event(
            "run", EventEnvelopeWithMetadata.from_event(Event(part="one"))
        )
        assert [tick.tick_data for tick in await store.get_ticks("run")] == [
            {"part": "one"}
        ]
        assert len(await store.query_events("run")) == 1
        state = store.create_state_store("run")
        await state.set("value", "one")
        assert await state.get("value") == "one"
    finally:
        await store.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_upgrade_restores_missing_constraints_without_changing_existing_rows(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    schema, _ = schema_names
    try:
        await run_migrations(pg_conn, schema=schema)
        await pg_conn.execute(
            f'ALTER TABLE "{schema}".wf_events DROP CONSTRAINT uq_wf_events_run_id_sequence'
        )
        await pg_conn.execute(
            f'ALTER TABLE "{schema}".wf_ticks DROP CONSTRAINT uq_wf_ticks_run_id_sequence'
        )
        await pg_conn.execute(
            f'DELETE FROM "{schema}".schema_migrations WHERE package=$1 AND version=3',
            "server",
        )
        await pg_conn.execute(
            f'''INSERT INTO "{schema}".wf_ticks (run_id, sequence, timestamp, tick_data)
                VALUES ('run', 3, NOW(), '{{"marker": "kept"}}'::jsonb)'''
        )
        await run_migrations(pg_conn, schema=schema)
        rows = await pg_conn.fetch(
            f'SELECT run_id, sequence, tick_data FROM "{schema}".wf_ticks'
        )
        assert [(row["run_id"], row["sequence"]) for row in rows] == [("run", 3)]
        with pytest.raises(asyncpg.UniqueViolationError):
            await pg_conn.execute(
                f'''INSERT INTO "{schema}".wf_ticks (run_id, sequence, timestamp, tick_data)
                    VALUES ('run', 3, NOW(), '{{}}'::jsonb)'''
            )
        versions = await pg_conn.fetch(
            f'SELECT version FROM "{schema}".schema_migrations WHERE package=$1',
            "server",
        )
        assert {row["version"] for row in versions} == {1, 2, 3}
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.mark.parametrize("table", ["wf_events", "wf_ticks"])
async def test_duplicate_cursor_upgrade_fails_closed_without_losing_data(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str], table: str
) -> None:
    schema, _ = schema_names
    previous = await pg_conn.fetchval("SHOW search_path")
    try:
        await run_migrations(pg_conn, schema=schema)
        await pg_conn.execute(
            f'ALTER TABLE "{schema}".wf_events DROP CONSTRAINT uq_wf_events_run_id_sequence'
        )
        await pg_conn.execute(
            f'ALTER TABLE "{schema}".wf_ticks DROP CONSTRAINT uq_wf_ticks_run_id_sequence'
        )
        await pg_conn.execute(
            f'DELETE FROM "{schema}".schema_migrations WHERE package=$1 AND version=3',
            "server",
        )
        if table == "wf_events":
            await pg_conn.execute(
                f'''INSERT INTO "{schema}".wf_events (run_id, sequence, timestamp, event_json)
                    VALUES ('run', 0, NOW(), '{{"row":1}}'), ('run', 0, NOW(), '{{"row":2}}')'''
            )
        else:
            await pg_conn.execute(
                f'''INSERT INTO "{schema}".wf_ticks (run_id, sequence, timestamp, tick_data)
                    VALUES ('run', 0, NOW(), '{{"row":1}}'::jsonb), ('run', 0, NOW(), '{{"row":2}}'::jsonb)'''
            )
        with pytest.raises(
            asyncpg.RaiseError, match="duplicate run_id run, sequence 0"
        ):
            await run_migrations(pg_conn, schema=schema)
        assert await pg_conn.fetchval(f'SELECT count(*) FROM "{schema}".{table}') == 2
        assert not await pg_conn.fetchval(
            f'SELECT EXISTS (SELECT 1 FROM "{schema}".schema_migrations WHERE package=$1 AND version=3)',
            "server",
        )
        assert await pg_conn.fetchval("SHOW search_path") == previous
        constraints = await pg_conn.fetchval(
            """SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
               JOIN pg_namespace n ON n.oid=t.relnamespace
               WHERE n.nspname=$1 AND c.conname IN ('uq_wf_events_run_id_sequence','uq_wf_ticks_run_id_sequence')""",
            schema,
        )
        assert constraints == 0
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_event_and_tick_pagination_delivers_each_concurrent_write_once(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    schema, _ = schema_names
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    store = PostgresWorkflowStore(dsn, schema=schema, pool_min_size=2, pool_max_size=20)
    try:
        await store.start()
        count = 225
        await asyncio.gather(
            *(store.append_tick("run", {"index": i}) for i in range(count)),
            *(
                store.append_event(
                    "run", EventEnvelopeWithMetadata.from_event(Event(index=i))
                )
                for i in range(count)
            ),
        )
        ticks = [tick async for tick in store.stream_ticks("run")]
        assert [tick.sequence for tick in ticks] == list(range(count))
        assert {tick.tick_data["index"] for tick in ticks} == set(range(count))
        cursor = -1
        events = []
        while True:
            page = await store.query_events("run", after_sequence=cursor, limit=17)
            if not page:
                break
            events.extend(page)
            cursor = page[-1].sequence
        assert [event.sequence for event in events] == list(range(count))
    finally:
        await store.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_upgrade_legacy_version_table_in_own_schema(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    first, second = schema_names
    try:
        await run_migrations(pg_conn, schema=first)
        await run_migrations(pg_conn, schema=second)
        await pg_conn.execute(
            f'ALTER TABLE "{second}".schema_migrations DROP CONSTRAINT schema_migrations_pkey'
        )
        await pg_conn.execute(
            f'ALTER TABLE "{second}".schema_migrations DROP COLUMN package'
        )
        await pg_conn.execute(
            f'ALTER TABLE "{second}".schema_migrations ADD PRIMARY KEY(version)'
        )
        await run_migrations(pg_conn, schema=second)
        versions = await pg_conn.fetch(
            f'SELECT package, version FROM "{second}".schema_migrations ORDER BY version'
        )
        assert [(item["package"], item["version"]) for item in versions] == [
            ("server", 1),
            ("server", 2),
            ("server", 3),
        ]
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{first}" CASCADE')
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{second}" CASCADE')


@pytest.mark.parametrize(
    "schema", ["bad-name", "unsafe; DROP SCHEMA public", "1starts_with_digit"]
)
async def test_invalid_schema_cannot_change_connection_or_acquire_migration_lock(
    pg_conn: asyncpg.Connection, schema: str
) -> None:
    previous = await pg_conn.fetchval("SHOW search_path")
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        await run_migrations(pg_conn, schema=schema)
    assert await pg_conn.fetchval("SHOW search_path") == previous
    assert await pg_conn.fetchval(
        "SELECT pg_try_advisory_lock($1)", 7_201_407_233_458_173
    )
    await pg_conn.execute("SELECT pg_advisory_unlock($1)", 7_201_407_233_458_173)


async def test_default_and_explicit_public_schema_share_sequence_ownership(
    pg_conn: asyncpg.Connection,
) -> None:
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    default_store = PostgresWorkflowStore(dsn, pool_min_size=2, pool_max_size=20)
    explicit_store = PostgresWorkflowStore(
        dsn, schema="public", pool_min_size=2, pool_max_size=20
    )
    run_id = f"run_{uuid.uuid4().hex}"
    try:
        await default_store.start()
        await explicit_store.start()
        count = 80
        await asyncio.gather(
            *(
                (default_store if i % 2 == 0 else explicit_store).append_tick(
                    run_id, {"index": i}
                )
                for i in range(count)
            )
        )
        rows = await default_store.get_ticks(run_id)
        assert [row.sequence for row in rows] == list(range(count))
        assert {row.tick_data["index"] for row in rows} == set(range(count))
    finally:
        await default_store.close()
        await explicit_store.close()
        await pg_conn.execute("DELETE FROM public.wf_ticks WHERE run_id = $1", run_id)


async def test_failed_legacy_table_upgrade_rolls_back_all_ddl(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    schema, _ = schema_names
    try:
        await pg_conn.execute(f'CREATE SCHEMA "{schema}"')
        await pg_conn.execute(
            f'CREATE TABLE "{schema}".schema_migrations (version INTEGER CONSTRAINT differently_named_key PRIMARY KEY)'
        )
        with pytest.raises(asyncpg.InvalidTableDefinitionError):
            await run_migrations(pg_conn, schema=schema)
        columns = await pg_conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name='schema_migrations'",
            schema,
        )
        assert {row["column_name"] for row in columns} == {"version"}
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_oversized_schema_identifier_fails_before_postgres_truncates_it(
    pg_conn: asyncpg.Connection,
) -> None:
    prefix = f"test_long_{uuid.uuid4().hex}_" + "x" * 40
    assert len(prefix) > 63
    try:
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            await run_migrations(pg_conn, schema=prefix)
        assert not await pg_conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)", prefix[:63]
        )
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{prefix[:63]}" CASCADE')


@pytest.mark.parametrize("kind", ["event", "tick"])
async def test_cancelled_append_waiting_on_lock_leaves_no_record_or_held_lock(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str], kind: str
) -> None:
    schema, _ = schema_names
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    store = PostgresWorkflowStore(dsn, schema=schema)
    guard: asyncpg.Connection | None = None
    pending: asyncio.Task[None] | None = None
    try:
        await store.start()
        guard = await asyncpg.connect(dsn)
        assert guard is not None
        transaction = guard.transaction()
        await transaction.start()
        table = (
            f'"{schema}"."wf_events"' if kind == "event" else f'"{schema}"."wf_ticks"'
        )
        await guard.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, $2::regclass::oid::bigint))",
            "run",
            table,
        )
        if kind == "event":
            pending = asyncio.create_task(
                store.append_event("run", EventEnvelopeWithMetadata.from_event(Event()))
            )
        else:
            pending = asyncio.create_task(store.append_tick("run", {"value": 1}))

        # Wait until PostgreSQL reports a blocked advisory lock, rather than
        # relying on scheduling time or a fixed sleep.
        async def wait_for_blocked_lock() -> None:
            while True:
                waiting = await pg_conn.fetchval(
                    """SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
                       WHERE NOT l.granted AND l.locktype='advisory' AND a.datname=current_database()"""
                )
                if waiting:
                    return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_blocked_lock(), timeout=5)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await transaction.rollback()
        await guard.close()
        guard = None
        if kind == "event":
            await store.append_event(
                "run", EventEnvelopeWithMetadata.from_event(Event())
            )
            assert [row.sequence for row in await store.query_events("run")] == [0]
        else:
            await store.append_tick("run", {"value": 2})
            assert [row.sequence for row in await store.get_ticks("run")] == [0]
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        if guard is not None:
            await guard.close()
        await store.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_independent_run_and_stream_locks_do_not_block_each_other(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    schema, _ = schema_names
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    store = PostgresWorkflowStore(dsn, schema=schema)
    guard: asyncpg.Connection | None = None
    try:
        await store.start()
        guard = await asyncpg.connect(dsn)
        assert guard is not None
        transaction = guard.transaction()
        await transaction.start()
        await guard.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, $2::regclass::oid::bigint))",
            "first",
            f'"{schema}"."wf_ticks"',
        )
        # A different run and the event stream of the same run have separate
        # ownership; neither should wait on the guarded tick stream.
        await asyncio.wait_for(
            asyncio.gather(
                store.append_tick("second", {"value": 1}),
                store.append_event(
                    "first", EventEnvelopeWithMetadata.from_event(Event())
                ),
            ),
            timeout=2,
        )
        assert [row.sequence for row in await store.get_ticks("second")] == [0]
        assert [row.sequence for row in await store.query_events("first")] == [0]
        await transaction.rollback()
    finally:
        if guard is not None:
            await guard.close()
        await store.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_migration_failure_inside_caller_transaction_preserves_original_error_and_unlocks(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    schema, _ = schema_names
    previous = await pg_conn.fetchval("SHOW search_path")
    try:
        await pg_conn.execute(f'CREATE SCHEMA "{schema}"')
        # A view prevents creating the expected migration table and forces an
        # SQL error before any numbered migration transaction is entered.
        await pg_conn.execute(
            f'CREATE VIEW "{schema}".schema_migrations AS SELECT 1 AS version'
        )
        async with pg_conn.transaction():
            with pytest.raises(asyncpg.WrongObjectTypeError):
                await run_migrations(pg_conn, schema=schema)
            assert await pg_conn.fetchval("SHOW search_path") == previous
            assert await pg_conn.fetchval("SELECT 1") == 1
        assert await pg_conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", 7_201_407_233_458_173
        )
        await pg_conn.execute("SELECT pg_advisory_unlock($1)", 7_201_407_233_458_173)
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_migration_prelude_failure_rolls_back_savepoint_and_releases_lock(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    schema, _ = schema_names
    previous = await pg_conn.fetchval("SHOW search_path")
    observer = await asyncpg.connect(os.environ["LLAMA_TEST_POSTGRES_DSN"])
    try:
        await pg_conn.execute(f'CREATE SCHEMA "{schema}"')
        await pg_conn.execute(
            f"CREATE TYPE \"{schema}\".schema_migrations AS ENUM ('legacy')"
        )
        async with pg_conn.transaction():
            with pytest.raises(asyncpg.DuplicateObjectError):
                await run_migrations(pg_conn, schema=schema)
            assert await pg_conn.fetchval("SHOW search_path") == previous
            assert await pg_conn.fetchval("SELECT 1") == 1
        assert await observer.fetchval(
            "SELECT pg_try_advisory_lock($1)", 7_201_407_233_458_173
        )
        await observer.execute("SELECT pg_advisory_unlock($1)", 7_201_407_233_458_173)
    finally:
        await pg_conn.execute("SELECT pg_advisory_unlock_all()")
        await observer.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.mark.parametrize("write_then_fail", [False, True])
async def test_postgres_journal_failure_is_terminal_without_retry(
    pg_conn: asyncpg.Connection,
    schema_names: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
    write_then_fail: bool,
) -> None:
    schema, _ = schema_names
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    store = PostgresWorkflowStore(dsn, schema=schema)
    append = store.append_tick
    attempts = 0

    async def fail_result(run_id: str, tick_data: dict[str, object]) -> None:
        nonlocal attempts
        serialized = tick_data["serialized_tick"]
        assert isinstance(serialized, str)
        tick = JsonSerializer().deserialize(serialized)
        if isinstance(tick, TickStepResult):
            attempts += 1
            if write_then_fail:
                await append(run_id, tick_data)
            raise OSError("postgres tick acknowledgement failed")
        await append(run_id, tick_data)

    monkeypatch.setattr(store, "append_tick", fail_result)
    workflow = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=store, idle_timeout=60.0)
    server.add_workflow("test", workflow)
    try:
        async with server.contextmanager():
            started = await server._service.start_workflow(workflow, "handler")
            assert started.run_id is not None
            result = await asyncio.wait_for(
                server._service.await_workflow(started), timeout=3
            )
            assert result.status == "failed"
            assert result.error is not None and "acknowledgement failed" in result.error
            assert attempts == 1
            events = await store.query_events(started.run_id)
            assert events[-1].event.type == "WorkflowFailedEvent"
            stored_ticks = await store.get_ticks(started.run_id)
            stored_results = [
                item
                for item in stored_ticks
                if isinstance(
                    JsonSerializer().deserialize(item.tick_data["serialized_tick"]),
                    TickStepResult,
                )
            ]
            assert len(stored_results) == int(write_then_fail)
    finally:
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_distinct_schemas_keep_identical_run_ids_and_cursors_separate(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str]
) -> None:
    first, second = schema_names
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    left = PostgresWorkflowStore(dsn, schema=first, pool_min_size=2, pool_max_size=10)
    right = PostgresWorkflowStore(dsn, schema=second, pool_min_size=2, pool_max_size=10)
    try:
        await left.start()
        await right.start()
        count = 30
        await asyncio.gather(
            *(
                left.append_tick("shared-run", {"owner": "left", "index": i})
                for i in range(count)
            ),
            *(
                right.append_tick("shared-run", {"owner": "right", "index": i})
                for i in range(count)
            ),
            *(
                left.append_event(
                    "shared-run",
                    EventEnvelopeWithMetadata.from_event(Event(owner="left")),
                )
                for _ in range(count)
            ),
            *(
                right.append_event(
                    "shared-run",
                    EventEnvelopeWithMetadata.from_event(Event(owner="right")),
                )
                for _ in range(count)
            ),
        )
        for store, owner in [(left, "left"), (right, "right")]:
            ticks = [tick async for tick in store.stream_ticks("shared-run")]
            assert [tick.sequence for tick in ticks] == list(range(count))
            assert {tick.tick_data["owner"] for tick in ticks} == {owner}
            assert {tick.tick_data["index"] for tick in ticks} == set(range(count))
            events = await store.query_events("shared-run")
            assert [event.sequence for event in events] == list(range(count))
    finally:
        await left.close()
        await right.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{first}" CASCADE')
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{second}" CASCADE')


@pytest.mark.parametrize("kind", ["event", "tick"])
async def test_failed_postgres_insert_rolls_back_and_next_sequence_is_reusable(
    pg_conn: asyncpg.Connection, schema_names: tuple[str, str], kind: str
) -> None:
    schema, _ = schema_names
    dsn = os.environ["LLAMA_TEST_POSTGRES_DSN"]
    store = PostgresWorkflowStore(dsn, schema=schema)
    table = "wf_events" if kind == "event" else "wf_ticks"
    try:
        await store.start()
        await pg_conn.execute(
            f'''CREATE FUNCTION "{schema}".reject_insert() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'injected insert failure'; END $$'''
        )
        await pg_conn.execute(
            f'CREATE TRIGGER reject_insert BEFORE INSERT ON "{schema}".{table} '
            f'FOR EACH ROW EXECUTE FUNCTION "{schema}".reject_insert()'
        )
        with pytest.raises(asyncpg.RaiseError, match="injected insert failure"):
            if kind == "event":
                await store.append_event(
                    "run", EventEnvelopeWithMetadata.from_event(Event())
                )
            else:
                await store.append_tick("run", {"value": 1})
        await pg_conn.execute(f'DROP TRIGGER reject_insert ON "{schema}".{table}')
        if kind == "event":
            await store.append_event(
                "run", EventEnvelopeWithMetadata.from_event(Event())
            )
            assert [item.sequence for item in await store.query_events("run")] == [0]
        else:
            await store.append_tick("run", {"value": 2})
            assert [item.sequence for item in await store.get_ticks("run")] == [0]
    finally:
        await store.close()
        await pg_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
