# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""DBOS-specific runtime tests for adapter behavior.

These tests focus on the internal mechanics of the DBOS adapter,
particularly around run_id matching and state store availability.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from types import SimpleNamespace
from typing import Any, Generator, cast
from unittest.mock import patch

import asyncpg
import pytest
from dbos import DBOS, DBOSConfig
from llama_agents.dbos import DBOSRuntime
from llama_agents.dbos.journal.crud import SqliteJournalCrud
from llama_agents.dbos.journal.task_journal import TaskJournal
from llama_agents.dbos.runtime import InternalDBOSAdapter
from llama_agents.server._pool import PoolProvider
from llama_agents.server._store.postgres_state_store import PostgresStateStore
from pydantic import Field
from sqlalchemy.engine import Engine
from workflows.context import Context
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.types.named_task import WorkerTask
from workflows.testing import WorkflowTestRunner
from workflows.workflow import Workflow


def _fake_sqlite_engine() -> Engine:
    return cast(
        Engine,
        SimpleNamespace(
            dialect=SimpleNamespace(name="sqlite"),
            url=SimpleNamespace(database=":memory:"),
        ),
    )


def test_postgres_adapter_uses_resolved_pool_for_sync_state_store() -> None:
    pool = cast(asyncpg.Pool, object())

    async def factory() -> asyncpg.Pool:
        raise AssertionError("resolved pool should be used synchronously")

    adapter = InternalDBOSAdapter(
        run_id="run-1",
        engine=cast(
            Engine, SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        ),
        pool=PoolProvider.borrowed(factory),
        resolved_pool=pool,
    )

    state_store = adapter.get_state_store()

    assert isinstance(state_store, PostgresStateStore)
    assert state_store._pool is pool


@pytest.fixture(scope="module")
def dbos_config(tmp_path_factory: pytest.TempPathFactory) -> DBOSConfig:
    """Create DBOS config with a fresh SQLite database."""
    db_file = tmp_path_factory.mktemp("dbos") / "dbos_debug_test.sqlite3"
    system_db_url = f"sqlite+pysqlite:///{db_file}?check_same_thread=false"
    return {
        "name": "workflows-dbos-debug",
        "system_database_url": system_db_url,
        "run_admin_server": False,
    }  # type: ignore[return-value]


@pytest.fixture(scope="module")
def dbos_runtime(
    dbos_config: DBOSConfig,
) -> Generator[DBOSRuntime, None, None]:
    """Module-scoped DBOS runtime with fast polling for tests."""
    DBOS(config=dbos_config)
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    try:
        yield runtime
    finally:
        runtime.destroy_sync()


class DebugEvent(Event):
    captured_run_id: str = Field(default="")
    captured_dbos_workflow_id: str = Field(default="")
    state_store_available: bool = Field(default=False)


class RunIdCaptureWorkflow(Workflow):
    """Workflow that captures run_id info for debugging."""

    @step
    async def capture_ids(self, ev: StartEvent) -> StopEvent:
        dbos_workflow_id = DBOS.workflow_id or "None"
        return StopEvent(result={"dbos_workflow_id": dbos_workflow_id})


class StateStoreAccessWorkflow(Workflow):
    """Workflow that attempts to access state store."""

    @step
    async def access_store(self, ctx: Context, ev: StartEvent) -> StopEvent:
        dbos_workflow_id = DBOS.workflow_id or "None"

        try:
            await ctx.store.set("test_key", "test_value")
            value = await ctx.store.get("test_key")
            store_works = value == "test_value"
        except Exception as e:
            return StopEvent(
                result={
                    "dbos_workflow_id": dbos_workflow_id,
                    "store_works": False,
                    "error": str(e),
                }
            )

        return StopEvent(
            result={
                "dbos_workflow_id": dbos_workflow_id,
                "store_works": store_works,
            }
        )


class StateStoreCounterWorkflow(Workflow):
    """Workflow that increments a counter in state store."""

    @step
    async def increment(self, ctx: Context, ev: StartEvent) -> StopEvent:
        cur = await ctx.store.get("counter", default=0)
        await ctx.store.set("counter", cur + 1)
        return StopEvent(result=cur + 1)


@pytest.mark.asyncio
async def test_dbos_workflow_id_available(dbos_runtime: DBOSRuntime) -> None:
    """Verify DBOS.workflow_id is set inside workflow execution."""
    wf = RunIdCaptureWorkflow(runtime=dbos_runtime)
    await dbos_runtime.launch()

    r = await WorkflowTestRunner(wf).run()
    result = r.result

    assert result["dbos_workflow_id"] != "None", (
        "DBOS.workflow_id should be set inside workflow"
    )


@pytest.mark.asyncio
async def test_state_store_access_in_step(dbos_runtime: DBOSRuntime) -> None:
    """Test whether state store is accessible inside a workflow step."""
    wf = StateStoreAccessWorkflow(runtime=dbos_runtime)
    await dbos_runtime.launch()

    r = await WorkflowTestRunner(wf).run()
    result = r.result

    assert result["store_works"], (
        f"State store should be accessible. Got error: {result.get('error', 'unknown')}"
    )


@pytest.mark.asyncio
async def test_internal_adapter_run_id_matches(dbos_runtime: DBOSRuntime) -> None:
    """Verify internal adapter run_id matches DBOS.workflow_id."""
    captured_ids: dict[str, Any] = {}

    class IdTracingWorkflow(Workflow):
        @step
        async def trace_ids(self, ev: StartEvent) -> StopEvent:
            captured_ids["dbos_workflow_id"] = DBOS.workflow_id

            internal_adapter = dbos_runtime.get_internal_adapter(self)
            captured_ids["adapter_run_id"] = internal_adapter.run_id

            store = internal_adapter.get_state_store()
            captured_ids["state_store_found"] = store is not None

            return StopEvent(result="done")

    wf = IdTracingWorkflow(runtime=dbos_runtime)
    await dbos_runtime.launch()

    await WorkflowTestRunner(wf).run()

    assert captured_ids["adapter_run_id"] == captured_ids["dbos_workflow_id"], (
        f"Adapter run_id '{captured_ids['adapter_run_id']}' should match "
        f"DBOS.workflow_id '{captured_ids['dbos_workflow_id']}'"
    )
    assert captured_ids["state_store_found"], "State store should be available"


@pytest.mark.asyncio
async def test_external_run_id_vs_internal(dbos_runtime: DBOSRuntime) -> None:
    """Compare external adapter run_id with what's seen internally."""
    internal_run_id: str | None = None

    class CompareWorkflow(Workflow):
        @step
        async def capture(self, ev: StartEvent) -> StopEvent:
            nonlocal internal_run_id
            internal_run_id = DBOS.workflow_id
            return StopEvent(result="done")

    wf = CompareWorkflow(runtime=dbos_runtime)
    await dbos_runtime.launch()

    handler = wf.run()
    external_run_id = handler.run_id

    await handler

    assert external_run_id == internal_run_id, (
        f"External run_id '{external_run_id}' should match "
        f"internal DBOS.workflow_id '{internal_run_id}'"
    )


@pytest.mark.asyncio
async def test_state_store_lazy_creation(dbos_runtime: DBOSRuntime) -> None:
    """Test that state store is lazily created by the internal adapter."""
    store_info: dict[str, Any] = {}

    class LazyStoreWorkflow(Workflow):
        @step
        async def check_store(self, ctx: Context, ev: StartEvent) -> StopEvent:
            internal_adapter = dbos_runtime.get_internal_adapter(self)

            # First call should create the store
            store1 = internal_adapter.get_state_store()
            store_info["first_store_id"] = id(store1)
            store_info["first_store_exists"] = store1 is not None

            # Second call should return the same store
            store2 = internal_adapter.get_state_store()
            store_info["second_store_id"] = id(store2)
            store_info["same_store"] = store1 is store2

            # Store should work
            await ctx.store.set("lazy_key", "lazy_value")
            value = await ctx.store.get("lazy_key")
            store_info["store_works"] = value == "lazy_value"

            return StopEvent(result="done")

    wf = LazyStoreWorkflow(runtime=dbos_runtime)
    await dbos_runtime.launch()

    await WorkflowTestRunner(wf).run()

    assert store_info["first_store_exists"], "Store should be created on first access"
    assert store_info["same_store"], "Same store instance should be returned"
    assert store_info["store_works"], "Store should be functional"


@pytest.mark.asyncio
async def test_run_workflow_does_not_create_store(dbos_runtime: DBOSRuntime) -> None:
    """Verify run_workflow doesn't eagerly create a state store."""
    call_log: list[dict[str, Any]] = []
    original_run_workflow = dbos_runtime.run_workflow

    def patched_run_workflow(*args: Any, **kwargs: Any) -> Any:
        call_log.append({"run_id": kwargs.get("run_id")})
        return original_run_workflow(*args, **kwargs)

    class SimpleWf(Workflow):
        @step
        async def do_it(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="done")

    wf = SimpleWf(runtime=dbos_runtime)
    await dbos_runtime.launch()

    with patch.object(dbos_runtime, "run_workflow", patched_run_workflow):
        handler = wf.run()
        await handler

    assert len(call_log) == 1, "run_workflow should be called exactly once"


@pytest.mark.asyncio
async def test_replay_wait_for_next_task_timeout_returns_none(
    journal_db_path: str,
    sqlite_engine: Engine,
) -> None:
    """Replay wait timeout should return None and not raise."""
    run_id = "replay-timeout-run"

    crud = SqliteJournalCrud(db_path=journal_db_path)
    journal = TaskJournal(run_id, crud)
    await journal.load()
    await journal.record("step_a:0")

    adapter = InternalDBOSAdapter(
        run_id=run_id, engine=sqlite_engine, db_path=journal_db_path
    )
    task = asyncio.create_task(asyncio.sleep(5.0))

    try:
        result = await adapter.wait_for_next_task(
            [WorkerTask("step_a", 0, task)],
            [],
            timeout=0.01,
        )
        assert result.completed is None
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_async_launch_runs_dbos_launch_on_caller_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Async launch should preserve the active application loop."""
    runtime = DBOSRuntime(run_migrations_on_launch=False)
    observed: dict[str, asyncio.AbstractEventLoop | None] = {"loop": None}

    def fake_launch() -> None:
        observed["loop"] = asyncio.get_running_loop()

    fake_dbos = SimpleNamespace(launch=fake_launch, destroy=lambda: None)
    monkeypatch.setattr("llama_agents.dbos.runtime.DBOS", fake_dbos)
    monkeypatch.setattr(
        DBOSRuntime,
        "_get_sql_engine",
        lambda self: _fake_sqlite_engine(),
    )

    await runtime.launch()

    assert observed["loop"] is asyncio.get_running_loop()

    await runtime.destroy()


def test_launch_sync_offloads_dbos_launch_from_asyncio_run_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync launch should not bind DBOS to the temporary asyncio.run loop."""
    runtime = DBOSRuntime(run_migrations_on_launch=False)
    observed: dict[str, bool] = {"saw_running_loop": False}

    def fake_launch() -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            observed["saw_running_loop"] = False
        else:
            observed["saw_running_loop"] = True

    fake_dbos = SimpleNamespace(launch=fake_launch, destroy=lambda: None)
    monkeypatch.setattr("llama_agents.dbos.runtime.DBOS", fake_dbos)
    monkeypatch.setattr(
        DBOSRuntime,
        "_get_sql_engine",
        lambda self: _fake_sqlite_engine(),
    )

    runtime.launch_sync()

    assert not observed["saw_running_loop"]

    runtime.destroy_sync()


@pytest.mark.asyncio
async def test_launch_sync_raises_in_async_context() -> None:
    """Sync launch should fail loudly when called from an async context."""
    runtime = DBOSRuntime(run_migrations_on_launch=False)

    with pytest.raises(RuntimeError, match="use 'await runtime.launch\\(\\)' instead"):
        runtime.launch_sync()


def test_launch_sync_raises_with_executor_lease() -> None:
    """Executor leasing requires async launch because it owns async tasks."""
    runtime = DBOSRuntime(
        run_migrations_on_launch=False,
        _experimental_executor_lease={"pool_size": 1},
    )

    with pytest.raises(RuntimeError, match="_experimental_executor_lease"):
        runtime.launch_sync()


def test_resolve_pool_sizes_explicit_config() -> None:
    """When pool_size is set in config it wins over DBOS sys_db config."""
    runtime = DBOSRuntime(pool_size=7)
    min_size, max_size = runtime._resolve_pool_sizes()
    assert min_size == 7
    assert max_size == 7


def test_resolve_pool_sizes_explicit_min_and_max() -> None:
    runtime = DBOSRuntime(pool_size=8, pool_min_size=2)
    min_size, max_size = runtime._resolve_pool_sizes()
    assert min_size == 2
    assert max_size == 8


def test_resolve_pool_sizes_min_clamped_to_max() -> None:
    runtime = DBOSRuntime(pool_size=4, pool_min_size=10)
    min_size, max_size = runtime._resolve_pool_sizes()
    assert min_size == 4
    assert max_size == 4


def test_resolve_pool_sizes_floor_at_two() -> None:
    """pool_size=1 is bumped to 2 so the LISTEN connection doesn't starve queries."""
    runtime = DBOSRuntime(pool_size=1)
    min_size, max_size = runtime._resolve_pool_sizes()
    assert max_size == 2
    assert min_size == 2


def test_resolve_pool_sizes_falls_back_to_dbos_sys_db_pool_size() -> None:
    """Without explicit config, picks up DBOS's configured sys_db pool_size."""
    runtime = DBOSRuntime()
    fake_dbos = SimpleNamespace(
        _config={"sys_db_engine_kwargs": {"pool_size": 17}},
    )
    with patch("llama_agents.dbos.runtime._get_dbos_instance", return_value=fake_dbos):
        min_size, max_size = runtime._resolve_pool_sizes()
    assert max_size == 17
    assert min_size == 17


def test_resolve_pool_sizes_falls_back_to_constant_when_dbos_unavailable() -> None:
    """If DBOS isn't constructed, defaults to the library's fallback constant."""
    runtime = DBOSRuntime()
    with patch(
        "llama_agents.dbos.runtime._get_dbos_instance",
        side_effect=RuntimeError("not constructed"),
    ):
        min_size, max_size = runtime._resolve_pool_sizes()
    assert max_size == 10
    assert min_size == 10


def test_register_forwards_max_recovery_attempts() -> None:
    """When set, max_recovery_attempts is forwarded to @DBOS.workflow."""

    class _W(Workflow):
        @step
        async def go(self, ctx: Context, ev: StartEvent) -> StopEvent:
            return StopEvent(result="ok")

    runtime = DBOSRuntime(max_recovery_attempts=3)
    captured: dict[str, Any] = {}

    def _capture(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return lambda fn: fn

    with patch("llama_agents.dbos.runtime.DBOS.workflow", _capture):
        runtime.register(_W())
    assert captured["max_recovery_attempts"] == 3
