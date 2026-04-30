# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
DBOS Runtime for durable workflow execution.

This module provides the DBOSRuntime class for running LlamaIndex workflows
with durable execution backed by DBOS.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, AsyncGenerator, TypedDict, cast

import asyncpg
from llama_agents.client.protocol.serializable_events import (
    EventEnvelopeWithMetadata,
)
from llama_agents.dbos._store import POSTGRES_MIGRATION_SOURCE, SQLITE_MIGRATION_SOURCE
from llama_agents.server._pool import PoolProvider
from llama_agents.server._runtime.event_interceptor import EventInterceptorDecorator
from llama_agents.server._runtime.persistence_runtime import TickPersistenceDecorator
from llama_agents.server._store import (
    POSTGRES_MIGRATION_SOURCE as SERVER_POSTGRES_MIGRATION_SOURCE,
)
from llama_agents.server._store import (
    SQLITE_MIGRATION_SOURCE as SERVER_SQLITE_MIGRATION_SOURCE,
)
from llama_agents.server._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    StoredEvent,
    StoredTick,
)
from llama_agents.server._store.postgres.migrate import (
    run_migrations as pg_run_migrations,
)
from llama_agents.server._store.postgres_state_store import PostgresStateStore
from llama_agents.server._store.postgres_workflow_store import (
    PostgresWorkflowStore,
)
from llama_agents.server._store.sqlite.migrate import (
    run_migrations as sqlite_run_migrations,
)
from llama_agents.server._store.sqlite.sqlite_state_store import SqliteStateStore
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from llama_index_instrumentation import get_dispatcher
from pydantic import BaseModel
from sqlalchemy.engine import URL as SaURL
from sqlalchemy.engine import Engine
from typing_extensions import Unpack
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    StateStore,
    deserialize_state_from_dict,
    infer_state_type,
)
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.named_task import (
    NamedTask,
    PendingStart,
    all_tasks,
    find_by_key,
    get_key,
)
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    WaitForNextTaskResult,
    WaitResult,
    WaitResultTick,
    WaitResultTimeout,
)
from workflows.runtime.types.step_function import (
    StepWorkerFunction,
    as_step_worker_functions,
    create_workflow_run_function,
)
from workflows.runtime.types.ticks import WorkflowTick
from workflows.workflow import Workflow

from dbos import DBOS, SetWorkflowID, WorkflowHandleAsync
from dbos._context import get_local_dbos_context
from dbos._dbos import _get_dbos_instance
from dbos._error import DBOSNonExistentWorkflowError

from .executor_lease import ExecutorLeaseManager
from .idle_release import DBOSIdleReleaseDecorator
from .journal.crud import (
    JOURNAL_TABLE_NAME,
    JournalCrud,
    PostgresJournalCrud,
    SqliteJournalCrud,
)
from .journal.lifecycle import (
    PostgresRunLifecycleLock,
    RunLifecycleLock,
    SqliteRunLifecycleLock,
)
from .journal.task_journal import TaskJournal

STATE_TABLE_NAME = "workflow_state"

logger = logging.getLogger(__name__)


class DBOSWorkflowStore(AbstractWorkflowStore):
    """Lazy proxy that defers dialect resolution until first use.

    Wraps a factory callable that produces the real store (Postgres or Sqlite).
    The factory is called once on first access; all abstract methods delegate
    to the resolved store.
    """

    def __init__(self, factory: Callable[[], AbstractWorkflowStore]) -> None:
        self._factory = factory
        self._inner: AbstractWorkflowStore | None = None

    def _resolve(self) -> AbstractWorkflowStore:
        if self._inner is None:
            self._inner = self._factory()
        return self._inner

    @property
    def poll_interval(self) -> float:  # type: ignore[override]
        return self._resolve().poll_interval

    def create_state_store(
        self,
        run_id: str,
        state_type: type[Any] | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> StateStore[Any]:
        return self._resolve().create_state_store(
            run_id, state_type, serialized_state, serializer
        )

    async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
        return await self._resolve().query(query)

    async def update(self, handler: PersistentHandler) -> None:
        await self._resolve().update(handler)

    async def delete(self, query: HandlerQuery) -> int:
        return await self._resolve().delete(query)

    async def append_event(self, run_id: str, event: EventEnvelopeWithMetadata) -> None:
        await self._resolve().append_event(run_id, event)

    async def query_events(
        self, run_id: str, after_sequence: int | None = None, limit: int | None = None
    ) -> list[StoredEvent]:
        return await self._resolve().query_events(run_id, after_sequence, limit)

    async def append_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        await self._resolve().append_tick(run_id, tick_data)

    async def get_ticks(self, run_id: str) -> list[StoredTick]:
        return await self._resolve().get_ticks(run_id)

    def stream_ticks(self, run_id: str) -> AsyncIterator[StoredTick]:
        return self._resolve().stream_ticks(run_id)


class ExecutorLeaseConfig(TypedDict, total=False):
    """Configuration for automatic executor lease management.

    When provided, DBOSRuntime will acquire a lease from a pool of executor
    slots on launch and release it on destroy. The leased slot ID replaces
    the DBOS executor_id.
    """

    pool_size: int
    """Number of executor slots available. Required."""
    acquire_timeout: float
    """Max seconds to wait for a slot. Default 60."""
    heartbeat_interval: float
    """Seconds between heartbeats. Default 10."""
    lease_timeout: float
    """Seconds before a lease is considered stale. Default 30."""
    slot_prefix: str
    """Prefix for slot names. Default "executor"."""


class DBOSRuntimeConfig(TypedDict, total=False):
    """Configuration options for DBOSRuntime.

    All fields are optional — defaults are resolved at launch time.
    """

    polling_interval_sec: float
    run_migrations_on_launch: bool
    schema: str | None
    state_table_name: str
    journal_table_name: str
    pool_size: int
    pool_min_size: int
    max_recovery_attempts: int
    _experimental_executor_lease: ExecutorLeaseConfig | None


# Final fallback if neither config nor DBOS sys_db config can be read.
# Matches asyncpg's stock create_pool default (10) and the previous hardcoded
# value used here.
_DEFAULT_POOL_SIZE_FALLBACK = 10


DEFAULT_STATE_TABLE_NAME = STATE_TABLE_NAME
DEFAULT_JOURNAL_TABLE_NAME = JOURNAL_TABLE_NAME


def _resolve_schema(config: DBOSRuntimeConfig, engine: Engine) -> str | None:
    """Resolve schema from config, falling back to dialect-based default.

    If "schema" was explicitly provided (even as None), uses that value.
    Otherwise, defaults to "dbos" for PostgreSQL and None for SQLite.
    """
    if "schema" in config:
        return config["schema"]
    is_postgres = engine.dialect.name == "postgresql"
    return "dbos" if is_postgres else None


def _sqlalchemy_url_to_asyncpg_dsn(url: SaURL) -> str:
    """Convert a SQLAlchemy URL to an asyncpg-compatible DSN.

    Strips dialect driver suffixes (e.g. postgresql+psycopg2 -> postgresql)
    and renders the URL as a plain connection string.
    """
    # url is a sqlalchemy.engine.URL object
    # Set the drivername to plain 'postgresql' for asyncpg
    plain_url = url.set(drivername="postgresql")
    return plain_url.render_as_string(hide_password=False)


# Very long timeout for unbounded waits - encourages workflow to sleep.
# DBOS's default 60s is too short and gets recorded to event logs.
_UNBOUNDED_WAIT_TIMEOUT_SECONDS = 60 * 60 * 24  # 1 day


@DBOS.step()
def _durable_time() -> float:
    """
    Get current timestamp, wrapped as a DBOS step so that it's snapshotted and replayed
    This could be made more consistent if it got the timestamp from the DB.
    """
    return time.time()


class DBOSRuntime(Runtime):
    """
    DBOS-backed workflow runtime for durable execution.

    Workflows are registered at launch() time with stable names,
    enabling distributed workers and recovery.

    State is persisted to the database using SQL state stores,
    enabling state recovery across process restarts.
    """

    def __init__(self, **kwargs: Unpack[DBOSRuntimeConfig]) -> None:
        """Initialize the DBOS runtime.

        Args:
            **kwargs: Configuration options. See DBOSRuntimeConfig for details.
                polling_interval_sec: Interval for polling workflow results. Default 1.0.
                run_migrations_on_launch: Auto-run migrations on launch(). Default True.
                schema: Database schema name. Default: auto-detected at launch
                    ("dbos" for PostgreSQL, None for SQLite). Pass None explicitly
                    to force no schema even on PostgreSQL.
                state_table_name: State table name. Default "workflow_state".
                journal_table_name: Journal table name. Default "workflow_journal".
                pool_size: Maximum size of the asyncpg pool shared across the
                    runtime, workflow store, and (when configured) executor
                    lease manager. Defaults to DBOS's configured ``sys_db``
                    pool_size at launch, falling back to 10 when DBOS config
                    is unavailable.
                pool_min_size: Minimum size of the asyncpg pool. Defaults to
                    ``pool_size``.
                max_recovery_attempts: Forwarded to ``@DBOS.workflow``.
                    Caps how many times a workflow is replayed after a crash
                    before being marked ``MAX_RECOVERY_ATTEMPTS_EXCEEDED``.
                    Defaults to DBOS's own default when unset.
                _experimental_executor_lease: Lease-based executor identity.
                    When set, the runtime acquires a named slot from a
                    Postgres-backed pool on launch and uses it as the DBOS
                    executor_id. This replaces the need for stable hostnames
                    (e.g. from a StatefulSet) and allows plain Deployments to
                    coordinate executor identity across replicas.

                    Operational requirements:

                    - The deploying orchestrator must not run more than
                      ``pool_size`` replicas simultaneously. In Kubernetes
                      this means setting ``maxSurge: 0`` on the Deployment
                      rolling-update strategy so that new pods only start
                      after old ones terminate and release their lease.
                      Without this, new replicas block on lease acquisition
                      and never pass health checks.
                    - Scaling down below the number of replicas that hold
                      active workflows will orphan those workflows — they
                      remain assigned to an executor that no longer exists
                      and won't resume until the lease expires and another
                      replica reclaims the slot.
        """
        super().__init__()
        self.config: DBOSRuntimeConfig = dict(kwargs)  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

        # Workflow tracking state
        self._tracked_workflows: list[Workflow] = []
        self._tracked_workflow_ids: set[int] = set()  # Track by id for dedup
        self._registered: dict[int, RegisteredWorkflow] = {}  # keyed by id(workflow)

        self._dbos_launched = False
        # Signaled once DBOS is launched and config (engine, schema, etc.) is
        # resolved.  Recovery workflows on DBOS's background loop may call
        # get_internal_adapter before our launch() method returns; this event
        # lets them block briefly until the config is ready.
        self._launch_ready = threading.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._sql_engine: Engine | None = None
        self._migrations_run = False

        # Native driver resources (resolved at launch time)
        self._pool: asyncpg.Pool | None = None
        self._pool_lock: asyncio.Lock = asyncio.Lock()
        self._dsn: str | None = None  # asyncpg DSN for lazy pool creation
        self._db_path: str | None = None  # sqlite path
        self._schema: str | None = None
        self._workflow_store: AbstractWorkflowStore | None = None
        self._lease_manager: ExecutorLeaseManager | None = None
        self._lease_watch_task: asyncio.Task[None] | None = None

    def _track_task(self, task: asyncio.Task[Any]) -> None:
        self._tasks.append(task)
        task.add_done_callback(self._tasks.remove)

    def track_workflow(self, workflow: Workflow) -> None:
        """Track a workflow for registration at launch time.

        If launch() was already called, registers the workflow immediately.
        This allows late registration for testing scenarios.
        """
        if self._dbos_launched:
            # Already launched - register immediately
            registered = self.register(workflow)
            self._registered[id(workflow)] = registered
        else:
            wf_id = id(workflow)
            if wf_id not in self._tracked_workflow_ids:
                self._tracked_workflows.append(workflow)
                self._tracked_workflow_ids.add(wf_id)

    def get_registered(self, workflow: Workflow) -> RegisteredWorkflow | None:
        """Get the registered workflow if available."""
        return self._registered.get(id(workflow))

    def register(self, workflow: Workflow) -> RegisteredWorkflow:
        """
        Wrap workflow with DBOS decorators.

        Called at launch() time for each tracked workflow.
        Uses workflow.workflow_name for stable DBOS registration names.
        Idempotent: returns existing registration if already registered.
        """
        # Return existing registration if already registered
        existing = self._registered.get(id(workflow))
        if existing is not None:
            return existing

        # Use workflow's name directly
        name = workflow.workflow_name

        # Create DBOS-wrapped control loop with stable name
        wf_kwargs: dict[str, Any] = {"name": f"{name}.control_loop"}
        if "max_recovery_attempts" in self.config:
            wf_kwargs["max_recovery_attempts"] = self.config["max_recovery_attempts"]

        @DBOS.workflow(**wf_kwargs)
        async def _dbos_control_loop(
            init_state: BrokerState,
            start_event: StartEvent | None = None,
            tags: dict[str, Any] | None = None,
        ) -> StopEvent:
            if tags is None:
                tags = {}
            # Eagerly resolve the asyncpg pool so the adapter can use it
            # synchronously in get_state_store / is_replaying.
            if self._dsn is not None:
                await self._ensure_pool()
            workflow_run_fn = create_workflow_run_function(workflow)
            return await workflow_run_fn(init_state, start_event, tags)

        # Wrap steps with stable names
        wrapped_steps: dict[str, StepWorkerFunction] = {
            step_name: DBOS.step(name=f"{name}.{step_name}")(step)
            for step_name, step in as_step_worker_functions(workflow).items()
        }

        registered = RegisteredWorkflow(
            workflow=workflow, workflow_run_fn=_dbos_control_loop, steps=wrapped_steps
        )
        self._registered[id(workflow)] = registered
        return registered

    def _get_sql_engine(self) -> Engine:
        """Get the SQLAlchemy engine from DBOS for state storage.

        Uses DBOS's app database if configured, otherwise falls back to sys database.

        Returns:
            SQLAlchemy Engine for state storage.

        Raises:
            RuntimeError: If no database is available.
        """
        if self._sql_engine is not None:
            return self._sql_engine

        dbos = _get_dbos_instance()

        # Try app database first, fall back to system database
        app_db = dbos._app_db
        if app_db is not None:
            self._sql_engine = app_db.engine
            return self._sql_engine

        # Fall back to system database
        sys_db = dbos._sys_db
        self._sql_engine = sys_db.engine
        return self._sql_engine

    def _resolve_pool_sizes(self) -> tuple[int, int]:
        """Return ``(min_size, max_size)`` for the asyncpg pool.

        Resolution order for max:
          1. ``pool_size`` from DBOSRuntimeConfig.
          2. DBOS's configured sys_db pool_size, if DBOS is constructed.
          3. ``_DEFAULT_POOL_SIZE_FALLBACK``.

        Min defaults to ``pool_min_size`` if explicitly set, else equals max.
        """
        max_size = self.config.get("pool_size")
        if max_size is None:
            try:
                dbos_inst = _get_dbos_instance()
                sys_kwargs = dbos_inst._config.get("sys_db_engine_kwargs") or {}
                max_size = sys_kwargs.get("pool_size")
            except Exception:
                max_size = None
        if max_size is None:
            max_size = _DEFAULT_POOL_SIZE_FALLBACK
        # The workflow store permanently holds one connection for LISTEN/NOTIFY,
        # so the pool must have at least 2 to avoid deadlocking queries.
        if max_size < 2:
            max_size = 2

        min_size = self.config.get("pool_min_size", max_size)
        # Clamp min ≤ max defensively in case both were set.
        if min_size > max_size:
            min_size = max_size
        return min_size, max_size

    async def _ensure_pool(self) -> asyncpg.Pool:
        """Get or lazily create the asyncpg connection pool.

        Only valid for postgres dialect. Raises RuntimeError for sqlite.
        """
        if self._pool is not None:
            return self._pool
        async with self._pool_lock:
            if self._pool is not None:
                return self._pool
            if self._dsn is None:
                raise RuntimeError(
                    "No asyncpg DSN configured. Either not launched or using sqlite dialect."
                )
            min_size, max_size = self._resolve_pool_sizes()
            self._pool = await asyncpg.create_pool(
                dsn=self._dsn,
                min_size=min_size,
                max_size=max_size,
            )
            return self._pool

    async def run_migrations(self) -> None:
        """Run database migrations for all workflow tables.

        Uses the file-based migration system to create/update workflow store,
        state, and journal tables. Idempotent - safe to call multiple times.

        Can be called explicitly before launch() when run_migrations_on_launch=False,
        allowing for custom migration timing (e.g., during application startup).

        Requires DBOS to be launched first (calls _get_sql_engine internally).
        """
        if self._migrations_run:
            return

        engine = self._get_sql_engine()
        schema = _resolve_schema(self.config, engine)

        _PG_SOURCES = [
            SERVER_POSTGRES_MIGRATION_SOURCE,
            POSTGRES_MIGRATION_SOURCE,
        ]
        _SQLITE_SOURCES = [
            SERVER_SQLITE_MIGRATION_SOURCE,
            SQLITE_MIGRATION_SOURCE,
        ]

        if engine.dialect.name == "postgresql":
            dsn = _sqlalchemy_url_to_asyncpg_dsn(engine.url)
            conn = await asyncpg.connect(dsn)
            try:
                await pg_run_migrations(conn, schema=schema, sources=_PG_SOURCES)
            finally:
                await conn.close()
        else:
            db_path = str(engine.url.database) if engine.url.database else ":memory:"
            conn = sqlite3.connect(db_path)
            try:
                sqlite_run_migrations(conn, sources=_SQLITE_SOURCES)
            finally:
                conn.close()

        self._migrations_run = True
        logger.info("Database migrations completed")

    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
        adapter_state: dict[str, Any] | None = None,
    ) -> ExternalRunAdapter:
        """Set up a workflow run with SQL-backed state storage.

        State is persisted to the database, enabling recovery across
        process restarts and distributed execution.

        Args:
            run_id: Unique identifier for this workflow run.
            workflow: The workflow to run.
            init_state: Initial broker state for the control loop.
            start_event: Optional start event to kick off the workflow.
            serialized_state: Optional pre-populated state from InMemoryStateStore.to_dict().
                If provided, this state is written to the database before the workflow
                starts, allowing workflows to begin with pre-set initial values.
            serializer: Serializer for state data. Defaults to JsonSerializer.
            adapter_state: Optional adapter state (unused for DBOS).
        """
        if not self._dbos_launched:
            raise RuntimeError(
                "DBOS runtime not launched. Call runtime.launch() before running workflows."
            )

        registered = self.get_registered(workflow)
        if registered is None:
            raise RuntimeError(
                "DBOSRuntime workflows must be registered before running. Did you forget to call runtime.launch()?"
            )

        # Capture values needed in the async task closure
        active_serializer = serializer or JsonSerializer()

        async def _run_workflow() -> WorkflowHandleAsync[Any]:
            with SetWorkflowID(run_id):
                # Write initial state to DB before starting workflow (non-blocking to caller)
                if serialized_state:
                    if self._dsn is not None:
                        pool = await self._ensure_pool()
                        store: StateStore[Any] = PostgresStateStore(
                            pool=pool,
                            run_id=run_id,
                            state_type=infer_state_type(workflow),
                            serializer=active_serializer,
                            schema=self._schema,
                        )
                    elif self._db_path is not None:
                        store = SqliteStateStore(
                            db_path=self._db_path,
                            run_id=run_id,
                            state_type=infer_state_type(workflow),
                            serializer=active_serializer,
                        )
                    else:
                        raise RuntimeError("No pool or db_path configured.")
                    # Deserialize and save the initial state
                    state = deserialize_state_from_dict(
                        serialized_state,
                        active_serializer,
                        state_type=infer_state_type(workflow),
                    )
                    await store.set_state(state)

                try:
                    return await DBOS.start_workflow_async(
                        registered.workflow_run_fn,
                        init_state,
                        start_event,
                        get_dispatcher().capture_propagation_context(),
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to submit work to DBOS for {run_id} with start event: {start_event} and init state: {init_state}. Error: {e}",
                        exc_info=True,
                    )
                    raise e

        # Create startup task and pass to adapter so it can await workflow readiness
        startup_task = asyncio.create_task(_run_workflow())
        self._track_task(startup_task)

        return ExternalDBOSAdapter(
            run_id,
            self.config.get("polling_interval_sec", 1.0),
            startup_task,
        )

    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        # Wait for launch config to be ready. Recovery workflows on DBOS's
        # background loop may arrive before launch() finishes setting up.
        self._launch_ready.wait(timeout=30)
        if not self._dbos_launched:
            raise RuntimeError(
                "DBOS runtime not launched. Call runtime.launch() before running workflows."
            )
        run_id = DBOS.workflow_id
        if run_id is None:
            raise RuntimeError(
                "No current run id. Must be called within a workflow run."
            )

        # Infer state_type from the workflow for typed state support
        state_type = infer_state_type(workflow)

        engine = self._get_sql_engine()
        return InternalDBOSAdapter(
            run_id,
            engine,
            state_type,
            schema=self._schema,
            state_table_name=self.config.get(
                "state_table_name", DEFAULT_STATE_TABLE_NAME
            ),
            journal_table_name=self.config.get(
                "journal_table_name", DEFAULT_JOURNAL_TABLE_NAME
            ),
            pool=PoolProvider.borrowed(self._ensure_pool)
            if self._dsn is not None
            else None,
            resolved_pool=self._pool,
            db_path=self._db_path,
        )

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        if not self._dbos_launched:
            raise RuntimeError(
                "DBOS runtime not launched. Call runtime.launch() before running workflows."
            )
        return ExternalDBOSAdapter(run_id, self.config.get("polling_interval_sec", 1.0))

    def create_workflow_store(self) -> AbstractWorkflowStore:
        """Return the cached workflow store, creating it on first call.

        Detects the engine dialect and creates the appropriate store:
        - PostgreSQL: PostgresWorkflowStore using asyncpg with LISTEN/NOTIFY
        - SQLite: SqliteWorkflowStore using raw sqlite3

        Returns a lazy proxy so this can be called before launch(). The real
        store is resolved on first use (which happens after launch()).
        """
        if self._workflow_store is not None:
            return self._workflow_store

        def _factory() -> AbstractWorkflowStore:
            engine = self._get_sql_engine()
            schema = _resolve_schema(self.config, engine)

            if engine.dialect.name == "postgresql":
                dsn = _sqlalchemy_url_to_asyncpg_dsn(engine.url)
                logger.info(
                    "Using PostgresWorkflowStore (asyncpg) for workflow storage"
                )
                # Share the runtime's asyncpg pool — PostgresWorkflowStore
                # borrows it via the factory and never owns its lifecycle.
                return PostgresWorkflowStore(
                    dsn=dsn,
                    schema=schema,
                    pool=PoolProvider.borrowed(self._ensure_pool),
                )

            db_path = str(engine.url.database) if engine.url.database else ":memory:"
            logger.info("Using SqliteWorkflowStore for workflow storage")
            return SqliteWorkflowStore(db_path=db_path, auto_migrate=False)

        self._workflow_store = DBOSWorkflowStore(_factory)
        return self._workflow_store

    def _create_journal_crud_factory(self) -> Callable[[], JournalCrud]:
        """Create a factory for JournalCrud that resolves the database backend.

        Returns a factory rather than an instance because DB config (db_path,
        dsn, pool) isn't available until ``launch()`` runs, but the decorator
        chain is built before launch via ``build_server_runtime()``.
        """

        def _factory() -> JournalCrud:
            journal_table_name = self.config.get(
                "journal_table_name", DEFAULT_JOURNAL_TABLE_NAME
            )
            if self._db_path is not None:
                return SqliteJournalCrud(
                    db_path=self._db_path,
                    table_name=journal_table_name,
                )
            if self._pool is not None:
                return PostgresJournalCrud(
                    self._pool,
                    table_name=journal_table_name,
                    schema=self._schema,
                )
            raise RuntimeError(
                "No database configured for journal. Was launch() called?"
            )

        return _factory

    def _create_lifecycle_lock_factory(
        self,
    ) -> Callable[[], Awaitable[RunLifecycleLock]]:
        """Create an async factory for RunLifecycleLock that resolves the database backend."""

        async def _factory() -> RunLifecycleLock:
            if self._db_path is not None:
                return SqliteRunLifecycleLock(db_path=self._db_path)
            if self._dsn is not None:
                pool = await self._ensure_pool()
                return PostgresRunLifecycleLock(pool, schema=self._schema)
            raise RuntimeError(
                "No database configured for lifecycle lock. Was launch() called?"
            )

        return _factory

    def _finalize_launch(self) -> None:
        """Resolve DB-backed resources after DBOS.launch() completes."""
        engine = self._get_sql_engine()
        self._schema = _resolve_schema(self.config, engine)
        if engine.dialect.name == "postgresql":
            self._dsn = _sqlalchemy_url_to_asyncpg_dsn(engine.url)
        else:
            self._db_path = (
                str(engine.url.database) if engine.url.database else ":memory:"
            )
        self._dbos_launched = True
        self._launch_ready.set()

    async def _prepare_launch(self, *, start_lease_watch: bool) -> None:
        """Run the async setup required before calling DBOS.launch()."""
        if self._dbos_launched:
            return  # Already launched

        # Set self._dsn early when the system database is postgres so the
        # shared asyncpg pool is reachable before DBOS.launch completes
        # (executor lease pre-launch path needs this). Best-effort — if DBOS
        # isn't constructed yet (e.g. tests that monkeypatch the launch path),
        # _finalize_launch will populate self._dsn after launch completes.
        try:
            dbos_inst_pre = _get_dbos_instance()
            sys_db_url = dbos_inst_pre._config.get("system_database_url", "") or ""
            if sys_db_url and not sys_db_url.startswith("sqlite"):
                self._dsn = sys_db_url
        except Exception:
            logger.debug(
                "Could not pre-resolve DSN before DBOS construction", exc_info=True
            )

        # Acquire executor lease if configured.
        # Migrations must run first so the executor_leases table exists.
        lease_config = self.config.get("_experimental_executor_lease")
        if lease_config is not None:
            pool_size = lease_config.get("pool_size")
            if pool_size is None:
                raise ValueError("_experimental_executor_lease.pool_size is required")

            # Get DSN from DBOS config before launch
            dbos_inst = _get_dbos_instance()
            dsn = dbos_inst._config.get("system_database_url", "")
            if not dsn or dsn.startswith("sqlite"):
                raise ValueError(
                    "Executor leasing requires a PostgreSQL system_database_url in DBOS config"
                )

            schema = self.config.get("schema", "dbos") or "dbos"

            # Run migrations before lease acquisition so the table exists
            if self.config.get("run_migrations_on_launch", True):
                conn = await asyncpg.connect(dsn)
                try:
                    await pg_run_migrations(
                        conn,
                        schema=schema,
                        sources=[
                            SERVER_POSTGRES_MIGRATION_SOURCE,
                            POSTGRES_MIGRATION_SOURCE,
                        ],
                    )
                finally:
                    await conn.close()
                self._migrations_run = True
                logger.info("Database migrations completed (pre-lease)")

            self._lease_manager = ExecutorLeaseManager(
                pool=PoolProvider.borrowed(self._ensure_pool),
                pool_size=pool_size,
                heartbeat_interval=lease_config.get("heartbeat_interval", 10.0),
                lease_timeout=lease_config.get("lease_timeout", 30.0),
                slot_prefix=lease_config.get("slot_prefix", "executor"),
                schema=schema,
            )
            acquire_timeout = lease_config.get("acquire_timeout", 60.0)
            await self._lease_manager.acquire(timeout=acquire_timeout)

            # Reinitialize DBOS with the leased executor_id.
            # DBOS is a singleton and hasn't been launched yet, so reinit is safe.
            config = dict(dbos_inst._config)
            config["executor_id"] = self._lease_manager.executor_id
            DBOS(config=cast(Any, config))
            logger.info("Acquired executor lease: %s", self._lease_manager.executor_id)
            if start_lease_watch:
                self._lease_watch_task = asyncio.create_task(self._watch_lease())

        # Register each pending workflow with DBOS
        for workflow in self._tracked_workflows:
            # Register with DBOS (this applies decorators)
            registered = self.register(workflow)
            self._registered[id(workflow)] = registered

    async def _post_launch(self) -> None:
        """Run async work after DBOS.launch() has captured its target context."""
        # Run migrations after DBOS is launched (if configured)
        if self.config.get("run_migrations_on_launch", True):
            await self.run_migrations()

    def build_server_runtime(self, *, idle_timeout: float = 600.0) -> Runtime:
        """Build the decorator chain for use with WorkflowServer.

        Wraps the DBOS runtime with:
        - TickPersistenceDecorator (persists ticks to workflow store)
        - EventInterceptorDecorator (blocks events from reaching DBOS streams)
        - DBOSIdleReleaseDecorator (releases idle workflows after timeout)

        Chain order (outermost first):
        DBOSIdleReleaseDecorator → EventInterceptorDecorator → TickPersistenceDecorator → DBOSRuntime

        Args:
            idle_timeout: Seconds to wait after a workflow becomes idle before
                releasing it. Defaults to 10 minutes.

        The returned runtime should be passed as the ``runtime`` argument
        to ``WorkflowServer``.
        """
        store = self.create_workflow_store()
        tick_persistence = TickPersistenceDecorator(self, store)
        return DBOSIdleReleaseDecorator(
            EventInterceptorDecorator(tick_persistence),
            store=store,
            idle_timeout=idle_timeout,
            journal_crud=self._create_journal_crud_factory(),
            lifecycle_lock=self._create_lifecycle_lock_factory(),
        )

    async def launch(self) -> None:
        """
        Launch DBOS and register all tracked workflows.

        Must be called before running any workflows.
        Runs database migrations unless run_migrations_on_launch=False.
        If ``_experimental_executor_lease`` is set in the config, acquires a
        lease slot first.
        """
        await self._prepare_launch(start_lease_watch=True)
        if self._dbos_launched:
            return
        # Async server startup should keep DBOS on the caller's live loop so
        # startup recovery and later HTTP handlers share the same long-lived
        # application event loop.
        DBOS.launch()
        self._finalize_launch()
        await self._post_launch()

    def launch_sync(self) -> None:
        """Launch DBOS from synchronous code without capturing asyncio.run()'s loop."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError(
                "DBOSRuntime.launch_sync() cannot be called from an async context; "
                "use 'await runtime.launch()' instead."
            )

        if self.config.get("_experimental_executor_lease") is not None:
            raise RuntimeError(
                "DBOSRuntime.launch_sync() does not support "
                "'_experimental_executor_lease'; use 'await runtime.launch()' instead."
            )

        asyncio.run(self._prepare_launch(start_lease_watch=False))
        if self._dbos_launched:
            return
        DBOS.launch()
        self._finalize_launch()
        asyncio.run(self._post_launch())

    @property
    def is_launched(self) -> bool:
        return self._dbos_launched

    @property
    def lease_lost_event(self) -> asyncio.Event | None:
        """Event that is set when the executor lease is lost.

        Returns None if executor leasing is not configured.
        """
        if self._lease_manager is None:
            return None
        return self._lease_manager.lease_lost_event

    async def _watch_lease(self) -> None:
        assert self._lease_manager is not None
        await self._lease_manager.lease_lost_event.wait()
        logger.critical("Executor lease lost — shutting down runtime")
        await self.destroy()

    async def destroy(self, destroy_dbos: bool = True) -> None:
        """Clean up DBOS runtime resources.

        Args:
            destroy_dbos: If True (default), also calls DBOS.destroy().
                Set to False when DBOS lifecycle is managed externally
                (e.g., shared across multiple runtimes in tests).
        """
        if not self._dbos_launched:
            return  # Already destroyed or never launched

        # Cancel lease watcher first to avoid re-entrant destroy
        if self._lease_watch_task is not None:
            self._lease_watch_task.cancel()
            try:
                await self._lease_watch_task
            except asyncio.CancelledError:
                pass
            self._lease_watch_task = None

        if self._lease_manager is not None:
            await self._lease_manager.release()
            self._lease_manager = None

        self._tracked_workflows.clear()
        self._tracked_workflow_ids.clear()
        self._registered.clear()
        self._dbos_launched = False
        self._launch_ready = threading.Event()
        self._sql_engine = None
        self._migrations_run = False
        self._dsn = None
        self._db_path = None
        self._schema = None
        # Shut down the workflow store's listener before terminating the pool.
        # Otherwise pool.terminate() kills the LISTEN connection, which fires
        # _on_listen_termination and spawns a reconnect task during shutdown.
        if self._workflow_store is not None:
            inner = (
                self._workflow_store._inner
                if isinstance(self._workflow_store, DBOSWorkflowStore)
                else self._workflow_store
            )
            if isinstance(inner, PostgresWorkflowStore):
                await inner.close()
            self._workflow_store = None
        if self._pool is not None:
            try:
                self._pool.terminate()
            except Exception:
                logger.debug(
                    "Failed to terminate asyncpg pool during destroy", exc_info=True
                )
            self._pool = None
        # Wait for cancelled tasks to unwind before destroying DBOS.
        tasks_to_cancel = [t for t in self._tasks if not t.done()]
        for task in tasks_to_cancel:
            task.cancel()
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        if destroy_dbos:
            DBOS.destroy()


_IO_STREAM_PUBLISHED_EVENTS_NAME = "published_events"
_IO_STREAM_TICK_TOPIC = "ticks"


class InternalDBOSAdapter(InternalRunAdapter):
    """
    Internal DBOS adapter for the workflow control loop.

    - send_event sends ticks via DBOS.send (using run_in_executor to escape step context)
    - wait_receive receives ticks via DBOS.recv_async
    - write_to_event_stream publishes events via DBOS streams
    - get_now returns a durable timestamp
    - close sends shutdown signal to wake blocked recv
    - wait_for_next_task coordinates task completion ordering for deterministic replay
    """

    def __init__(
        self,
        run_id: str,
        engine: Engine,
        state_type: type[BaseModel] | None = None,
        schema: str | None = None,
        state_table_name: str = DEFAULT_STATE_TABLE_NAME,
        journal_table_name: str = DEFAULT_JOURNAL_TABLE_NAME,
        pool: PoolProvider | None = None,
        resolved_pool: asyncpg.Pool | None = None,
        db_path: str | None = None,
    ) -> None:
        self._run_id = run_id
        self._engine = engine
        self._state_type = state_type
        self._schema = schema
        self._state_table_name = state_table_name
        self._journal_table_name = journal_table_name
        self._pool_provider = pool
        self._resolved_pool = resolved_pool
        self._db_path = db_path
        self._closed = False
        self._shutdown_event = asyncio.Event()
        self._state_store: StateStore[Any] | None = None
        # Journal for deterministic task ordering - lazily initialized
        self._journal: TaskJournal | None = None
        self._orphan_purge_done = False

    @property
    def run_id(self) -> str:
        return self._run_id

    async def write_to_event_stream(self, event: Event) -> None:
        await DBOS.write_stream_async(_IO_STREAM_PUBLISHED_EVENTS_NAME, event)

    async def get_now(self) -> float:
        return _durable_time()

    async def send_event(self, tick: WorkflowTick) -> None:
        # Use run_in_executor to escape DBOS step context.
        # DBOS yells at you for writing to the event stream from a step (since is not idempotent)
        # However that's the expected semantics of llama index workflow steps, so it's ok.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: DBOS.send(self._run_id, tick, topic=_IO_STREAM_TICK_TOPIC),
        )

    async def wait_receive(
        self,
        timeout_seconds: float | None = None,
    ) -> WaitResult:
        """Wait for tick via DBOS.recv_async. Raises CancelledError on shutdown."""
        if self._closed:
            raise asyncio.CancelledError("Adapter closed")

        recv_task = asyncio.ensure_future(
            DBOS.recv_async(
                _IO_STREAM_TICK_TOPIC,
                timeout_seconds=timeout_seconds or _UNBOUNDED_WAIT_TIMEOUT_SECONDS,
            )
        )
        shutdown_task = asyncio.ensure_future(self._shutdown_event.wait())
        try:
            done, _ = await asyncio.wait(
                {recv_task, shutdown_task}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            recv_task.cancel()
            shutdown_task.cancel()
            raise

        if shutdown_task in done:
            recv_task.cancel()
            raise asyncio.CancelledError("Adapter closed")

        shutdown_task.cancel()
        result = recv_task.result()
        if result is None:
            return WaitResultTimeout()
        return WaitResultTick(tick=result)

    async def close(self) -> None:
        """Signal shutdown using process-local event to wake blocked recv."""
        if self._closed:
            return
        self._closed = True
        self._shutdown_event.set()

    async def _resolve_pool(self) -> asyncpg.Pool:
        """Resolve the asyncpg pool, lazily creating it via the runtime callback."""
        if self._resolved_pool is not None:
            return self._resolved_pool
        if self._pool_provider is None:
            raise RuntimeError(
                "No asyncpg pool configured. Either not launched or using sqlite dialect."
            )
        self._resolved_pool = await self._pool_provider.get()
        return self._resolved_pool

    def _get_or_create_state_store(self) -> StateStore[Any]:
        """Get or lazily create the state store.

        For PostgreSQL, the pool must be resolved first via _resolve_pool().
        Call _ensure_resources() before accessing the state store.
        """
        if self._state_store is None:
            if self._resolved_pool is not None:
                self._state_store = PostgresStateStore(
                    pool=self._resolved_pool,
                    run_id=self._run_id,
                    state_type=cast(type[Any], self._state_type),
                    schema=self._schema,
                )
            elif self._db_path is not None:
                self._state_store = SqliteStateStore(
                    db_path=self._db_path,
                    run_id=self._run_id,
                    state_type=cast(type[Any], self._state_type),
                )
            else:
                raise RuntimeError(
                    "No pool or db_path configured for state store. "
                    "Ensure the runtime pool is initialized before accessing state."
                )
        return self._state_store

    def get_state_store(self) -> StateStore[Any] | None:
        return self._get_or_create_state_store()

    def is_replaying(self) -> bool:
        if (
            self._journal is None
            and self._resolved_pool is None
            and self._db_path is None
        ):
            return False
        journal = self._get_or_create_journal()
        return journal.is_replaying()

    def _get_or_create_journal(self) -> TaskJournal:
        """Get or lazily create the task journal."""
        if self._journal is None:
            if self._resolved_pool is not None:
                crud = PostgresJournalCrud(
                    pool=self._resolved_pool,
                    table_name=self._journal_table_name,
                    schema=self._schema,
                )
            elif self._db_path is not None:
                crud = SqliteJournalCrud(
                    db_path=self._db_path,
                    table_name=self._journal_table_name,
                )
            else:
                raise RuntimeError("No pool or db_path configured for journal.")
            self._journal = TaskJournal(self._run_id, crud)
        return self._journal

    async def _purge_orphaned_operations(self, journal: TaskJournal) -> None:
        """Purge orphaned operation_outputs beyond the current fid.

        Called once at the replay→fresh transition to remove stale rows left by
        a previous crashed recovery. Also truncates stale journal entries.
        """
        if self._orphan_purge_done:
            return
        self._orphan_purge_done = True

        if not journal.has_entries:
            return

        ctx = get_local_dbos_context()
        assert ctx is not None, "Expected DBOS context during workflow execution"
        current_fid = ctx.function_id

        await journal.purge_stale(current_fid)

        logger.debug(
            "Purged orphaned operation_outputs for %s beyond fid %d",
            self._run_id,
            current_fid,
        )

    async def wait_for_next_task(
        self,
        running: list[NamedTask],
        pending: list[PendingStart],
        timeout: float | None = None,
    ) -> WaitForNextTaskResult:
        """Wait for and return the next task that should complete.

        Starts each pending coroutine with an ``asyncio.sleep(0)`` yield between
        them so that every task's synchronous preamble (including DBOS function_id
        acquisition) runs in deterministic order.

        During replay, waits for the specific task that completed in the original run.
        During fresh execution, waits for any task and records the completion order.

        Args:
            running: Already-started tasks from previous iterations.
            pending: Coroutines to start this iteration.
            timeout: Timeout in seconds, None for no timeout.

        Returns:
            WaitForNextTaskResult with completed task and newly started NamedTasks.
        """
        # Resolve pool before journal creation (needed for postgres)
        if self._pool_provider is not None and self._resolved_pool is None:
            await self._resolve_pool()

        # Load journal before starting pending coroutines so the orphan purge
        # runs before new fids are consumed.
        journal = self._get_or_create_journal()
        await journal.load()
        expected_key = journal.next_expected_key()

        if expected_key is None and not self._orphan_purge_done:
            await self._purge_orphaned_operations(journal)

        # Start each pending coroutine with a yield between each to ensure
        # deterministic function_id ordering for DBOS replay.
        started: list[NamedTask] = []
        for p in pending:
            started.append(p.start(asyncio.create_task(p.coro)))
            await asyncio.sleep(0)

        all_named = running + started
        tasks = all_tasks(all_named)
        if not tasks:
            return WaitForNextTaskResult(None, started)

        if expected_key is not None:
            # Replay mode: wait for specific task
            target_task = find_by_key(all_named, expected_key)

            if target_task is None:
                logger.warning(
                    f"Non-deterministic execution detected during replay! "
                    f"Expected task {expected_key} not in set yet. "
                    f"Falling back to awaiting all tasks."
                )
            else:
                try:
                    await asyncio.wait_for(asyncio.shield(target_task), timeout=timeout)
                except (asyncio.TimeoutError, TimeoutError):
                    return WaitForNextTaskResult(None, started)
                journal.advance()
                return WaitForNextTaskResult(target_task, started)

        # Fresh execution: wait for first, record it
        done, _ = await asyncio.wait(
            tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if not done:
            return WaitForNextTaskResult(None, started)

        completed = done.pop()
        key = get_key(all_named, completed)
        await journal.record(key)

        return WaitForNextTaskResult(completed, started)


class ExternalDBOSAdapter(ExternalRunAdapter):
    """
    External DBOS adapter for workflow interaction.

    - send_event puts ticks into the shared mailbox queue
    - stream_published_events reads from DBOS streams
    - close is a no-op
    """

    def __init__(
        self,
        run_id: str,
        polling_interval_sec: float = 1.0,
        startup_task: asyncio.Task[WorkflowHandleAsync[Any]] | None = None,
    ) -> None:
        self._run_id = run_id
        self._polling_interval_sec = polling_interval_sec
        self._startup_task = startup_task  # None means workflow already started
        self._handle: WorkflowHandleAsync[Any] | None = None

    @property
    def run_id(self) -> str:
        """Get the workflow run ID."""
        return self._run_id

    async def send_event(self, tick: WorkflowTick) -> None:
        await self._ensure_workflow_started()
        await DBOS.send_async(self._run_id, tick, topic=_IO_STREAM_TICK_TOPIC)

    async def stream_published_events(self) -> AsyncGenerator[Event, None]:
        await self._ensure_workflow_started()

        async for event in DBOS.read_stream_async(self.run_id, "published_events"):
            yield event

    async def get_result(self) -> StopEvent:
        handle = await self._ensure_workflow_started()
        return await handle.get_result(polling_interval_sec=self._polling_interval_sec)

    async def _ensure_workflow_started(self) -> WorkflowHandleAsync[Any]:
        """Wait for the workflow startup task to complete and return the handle."""
        if self._startup_task is not None:
            self._handle = await self._startup_task
            self._startup_task = None  # Clear after awaiting
        if self._handle is None:
            # Fallback: workflow was started elsewhere, retrieve with retry since
            # there can be a race between start_workflow_async completing and the
            # workflow becoming retrievable in DBOS.

            for attempt in range(20):
                try:
                    self._handle = await DBOS.retrieve_workflow_async(self.run_id)
                    break
                except DBOSNonExistentWorkflowError:
                    if attempt == 19:
                        raise
                    await asyncio.sleep(0.1 * (attempt + 1))
        assert self._handle is not None
        return self._handle
