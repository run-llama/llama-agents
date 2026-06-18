# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, AsyncGenerator
from warnings import catch_warnings, simplefilter

from llama_agents.client.protocol import HandlerData
from workflows import Context, Workflow
from workflows.events import Event, StartEvent, StopEvent
from workflows.handler import WorkflowHandler
from workflows.plugins.basic import BasicRuntime
from workflows.runtime.types.plugin import Runtime
from workflows.utils import _nanoid as nanoid

from ._runtime.idle_release_runtime import IdleReleaseDecorator
from ._runtime.persistence_runtime import (
    RESUME_FRESH_HANDLER_GRACE,
    PersistenceDecorator,
    TickPersistenceDecorator,
)
from ._runtime.server_runtime import ServerRuntimeDecorator
from ._service import EventSendError, _WorkflowService
from ._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    is_terminal_status,
)
from ._store.memory_workflow_store import MemoryWorkflowStore


@dataclass(frozen=True)
class DurableWorkflowHandler:
    """In-process handle for a persisted workflow run."""

    handler_id: str
    workflow_name: str
    run_id: str
    _workflow: Workflow
    _adapter: WorkflowHandler
    _runtime: ManagedWorkflowRuntime

    async def result(self) -> Any:
        """Wait for the workflow result payload."""
        return await self._adapter

    async def stop_event_result(self) -> StopEvent:
        """Wait for the workflow `StopEvent`."""
        return await self._adapter.stop_event_result()

    async def send_event(self, event: Event, step: str | None = None) -> None:
        """Send an event to this workflow run."""
        await self._runtime.send_event(self.handler_id, event, step=step)

    async def status(self) -> PersistentHandler:
        """Load the latest persisted handler status."""
        return await self._runtime.get_handler_status(self.handler_id)

    def workflow_handler(self) -> WorkflowHandler:
        """Return the underlying workflows `WorkflowHandler`."""
        return self._adapter

    def is_done(self) -> bool:
        """Return True when the active in-process workflow task is done."""
        return self._adapter.is_done()

    def _abort(self) -> None:
        with catch_warnings():
            simplefilter("ignore", DeprecationWarning)
            self._adapter.cancel()


class ManagedWorkflowRuntime:
    """Shared durable workflow lifecycle used by HTTP and in-process runtimes."""

    def __init__(
        self,
        *,
        workflow_store: AbstractWorkflowStore | None = None,
        runtime: Runtime | None = None,
        resume_existing: bool = True,
        resume_fresh_handler_grace: timedelta | None = None,
        wait_for_resume: bool = False,
        idle_timeout: float | None = None,
        abort_active_on_stop: bool = True,
        enable_persistence: bool = True,
        start_store_before_runtime: bool = True,
        persistence_backoff: list[float] | None = None,
    ) -> None:
        if enable_persistence and idle_timeout is None and not abort_active_on_stop:
            raise ValueError(
                "abort_active_on_stop=False requires idle_timeout or a custom runtime"
            )
        self._store = (
            workflow_store if workflow_store is not None else MemoryWorkflowStore()
        )
        inner = runtime if runtime is not None else BasicRuntime()
        self._persistence: PersistenceDecorator | None = None
        self._wait_for_resume = wait_for_resume
        self._abort_active_on_stop = abort_active_on_stop
        self._start_store_before_runtime = start_store_before_runtime
        if not enable_persistence:
            managed: Runtime = inner
        else:
            persisted: TickPersistenceDecorator
            if resume_existing:
                # The HTTP server keeps a grace window for request races during ASGI
                # startup. In-process callers can disable it and wait for resume.
                self._persistence = PersistenceDecorator(
                    inner,
                    store=self._store,
                    resume_fresh_handler_grace=resume_fresh_handler_grace,
                )
                persisted = self._persistence
            else:
                persisted = TickPersistenceDecorator(inner, store=self._store)
            managed = (
                IdleReleaseDecorator(
                    persisted,
                    store=self._store,
                    idle_timeout=idle_timeout,
                )
                if idle_timeout is not None
                else persisted
            )
        self._runtime = ServerRuntimeDecorator(
            managed,
            store=self._store,
            persistence_backoff=persistence_backoff,
        )
        self._service = _WorkflowService(runtime=self._runtime, store=self._store)
        self._active_handlers: dict[str, DurableWorkflowHandler] = {}
        self._started = False

    @property
    def store(self) -> AbstractWorkflowStore:
        """The backing workflow store."""
        return self._store

    @property
    def runtime(self) -> ServerRuntimeDecorator:
        """The decorated workflow runtime."""
        return self._runtime

    @property
    def service(self) -> _WorkflowService:
        """The handler lifecycle service."""
        return self._service

    def add_workflow(self, name: str, workflow: Workflow) -> None:
        """Register a workflow under a stable name for new runs and resume."""
        self._service.add_workflow(name, workflow)

    def get_workflows(self) -> dict[str, Workflow]:
        """Return registered workflows by name."""
        return self._service.get_workflows()

    async def start(self) -> ManagedWorkflowRuntime:
        """Start the store and runtime, resuming existing runs if enabled."""
        if self._started:
            return self
        if self._start_store_before_runtime:
            await self._store.start()
        await self._service.start()
        if not self._start_store_before_runtime:
            await self._store.start()
        if (
            self._wait_for_resume
            and self._persistence is not None
            and self._persistence.resume_task is not None
        ):
            await self._persistence.resume_task
            await self._capture_resumed_handlers()
        self._started = True
        return self

    async def stop(self) -> None:
        """Stop active workflow tasks and release runtime resources."""
        if not self._started:
            return
        if self._abort_active_on_stop:
            await self.abort_active_handlers()
        self._active_handlers.clear()
        await self._service.stop()
        self._started = False

    @asynccontextmanager
    async def contextmanager(self) -> AsyncGenerator[ManagedWorkflowRuntime, None]:
        """Use this runtime as an async context manager."""
        await self.start()
        try:
            yield self
        finally:
            await self.stop()

    async def __aenter__(self) -> ManagedWorkflowRuntime:
        return await self.start()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        await self.stop()

    async def run(
        self,
        workflow_name: str,
        *,
        handler_id: str | None = None,
        start_event: StartEvent | None = None,
        context: Context | None = None,
        **start_event_kwargs: Any,
    ) -> DurableWorkflowHandler:
        """Start a workflow and return an in-process durable handler."""
        self._ensure_started()
        workflow = self._service.get_workflow(workflow_name)
        if workflow is None:
            raise ValueError(f"Workflow {workflow_name!r} is not registered")
        if start_event is not None and start_event_kwargs:
            raise ValueError("start_event cannot be combined with keyword arguments")
        if start_event_kwargs:
            start_event = workflow._get_start_event_instance(None, **start_event_kwargs)
        durable_handler_id = handler_id if handler_id is not None else nanoid()
        await self._raise_if_active_handler_exists(durable_handler_id)
        data = await self._service.start_workflow(
            workflow,
            durable_handler_id,
            start_event=start_event,
            context=context,
        )
        if data.run_id is None:
            raise RuntimeError(f"Handler {durable_handler_id!r} has no run ID")
        return self._track_handler(self._build_handler(data))

    async def get_handler_status(self, handler_id: str) -> PersistentHandler:
        """Load one persisted handler by id."""
        found = await self._store.query(HandlerQuery(handler_id_in=[handler_id]))
        if not found:
            raise KeyError(f"Handler {handler_id!r} not found")
        return found[0]

    async def query_handlers(self, query: HandlerQuery) -> list[PersistentHandler]:
        """Query persisted workflow handlers."""
        return await self._store.query(query)

    async def send_event(
        self,
        handler_id: str,
        event: Event,
        step: str | None = None,
    ) -> None:
        """Send an event to a persisted running handler."""
        self._ensure_started()
        try:
            await self._service.send_event(handler_id, event, step=step)
        except EventSendError as exc:
            raise RuntimeError(str(exc)) from exc

    async def load_active_handler(self, handler_id: str) -> DurableWorkflowHandler:
        """Return an in-memory handle for a currently active run."""
        self._ensure_started()
        persisted = await self.get_handler_status(handler_id)
        if self._is_released_handler(persisted):
            raise RuntimeError(f"Handler {handler_id!r} is not active")
        data = await self._service.load_handler(handler_id)
        if data is None:
            raise KeyError(f"Handler {handler_id!r} not found")
        if data.run_id is None:
            raise RuntimeError(f"Handler {handler_id!r} has no run ID")
        try:
            return self._track_handler(self._build_handler(data))
        except RuntimeError as exc:
            raise RuntimeError(f"Handler {handler_id!r} is not active") from exc

    def _build_handler(self, data: HandlerData) -> DurableWorkflowHandler:
        if data.run_id is None:
            raise RuntimeError(f"Handler {data.handler_id!r} has no run ID")
        workflow = self._service.get_workflow(data.workflow_name)
        if workflow is None:
            raise RuntimeError(f"Workflow {data.workflow_name!r} is not registered")
        adapter = WorkflowHandler(
            workflow=workflow,
            external_adapter=self._runtime.get_external_adapter(data.run_id),
        )
        return DurableWorkflowHandler(
            handler_id=data.handler_id,
            workflow_name=data.workflow_name,
            run_id=data.run_id,
            _workflow=workflow,
            _adapter=adapter,
            _runtime=self,
        )

    def _track_handler(self, handler: DurableWorkflowHandler) -> DurableWorkflowHandler:
        self._active_handlers[handler.handler_id] = handler
        return handler

    async def _capture_resumed_handlers(self) -> None:
        running = await self._store.query(
            HandlerQuery(status_in=["running"], is_idle=False)
        )
        for handler in running:
            if handler.run_id is None:
                continue
            if self._service.get_workflow(handler.workflow_name) is None:
                continue
            try:
                data = await self._service.load_handler(handler.handler_id)
                if data is not None:
                    self._track_handler(self._build_handler(data))
            except RuntimeError:
                continue

    def _is_released_handler(self, handler: PersistentHandler) -> bool:
        if handler.run_id is None or handler.idle_since is None:
            return False
        decorated = self._runtime._decorated
        return (
            isinstance(decorated, IdleReleaseDecorator)
            and handler.run_id not in decorated._active_run_ids
        )

    async def _raise_if_active_handler_exists(self, handler_id: str) -> None:
        found = await self._store.query(HandlerQuery(handler_id_in=[handler_id]))
        if not found:
            return
        existing = found[0]
        if not is_terminal_status(existing.status):
            raise RuntimeError(f"Handler {handler_id!r} is already running")

    async def _wait_for_aborted_handlers(self) -> None:
        for _ in range(10):
            if all(handler.is_done() for handler in self._active_handlers.values()):
                return
            await asyncio.sleep(0.05)

    async def abort_active_handlers(self) -> None:
        """Force-stop active local handlers without marking them cancelled."""
        for handler in list(self._active_handlers.values()):
            if not handler.is_done():
                self._abort_handler(handler)
        await self._wait_for_aborted_handlers()

    def _abort_handler(self, handler: DurableWorkflowHandler) -> None:
        unsupported_error: NotImplementedError | None = None
        try:
            handler._abort()
            return
        except NotImplementedError as exc:
            unsupported_error = exc
        decorated = self._runtime._decorated
        if isinstance(decorated, IdleReleaseDecorator):
            decorated._abort_inner_run(handler.run_id)
            decorated._active_run_ids.discard(handler.run_id)
            return
        assert unsupported_error is not None
        raise unsupported_error

    def _ensure_started(self) -> None:
        if not self._started:
            raise RuntimeError("ManagedWorkflowRuntime is not started")


class DurableWorkflowRuntime:
    """In-process durable workflow runtime backed by a workflow store.

    This is the non-HTTP counterpart to `WorkflowServer`: it uses the same
    handler, event, tick, and state-store persistence path, but gives callers a
    small local API instead of an ASGI app.
    """

    def __init__(
        self,
        *,
        workflow_store: AbstractWorkflowStore | None = None,
        runtime: Runtime | None = None,
        resume_existing: bool = True,
        resume_fresh_handler_grace: timedelta | None = None,
        wait_for_resume: bool = True,
        idle_timeout: float | None = 60.0,
        abort_active_on_stop: bool = True,
        persistence_backoff: list[float] | None = None,
    ) -> None:
        self._managed_runtime = ManagedWorkflowRuntime(
            workflow_store=workflow_store,
            runtime=runtime,
            resume_existing=resume_existing,
            resume_fresh_handler_grace=resume_fresh_handler_grace,
            wait_for_resume=wait_for_resume,
            idle_timeout=idle_timeout if resume_existing else None,
            abort_active_on_stop=abort_active_on_stop,
            enable_persistence=True,
            start_store_before_runtime=True,
            persistence_backoff=persistence_backoff,
        )

    @property
    def store(self) -> AbstractWorkflowStore:
        """The backing workflow store."""
        return self._managed_runtime.store

    def add_workflow(self, name: str, workflow: Workflow) -> None:
        """Register a workflow under a stable name for new runs and resume."""
        self._managed_runtime.add_workflow(name, workflow)

    def get_workflows(self) -> dict[str, Workflow]:
        """Return registered workflows by name."""
        return self._managed_runtime.get_workflows()

    async def start(self) -> DurableWorkflowRuntime:
        """Start the store and runtime, resuming existing runs if enabled."""
        await self._managed_runtime.start()
        return self

    async def stop(self) -> None:
        """Stop active workflow tasks and release runtime resources."""
        await self._managed_runtime.stop()

    @asynccontextmanager
    async def contextmanager(self) -> AsyncGenerator[DurableWorkflowRuntime, None]:
        """Use this runtime as an async context manager."""
        await self.start()
        try:
            yield self
        finally:
            await self.stop()

    async def __aenter__(self) -> DurableWorkflowRuntime:
        return await self.start()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        await self.stop()

    async def run(
        self,
        workflow_name: str,
        *,
        handler_id: str | None = None,
        start_event: StartEvent | None = None,
        context: Context | None = None,
        **start_event_kwargs: Any,
    ) -> DurableWorkflowHandler:
        """Start a workflow and return an in-process durable handler."""
        return await self._managed_runtime.run(
            workflow_name,
            handler_id=handler_id,
            start_event=start_event,
            context=context,
            **start_event_kwargs,
        )

    async def get_handler_status(self, handler_id: str) -> PersistentHandler:
        """Load one persisted handler by id."""
        return await self._managed_runtime.get_handler_status(handler_id)

    async def query_handlers(self, query: HandlerQuery) -> list[PersistentHandler]:
        """Query persisted workflow handlers."""
        return await self._managed_runtime.query_handlers(query)

    async def send_event(
        self,
        handler_id: str,
        event: Event,
        step: str | None = None,
    ) -> None:
        """Send an event to a persisted running handler."""
        await self._managed_runtime.send_event(handler_id, event, step=step)

    async def load_active_handler(self, handler_id: str) -> DurableWorkflowHandler:
        """Return an in-memory handle for a currently active run."""
        return await self._managed_runtime.load_active_handler(handler_id)


def default_resume_fresh_handler_grace() -> timedelta:
    """Return the HTTP server's default fresh-handler resume grace."""
    return RESUME_FRESH_HANDLER_GRACE
