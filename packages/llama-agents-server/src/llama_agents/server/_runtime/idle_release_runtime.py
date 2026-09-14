# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""IdleReleaseDecorator and supporting adapters.

Wraps a PersistenceDecorator to add idle detection, memory release, and
reload-on-demand for idle workflow handlers.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import Any

from typing_extensions import override
from workflows.context.serializers import BaseSerializer
from workflows.events import (
    Event,
    StartEvent,
    WorkflowIdleEvent,
)
from workflows.runtime.runtime_decorators import (
    BaseExternalRunAdapterDecorator,
    BaseInternalRunAdapterDecorator,
    BaseRuntimeDecorator,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    V2RuntimeCompatibilityShim,
)
from workflows.runtime.types.ticks import WorkflowTick
from workflows.workflow import Workflow

from .._keyed_lock import KeyedLock
from .._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
)
from .persistence_runtime import TickPersistenceDecorator

logger = logging.getLogger(__name__)


class _IdleReleaseInternalRunAdapter(BaseInternalRunAdapterDecorator):
    """Internal adapter that detects idle events and schedules release."""

    def __init__(
        self,
        decorated: InternalRunAdapter,
        runtime: IdleReleaseDecorator,
        store: AbstractWorkflowStore,
    ) -> None:
        super().__init__(decorated)
        self._runtime = runtime
        self._store = store

    @override
    async def write_to_event_stream(self, event: Event) -> None:
        if isinstance(event, WorkflowIdleEvent):
            idle_since = datetime.now(timezone.utc)
            await self._store.update_handler_status(
                self.run_id, status="running", idle_since=idle_since
            )
        await super().write_to_event_stream(event)
        if isinstance(event, WorkflowIdleEvent):
            self._runtime._spawn_task(self._runtime._deferred_release(self.run_id))


class IdleReleaseExternalRunAdapter(BaseExternalRunAdapterDecorator):
    """Proxy adapter that adds reload-on-demand for idle-released handlers.

    The inner adapter is resolved lazily via a property because
    ``get_external_adapter`` is sync but reload is async — the inner run
    may not exist yet when this adapter is constructed.
    """

    def __init__(self, runtime: IdleReleaseDecorator, run_id: str) -> None:
        # Intentionally skip super().__init__ — _decorated is a lazy property.
        self._runtime = runtime
        self._run_id = run_id

    @property  # type: ignore[override]
    def _decorated(self) -> ExternalRunAdapter:
        return self._runtime._decorated.get_external_adapter(self._run_id)

    @_decorated.setter
    def _decorated(self, value: ExternalRunAdapter) -> None:
        pass

    @property
    def run_id(self) -> str:
        return self._run_id

    @override
    async def send_event(self, tick: WorkflowTick) -> None:
        async with self._runtime._reload_lock(self.run_id):
            if self.run_id not in self._runtime._active_run_ids:
                await self._runtime._ensure_active_run_locked(self.run_id)
            else:
                await self._runtime._store.update_handler_status(
                    self.run_id, idle_since=None
                )
            await self._decorated.send_event(tick)


class IdleReleaseDecorator(BaseRuntimeDecorator):
    """Runtime decorator for idle detection, memory release, and reload-on-demand.

    Must wrap a TickPersistenceDecorator (or compatible runtime) to access
    context_from_ticks for reloading released handlers.
    """

    def __init__(
        self,
        decorated: TickPersistenceDecorator,
        store: AbstractWorkflowStore,
        idle_timeout: float = 60.0,
    ) -> None:
        super().__init__(decorated)
        self._store = store
        self._persistence: TickPersistenceDecorator = decorated
        self._reload_lock = KeyedLock()
        self._active_run_ids: set[str] = set()
        self._background_tasks: set[asyncio.Task[None]] = set()
        self.stop_task: asyncio.Task[None] | None = None
        self._idle_timeout = idle_timeout

    def _spawn_task(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    @override
    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> ExternalRunAdapter:
        self._active_run_ids.add(run_id)
        return super().run_workflow(
            run_id,
            workflow,
            init_state,
            start_event=start_event,
            serialized_state=serialized_state,
            serializer=serializer,
        )

    @override
    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        inner_adapter = self._decorated.get_internal_adapter(workflow)
        return _IdleReleaseInternalRunAdapter(inner_adapter, self, self._store)

    @override
    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        return IdleReleaseExternalRunAdapter(self, run_id)

    async def _deferred_release(self, run_id: str) -> None:
        """Wait for idle_timeout then release the handler if still idle."""
        await asyncio.sleep(self._idle_timeout)
        await self._release_idle_handler(run_id)

    async def _release_idle_handler(self, run_id: str) -> None:
        """Release an idle handler from memory."""
        async with self._reload_lock(run_id):
            handlers = await self._store.query(HandlerQuery(run_id_in=[run_id]))
            if len(handlers) != 1 or handlers[0].idle_since is None:
                return
            elapsed = (
                datetime.now(timezone.utc) - handlers[0].idle_since
            ).total_seconds()
            if elapsed < self._idle_timeout:
                return
            if run_id not in self._active_run_ids:
                return
            self._active_run_ids.discard(run_id)
            self._abort_inner_run(run_id)
            logger.info(f"Released idle handler [run_id={run_id}] from memory")

    def _abort_inner_run(self, run_id: str) -> None:
        """Cancel the inner runtime's control loop task for a run."""
        try:
            inner_adapter = self._decorated.get_external_adapter(run_id)
        except Exception:
            return
        if isinstance(inner_adapter, V2RuntimeCompatibilityShim):
            inner_adapter.abort()
        else:
            raise ValueError(f"Inner adapter {inner_adapter} does not support abort")

    async def _ensure_active_run(self, run_id: str) -> None:
        if run_id in self._active_run_ids:
            return
        async with self._reload_lock(run_id):
            await self._ensure_active_run_locked(run_id)

    async def _ensure_active_run_locked(self, run_id: str) -> None:
        if run_id in self._active_run_ids:
            return
        handlers = await self._store.query(HandlerQuery(run_id_in=[run_id]))
        if len(handlers) != 1:
            raise ValueError(
                f"Expected 1 handler for run {run_id}, got {len(handlers)}"
            )
        handler = handlers[0]
        workflow = self._persistence.get_tracked_workflow(handler.workflow_name)
        if workflow is None:
            raise ValueError(f"Workflow {handler.workflow_name} not found")
        replayed = await self._persistence.context_from_ticks(workflow, run_id)
        context = replayed.context if replayed is not None else None
        workflow.run(ctx=context, run_id=run_id)
        self._active_run_ids.add(run_id)
        await self._store.update_handler_status(run_id, idle_since=None)
        logger.info(
            f"Reloaded workflow [handler_id={handler.handler_id}, run_id={run_id}] from persistence"
        )

    @override
    async def destroy(self) -> None:
        await super().destroy()
        if self.stop_task is not None:
            try:
                self.stop_task.cancel()
            except Exception:
                pass
        self.stop_task = self._spawn_task(self._on_server_stop())

    async def _on_server_stop(self) -> None:
        """Cancel all active runs."""
        run_ids = list(self._active_run_ids)
        logger.info(f"Shutting down. Cancelling {len(run_ids)} handlers.")
        for run_id in run_ids:
            self._abort_inner_run(run_id)
        self._active_run_ids.clear()
