# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Idle detection and release for DBOS-backed workflows.

Uses a ``RunLifecycleLock`` to coordinate the release/resume state machine
(active → releasing → released → active) across replicas. See
``packages/llama-agents-dbos/ARCHITECTURE.md`` for details.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

from llama_agents.dbos.journal.crud import JournalCrud
from llama_agents.dbos.journal.lifecycle import RunLifecycleLock, RunLifecycleState
from llama_agents.server._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    stream_workflow_ticks,
)
from typing_extensions import override
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import infer_state_type
from workflows.context.state_store_integration import state_store_handoff
from workflows.events import Event, WorkflowIdleEvent
from workflows.runtime.control_loop import (
    rebuild_state_from_ticks,
    rebuild_state_from_ticks_stream,
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
    WaitResult,
    WaitResultTick,
)
from workflows.runtime.types.ticks import (
    TickIdleRelease,
    WorkflowTick,
)
from workflows.workflow import Workflow

from dbos import DBOS

logger = logging.getLogger(__name__)


# How long to wait before declaring a "releasing" state as crashed
CRASH_TIMEOUT_SECONDS = 120.0


class _DBOSIdleReleaseInternalRunAdapter(BaseInternalRunAdapterDecorator):
    """Internal adapter that detects idle events and schedules release."""

    def __init__(
        self,
        decorated: InternalRunAdapter,
        runtime: DBOSIdleReleaseDecorator,
        store: AbstractWorkflowStore,
    ) -> None:
        super().__init__(decorated)
        self._runtime = runtime
        self._store = store

    @override
    async def wait_receive(
        self,
        timeout_seconds: float | None = None,
    ) -> WaitResult:
        result = await super().wait_receive(timeout_seconds)
        if isinstance(result, WaitResultTick):
            self._runtime._cancel_deferred_release(self.run_id)
        return result

    @override
    async def write_to_event_stream(self, event: Event) -> None:
        await super().write_to_event_stream(event)
        if isinstance(event, WorkflowIdleEvent):
            self._runtime._schedule_deferred_release(self.run_id)


class DBOSIdleReleaseExternalRunAdapter(BaseExternalRunAdapterDecorator):
    """Proxy adapter that adds reload-on-demand for idle-released DBOS handlers.

    The inner adapter is resolved lazily because ``get_external_adapter`` is
    sync but reload (continue-as-new) is async.
    """

    def __init__(self, runtime: DBOSIdleReleaseDecorator, run_id: str) -> None:
        # Intentionally skip super().__init__ -- _decorated is a lazy property.
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
        lifecycle = await self._runtime._get_lifecycle()
        while True:
            result = await lifecycle.try_begin_resume(
                self.run_id, crash_timeout_seconds=CRASH_TIMEOUT_SECONDS
            )
            if result is None:
                await self._decorated.send_event(tick)
                return
            if result == RunLifecycleState.released:
                await self._runtime._do_resume(self.run_id, pending_tick=tick)
                return
            # releasing — poll until it completes or times out
            await asyncio.sleep(0.5)


class DBOSIdleReleaseDecorator(BaseRuntimeDecorator):
    """Runtime decorator for idle detection, release via TickIdleRelease,
    and reload via reusing the same run_id for DBOS-backed workflows.

    Uses a distributed lifecycle lock to coordinate release/resume across
    replicas. The state machine is: active → releasing → released → active.

    Must wrap an EventInterceptorDecorator (or compatible runtime) that
    wraps a DBOSRuntime.
    """

    def __init__(
        self,
        decorated: BaseRuntimeDecorator,
        store: AbstractWorkflowStore,
        idle_timeout: float = 60.0,
        journal_crud: Callable[[], JournalCrud] | None = None,
        lifecycle_lock: Callable[[], Awaitable[RunLifecycleLock]]
        | Callable[[], RunLifecycleLock]
        | None = None,
    ) -> None:
        super().__init__(decorated)
        self._store = store
        self._deferred_release_tasks: dict[str, asyncio.Task[None]] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._idle_timeout = idle_timeout
        self._workflows: dict[str, Workflow] = {}
        self._journal_crud_factory = journal_crud
        self._journal_crud_instance: JournalCrud | None = None
        if lifecycle_lock is None:
            raise ValueError("lifecycle_lock is required")
        self._lifecycle_lock_factory = lifecycle_lock
        self._lifecycle_lock_instance: RunLifecycleLock | None = None

    @property
    def _journal_crud(self) -> JournalCrud | None:
        if self._journal_crud_factory is None:
            return None
        if self._journal_crud_instance is None:
            self._journal_crud_instance = self._journal_crud_factory()
        return self._journal_crud_instance

    async def _get_lifecycle(self) -> RunLifecycleLock:
        if self._lifecycle_lock_instance is None:
            result = self._lifecycle_lock_factory()
            if isinstance(result, Awaitable):
                self._lifecycle_lock_instance = await result  # type: ignore[ty:invalid-assignment]
            else:
                self._lifecycle_lock_instance = result
        return self._lifecycle_lock_instance  # type: ignore[ty:invalid-return-type]

    @override
    def track_workflow(self, workflow: Workflow) -> None:
        self._workflows[workflow.workflow_name] = workflow
        super().track_workflow(workflow)

    @override
    def untrack_workflow(self, workflow: Workflow) -> None:
        self._workflows.pop(workflow.workflow_name, None)
        super().untrack_workflow(workflow)

    def _spawn_task(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _schedule_deferred_release(self, run_id: str) -> None:
        """Cancel any existing timer for run_id and schedule a new one."""
        self._cancel_deferred_release(run_id)
        task = self._spawn_task(self._deferred_release(run_id))
        self._deferred_release_tasks[run_id] = task

    def _cancel_deferred_release(self, run_id: str) -> None:
        """Cancel a pending deferred release timer for run_id, if any."""
        task = self._deferred_release_tasks.pop(run_id, None)
        if task is not None and not task.done():
            task.cancel()

    @override
    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        inner_adapter = self._decorated.get_internal_adapter(workflow)
        return _DBOSIdleReleaseInternalRunAdapter(inner_adapter, self, self._store)

    @override
    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        return DBOSIdleReleaseExternalRunAdapter(self, run_id)

    async def _deferred_release(self, run_id: str) -> None:
        """Wait for idle_timeout then release the handler if still idle."""
        await asyncio.sleep(self._idle_timeout)
        self._deferred_release_tasks.pop(run_id, None)
        await self._release_idle_handler(run_id)

    async def _release_idle_handler(self, run_id: str) -> None:
        """Release an idle handler by sending TickIdleRelease."""
        lifecycle = await self._get_lifecycle()
        if not await lifecycle.begin_release(run_id):
            return

        external = self._decorated.get_external_adapter(run_id)
        await external.send_event(TickIdleRelease())
        logger.info(f"Released idle DBOS handler [run_id={run_id}]")

        self._spawn_task(self._await_and_mark_released(run_id, external))

    async def _await_and_mark_released(
        self, run_id: str, external: ExternalRunAdapter
    ) -> None:
        """Await workflow completion, then mark as released and set idle_since."""
        try:
            await external.get_result()

            lifecycle = await self._get_lifecycle()
            await lifecycle.complete_release(run_id)

            # Set idle_since NOW — after the workflow is fully released
            await self._store.update_handler_status(
                run_id, status="running", idle_since=datetime.now(timezone.utc)
            )

            logger.info(f"Marked handler as released [run_id={run_id}]")
        except Exception:
            logger.warning(
                f"Failed to mark released for run_id={run_id}", exc_info=True
            )

    async def _broker_state_from_ticks(
        self, workflow: Workflow, run_id: str
    ) -> BrokerState:
        """Rebuild BrokerState from persisted ticks."""
        init_state = BrokerState.from_workflow(workflow)
        return await rebuild_state_from_ticks_stream(
            init_state, stream_workflow_ticks(self._store, run_id), run_id=run_id
        )

    async def _do_resume(
        self,
        run_id: str,
        pending_tick: WorkflowTick | None = None,
    ) -> tuple[str, ExternalRunAdapter]:
        """Resume a workflow that was previously idle-released.

        Waits for the old DBOS workflow to finish (works cross-replica),
        purges DBOS/journal state, rebuilds from ticks, and starts a fresh
        DBOS workflow with the same run_id.

        Args:
            run_id: The workflow run ID to resume.
            pending_tick: An optional tick to include in the rebuilt state
                before starting the workflow. This avoids a race where the
                resumed workflow goes idle again before the tick is delivered.

        Returns (run_id, external_adapter).
        """
        self._cancel_deferred_release(run_id)

        # Wait for old DBOS workflow to finish (cross-replica safe)
        try:
            handle = await DBOS.retrieve_workflow_async(run_id)
            await handle.get_result()
        except Exception:
            logger.warning(
                f"Failed to await old DBOS workflow for run_id={run_id}",
                exc_info=True,
            )

        # Look up handler to get workflow_name
        handlers = await self._store.query(HandlerQuery(run_id_in=[run_id]))
        if len(handlers) != 1:
            raise ValueError(
                f"Expected 1 handler for run {run_id}, got {len(handlers)}"
            )
        handler = handlers[0]

        workflow = self._workflows.get(handler.workflow_name)
        if workflow is None:
            raise ValueError(f"Workflow {handler.workflow_name} not found")

        # Rebuild BrokerState from persisted ticks
        init_state = await self._broker_state_from_ticks(workflow, run_id)

        # Include the pending tick in the rebuilt state so the control loop
        # has it queued before it starts processing.
        if pending_tick is not None:
            init_state = rebuild_state_from_ticks(
                init_state, [pending_tick], run_id=run_id
            )

        # Carry over state from old run's state store
        serializer = JsonSerializer()
        serialized_state: dict[str, Any] | None = None
        state_type = infer_state_type(workflow)
        if state_type is not None:
            try:
                old_state_store = self._store.create_state_store(
                    run_id, state_type=state_type
                )
                serialized_state = await state_store_handoff(
                    old_state_store, serializer
                )
            except Exception:
                logger.warning(
                    f"Failed to carry over state from run {run_id}", exc_info=True
                )

        # Purge DBOS state and journal so the same run_id can be reused.
        try:
            await DBOS.delete_workflow_async(run_id)
        except Exception:
            logger.debug(
                f"DBOS state already purged for run_id={run_id}", exc_info=True
            )
        if self._journal_crud is not None:
            try:
                await self._journal_crud.delete(run_id)
            except Exception:
                logger.debug(
                    f"Journal already purged for run_id={run_id}", exc_info=True
                )

        # Start new workflow run with the same run_id.
        new_adapter = self._decorated.run_workflow(
            run_id,
            workflow,
            init_state,
            serialized_state=serialized_state,
            serializer=serializer,
        )

        handler.status = "running"
        handler.updated_at = datetime.now(timezone.utc)
        handler.idle_since = None
        await self._store.update(handler)

        logger.info(f"Resumed DBOS workflow [run_id={run_id}]")
        return run_id, new_adapter
