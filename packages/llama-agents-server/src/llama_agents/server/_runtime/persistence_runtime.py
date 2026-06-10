# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""TickPersistenceDecorator, PersistenceDecorator, and _PersistenceInternalRunAdapter.

TickPersistenceDecorator provides tick persistence, workflow tracking, and
context_from_ticks.  PersistenceDecorator extends it with auto-restart on
server start.  Neither handles idle detection — that lives in
IdleReleaseDecorator.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator, Coroutine
from dataclasses import dataclass
from typing import Any

from typing_extensions import override
from workflows import Context
from workflows.context.context_types import SerializedContext
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store_integration import decode_seed_state
from workflows.errors import WorkflowCancelledByUser
from workflows.events import IdleReleasedEvent, StartEvent, StopEvent
from workflows.runtime.control_loop import replay_ticks_stream
from workflows.runtime.runtime_decorators import (
    BaseInternalRunAdapterDecorator,
    BaseRuntimeDecorator,
)
from workflows.runtime.types.commands import (
    CommandCompleteRun,
    CommandFailWorkflow,
    CommandHalt,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    Runtime,
)
from workflows.runtime.types.ticks import (
    TickStepResult,
    WorkflowTick,
    WorkflowTickAdapter,
)
from workflows.workflow import Workflow

from .._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    Status,
    as_legacy_context_store,
    stream_workflow_ticks,
)
from .._store.sqlite.sqlite_state_store import SqliteStateStore

logger = logging.getLogger(__name__)


@dataclass
class ReplayedContext:
    """Result of replaying persisted ticks into a Context.

    Attributes:
        context: Rebuilt Context ready to be passed to ``workflow.run()``.
        exit_command: The reducer-emitted terminal command if the tick stream
            terminated, else None. Callers map this to handler status (see
            :func:`handler_status_from_exit_command`).
    """

    context: Context
    exit_command: CommandCompleteRun | CommandFailWorkflow | CommandHalt | None = None


def handler_status_from_exit_command(
    command: CommandCompleteRun | CommandFailWorkflow | CommandHalt,
) -> tuple[Status, StopEvent | None, str | None] | None:
    """Map a reducer exit command to (status, result, error).

    Returns None for CommandCompleteRun(IdleReleasedEvent) — idle release is
    not a real completion, just how the reducer signals the runner to exit.
    """
    if isinstance(command, CommandCompleteRun):
        if isinstance(command.result, IdleReleasedEvent):
            return None
        return ("completed", command.result, None)
    if isinstance(command, CommandFailWorkflow):
        return ("failed", None, str(command.exception))
    # CommandHalt: timeout or cancel (both reach this replay path)
    if isinstance(command.exception, WorkflowCancelledByUser):
        return ("cancelled", None, None)
    return ("failed", None, str(command.exception))


class _PersistenceInternalRunAdapter(BaseInternalRunAdapterDecorator):
    """Internal adapter that persists ticks to the workflow store."""

    def __init__(
        self,
        decorated: InternalRunAdapter,
        store: AbstractWorkflowStore,
    ) -> None:
        super().__init__(decorated)
        self._store = store

    @override
    async def on_tick(self, tick: WorkflowTick) -> None:
        await super().on_tick(tick)
        tick_data = WorkflowTickAdapter.dump_python(tick, mode="json")
        try:
            await self._store.append_tick(self.run_id, tick_data)
        except Exception:
            logger.exception(
                "Failed to persist tick for run %s",
                self.run_id,
            )

    @override
    async def after_tick(self, tick: WorkflowTick) -> None:
        await super().after_tick(tick)
        if not isinstance(tick, TickStepResult):
            return
        tick_data = WorkflowTickAdapter.dump_python(tick, mode="json")
        try:
            await self._store.after_tick(self.run_id, tick_data)
        except Exception:
            logger.exception(
                "Failed to gather pending writes for run %s",
                self.run_id,
            )


class TickPersistenceDecorator(BaseRuntimeDecorator):
    """Runtime decorator for tick persistence and workflow tracking.

    Provides tick storage via internal adapter, workflow tracking by name,
    and context_from_ticks for rebuilding state from persisted ticks.
    """

    def __init__(
        self,
        decorated: Runtime,
        store: AbstractWorkflowStore,
    ) -> None:
        super().__init__(decorated)
        self._store = store
        self._workflows_by_name: dict[str, Workflow] = {}
        self._active_run_ids: set[str] = set()

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
        return _PersistenceInternalRunAdapter(inner_adapter, self._store)

    @override
    def track_workflow(self, workflow: Workflow) -> None:
        self._workflows_by_name[workflow.workflow_name] = workflow
        super().track_workflow(workflow)

    @override
    def untrack_workflow(self, workflow: Workflow) -> None:
        self._workflows_by_name.pop(workflow.workflow_name, None)
        super().untrack_workflow(workflow)

    def get_tracked_workflow(self, name: str) -> Workflow | None:
        """Look up a tracked workflow by name (used by IdleReleaseDecorator)."""
        return self._workflows_by_name.get(name)

    async def context_from_ticks(
        self, workflow: Workflow, run_id: str
    ) -> ReplayedContext | None:
        """Rebuild a Context from persisted ticks (and legacy ctx if available).

        Returns the Context plus the reducer's exit command if the tick
        stream already terminated. Callers use ``exit_command`` to finalize
        handlers instead of resuming them.
        """
        serializer = JsonSerializer()
        legacy_ctx = self._get_legacy_ctx(run_id)

        tick_stream = stream_workflow_ticks(self._store, run_id)
        try:
            first_tick = await tick_stream.__anext__()
        except StopAsyncIteration:
            first_tick = None

        if first_tick is None and not legacy_ctx:
            return None

        if legacy_ctx:
            await self._seed_legacy_state(run_id, legacy_ctx)
            parsed = SerializedContext.from_dict_auto(legacy_ctx)
            init_state = BrokerState.from_serialized(parsed, workflow, serializer)
        else:
            init_state = BrokerState.from_workflow(workflow)

        exit_command: CommandCompleteRun | CommandFailWorkflow | CommandHalt | None = (
            None
        )
        if first_tick is not None:

            async def _with_first() -> AsyncIterator[WorkflowTick]:
                yield first_tick
                async for tick in tick_stream:
                    yield tick

            replay = await replay_ticks_stream(init_state, _with_first())
            init_state = replay.state
            exit_command = replay.exit_command

        serialized = init_state.to_serialized(serializer)
        context = Context.from_dict(
            workflow=workflow, data=serialized.model_dump(), serializer=serializer
        )
        return ReplayedContext(context=context, exit_command=exit_command)

    def _get_legacy_ctx(self, run_id: str) -> dict[str, Any] | None:
        legacy_store = as_legacy_context_store(self._store)
        if legacy_store is None:
            return None
        try:
            return legacy_store.get_legacy_ctx(run_id)
        except Exception:
            logger.warning(
                "Failed to read legacy ctx for run %s", run_id, exc_info=True
            )
            return None

    async def _seed_legacy_state(self, run_id: str, legacy_ctx: dict[str, Any]) -> None:
        """Eagerly migrate a legacy ctx state snapshot into the state table.

        No-op when the state table already has a row for the run (a previous
        partial run's state must win over the legacy snapshot).
        """
        try:
            parsed = SerializedContext.from_dict_auto(legacy_ctx)
        except Exception:
            logger.warning(
                "Failed to parse legacy ctx for state migration, run %s", run_id
            )
            return

        state_data = parsed.state
        if not state_data:
            return

        state_store = self._store.create_state_store(run_id)
        if not isinstance(state_store, SqliteStateStore):
            return

        conn = sqlite3.connect(state_store._db_path)
        try:
            row = conn.execute(
                "SELECT 1 FROM workflow_state WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is not None:
                return
        finally:
            conn.close()

        state = decode_seed_state(state_data, JsonSerializer())
        await state_store.set_state(state)


class PersistenceDecorator(TickPersistenceDecorator):
    """Runtime decorator that extends TickPersistenceDecorator with auto-restart.

    Resumes previously running workflows on server start.
    """

    def __init__(
        self,
        decorated: Runtime,
        store: AbstractWorkflowStore,
    ) -> None:
        super().__init__(decorated, store)
        self._background_tasks: set[asyncio.Task[None]] = set()
        self.resume_task: asyncio.Task[None] | None = None

    def _spawn_task(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    @override
    async def launch(self) -> None:
        await super().launch()
        self.resume_task = self._spawn_task(
            self._on_server_start(self._workflows_by_name)
        )

    async def _on_server_start(self, registered_workflows: dict[str, Workflow]) -> None:
        """Resume previously running (non-idle) workflows from persistence."""
        handlers = await self._store.query(
            HandlerQuery(
                status_in=["running"],
                workflow_name_in=list(registered_workflows.keys()),
                is_idle=False,
            )
        )
        for persistent in handlers:
            workflow = registered_workflows.get(persistent.workflow_name)
            if workflow is None:
                continue
            if persistent.run_id is None:
                logger.error(f"Run ID is required for handler {persistent.handler_id}")
                continue
            run_id = persistent.run_id
            if run_id in self._active_run_ids:
                continue
            try:
                replayed = await self.context_from_ticks(workflow, run_id)

                if replayed is None:
                    # A fresh-start attempt here would build a StartEvent from
                    # empty kwargs and loop on every boot for workflows with
                    # required fields. Mark failed so the handler stops being
                    # picked up by the next resume query.
                    logger.warning(
                        "No replayable state for handler %s (workflow %s); "
                        "marking as failed",
                        persistent.handler_id,
                        persistent.workflow_name,
                    )
                    await self._store.update_handler_status(
                        run_id,
                        status="failed",
                        error="handler crashed before persisting any state; cannot resume",
                    )
                    continue

                finalize = (
                    handler_status_from_exit_command(replayed.exit_command)
                    if replayed.exit_command is not None
                    else None
                )
                if finalize is not None:
                    status, result, error = finalize
                    logger.warning(
                        "Replay for handler %s (workflow %s) terminated as %s; "
                        "finalizing without resume",
                        persistent.handler_id,
                        persistent.workflow_name,
                        status,
                    )
                    await self._store.update_handler_status(
                        run_id,
                        status=status,
                        result=result,
                        error=error,
                    )
                    continue

                workflow.run(ctx=replayed.context, run_id=run_id)
            except Exception as e:
                logger.error(
                    f"Failed to resume handler {persistent.handler_id} for workflow {persistent.workflow_name}: {e}"
                )
                try:
                    await self._store.update_handler_status(
                        run_id, status="failed", error=str(e)
                    )
                except Exception:
                    logger.exception(
                        "Failed to mark resume-failed handler %s as failed",
                        persistent.handler_id,
                    )
                continue

    @override
    async def destroy(self) -> None:
        await super().destroy()
        if self.resume_task is not None:
            try:
                self.resume_task.cancel()
            except Exception:
                pass
