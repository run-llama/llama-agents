"""External context - for handlers and code outside workflow steps."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, AsyncGenerator, Coroutine, Generic

from typing_extensions import TypeVar

from workflows.context.context_types import MODEL_T
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import StateStore
from workflows.errors import WorkflowRuntimeError
from workflows.events import StopEvent
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    SnapshottableAdapter,
    V2RuntimeCompatibilityShim,
    as_snapshottable_adapter,
    as_v2_runtime_compatibility_shim,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickAddEvent, TickCancelRun, WorkflowTick

if TYPE_CHECKING:
    from workflows.context.serializers import BaseSerializer
    from workflows.events import Event
    from workflows.workflow import Workflow

RunResultT = TypeVar("RunResultT", default=Any)  # type: ignore[misc]


class ExternalContext(Generic[MODEL_T, RunResultT]):
    """Context for handler code and external workflow interaction.

    Used by WorkflowHandler to send events into the workflow,
    stream events out, and retrieve the final result.
    """

    _workflow: Workflow
    _external_adapter: ExternalRunAdapter
    _workers: list[asyncio.Task[Any]]
    _serializer: BaseSerializer

    def __init__(
        self,
        workflow: Workflow,
        external_adapter: ExternalRunAdapter,
        serializer: BaseSerializer = JsonSerializer(),
    ) -> None:
        self._workflow = workflow
        self._external_adapter = external_adapter
        self._serializer = serializer
        self._workers = []

    @property
    def is_running(self) -> bool:
        """Whether the workflow is currently running."""
        as_shim = as_v2_runtime_compatibility_shim(self._external_adapter)
        if as_shim is None:
            # Assume running if not v2 runtime compatible. This is mainly just used for resuming
            # an interrupted serialized context, which is not supported the same in distributed runtimes
            return True

        return as_shim.is_running

    def _execute_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Execute a coroutine as a background task."""
        task = asyncio.create_task(coro)
        self._workers.append(task)

        def _remove_task(_: asyncio.Task[Any]) -> None:
            try:
                self._workers.remove(task)
            except ValueError:
                # Task was already cleared during shutdown or cleanup.
                pass

        task.add_done_callback(_remove_task)
        return task

    @property
    def _tick_log(self) -> list[WorkflowTick]:
        """Get the tick log from the snapshottable adapter."""
        return self._require_snapshottable().replay()

    def _require_snapshottable(self) -> SnapshottableAdapter:
        snapshottable = as_snapshottable_adapter(self._external_adapter)
        if snapshottable is None:
            raise WorkflowRuntimeError(
                f"Runtime of type {self._workflow.runtime.__class__.__qualname__} is not snapshottable"
            )
        return snapshottable

    @property
    def _state(self) -> BrokerState:
        """Compute current state from init state and tick log."""
        from workflows.runtime.control_loop import rebuild_state_from_ticks

        ticks = self._tick_log
        snapshottable = self._require_snapshottable()
        state = snapshottable.init_state
        new_state = rebuild_state_from_ticks(
            state, ticks, run_id=self._external_adapter.run_id
        )
        return new_state

    @property
    def store(self) -> StateStore[MODEL_T]:
        """Access workflow state store."""
        state_store = self._external_adapter.get_state_store()
        if state_store is None:
            raise RuntimeError("State store not available from adapter")
        return state_store  # type: ignore[return-value]

    def send_event(self, message: Event, step: str | None = None) -> None:
        """Send an event into the workflow."""
        if step is not None:
            self._workflow._validate_valid_step_message(step, message)

        self._execute_task(
            self._external_adapter.send_event(
                TickAddEvent(
                    event=message,
                    step_id=StepId.root(step) if step is not None else None,
                )
            )
        )

    async def running_steps(self) -> list[str]:
        """Get list of currently running step names."""
        state = self._state
        return [
            step for step in state.workers.keys() if state.workers[step].in_progress
        ]

    def _require_v2_runtime_compatibility(self) -> V2RuntimeCompatibilityShim:
        v2_shim = as_v2_runtime_compatibility_shim(self._external_adapter)
        if v2_shim is None:
            raise WorkflowRuntimeError(
                f"Runtime of type {self._workflow.runtime.__class__.__qualname__} is not v2 runtime compatible"
            )
        return v2_shim

    def get_result(self) -> StopEvent:
        """Get the workflow's final result. Raises if not yet complete."""
        result = self._require_v2_runtime_compatibility().get_result_or_none()
        if result is None:
            raise WorkflowRuntimeError(
                f"Workflow run with run_id {self._external_adapter.run_id} is not complete"
            )
        return result

    def stream_events(self) -> AsyncGenerator[Event, None]:
        """Stream events published by the workflow."""
        return self._external_adapter.stream_published_events()

    def to_dict(self, serializer: BaseSerializer | None = None) -> dict[str, Any]:
        """Serialize context state for persistence."""
        active_serializer = serializer or self._serializer

        # Fetch state store from adapter and serialize
        state_data = {}
        state_store = self._external_adapter.get_state_store()
        if state_store is not None:
            state_data = state_store.to_dict(active_serializer)

        # Get the broker state
        broker_state = self._state

        context = broker_state.to_serialized(active_serializer)
        context.state = state_data
        return context.model_dump(mode="python")

    def cancel(self) -> None:
        """Request workflow cancellation."""
        self._execute_task(self._external_adapter.send_event(TickCancelRun()))

    async def shutdown(self) -> None:
        """Cancel the running workflow and clean up resources.

        Sends a cancel signal, cancels all outstanding workers (both external
        and broker workers), and closes the adapter. State remains available
        for inspection.
        """
        await self._external_adapter.send_event(TickCancelRun())
        # Clean up external context workers
        for worker in self._workers:
            worker.cancel()
        self._workers.clear()
        await self._external_adapter.close()
