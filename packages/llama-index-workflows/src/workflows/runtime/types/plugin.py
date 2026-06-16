# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
A runtime interface to switch out a broker runtime (external library or service that manages durable/distributed step execution).
"""

from __future__ import annotations

import asyncio
import weakref
from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Coroutine,
    Generator,
    Literal,
    Protocol,
)

from workflows.context.state_store import StateStore
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.types.named_task import (
    NamedTask,
    PendingStart,
    all_tasks,
    pick_highest_priority,
)

if TYPE_CHECKING:
    from workflows.context.context import Context
    from workflows.context.serializers import BaseSerializer
    from workflows.runtime.types.internal_state import BrokerState
    from workflows.runtime.types.step_function import StepWorkerFunction
    from workflows.workflow import Workflow
from workflows.runtime.types.ticks import TickCancelRun, WorkflowTick

# Context variable for implicit runtime scoping
_current_runtime: ContextVar[Runtime | None] = ContextVar(
    "current_runtime", default=None
)


@dataclass
class WaitResultTick:
    """Result containing a received tick."""

    tick: WorkflowTick
    type: Literal["tick"] = "tick"


@dataclass
class WaitResultTimeout:
    """Result indicating timeout expiration."""

    type: Literal["timeout"] = "timeout"


WaitResult = WaitResultTick | WaitResultTimeout


@dataclass
class WaitForNextTaskResult:
    """Result from wait_for_next_task containing the completed task and newly started tasks."""

    completed: asyncio.Task[Any] | None
    started: list[NamedTask]


@dataclass
class RegisteredWorkflow:
    workflow: Workflow
    workflow_run_fn: WorkflowRunFunction
    steps: dict[str, StepWorkerFunction]


class InternalRunAdapter(ABC):
    """
    Adapter interface for use INSIDE a workflow's control loop.

    This adapter is used by the workflow execution engine (broker) to receive
    ticks from external sources, publish events to listeners, manage timing,
    and perform durable sleeps.

    The InternalRunAdapter is created by Runtime.new_internal_adapter() for each
    workflow run and is passed to the control loop function. It provides the
    internal-facing side of workflow communication:
    - Receiving ticks from the external mailbox (wait_receive)
    - Publishing events that external code can stream (write_to_event_stream)
    - Getting current time with durability support (get_now)
    - Sleeping with durability support (sleep)

    The run_id is always available and required at construction time.
    """

    @property
    @abstractmethod
    def run_id(self) -> str:
        """
        The unique identifier for this workflow run.

        Always available - required at adapter construction time.
        """
        ...

    @abstractmethod
    async def write_to_event_stream(self, event: Event) -> None:
        """
        Publish an event to external listeners.

        Called from inside the workflow to emit events that can be observed
        by external code via the ExternalRunAdapter's stream_published_events().
        """
        ...

    @abstractmethod
    async def get_now(self) -> float:
        """
        Get the current time in seconds since epoch.

        Called from within the workflow control loop. For durable workflows,
        this should return a memoized/replayed value to ensure deterministic
        replay behavior.
        """
        ...

    @abstractmethod
    async def send_event(self, tick: WorkflowTick) -> None:
        """
        Send a tick into the workflow's own mailbox from within the control loop.

        Called from inside the workflow (e.g., from step functions via ctx.send_event)
        to inject events back into the workflow's execution. The tick will be
        received by wait_receive() on the next iteration.
        """
        ...

    @abstractmethod
    async def wait_receive(
        self,
        timeout_seconds: float | None = None,
    ) -> WaitResult:
        """
        Wait for next tick OR timeout expiration.

        This is the primary method for the control loop to wait for events.
        It combines receiving ticks and timeout handling into a single
        deterministic operation.

        Args:
            timeout_seconds: Max time to wait. None means wait indefinitely.

        Returns:
            WaitResultTick if a tick was received
            WaitResultTimeout if timeout expired before receiving tick

        This is a DURABLE operation for durable runtimes:
        - On replay, already-elapsed time is accounted for
        - If timeout already expired in previous run, returns immediately
        """
        ...

    async def close(self) -> None:
        """
        Release resources for a completed/failed workflow.

        Called by the control loop's cleanup_tasks() when the workflow is
        finishing (completion, failure, halt, or unwind). Implementations
        should wake any blocked wait_receive() calls so the control loop
        can exit.

        WARNING: This may be destructive — e.g. sending a durable message
        that prevents the workflow from resuming later. Only called when
        the workflow's outcome is already determined.

        Default is no-op.
        """
        pass

    def get_state_store(self) -> StateStore[Any] | None:
        """
        Get the state store for this workflow run.

        Returns the state store from the runtime, or None if not initialized.
        Default implementation returns None.
        """
        return None

    async def finalize_step(self) -> None:
        """
        Called after a step function completes to perform any adapter-specific cleanup.

        This is called after all background tasks spawned during the step have completed.
        Adapters can override to perform additional finalization (e.g., flush buffers,
        sync state). Default is no-op.
        """
        pass

    def is_replaying(self) -> bool:
        """Whether the adapter is currently replaying recorded operations.

        During replay, side effects like persisting events to external stores
        should be skipped to avoid duplicates. Default is False (live execution).
        """
        return False

    async def on_tick(self, tick: WorkflowTick) -> None:
        """
        Called whenever a tick event is processed by the control loop.

        This method is invoked for both external ticks (sent via send_event)
        and internal ticks (generated by step completions, timeouts, etc.).
        Adapters can override to record ticks, persist them, etc.
        Default is no-op.
        """
        pass

    async def after_tick(self, tick: WorkflowTick) -> None:
        """Called after a tick's commands have been processed.

        Fires for non-terminal ticks only — terminal commands (CompleteRun,
        Halt, FailWorkflow) return/raise before this hook runs. Terminal
        cleanup is handled separately via the store's event persistence path.
        """
        pass

    async def wait_for_next_task(
        self,
        running: list[NamedTask],
        pending: list[PendingStart],
        timeout: float | None = None,
    ) -> WaitForNextTaskResult:
        """Wait for and return the next task that should complete.

        The adapter is responsible for starting pending coroutines as asyncio tasks.
        This allows adapters to control task startup ordering (e.g., for deterministic
        function_id acquisition in DBOS).

        Args:
            running: Already-started tasks from previous iterations.
            pending: Coroutines to start this iteration.
            timeout: Timeout in seconds, None for no timeout.

        Returns:
            WaitForNextTaskResult with the completed task and newly started NamedTasks.

        IMPORTANT: Must return at most ONE completed task per call.
        """
        started = [p.start(asyncio.create_task(p.coro)) for p in pending]
        all_named = running + started
        tasks = all_tasks(all_named)
        if not tasks:
            return WaitForNextTaskResult(None, started)
        done, _ = await asyncio.wait(
            tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        completed = pick_highest_priority(all_named, done) if done else None
        return WaitForNextTaskResult(completed, started)


class ExternalRunAdapter(ABC):
    """
    Adapter interface for use OUTSIDE a workflow's control loop.

    This adapter is used by external code (e.g., HTTP handlers, client code)
    to interact with a running workflow - sending events into the workflow
    and streaming events published by the workflow.

    The ExternalRunAdapter is created by Runtime.new_external_adapter() and
    provides the external-facing side of workflow communication:
    - Sending ticks into the workflow mailbox (send_event)
    - Streaming events published by the workflow (stream_published_events)
    - Cleaning up resources when done (close)

    The run_id is always available and matches the internal adapter's run_id.
    """

    @property
    @abstractmethod
    def run_id(self) -> str:
        """
        The unique identifier for this workflow run.

        Always available - matches the InternalRunAdapter's run_id.
        """
        ...

    @abstractmethod
    async def send_event(self, tick: WorkflowTick) -> None:
        """
        Send a tick into the workflow mailbox.

        Called from outside the workflow to inject events into the workflow's
        execution. The tick will be received by the internal adapter's
        wait_receive() method.
        """
        ...

    @abstractmethod
    def stream_published_events(self) -> AsyncGenerator[Event, None]:
        """
        Stream events published by the workflow.

        Called from outside the workflow to observe events emitted by the
        workflow via the internal adapter's write_to_event_stream().
        Returns an async generator that yields events as they are published.
        """
        ...

    async def close(self) -> None:
        """
        Clean up adapter resources.

        Called when done interacting with the workflow to release any
        resources held by this adapter (e.g., close streams, release locks).
        """

        pass

    @abstractmethod
    async def get_result(self) -> StopEvent:
        """
        Get the result of the workflow run, if completed. Will raise if the workflow failed or was cancelled
        """
        ...

    async def cancel(self) -> None:
        """
        Cancel the workflow run if it is still running.
        """
        await self.send_event(TickCancelRun())

    def get_state_store(self) -> StateStore[Any] | None:
        """
        Get the state store for this workflow run.

        Returns the state store if this adapter owns it, or None if state
        is managed externally. Default implementation returns None.
        """
        return None


@dataclass
class RunContext:
    """Payload handed from `create_workflow_run_function` to the control loop
    across the narrow `ControlLoopFunction` boundary.
    """

    workflow: Workflow
    run_adapter: InternalRunAdapter
    context: Context
    steps: dict[str, StepWorkerFunction]


@dataclass
class RunContextContainer:
    """Mutable one-shot holder for a `RunContext`.

    The control loop calls `consume()` at entry, which drops the container's
    reference to the payload. Because `asyncio` snapshots the current `Context`
    when scheduling handles (e.g. `loop.call_later` for periodic timers like
    aiohttp's `TCPConnector._cleanup_closed`), any such snapshot would otherwise
    keep the workflow object graph alive via this container until the timer
    fires. Clearing the single shared instance breaks that reference chain.
    """

    payload: RunContext | None

    def consume(self) -> RunContext:
        payload, self.payload = self.payload, None
        if payload is None:
            raise RuntimeError("RunContext has already been consumed")
        return payload


_current_run: ContextVar[RunContextContainer | None] = ContextVar(
    "current_run", default=None
)


@contextmanager
def run_context(ctx: RunContext) -> Generator[RunContextContainer, None, None]:
    """Set the current run context for the duration of a workflow run."""
    container = RunContextContainer(payload=ctx)
    token = _current_run.set(container)
    try:
        yield container
    finally:
        _current_run.reset(token)


def consume_current_run() -> RunContext:
    """Consume the current `RunContext` payload exactly once.

    Drops the container's strong reference to the payload so that any `Context`
    snapshot taken by `asyncio` (e.g. via `loop.call_later`) cannot pin the
    workflow graph through it.
    """
    container = _current_run.get()
    if container is None:
        raise RuntimeError("Not in a workflow run context")
    return container.consume()


class WorkflowSet:
    """Identity-based weak set for tracking Workflow instances.

    Uses id() as the key and weakref.ref with cleanup callbacks to
    avoid hashability requirements and memory leaks.
    """

    def __init__(self) -> None:
        self._refs: dict[int, weakref.ref[Workflow]] = {}

    def add(self, workflow: Workflow) -> None:
        obj_id = id(workflow)
        if obj_id in self._refs:
            return

        def _cleanup(ref: weakref.ref[Workflow], _id: int = obj_id) -> None:
            self._refs.pop(_id, None)

        self._refs[obj_id] = weakref.ref(workflow, _cleanup)

    def discard(self, workflow: Workflow) -> None:
        self._refs.pop(id(workflow), None)

    def __contains__(self, workflow: Workflow) -> bool:
        ref = self._refs.get(id(workflow))
        if ref is None:
            return False
        return ref() is not None

    def __iter__(self) -> Generator[Workflow, None, None]:
        for ref in list(self._refs.values()):
            obj = ref()
            if obj is not None:
                yield obj

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __bool__(self) -> bool:
        return any(ref() is not None for ref in self._refs.values())


class Runtime(ABC):
    """
    Abstract base class for workflow execution runtimes.

    Runtimes control how workflows are registered, launched, and executed.
    The default BasicRuntime uses asyncio; Other's plug into their own durability and distributed execution models.

    Lifecycle:
    1. Create runtime instance
    2. Create workflow instances (auto-register with runtime via registering())
    3. Call launch() to start workers/register with backend
    4. Run workflows
    5. Call destroy() to clean up

    Use registering() context manager for implicit workflow registration.
    """

    def __init__(self) -> None:
        self._pending: WorkflowSet = WorkflowSet()
        self._launched: bool = False

    @property
    def is_launched(self) -> bool:
        return self._launched

    _token: Token[Runtime | None]

    def get_or_register(self, workflow: Workflow) -> RegisteredWorkflow:
        """Get the registered workflow if available, otherwise register it."""
        registered = self.get_registered(workflow)
        if registered is None:
            registered = self.register(workflow)
        return registered

    @abstractmethod
    def register(self, workflow: Workflow) -> RegisteredWorkflow:
        """
        Register a workflow with the runtime.

        Called at launch() time for each tracked workflow. Runtimes can
        wrap the control_loop and steps to fit in their registration/decoration model.

        Returns RegisteredWorkflow with wrapped functions
        """
        ...

    @abstractmethod
    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> ExternalRunAdapter:
        """
        Launch a workflow run.

        The runtime creates and owns the state store based on serialized_state.
        Returns the external adapter for the workflow run.

        Args:
            run_id: Unique identifier for this workflow run.
            registered: The registered workflow to run.
            init_state: Initial broker state (queues, workers, etc).
            start_event: Optional start event to begin the workflow.
            serialized_state: Serialized state store data to restore from.
            serializer: Serializer to use for deserializing state.
        """
        ...

    @abstractmethod
    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        """
        Get the internal adapter for a workflow run.

        Called on each workflow.run() to instantiate an interface for the workflow run internals to communicate with the runtime.

        Args:
            workflow: The workflow instance being run. Used by runtimes to access workflow metadata (e.g., state type).
        """
        ...

    @abstractmethod
    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        """
        Get the external adapter for a workflow run.

        Called after launching a workflow run, or when getting a handle for an existing workflow run.
        Used to send events into the workflow and stream published events.

        The run_id must match the internal adapter's run_id for the same run.
        The external adapter is used by client code interacting with the workflow.
        """
        ...

    async def launch(self) -> None:
        """
        Launch the runtime and register all tracked workflows.

        For many runtime's, this must be called before running workflows.
        """
        self._launched = True
        for wf in self._pending:
            self.register(wf)
            wf._runtime_locked = True
        self._pending = WorkflowSet()

    async def destroy(self) -> None:
        """
        Clean up runtime resources.

        Called when done with the runtime. Stops workers, closes connections.
        """
        self._launched = False

    def launch_sync(self) -> None:
        """Synchronous convenience wrapper for :meth:`launch`."""
        asyncio.run(self.launch())

    def destroy_sync(self) -> None:
        """Synchronous convenience wrapper for :meth:`destroy`."""
        asyncio.run(self.destroy())

    def track_workflow(self, workflow: Workflow) -> None:
        """
        Track a workflow instance for registration at launch time.

        Called by Workflow.__init__ to register with the runtime.
        """
        self._pending.add(workflow)

    def untrack_workflow(self, workflow: Workflow) -> None:
        """Remove a workflow from this runtime's tracking set."""
        self._pending.discard(workflow)

    def get_registered(self, workflow: Workflow) -> RegisteredWorkflow | None:
        """
        Get the registered workflow if available.

        Returns the pre-registered workflow from launch(), or None if not tracked.
        """
        return None

    @contextmanager
    def registering(self) -> Generator[Runtime, None, None]:
        """
        Context manager for implicit workflow registration.

        Workflows created inside this block will automatically set this runtime as their runtime.
        """
        token = _current_runtime.set(self)
        try:
            yield self
        finally:
            _current_runtime.reset(token)


class SnapshottableAdapter(ABC):
    """
    Mixin interface that adds snapshot/replay capabilities to adapters.

    This is a standalone mixin (not inheriting from InternalRunAdapter or
    ExternalRunAdapter) that can be combined with adapter implementations
    to add init_state and replay capabilities for state reconstruction.

    Use `as_snapshottable_adapter()` to check if an adapter supports snapshotting.
    """

    @property
    @abstractmethod
    def init_state(self) -> BrokerState:
        """
        Get the initial state of the adapter.
        """
        ...

    @abstractmethod
    def replay(self) -> list[WorkflowTick]:
        """
        Return the recorded ticks for replay.

        Returns all ticks that were recorded via on_tick(), in the order
        they were received. Used for debugging and workflow replay.
        """
        ...


def as_snapshottable_adapter(
    adapter: ExternalRunAdapter | InternalRunAdapter,
) -> SnapshottableAdapter | None:
    """
    Check if an internal adapter supports snapshotting.

    Returns the adapter cast to SnapshottableAdapter if it implements
    the snapshotting interface, or None otherwise.
    """
    if isinstance(adapter, SnapshottableAdapter):
        return adapter
    return None


class V2RuntimeCompatibilityShim(ABC):
    """
    This interface will be deleted in V3. Temporary shim to support deprecated v2 functionality
    """

    @abstractmethod
    def get_result_or_none(self) -> StopEvent | None:
        """
        Get the result of the workflow run, if completed. Will raise if the workflow failed or was cancelled, otherwise return None if still running
        """
        ...

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """
        Check if the workflow run is still running.
        """
        ...

    @abstractmethod
    def abort(self) -> None:
        """
        Forcefully abort the workflow execution (ungraceful hard cancel).

        This immediately terminates execution by cancelling the underlying task.
        Unlike cancel() which sends a graceful cancellation signal:
        - In-flight step work is cancelled immediately
        - No WorkflowCancelledEvent is emitted
        - The workflow does not finalize gracefully

        This is deprecated v2 behavior - prefer cancel_run() for graceful cancellation.
        """
        ...


def as_v2_runtime_compatibility_shim(
    adapter: ExternalRunAdapter,
) -> V2RuntimeCompatibilityShim | None:
    """
    Check if an adapter supports the V2 runtime compatibility shim.
    """
    if isinstance(adapter, V2RuntimeCompatibilityShim):
        return adapter
    return None


class ControlLoopFunction(Protocol):
    """
    Protocol for a function that starts and runs the internal control loop for a workflow run.
    Runtime decorators to the control loop function must maintain this signature.
    """

    def __call__(
        self,
        start_event: Event | None,
        init_state: BrokerState | None,
        run_id: str,
    ) -> Coroutine[None, None, StopEvent]: ...


class WorkflowRunFunction(Protocol):
    """
    Protocol for a function that runs a workflow. Wraps a control loop function with glue to the runtime.
    """

    def __call__(
        self,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        tags: dict[str, Any] | None = None,
    ) -> Coroutine[None, None, StopEvent]: ...
