# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import time
import weakref
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator

if TYPE_CHECKING:
    from workflows.workflow import Workflow

from llama_index_instrumentation import get_dispatcher

from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    InMemoryStateStore,
    StateStore,
    infer_state_type,
)
from workflows.errors import WorkflowRuntimeError
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    SnapshottableAdapter,
    V2RuntimeCompatibilityShim,
    WaitResult,
    WaitResultTick,
    WaitResultTimeout,
)
from workflows.runtime.types.step_function import (
    as_step_worker_functions,
    create_workflow_run_function,
)
from workflows.runtime.types.ticks import WorkflowTick
from workflows.workflow import Workflow


class AsyncioAdapterQueues:
    """Shared state between internal and external adapters.

    The `complete` task is set by run_workflow() after instantiation due to
    circular dependency: the task closure captures this object to prevent
    premature GC from the WeakValueDictionary.
    """

    # Set by run_workflow() after task creation
    complete: asyncio.Task[StopEvent]

    def __init__(
        self,
        run_id: str,
        init_state: BrokerState,
        state_store: StateStore[Any] | None = None,
    ):
        self.run_id = run_id
        self.init_state = init_state
        self.ticks: list[WorkflowTick] = []
        self.state_store = state_store

    # created lazily via cached_property for Python 3.14+ compatibility (they require a running event loop)
    @functools.cached_property
    def receive_queue(self) -> asyncio.Queue[WorkflowTick]:
        return asyncio.Queue[WorkflowTick]()

    # created lazily via cached_property for Python 3.14+ compatibility (they require a running event loop)
    @functools.cached_property
    def publish_queue(self) -> asyncio.Queue[Event]:
        return asyncio.Queue[Event]()

    # created lazily via cached_property for Python 3.14+ compatibility (they require a running event loop)
    @functools.cached_property
    def stream_lock(self) -> asyncio.Lock:
        return asyncio.Lock()


class InternalAsyncioAdapter(InternalRunAdapter, SnapshottableAdapter):
    """
    Internal adapter for asyncio-based workflow execution.

    Used by the workflow control loop to receive ticks, publish events,
    and manage timing. Also supports snapshotting for debugging/replay.
    """

    def __init__(self, queues: AsyncioAdapterQueues) -> None:
        self._queues = queues

    @property
    def run_id(self) -> str:
        return self._queues.run_id

    @property
    def init_state(self) -> BrokerState:
        return self._queues.init_state

    async def write_to_event_stream(self, event: Event) -> None:
        self._queues.publish_queue.put_nowait(event)

    async def get_now(self) -> float:
        return time.monotonic()

    async def send_event(self, tick: WorkflowTick) -> None:
        self._queues.receive_queue.put_nowait(tick)

    async def wait_receive(
        self,
        timeout_seconds: float | None = None,
    ) -> WaitResult:
        """Wait for tick with optional timeout using asyncio primitives."""
        try:
            if timeout_seconds is None:
                tick = await self._queues.receive_queue.get()
            else:
                tick = await asyncio.wait_for(
                    self._queues.receive_queue.get(),
                    timeout=timeout_seconds,
                )
            return WaitResultTick(tick=tick)
        except asyncio.TimeoutError:
            return WaitResultTimeout()

    async def on_tick(self, tick: WorkflowTick) -> None:
        self._queues.ticks.append(tick)

    def replay(self) -> list[WorkflowTick]:
        return self._queues.ticks

    def get_state_store(self) -> StateStore[Any] | None:
        return self._queues.state_store


class ExternalAsyncioAdapter(
    ExternalRunAdapter, SnapshottableAdapter, V2RuntimeCompatibilityShim
):
    """
    External adapter for asyncio-based workflow execution.

    Used by external code to send events into the workflow
    and stream events published by the workflow.
    """

    def __init__(self, outer: BasicRuntime, queues: AsyncioAdapterQueues) -> None:
        self._outer = outer
        self._queues = queues

    @property
    def run_id(self) -> str:
        return self._queues.run_id

    async def send_event(self, tick: WorkflowTick) -> None:
        self._queues.receive_queue.put_nowait(tick)

    async def stream_published_events(self) -> AsyncGenerator[Event, None]:
        async with self._queues.stream_lock:
            if self._queues.complete.done() and self._queues.publish_queue.empty():
                raise WorkflowRuntimeError(
                    "Event stream already consumed. "
                    "Events can only be streamed once per workflow run."
                )
            while True:
                item = await self._queues.publish_queue.get()
                yield item
                if isinstance(item, StopEvent):
                    break

    def replay(self) -> list[WorkflowTick]:
        return self._queues.ticks

    def get_state_store(self) -> StateStore[Any] | None:
        return self._queues.state_store

    async def get_result(self) -> StopEvent:
        return await self._queues.complete

    def get_result_or_none(self) -> StopEvent | None:
        if not self._queues.complete.done():
            return None
        return self._queues.complete.result()

    @property
    def is_running(self) -> bool:
        return not self._queues.complete.done()

    def abort(self) -> None:
        """Abort by cancelling the control loop task."""
        if not self._queues.complete.done():
            self._queues.complete.cancel()
        self._outer._queues.pop(self.run_id, None)

    @property
    def init_state(self) -> BrokerState:
        return self._queues.init_state


class BasicRuntime(Runtime):
    """Default asyncio-based runtime with no durability."""

    @property
    def is_launched(self) -> bool:
        # BasicRuntime doesn't require launch() — always ready
        return True

    def __init__(self) -> None:
        super().__init__()
        # WeakValueDictionary allows queues to be GC'd when no adapters reference them.
        # The task closure in run_workflow() captures a strong reference, keeping
        # queues alive for fire-and-forget workflows even if the external adapter is dropped.
        self._queues: weakref.WeakValueDictionary[str, AsyncioAdapterQueues] = (
            weakref.WeakValueDictionary()
        )
        # Keyed by id(workflow) so each instance has independent concurrency limits
        self._max_concurrent_runs: weakref.WeakValueDictionary[
            int, asyncio.Semaphore
        ] = weakref.WeakValueDictionary()

    def register(self, workflow: Workflow) -> RegisteredWorkflow:
        return RegisteredWorkflow(
            workflow=workflow,
            workflow_run_fn=create_workflow_run_function(workflow),
            steps=as_step_worker_functions(workflow),
        )

    def _get_or_create_queues(
        self, run_id: str, init_state: BrokerState
    ) -> AsyncioAdapterQueues:
        """Get existing queues or create new ones for a run_id."""
        queues = self._queues.get(run_id)
        if queues is None:
            queues = AsyncioAdapterQueues(run_id=run_id, init_state=init_state)
            self._queues[run_id] = queues
        return queues

    @asynccontextmanager
    async def _maybe_acquire_max_concurrent_runs(
        self, workflow: Workflow, run_id: str
    ) -> AsyncGenerator[None, None]:
        if workflow._num_concurrent_runs is None:
            yield
        else:
            # Key by instance id so each workflow instance has independent concurrency limits
            workflow_id = id(workflow)
            if workflow_id in self._max_concurrent_runs:
                sem = self._max_concurrent_runs[workflow_id]
            else:
                sem = asyncio.Semaphore(workflow._num_concurrent_runs)
                self._max_concurrent_runs[workflow_id] = sem
            async with sem:
                yield

    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> ExternalRunAdapter:
        """Set up a workflow run. Currently only creates state store.

        Note: Execution is still managed by the broker for now. This will
        change as we refactor to have the runtime fully own execution.
        """
        if run_id in self._queues:
            # not supported in any way right now. Might make sense to support run as new, or some other idempotency semantics
            raise RuntimeError(f"Workflow run with run_id '{run_id}' already exists.")

        registered = self.get_or_register(workflow)

        # Create state store from serialized state or infer type from workflow
        active_serializer = serializer or JsonSerializer()
        if serialized_state:
            state_store = InMemoryStateStore.from_dict(
                serialized_state, active_serializer
            )
        else:
            # Infer state type from workflow step configs
            state_type = infer_state_type(registered.workflow)
            state_store = InMemoryStateStore(state_type())
        # might want to lock this better. Unlikely race condition if you spam with the same run_id.
        queues = self._get_or_create_queues(run_id, init_state)
        queues.state_store = state_store

        # Capture propagation context (otel trace, instrument tags, etc.)
        # BEFORE creating the task — contextvars won't be inherited.
        captured_tags = get_dispatcher().capture_propagation_context()

        async def run_with_concurrency_limit() -> StopEvent:
            # Capture strong reference to queues for the task's lifetime,
            # enabling fire-and-forget even if the caller drops the external adapter.
            _ = queues
            async with self._maybe_acquire_max_concurrent_runs(workflow, run_id):
                return await registered.workflow_run_fn(
                    init_state, start_event, captured_tags
                )

        with setting_run_id(run_id):
            # actually pump the task through the runtime
            task = asyncio.create_task(run_with_concurrency_limit())
            queues.complete = task
            return self.get_external_adapter(run_id)

    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        run_id = get_current_run_id()
        if run_id is None:
            raise RuntimeError(
                "No current run id. Must be called within a workflow run."
            )
        if run_id not in self._queues:
            raise RuntimeError(
                f"No queues found for run_id '{run_id}'. Must be called within a workflow run."
            )
        queues = self._queues[run_id]
        return InternalAsyncioAdapter(queues)

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        if run_id not in self._queues:
            raise RuntimeError(f"No active workflow with run_id '{run_id}'. ")
        return ExternalAsyncioAdapter(self, self._queues[run_id])


_current_run_id: ContextVar[str | None] = ContextVar("current_run_id", default=None)


def get_current_run_id() -> str | None:
    """Get the current run ID, if set."""
    return _current_run_id.get()


@contextmanager
def setting_run_id(run_id: str) -> Generator[None, None, None]:
    """Set the current run ID for the duration of the block."""
    token = _current_run_id.set(run_id)
    try:
        yield
    finally:
        _current_run_id.reset(token)


basic_runtime = BasicRuntime()
