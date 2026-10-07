# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import time
import weakref
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Generator,
    Iterable,
)

if TYPE_CHECKING:
    from workflows.workflow import Workflow

from llama_index_instrumentation import get_dispatcher

from workflows.context.context import Context
from workflows.context.context_types import SerializedContext
from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import (
    InMemoryStateStore,
    StateCheckpoint,
    StateStore,
    infer_state_type,
    is_durable_serialized_state,
)
from workflows.errors import WorkflowRuntimeError
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.control_loop.reduce import _reduce_tick
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
from workflows.runtime.types.ticks import WorkflowTick, WorkflowTickAdapter
from workflows.workflow import Workflow


@dataclass(frozen=True)
class JournalRecord:
    """A tick yielded by `BasicRuntime.journal`.

    Attributes:
        seq: Journal position, starting at the snapshot's `journal_seq`.
            Numbering continues across resumes.
        data: Encoded tick. Store it unchanged for `BasicRuntime.restore`.
    """

    seq: int
    data: str


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
        # run_workflow sets the serializer used for journal records.
        self.serializer: BaseSerializer | None = None

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

    # Create the event lazily because Python 3.14+ requires a running loop.
    @functools.cached_property
    def ticks_changed(self) -> asyncio.Event:
        """Wake journal readers when a tick arrives or the run ends."""
        return asyncio.Event()


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
        # Wall clock, not monotonic: get_now timestamps (first_attempt_at,
        # retry not_before) are persisted in snapshots and compared across
        # process restarts, so they must live in a cross-process time domain.
        return time.time()

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
        self._queues.ticks_changed.set()

    def replay(self) -> list[WorkflowTick]:
        return self._queues.ticks

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        if namespace:
            return None
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

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        if namespace:
            return None
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
        active_serializer = (
            serializer
            if serializer is not None
            else workflow.runtime.get_serializer(workflow)
        )
        if serialized_state:
            if is_durable_serialized_state(serialized_state):
                store_type = serialized_state.get("store_type")
                raise WorkflowRuntimeError(
                    f"BasicRuntime cannot restore durable state store '{store_type}'. "
                    "Use the matching durable runtime or pass an in-memory context snapshot."
                )
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
        queues.serializer = active_serializer

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
            task.add_done_callback(lambda _: queues.ticks_changed.set())
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

    async def journal(self, run_id: str) -> AsyncIterator[JournalRecord]:
        """Yield saved ticks, then new ticks as they arrive.

        Call this on the `BasicRuntime` the workflow runs on, usually the
        `workflows.plugins.basic_runtime` default. The runtime keeps all
        ticks from the current session in memory, so readers can start after
        `run()` and still receive the whole session. Stops after the run ends
        and all records have been yielded.
        """
        queues = self._queues.get(run_id)
        if queues is None:
            raise RuntimeError(f"No active workflow with run_id '{run_id}'. ")
        serializer = queues.serializer
        assert serializer is not None
        start_seq = queues.init_state.journal_seq
        index = 0
        while True:
            # Clear before yielding so ticks arriving during a yield wake the reader.
            queues.ticks_changed.clear()
            while index < len(queues.ticks):
                yield JournalRecord(
                    seq=start_seq + index,
                    data=serializer.serialize(queues.ticks[index]),
                )
                index += 1
            if queues.complete.done():
                return
            await queues.ticks_changed.wait()

    def restore(
        self,
        workflow: Workflow,
        snapshot: dict[str, Any] | None,
        records: Iterable[JournalRecord],
        serializer: BaseSerializer | None = None,
    ) -> Context:
        """Replay records after a snapshot and return a context ready to run.

        Args:
            workflow: The workflow the records were journaled for.
            snapshot: A `Context.to_dict()` result, or None for a fresh run.
                Its `state` is carried over unchanged.
            records: Records from `journal()`. Skip records already covered
                by the snapshot's `journal_seq`.
            serializer: Serializer the snapshot and records were written with.
                Defaults to the workflow's serializer.

        Raises:
            ValueError: If a sequence number is missing.

        Save the restored context with `to_dict()` to compact the journal.
        """
        active_serializer = (
            serializer if serializer is not None else self.get_serializer(workflow)
        )
        if snapshot is None:
            parsed = SerializedContext()
            state = BrokerState.from_workflow(workflow)
        else:
            parsed = SerializedContext.from_dict_auto(snapshot, active_serializer)
            state = BrokerState.from_serialized(parsed, workflow, active_serializer)
        for record in records:
            if record.seq < state.journal_seq:
                continue
            if record.seq > state.journal_seq:
                raise ValueError(
                    f"Journal gap: expected seq {state.journal_seq}, got {record.seq}"
                )
            with active_serializer.validation_context():
                tick = WorkflowTickAdapter.validate_python(
                    active_serializer.deserialize(record.data)
                )
            state, _ = _reduce_tick(tick, state, time.time())
        restored = state.to_serialized(active_serializer)
        restored.state = parsed.state
        return Context.from_dict(
            workflow, restored.model_dump(mode="python"), serializer=active_serializer
        )

    def state_checkpoint(self, run_id: str) -> StateCheckpoint:
        """Return a checkpoint of the run's committed state.

        O(1): the checkpoint references the committed model and copies
        nothing. Use ``StateCheckpoint.diff`` against an earlier checkpoint
        for a JSON Patch of what changed. Edits made by mutating a value
        returned from ``store.get`` in place are not visible to the diff.
        """
        queues = self._queues.get(run_id)
        if queues is None:
            raise RuntimeError(f"No active workflow with run_id '{run_id}'.")
        store = queues.state_store
        if not isinstance(store, InMemoryStateStore):
            raise TypeError(
                f"Run '{run_id}' has no in-memory state store to checkpoint"
            )
        return store.checkpoint()


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
