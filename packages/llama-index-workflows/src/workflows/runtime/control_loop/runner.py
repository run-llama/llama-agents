# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import heapq
import logging
from typing import TYPE_CHECKING

from workflows.context.state_store import (
    StateStore,
)
from workflows.errors import (
    WorkflowRuntimeError,
)
from workflows.events import (
    Event,
    StopEvent,
    _set_event_origin_namespace,
)
from workflows.runtime.types.commands import (
    CommandCancelNamespace,
    CommandCompleteRun,
    CommandFailWorkflow,
    CommandHalt,
    CommandPublishEvent,
    CommandQueueEvent,
    CommandRunWorker,
    CommandScheduleIdleCheck,
    CommandScheduleNamespaceTimeout,
    CommandScheduleWaiterTimeout,
    CommandScheduleWakeup,
    WorkflowCommand,
)
from workflows.runtime.types.internal_state import (
    BrokerState,
    InProgressState,
)
from workflows.runtime.types.invocation import namespace_startswith, slot_namespace
from workflows.runtime.types.named_task import (
    PendingPull,
    PendingStart,
    PendingWorker,
    PullTask,
    WorkerTask,
)
from workflows.runtime.types.plugin import (
    InternalRunAdapter,
    WaitResultTick,
    consume_current_run,
)
from workflows.runtime.types.results import (
    RetryAttempt,
    RetryDecision,
    StepFunctionResult,
    StepWorkerFailed,
    StepWorkerResult,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickIdleCheck,
    TickNamespaceTimeout,
    TickStepResult,
    TickTimeout,
    TickWaiterTimeout,
    TickWakeup,
    WorkflowTick,
)
from workflows.workflow import Workflow

if TYPE_CHECKING:
    from workflows.context.context import Context
    from workflows.runtime.types.step_function import StepWorkerFunction

from workflows.runtime.control_loop.reduce import (
    _decide_retry_delay,
    _reduce_tick,
    prepare_tick_for_reduce,
    rewind_in_progress,
)

logger = logging.getLogger("workflows.runtime.control_loop")


def _is_shutdown_error(e: BaseException) -> bool:
    if isinstance(e, (asyncio.CancelledError, KeyboardInterrupt)):
        return True
    msg = str(e)
    return (
        "cannot schedule new futures after shutdown" in msg
        or "Event loop is closed" in msg
    )


async def _single_pull(adapter: InternalRunAdapter) -> WorkflowTick | None:
    """Single-iteration pull: calls wait_receive once and returns the tick.

    Returns None if timeout (shouldn't happen with unbounded wait).
    """
    wait_result = await adapter.wait_receive(None)
    if isinstance(wait_result, WaitResultTick):
        return wait_result.tick
    return None


class _ControlLoopRunner:
    """
    Private class to encapsulate the async control loop runtime state and behavior.
    Keeps the pure transformation functions at module level for testability.

    This control loop uses a sequential, deterministic design:
    - Scheduled wakeups are tracked in a heap (for timeouts/delays)
    - External events come via wait_receive
    - No concurrent timeout tasks, ensuring deterministic ordering for replay
    """

    def __init__(
        self,
        workflow: Workflow,
        adapter: InternalRunAdapter,
        context: Context,
        step_workers: dict[StepId, StepWorkerFunction],
        init_state: BrokerState,
        instances: dict[tuple[str, ...], Workflow] | None = None,
    ):
        self.workflow = workflow
        self.adapter = adapter
        self.context = context
        self.step_workers = step_workers
        # Per-run map from a step's namespace to the workflow instance that owns
        # it. Dispatch binds the bare step name against this instance, so the
        # registered worker table can stay unbound (GC-friendly). Defaults to
        # root -> the run's workflow when no children are wired.
        self.instances: dict[tuple[str, ...], Workflow] = (
            instances if instances is not None else {(): workflow}
        )
        self.state = init_state
        self.worker_tasks: set[asyncio.Task[TickStepResult]] = set()
        # Transient tick buffer - drained synchronously at start of each loop iteration
        self.tick_buffer: list[WorkflowTick] = []
        # Pending items to be processed (from rehydration or delayed ticks)
        for tick in self.state.rehydrate_with_ticks():
            self.tick_buffer.append(tick)
        # Scheduled wakeups: heap of (wakeup_time, sequence, tick) tuples
        # The sequence counter ensures deterministic ordering when timestamps are equal,
        # avoiding TypeError from comparing WorkflowTick objects that don't implement __lt__
        self.scheduled_wakeups: list[tuple[float, int, WorkflowTick]] = []
        self._wakeup_sequence = 0
        # Pull task sequence counter for deterministic journaling
        self._pull_sequence = 0
        # Map from worker task to (step_id, invocation_namespace, worker_id) key
        self._task_keys: dict[
            asyncio.Task[TickStepResult], tuple[StepId, tuple[str, ...], int]
        ] = {}
        # Whether a TickIdleCheck is currently in tick_buffer
        self._idle_check_pending = False
        # Pending worker coroutines not yet started (started by adapter in wait_for_next_task)
        self._pending_workers: list[PendingStart] = []

    def _resolve_state_view(self, namespace: tuple[str, ...]) -> StateStore | None:
        """Resolve a step's own per-namespace state view from the adapter.

        Each namespace owns an isolated record; the adapter mints (and caches)
        the per-namespace store, so this stays a thin lookup. ``None`` when the
        adapter vends no store.
        """
        return self.adapter.get_state_store(namespace)

    def schedule_tick(self, tick: WorkflowTick, at_time: float) -> None:
        """Schedule a tick to be processed at a specific time."""
        seq = self._wakeup_sequence
        self._wakeup_sequence += 1
        heapq.heappush(self.scheduled_wakeups, (at_time, seq, tick))

    def schedule_active_namespace_timeouts(self) -> None:
        """Re-arm child namespace deadlines restored from serialized state."""
        for namespace, started_at in sorted(self.state.namespace_started.items()):
            timeout = self.state.config.namespace_timeouts.get(
                slot_namespace(namespace)
            )
            if timeout is None:
                continue
            self.schedule_tick(
                TickNamespaceTimeout(
                    namespace=namespace,
                    timeout=timeout,
                    started_at=started_at,
                ),
                at_time=started_at + timeout,
            )

    def next_wakeup_timeout(self, now: float) -> float | None:
        """Calculate timeout until next scheduled wakeup.

        Returns None if no scheduled wakeups, otherwise returns
        the number of seconds until the next scheduled tick is due.
        """
        if not self.scheduled_wakeups:
            return None
        next_time, _, _ = self.scheduled_wakeups[0]
        return max(0, next_time - now)

    def pop_due_ticks(self, now: float) -> list[WorkflowTick]:
        """Pop all ticks that are due (scheduled time <= now)."""
        due = []
        while self.scheduled_wakeups and self.scheduled_wakeups[0][0] <= now:
            _, _, tick = heapq.heappop(self.scheduled_wakeups)
            due.append(tick)
        return due

    def run_worker(self, command: CommandRunWorker) -> None:
        """Queue a worker for a step function.

        Workers are stored as pending coroutines and started by the adapter
        in wait_for_next_task, which allows the adapter to control startup
        ordering for deterministic execution.
        """

        async def _run_worker() -> TickStepResult:
            worker: InProgressState | None = None
            try:
                worker = next(
                    (
                        w
                        for w in self.state.workers[command.step_id].in_progress
                        if w.worker_id == command.id
                        and w.invocation_namespace == command.invocation_namespace
                    ),
                    None,
                )
                if worker is None:
                    raise WorkflowRuntimeError(
                        f"Worker {command.id} not found in in_progress. This should not happen."
                    )
                snapshot = worker.shared_state
                step_fn: StepWorkerFunction = self.step_workers[command.step_id]
                # Bind the bare step name against the instance that owns this
                # namespace (root -> parent, child slot -> child instance).
                static_namespace = slot_namespace(command.invocation_namespace)
                instance = self.instances[static_namespace]
                # Resolve the step's state view now (the backend store/pool is
                # ready once this coroutine runs) and thread it into the step.
                state_store = self._resolve_state_view(command.invocation_namespace)

                result = await step_fn(
                    state=snapshot,
                    step_name=command.step_id.name,
                    event=command.event,
                    workflow=instance,
                    bound_events=command.bound_events,
                    namespace=command.invocation_namespace,
                    state_store=state_store,
                    retry=RetryAttempt(
                        retry_number=worker.attempts,
                        first_attempt_at=worker.first_attempt_at,
                        last_exception=worker.last_exception,
                        last_failed_at=worker.last_failed_at,
                        recovery_counts=dict(worker.recovery_counts),
                    ),
                )
                # Return result for main loop to process
                return TickStepResult(
                    step_id=command.step_id,
                    worker_id=command.id,
                    invocation_namespace=command.invocation_namespace,
                    event=command.event,
                    result=self._stamp_retry_decisions(command.step_id, worker, result),
                )
            except Exception as e:
                if _is_shutdown_error(e):
                    logger.debug("step worker interrupted by shutdown: %s", e)
                else:
                    logger.error(
                        "error running step worker function: %s", e, exc_info=True
                    )
                failed = StepWorkerFailed(
                    exception=e, failed_at=await self.adapter.get_now()
                )
                return TickStepResult(
                    step_id=command.step_id,
                    worker_id=command.id,
                    invocation_namespace=command.invocation_namespace,
                    event=command.event,
                    result=self._stamp_retry_decisions(
                        command.step_id, worker, [failed]
                    ),
                )

        self._pending_workers.append(
            PendingWorker(
                command.step_id,
                command.id,
                _run_worker(),
                invocation_namespace=command.invocation_namespace,
            )
        )

    def _stamp_retry_decisions(
        self,
        step_id: StepId,
        worker: InProgressState | None,
        results: list[StepFunctionResult],
    ) -> list[StepFunctionResult]:
        """Record the retry policy's verdict and dispatch time inside failures.

        Runs at tick creation, before the tick is journaled, so both become
        replayable data: re-reducing the journaled tick reads the verdict
        instead of re-invoking policy code (which may have changed between
        the live run and a later replay), and restores the true
        first_attempt_at instead of the rebuild-time stamp.
        """
        if worker is None:
            return results
        policy = self.state.workers[step_id].config.retry_policy
        out: list[StepFunctionResult] = []
        for result in results:
            if isinstance(result, StepWorkerFailed) and result.retry_decision is None:
                delay = _decide_retry_delay(
                    policy,
                    elapsed_time=result.failed_at - worker.first_attempt_at,
                    failures=worker.attempts + 1,
                    exception=result.exception,
                    run_id=self.adapter.run_id,
                    step_name=str(step_id),
                )
                result = result.model_copy(
                    update={
                        "retry_decision": RetryDecision(delay=delay),
                        "first_attempt_at": worker.first_attempt_at,
                    }
                )
            out.append(result)
        return out

    async def process_command(self, command: WorkflowCommand) -> None | StopEvent:
        """Process a single command returned from tick reduction."""
        if isinstance(command, CommandQueueEvent):
            self.tick_buffer.append(
                TickAddEvent(
                    event=command.event,
                    step_id=command.step_id,
                    origin_namespace=command.origin_namespace,
                    recovery_counts=dict(command.recovery_counts),
                    scope_path=command.scope_path,
                )
            )
            return None
        elif isinstance(command, CommandRunWorker):
            self.run_worker(command)
            return None
        elif isinstance(command, CommandHalt):
            await self.cleanup_tasks()
            if command.exception is not None:
                raise command.exception
        elif isinstance(command, CommandCompleteRun):
            await self.cleanup_tasks()
            return command.result
        elif isinstance(command, CommandPublishEvent):
            if command.origin_namespace:
                _set_event_origin_namespace(command.event, command.origin_namespace)
            await self.adapter.write_to_event_stream(command.event)
            return None
        elif isinstance(command, CommandFailWorkflow):
            await self.cleanup_tasks()
            raise command.exception
        elif isinstance(command, CommandCancelNamespace):
            await self._cancel_namespace_tasks(command.namespace)
            return None
        elif isinstance(command, CommandScheduleIdleCheck):
            if not self._idle_check_pending:
                self.tick_buffer.append(TickIdleCheck())
                self._idle_check_pending = True
            return None
        elif isinstance(command, CommandScheduleWaiterTimeout):
            now = await self.adapter.get_now()
            self.schedule_tick(
                TickWaiterTimeout(
                    step_id=command.step_id,
                    waiter_id=command.waiter_id,
                    invocation_namespace=command.invocation_namespace,
                ),
                at_time=now + command.timeout,
            )
            return None
        elif isinstance(command, CommandScheduleWakeup):
            self.schedule_tick(TickWakeup(due=command.at_time), at_time=command.at_time)
            return None
        elif isinstance(command, CommandScheduleNamespaceTimeout):
            self.schedule_tick(
                TickNamespaceTimeout(
                    namespace=command.namespace,
                    timeout=command.timeout,
                    started_at=command.started_at,
                ),
                at_time=command.started_at + command.timeout,
            )
            return None
        else:
            raise ValueError(f"Unknown command type: {type(command)}")

    async def _cancel_namespace_tasks(self, namespace: tuple[str, ...]) -> None:
        """Cancel worker tasks for a namespace and its descendants.

        The reducer has already cleared the namespace's journaled buffers; this
        cancels the live coroutines that back them (prefix-matched, so a
        terminated child takes its grandchildren too) so an orphaned task cannot
        complete and report into a now-empty worker slot. Pending workers not yet
        started are dropped and their coroutines closed.
        """
        # Drop not-yet-started pending workers for this namespace.
        kept: list[PendingStart] = []
        for pending in self._pending_workers:
            if isinstance(pending, PendingWorker) and namespace_startswith(
                pending.invocation_namespace, namespace
            ):
                pending.coro.close()
            else:
                kept.append(pending)
        self._pending_workers = kept

        # Cancel running worker tasks for this namespace.
        to_cancel = [
            task
            for task, key in self._task_keys.items()
            if namespace_startswith(key[1], namespace)
        ]
        for task in to_cancel:
            task.cancel()
            self.worker_tasks.discard(task)
            self._task_keys.pop(task, None)
        if to_cancel:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*to_cancel, return_exceptions=True),
                    timeout=0.5,
                )
            except Exception:
                pass

    async def cleanup_tasks(self) -> None:
        """Cancel and cleanup all running worker tasks and pending coroutines."""
        # Close pending coroutines that were never started
        for p in self._pending_workers:
            p.coro.close()
        self._pending_workers.clear()

        # Signal adapter to stop waiting
        try:
            await self.adapter.close()
        except Exception:
            pass

        # Cancel worker tasks
        for task in self.worker_tasks:
            task.cancel()

        try:
            if self.worker_tasks:
                await asyncio.wait_for(
                    asyncio.gather(*self.worker_tasks, return_exceptions=True),
                    timeout=0.5,
                )
        except Exception:
            pass

        self.worker_tasks.clear()
        self._task_keys.clear()

    async def run(
        self, start_event: Event | None = None, start_with_timeout: bool = True
    ) -> StopEvent:
        """
        Run the control loop until completion.

        This uses a sequential, deterministic design that combines timeout
        handling with event waiting in a single operation, ensuring
        deterministic ordering for replay.

        Args:
            start_event: Optional initial event to process
            start_with_timeout: Whether to start the timeout timer

        Returns:
            The final StopEvent from the workflow
        """

        # Queue initial event
        if start_event is not None:
            self.tick_buffer.append(TickAddEvent(event=start_event))

        start = await self.adapter.get_now()
        # Schedule workflow timeout if configured
        if start_with_timeout and self.workflow._timeout is not None:
            # Get initial time
            timeout_time = start + self.workflow._timeout
            self.schedule_tick(
                TickTimeout(timeout=self.workflow._timeout),
                at_time=timeout_time,
            )

        self.schedule_active_namespace_timeouts()

        # Resume any in-progress work
        self.state, commands = rewind_in_progress(self.state, start)
        for command in commands:
            try:
                await self.process_command(command)
            except Exception:
                await self.cleanup_tasks()
                raise

        # Initialize pull task (single-iteration)
        pull_task: asyncio.Task[list[WorkflowTick]] | None = None

        # Main event loop
        try:
            while True:
                # Yield to let fire-and-forget tasks run (e.g., ctx.send_event)
                await asyncio.sleep(0)

                # Get current time
                now = await self.adapter.get_now()

                # optimization, only reload "now" if any work was done
                was_buffered = bool(self.tick_buffer)
                # Drain and process buffered ticks first (from rehydration, queue_tick, etc.)
                while self.tick_buffer:
                    tick = self.tick_buffer.pop(0)
                    if isinstance(tick, TickIdleCheck):
                        if self.tick_buffer:
                            self.tick_buffer.append(tick)
                            continue
                        self._idle_check_pending = False
                    result = await self._process_tick(tick)
                    if result is not None:
                        return result

                # optimization
                if was_buffered:
                    now = await self.adapter.get_now()

                # Calculate timeout for next scheduled wakeup
                timeout = self.next_wakeup_timeout(now)

                # Build pending list: new workers + pull if needed
                pending: list[PendingStart] = list(self._pending_workers)
                self._pending_workers.clear()

                if pull_task is None:
                    pull_sequence = self._pull_sequence
                    self._pull_sequence += 1
                    pending.append(
                        PendingPull(pull_sequence, _single_pull(self.adapter))
                    )
                else:
                    pull_sequence = self._pull_sequence - 1

                # Build running list from existing tasks
                running: list[WorkerTask | PullTask] = [
                    WorkerTask(
                        key[0],
                        key[2],
                        task,
                        invocation_namespace=key[1],
                    )
                    for task in self.worker_tasks
                    for key in [self._task_keys.get(task)]
                    if key is not None
                ]
                if pull_task is not None:
                    running.append(PullTask(pull_sequence, pull_task))

                result = await self.adapter.wait_for_next_task(
                    running, pending, timeout
                )

                if len(result.started) != len(pending):
                    raise RuntimeError(
                        f"Adapter started {len(result.started)} tasks but "
                        f"{len(pending)} were pending. Every pending coroutine "
                        f"must be started."
                    )

                # Merge started tasks into tracking
                for nt in result.started:
                    if isinstance(nt, PullTask):
                        pull_task = nt.task
                    elif isinstance(nt, WorkerTask):
                        self.worker_tasks.add(nt.task)
                        self._task_keys[nt.task] = (
                            nt.step_id,
                            nt.invocation_namespace,
                            nt.worker_id,
                        )

                completed_task = result.completed

                if completed_task is None:
                    # Timeout - process scheduled ticks
                    now = await self.adapter.get_now()
                    for due_tick in self.pop_due_ticks(now):
                        self.tick_buffer.append(due_tick)
                    continue

                # Process the single completed task
                if completed_task is pull_task:
                    # Pull task completed
                    try:
                        pull_tick = completed_task.result()
                    except asyncio.CancelledError:
                        pull_task = None
                    except Exception:
                        logger.exception("Pull task failed", exc_info=True)
                        pull_task = None
                    else:
                        pull_task = None
                        if pull_tick is not None:
                            self.tick_buffer.append(pull_tick)
                else:
                    # Worker task completed
                    self.worker_tasks.discard(completed_task)
                    self._task_keys.pop(completed_task, None)
                    try:
                        tick_result = completed_task.result()
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        logger.exception(
                            "Worker task failed unexpectedly", exc_info=True
                        )
                    else:
                        # Check if this worker returned a *root* StopEvent - if
                        # so, cancel other workers immediately to prevent them
                        # from writing to the event stream after workflow
                        # completion. A child's StopEvent is only a boundary
                        # event, so it must not cancel the parent's workers.
                        for res in tick_result.result:
                            if (
                                isinstance(res, StepWorkerResult)
                                and isinstance(res.result, StopEvent)
                                and tick_result.step_id.is_root
                            ):
                                await self.cleanup_tasks()
                                break
                        self.tick_buffer.append(tick_result)

        finally:
            # Cancel pull task if running
            if pull_task is not None:
                pull_task.cancel()
                try:
                    await pull_task
                except (asyncio.CancelledError, Exception):
                    pass
            await self.cleanup_tasks()

    async def _process_tick(self, tick: WorkflowTick) -> StopEvent | None:
        """Process a single tick and return StopEvent if workflow completes."""
        try:
            start = await self.adapter.get_now()
            tick = prepare_tick_for_reduce(tick, self.state)
            self.state, commands = _reduce_tick(
                tick, self.state, start, run_id=self.adapter.run_id
            )
        except Exception:
            await self.cleanup_tasks()
            logger.error(
                "Unexpected error in internal control loop of workflow. This shouldn't happen. ",
                exc_info=True,
            )
            raise

        await self.adapter.on_tick(tick)

        for command in commands:
            try:
                result = await self.process_command(command)
            except Exception:
                await self.cleanup_tasks()
                raise

            if result is not None:
                return result

        await self.adapter.after_tick(tick)
        return None


async def control_loop(
    start_event: Event | None,
    init_state: BrokerState | None,
    run_id: str,
) -> StopEvent:
    """
    The main async control loop for a workflow run.
    """
    # Consume the RunContext immediately so the container's strong reference
    # to the workflow graph is dropped before any step gets a chance to schedule
    # an asyncio handle whose Context snapshot would otherwise pin it.
    run = consume_current_run()
    state = init_state or BrokerState.from_workflow(run.workflow)
    runner = _ControlLoopRunner(
        run.workflow,
        run.run_adapter,
        run.context,
        run.steps,
        state,
        instances=run.workflow._namespace_instances(),
    )
    return await runner.run(start_event=start_event)
