# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import hashlib
import heapq
import inspect
import logging
import time
from collections.abc import AsyncIterable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from workflows.errors import (
    WorkflowCancelledByUser,
    WorkflowRuntimeError,
    WorkflowTimeoutError,
)
from workflows.events import (
    Event,
    IdleReleasedEvent,
    InputRequiredEvent,
    StartEvent,
    StepFailedEvent,
    StepState,
    StepStateChanged,
    StopEvent,
    UnhandledEvent,
    WorkflowCancelledEvent,
    WorkflowFailedEvent,
    WorkflowIdleEvent,
    WorkflowTimedOutEvent,
    set_event_origin_namespace,
)
from workflows.runtime.types.commands import (
    CommandCompleteRun,
    CommandFailWorkflow,
    CommandHalt,
    CommandPublishEvent,
    CommandQueueEvent,
    CommandRunWorker,
    CommandScheduleIdleCheck,
    CommandScheduleWaiterTimeout,
    WorkflowCommand,
    indicates_exit,
)
from workflows.runtime.types.internal_state import (
    BrokerState,
    EventAttempt,
    InProgressState,
    InternalStepWorkerState,
)
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
    AddCollectedEvent,
    AddWaiter,
    DeleteCollectedEvent,
    DeleteWaiter,
    RetryAttempt,
    StepWorkerFailed,
    StepWorkerResult,
    StepWorkerState,
    StepWorkerWaiter,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickIdleCheck,
    TickIdleRelease,
    TickPublishEvent,
    TickStepResult,
    TickTimeout,
    TickWaiterTimeout,
    WorkflowTick,
)
from workflows.workflow import Workflow


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


if TYPE_CHECKING:
    from workflows.context.context import Context
    from workflows.runtime.types.step_function import StepWorkerFunction


logger = logging.getLogger(__name__)


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
        # Map from worker task to (step_id, worker_id) key
        self._task_keys: dict[asyncio.Task[TickStepResult], tuple[StepId, int]] = {}
        # Whether a TickIdleCheck is currently in tick_buffer
        self._idle_check_pending = False
        # Pending worker coroutines not yet started (started by adapter in wait_for_next_task)
        self._pending_workers: list[PendingStart] = []

    def schedule_tick(self, tick: WorkflowTick, at_time: float) -> None:
        """Schedule a tick to be processed at a specific time."""
        seq = self._wakeup_sequence
        self._wakeup_sequence += 1
        heapq.heappush(self.scheduled_wakeups, (at_time, seq, tick))

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
            try:
                worker = next(
                    (
                        w
                        for w in self.state.workers[command.step_id].in_progress
                        if w.worker_id == command.id
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
                instance = self.instances[command.step_id.namespace]

                result = await step_fn(
                    state=snapshot,
                    step_name=command.step_id.name,
                    event=command.event,
                    workflow=instance,
                    namespace=command.step_id.namespace,
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
                    event=command.event,
                    result=result,
                )
            except Exception as e:
                if _is_shutdown_error(e):
                    logger.debug("step worker interrupted by shutdown: %s", e)
                else:
                    logger.error(
                        "error running step worker function: %s", e, exc_info=True
                    )
                return TickStepResult(
                    step_id=command.step_id,
                    worker_id=command.id,
                    event=command.event,
                    result=[
                        StepWorkerFailed(
                            exception=e, failed_at=await self.adapter.get_now()
                        )
                    ],
                )

        self._pending_workers.append(
            PendingWorker(command.step_id, command.id, _run_worker())
        )

    async def process_command(self, command: WorkflowCommand) -> None | StopEvent:
        """Process a single command returned from tick reduction."""
        if isinstance(command, CommandQueueEvent):
            event = TickAddEvent(
                event=command.event,
                step_id=command.step_id,
                origin_namespace=command.origin_namespace,
                attempts=command.attempts,
                first_attempt_at=command.first_attempt_at,
                last_exception=command.last_exception,
                last_failed_at=command.last_failed_at,
                recovery_counts=dict(command.recovery_counts),
            )
            if command.delay is not None and command.delay > 0:
                now = await self.adapter.get_now()
                self.schedule_tick(event, at_time=now + command.delay)
            else:
                self.tick_buffer.append(event)
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
                set_event_origin_namespace(command.event, command.origin_namespace)
            await self.adapter.write_to_event_stream(command.event)
            return None
        elif isinstance(command, CommandFailWorkflow):
            await self.cleanup_tasks()
            raise command.exception
        elif isinstance(command, CommandScheduleIdleCheck):
            if not self._idle_check_pending:
                self.tick_buffer.append(TickIdleCheck())
                self._idle_check_pending = True
            return None
        elif isinstance(command, CommandScheduleWaiterTimeout):
            now = await self.adapter.get_now()
            self.schedule_tick(
                TickWaiterTimeout(step_id=command.step_id, waiter_id=command.waiter_id),
                at_time=now + command.timeout,
            )
            return None
        else:
            raise ValueError(f"Unknown command type: {type(command)}")

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

        # Resume any in-progress work
        self.state, commands = rewind_in_progress(self.state, start)
        for command in commands:
            try:
                await self.process_command(command)
            except Exception:
                await self.cleanup_tasks()
                raise

        # Initialize pull task (single-iteration)
        pull_task: asyncio.Task[WorkflowTick | None] | None = None

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
                    WorkerTask(key[0], key[1], task)
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
                        self._task_keys[nt.task] = (nt.step_id, nt.worker_id)

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


def rebuild_state_from_ticks(
    state: BrokerState,
    ticks: list[WorkflowTick],
) -> BrokerState:
    """Rebuild the state from a list of ticks.

    When reconstructing state (e.g., for checkpointing), we must first apply
    rewind_in_progress() to match what happens at runtime when resuming a workflow.
    This clears in_progress, moves events back to the queue, and then re-assigns
    new worker IDs starting from 0.

    Without this, resuming a workflow and then checkpointing again would fail
    because the original in_progress worker IDs don't match the new worker IDs
    assigned after rewind.
    """
    # Apply rewind_in_progress to match what happens at runtime when resuming.
    # This re-assigns worker IDs so they align with the ticks that were recorded
    # after the workflow was resumed.
    state, _ = rewind_in_progress(state, time.time())

    # Replay ticks to rebuild state
    for tick in ticks:
        state, _ = _reduce_tick(
            tick, state, time.time()
        )  # somewhat broken kludge on the timestamps, need to move these to ticks
    return state


ExitCommand = CommandCompleteRun | CommandFailWorkflow | CommandHalt


@dataclass
class ReplayResult:
    """Result of replaying a tick stream.

    Attributes:
        state: Rebuilt broker state after applying all ticks.
        exit_command: The last exit-indicating command emitted during replay,
            or None if the stream never terminated. Lets callers classify
            terminal outcome (success / failure / cancel / timeout) using the
            same command the runtime would have produced, without a second
            pass over the ticks.
    """

    state: BrokerState
    exit_command: ExitCommand | None = None


async def replay_ticks_stream(
    state: BrokerState,
    ticks: AsyncIterable[WorkflowTick],
) -> ReplayResult:
    """Replay a tick stream, returning state plus the last exit-indicating command.

    The reducer already emits CommandCompleteRun / CommandFailWorkflow /
    CommandHalt when it processes terminal ticks; this surfaces them instead
    of discarding, so callers can classify terminal outcome (success /
    failure / cancel / timeout) without a second pass over the ticks.
    """
    state, _ = rewind_in_progress(state, time.time())
    exit_command: ExitCommand | None = None
    async for tick in ticks:
        state, commands = _reduce_tick(tick, state, time.time())
        for command in commands:
            if isinstance(
                command, (CommandCompleteRun, CommandFailWorkflow, CommandHalt)
            ):
                # Last wins: a successful retry supersedes earlier failures.
                exit_command = command
    return ReplayResult(state=state, exit_command=exit_command)


async def rebuild_state_from_ticks_stream(
    state: BrokerState,
    ticks: AsyncIterable[WorkflowTick],
) -> BrokerState:
    """Streaming variant of :func:`rebuild_state_from_ticks`.

    Thin wrapper over :func:`replay_ticks_stream` that discards the exit
    command. Prefer ``replay_ticks_stream`` when you need terminal info.
    """
    return (await replay_ticks_stream(state, ticks)).state


def _reduce_tick(
    tick: WorkflowTick,
    init: BrokerState,
    now_seconds: float,
    run_id: str | None = None,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    if isinstance(tick, TickStepResult):
        state, commands = _process_step_result_tick(tick, init, now_seconds, run_id)
    elif isinstance(tick, TickAddEvent):
        state, commands = _process_add_event_tick(tick, init, now_seconds)
    elif isinstance(tick, TickCancelRun):
        state, commands = _process_cancel_run_tick(tick, init)
    elif isinstance(tick, TickIdleRelease):
        # Return early — idle release does not schedule idle checks
        return init, [CommandCompleteRun(result=IdleReleasedEvent())]
    elif isinstance(tick, TickPublishEvent):
        state, commands = _process_publish_event_tick(tick, init)
    elif isinstance(tick, TickTimeout):
        state, commands = _process_timeout_tick(tick, init)
    elif isinstance(tick, TickWaiterTimeout):
        state, commands = _process_waiter_timeout_tick(tick, init, now_seconds)
    elif isinstance(tick, TickIdleCheck):
        # Return early — idle check ticks don't schedule further idle checks
        if _check_idle_state(init):
            return init, [CommandPublishEvent(WorkflowIdleEvent())]
        return init, []
    else:
        raise ValueError(f"Unknown tick type: {type(tick)}")

    # After any non-idle-check tick, schedule an idle check if state is quiescent
    if _check_idle_state(state):
        commands.append(CommandScheduleIdleCheck())

    return state, commands


def rewind_in_progress(
    state: BrokerState,
    now_seconds: float,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Rewind the in_progress state, extracting commands to re-initiate the workers"""
    state = state.deepcopy()
    commands: list[WorkflowCommand] = []
    for step_id, step_state in sorted(state.workers.items(), key=lambda x: str(x[0])):
        for in_progress in step_state.in_progress:
            step_state.queue.insert(
                0,
                EventAttempt(
                    event=in_progress.event,
                    attempts=in_progress.attempts,
                    first_attempt_at=in_progress.first_attempt_at,
                    last_exception=in_progress.last_exception,
                    last_failed_at=in_progress.last_failed_at,
                    recovery_counts=dict(in_progress.recovery_counts),
                ),
            )
        step_state.in_progress = []
        while (
            len(step_state.queue) > 0
            and len(step_state.in_progress) < step_state.config.num_workers
        ):
            event = step_state.queue.pop(0)
            commands.extend(
                _add_or_enqueue_event(event, step_id, step_state, now_seconds)
            )
    return state, commands


def _check_idle_state(state: BrokerState) -> bool:
    """Returns True if workflow is idle (no work can advance internally).

    A workflow is idle when:
    1. The workflow is running (hasn't completed/failed/cancelled)
    2. All steps have no pending events in their queues
    3. All steps have no workers currently executing
    """
    if not state.is_running:
        return False

    for worker_state in state.workers.values():
        if worker_state.queue or worker_state.in_progress:
            return False

    return True


def _process_step_result_tick(
    tick: TickStepResult,
    init: BrokerState,
    now_seconds: float,
    run_id: str | None = None,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """
    processes the results from a step function execution
    """
    state = init.deepcopy()
    commands: list[WorkflowCommand] = []
    worker_state = state.workers[tick.step_id]
    # get the current execution details and mark it as no longer in progress
    this_execution = next(
        (w for w in worker_state.in_progress if w.worker_id == tick.worker_id), None
    )
    if this_execution is None:
        # this should not happen unless there's a logic bug in the control loop
        raise ValueError(f"Worker {tick.worker_id} not found in in_progress")
    output_event_name: str | None = None

    did_complete_step = bool(
        [x for x in tick.result if isinstance(x, StepWorkerResult)]
    )
    step_no_longer_in_progress = True

    for result in tick.result:
        if isinstance(result, StepWorkerResult):
            output_event_name = str(type(result.result))
            if isinstance(result.result, StopEvent) and tick.step_id.is_root:
                # huzzah! The (root) workflow has completed
                commands.append(
                    CommandPublishEvent(event=result.result)
                )  # stop event always published to the stream
                state.is_running = False
                # Clear collected_events and collected_waiters since workflow is complete
                for worker in state.workers.values():
                    worker.collected_events.clear()
                    worker.collected_waiters.clear()
                commands.append(CommandCompleteRun(result=result.result))
            elif isinstance(result.result, StopEvent):
                # A child's StopEvent is a *boundary* event, not a run terminal:
                # terminate the child namespace (clear its buffers) and re-inject
                # the event into the parent namespace as an ordinary routable
                # event. The run keeps running; only the root StopEvent above
                # completes it. The event is NOT published to the stream — that
                # would signal completion to root stream consumers (Phase 5 adds
                # opt-in child streaming).
                parent_namespace = tick.step_id.namespace[:-1]
                for child_step_id, worker in state.workers.items():
                    if child_step_id.namespace == tick.step_id.namespace:
                        worker.collected_events.clear()
                        worker.collected_waiters.clear()
                commands.append(
                    CommandQueueEvent(
                        event=result.result,
                        origin_namespace=parent_namespace,
                        recovery_counts=dict(this_execution.recovery_counts),
                    )
                )
            elif isinstance(result.result, Event):
                # queue any subsequent events
                # human input required are automatically published to the stream
                if isinstance(result.result, InputRequiredEvent):
                    commands.append(
                        CommandPublishEvent(
                            event=result.result,
                            origin_namespace=tick.step_id.namespace,
                        )
                    )
                commands.append(
                    CommandQueueEvent(
                        event=result.result,
                        origin_namespace=tick.step_id.namespace,
                        recovery_counts=dict(this_execution.recovery_counts),
                    )
                )
            elif result.result is None:
                # None means skip
                pass
            else:
                logger.warning(
                    f"Unknown result type returned from step function ({tick.step_id}): {type(result.result)}"
                )
        elif isinstance(result, StepWorkerFailed):
            # Schedule a retry if permitted, otherwise fail the workflow
            retries = worker_state.config.retry_policy
            failures = this_execution.attempts + 1
            elapsed_time = result.failed_at - this_execution.first_attempt_at
            jitter_seed = (
                int(
                    hashlib.sha256(
                        f"{run_id}:{tick.step_id}:{failures}".encode()
                    ).hexdigest(),
                    16,
                )
                & 0xFFFF_FFFF
                if run_id is not None
                else None
            )
            if retries is not None:
                _next_params = inspect.signature(retries.next).parameters
                _seed_kwarg = {"seed": jitter_seed} if "seed" in _next_params else {}
                delay = retries.next(
                    elapsed_time, failures, result.exception, **_seed_kwarg
                )
            else:
                delay = None
            if delay is not None:
                commands.append(
                    CommandQueueEvent(
                        event=tick.event,
                        delay=delay,
                        step_id=tick.step_id,
                        attempts=this_execution.attempts + 1,
                        first_attempt_at=this_execution.first_attempt_at,
                        last_exception=result.exception,
                        last_failed_at=result.failed_at,
                        recovery_counts=dict(this_execution.recovery_counts),
                    )
                )
            else:
                exception = result.exception
                total_attempts = this_execution.attempts + 1
                elapsed = result.failed_at - this_execution.first_attempt_at

                # Catch-error handlers are a root-workflow concept, keyed by
                # bare step name; only root steps route to them.
                handler_name = (
                    state.config.handler_for_step.get(tick.step_id.name)
                    if tick.step_id.is_root
                    else None
                )
                handler = (
                    state.config.catch_error_handlers.get(handler_name)
                    if handler_name is not None
                    else None
                )
                current_count = (
                    this_execution.recovery_counts.get(handler.step_name, 0)
                    if handler is not None
                    else 0
                )
                new_count = current_count + 1
                should_route = (
                    handler is not None and new_count <= handler.max_recoveries
                )
                if should_route and handler is not None:
                    # Route to the catch-error handler. Keep workflow running so
                    # the handler can produce either a StopEvent or a new failure.
                    step_failed_event = StepFailedEvent(
                        step_name=str(tick.step_id),
                        input_event=tick.event,
                        exception=exception,
                        attempts=total_attempts,
                        elapsed_seconds=elapsed,
                        failed_at=datetime.fromtimestamp(
                            result.failed_at, tz=timezone.utc
                        ),
                    )
                    commands.append(
                        CommandQueueEvent(
                            event=step_failed_event,
                            step_id=StepId.root(handler.step_name),
                            recovery_counts={
                                **this_execution.recovery_counts,
                                handler.step_name: new_count,
                            },
                        )
                    )
                else:
                    # Publish a WorkflowFailedEvent to inform stream consumers about the failure
                    state.is_running = False
                    commands.append(
                        CommandPublishEvent(
                            event=WorkflowFailedEvent(
                                step_name=str(tick.step_id),
                                exception=exception,
                                attempts=total_attempts,
                                elapsed_seconds=elapsed,
                            )
                        )
                    )
                    commands.append(
                        CommandFailWorkflow(step_id=tick.step_id, exception=exception)
                    )
        elif isinstance(result, AddCollectedEvent):
            # The current state of collected events.
            collected_events = state.workers[tick.step_id].collected_events.setdefault(
                result.event_id, []
            )
            # the events snapshot that was sent with the step function execution that yielded this result
            sent_events = this_execution.shared_state.collected_events.get(
                result.event_id, []
            )
            if len(collected_events) > len(sent_events):
                # rerun it, and don't append now to ensure serializability
                # updating the run state
                step_no_longer_in_progress = False
                updated_state = replace(
                    this_execution.shared_state,
                    collected_events={
                        x: list(y)
                        for x, y in state.workers[tick.step_id].collected_events.items()
                    },
                )
                this_execution.shared_state = updated_state
                commands.append(
                    CommandRunWorker(
                        step_id=tick.step_id,
                        event=result.event,
                        id=this_execution.worker_id,
                    )
                )
            else:
                collected_events.append(result.event)
        elif isinstance(result, DeleteCollectedEvent):
            if did_complete_step:  # allow retries to grab the events
                # indicates that a run has successfully collected its events, and they can be deleted from the collected events state
                state.workers[tick.step_id].collected_events.pop(result.event_id, None)
        elif isinstance(result, AddWaiter):
            # indicates that a run has added a waiter to the collected waiters state
            existing = next(
                (
                    (i)
                    for i, x in enumerate(worker_state.collected_waiters)
                    if x.waiter_id == result.waiter_id
                ),
                None,
            )
            new_waiter = StepWorkerWaiter(
                waiter_id=result.waiter_id,
                event=this_execution.event,
                waiting_for_event=result.event_type,
                requirements=result.requirements,
                has_requirements=bool(len(result.requirements)),
                resolved_event=None,
            )
            if existing is not None:
                worker_state.collected_waiters[existing] = new_waiter
            else:
                worker_state.collected_waiters.append(new_waiter)
                if result.waiter_event:
                    commands.append(
                        CommandPublishEvent(
                            event=result.waiter_event,
                            origin_namespace=tick.step_id.namespace,
                        )
                    )
                if result.timeout is not None:
                    commands.append(
                        CommandScheduleWaiterTimeout(
                            step_id=tick.step_id,
                            waiter_id=result.waiter_id,
                            timeout=result.timeout,
                        )
                    )

        elif isinstance(result, DeleteWaiter):
            if did_complete_step:  # allow retries to grab the waiter events
                # indicates that a run has obtained the waiting event, and it can be deleted from the collected waiters state
                to_remove = result.waiter_id
                waiters = state.workers[tick.step_id].collected_waiters
                item = next(filter(lambda w: w.waiter_id == to_remove, waiters), None)
                if item is not None:
                    waiters.remove(item)
        else:
            raise ValueError(f"Unknown result type: {type(result)}")

    is_completed = len([x for x in commands if indicates_exit(x)]) > 0
    if step_no_longer_in_progress:
        commands.insert(
            0,
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.NOT_RUNNING,
                    name=str(tick.step_id),
                    input_event_name=str(type(tick.event)),
                    output_event_name=output_event_name,
                    worker_id=str(tick.worker_id),
                ),
                origin_namespace=tick.step_id.namespace,
            ),
        )
        worker_state.in_progress.remove(this_execution)
    # enqueue next events if there are any
    if not is_completed:
        while (
            len(worker_state.queue) > 0
            and len(worker_state.in_progress) < worker_state.config.num_workers
        ):
            event = worker_state.queue.pop(0)
            subcommands = _add_or_enqueue_event(
                event, tick.step_id, worker_state, now_seconds
            )
            commands.extend(subcommands)

    return state, commands


def _add_or_enqueue_event(
    event: EventAttempt,
    step_id: StepId,
    state: InternalStepWorkerState,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """
    Small helper to assist in adding an event to a step worker state, or enqueuing it if it's not accepted.
    Note! This mutates the state, assuming that its already been deepcopied in an outer scope.
    """
    commands: list[WorkflowCommand] = []
    # Determine if there is available capacity based on in_progress workers
    has_space = len(state.in_progress) < state.config.num_workers
    if has_space:
        # Assign the smallest available worker id
        used = set(x.worker_id for x in state.in_progress)
        id_candidates = [i for i in range(state.config.num_workers) if i not in used]
        id = id_candidates[0]
        state_copy = state._deepcopy()
        shared_state: StepWorkerState = StepWorkerState(
            step_name=step_id.name,
            collected_events=state_copy.collected_events,
            collected_waiters=state_copy.collected_waiters,
        )
        state.in_progress.append(
            InProgressState(
                event=event.event,
                worker_id=id,
                shared_state=shared_state,
                attempts=event.attempts or 0,
                first_attempt_at=event.first_attempt_at or now_seconds,
                last_exception=event.last_exception,
                last_failed_at=event.last_failed_at,
                recovery_counts=dict(event.recovery_counts),
            )
        )
        commands.append(CommandRunWorker(step_id=step_id, event=event.event, id=id))
        commands.append(
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.RUNNING,
                    name=str(step_id),
                    input_event_name=type(event.event).__name__,
                    worker_id=str(id),
                ),
                origin_namespace=step_id.namespace,
            )
        )
    else:
        commands.append(
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.PREPARING,
                    name=str(step_id),
                    input_event_name=type(event.event).__name__,
                    worker_id="<enqueued>",
                ),
                origin_namespace=step_id.namespace,
            )
        )
        state.queue.append(event)
    return commands


def _event_routes_to(
    origin_namespace: tuple[str, ...],
    target_namespace: tuple[str, ...],
    event: Event,
) -> bool:
    """Whether a type-routed event from ``origin_namespace`` reaches a step in
    ``target_namespace``.

    An event stays within the namespace that emitted it, except that a
    ``StartEvent`` may cross *down* into a direct child namespace (that is how a
    parent triggers a child). A child's ``StopEvent`` crossing back *up* is
    handled by re-injecting it with the parent namespace as its origin, so it is
    just an ordinary same-namespace route here.
    """
    if target_namespace == origin_namespace:
        return True
    if (
        isinstance(event, StartEvent)
        and len(target_namespace) == len(origin_namespace) + 1
        and target_namespace[: len(origin_namespace)] == origin_namespace
    ):
        return True
    return False


def _process_add_event_tick(
    tick: TickAddEvent, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    state = init.deepcopy()
    # iterate through the steps, and add to steps work queue if it's accepted.
    commands: list[WorkflowCommand] = []
    handled = False
    if isinstance(tick.event, StartEvent):
        state.is_running = True

    # First, check if the event resolves any waiters. Track which steps were
    # woken via waiter resolution so we don't also route the event to them
    # as a normal accepted event (which would cause duplicate processing).
    waiter_resolved_steps: set[StepId] = set()
    for step_id, step_config in state.config.steps.items():
        # Scope waiter resolution to the event's namespace (exact step when the
        # tick is targeted), so a child's waiter is only woken by events in its
        # own namespace.
        if tick.step_id is not None:
            if tick.step_id != step_id:
                continue
        elif not _event_routes_to(tick.origin_namespace, step_id.namespace, tick.event):
            continue
        wait_conditions = state.workers[step_id].collected_waiters
        for wait_condition in wait_conditions:
            is_match = type(tick.event) is wait_condition.waiting_for_event
            is_match = is_match and all(
                getattr(tick.event, k, None) == v
                for k, v in wait_condition.requirements.items()
            )
            if is_match:
                handled = True
                waiter_resolved_steps.add(step_id)
                wait_condition.resolved_event = tick.event
                subcommands = _add_or_enqueue_event(
                    EventAttempt(event=wait_condition.event),
                    step_id,
                    state.workers[step_id],
                    now_seconds,
                )
                commands.extend(subcommands)

    # Then route to accepting steps, skipping any that were already woken
    # via waiter resolution above.
    for step_id, step_config in state.config.steps.items():
        if step_id in waiter_resolved_steps:
            continue
        is_accepted = type(tick.event) in step_config.accepted_events
        if tick.step_id is not None:
            routes = tick.step_id == step_id
        else:
            routes = _event_routes_to(
                tick.origin_namespace, step_id.namespace, tick.event
            )
        if is_accepted and routes:
            handled = True
            subcommands = _add_or_enqueue_event(
                EventAttempt(
                    event=tick.event,
                    attempts=tick.attempts,
                    first_attempt_at=tick.first_attempt_at,
                    last_exception=tick.last_exception,
                    last_failed_at=tick.last_failed_at,
                    recovery_counts=dict(tick.recovery_counts),
                ),
                step_id,
                state.workers[step_id],
                now_seconds,
            )
            commands.extend(subcommands)
    if not handled:
        # InputRequiredEvent subclasses are intentionally designed to be handled
        # externally by human consumers, not by workflow steps. Don't emit
        # UnhandledEvent for these since they're working as intended.
        if not isinstance(tick.event, InputRequiredEvent):
            event_cls = type(tick.event)
            commands.append(
                CommandPublishEvent(
                    UnhandledEvent(
                        event_type=event_cls.__name__,
                        qualified_name=f"{event_cls.__module__}.{event_cls.__name__}",
                        step_name=str(tick.step_id)
                        if tick.step_id is not None
                        else None,
                        idle=_check_idle_state(state),
                    )
                )
            )
    return state, commands


def _process_cancel_run_tick(
    tick: TickCancelRun, init: BrokerState
) -> tuple[BrokerState, list[WorkflowCommand]]:
    state = init.deepcopy()
    # Retain running state for resumption.
    return state, [
        CommandPublishEvent(event=WorkflowCancelledEvent()),
        CommandHalt(exception=WorkflowCancelledByUser()),
    ]


def _process_publish_event_tick(
    tick: TickPublishEvent, init: BrokerState
) -> tuple[BrokerState, list[WorkflowCommand]]:
    # doesn't affect state. Pass through as publish command
    return init, [CommandPublishEvent(event=tick.event)]


def _process_timeout_tick(
    tick: TickTimeout, init: BrokerState
) -> tuple[BrokerState, list[WorkflowCommand]]:
    state = init.deepcopy()
    state.is_running = False
    active_steps = [
        str(step_id)
        for step_id, worker_state in init.workers.items()
        if len(worker_state.in_progress) > 0
    ]
    steps_info = (
        "Currently active steps: " + ", ".join(active_steps)
        if active_steps
        else "No steps active"
    )
    return state, [
        CommandPublishEvent(
            event=WorkflowTimedOutEvent(
                timeout=tick.timeout,
                active_steps=active_steps,
            )
        ),
        CommandHalt(
            exception=WorkflowTimeoutError(
                f"Operation timed out after {tick.timeout} seconds. {steps_info}"
            )
        ),
    ]


def _process_waiter_timeout_tick(
    tick: TickWaiterTimeout, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    state = init.deepcopy()
    commands: list[WorkflowCommand] = []
    if tick.step_id not in state.workers:
        return state, commands
    worker_state = state.workers[tick.step_id]
    waiter = next(
        (w for w in worker_state.collected_waiters if w.waiter_id == tick.waiter_id),
        None,
    )
    # Only act if the waiter is still pending (not yet resolved by an event)
    if waiter is None or waiter.resolved_event is not None:
        return state, commands
    waiter.timed_out = True
    subcommands = _add_or_enqueue_event(
        EventAttempt(event=waiter.event),
        tick.step_id,
        worker_state,
        now_seconds,
    )
    commands.extend(subcommands)
    return state, commands
