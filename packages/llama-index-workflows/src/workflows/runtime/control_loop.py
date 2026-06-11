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
from typing import TYPE_CHECKING, Any

from workflows._event_matching import event_matches, step_accepts_event
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
)
from workflows.retry_policy import RetryPolicy
from workflows.runtime.types.commands import (
    CommandCompleteRun,
    CommandFailWorkflow,
    CommandHalt,
    CommandPublishEvent,
    CommandQueueEvent,
    CommandRunWorker,
    CommandScheduleIdleCheck,
    CommandScheduleWaiterTimeout,
    CommandScheduleWakeup,
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
    RetryDecision,
    StepFunctionResult,
    StepWorkerFailed,
    StepWorkerResult,
    StepWorkerState,
    StepWorkerWaiter,
)
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickIdleCheck,
    TickIdleRelease,
    TickPublishEvent,
    TickStepResult,
    TickTimeout,
    TickWaiterTimeout,
    TickWakeup,
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
        step_workers: dict[str, StepWorkerFunction],
        init_state: BrokerState,
    ):
        self.workflow = workflow
        self.adapter = adapter
        self.context = context
        self.step_workers = step_workers
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
        # Map from worker task to (step_name, worker_id) key
        self._task_keys: dict[asyncio.Task[TickStepResult], tuple[str, int]] = {}
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
            worker: InProgressState | None = None
            try:
                worker = next(
                    (
                        w
                        for w in self.state.workers[command.step_name].in_progress
                        if w.worker_id == command.id
                    ),
                    None,
                )
                if worker is None:
                    raise WorkflowRuntimeError(
                        f"Worker {command.id} not found in in_progress. This should not happen."
                    )
                snapshot = worker.shared_state
                step_fn: StepWorkerFunction = self.step_workers[command.step_name]

                result = await step_fn(
                    state=snapshot,
                    step_name=command.step_name,
                    event=command.event,
                    workflow=self.workflow,
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
                    step_name=command.step_name,
                    worker_id=command.id,
                    event=command.event,
                    result=self._stamp_retry_decisions(
                        command.step_name, worker, result
                    ),
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
                    step_name=command.step_name,
                    worker_id=command.id,
                    event=command.event,
                    result=self._stamp_retry_decisions(
                        command.step_name, worker, [failed]
                    ),
                )

        self._pending_workers.append(
            PendingWorker(command.step_name, command.id, _run_worker())
        )

    def _stamp_retry_decisions(
        self,
        step_name: str,
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
        policy = self.state.workers[step_name].config.retry_policy
        out: list[StepFunctionResult] = []
        for result in results:
            if isinstance(result, StepWorkerFailed) and result.retry_decision is None:
                delay = _decide_retry_delay(
                    policy,
                    elapsed_time=result.failed_at - worker.first_attempt_at,
                    failures=worker.attempts + 1,
                    exception=result.exception,
                    run_id=self.adapter.run_id,
                    step_name=step_name,
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
                    step_name=command.step_name,
                    attempts=command.attempts,
                    first_attempt_at=command.first_attempt_at,
                    last_exception=command.last_exception,
                    last_failed_at=command.last_failed_at,
                    recovery_counts=dict(command.recovery_counts),
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
                TickWaiterTimeout(
                    step_name=command.step_name, waiter_id=command.waiter_id
                ),
                at_time=now + command.timeout,
            )
            return None
        elif isinstance(command, CommandScheduleWakeup):
            self.schedule_tick(TickWakeup(due=command.at_time), at_time=command.at_time)
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
                        self._task_keys[nt.task] = (nt.step_name, nt.worker_id)

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
                        # Check if this worker returned a StopEvent - if so,
                        # cancel other workers immediately to prevent them from
                        # writing to the event stream after workflow completion
                        for res in tick_result.result:
                            if isinstance(res, StepWorkerResult) and isinstance(
                                res.result, StopEvent
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
        run.workflow, run.run_adapter, run.context, run.steps, state
    )
    return await runner.run(start_event=start_event)


def rebuild_state_from_ticks(
    state: BrokerState,
    ticks: list[WorkflowTick],
    run_id: str | None = None,
) -> BrokerState:
    """Rebuild the state from a list of ticks.

    When reconstructing state (e.g., for checkpointing), we must first apply
    rewind_in_progress() to match what happens at runtime when resuming a workflow.
    This clears in_progress, moves events back to the queue, and then re-assigns
    new worker IDs starting from 0.

    Without this, resuming a workflow and then checkpointing again would fail
    because the original in_progress worker IDs don't match the new worker IDs
    assigned after rewind.

    run_id must match the live run's id whenever it is known: it seeds retry
    jitter, so replaying a failure tick recomputes the same delay (and thus
    the same not_before) the live run journaled in its TickWakeup.
    """
    # Apply rewind_in_progress to match what happens at runtime when resuming.
    # This re-assigns worker IDs so they align with the ticks that were recorded
    # after the workflow was resumed.
    state, _ = rewind_in_progress(state, time.time())

    # Replay ticks to rebuild state
    for tick in ticks:
        state, _ = _reduce_tick(
            tick, state, time.time(), run_id=run_id
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
    run_id: str | None = None,
) -> ReplayResult:
    """Replay a tick stream, returning state plus the last exit-indicating command.

    The reducer already emits CommandCompleteRun / CommandFailWorkflow /
    CommandHalt when it processes terminal ticks; this surfaces them instead
    of discarding, so callers can classify terminal outcome (success /
    failure / cancel / timeout) without a second pass over the ticks.

    run_id must match the live run's id whenever it is known: it seeds retry
    jitter, so replaying a failure tick recomputes the same delay (and thus
    the same not_before) the live run journaled in its TickWakeup.
    """
    state, _ = rewind_in_progress(state, time.time())
    exit_command: ExitCommand | None = None
    async for tick in ticks:
        state, commands = _reduce_tick(tick, state, time.time(), run_id=run_id)
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
    run_id: str | None = None,
) -> BrokerState:
    """Streaming variant of :func:`rebuild_state_from_ticks`.

    Thin wrapper over :func:`replay_ticks_stream` that discards the exit
    command. Prefer ``replay_ticks_stream`` when you need terminal info.
    """
    return (await replay_ticks_stream(state, ticks, run_id=run_id)).state


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
    elif isinstance(tick, TickWakeup):
        state, commands = _process_wakeup_tick(tick, init, now_seconds)
    else:
        raise ValueError(f"Unknown tick type: {type(tick)}")

    # After any non-idle-check tick, schedule an idle check if state is quiescent
    if _check_idle_state(state):
        commands.append(CommandScheduleIdleCheck())

    return state, commands


def _is_eligible(attempt: EventAttempt) -> bool:
    """Delayed attempts (not_before set) only become eligible via TickWakeup.

    Eligibility is a state flip recorded in the tick journal, never a clock
    comparison: the reducer must make identical dispatch decisions when
    replaying journaled ticks as it did during the live run.
    """
    return attempt.not_before is None


def _decide_retry_delay(
    policy: RetryPolicy | None,
    *,
    elapsed_time: float,
    failures: int,
    exception: Exception,
    run_id: str | None,
    step_name: str,
) -> float | None:
    """Ask the retry policy for the delay before the next attempt.

    Returns seconds to wait, or None to stop retrying. Jitter is seeded from
    (run_id, step_name, failures) so the same failure always samples the same
    delay — required when this runs during a replay of legacy ticks that did
    not journal the decision. Policies whose ``next`` predates the ``seed``
    kwarg are called without it.
    """
    if policy is None:
        return None
    jitter_seed = (
        int(
            hashlib.sha256(f"{run_id}:{step_name}:{failures}".encode()).hexdigest(),
            16,
        )
        & 0xFFFF_FFFF
        if run_id is not None
        else None
    )
    next_params = inspect.signature(policy.next).parameters
    seed_kwarg: dict[str, Any] = {"seed": jitter_seed} if "seed" in next_params else {}
    return policy.next(elapsed_time, failures, exception, **seed_kwarg)


def _drain_eligible_queue(
    step_name: str,
    state: InternalStepWorkerState,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Dispatch eligible queued attempts while worker capacity remains.

    Scans past ineligible (delayed) attempts so they neither block eligible
    work queued behind them nor consume a worker slot. Relative order among
    eligible attempts is preserved.
    """
    commands: list[WorkflowCommand] = []
    while len(state.in_progress) < state.config.num_workers:
        index = next(
            (i for i, a in enumerate(state.queue) if _is_eligible(a)),
            None,
        )
        if index is None:
            break
        attempt = state.queue.pop(index)
        commands.extend(_add_or_enqueue_event(attempt, step_name, state, now_seconds))
    return commands


def rewind_in_progress(
    state: BrokerState,
    now_seconds: float,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Rewind the in_progress state, extracting commands to re-initiate the workers.

    Also re-arms wakeups for queued delayed attempts so retry delays survive
    snapshot/resume. Even past-due attempts go through a wakeup tick (the
    runner fires past-due times immediately) rather than dispatching here:
    the dispatch is then a journaled tick, keeping replay deterministic.
    """
    state = state.deepcopy()
    commands: list[WorkflowCommand] = []
    for step_name, step_state in sorted(state.workers.items(), key=lambda x: x[0]):
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
        commands.extend(_drain_eligible_queue(step_name, step_state, now_seconds))
        for attempt in step_state.queue:
            if attempt.not_before is not None:
                commands.append(CommandScheduleWakeup(at_time=attempt.not_before))
    return state, commands


def _check_idle_state(state: BrokerState) -> bool:
    """Returns True if workflow is idle (no work can advance internally).

    A workflow is idle when:
    1. The workflow is running (hasn't completed/failed/cancelled)
    2. All steps have no pending events in their queues
    3. All steps have no workers currently executing

    A queued attempt with a future not_before (a delayed retry) is pending
    work: the workflow is not idle during a retry-delay window, so idle
    release defers until the retry resolves.
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
    worker_state = state.workers[tick.step_name]
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
            if isinstance(result.result, StopEvent):
                # huzzah! The workflow has completed
                commands.append(
                    CommandPublishEvent(event=result.result)
                )  # stop event always published to the stream
                state.is_running = False
                # Clear collected_events and collected_waiters since workflow is complete
                for worker in state.workers.values():
                    worker.collected_events.clear()
                    worker.collected_waiters.clear()
                commands.append(CommandCompleteRun(result=result.result))
            elif isinstance(result.result, Event):
                # queue any subsequent events
                # human input required are automatically published to the stream
                if isinstance(result.result, InputRequiredEvent):
                    commands.append(CommandPublishEvent(event=result.result))
                commands.append(
                    CommandQueueEvent(
                        event=result.result,
                        recovery_counts=dict(this_execution.recovery_counts),
                    )
                )
            elif result.result is None:
                # None means skip
                pass
            else:
                logger.warning(
                    f"Unknown result type returned from step function ({tick.step_name}): {type(result.result)}"
                )
        elif isinstance(result, StepWorkerFailed):
            # Schedule a retry if permitted, otherwise fail the workflow.
            # Prefer the journaled dispatch time: rebuilds re-stamp
            # this_execution.first_attempt_at with the rebuild clock, which
            # would silently restart elapsed-based retry budgets on resume.
            first_attempt_at = (
                result.first_attempt_at
                if result.first_attempt_at is not None
                else this_execution.first_attempt_at
            )
            if result.retry_decision is not None:
                # The decision was journaled inside the failure tick; replay
                # consumes it as data and never re-invokes policy code, so a
                # policy whose parameters changed between the live run and a
                # replay cannot diverge from the journaled TickWakeup.due.
                delay = result.retry_decision.delay
            else:
                # Legacy tick (journaled before decisions were recorded):
                # recompute via the policy, seeding jitter from the run id so
                # replay samples the same delay the live run did.
                delay = _decide_retry_delay(
                    worker_state.config.retry_policy,
                    elapsed_time=result.failed_at - first_attempt_at,
                    failures=this_execution.attempts + 1,
                    exception=result.exception,
                    run_id=run_id,
                    step_name=tick.step_name,
                )
            if delay is not None:
                # Re-queue the attempt directly into persisted state, carrying
                # an absolute eligibility time. not_before derives from the
                # journaled failure timestamp (not the current clock) so replay
                # computes the identical value. Dropping the wakeup never loses
                # work: resume re-arms it from the queue (rewind_in_progress).
                not_before = result.failed_at + delay if delay > 0 else None
                worker_state.queue.insert(
                    0,
                    EventAttempt(
                        event=tick.event,
                        attempts=this_execution.attempts + 1,
                        first_attempt_at=first_attempt_at,
                        last_exception=result.exception,
                        last_failed_at=result.failed_at,
                        recovery_counts=dict(this_execution.recovery_counts),
                        not_before=not_before,
                    ),
                )
                if not_before is not None:
                    commands.append(CommandScheduleWakeup(at_time=not_before))
            else:
                exception = result.exception
                total_attempts = this_execution.attempts + 1
                elapsed = result.failed_at - first_attempt_at

                handler_name = state.config.handler_for_step.get(tick.step_name)
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
                        step_name=tick.step_name,
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
                            step_name=handler.step_name,
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
                                step_name=tick.step_name,
                                exception=exception,
                                attempts=total_attempts,
                                elapsed_seconds=elapsed,
                            )
                        )
                    )
                    commands.append(
                        CommandFailWorkflow(
                            step_name=tick.step_name, exception=exception
                        )
                    )
        elif isinstance(result, AddCollectedEvent):
            # The current state of collected events.
            collected_events = state.workers[
                tick.step_name
            ].collected_events.setdefault(result.event_id, [])
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
                        for x, y in state.workers[
                            tick.step_name
                        ].collected_events.items()
                    },
                )
                this_execution.shared_state = updated_state
                commands.append(
                    CommandRunWorker(
                        step_name=tick.step_name,
                        event=result.event,
                        id=this_execution.worker_id,
                    )
                )
            else:
                collected_events.append(result.event)
        elif isinstance(result, DeleteCollectedEvent):
            if did_complete_step:  # allow retries to grab the events
                # indicates that a run has successfully collected its events, and they can be deleted from the collected events state
                state.workers[tick.step_name].collected_events.pop(
                    result.event_id, None
                )
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
                    commands.append(CommandPublishEvent(event=result.waiter_event))
                if result.timeout is not None:
                    commands.append(
                        CommandScheduleWaiterTimeout(
                            step_name=tick.step_name,
                            waiter_id=result.waiter_id,
                            timeout=result.timeout,
                        )
                    )

        elif isinstance(result, DeleteWaiter):
            if did_complete_step:  # allow retries to grab the waiter events
                # indicates that a run has obtained the waiting event, and it can be deleted from the collected waiters state
                to_remove = result.waiter_id
                waiters = state.workers[tick.step_name].collected_waiters
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
                    name=tick.step_name,
                    input_event_name=str(type(tick.event)),
                    output_event_name=output_event_name,
                    worker_id=str(tick.worker_id),
                )
            ),
        )
        worker_state.in_progress.remove(this_execution)
    # enqueue next events if there are any
    if not is_completed:
        commands.extend(
            _drain_eligible_queue(tick.step_name, worker_state, now_seconds)
        )

    return state, commands


def _add_or_enqueue_event(
    event: EventAttempt,
    step_name: str,
    state: InternalStepWorkerState,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """
    Small helper to assist in adding an event to a step worker state, or enqueuing it if it's not accepted.
    Note! This mutates the state, assuming that its already been deepcopied in an outer scope.
    """
    commands: list[WorkflowCommand] = []
    # Determine if there is available capacity based on in_progress workers.
    # Delayed attempts (not_before set) are never dispatched here; they wait
    # in the queue without consuming a worker slot until a wakeup flips them.
    has_space = len(state.in_progress) < state.config.num_workers and _is_eligible(
        event
    )
    if has_space:
        # Assign the smallest available worker id
        used = set(x.worker_id for x in state.in_progress)
        id_candidates = [i for i in range(state.config.num_workers) if i not in used]
        id = id_candidates[0]
        state_copy = state._deepcopy()
        shared_state: StepWorkerState = StepWorkerState(
            step_name=step_name,
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
        commands.append(CommandRunWorker(step_name=step_name, event=event.event, id=id))
        commands.append(
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.RUNNING,
                    name=step_name,
                    input_event_name=type(event.event).__name__,
                    worker_id=str(id),
                )
            )
        )
    else:
        commands.append(
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.PREPARING,
                    name=step_name,
                    input_event_name=type(event.event).__name__,
                    worker_id="<enqueued>",
                )
            )
        )
        state.queue.append(event)
    return commands


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
    waiter_resolved_steps: set[str] = set()
    for step_name, step_config in state.config.steps.items():
        wait_conditions = state.workers[step_name].collected_waiters
        for wait_condition in wait_conditions:
            is_match = event_matches(
                tick.event,
                wait_condition.waiting_for_event,
                allow_subclasses=step_config.accept_event_subclasses,
            )
            is_match = is_match and all(
                getattr(tick.event, k, None) == v
                for k, v in wait_condition.requirements.items()
            )
            if is_match:
                handled = True
                waiter_resolved_steps.add(step_name)
                wait_condition.resolved_event = tick.event
                subcommands = _add_or_enqueue_event(
                    EventAttempt(event=wait_condition.event),
                    step_name,
                    state.workers[step_name],
                    now_seconds,
                )
                commands.extend(subcommands)

    # Then route to accepting steps, skipping any that were already woken
    # via waiter resolution above.
    for step_name, step_config in state.config.steps.items():
        if step_name in waiter_resolved_steps:
            continue
        is_accepted = step_accepts_event(
            tick.event,
            step_config.accepted_events,
            allow_subclasses=step_config.accept_event_subclasses,
        )
        if is_accepted and (tick.step_name is None or tick.step_name == step_name):
            handled = True
            _consume_superseded_delayed_attempt(tick, state.workers[step_name])
            subcommands = _add_or_enqueue_event(
                EventAttempt(
                    event=tick.event,
                    attempts=tick.attempts,
                    first_attempt_at=tick.first_attempt_at,
                    last_exception=tick.last_exception,
                    last_failed_at=tick.last_failed_at,
                    recovery_counts=dict(tick.recovery_counts),
                ),
                step_name,
                state.workers[step_name],
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
                        step_name=tick.step_name,
                        idle=_check_idle_state(state),
                    )
                )
            )
    return state, commands


def _consume_superseded_delayed_attempt(
    tick: TickAddEvent, worker_state: InternalStepWorkerState
) -> None:
    """Compat shim for journals written before delayed retries lived in state.

    Older versions re-delivered a delayed retry as a journaled TickAddEvent
    carrying retry metadata (attempts > 0). The current reducer, replaying the
    same journal's failure tick, also queues the attempt with a not_before.
    Without this, replaying an old journal double-represents the retry: the
    TickAddEvent dispatches one copy while the phantom queued attempt blocks
    idle release and re-runs the step after a resume. The current retry path
    never emits attempts-bearing TickAddEvents, so this only matches
    old-format journals.
    """
    if not tick.attempts:
        return
    match = next(
        (
            i
            for i, a in enumerate(worker_state.queue)
            if a.not_before is not None
            and a.attempts == tick.attempts
            and type(a.event) is type(tick.event)
        ),
        None,
    )
    if match is not None:
        worker_state.queue.pop(match)


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
        step_name
        for step_name, worker_state in init.workers.items()
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


def _process_wakeup_tick(
    tick: TickWakeup, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Flip due delayed attempts to eligible, then dispatch what capacity allows.

    Eligibility flips on the tick's ``due`` value (recorded when the wakeup
    was scheduled), never the current clock, so replaying journaled ticks
    makes the same dispatch decisions as the live run. Spurious or duplicate
    wakeups are harmless no-ops.
    """
    state = init.deepcopy()
    commands: list[WorkflowCommand] = []
    for step_name, worker_state in sorted(state.workers.items(), key=lambda x: x[0]):
        for attempt in worker_state.queue:
            if attempt.not_before is not None and attempt.not_before <= tick.due:
                attempt.not_before = None
        commands.extend(_drain_eligible_queue(step_name, worker_state, now_seconds))
    return state, commands


def _process_waiter_timeout_tick(
    tick: TickWaiterTimeout, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    state = init.deepcopy()
    commands: list[WorkflowCommand] = []
    if tick.step_name not in state.workers:
        return state, commands
    worker_state = state.workers[tick.step_name]
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
        tick.step_name,
        worker_state,
        now_seconds,
    )
    commands.extend(subcommands)
    return state, commands
