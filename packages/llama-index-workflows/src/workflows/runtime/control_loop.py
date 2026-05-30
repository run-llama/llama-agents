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
from typing import TYPE_CHECKING, cast

from workflows.collect import Collect, Take
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
from workflows.runtime.types.commands import (
    CommandCloseBatch,
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
    Batch,
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
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickBatchClosed,
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


async def _single_pull(adapter: InternalRunAdapter) -> list[WorkflowTick]:
    """Block for the next tick, then drain any immediately-available ones.

    Delivering a burst together (instead of one tick per loop iteration) keeps a
    set of events emitted back-to-back — e.g. several ``ctx.send_event`` calls
    from one step — from being interleaved with idle checks. That matters for an
    All() collect fed by ``send_event``: it must observe the whole burst before
    the quiescence check decides the batch is complete, or it would fire early
    with a truncated batch. FIFO order (hence replay order) is preserved.

    The extra drain is a best-effort non-blocking poll exposed only by adapters
    that can do it cheaply (the in-memory runtime via ``drain_ready``). Remote
    adapters omit it and fall back to one-tick-at-a-time delivery.
    """
    wait_result = await adapter.wait_receive(None)
    if not isinstance(wait_result, WaitResultTick):
        return []
    ticks: list[WorkflowTick] = [wait_result.tick]
    drain_ready = getattr(adapter, "drain_ready", None)
    if callable(drain_ready):
        extra = cast("list[WorkflowTick]", drain_ready())
        ticks.extend(extra)
    return ticks


if TYPE_CHECKING:
    from workflows.context.context import Context
    from workflows.runtime.types.step_function import StepWorkerFunction


logger = logging.getLogger(__name__)


class _EmptyBatchTrigger(Event):
    """Internal carrier event for firing a batch-collect step on an empty batch.

    Never routed or persisted as user data — it only occupies the ``event`` slot
    of a worker invocation whose real payload is the (empty) ``batch_input``.
    """


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
                    step_name=command.step_name,
                    worker_id=command.id,
                    event=command.event,
                    result=[
                        StepWorkerFailed(
                            exception=e, failed_at=await self.adapter.get_now()
                        )
                    ],
                )

        self._pending_workers.append(
            PendingWorker(command.step_name, command.id, _run_worker())
        )

    async def process_command(self, command: WorkflowCommand) -> None | StopEvent:
        """Process a single command returned from tick reduction."""
        if isinstance(command, CommandQueueEvent):
            event = TickAddEvent(
                event=command.event,
                step_name=command.step_name,
                attempts=command.attempts,
                first_attempt_at=command.first_attempt_at,
                last_exception=command.last_exception,
                last_failed_at=command.last_failed_at,
                recovery_counts=dict(command.recovery_counts),
                batch_stack=command.batch_stack,
                batch_counted=command.batch_counted,
            )
            if command.delay is not None and command.delay > 0:
                now = await self.adapter.get_now()
                self.schedule_tick(event, at_time=now + command.delay)
            else:
                self.tick_buffer.append(event)
            return None
        elif isinstance(command, CommandCloseBatch):
            self.tick_buffer.append(
                TickBatchClosed(
                    batch_id=command.batch_id,
                    step_name=command.step_name,
                    batch_stack=command.batch_stack,
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
                        pull_ticks = completed_task.result()
                    except asyncio.CancelledError:
                        pull_task = None
                    except Exception:
                        logger.exception("Pull task failed", exc_info=True)
                        pull_task = None
                    else:
                        pull_task = None
                        self.tick_buffer.extend(pull_ticks)
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
    elif isinstance(tick, TickBatchClosed):
        state, commands = _process_batch_closed_tick(tick, init, now_seconds)
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
            # An All() collect fed by ctx.send_event (no fan-out producer) has no
            # batch to close it, so its members sit in the unbatched ("") buffer.
            # When the workflow has otherwise gone quiescent, that buffer IS the
            # full batch — fire those collects once before declaring idle.
            flushed, flush_commands = _flush_unbatched_collects(init, now_seconds)
            if flush_commands:
                return flushed, flush_commands
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
                    batch_stack=in_progress.batch_stack,
                ),
            )
        step_state.in_progress = []
        while (
            len(step_state.queue) > 0
            and len(step_state.in_progress) < step_state.config.num_workers
        ):
            event = step_state.queue.pop(0)
            commands.extend(
                _add_or_enqueue_event(event, step_name, step_state, now_seconds)
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


def _mint_batch_id(
    state: BrokerState, origin_stack: tuple[str, ...], step_name: str
) -> str:
    """Mint a deterministic, run-id-free batch id and advance the counter.

    The id is a pure function of the parent ``origin_stack`` (the producer's
    tree-path), the producing ``step_name``, and a monotonic sequence number kept
    in broker state. Because the reducer is pure and replayed tick-by-tick (see
    ``replay_ticks_stream``), the same fan-out reproduces the same sequence value,
    hence the same id, on replay — and crucially with NO ``run_id`` in the digest,
    a replay (``run_id=None``) and the live run produce identical ids, so a
    snapshot taken mid-fan-out resumes onto the same lineage. NO uuid / random /
    wall-clock / id() — those would corrupt replayed lineage.
    """
    seq = state.batch_seq
    state.batch_seq = seq + 1
    path = ">".join(origin_stack)
    digest = hashlib.sha256(f"{path}:{step_name}:{seq}".encode()).hexdigest()
    return f"batch-{digest[:16]}"


def _clear_batch_state(state: BrokerState) -> None:
    """Drop all open-batch lineage on a terminal path.

    Called on every workflow exit (StopEvent, cancel, timeout). After the run
    ends no batch can close and no collect can fire, so the open batches and
    per-step fan-in buffers are dead weight that must not survive into a
    serialized snapshot.
    """
    state.batches.clear()
    for worker in state.workers.values():
        worker.batch_buffers.clear()
        worker.batch_fired.clear()


def _count_accepting_steps(state: BrokerState, event_type: type) -> int:
    """Number of steps that accept ``event_type`` — the work-item fan-out factor.

    An event routed at a batch level becomes one work item per accepting step
    (1:1 *and* collect steps count). This is the per-emission birth count for the
    live set: a single emitted event accepted by N steps is N work items.
    """
    return sum(
        1 for cfg in state.config.steps.values() if event_type in cfg.accepted_events
    )


def _apply_live_delta(
    state: BrokerState, batch_id: str | None, delta: int
) -> list[WorkflowCommand]:
    """Adjust an open batch's live-set cardinality and close it on empty.

    ``batch_id`` may be ``None`` (no enclosing batch) or unknown (already closed)
    — both are no-ops. Closing pops the batch and emits a single
    ``CommandCloseBatch`` so its bound collects fire.
    """
    if batch_id is None:
        return []
    batch = state.batches.get(batch_id)
    if batch is None:
        return []
    batch.live += delta
    if batch.live <= 0:
        return _close_batch(state, batch_id)
    return []


def _close_batch(state: BrokerState, batch_id: str) -> list[WorkflowCommand]:
    """Emit a CommandCloseBatch for ``batch_id`` and remove its record.

    ``batch_stack`` on the command is the batch's origin (producer trigger)
    stack, so a collect step's outputs inherit the stack with the closed id
    already popped. The producer step rides on the command so downstream binding
    is resolved from the producer, never from whichever path closed the batch.
    """
    batch = state.batches.pop(batch_id, None)
    if batch is None:
        return []
    return [
        CommandCloseBatch(
            batch_id=batch_id,
            step_name=batch.producer,
            batch_stack=batch.origin_stack,
        )
    ]


def _cardinality_threshold(collect: Collect | None) -> int | None:
    """Member count that triggers an early release, or None for fire-on-close.

    ``Take(n)`` releases as soon as ``n`` members have arrived; ``All`` (and an
    absent marker) fires only when the batch closes.
    """
    if collect is None:
        return None
    card = collect.cardinality
    if isinstance(card, Take):
        return card.n
    return None


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

    # Batch lineage (L2). The trigger stack is carried on the in-progress state.
    # A fan-out step (list[E] return) mints ONE fresh batch id for this completed
    # execution and stamps every event it emits, then closes the batch. A 1:1
    # step's outputs inherit the trigger stack verbatim.
    trigger_stack = this_execution.batch_stack
    is_fan_out = worker_state.config.is_fan_out
    fan_out_batch_id: str | None = None
    if is_fan_out and did_complete_step:
        fan_out_batch_id = _mint_batch_id(state, trigger_stack, tick.step_name)
    if fan_out_batch_id is not None:
        emit_stack: tuple[str, ...] = trigger_stack + (fan_out_batch_id,)
    else:
        emit_stack = trigger_stack

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
                # Drop all batch lineage: the run is over, so no batch can close
                # and no collect can fire. Leaving it would leak into a snapshot.
                _clear_batch_state(state)
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
                        batch_stack=emit_stack,
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
            # Schedule a retry if permitted, otherwise fail the workflow
            retries = worker_state.config.retry_policy
            failures = this_execution.attempts + 1
            elapsed_time = result.failed_at - this_execution.first_attempt_at
            jitter_seed = (
                int(
                    hashlib.sha256(
                        f"{run_id}:{tick.step_name}:{failures}".encode()
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
                        step_name=tick.step_name,
                        attempts=this_execution.attempts + 1,
                        first_attempt_at=this_execution.first_attempt_at,
                        last_exception=result.exception,
                        last_failed_at=result.failed_at,
                        recovery_counts=dict(this_execution.recovery_counts),
                        # A retry re-runs the SAME work item: keep its batch_stack
                        # so it still closes its batch, and mark it counted so
                        # re-queuing does not mint a second live-set member.
                        batch_stack=trigger_stack,
                    )
                )
            else:
                exception = result.exception
                total_attempts = this_execution.attempts + 1
                elapsed = result.failed_at - this_execution.first_attempt_at

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
                            # The recovered branch continues at the same batch
                            # level: carry the failing work item's stack so the
                            # handler's output stays in-batch and the batch can
                            # still close. batch_counted stays True — the handler
                            # event is this work item's same-level successor,
                            # pre-counted below.
                            batch_stack=trigger_stack,
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

    # ---- Batch lineage bookkeeping (L2): resolve one work item ----
    # This execution IS one live work item in its enclosing batch
    # (``trigger_stack[-1]``). Resolving it removes it from the live set and adds
    # its same-level successors. There is exactly one resolve rule, applied below;
    # closure is "live set empty", never a counter heuristic.
    emitted_events = [
        x.result
        for x in tick.result
        if isinstance(x, StepWorkerResult) and isinstance(x.result, Event)
    ]
    scheduled_retry = any(
        isinstance(c, CommandQueueEvent) and c.step_name == tick.step_name
        for c in commands
    )
    failing_run = any(isinstance(c, CommandFailWorkflow) for c in commands)
    terminal_run = any(indicates_exit(c) for c in commands)

    enclosing = trigger_stack[-1] if trigger_stack else None
    skip_accounting = (
        not did_complete_step or scheduled_retry or failing_run or terminal_run
    )

    if not skip_accounting and is_fan_out and fan_out_batch_id is not None:
        # (a) Fan-out descent. Open the child batch B'. Its live set is seeded
        # with one work item per emitted member per accepting step. The enclosing
        # batch trades this producer's work item for one PLACEHOLDER per collect
        # bound to B' — each resolves when that collect fires its summary back at
        # the enclosing level (so nesting is represented, not counted). A child
        # with no members closes immediately (its bound collects fire empty).
        members = [ev for ev in emitted_events if not isinstance(ev, StopEvent)]
        bound_collects = state.config.fan_in_bindings.get(tick.step_name, ())
        seed = sum(_count_accepting_steps(state, type(m)) for m in members)
        state.batches[fan_out_batch_id] = Batch(
            batch_id=fan_out_batch_id,
            producer=tick.step_name,
            origin_stack=trigger_stack,
            bound_collects=bound_collects,
            live=seed,
        )
        commands.extend(_apply_live_delta(state, enclosing, len(bound_collects) - 1))
        if seed == 0:
            commands.extend(_close_batch(state, fan_out_batch_id))
    elif not skip_accounting:
        # (b) Same-level resolution (1:1 step, or a collect step firing its
        # summary). Remove this work item and add its same-level successors: one
        # work item per accepting step per emitted event. A step that returns
        # None / sends nothing adds zero successors and simply leaves the set.
        same_level = [ev for ev in emitted_events if not isinstance(ev, StopEvent)]
        successors = sum(_count_accepting_steps(state, type(ev)) for ev in same_level)
        commands.extend(_apply_live_delta(state, enclosing, successors - 1))

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
        while (
            len(worker_state.queue) > 0
            and len(worker_state.in_progress) < worker_state.config.num_workers
        ):
            event = worker_state.queue.pop(0)
            subcommands = _add_or_enqueue_event(
                event, tick.step_name, worker_state, now_seconds
            )
            commands.extend(subcommands)

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
    # Determine if there is available capacity based on in_progress workers
    has_space = len(state.in_progress) < state.config.num_workers
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
            batch_stack=event.batch_stack,
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
                batch_stack=event.batch_stack,
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
            is_match = type(tick.event) is wait_condition.waiting_for_event
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

    # Live-set birth for ctx.send_event events. Reducer-emitted events (1:1
    # outputs, fan-out members, retries, catch_error routing) already had their
    # work items counted at the producing step's resolve (batch_counted=True). A
    # send_event arrives uncounted, so register its work items here — one per
    # accepting step against its batch — matching the resolve-time pre-count. The
    # collect-delivery and step-resolve removals below balance it back out.
    batch_top = tick.batch_stack[-1] if tick.batch_stack else None
    if not tick.batch_counted and batch_top in state.batches:
        commands.extend(
            _apply_live_delta(
                state, batch_top, _count_accepting_steps(state, type(tick.event))
            )
        )

    # Then route to accepting steps, skipping any that were already woken
    # via waiter resolution above.
    for step_name, step_config in state.config.steps.items():
        if step_name in waiter_resolved_steps:
            continue
        is_accepted = type(tick.event) in step_config.accepted_events
        if is_accepted and (tick.step_name is None or tick.step_name == step_name):
            handled = True
            worker_state = state.workers[step_name]
            if worker_state.config.batch_collect_param is not None:
                # Batch-lineage fan-in (L2/L3): buffer the event keyed by its
                # innermost batch id. Events with no batch stack (outside any
                # fan-out) land under the empty-key bucket; a producer that fans
                # out via a typed list/iterator return always stamps an id.
                batch_id = tick.batch_stack[-1] if tick.batch_stack else ""
                already_fired = batch_id in worker_state.batch_fired
                if not already_fired:
                    buf = worker_state.batch_buffers.setdefault(batch_id, [])
                    buf.append(tick.event)
                    # Cardinality release (L3): Take(n) fires as soon
                    # as ``n`` members arrive, with the first ``n``, instead of
                    # waiting for the batch to close. All() has no threshold and
                    # fires only on TickBatchClosed (the L2 path below). The
                    # output stack is the trigger stack with this batch id popped
                    # — i.e. the enclosing stack.
                    threshold = _cardinality_threshold(
                        worker_state.config.batch_collect
                    )
                    if threshold is not None and len(buf) >= threshold:
                        released = list(buf[:threshold])
                        output_stack = (
                            tuple(tick.batch_stack[:-1]) if tick.batch_stack else ()
                        )
                        worker_state.batch_fired.add(batch_id)
                        worker_state.batch_buffers.pop(batch_id, None)
                        commands.extend(
                            _fire_batch_collect(
                                step_name,
                                worker_state,
                                released,
                                output_stack,
                                now_seconds,
                            )
                        )
                # Collect delivery: this member's work item resolves to ZERO
                # same-level successors, so it simply leaves the live set. Counted
                # ONCE per work item (per accepting collect step), never per
                # consumer of the emitted event — so two collects of the same type
                # each see the full batch instead of racing one decrement. Runs
                # even after an early release so the last member still closes the
                # batch. No-op when the event has no batch (the "" bucket).
                commands.extend(_apply_live_delta(state, batch_id or None, -1))
                continue
            subcommands = _add_or_enqueue_event(
                EventAttempt(
                    event=tick.event,
                    attempts=tick.attempts,
                    first_attempt_at=tick.first_attempt_at,
                    last_exception=tick.last_exception,
                    last_failed_at=tick.last_failed_at,
                    recovery_counts=dict(tick.recovery_counts),
                    batch_stack=tuple(tick.batch_stack),
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
    _clear_batch_state(state)
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


def _fire_batch_collect(
    step_name: str,
    worker_state: InternalStepWorkerState,
    batch: list[Event],
    output_stack: tuple[str, ...],
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Launch a batch-collect step once with the full buffered ``batch``.

    The buffered list rides on the worker snapshot's ``batch_input``; the worker
    wrapper binds it to the step's ``list[E]`` parameter. ``output_stack`` is the
    trigger stack the collect step's own outputs should inherit (the closed
    batch id already popped). A representative carrier event occupies the
    ``event`` slot but is ignored by the wrapper.
    """
    commands: list[WorkflowCommand] = []
    used = set(x.worker_id for x in worker_state.in_progress)
    id_candidates = [i for i in range(worker_state.config.num_workers) if i not in used]
    if not id_candidates:
        # No free slot within num_workers. A batch-collect fires once and is not
        # re-queued, so it must still run — but it MUST get an id distinct from
        # every other in-progress execution, or two overlapping batch closes
        # (e.g. num_workers=1) would both grab worker 0 and alias each other's
        # in-progress state, losing one batch's result. Allocate above the range.
        worker_id = (max(used) + 1) if used else 0
    else:
        worker_id = id_candidates[0]
    # Carrier event: the wrapper ignores it and binds the list from batch_input.
    carrier: Event = batch[0] if batch else _EmptyBatchTrigger()
    state_copy = worker_state._deepcopy()
    shared_state = StepWorkerState(
        step_name=step_name,
        collected_events=state_copy.collected_events,
        collected_waiters=state_copy.collected_waiters,
        batch_input=list(batch),
        batch_stack=output_stack,
    )
    worker_state.in_progress.append(
        InProgressState(
            event=carrier,
            worker_id=worker_id,
            shared_state=shared_state,
            attempts=0,
            first_attempt_at=now_seconds,
            batch_stack=output_stack,
        )
    )
    commands.append(CommandRunWorker(step_name=step_name, event=carrier, id=worker_id))
    commands.append(
        CommandPublishEvent(
            StepStateChanged(
                step_state=StepState.RUNNING,
                name=step_name,
                input_event_name=type(carrier).__name__,
                worker_id=str(worker_id),
            )
        )
    )
    return commands


def _flush_unbatched_collects(
    init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Fire All() collects buffering unbatched (``ctx.send_event``) members.

    These members have no fan-out batch to close them, so an All() collect would
    hang. When the workflow is otherwise idle, the unbatched buffer is the
    complete batch: fire each such collect once with it. ``Take(n)`` collects are
    untouched — they release on their own threshold. No-op (returns ``init``
    unchanged) when there is nothing to flush.
    """
    candidates = [
        (name, ws)
        for name, ws in init.workers.items()
        if ws.config.batch_collect_param is not None
        and _cardinality_threshold(ws.config.batch_collect) is None
        and ws.batch_buffers.get("")
        and "" not in ws.batch_fired
    ]
    if not candidates:
        return init, []
    state = init.deepcopy()
    commands: list[WorkflowCommand] = []
    for name, _ in candidates:
        worker_state = state.workers[name]
        released = list(worker_state.batch_buffers.pop("", []))
        worker_state.batch_fired.add("")
        commands.extend(
            _fire_batch_collect(name, worker_state, released, (), now_seconds)
        )
    return state, commands


def _process_batch_closed_tick(
    tick: TickBatchClosed, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Fire the collect step(s) statically bound to this batch, once each.

    The bound collects come from the build-time ``fan_in_bindings`` keyed on the
    batch's producer (carried on the close tick as ``step_name``) — no runtime
    graph walk. Each bound collect fires exactly once with its buffered list,
    EVEN when empty (every branch died / the batch was empty), which is what lets
    a middle batch that closes empty still summarize up to its parent. A collect
    already released early via a ``Take`` cardinality is not re-fired.

    The fired collect step's outputs inherit ``tick.batch_stack`` (the producer's
    trigger stack), i.e. the closed id is popped — implemented by stamping the
    in-progress state with that stack.
    """
    state = init.deepcopy()
    commands: list[WorkflowCommand] = []
    for step_name in state.config.fan_in_bindings.get(tick.step_name, ()):
        worker_state = state.workers.get(step_name)
        if worker_state is None or worker_state.config.batch_collect_param is None:
            continue
        if tick.batch_id in worker_state.batch_fired:
            # Already released early via a Take cardinality. Don't re-fire; just
            # clear the per-batch tracking now that it's closed.
            worker_state.batch_fired.discard(tick.batch_id)
            worker_state.batch_buffers.pop(tick.batch_id, None)
            continue
        batch = worker_state.batch_buffers.pop(tick.batch_id, [])
        commands.extend(
            _fire_batch_collect(
                step_name,
                worker_state,
                batch,
                tuple(tick.batch_stack),
                now_seconds,
            )
        )
    return state, commands
