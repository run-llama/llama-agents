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
    CollectionBinding,
    CollectionReleaseState,
    CollectionStreamInstance,
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
    CollectionReleasePayload,
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
    """Block for the next tick."""
    wait_result = await adapter.wait_receive(None)
    if not isinstance(wait_result, WaitResultTick):
        return []
    return [wait_result.tick]


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
                scope_path=command.scope_path,
                collection_release_payload=command.collection_release_payload,
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
                        # An idle check confirms quiescence, so it must observe
                        # a settled view: defer it behind any pending ticks,
                        # and drop it entirely while a delayed re-delivery
                        # (retry) is scheduled — the workflow is not idle,
                        # work is coming.
                        if self.tick_buffer:
                            self.tick_buffer.append(tick)
                            continue
                        if any(
                            isinstance(scheduled, TickAddEvent)
                            for _, _, scheduled in self.scheduled_wakeups
                        ):
                            self._idle_check_pending = False
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
            stuck = _detect_stuck_streams(init)
            if stuck is not None:
                stuck_step, stuck_error = stuck
                state = init.deepcopy()
                state.is_running = False
                return state, [
                    CommandPublishEvent(
                        event=WorkflowFailedEvent(
                            step_name=stuck_step,
                            exception=stuck_error,
                            attempts=1,
                            elapsed_seconds=0.0,
                        )
                    ),
                    CommandFailWorkflow(step_name=stuck_step, exception=stuck_error),
                ]
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
                    scope_path=in_progress.scope_path,
                    collection_release_payload=in_progress.shared_state.collection_release_payload,
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


def _detect_stuck_streams(
    state: BrokerState,
) -> tuple[str, WorkflowRuntimeError] | None:
    """Detect a provably-stuck run while the state is quiescent.

    Two conditions, returned as ``(step_name, error)``:

    - An unreleased release-state whose stream no longer exists. The close
      path fires releases inline within the same reduce, so this should be
      impossible; if it ever appears (corrupted or version-skewed persisted
      state), the release can never fire — fail loudly instead of hanging.
    - Open streams with no unresolved waiter. An unresolved waiter suppresses
      detection — external events can re-enter scoped work only via waiters,
      so a pending waiter means the open streams may still legitimately close
      (HITL workflows). Without one, an open stream can never reach zero open
      work items: the run would hang to timeout (or forever).
    """
    orphaned = next(
        (
            release
            for release in state.collection_release_states.values()
            if not release.released and release.stream_id not in state.streams
        ),
        None,
    )
    if orphaned is not None:
        binding = state.config.collection_bindings.get(orphaned.binding_id)
        step_name = binding.target_step if binding is not None else "<unknown>"
        return step_name, WorkflowRuntimeError(
            f"Workflow is idle with a pending collect release for step "
            f"{step_name!r} (binding {orphaned.binding_id!r}) whose stream "
            f"{orphaned.stream_id!r} no longer exists, so the release can "
            "never fire. This indicates corrupted persisted state (e.g. a "
            "snapshot written by an incompatible library version)."
        )
    if not state.streams:
        return None
    has_unresolved_waiter = any(
        waiter.resolved_event is None and not waiter.timed_out
        for worker_state in state.workers.values()
        for waiter in worker_state.collected_waiters
    )
    if has_unresolved_waiter:
        return None
    first_leaked = next(iter(state.streams.values()))
    details = "; ".join(
        f"stream {stream.stream_id!r} opened by step {stream.source_step!r} "
        f"with {stream.open_work_items} open work item(s)"
        for stream in state.streams.values()
    )
    return first_leaked.source_step, WorkflowRuntimeError(
        "Workflow is idle but collection streams are still open, so the run "
        f"can never complete: {details}. This usually means work inside a "
        "fan-out stream was consumed in a way that can never finish the "
        "stream — for example a join over inputs produced at different "
        "stream levels, or ctx.wait_for_event interplay that swallowed a "
        "member. To gate in-stream work on external input, use "
        "ctx.wait_for_event in the producing step; see the workflows.collect "
        "documentation for the supported fan-out/fan-in shapes."
    )


def _mint_stream_id(
    state: BrokerState, scope_path: tuple[str, ...], step_name: str
) -> str:
    seq = state.stream_seq
    state.stream_seq = seq + 1
    path = ">".join(scope_path)
    digest = hashlib.sha256(f"{path}:{step_name}:{seq}".encode()).hexdigest()
    return f"stream-{digest[:16]}"


def _clear_collection_state(state: BrokerState) -> None:
    state.streams.clear()
    state.collection_release_states.clear()


def _count_accepting_steps(state: BrokerState, event_type: type) -> int:
    """Number of steps that accept ``event_type`` — the work-item fan-out factor.

    An event routed at a stream level becomes one work item per accepting step
    (1:1 *and* collect steps count). This is the per-emission birth count for the
    open_work_items set: a single emitted event accepted by N steps is N work items.
    """
    return sum(
        1 for cfg in state.config.steps.values() if event_type in cfg.accepted_events
    )


def _apply_stream_work_delta(
    state: BrokerState, stream_id: str | None, delta: int, now_seconds: float
) -> list[WorkflowCommand]:
    if stream_id is None:
        return []
    stream = state.streams.get(stream_id)
    if stream is None:
        if delta < 0:
            logger.warning(
                "Stream accounting: ignoring a work-item decrement for "
                "unknown or already-closed stream %r.",
                stream_id,
            )
        return []
    stream.open_work_items += delta
    if stream.open_work_items < 0:
        # Provably corrupt accounting. Log loudly and let the <= 0 close
        # below fail fast instead of wedging the stream open.
        logger.error(
            "Stream accounting: open_work_items went negative (%d) for "
            "stream %r from step %r. This is a runtime accounting bug.",
            stream.open_work_items,
            stream_id,
            stream.source_step,
        )
    if stream.closed_to_new_items and stream.open_work_items <= 0:
        return _close_collection_stream(state, stream_id, now_seconds)
    return []


def _close_collection_stream(
    state: BrokerState, stream_id: str, now_seconds: float
) -> list[WorkflowCommand]:
    """Close a stream and fire its pending releases inline, within the reduce.

    Safe at the moment the counter zeroes: births happen at emission, before
    the close-triggering decrement, so no member can still be in flight and
    the release buffers are complete. Firing inline removes the snapshot
    window that existed when the close traveled as a separate buffered tick —
    a ``ctx.to_dict()`` between the reduce that popped the stream and the
    close tick's processing would capture an unreleased release-state with no
    stream, hanging the resume. Close effects now re-derive deterministically
    from whichever tick zeroed the counter.
    """
    stream = state.streams.pop(stream_id, None)
    if stream is None:
        return []
    commands: list[WorkflowCommand] = []
    for binding in state.config.bindings_for_source(stream.source_step):
        worker_state = state.workers.get(binding.target_step)
        if worker_state is None or worker_state.config.collection_param is None:
            continue
        key = _release_state_key(stream_id, binding.id)
        release_state = state.collection_release_states.pop(
            key,
            CollectionReleaseState(
                binding_id=binding.id,
                stream_id=stream_id,
            ),
        )
        release = _release_on_close(binding, release_state)
        if release is None:
            continue
        commands.extend(
            _fire_collection_release(
                binding,
                stream_id,
                worker_state,
                release,
                tuple(stream.scope_path),
                now_seconds,
            )
        )
    return commands


def _redeliver_work_item(
    this_execution: InProgressState,
    *,
    event: Event,
    step_name: str,
    recovery_counts: dict[str, int],
    carry_payload: bool,
    delay: float | None = None,
    attempts: int | None = None,
    first_attempt_at: float | None = None,
    last_exception: Exception | None = None,
    last_failed_at: float | None = None,
) -> CommandQueueEvent:
    """Build a re-delivery command from the live execution record.

    The work item's identity (scope, collect payload) travels whole from the
    in-progress record — never reassembled from fragments. ``carry_payload`` is
    True when re-delivering the record's own invocation event (retry); a routed
    successor event (e.g. a catch_error handler's StepFailedEvent) is ordinary
    same-scope dispatch and must not carry the payload.
    """
    return CommandQueueEvent(
        event=event,
        step_name=step_name,
        delay=delay,
        attempts=attempts,
        first_attempt_at=first_attempt_at,
        last_exception=last_exception,
        last_failed_at=last_failed_at,
        recovery_counts=recovery_counts,
        scope_path=this_execution.scope_path,
        collection_release_payload=(
            this_execution.shared_state.collection_release_payload
            if carry_payload
            else None
        ),
    )


def _take_threshold(collect: Collect | None) -> int | None:
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

    # Optimistic-concurrency guard for collect firings. A DeleteCollectedEvent
    # means this invocation consumed a collect buffer (snapshot + trigger). If
    # the live buffer changed since the snapshot — a concurrent worker fired it
    # or buffered another member first — this firing observed a stale batch:
    # discard every result and rerun the same work item against the fresh
    # buffer. Without this, two racing arrivals can both fire with the same
    # buffered member, duplicating it downstream.
    stale_firing = any(
        isinstance(r, DeleteCollectedEvent)
        and len(worker_state.collected_events.get(r.event_id, []))
        != len(this_execution.shared_state.collected_events.get(r.event_id, []))
        for r in tick.result
    )
    if stale_firing:
        this_execution.shared_state = replace(
            this_execution.shared_state,
            collected_events={
                x: list(y) for x, y in worker_state.collected_events.items()
            },
        )
        commands.append(
            CommandRunWorker(
                step_name=tick.step_name,
                event=this_execution.event,
                id=this_execution.worker_id,
            )
        )
        return state, commands

    output_event_name: str | None = None

    did_complete_step = any(isinstance(x, StepWorkerResult) for x in tick.result)
    step_no_longer_in_progress = True

    # Collection stream scope. The trigger path is carried on the in-progress
    # state. Streams are runtime facts: an execution that actually returned a
    # list (worker-reported via ``fanned_out``) mints ONE fresh stream id,
    # stamps every event it emits, then closes the stream. An execution of a
    # fan-out-annotated step that took a non-list branch (None or a declared
    # bare union member) mints nothing. A 1:1 step's outputs inherit the
    # trigger stack verbatim.
    trigger_stack = this_execution.scope_path
    fanned_out = any(
        isinstance(x, StepWorkerResult) and x.fanned_out for x in tick.result
    )
    fan_out_stream_id: str | None = None
    if fanned_out:
        fan_out_stream_id = _mint_stream_id(state, trigger_stack, tick.step_name)
    if fan_out_stream_id is not None:
        emit_stack: tuple[str, ...] = trigger_stack + (fan_out_stream_id,)
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
                # Drop open collection state; no release can fire after the run ends.
                _clear_collection_state(state)
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
                        scope_path=emit_stack,
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
                # A retry re-runs the SAME work item: the record travels whole
                # (scope, collect payload) so it still closes its stream and a
                # collect invocation re-fires with its original batch.
                commands.append(
                    _redeliver_work_item(
                        this_execution,
                        event=this_execution.event,
                        step_name=tick.step_name,
                        recovery_counts=dict(this_execution.recovery_counts),
                        carry_payload=True,
                        delay=delay,
                        attempts=this_execution.attempts + 1,
                        first_attempt_at=this_execution.first_attempt_at,
                        last_exception=result.exception,
                        last_failed_at=result.failed_at,
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
                    # The recovered branch continues at the same stream level:
                    # the handler event inherits the failing work item's scope
                    # so its output stays in-stream and the stream can still
                    # close. It routes to the handler step, so it must not
                    # carry the collect payload.
                    commands.append(
                        _redeliver_work_item(
                            this_execution,
                            event=step_failed_event,
                            step_name=handler.step_name,
                            recovery_counts={
                                **this_execution.recovery_counts,
                                handler.step_name: new_count,
                            },
                            carry_payload=False,
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
            snapshot_events = this_execution.shared_state.collected_events.get(
                result.event_id, []
            )
            if len(collected_events) > len(snapshot_events):
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
                # Store the suspended work item's record so resume re-delivers
                # it whole: same stream scope, same collect batch.
                scope_path=this_execution.scope_path,
                collection_release_payload=this_execution.shared_state.collection_release_payload,
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

    # Resolve this work item in its enclosing stream. Completion removes this
    # item and adds same-scope successors. Stream close is driven only by
    # source exhaustion plus ``open_work_items == 0``.
    emitted_non_stop = [
        x.result
        for x in tick.result
        if isinstance(x, StepWorkerResult)
        and isinstance(x.result, Event)
        and not isinstance(x.result, StopEvent)
    ]
    scheduled_retry = any(
        isinstance(c, CommandQueueEvent) and c.step_name == tick.step_name
        for c in commands
    )
    failing_run = any(isinstance(c, CommandFailWorkflow) for c in commands)
    terminal_run = any(indicates_exit(c) for c in commands)
    # A scheduled rerun is the SAME live work item — only its final completion
    # may consume it. Counting each rerun would drift the stream counter under
    # concurrency and close the stream early.
    rerun_scheduled = not step_no_longer_in_progress
    step_failed = any(isinstance(x, StepWorkerFailed) for x in tick.result)
    added_waiter = any(isinstance(x, AddWaiter) for x in tick.result)

    enclosing = trigger_stack[-1] if trigger_stack else None
    skip_accounting = (
        not did_complete_step
        or rerun_scheduled
        or scheduled_retry
        or failing_run
        or terminal_run
    )

    if not skip_accounting and fan_out_stream_id is not None:
        bindings = state.config.bindings_for_source(tick.step_name)
        accepting_binding_ids = tuple(binding.id for binding in bindings)
        seed = sum(_count_accepting_steps(state, type(m)) for m in emitted_non_stop)
        state.streams[fan_out_stream_id] = CollectionStreamInstance(
            stream_id=fan_out_stream_id,
            source_step=tick.step_name,
            source_execution_id=f"{tick.step_name}:{tick.worker_id}:{state.stream_seq - 1}",
            parent_stream_id=enclosing,
            scope_path=trigger_stack,
            accepting_binding_ids=accepting_binding_ids,
            open_work_items=seed,
            closed_to_new_items=True,
        )
        # The parent work item now waits for each child collection release.
        commands.extend(
            _apply_stream_work_delta(
                state, enclosing, len(accepting_binding_ids) - 1, now_seconds
            )
        )
        if seed == 0:
            commands.extend(
                _close_collection_stream(state, fan_out_stream_id, now_seconds)
            )
    elif not skip_accounting:
        # Same-level resolution (1:1 step, or a collect step firing its
        # summary). Remove this work item and add its same-level successors: one
        # work item per accepting step per emitted event. A step that returns
        # None adds zero successors and simply leaves the set.
        successors = sum(
            _count_accepting_steps(state, type(ev)) for ev in emitted_non_stop
        )
        commands.extend(
            _apply_stream_work_delta(state, enclosing, successors - 1, now_seconds)
        )
    elif (
        not did_complete_step
        and not rerun_scheduled
        and not step_failed
        and not added_waiter
    ):
        # Buffer absorption: a multi-slot join invocation that only absorbed
        # its trigger into the slot buffer (one or more AddCollectedEvents), or
        # silently dropped it (a duplicate arrival for an already-filled slot
        # records no result at all). Either way the invocation is over and
        # emitted nothing, so its work item is consumed here — once per work
        # item, no matter how many buffers were touched. A buffering run that
        # also completed (legacy collect_events returning None) consumes via
        # the completion rule above instead; a failed, retried, or suspended
        # (waiter) run resolves through its later re-delivery.
        commands.extend(_apply_stream_work_delta(state, enclosing, -1, now_seconds))

    is_completed = any(indicates_exit(c) for c in commands)
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
            collection_release_payload=event.collection_release_payload._copy()
            if event.collection_release_payload is not None
            else None,
            scope_path=event.scope_path,
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
                scope_path=event.scope_path,
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

    # A payload-carrying tick is a re-delivered collect invocation (retry,
    # waiter re-ping after resume, serialized requeue). It routes directly to
    # the binding's target step — before waiter matching, before the
    # member-arrival path — so it can never be swallowed as a stream member or
    # resolve an unrelated waiter. The event is derived from the payload, the
    # authoritative work record.
    if tick.collection_release_payload is not None:
        payload = tick.collection_release_payload
        binding = state.config.collection_bindings.get(payload.binding_id)
        if binding is None:
            raise WorkflowRuntimeError(
                f"Collect invocation re-delivered for unknown binding "
                f"{payload.binding_id!r} (stream {payload.stream_id!r}). "
                "Workflow state is corrupt."
            )
        commands.extend(
            _add_or_enqueue_event(
                EventAttempt(
                    event=payload.as_event(),
                    attempts=tick.attempts,
                    first_attempt_at=tick.first_attempt_at,
                    last_exception=tick.last_exception,
                    last_failed_at=tick.last_failed_at,
                    recovery_counts=dict(tick.recovery_counts),
                    scope_path=tuple(tick.scope_path),
                    collection_release_payload=payload,
                ),
                binding.target_step,
                state.workers[binding.target_step],
                now_seconds,
            )
        )
        return state, commands

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
                # Resume re-delivers the suspended work item whole from the
                # waiter record: original trigger, stream scope, collect batch.
                subcommands = _add_or_enqueue_event(
                    EventAttempt(
                        event=wait_condition.event,
                        scope_path=wait_condition.scope_path,
                        collection_release_payload=wait_condition.collection_release_payload,
                    ),
                    step_name,
                    state.workers[step_name],
                    now_seconds,
                )
                commands.extend(subcommands)

    # Then route to accepting steps, skipping any that were already woken
    # via waiter resolution above.
    for step_name, step_config in state.config.steps.items():
        is_accepted = type(tick.event) in step_config.accepted_events
        is_targeted = tick.step_name is None or tick.step_name == step_name
        if step_name in waiter_resolved_steps:
            if is_accepted and is_targeted and tick.scope_path:
                # The waiter swallowed a delivery this step would otherwise
                # have received. The delivery was birth-counted as a work item
                # in its stream, so consume it here — otherwise the stream can
                # never close. This covers both 1:1 steps and collect steps
                # parked on wait_for_event of their own member type (the
                # swallowed member never joins the batch; the waiter consumed
                # it).
                commands.extend(
                    _apply_stream_work_delta(
                        state, tick.scope_path[-1], -1, now_seconds
                    )
                )
            continue
        if is_accepted and is_targeted:
            handled = True
            worker_state = state.workers[step_name]
            if worker_state.config.collection_param is not None:
                if not tick.scope_path:
                    # Scope-less events (ctx.send_event, external sends) can
                    # never join a collect batch — members reach a collect step
                    # only by being emitted inside a fan-out stream.
                    if tick.step_name is not None:
                        logger.warning(
                            "Ignoring %s sent to collect step %r via "
                            "send_event(step=...): a collect step cannot "
                            "receive targeted events; it only collects events "
                            "emitted inside a fan-out stream.",
                            type(tick.event).__name__,
                            step_name,
                        )
                    else:
                        logger.warning(
                            "Ignoring %s for collect step %r: it was sent "
                            "outside any collection stream (e.g. via "
                            "ctx.send_event) so it cannot join a batch.",
                            type(tick.event).__name__,
                            step_name,
                        )
                    continue
                stream_id = tick.scope_path[-1]
                binding = state.config.binding_for_target(
                    stream_id, step_name, state.streams
                )
                if binding is None:
                    # Dropped member: its nearest stream has no binding to this
                    # collect step. Balance the stream accounting for the dead
                    # work item and say so.
                    logger.warning(
                        "Dropping %s for collect step %r: its enclosing "
                        "stream %r has no collection binding targeting that "
                        "step, so it can never join a batch.",
                        type(tick.event).__name__,
                        step_name,
                        stream_id,
                    )
                    commands.extend(
                        _apply_stream_work_delta(state, stream_id, -1, now_seconds)
                    )
                    continue
                release_state = _release_state_for(state, stream_id, binding)
                if not release_state.released:
                    release_state.buffer.append(tick.event)
                    release = _release_on_item(binding, release_state)
                    if release is not None:
                        commands.extend(
                            _fire_collection_release(
                                binding,
                                stream_id,
                                worker_state,
                                release,
                                tuple(tick.scope_path[:-1]),
                                now_seconds,
                            )
                        )
                commands.extend(
                    _apply_stream_work_delta(state, stream_id, -1, now_seconds)
                )
                continue
            subcommands = _add_or_enqueue_event(
                EventAttempt(
                    event=tick.event,
                    attempts=tick.attempts,
                    first_attempt_at=tick.first_attempt_at,
                    last_exception=tick.last_exception,
                    last_failed_at=tick.last_failed_at,
                    recovery_counts=dict(tick.recovery_counts),
                    scope_path=tuple(tick.scope_path),
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
    _clear_collection_state(state)
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
    # Timeout resumes the suspended work item whole, like waiter resolution.
    subcommands = _add_or_enqueue_event(
        EventAttempt(
            event=waiter.event,
            scope_path=waiter.scope_path,
            collection_release_payload=waiter.collection_release_payload,
        ),
        tick.step_name,
        worker_state,
        now_seconds,
    )
    commands.extend(subcommands)
    return state, commands


def _release_state_key(stream_id: str, binding_id: str) -> str:
    return f"{stream_id}:{binding_id}"


def _release_state_for(
    state: BrokerState, stream_id: str, binding: CollectionBinding
) -> CollectionReleaseState:
    key = _release_state_key(stream_id, binding.id)
    release_state = state.collection_release_states.get(key)
    if release_state is None:
        release_state = CollectionReleaseState(
            binding_id=binding.id,
            stream_id=stream_id,
        )
        state.collection_release_states[key] = release_state
    return release_state


def _release_on_item(
    binding: CollectionBinding, release_state: CollectionReleaseState
) -> list[Event] | None:
    threshold = _take_threshold(binding.policy)
    if threshold is None or len(release_state.buffer) < threshold:
        return None
    release_state.released = True
    release_state.cursor = threshold
    return list(release_state.buffer[:threshold])


def _release_on_close(
    binding: CollectionBinding, release_state: CollectionReleaseState
) -> list[Event] | None:
    if release_state.released:
        return None
    release_state.released = True
    threshold = _take_threshold(binding.policy)
    if threshold is not None:
        release_state.cursor = min(threshold, len(release_state.buffer))
        return list(release_state.buffer[:threshold])
    return list(release_state.buffer)


def _fire_collection_release(
    binding: CollectionBinding,
    stream_id: str,
    worker_state: InternalStepWorkerState,
    events: list[Event],
    output_stack: tuple[str, ...],
    now_seconds: float,
) -> list[WorkflowCommand]:
    payload = CollectionReleasePayload(
        binding_id=binding.id,
        stream_id=stream_id,
        events=list(events),
        output_scope_path=output_stack,
    )
    return _add_or_enqueue_event(
        EventAttempt(
            event=payload.as_event(),
            scope_path=output_stack,
            collection_release_payload=payload,
        ),
        binding.target_step,
        worker_state,
        now_seconds,
    )
