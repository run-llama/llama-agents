# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import hashlib
import inspect
import logging
import time
from collections.abc import AsyncIterable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from workflows._event_matching import (
    event_matches,
    step_accepts_event,
)
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
from workflows.runtime.control_loop.streams import (
    WorkDisposition,
    _adjust_open_work_items,
    _classify_work_item,
    _clear_collection_state,
    _close_collection_stream,
    _count_accepting_steps,
    _detect_stuck_streams,
    _fire_collection_release,
    _mint_stream_id,
    _release_on_item,
    _release_state_for,
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
    CommandScheduleWakeup,
    WorkflowCommand,
    indicates_exit,
)
from workflows.runtime.types.internal_state import (
    BrokerState,
    CollectionStreamInstance,
    EventAttempt,
    InProgressState,
    InternalStepWorkerState,
)
from workflows.runtime.types.results import (
    AddCollectedEvent,
    AddWaiter,
    DeleteCollectedEvent,
    DeleteWaiter,
    StepFunctionResult,
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
    TickWakeup,
    WorkflowTick,
)

logger = logging.getLogger("workflows.runtime.control_loop")


def _root_step_key(step_id: StepId) -> str:
    return str(step_id)


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
                        )
                    ),
                    CommandFailWorkflow(
                        step_id=StepId.root(stuck_step), exception=stuck_error
                    ),
                ]
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


def _next_work_item_id(state: BrokerState) -> str:
    """Mint a deterministic identity for one routed work item."""
    state.work_item_seq += 1
    return f"work_item_{state.work_item_seq}"


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
    step_id: StepId,
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
        commands.extend(_add_or_enqueue_event(attempt, step_id, state, now_seconds))
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
        step_id = StepId.root(step_name)
        for in_progress in step_state.in_progress:
            step_state.queue.insert(
                0,
                EventAttempt(
                    event=in_progress.event,
                    bound_events=in_progress.bound_events,
                    attempts=in_progress.attempts,
                    first_attempt_at=in_progress.first_attempt_at,
                    last_exception=in_progress.last_exception,
                    last_failed_at=in_progress.last_failed_at,
                    recovery_counts=dict(in_progress.recovery_counts),
                    scope_path=in_progress.scope_path,
                    collection_release_payload=in_progress.shared_state.collection_release_payload,
                    work_item_id=in_progress.work_item_id,
                ),
            )
        step_state.in_progress = []
        commands.extend(_drain_eligible_queue(step_id, step_state, now_seconds))
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


def _collect_buffer_diverged(live: list[Event], snapshot: list[Event]) -> bool:
    """True when a live ctx.collect_events() buffer no longer matches a snapshot."""
    return len(live) != len(snapshot) or any(a is not b for a, b in zip(live, snapshot))


def _queue_catch_error_event(
    this_execution: InProgressState,
    *,
    event: Event,
    step_id: StepId,
    recovery_counts: dict[str, int],
) -> CommandQueueEvent:
    """Build a catch_error dispatch in the failed work item's stream scope."""
    return CommandQueueEvent(
        event=event,
        step_id=step_id,
        recovery_counts=recovery_counts,
        scope_path=this_execution.scope_path,
    )


@dataclass
class _StepResultAcc:
    """Mutable accumulator threaded through one step-result reduction.

    Holds the commands emitted so far plus the per-result flags that the
    apply-results phase sets and the stream-accounting / finalize phases read.
    """

    commands: list[WorkflowCommand]
    output_event_name: str | None = None
    # Cleared when a worker is re-run mid-flight (stale collect buffer): the
    # execution stays in_progress and must not emit a NOT_RUNNING transition.
    step_no_longer_in_progress: bool = True
    # The failed work item was re-delivered (retry queued or routed to a
    # catch_error handler) rather than consumed.
    redelivery_scheduled: bool = False


@dataclass(frozen=True)
class _FanOutScope:
    """Collection-stream scope for the events this execution emits.

    Streams are runtime facts: an execution that actually returned a list
    (worker-reported via ``fanned_out``) mints ONE fresh stream id, stamps
    every event it emits with it, then closes the stream. A fan-out-annotated
    step that took a non-list branch (None or a declared bare union member)
    mints nothing. A 1:1 step's outputs inherit the trigger stack verbatim.
    """

    # The trigger path carried on the in-progress execution.
    trigger_stack: tuple[str, ...]
    fanned_out: bool
    fan_out_stream_id: str | None
    # Scope stamped onto emitted events: trigger_stack, plus the fresh stream
    # id when this execution fanned out.
    emit_stack: tuple[str, ...]


def _find_in_progress(
    worker_state: InternalStepWorkerState, worker_id: int
) -> InProgressState:
    execution = next(
        (w for w in worker_state.in_progress if w.worker_id == worker_id), None
    )
    if execution is None:
        # this should not happen unless there's a logic bug in the control loop
        raise ValueError(f"Worker {worker_id} not found in in_progress")
    return execution


def _rerun_for_stale_collect_buffer(
    tick: TickStepResult,
    worker_state: InternalStepWorkerState,
    this_execution: InProgressState,
) -> list[WorkflowCommand] | None:
    """Re-run the work item against fresh state if its collect snapshot is stale.

    Legacy ctx.collect_events() buffers are optimistic snapshots. If another
    worker changed the live buffer before this invocation consumed it, the
    invocation's result is invalid; re-run the same work item. Returns the
    re-run command (and refreshes the execution's snapshot), or None when the
    snapshot is still current.
    """
    stale_firing = any(
        isinstance(r, DeleteCollectedEvent)
        and _collect_buffer_diverged(
            worker_state.collected_events.get(r.event_id, []),
            this_execution.shared_state.collected_events.get(r.event_id, []),
        )
        for r in tick.result
    )
    if not stale_firing:
        return None
    this_execution.shared_state = replace(
        this_execution.shared_state,
        collected_events={x: list(y) for x, y in worker_state.collected_events.items()},
    )
    return [
        CommandRunWorker(
            step_id=tick.step_id,
            event=this_execution.event,
            bound_events=this_execution.bound_events,
            id=this_execution.worker_id,
        )
    ]


def _fan_out_scope(
    state: BrokerState,
    step_name: str,
    this_execution: InProgressState,
    *,
    fanned_out: bool,
) -> _FanOutScope:
    trigger_stack = this_execution.scope_path
    fan_out_stream_id = (
        _mint_stream_id(state, trigger_stack, step_name) if fanned_out else None
    )
    emit_stack = (
        trigger_stack + (fan_out_stream_id,)
        if fan_out_stream_id is not None
        else trigger_stack
    )
    return _FanOutScope(
        trigger_stack=trigger_stack,
        fanned_out=fanned_out,
        fan_out_stream_id=fan_out_stream_id,
        emit_stack=emit_stack,
    )


def _apply_step_result(
    result: StepFunctionResult,
    *,
    tick: TickStepResult,
    state: BrokerState,
    worker_state: InternalStepWorkerState,
    this_execution: InProgressState,
    scope: _FanOutScope,
    acc: _StepResultAcc,
    did_complete_step: bool,
    run_id: str | None,
) -> None:
    """Apply a single result item from a step execution to the state."""
    step_id = tick.step_id
    step_name = _root_step_key(step_id)
    if isinstance(result, StepWorkerResult):
        acc.output_event_name = str(type(result.result))
        if isinstance(result.result, StopEvent):
            # huzzah! The workflow has completed
            acc.commands.append(
                CommandPublishEvent(event=result.result)
            )  # stop event always published to the stream
            state.is_running = False
            # Clear collected_events and collected_waiters since workflow is complete
            for worker in state.workers.values():
                worker.queue.clear()
                worker.in_progress.clear()
                worker.collected_events.clear()
                worker.static_collect_events.clear()
                worker.collected_waiters.clear()
            # Drop open collection state; no release can fire after the run ends.
            _clear_collection_state(state)
            acc.commands.append(CommandCompleteRun(result=result.result))
        elif isinstance(result.result, Event):
            # queue any subsequent events
            # human input required are automatically published to the stream
            if isinstance(result.result, InputRequiredEvent):
                acc.commands.append(CommandPublishEvent(event=result.result))
            acc.commands.append(
                CommandQueueEvent(
                    event=result.result,
                    recovery_counts=dict(this_execution.recovery_counts),
                    scope_path=scope.emit_stack,
                )
            )
        elif result.result is None:
            # None means skip
            pass
        else:
            logger.warning(
                f"Unknown result type returned from step function ({step_name}): {type(result.result)}"
            )
    elif isinstance(result, StepWorkerFailed):
        _schedule_retry_or_route_failure(
            result,
            tick=tick,
            state=state,
            worker_state=worker_state,
            this_execution=this_execution,
            acc=acc,
            run_id=run_id,
        )
    elif isinstance(result, AddCollectedEvent):
        # The current state of collected events.
        collected_events = state.workers[step_name].collected_events.setdefault(
            result.event_id, []
        )
        # the events snapshot that was sent with the step function execution that yielded this result
        snapshot_events = this_execution.shared_state.collected_events.get(
            result.event_id, []
        )
        if len(collected_events) > len(snapshot_events):
            # rerun it, and don't append now to ensure serializability
            # updating the run state
            acc.step_no_longer_in_progress = False
            updated_state = replace(
                this_execution.shared_state,
                collected_events={
                    x: list(y)
                    for x, y in state.workers[step_name].collected_events.items()
                },
            )
            this_execution.shared_state = updated_state
            acc.commands.append(
                CommandRunWorker(
                    step_id=step_id,
                    event=result.event,
                    bound_events=this_execution.bound_events,
                    id=this_execution.worker_id,
                )
            )
        else:
            collected_events.append(result.event)
    elif isinstance(result, DeleteCollectedEvent):
        if did_complete_step:  # allow retries to grab the events
            # indicates that a run has successfully collected its events, and they can be deleted from the collected events state
            state.workers[step_name].collected_events.pop(result.event_id, None)
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
            bound_events=this_execution.bound_events,
            waiting_for_event=result.event_type,
            requirements=result.requirements,
            has_requirements=bool(len(result.requirements)),
            resolved_event=None,
            # Store the suspended work item's record so resume re-delivers
            # it whole: same stream scope, same collect batch.
            scope_path=this_execution.scope_path,
            collection_release_payload=this_execution.shared_state.collection_release_payload,
            work_item_id=this_execution.work_item_id,
        )
        if existing is not None:
            worker_state.collected_waiters[existing] = new_waiter
        else:
            worker_state.collected_waiters.append(new_waiter)
            if result.waiter_event:
                acc.commands.append(CommandPublishEvent(event=result.waiter_event))
            if result.timeout is not None:
                acc.commands.append(
                    CommandScheduleWaiterTimeout(
                        step_id=step_id,
                        waiter_id=result.waiter_id,
                        timeout=result.timeout,
                    )
                )
    elif isinstance(result, DeleteWaiter):
        if did_complete_step:  # allow retries to grab the waiter events
            # indicates that a run has obtained the waiting event, and it can be deleted from the collected waiters state
            to_remove = result.waiter_id
            waiters = state.workers[step_name].collected_waiters
            item = next(filter(lambda w: w.waiter_id == to_remove, waiters), None)
            if item is not None:
                waiters.remove(item)
    else:
        raise ValueError(f"Unknown result type: {type(result)}")


def _schedule_retry_or_route_failure(
    result: StepWorkerFailed,
    *,
    tick: TickStepResult,
    state: BrokerState,
    worker_state: InternalStepWorkerState,
    this_execution: InProgressState,
    acc: _StepResultAcc,
    run_id: str | None,
) -> None:
    """Handle a failed execution: schedule a retry if permitted, route to a
    catch_error handler if one applies, otherwise fail the workflow."""
    step_id = tick.step_id
    step_name = _root_step_key(step_id)
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
            step_name=step_name,
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
                event=this_execution.event,
                bound_events=this_execution.bound_events,
                attempts=this_execution.attempts + 1,
                first_attempt_at=first_attempt_at,
                last_exception=result.exception,
                last_failed_at=result.failed_at,
                recovery_counts=dict(this_execution.recovery_counts),
                not_before=not_before,
                scope_path=this_execution.scope_path,
                collection_release_payload=this_execution.shared_state.collection_release_payload,
                work_item_id=this_execution.work_item_id,
            ),
        )
        if not_before is not None:
            acc.commands.append(CommandScheduleWakeup(at_time=not_before))
        acc.redelivery_scheduled = True
        return

    exception = result.exception
    total_attempts = this_execution.attempts + 1
    elapsed = result.failed_at - first_attempt_at

    handler_name = state.config.handler_for_step.get(step_name)
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
    should_route = handler is not None and new_count <= handler.max_recoveries
    if should_route and handler is not None:
        # Route to the catch-error handler. Keep workflow running so
        # the handler can produce either a StopEvent or a new failure.
        step_failed_event = StepFailedEvent(
            step_name=step_name,
            input_event=tick.event,
            exception=exception,
            attempts=total_attempts,
            elapsed_seconds=elapsed,
            failed_at=datetime.fromtimestamp(result.failed_at, tz=timezone.utc),
        )
        # The recovered branch continues at the same stream level:
        # the handler event inherits the failing work item's scope
        # so its output stays in-stream and the stream can still
        # close. It routes to the handler step, so it must not
        # carry the collect payload.
        acc.commands.append(
            _queue_catch_error_event(
                this_execution,
                event=step_failed_event,
                step_id=StepId.root(handler.step_name),
                recovery_counts={
                    **this_execution.recovery_counts,
                    handler.step_name: new_count,
                },
            )
        )
        acc.redelivery_scheduled = True
    else:
        # Publish a WorkflowFailedEvent to inform stream consumers about the failure
        state.is_running = False
        acc.commands.append(
            CommandPublishEvent(
                event=WorkflowFailedEvent(
                    step_name=step_name,
                    exception=exception,
                    attempts=total_attempts,
                    elapsed_seconds=elapsed,
                )
            )
        )
        acc.commands.append(CommandFailWorkflow(step_id=step_id, exception=exception))


def _resolve_work_item_in_stream(
    tick: TickStepResult,
    state: BrokerState,
    scope: _FanOutScope,
    acc: _StepResultAcc,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Resolve this execution's work item in its enclosing collection stream.

    Completion removes this item and adds same-scope successors. Stream close
    is driven only by source exhaustion plus ``open_work_items == 0``.
    Classification happens once, before any counter mutation.
    """
    step_name = _root_step_key(tick.step_id)
    commands: list[WorkflowCommand] = []
    emitted_non_stop = [
        x.result
        for x in tick.result
        if isinstance(x, StepWorkerResult)
        and isinstance(x.result, Event)
        and not isinstance(x.result, StopEvent)
    ]
    enclosing = scope.trigger_stack[-1] if scope.trigger_stack else None
    disposition = _classify_work_item(
        tick,
        acc.commands,
        rerun_scheduled=not acc.step_no_longer_in_progress,
        redelivery_scheduled=acc.redelivery_scheduled,
        fanned_out=scope.fanned_out,
    )

    if (
        disposition is WorkDisposition.FANNED_OUT
        and scope.fan_out_stream_id is not None
    ):
        bindings = state.config.bindings_for_source(step_name)
        accepting_binding_ids = tuple(binding.id for binding in bindings)
        seed = sum(_count_accepting_steps(state, type(m)) for m in emitted_non_stop)
        state.streams[scope.fan_out_stream_id] = CollectionStreamInstance(
            stream_id=scope.fan_out_stream_id,
            source_step=step_name,
            scope_path=scope.trigger_stack,
            accepting_binding_ids=accepting_binding_ids,
            open_work_items=seed,
        )
        # The parent work item now waits for each child collection release.
        commands.extend(
            _adjust_open_work_items(
                state, enclosing, len(accepting_binding_ids) - 1, now_seconds
            )
        )
        if seed == 0:
            commands.extend(
                _close_collection_stream(state, scope.fan_out_stream_id, now_seconds)
            )
    elif disposition is WorkDisposition.COMPLETED:
        # Same-level resolution (1:1 step, or a collect step firing its
        # summary). Remove this work item and add its same-level successors: one
        # work item per accepting step per emitted event. A step that returns
        # None adds zero successors and simply leaves the set.
        successors = sum(
            _count_accepting_steps(state, type(ev)) for ev in emitted_non_stop
        )
        commands.extend(
            _adjust_open_work_items(state, enclosing, successors - 1, now_seconds)
        )
    elif disposition is WorkDisposition.ABSORBED:
        # Consumed once per work item, no matter how many slot buffers the
        # invocation touched.
        commands.extend(_adjust_open_work_items(state, enclosing, -1, now_seconds))
    # STILL_LIVE re-delivers and resolves later; RUN_ENDING needs no accounting.
    return commands


def _process_step_result_tick(
    tick: TickStepResult,
    init: BrokerState,
    now_seconds: float,
    run_id: str | None = None,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Process the results from a step function execution.

    Reads top to bottom as the lifecycle of one finished execution:
    locate it, bail out to a re-run if its collect snapshot went stale,
    apply each returned result, resolve the work item in its stream, then
    finalize (emit the NOT_RUNNING transition, drop it from in_progress,
    dispatch any newly-eligible queued work).
    """
    state = init.deepcopy()
    step_id = tick.step_id
    step_name = _root_step_key(step_id)
    worker_state = state.workers[step_name]
    this_execution = _find_in_progress(worker_state, tick.worker_id)

    rerun = _rerun_for_stale_collect_buffer(tick, worker_state, this_execution)
    if rerun is not None:
        return state, rerun

    fanned_out = any(
        isinstance(x, StepWorkerResult) and x.fanned_out for x in tick.result
    )
    scope = _fan_out_scope(state, step_name, this_execution, fanned_out=fanned_out)

    did_complete_step = any(isinstance(x, StepWorkerResult) for x in tick.result)
    acc = _StepResultAcc(commands=[])
    for result in tick.result:
        _apply_step_result(
            result,
            tick=tick,
            state=state,
            worker_state=worker_state,
            this_execution=this_execution,
            scope=scope,
            acc=acc,
            did_complete_step=did_complete_step,
            run_id=run_id,
        )

    acc.commands.extend(
        _resolve_work_item_in_stream(tick, state, scope, acc, now_seconds)
    )

    is_completed = any(indicates_exit(c) for c in acc.commands)
    if acc.step_no_longer_in_progress:
        acc.commands.insert(
            0,
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.NOT_RUNNING,
                    name=step_name,
                    input_event_name=str(type(tick.event)),
                    output_event_name=acc.output_event_name,
                    worker_id=str(tick.worker_id),
                )
            ),
        )
        if this_execution in worker_state.in_progress:
            worker_state.in_progress.remove(this_execution)
    # enqueue next events if there are any
    if not is_completed:
        acc.commands.extend(_drain_eligible_queue(step_id, worker_state, now_seconds))

    return state, acc.commands


def _static_collect_events(
    event: Event,
    worker_state: InternalStepWorkerState,
) -> dict[str, Event] | None:
    """Buffer a statically routed fan-in event until every declared slot is filled."""
    collect_params = worker_state.config.collect_params
    if collect_params is None:
        return None

    worker_state.static_collect_events.append(event)
    selected_indices = _select_static_collect_batch(
        events=worker_state.static_collect_events,
        collect_params=collect_params,
        allow_subclasses=worker_state.config.accept_event_subclasses,
    )
    if selected_indices is None:
        return None

    binding = {
        param_name: worker_state.static_collect_events[event_index]
        for (param_name, _), event_index in zip(collect_params, selected_indices)
    }
    selected = set(selected_indices)
    worker_state.static_collect_events = [
        pending
        for index, pending in enumerate(worker_state.static_collect_events)
        if index not in selected
    ]
    return binding


def _select_static_collect_batch(
    events: list[Event],
    collect_params: list[tuple[str, Any]],
    allow_subclasses: bool,
) -> list[int] | None:
    def search(param_index: int, used: set[int]) -> list[int] | None:
        if param_index == len(collect_params):
            return []

        _, event_type = collect_params[param_index]
        for event_index, candidate in enumerate(events):
            if event_index in used:
                continue
            if not event_matches(
                candidate,
                event_type,
                allow_subclasses=allow_subclasses,
            ):
                continue

            used.add(event_index)
            rest = search(param_index + 1, used)
            used.remove(event_index)
            if rest is not None:
                return [event_index, *rest]
        return None

    return search(0, set())


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
    step_name = _root_step_key(step_id)
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
            collection_release_payload=event.collection_release_payload._copy()
            if event.collection_release_payload is not None
            else None,
            scope_path=event.scope_path,
            work_item_id=event.work_item_id,
        )
        state.in_progress.append(
            InProgressState(
                event=event.event,
                bound_events=event.bound_events,
                worker_id=id,
                shared_state=shared_state,
                attempts=event.attempts or 0,
                first_attempt_at=event.first_attempt_at or now_seconds,
                last_exception=event.last_exception,
                last_failed_at=event.last_failed_at,
                recovery_counts=dict(event.recovery_counts),
                scope_path=event.scope_path,
                work_item_id=event.work_item_id,
            )
        )
        commands.append(
            CommandRunWorker(
                step_id=step_id,
                event=event.event,
                id=id,
                bound_events=event.bound_events,
            )
        )
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


@dataclass
class _RouteResult:
    """Outcome of routing an added event to accepting steps."""

    commands: list[WorkflowCommand]
    # At least one step accepted (and was targeted by) the event.
    handled: bool = False
    # The run was failed (a targeted send to a collect step); the caller must
    # return immediately rather than emit further commands.
    failed: bool = False


def _redeliver_collection_payload(
    tick: TickAddEvent, state: BrokerState, now_seconds: float
) -> list[WorkflowCommand] | None:
    """Re-deliver a payload-carrying tick straight to its collect target.

    A payload-carrying tick is a re-delivered collect invocation (retry,
    waiter re-ping after resume, serialized requeue). It routes directly to
    the binding's target step — before waiter matching, before the
    member-arrival path — so it can never be swallowed as a stream member or
    resolve an unrelated waiter. The event is derived from the payload, the
    authoritative work record. Returns None when this is not a payload tick.
    """
    if tick.collection_release_payload is None:
        return None
    payload = tick.collection_release_payload
    binding = state.config.collection_bindings.get(payload.binding_id)
    if binding is None:
        raise WorkflowRuntimeError(
            f"Collect invocation re-delivered for unknown binding "
            f"{payload.binding_id!r} (stream {payload.stream_id!r}). "
            "Workflow state is corrupt."
        )
    return _add_or_enqueue_event(
        EventAttempt(
            event=payload.as_event(),
            attempts=tick.attempts,
            first_attempt_at=tick.first_attempt_at,
            last_exception=tick.last_exception,
            last_failed_at=tick.last_failed_at,
            recovery_counts=dict(tick.recovery_counts),
            scope_path=tuple(tick.scope_path),
            collection_release_payload=payload,
            work_item_id=tick.work_item_id,
        ),
        StepId.root(binding.target_step),
        state.workers[binding.target_step],
        now_seconds,
    )


def _resolve_waiters(
    tick: TickAddEvent, state: BrokerState, now_seconds: float
) -> tuple[list[WorkflowCommand], set[str]]:
    """Resolve any waiters the event satisfies, resuming their work items.

    Returns the emitted commands plus the set of steps woken via waiter
    resolution, so the routing pass can skip them and avoid double-processing
    the same delivery as a normally-accepted event.
    """
    commands: list[WorkflowCommand] = []
    waiter_resolved_steps: set[str] = set()
    for step_name, step_config in state.config.steps.items():
        step_id = StepId.root(step_name)
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
                waiter_resolved_steps.add(step_name)
                wait_condition.resolved_event = tick.event
                # Resume re-delivers the suspended work item whole from the
                # waiter record: original trigger, stream scope, collect batch.
                commands.extend(
                    _add_or_enqueue_event(
                        EventAttempt(
                            event=wait_condition.event,
                            bound_events=wait_condition.bound_events,
                            scope_path=wait_condition.scope_path,
                            collection_release_payload=wait_condition.collection_release_payload,
                            work_item_id=wait_condition.work_item_id,
                        ),
                        step_id,
                        state.workers[step_name],
                        now_seconds,
                    )
                )
    return commands, waiter_resolved_steps


def _route_member_to_collect_step(
    tick: TickAddEvent,
    state: BrokerState,
    step_name: str,
    worker_state: InternalStepWorkerState,
    now_seconds: float,
) -> tuple[list[WorkflowCommand], bool]:
    """Route an accepted event into a collect step's batch buffer.

    A collect step only ever receives members emitted inside a fan-out stream.
    Returns ``(commands, failed)`` where ``failed`` signals the run was failed
    (a targeted send to a collect step, which the runtime cannot honor).
    """
    commands: list[WorkflowCommand] = []
    if not tick.scope_path:
        # Scope-less events (ctx.send_event, external sends) can
        # never join a collect batch — members reach a collect step
        # only by being emitted inside a fan-out stream.
        if tick.step_id is not None:
            # A targeted send is an explicit instruction the
            # runtime cannot honor; dropping it silently loses
            # data, so fail the run loudly instead.
            error = WorkflowRuntimeError(
                f"{type(tick.event).__name__} was sent to collect "
                f"step {step_name!r} via send_event(step=...), but "
                "a collect step cannot receive targeted events: it "
                "only collects events emitted inside a fan-out "
                "stream."
            )
            state.is_running = False
            commands.append(
                CommandPublishEvent(
                    event=WorkflowFailedEvent(step_name=step_name, exception=error)
                )
            )
            commands.append(
                CommandFailWorkflow(step_id=StepId.root(step_name), exception=error)
            )
            return commands, True
        # An untargeted send may be legitimate traffic for other
        # steps that merely overlaps an open stream — warn.
        logger.warning(
            "Ignoring %s for collect step %r: it was sent "
            "outside any collection stream (e.g. via "
            "ctx.send_event) so it cannot join a batch.",
            type(tick.event).__name__,
            step_name,
        )
        return commands, False
    stream_id = tick.scope_path[-1]
    binding = state.config.binding_for_target(stream_id, step_name, state.streams)
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
        commands.extend(_adjust_open_work_items(state, stream_id, -1, now_seconds))
        return commands, False
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
    commands.extend(_adjust_open_work_items(state, stream_id, -1, now_seconds))
    return commands, False


def _route_to_accepting_steps(
    tick: TickAddEvent,
    state: BrokerState,
    waiter_resolved_steps: set[str],
    now_seconds: float,
) -> _RouteResult:
    """Route the event to every step that accepts (and is targeted by) it.

    Steps already woken via waiter resolution are skipped — only their stream
    accounting is balanced for the delivery the waiter swallowed.
    """
    result = _RouteResult(commands=[])
    for step_name, step_config in state.config.steps.items():
        step_id = StepId.root(step_name)
        is_accepted = step_accepts_event(
            tick.event,
            step_config.accepted_events,
            allow_subclasses=step_config.accept_event_subclasses,
        )
        is_targeted = tick.step_id is None or _root_step_key(tick.step_id) == step_name
        if step_name in waiter_resolved_steps:
            if is_accepted and is_targeted and tick.scope_path:
                # The waiter swallowed a delivery this step would otherwise
                # have received. The delivery was birth-counted as a work item
                # in its stream, so consume it here — otherwise the stream can
                # never close. This covers both 1:1 steps and collect steps
                # parked on wait_for_event of their own member type (the
                # swallowed member never joins the batch; the waiter consumed
                # it).
                result.commands.extend(
                    _adjust_open_work_items(state, tick.scope_path[-1], -1, now_seconds)
                )
            continue
        if not (is_accepted and is_targeted):
            continue
        result.handled = True
        worker_state = state.workers[step_name]
        if worker_state.config.collection_param is not None:
            member_commands, failed = _route_member_to_collect_step(
                tick, state, step_name, worker_state, now_seconds
            )
            result.commands.extend(member_commands)
            if failed:
                result.failed = True
                return result
            continue
        _consume_superseded_delayed_attempt(tick, worker_state)
        bound_events = tick.bound_events
        if bound_events is None and worker_state.config.collect_params:
            bound_events = _static_collect_events(
                event=tick.event,
                worker_state=worker_state,
            )
            if bound_events is None:
                if tick.scope_path:
                    result.commands.extend(
                        _adjust_open_work_items(
                            state, tick.scope_path[-1], -1, now_seconds
                        )
                    )
                continue
        result.commands.extend(
            _add_or_enqueue_event(
                EventAttempt(
                    event=tick.event,
                    bound_events=bound_events,
                    attempts=tick.attempts,
                    first_attempt_at=tick.first_attempt_at,
                    last_exception=tick.last_exception,
                    last_failed_at=tick.last_failed_at,
                    recovery_counts=dict(tick.recovery_counts),
                    scope_path=tuple(tick.scope_path),
                    work_item_id=tick.work_item_id,
                ),
                step_id,
                state.workers[step_name],
                now_seconds,
            )
        )
    return result


def _unhandled_event_commands(
    tick: TickAddEvent, state: BrokerState
) -> list[WorkflowCommand]:
    # InputRequiredEvent subclasses are intentionally designed to be handled
    # externally by human consumers, not by workflow steps. Don't emit
    # UnhandledEvent for these since they're working as intended.
    if isinstance(tick.event, InputRequiredEvent):
        return []
    event_cls = type(tick.event)
    return [
        CommandPublishEvent(
            UnhandledEvent(
                event_type=event_cls.__name__,
                qualified_name=f"{event_cls.__module__}.{event_cls.__name__}",
                step_name=str(tick.step_id) if tick.step_id else None,
                idle=_check_idle_state(state),
            )
        )
    ]


def _process_add_event_tick(
    tick: TickAddEvent, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Add an incoming event to the workflow.

    Three phases: re-deliver a payload-carrying collect invocation straight to
    its target (and stop), else resolve any waiters the event satisfies, then
    route it to every accepting step. An event nothing handled is published as
    an UnhandledEvent.
    """
    state = init.deepcopy()
    if tick.work_item_id is None:
        # A collect re-delivery derives its id from the payload's stable
        # stream+binding key so it matches the invocation fired at release time
        # and never re-mints a fresh id on resume; everything else mints from the
        # monotonic counter.
        work_item_id = (
            tick.collection_release_payload.work_item_id()
            if tick.collection_release_payload is not None
            else _next_work_item_id(state)
        )
        tick = tick.model_copy(update={"work_item_id": work_item_id})
    if isinstance(tick.event, StartEvent):
        state.is_running = True

    payload_commands = _redeliver_collection_payload(tick, state, now_seconds)
    if payload_commands is not None:
        return state, payload_commands

    commands, waiter_resolved_steps = _resolve_waiters(tick, state, now_seconds)

    routed = _route_to_accepting_steps(tick, state, waiter_resolved_steps, now_seconds)
    commands.extend(routed.commands)
    if routed.failed:
        return state, commands

    handled = bool(waiter_resolved_steps) or routed.handled
    if not handled:
        commands.extend(_unhandled_event_commands(tick, state))
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
        step_id = StepId.root(step_name)
        for attempt in worker_state.queue:
            if attempt.not_before is not None and attempt.not_before <= tick.due:
                attempt.not_before = None
        commands.extend(_drain_eligible_queue(step_id, worker_state, now_seconds))
    return state, commands


def _process_waiter_timeout_tick(
    tick: TickWaiterTimeout, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    state = init.deepcopy()
    commands: list[WorkflowCommand] = []
    step_id = tick.step_id
    step_name = _root_step_key(step_id)
    if step_name not in state.workers:
        return state, commands
    worker_state = state.workers[step_name]
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
            bound_events=waiter.bound_events,
            scope_path=waiter.scope_path,
            collection_release_payload=waiter.collection_release_payload,
            work_item_id=waiter.work_item_id,
        ),
        step_id,
        worker_state,
        now_seconds,
    )
    commands.extend(subcommands)
    return state, commands
