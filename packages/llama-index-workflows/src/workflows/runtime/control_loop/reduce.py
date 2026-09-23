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
    _child_slots_accepting,
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
    CommandCancelNamespace,
    CommandCompleteRun,
    CommandFailWorkflow,
    CommandHalt,
    CommandPublishEvent,
    CommandQueueEvent,
    CommandRunWorker,
    CommandScheduleIdleCheck,
    CommandScheduleNamespaceTimeout,
    CommandScheduleTimeout,
    CommandScheduleWaiterTimeout,
    CommandScheduleWakeup,
    WorkflowCommand,
    indicates_exit,
)
from workflows.runtime.types.internal_state import (
    BrokerConfig,
    BrokerState,
    ChildBroker,
    CollectionStreamInstance,
    EventAttempt,
    InProgressState,
    InternalStepWorkerState,
)
from workflows.runtime.types.invocation import (
    INVOCATION_SEPARATOR,
    mint_slot_segment,
    slot_namespace,
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
    STAMPED_TICK_TYPES,
    TickAddEvent,
    TickCancelRun,
    TickIdleCheck,
    TickIdleRelease,
    TickNamespaceTimeout,
    TickPublishEvent,
    TickSessionStart,
    TickStepResult,
    TickTimeout,
    TickWaiterTimeout,
    TickWakeup,
    WorkflowTick,
)

logger = logging.getLogger("workflows.runtime.control_loop")


def rebuild_state_from_ticks(
    state: BrokerState,
    ticks: list[WorkflowTick],
    run_id: str | None = None,
) -> BrokerState:
    """Rebuild the state from a list of ticks.

    Applies rewind_in_progress() first, matching resume, so worker ids align
    with the recorded ticks. run_id seeds retry jitter so replaying a failure
    tick recomputes the delay the live run journaled.
    """
    state, _ = rewind_in_progress(state, time.time())
    for tick in ticks:
        state, _ = _reduce_tick(tick, state, time.time(), run_id=run_id)
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


# Slack for the alive-budget comparison. Accrual deltas come from one clock, so
# at a scheduled deadline the accrued budget equals the timeout to within float
# rounding; the epsilon keeps the "spent" test from missing by an ULP.
_ALIVE_EPS = 1e-6


def _effective_now(tick: WorkflowTick, now_seconds: float) -> float:
    """Prefer a journaled tick timestamp over the reducer's clock.

    Fixes the replay clock: a tick stamped before ``on_tick`` carries the live
    run's time, so replaying it makes the same time-based decisions. Old ticks
    (no stamp) fall back to ``now_seconds``.
    """
    stamped = _tick_stamp(tick)
    return stamped if stamped is not None else now_seconds


def _tick_stamp(tick: WorkflowTick) -> float | None:
    """The tick's journaled wall-clock stamp, or None for unstamped ticks.

    Only stamped ticks (the work + deadline ticks) accrue alive time. Old
    markerless journals carry no stamps, so they replay with an empty budget and
    never fire a spurious timeout under the legacy fallback.
    """
    return tick.stamped_at if isinstance(tick, STAMPED_TICK_TYPES) else None


def _accrue_alive(broker: BrokerState, stamp: float) -> None:
    """Accrue one broker's known-alive time up to ``stamp``.

    ``elapsed_alive`` gains the gap since the last reference (the process was
    alive across it); the reference then advances. The session-start marker moves
    the reference *without* accruing, so inter-session downtime never counts.
    """
    if broker.last_alive_stamp is not None:
        delta = stamp - broker.last_alive_stamp
        if delta > 0:
            broker.elapsed_alive += delta
    broker.last_alive_stamp = (
        stamp
        if broker.last_alive_stamp is None
        else max(broker.last_alive_stamp, stamp)
    )


def _accrue_descent(descent: _Descent, stamp: float | None) -> None:
    """Accrue alive time on every broker on the addressed path (root..target).

    A tick addressed to a nested broker is alive time for that broker and every
    ancestor it descends through, so a parent's budget advances whenever any
    descendant does work. A quiet parent still reaches its budget because the
    deadline tick's own accrual captures the final quiet-but-alive gap.
    """
    if stamp is None:
        return
    for parent_broker, _seg, _child in descent.chain:
        _accrue_alive(parent_broker, stamp)
    _accrue_alive(descent.broker, stamp)


def _reset_alive_stamps(state: BrokerState, stamp: float | None) -> None:
    """Session-start reset: advance every broker's accrual reference, no accrual.

    Forgives the inter-session downtime (the gap between the previous session's
    last tick and this resume) uniformly across the whole broker tree, on both
    the snapshot-resume and full-journal-replay paths.
    """

    def walk(broker: BrokerState) -> None:
        broker.last_alive_stamp = stamp
        for child in broker.children.values():
            walk(child.state)

    walk(state)


def _reduce_tick(
    tick: WorkflowTick,
    init: BrokerState,
    now_seconds: float,
    run_id: str | None = None,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    now_seconds = _effective_now(tick, now_seconds)
    if isinstance(tick, TickStepResult):
        state, commands = _dispatch_step_result(tick, init, now_seconds, run_id)
    elif isinstance(tick, TickAddEvent):
        state, commands = _dispatch_add_event(tick, init, now_seconds)
    elif isinstance(tick, TickCancelRun):
        state, commands = _process_cancel_run_tick(tick, init)
    elif isinstance(tick, TickIdleRelease):
        # Return early — idle release does not schedule idle checks
        return init, [CommandCompleteRun(result=IdleReleasedEvent())]
    elif isinstance(tick, TickPublishEvent):
        state, commands = _process_publish_event_tick(tick, init)
    elif isinstance(tick, TickTimeout):
        state, commands = _process_timeout_tick(tick, init)
    elif isinstance(tick, TickSessionStart):
        # Session boundary: advance every broker's accrual reference to this
        # stamp WITHOUT accruing, so the downtime since the previous session is
        # forgiven. Never schedules further work.
        state = init.deepcopy()
        _reset_alive_stamps(state, _tick_stamp(tick))
        return state, []
    elif isinstance(tick, TickWaiterTimeout):
        state, commands = _dispatch_waiter_timeout(tick, init, now_seconds)
    elif isinstance(tick, TickNamespaceTimeout):
        state, commands = _process_namespace_timeout_tick(tick, init, now_seconds)
    elif isinstance(tick, TickIdleCheck):
        # Return early — idle check ticks don't schedule further idle checks
        if _check_idle_state(init):
            stuck = _detect_stuck_streams_tree(init)
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


# --- Addressing: descend to the broker that owns a path-addressed tick --------


@dataclass
class _Descent:
    """A resolved path from root to the addressed broker.

    ``chain`` is the ancestor list ``(parent_broker, slot_key, child_record)``
    from root down; ``broker`` is the addressed broker; ``path`` its invocation
    path. The chain lets ascent reach the parent that owns a completing child.
    """

    chain: list[tuple[BrokerState, str, ChildBroker]]
    broker: BrokerState
    path: tuple[str, ...]


def _descend(root_state: BrokerState, path: tuple[str, ...]) -> _Descent | None:
    chain: list[tuple[BrokerState, str, ChildBroker]] = []
    broker = root_state
    for seg in path:
        child = broker.children.get(seg)
        if child is None:
            return None
        chain.append((broker, seg, child))
        broker = child.state
    return _Descent(chain=chain, broker=broker, path=path)


def _is_eligible(attempt: EventAttempt) -> bool:
    """Delayed attempts (not_before set) only become eligible via TickWakeup."""
    return attempt.not_before is None


def _is_static_child_path(path: tuple[str, ...]) -> bool:
    """True when an invocation path names a slot statically, not concretely.

    Concrete invocation segments minted at descent always carry the counter
    separator (``child#0``); a segment without it (``child``) is a static slot
    reference that can't identify one of possibly-overlapping invocations.
    """
    return any(INVOCATION_SEPARATOR not in seg for seg in path)


def _static_step_name(path: tuple[str, ...], step_id: StepId) -> str:
    """Published step name: the static (slot-name) path plus the local step name.

    Reduction keys steps by their bare local ``StepId``, but everything surfaced
    to consumers (StepStateChanged, StepFailedEvent, UnhandledEvent) re-prepends
    the static path — ``child/run`` for a child at ``child#0``, never
    ``child#0/run``. Root steps (empty path) keep their bare name.
    """
    return "/".join((*slot_namespace(path), step_id.name))


def _next_work_item_id(state: BrokerState) -> str:
    """Mint a deterministic identity for one routed work item, per broker."""
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

    Jitter is seeded from (run_id, step_name, failures) so the same failure
    always samples the same delay. Policies whose ``next`` predates the ``seed``
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
    path: tuple[str, ...],
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Dispatch eligible queued attempts while worker capacity remains.

    Scans past ineligible (delayed) attempts so they neither block eligible
    work queued behind them nor consume a worker slot. Relative order among
    eligible attempts is preserved.
    """
    commands: list[WorkflowCommand] = []
    while True:
        index = next(
            (
                i
                for i, a in enumerate(state.queue)
                if _is_eligible(a) and len(state.in_progress) < state.config.num_workers
            ),
            None,
        )
        if index is None:
            break
        attempt = state.queue.pop(index)
        commands.extend(
            _add_or_enqueue_event(attempt, step_id, state, path, now_seconds)
        )
    return commands


def rewind_in_progress(
    state: BrokerState,
    now_seconds: float,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Rewind in_progress work across the whole broker tree.

    Recurses, path-qualifying re-armed CommandRunWorker/CommandScheduleWakeup
    with each broker's invocation path so the runner registers them correctly.
    """
    state = state.deepcopy()
    commands: list[WorkflowCommand] = []
    _rewind_broker(state, (), commands, now_seconds)
    return state, commands


def _rewind_broker(
    state: BrokerState,
    path: tuple[str, ...],
    commands: list[WorkflowCommand],
    now_seconds: float,
) -> None:
    for step_id, step_state in sorted(state.workers.items(), key=lambda x: str(x[0])):
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
        commands.extend(_drain_eligible_queue(step_id, step_state, path, now_seconds))
        for attempt in step_state.queue:
            if attempt.not_before is not None:
                commands.append(CommandScheduleWakeup(at_time=attempt.not_before))
    for key, child in sorted(state.children.items()):
        _rewind_broker(child.state, (*path, key), commands, now_seconds)


def _check_idle_state(state: BrokerState) -> bool:
    """Returns True if the workflow is idle (no work can advance internally).

    Recurses the broker tree: idle iff the root is running and every broker's
    queues and in-progress executions are empty. A delayed retry (future
    not_before) counts as pending work, so idle release defers.
    """
    if not state.is_running:
        return False
    return not _broker_busy(state)


def _broker_busy(state: BrokerState) -> bool:
    for worker_state in state.workers.values():
        if worker_state.queue or worker_state.in_progress:
            return True
    return any(_broker_busy(child.state) for child in state.children.values())


def _detect_stuck_streams_tree(
    state: BrokerState,
) -> tuple[str, WorkflowRuntimeError] | None:
    """Recursively check every broker for a provably-stuck stream state."""

    def walk(broker: BrokerState, path: tuple[str, ...]):
        stuck = _detect_stuck_streams(broker, path)
        if stuck is not None:
            return stuck
        for key, child in broker.children.items():
            found = walk(child.state, (*path, key))
            if found is not None:
                return found
        return None

    return walk(state, ())


def _collect_buffer_diverged(live: list[Event], snapshot: list[Event]) -> bool:
    """True when a live ctx.collect_events() buffer no longer matches a snapshot."""
    return len(live) != len(snapshot) or any(a is not b for a, b in zip(live, snapshot))


def _queue_catch_error_event(
    this_execution: InProgressState,
    *,
    event: Event,
    step_id: StepId,
    path: tuple[str, ...],
    recovery_counts: dict[str, int],
) -> CommandQueueEvent:
    """Build a catch_error dispatch in the failed work item's stream scope."""
    return CommandQueueEvent(
        event=event,
        step_id=step_id,
        origin_namespace=path,
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
    # This execution returned a StopEvent (this workflow's terminal), recorded
    # for the caller to complete the broker (root) or ascend (child).
    stop_event: StopEvent | None = None
    # This execution's completion descended into one or more child brokers.
    boundary_descent: bool = False
    # This execution failed uncaught inside a child broker; the caller ascends it
    # to the parent boundary. Only ever set for a non-root broker (a root failure
    # ends the whole run inline).
    boundary_failure: _BoundaryFailure | None = None


@dataclass
class _BoundaryFailure:
    """An uncaught child failure/timeout ascending to its parent boundary.

    Carries what the parent needs to route a ``StepFailedEvent`` named for the
    child's path, and to publish the right terminal event if it ascends uncaught
    all the way to the root (a timeout publishes ``WorkflowTimedOutEvent``, a step
    failure publishes ``WorkflowFailedEvent``).
    """

    exception: Exception
    input_event: Event
    attempts: int
    elapsed_seconds: float
    failed_at: float
    # Static name of the step (or child) where the failure originated, used for
    # the terminal WorkflowFailedEvent if it ascends uncaught to the root. A step
    # failure keeps the internal step name (``child/run_child``); a timeout uses
    # the child's path.
    origin_step_name: str = ""
    timed_out_event: WorkflowTimedOutEvent | None = None


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
    worker_state: InternalStepWorkerState,
    worker_id: int,
) -> InProgressState:
    execution = next(
        (w for w in worker_state.in_progress if w.worker_id == worker_id),
        None,
    )
    if execution is None:
        # this should not happen unless there's a logic bug in the control loop
        raise ValueError(f"Worker {worker_id} not found in in_progress")
    return execution


def _rerun_for_stale_collect_buffer(
    tick: TickStepResult,
    worker_state: InternalStepWorkerState,
    this_execution: InProgressState,
    path: tuple[str, ...],
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
            invocation_namespace=path,
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


def _terminate_broker(state: BrokerState) -> None:
    """Clear this broker's own work and drop its whole child subtree.

    Used at root completion (whole-run teardown) and at a child's timeout
    expiry. Task cancellation is a separate runner concern (CommandCancelNamespace).
    """
    for worker in state.workers.values():
        worker.queue.clear()
        worker.in_progress.clear()
        worker.collected_events.clear()
        worker.static_collect_events.clear()
        worker.collected_waiters.clear()
    state.streams.clear()
    state.collection_release_states.clear()
    state.children.clear()


def _apply_step_result(
    result: StepFunctionResult,
    *,
    tick: TickStepResult,
    state: BrokerState,
    path: tuple[str, ...],
    worker_state: InternalStepWorkerState,
    this_execution: InProgressState,
    scope: _FanOutScope,
    acc: _StepResultAcc,
    did_complete_step: bool,
    run_id: str | None,
) -> None:
    """Apply a single result item from a step execution to the broker state."""
    step_id = tick.step_id
    step_name = str(step_id)
    if isinstance(result, StepWorkerResult):
        acc.output_event_name = str(type(result.result))
        if isinstance(result.result, StopEvent):
            # This workflow's terminal. Root completion and child ascent are
            # decided by the caller (which knows this broker's path); here we
            # just record it.
            acc.stop_event = result.result
        elif isinstance(result.result, Event):
            # human input required are automatically published to the stream
            if isinstance(result.result, InputRequiredEvent):
                acc.commands.append(
                    CommandPublishEvent(
                        event=result.result,
                        origin_namespace=path,
                    )
                )
            acc.commands.append(
                CommandQueueEvent(
                    event=result.result,
                    origin_namespace=path,
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
            path=path,
            worker_state=worker_state,
            this_execution=this_execution,
            acc=acc,
            run_id=run_id,
        )
    elif isinstance(result, AddCollectedEvent):
        # The current state of collected events.
        collected_events = state.workers[step_id].collected_events.setdefault(
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
            this_execution.shared_state = replace(
                this_execution.shared_state,
                collected_events={
                    x: list(y)
                    for x, y in state.workers[step_id].collected_events.items()
                },
            )
            acc.commands.append(
                CommandRunWorker(
                    step_id=step_id,
                    event=result.event,
                    bound_events=this_execution.bound_events,
                    id=this_execution.worker_id,
                    invocation_namespace=path,
                )
            )
        else:
            collected_events.append(result.event)
    elif isinstance(result, DeleteCollectedEvent):
        if did_complete_step:  # allow retries to grab the events
            # indicates that a run has successfully collected its events, and they can be deleted from the collected events state
            state.workers[step_id].collected_events.pop(result.event_id, None)
    elif isinstance(result, AddWaiter):
        # indicates that a run has added a waiter to the collected waiters state
        existing = next(
            (
                i
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
                acc.commands.append(
                    CommandPublishEvent(
                        event=result.waiter_event,
                        origin_namespace=path,
                    )
                )
            if result.timeout is not None:
                acc.commands.append(
                    CommandScheduleWaiterTimeout(
                        step_id=step_id,
                        waiter_id=result.waiter_id,
                        timeout=result.timeout,
                        invocation_namespace=path,
                    )
                )
    elif isinstance(result, DeleteWaiter):
        if did_complete_step:  # allow retries to grab the waiter events
            to_remove = result.waiter_id
            waiters = state.workers[step_id].collected_waiters
            item = next(
                filter(lambda w: w.waiter_id == to_remove, waiters),
                None,
            )
            if item is not None:
                waiters.remove(item)
    else:
        raise ValueError(f"Unknown result type: {type(result)}")


def _schedule_retry_or_route_failure(
    result: StepWorkerFailed,
    *,
    tick: TickStepResult,
    state: BrokerState,
    path: tuple[str, ...],
    worker_state: InternalStepWorkerState,
    this_execution: InProgressState,
    acc: _StepResultAcc,
    run_id: str | None,
) -> None:
    """Handle a failed execution: retry if permitted, route to a broker-local
    catch_error handler if one applies, otherwise surface the failure.

    A root broker's uncaught failure ends the whole run inline. A child broker's
    uncaught failure is recorded on ``acc`` for the caller to ascend to the
    parent boundary (routed there through the parent's own catch-error table).
    """
    step_id = tick.step_id
    step_name = str(step_id)
    first_attempt_at = (
        result.first_attempt_at
        if result.first_attempt_at is not None
        else this_execution.first_attempt_at
    )
    if result.retry_decision is not None:
        delay = result.retry_decision.delay
    else:
        delay = _decide_retry_delay(
            worker_state.config.retry_policy,
            elapsed_time=result.failed_at - first_attempt_at,
            failures=this_execution.attempts + 1,
            exception=result.exception,
            run_id=run_id,
            step_name=step_name,
        )
    if delay is not None:
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

    # Route a failed step to its broker-local catch-error handler, if any.
    handler_step_id = state.config.handler_for_step.get(step_id)
    handler = (
        state.config.catch_error_handlers.get(handler_step_id)
        if handler_step_id is not None
        else None
    )
    recovery_key = str(handler_step_id) if handler_step_id is not None else None
    current_count = (
        this_execution.recovery_counts.get(recovery_key, 0)
        if recovery_key is not None
        else 0
    )
    new_count = current_count + 1
    should_route = handler is not None and new_count <= handler.max_recoveries
    if should_route and handler_step_id is not None and recovery_key is not None:
        step_failed_event = StepFailedEvent(
            step_name=_static_step_name(path, step_id),
            input_event=tick.event,
            exception=exception,
            attempts=total_attempts,
            elapsed_seconds=elapsed,
            failed_at=datetime.fromtimestamp(result.failed_at, tz=timezone.utc),
        )
        acc.commands.append(
            _queue_catch_error_event(
                this_execution,
                event=step_failed_event,
                step_id=handler_step_id,
                path=path,
                recovery_counts={
                    **this_execution.recovery_counts,
                    recovery_key: new_count,
                },
            )
        )
        acc.redelivery_scheduled = True
    elif not path:
        # Root: an uncaught failure ends the whole run.
        state.is_running = False
        acc.commands.append(
            CommandPublishEvent(
                event=WorkflowFailedEvent(
                    step_name=_static_step_name(path, step_id),
                    exception=exception,
                    attempts=total_attempts,
                    elapsed_seconds=elapsed,
                )
            )
        )
        acc.commands.append(CommandFailWorkflow(step_id=step_id, exception=exception))
    else:
        # Child: uncaught locally → ascend to the parent as a boundary failure.
        acc.boundary_failure = _BoundaryFailure(
            exception=exception,
            input_event=tick.event,
            attempts=total_attempts,
            elapsed_seconds=elapsed,
            failed_at=result.failed_at,
            origin_step_name=_static_step_name(path, step_id),
        )


def _resolve_work_item_in_stream(
    tick: TickStepResult,
    state: BrokerState,
    path: tuple[str, ...],
    scope: _FanOutScope,
    acc: _StepResultAcc,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Resolve this execution's work item in its enclosing collection stream."""
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
        boundary_descent=acc.boundary_descent,
    )

    if (
        disposition is WorkDisposition.FANNED_OUT
        and scope.fan_out_stream_id is not None
    ):
        bindings = state.config.bindings_for_source(tick.step_id)
        accepting_binding_ids = tuple(binding.id for binding in bindings)
        seed = sum(_count_accepting_steps(state, m) for m in emitted_non_stop)
        state.streams[scope.fan_out_stream_id] = CollectionStreamInstance(
            stream_id=scope.fan_out_stream_id,
            source_step=tick.step_id,
            scope_path=scope.trigger_stack,
            accepting_binding_ids=accepting_binding_ids,
            open_work_items=seed,
        )
        commands.extend(
            _adjust_open_work_items(
                state, enclosing, len(accepting_binding_ids) - 1, path, now_seconds
            )
        )
        if seed == 0:
            commands.extend(
                _close_collection_stream(
                    state, scope.fan_out_stream_id, path, now_seconds
                )
            )
    elif disposition is WorkDisposition.FANNED_OUT:
        # A 1:1 completion that descended into a child broker (no local fan-out
        # stream). The child is one work item in this broker's enclosing stream;
        # it is consumed here, added to the child stream that the descent seeded.
        # Successors are the child's ascent, counted then. Net: -1 for this item
        # plus the descent's own +1 (already reflected by the boundary work item).
        successors = sum(_count_accepting_steps(state, ev) for ev in emitted_non_stop)
        commands.extend(
            _adjust_open_work_items(state, enclosing, successors, path, now_seconds)
        )
    elif disposition is WorkDisposition.COMPLETED:
        successors = sum(_count_accepting_steps(state, ev) for ev in emitted_non_stop)
        commands.extend(
            _adjust_open_work_items(state, enclosing, successors - 1, path, now_seconds)
        )
    elif disposition is WorkDisposition.ABSORBED:
        commands.extend(
            _adjust_open_work_items(state, enclosing, -1, path, now_seconds)
        )
    # STILL_LIVE re-delivers and resolves later; RUN_ENDING needs no accounting.
    return commands


def _dispatch_step_result(
    tick: TickStepResult,
    init: BrokerState,
    now_seconds: float,
    run_id: str | None = None,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Route a step result to its broker; ascend if the broker completed."""
    state = init.deepcopy()
    path = tick.invocation_namespace
    descent = _descend(state, path)
    if descent is None:
        # Late result from a torn-down child: loud and logged (orphan burn-off).
        logger.warning(
            "Discarding step result for step %r in dead invocation %r; "
            "publishing UnhandledEvent.",
            str(tick.step_id),
            "/".join(path),
        )
        event_cls = type(tick.event)
        return state, [
            CommandPublishEvent(
                UnhandledEvent(
                    event_type=event_cls.__name__,
                    qualified_name=f"{event_cls.__module__}.{event_cls.__name__}",
                    step_name=_static_step_name(path, tick.step_id),
                    idle=_check_idle_state(state),
                ),
                origin_namespace=path,
            )
        ]

    _accrue_descent(descent, _tick_stamp(tick))
    commands, stop_event, failure = _local_step_result(
        descent.broker, path, tick, now_seconds, run_id
    )

    if stop_event is not None and descent.chain:
        # Child completed: the parent that owns it pops the record, resolves the
        # boundary work item, and re-injects the stop into its own routing.
        parent_broker, slot_key, child = descent.chain[-1]
        commands.extend(
            _ascend_child_stop(
                parent_broker,
                path[:-1],
                slot_key,
                child,
                stop_event,
                now_seconds,
            )
        )
    elif failure is not None:
        # Child failed uncaught locally: ascend to the parent boundary (always a
        # non-root path here, so descent.chain is non-empty).
        commands.extend(
            _ascend_boundary_failure(descent.chain, path, failure, now_seconds)
        )
    return state, commands


def _local_step_result(
    broker: BrokerState,
    path: tuple[str, ...],
    tick: TickStepResult,
    now_seconds: float,
    run_id: str | None,
) -> tuple[list[WorkflowCommand], StopEvent | None, _BoundaryFailure | None]:
    """Reduce one finished execution within its addressed broker.

    Returns ``(commands, boundary_stop, boundary_failure)``. ``boundary_stop`` is
    set when a child broker (path != ()) returned its StopEvent; ``boundary_
    failure`` when a child broker failed uncaught locally. Either signals the
    caller to ascend at the parent boundary; both are ``None`` for the root.
    """
    step_id = tick.step_id
    worker_state = broker.workers[step_id]
    step_name = str(step_id)
    this_execution = _find_in_progress(worker_state, tick.worker_id)

    rerun = _rerun_for_stale_collect_buffer(tick, worker_state, this_execution, path)
    if rerun is not None:
        return rerun, None, None

    fanned_out = any(
        isinstance(x, StepWorkerResult) and x.fanned_out for x in tick.result
    )
    scope = _fan_out_scope(broker, step_name, this_execution, fanned_out=fanned_out)

    did_complete_step = any(isinstance(x, StepWorkerResult) for x in tick.result)
    acc = _StepResultAcc(commands=[])
    for result in tick.result:
        _apply_step_result(
            result,
            tick=tick,
            state=broker,
            path=path,
            worker_state=worker_state,
            this_execution=this_execution,
            scope=scope,
            acc=acc,
            did_complete_step=did_complete_step,
            run_id=run_id,
        )

    boundary_stop: StopEvent | None = None
    if acc.stop_event is not None:
        if not path:
            # Root StopEvent completes the whole run.
            broker.is_running = False
            _terminate_broker(broker)
            _publish_step_transition(acc, tick, path)
            acc.commands.append(CommandPublishEvent(event=acc.stop_event))
            acc.commands.append(CommandCompleteRun(result=acc.stop_event))
            return acc.commands, None, None
        # Child StopEvent: mark the broker done and let the caller ascend. The
        # parent pops the whole subtree, so no local teardown/accounting here.
        broker.is_running = False
        boundary_stop = acc.stop_event
        _publish_step_transition(acc, tick, path)
        if this_execution in worker_state.in_progress:
            worker_state.in_progress.remove(this_execution)
        return acc.commands, boundary_stop, None

    if acc.boundary_failure is not None:
        # Uncaught local failure in a child broker: mark it done and let the
        # caller ascend at the parent boundary (the parent pops this subtree, so
        # no local stream accounting is needed here).
        broker.is_running = False
        _publish_step_transition(acc, tick, path)
        if this_execution in worker_state.in_progress:
            worker_state.in_progress.remove(this_execution)
        return acc.commands, None, acc.boundary_failure

    acc.commands.extend(
        _resolve_work_item_in_stream(tick, broker, path, scope, acc, now_seconds)
    )

    is_completed = any(indicates_exit(c) for c in acc.commands)
    if acc.step_no_longer_in_progress:
        _publish_step_transition(acc, tick, path)
        if this_execution in worker_state.in_progress:
            worker_state.in_progress.remove(this_execution)
    if not is_completed:
        acc.commands.extend(
            _drain_eligible_queue(step_id, worker_state, path, now_seconds)
        )
    return acc.commands, boundary_stop, None


def _publish_step_transition(
    acc: _StepResultAcc,
    tick: TickStepResult,
    path: tuple[str, ...],
) -> None:
    acc.commands.insert(
        0,
        CommandPublishEvent(
            StepStateChanged(
                step_state=StepState.NOT_RUNNING,
                name=_static_step_name(path, tick.step_id),
                input_event_name=str(type(tick.event)),
                output_event_name=acc.output_event_name,
                worker_id=str(tick.worker_id),
            ),
            origin_namespace=path,
        ),
    )


def _ascend_child_stop(
    parent_broker: BrokerState,
    parent_path: tuple[str, ...],
    slot_key: str,
    child: ChildBroker,
    stop_event: StopEvent,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Resolve a completed child at its parent boundary.

    Pops the child subtree, cancels its live tasks, consumes the boundary work
    item in the parent's enclosing stream, and re-injects the child's StopEvent
    into the parent's own routing carrying the boundary scope + recovery lineage
    (fixing the flat model's scope-dropping re-inject).
    """
    parent_broker.children.pop(slot_key, None)
    child_path = (*parent_path, slot_key)
    commands: list[WorkflowCommand] = [CommandCancelNamespace(namespace=child_path)]
    enclosing = child.boundary_scope_path[-1] if child.boundary_scope_path else None
    # The child was one work item in the enclosing stream. Its same-scope
    # successors are the parent-local routing of the stop event; count them the
    # way a completed step's emissions are counted.
    successors = _count_accepting_steps(parent_broker, stop_event)
    commands.extend(
        _adjust_open_work_items(
            parent_broker, enclosing, successors - 1, parent_path, now_seconds
        )
    )
    commands.append(
        CommandQueueEvent(
            event=stop_event,
            origin_namespace=parent_path,
            scope_path=child.boundary_scope_path,
            recovery_counts=dict(child.boundary_recovery_counts),
        )
    )
    return commands


def _ascend_boundary_failure(
    chain: list[tuple[BrokerState, str, ChildBroker]],
    failing_path: tuple[str, ...],
    failure: _BoundaryFailure,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Propagate an uncaught child failure/timeout up the parent chain.

    At each level the parent pops the failing child (one path-prefixed
    CommandCancelNamespace tears its subtree down exactly once) and routes a
    StepFailedEvent named for the child's path through the parent's OWN wildcard
    catch-error table with normal ``max_recoveries`` accounting:

    - Caught: the boundary work item lives on as the handler invocation
      (still-live — no stream adjustment, exactly like a retry redelivery), in
      ``boundary_scope_path``.
    - Uncaught: the parent itself becomes the failing boundary to ITS parent,
      recursing. Uncaught at the root fails the run — publishing the timeout or
      failure terminal event. ``CommandHalt`` stays reserved for root
      cancel/root timeout.

    ``chain[i]`` is ``(broker_at_depth_i, slot_key, child_record_at_depth_i+1)``;
    the failing broker starts at ``failing_path`` (depth ``len(chain)``).
    """
    commands: list[WorkflowCommand] = []
    origin_path = failing_path
    # The subtree to tear down grows as the failure ascends; a single
    # path-prefixed cancel at the level where it is caught (or at root) covers
    # every popped descendant, so emit exactly one cancel rather than one per
    # level (a grandchild cancel is redundant with its parent's).
    topmost_cancel = failing_path
    for i in range(len(chain) - 1, -1, -1):
        parent_broker, slot_key, child_record = chain[i]
        parent_path = failing_path[:i]  # the parent broker's own invocation path
        parent_broker.children.pop(slot_key, None)
        topmost_cancel = failing_path

        handler_step_id = _wildcard_catch_error_handler(parent_broker.config)
        handler = (
            parent_broker.config.catch_error_handlers.get(handler_step_id)
            if handler_step_id is not None
            else None
        )
        if handler is not None and handler_step_id is not None:
            recovery_counts = dict(child_record.boundary_recovery_counts)
            recovery_key = str(handler_step_id)
            new_count = recovery_counts.get(recovery_key, 0) + 1
            if new_count <= handler.max_recoveries:
                # Caught here: tear the failing child's subtree down exactly once.
                commands.append(CommandCancelNamespace(namespace=topmost_cancel))
                step_failed_event = StepFailedEvent(
                    step_name="/".join(slot_namespace(failing_path)),
                    input_event=failure.input_event,
                    exception=failure.exception,
                    attempts=failure.attempts,
                    elapsed_seconds=failure.elapsed_seconds,
                    failed_at=datetime.fromtimestamp(
                        failure.failed_at, tz=timezone.utc
                    ),
                )
                commands.append(
                    CommandQueueEvent(
                        event=step_failed_event,
                        step_id=handler_step_id,
                        origin_namespace=parent_path,
                        scope_path=child_record.boundary_scope_path,
                        recovery_counts={**recovery_counts, recovery_key: new_count},
                    )
                )
                return commands
        # Uncaught at this level: the parent becomes the failing boundary upward.
        failing_path = parent_path

    # Reached the root uncaught: tear the topmost failing child's subtree down
    # exactly once, then fail the whole run.
    commands.append(CommandCancelNamespace(namespace=topmost_cancel))
    root = chain[0][0]
    root.is_running = False
    if failure.timed_out_event is not None:
        commands.append(
            CommandPublishEvent(event=failure.timed_out_event, origin_namespace=())
        )
    else:
        commands.append(
            CommandPublishEvent(
                event=WorkflowFailedEvent(
                    step_name=failure.origin_step_name
                    or "/".join(slot_namespace(origin_path)),
                    exception=failure.exception,
                    attempts=failure.attempts,
                    elapsed_seconds=failure.elapsed_seconds,
                )
            )
        )
    commands.append(
        CommandFailWorkflow(
            step_id=StepId(origin_path, origin_path[-1] if origin_path else ""),
            exception=failure.exception,
        )
    )
    return commands


def _static_collect_events(
    event: Event,
    worker_state: InternalStepWorkerState,
) -> dict[str, Event] | None:
    """Buffer a statically routed fan-in event until every declared slot is filled."""
    collect_params = worker_state.config.collect_params
    if collect_params is None:
        return None

    static_collect_events = worker_state.static_collect_events
    static_collect_events.append(event)
    selected_indices = _select_static_collect_batch(
        events=static_collect_events,
        collect_params=collect_params,
        allow_subclasses=worker_state.config.accept_event_subclasses,
    )
    if selected_indices is None:
        return None

    binding = {
        param_name: static_collect_events[event_index]
        for (param_name, _), event_index in zip(collect_params, selected_indices)
    }
    selected = set(selected_indices)
    worker_state.static_collect_events = [
        pending
        for index, pending in enumerate(static_collect_events)
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
    path: tuple[str, ...],
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Add an event to a step worker, or enqueue it if no capacity.

    Note! Mutates the worker state, assuming an outer deepcopy. ``path`` is the
    owning broker's invocation path, stamped onto the dispatch commands so the
    runner registers the worker task under the right invocation.
    """
    commands: list[WorkflowCommand] = []
    has_space = len(state.in_progress) < state.config.num_workers and _is_eligible(
        event
    )
    if has_space:
        used = set(x.worker_id for x in state.in_progress)
        id_candidates = [i for i in range(state.config.num_workers) if i not in used]
        id = id_candidates[0]
        shared_state: StepWorkerState = StepWorkerState(
            step_name=step_id.name,
            collected_events={k: list(v) for k, v in state.collected_events.items()},
            collected_waiters=[replace(w) for w in state.collected_waiters],
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
                invocation_namespace=path,
                bound_events=event.bound_events,
            )
        )
        commands.append(
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.RUNNING,
                    name=_static_step_name(path, step_id),
                    input_event_name=type(event.event).__name__,
                    worker_id=str(id),
                ),
                origin_namespace=path,
            )
        )
    else:
        commands.append(
            CommandPublishEvent(
                StepStateChanged(
                    step_state=StepState.PREPARING,
                    name=_static_step_name(path, step_id),
                    input_event_name=type(event.event).__name__,
                    worker_id="<enqueued>",
                ),
                origin_namespace=path,
            )
        )
        state.queue.append(event)
    return commands


# --- Add-event routing (broker-local, with boundary descent) ------------------


@dataclass
class _RouteResult:
    """Outcome of routing an added event within one broker."""

    commands: list[WorkflowCommand]
    handled: bool = False
    failed: bool = False


def _redeliver_collection_payload(
    tick: TickAddEvent, broker: BrokerState, path: tuple[str, ...], now_seconds: float
) -> list[WorkflowCommand] | None:
    """Re-deliver a payload-carrying tick straight to its collect target."""
    if tick.collection_release_payload is None:
        return None
    payload = tick.collection_release_payload
    binding = broker.config.collection_bindings.get(payload.binding_id)
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
        binding.target_step,
        broker.workers[binding.target_step],
        path,
        now_seconds,
    )


def _resolve_waiters(
    tick: TickAddEvent, broker: BrokerState, path: tuple[str, ...], now_seconds: float
) -> tuple[list[WorkflowCommand], set[StepId]]:
    """Resolve any local waiters the event satisfies, resuming their work items."""
    commands: list[WorkflowCommand] = []
    waiter_resolved_steps: set[StepId] = set()
    for step_id, step_config in broker.config.steps.items():
        # A targeted tick only resolves its addressed step's waiters; an
        # untargeted tick is matched against every step's waiters below by
        # ``event_matches`` (per-waiter event type + requirements).
        if tick.step_id is not None and tick.step_id != step_id:
            continue
        wait_conditions = broker.workers[step_id].collected_waiters
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
                waiter_resolved_steps.add(step_id)
                wait_condition.resolved_event = tick.event
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
                        broker.workers[step_id],
                        path,
                        now_seconds,
                    )
                )
    return commands, waiter_resolved_steps


def _route_member_to_collect_step(
    tick: TickAddEvent,
    broker: BrokerState,
    path: tuple[str, ...],
    step_id: StepId,
    worker_state: InternalStepWorkerState,
    now_seconds: float,
) -> tuple[list[WorkflowCommand], bool]:
    """Route an accepted event into a collect step's batch buffer."""
    commands: list[WorkflowCommand] = []
    if not tick.scope_path:
        if tick.step_id is not None:
            error = WorkflowRuntimeError(
                f"{type(tick.event).__name__} was sent to collect "
                f"step {step_id.name!r} via send_event(step=...), but "
                "a collect step cannot receive targeted events: it "
                "only collects events emitted inside a fan-out "
                "stream."
            )
            broker.is_running = False
            commands.append(
                CommandPublishEvent(
                    event=WorkflowFailedEvent(
                        step_name=_static_step_name(path, step_id), exception=error
                    )
                )
            )
            commands.append(CommandFailWorkflow(step_id=step_id, exception=error))
            return commands, True
        logger.warning(
            "Ignoring %s for collect step %r: it was sent "
            "outside any collection stream (e.g. via "
            "ctx.send_event) so it cannot join a batch.",
            type(tick.event).__name__,
            step_id.name,
        )
        return commands, False
    stream_id = tick.scope_path[-1]
    binding = broker.config.binding_for_target(stream_id, step_id, broker.streams)
    if binding is None:
        logger.warning(
            "Dropping %s for collect step %r: its enclosing "
            "stream %r has no collection binding targeting that "
            "step, so it can never join a batch.",
            type(tick.event).__name__,
            step_id.name,
            stream_id,
        )
        commands.extend(
            _adjust_open_work_items(broker, stream_id, -1, path, now_seconds)
        )
        return commands, False
    release_state = _release_state_for(broker, stream_id, binding)
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
                    path,
                    now_seconds,
                )
            )
    commands.extend(_adjust_open_work_items(broker, stream_id, -1, path, now_seconds))
    return commands, False


def _route_to_accepting_steps(
    tick: TickAddEvent,
    broker: BrokerState,
    path: tuple[str, ...],
    waiter_resolved_steps: set[StepId],
    now_seconds: float,
) -> _RouteResult:
    """Route the event to accepting local steps and descend into child slots.

    Broker-local: a non-targeted event routes to every local step that accepts
    it, and (unless targeted) descends into every child slot whose class accepts
    it as a StartEvent. Steps already woken via waiter resolution are skipped;
    their stream accounting is balanced for the delivery the waiter swallowed.
    """
    result = _RouteResult(commands=[])
    for step_id, step_config in broker.config.steps.items():
        is_accepted = step_accepts_event(
            tick.event,
            step_config.accepted_events,
            allow_subclasses=step_config.accept_event_subclasses,
        )
        routes = (tick.step_id == step_id) if tick.step_id is not None else is_accepted
        if step_id in waiter_resolved_steps:
            if is_accepted and routes and tick.scope_path:
                result.commands.extend(
                    _adjust_open_work_items(
                        broker, tick.scope_path[-1], -1, path, now_seconds
                    )
                )
            continue
        if not (is_accepted and routes):
            continue
        result.handled = True
        worker_state = broker.workers[step_id]
        if worker_state.config.collection_param is not None:
            member_commands, failed = _route_member_to_collect_step(
                tick, broker, path, step_id, worker_state, now_seconds
            )
            result.commands.extend(member_commands)
            if failed:
                result.failed = True
                return result
            continue
        if tick.step_id is not None:
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
                            broker, tick.scope_path[-1], -1, path, now_seconds
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
                worker_state,
                path,
                now_seconds,
            )
        )

    # Boundary descent: targeted sends never descend (targeted delivery wins).
    if tick.step_id is None:
        for slot in sorted(_child_slots_accepting(broker.config, tick.event)):
            result.handled = True
            result.commands.extend(
                _boundary_descend(broker, path, slot, tick, now_seconds)
            )
    return result


def _boundary_descend(
    parent_broker: BrokerState,
    path: tuple[str, ...],
    slot: str,
    tick: TickAddEvent,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Create a fresh child invocation and deliver the StartEvent into it.

    The child is one work item in the parent's enclosing stream (already
    birth-counted at the emitting step): the parent stream is untouched here.
    The child's boundary record is stamped from the delivering event; the child
    runs on a fresh empty scope and a fresh lineage.
    """
    child_config = parent_broker.config.child_config_for(slot)
    if child_config is None:  # pragma: no cover - guarded by _child_slots_accepting
        return []
    seq = parent_broker.child_seq.get(slot, 0)
    parent_broker.child_seq[slot] = seq + 1
    key = mint_slot_segment(slot, seq)
    child_path = (*path, key)
    child_state = BrokerState.from_config(child_config)
    parent_broker.children[key] = ChildBroker(
        slot=slot,
        state=child_state,
        boundary_scope_path=tuple(tick.scope_path),
        boundary_recovery_counts=dict(tick.recovery_counts),
    )
    child_tick = tick.model_copy(
        update={
            "origin_namespace": child_path,
            "scope_path": (),
            "step_id": None,
            "work_item_id": None,
            "recovery_counts": {},
            "bound_events": None,
        }
    )
    commands = _local_add_event(child_state, child_path, child_tick, now_seconds)
    commands.extend(_arm_child_deadline(child_state, child_path, now_seconds))
    return commands


def _unhandled_event_commands(
    tick: TickAddEvent, state: BrokerState, path: tuple[str, ...]
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
                step_name=_static_step_name(path, tick.step_id)
                if tick.step_id is not None
                else None,
                idle=_check_idle_state(state),
            ),
            origin_namespace=path,
        )
    ]


def _local_add_event(
    broker: BrokerState,
    path: tuple[str, ...],
    tick: TickAddEvent,
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Route an added event within its addressed broker.

    Mints a work-item id from this broker's counter, re-delivers a collect
    payload straight to its target, else resolves local waiters then routes to
    accepting local steps and descends into accepting child slots.
    """
    if tick.work_item_id is None:
        work_item_id = (
            tick.collection_release_payload.work_item_id()
            if tick.collection_release_payload is not None
            else _next_work_item_id(broker)
        )
        tick = tick.model_copy(update={"work_item_id": work_item_id})
    if isinstance(tick.event, StartEvent):
        broker.is_running = True

    payload_commands = _redeliver_collection_payload(tick, broker, path, now_seconds)
    if payload_commands is not None:
        return payload_commands

    commands, waiter_resolved_steps = _resolve_waiters(tick, broker, path, now_seconds)
    routed = _route_to_accepting_steps(
        tick, broker, path, waiter_resolved_steps, now_seconds
    )
    commands.extend(routed.commands)
    if routed.failed:
        return commands

    handled = bool(waiter_resolved_steps) or routed.handled
    if not handled:
        commands.extend(_unhandled_event_commands(tick, broker, path))

    return commands


def _arm_child_deadline(
    broker: BrokerState,
    path: tuple[str, ...],
    now_seconds: float,
) -> list[WorkflowCommand]:
    """Arm a fresh child broker's elapsed-alive deadline at birth.

    Called once at boundary descent (and again, freshly, when the child catches
    its own timeout and restarts). The child starts with an empty budget, so the
    deadline fires ``timeout`` seconds of alive time from now; the reducer
    confirms the budget on fire. The root broker's deadline is the runner's
    global ``TickTimeout``, never a namespace timeout.
    """
    timeout = broker.config.timeout
    if timeout is None:
        return []
    broker.elapsed_alive = 0.0
    broker.last_alive_stamp = now_seconds
    return [
        CommandScheduleNamespaceTimeout(
            namespace=path,
            timeout=timeout,
            at_time=now_seconds + timeout,
        )
    ]


def _dispatch_add_event(
    tick: TickAddEvent, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Descend to the addressed broker and route the event there.

    A send addressed to an absent (dead) invocation publishes a loud
    UnhandledEvent instead of silently re-entering a torn-down namespace.
    """
    state = init.deepcopy()
    path = tick.origin_namespace
    descent = _descend(state, path)
    if descent is None:
        if _is_static_child_path(path):
            # A static child path (e.g. "child/answer") names a slot, not one of
            # its possibly-overlapping concrete invocations. Reject loudly rather
            # than silently dropping — the caller must use "child#N/answer".
            error = WorkflowRuntimeError(
                f"Send addressed to static child path {'/'.join(path)!r} is "
                "ambiguous: it has no concrete child invocation. Use the "
                "concrete child invocation namespace from the event stream, "
                "e.g. 'child#0/step'."
            )
            state.is_running = False
            return state, [
                CommandPublishEvent(
                    event=WorkflowFailedEvent(step_name="/".join(path), exception=error)
                ),
                CommandFailWorkflow(step_id=StepId(path, path[-1]), exception=error),
            ]
        # Concrete-but-dead invocation: loud UnhandledEvent (orphan burn-off).
        return state, _unhandled_event_commands(tick, state, path)
    _accrue_descent(descent, _tick_stamp(tick))
    commands = _local_add_event(descent.broker, path, tick, now_seconds)
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


def _dispatch_waiter_timeout(
    tick: TickWaiterTimeout, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    state = init.deepcopy()
    descent = _descend(state, tick.invocation_namespace)
    if descent is None:
        return state, []  # broker popped: burn-off no-op
    _accrue_descent(descent, _tick_stamp(tick))
    broker = descent.broker
    path = tick.invocation_namespace
    commands: list[WorkflowCommand] = []
    if tick.step_id not in broker.workers:
        return state, commands
    worker_state = broker.workers[tick.step_id]
    waiter = next(
        (w for w in worker_state.collected_waiters if w.waiter_id == tick.waiter_id),
        None,
    )
    if waiter is None or waiter.resolved_event is not None:
        return state, commands
    waiter.timed_out = True
    commands.extend(
        _add_or_enqueue_event(
            EventAttempt(
                event=waiter.event,
                bound_events=waiter.bound_events,
                scope_path=waiter.scope_path,
                collection_release_payload=waiter.collection_release_payload,
                work_item_id=waiter.work_item_id,
            ),
            tick.step_id,
            worker_state,
            path,
            now_seconds,
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
    stamp = _tick_stamp(tick)
    if stamp is not None:
        _accrue_alive(state, stamp)
        if state.elapsed_alive + 1e-9 < tick.timeout:
            remaining = tick.timeout - state.elapsed_alive
            base = max(stamp, state.last_alive_stamp or stamp)
            return state, [
                CommandScheduleTimeout(
                    timeout=tick.timeout,
                    at_time=base + remaining,
                )
            ]
    state.is_running = False
    _clear_collection_state(state)
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


def _process_wakeup_tick(
    tick: TickWakeup, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Flip due delayed attempts to eligible across the tree, then dispatch.

    Deterministic sorted recursive walk so replay makes the same decisions.
    """
    state = init.deepcopy()
    stamp = _tick_stamp(tick)
    if stamp is not None:
        _accrue_alive(state, stamp)
    commands: list[WorkflowCommand] = []

    def walk(broker: BrokerState, path: tuple[str, ...]) -> None:
        for step_id, worker_state in sorted(
            broker.workers.items(), key=lambda x: str(x[0])
        ):
            for attempt in worker_state.queue:
                if attempt.not_before is not None and attempt.not_before <= tick.due:
                    attempt.not_before = None
            commands.extend(
                _drain_eligible_queue(step_id, worker_state, path, now_seconds)
            )
        for key, child in sorted(broker.children.items()):
            walk(child.state, (*path, key))

    walk(state, ())
    return state, commands


def _wildcard_catch_error_handler(config: BrokerConfig) -> StepId | None:
    """The broker's wildcard ``@catch_error`` handler (``for_steps is None``).

    A child's own timeout and an ascending boundary failure are not attributable
    to a specific in-broker step, so only a wildcard handler catches them; a
    step-scoped handler does not.
    """
    for handler_id, handler in config.catch_error_handlers.items():
        if handler.for_steps is None:
            return handler_id
    return None


def _process_namespace_timeout_tick(
    tick: TickNamespaceTimeout, init: BrokerState, now_seconds: float
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Check a child invocation's elapsed-alive budget on its deadline fire.

    A no-op if the child already completed (popped). Otherwise the child's alive
    time is accrued from the tick's stamp: if the budget is not yet truly spent
    (downtime forgiven, or a superseded re-armed deadline) it re-arms for the
    remaining budget; if spent it expires the child — its own wildcard
    ``@catch_error`` first (with a fresh budget), else a boundary failure ascends
    to the parent (an uncaught child timeout is never ``CommandHalt``, which is
    root-only).
    """
    state = init.deepcopy()
    namespace = tick.namespace
    descent = _descend(state, namespace)
    if descent is None:
        return state, []  # already popped: burn-off
    _accrue_descent(descent, _tick_stamp(tick))
    child_broker = descent.broker
    timeout = child_broker.config.timeout
    if timeout is None:
        return state, []  # no deadline configured (should not happen)
    if child_broker.elapsed_alive + _ALIVE_EPS < timeout:
        # Budget not truly spent: this fire was premature (downtime forgiven, or
        # a stale deadline left by a fresh re-arm). Re-arm for the remainder.
        remaining = timeout - child_broker.elapsed_alive
        return state, [
            CommandScheduleNamespaceTimeout(
                namespace=namespace,
                timeout=timeout,
                at_time=now_seconds + remaining,
            )
        ]
    return _expire_child_broker(state, descent, timeout, now_seconds)


def _expire_child_broker(
    state: BrokerState,
    descent: _Descent,
    timeout: float,
    now_seconds: float,
) -> tuple[BrokerState, list[WorkflowCommand]]:
    """Expire a child whose alive-time budget is spent: local catch, else ascend."""
    child_broker = descent.broker
    namespace = descent.path
    input_event: Event | None = None
    recovery_counts: dict[str, int] = {}
    active_steps: list[str] = []
    static_ns = slot_namespace(namespace)
    for step_id, worker in child_broker.workers.items():
        if worker.in_progress or worker.queue:
            active_steps.append("/".join((*static_ns, step_id.name)))
        if input_event is None:
            if worker.in_progress:
                input_event = worker.in_progress[0].event
                recovery_counts = dict(worker.in_progress[0].recovery_counts)
            elif worker.queue:
                input_event = worker.queue[0].event
                recovery_counts = dict(worker.queue[0].recovery_counts)
            elif worker.collected_waiters:
                input_event = worker.collected_waiters[0].event

    timeout_error = WorkflowTimeoutError(
        f"Child workflow '{'/'.join(static_ns)}' timed out after {timeout} seconds."
    )

    # The child's OWN wildcard @catch_error recovers it locally with a fresh
    # budget (never ascends). This mirrors an in-broker step's local catch.
    handler_step_id = _wildcard_catch_error_handler(child_broker.config)
    handler = (
        child_broker.config.catch_error_handlers.get(handler_step_id)
        if handler_step_id is not None
        else None
    )
    if handler is not None and handler_step_id is not None:
        recovery_key = str(handler_step_id)
        new_count = recovery_counts.get(recovery_key, 0) + 1
        if new_count <= handler.max_recoveries:
            _terminate_broker(child_broker)
            step_failed_event = StepFailedEvent(
                step_name="/".join(static_ns),
                input_event=input_event if input_event is not None else Event(),
                exception=timeout_error,
                attempts=1,
                elapsed_seconds=timeout,
                failed_at=datetime.fromtimestamp(now_seconds, tz=timezone.utc),
            )
            return state, [
                CommandCancelNamespace(namespace=namespace),
                CommandQueueEvent(
                    event=step_failed_event,
                    step_id=handler_step_id,
                    origin_namespace=namespace,
                    recovery_counts={**recovery_counts, recovery_key: new_count},
                ),
                # Fresh alive budget for the recovery attempt, armed from now.
                *_arm_child_deadline(child_broker, namespace, now_seconds),
            ]

    # Uncaught locally: ascend the timeout to the parent as a boundary failure.
    failure = _BoundaryFailure(
        exception=timeout_error,
        input_event=input_event if input_event is not None else Event(),
        attempts=1,
        elapsed_seconds=timeout,
        failed_at=now_seconds,
        origin_step_name="/".join(static_ns),
        timed_out_event=WorkflowTimedOutEvent(
            timeout=timeout, active_steps=active_steps
        ),
    )
    return state, _ascend_boundary_failure(
        descent.chain, namespace, failure, now_seconds
    )
