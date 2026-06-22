# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import hashlib
import logging
from enum import Enum

from workflows._event_matching import (
    step_accepts_type,
)
from workflows.collect import Collect, Take
from workflows.errors import (
    WorkflowRuntimeError,
)
from workflows.events import (
    Event,
)
from workflows.runtime.types.commands import (
    WorkflowCommand,
    indicates_exit,
)
from workflows.runtime.types.internal_state import (
    BrokerState,
    CollectionBinding,
    CollectionReleaseState,
    EventAttempt,
    InternalStepWorkerState,
)
from workflows.runtime.types.results import (
    AddCollectedEvent,
    AddWaiter,
    CollectionReleasePayload,
    StepWorkerFailed,
    StepWorkerResult,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickStepResult,
)

logger = logging.getLogger("workflows.runtime.control_loop")


def _detect_stuck_streams(
    state: BrokerState,
) -> tuple[str, WorkflowRuntimeError] | None:
    """Detect a provably-stuck run while the state is quiescent.

    Two conditions, returned as ``(step_name, error)``:

    - An unreleased release-state whose stream no longer exists. The close
      path fires releases inline within the same reduce, so this should be
      impossible; if it ever appears (corrupted or version-skewed persisted
      state), the release can never fire — fail loudly instead of hanging.
    - Open streams with no unresolved waiter *inside any of them*. A pending
      in-stream waiter represents scoped work that can still resume and close
      the stream. Without runnable work or such a waiter, an open stream can
      never reach zero open work items: the run would hang to timeout (or
      forever).
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
    has_in_stream_waiter = any(
        waiter.resolved_event is None
        and not waiter.timed_out
        and any(sid in state.streams for sid in waiter.scope_path)
        for worker_state in state.workers.values()
        for waiter in worker_state.collected_waiters
    )
    if has_in_stream_waiter:
        return None
    first_leaked = next(iter(state.streams.values()))
    details = "; ".join(
        f"stream {stream.stream_id!r} opened by step {stream.source_step!r} "
        f"with {stream.open_work_items} open work item(s)"
        for stream in state.streams.values()
    )
    return first_leaked.source_step, WorkflowRuntimeError(
        "Workflow is idle but collection streams are still open, so the run "
        f"can never complete: {details}. No queued, running, or resumable "
        "scoped work remains that can close the stream. This indicates "
        "corrupted persisted state, incompatible workflow/runtime changes "
        "since the run was snapshotted, or a runtime stream-accounting bug."
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
    open_work_items set: a single emitted event accepted by N steps is N work
    items. Must mirror the routing predicate in ``_process_add_event_tick``
    exactly (including subclass-aware acceptance) — a birth count that differs
    from the delivery count drifts the stream counter.
    """
    return sum(
        1
        for cfg in state.config.steps.values()
        if step_accepts_type(
            event_type,
            cfg.accepted_events,
            allow_subclasses=cfg.accept_event_subclasses,
        )
    )


def _adjust_open_work_items(
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
    if stream.open_work_items <= 0:
        return _close_collection_stream(state, stream_id, now_seconds)
    return []


def _close_collection_stream(
    state: BrokerState, stream_id: str, now_seconds: float
) -> list[WorkflowCommand]:
    """Close a zero-count stream and release any buffered collection batches."""
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


def _take_threshold(collect: Collect | None) -> int | None:
    if collect is None:
        return None
    card = collect.cardinality
    if isinstance(card, Take):
        return card.n
    return None


class WorkDisposition(Enum):
    """What happened to a work item when its execution finished.

    Stream accounting hinges on answering this exactly once per finished
    execution: COMPLETED and ABSORBED consume the item (adjusting the
    enclosing stream's open-work counter), FANNED_OUT consumes it into a
    child stream, STILL_LIVE and RUN_ENDING leave the counter untouched.
    """

    # Consumed; same-scope successors (one per accepting step per emitted
    # event) replace it in the enclosing stream.
    COMPLETED = "completed"
    # Consumed; the execution returned a list and opened a child stream.
    FANNED_OUT = "fanned_out"
    # Not consumed: the same work item re-delivers later (collect-buffer
    # rerun, scheduled retry, catch_error handler routing, or a waiter
    # suspension that resumes it whole).
    STILL_LIVE = "still_live"
    # Consumed; the invocation only buffered its trigger into a multi-slot
    # join (or silently dropped a duplicate arrival) and emitted nothing.
    ABSORBED = "absorbed"
    # The run is over (StopEvent, halt, or workflow failure); accounting moot.
    RUN_ENDING = "run_ending"


def _classify_work_item(
    tick: TickStepResult,
    commands: list[WorkflowCommand],
    *,
    rerun_scheduled: bool,
    redelivery_scheduled: bool,
    fanned_out: bool,
) -> WorkDisposition:
    """Classify a finished execution's work item, positively, in one place.

    Every case matches on what *did* happen (results returned, commands
    emitted). An unrecognized combination raises instead of falling into a
    residual bucket — a silently misclassified work item drifts the stream
    counter and wedges or prematurely closes the stream.
    """
    did_complete_step = any(isinstance(x, StepWorkerResult) for x in tick.result)
    step_failed = any(isinstance(x, StepWorkerFailed) for x in tick.result)
    added_waiter = any(isinstance(x, AddWaiter) for x in tick.result)
    step_name = str(tick.step_id)

    if any(indicates_exit(c) for c in commands):
        return WorkDisposition.RUN_ENDING
    if rerun_scheduled:
        # The same invocation reruns against a fresh collect-buffer snapshot;
        # only its final completion may consume the work item.
        return WorkDisposition.STILL_LIVE
    if step_failed and redelivery_scheduled:
        # Retry attempts may live directly in BrokerState, so the reducer tells
        # us explicitly when the failed work item was re-delivered.
        return WorkDisposition.STILL_LIVE
    if did_complete_step:
        return WorkDisposition.FANNED_OUT if fanned_out else WorkDisposition.COMPLETED
    if added_waiter:
        return WorkDisposition.STILL_LIVE
    if all(isinstance(x, AddCollectedEvent) for x in tick.result):
        # Only buffer writes (or no recorded results at all — a duplicate
        # arrival for an already-filled slot): the invocation is over and
        # emitted nothing.
        return WorkDisposition.ABSORBED
    raise WorkflowRuntimeError(
        f"Cannot classify finished execution of step {step_name!r} for "
        f"stream accounting: results={[type(r).__name__ for r in tick.result]}. "
        "This is a runtime bug."
    )


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
    return list(release_state.buffer[:threshold])


def _release_on_close(
    binding: CollectionBinding, release_state: CollectionReleaseState
) -> list[Event] | None:
    if release_state.released:
        return None
    release_state.released = True
    threshold = _take_threshold(binding.policy)
    if threshold is not None:
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
    # Inline import breaks the reduce<->streams cycle: _add_or_enqueue_event is
    # the shared dispatch primitive owned by reduce.py, which imports this module
    # at top level. This release-firing path is the only streams->reduce edge.
    from workflows.runtime.control_loop.reduce import _add_or_enqueue_event

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
            work_item_id=payload.work_item_id(),
        ),
        StepId.root(binding.target_step),
        worker_state,
        now_seconds,
    )
