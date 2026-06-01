# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import dataclasses
import importlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from workflows.context.context_types import (
    CURRENT_SERIALIZED_VERSION,
    SerializedBatch,
    SerializedContext,
    SerializedEventAttempt,
    SerializedStepWorkerState,
    SerializedWaiter,
)
from workflows.context.serializers import JsonSerializer
from workflows.decorators import CatchErrorHandler, StepConfig
from workflows.events import Event
from workflows.retry_policy import RetryPolicy
from workflows.runtime.types.results import StepWorkerState, StepWorkerWaiter
from workflows.runtime.types.ticks import TickAddEvent, WorkflowTick
from workflows.workflow import Workflow

if TYPE_CHECKING:
    from workflows.context.context_types import SerializedContext
    from workflows.context.serializers import BaseSerializer


@dataclass()
class Batch:
    """An open fan-out batch, modeled as an explicit live set of work items.

    A **work item** is one ``(event, step)`` pair: an event routed to a step that
    accepts it at this batch's level. It is the unit of accounting — an event
    accepted by N steps is N work items, not one. ``live`` is the cardinality of
    the live set ``L(B)``; the batch closes exactly when it reaches 0.

    The live set is seeded at open (one work item per emitted member per
    accepting step), grows when a resolving work item emits same-level
    successors, and shrinks when a work item resolves (a step completes, a member
    is delivered to a collect, or a child batch's collect summary lands). A
    fan-out work item is replaced by a placeholder (one per bound collect of the
    child batch) that resolves when the child's collect fires — so nesting is
    represented, not counted.

    Attributes:
        batch_id: Stable, run-id-free id (deterministic on replay).
        producer: The fan-out step that opened the batch.
        origin_stack: The producer's trigger stack — the stack the batch id was
            pushed onto. A collect step's outputs inherit this (closed id popped).
        bound_collects: Collect step names statically bound to this batch level
            (their element type is produced at this level). Fired once on close,
            even with an empty buffer. Computed at build, never via a runtime walk.
        live: Cardinality of the live set ``L(B)``. Close when it hits 0.
    """

    batch_id: str
    producer: str
    origin_stack: tuple[str, ...]
    bound_collects: tuple[str, ...]
    live: int = 0

    def _copy(self) -> Batch:
        return dataclasses.replace(self)


@dataclass()
class BrokerState:
    """
    Complete state of the workflow broker at a given point in time.

    This is the primary state object passed through the control loop's reducer pattern.
    Each tick processes this state and returns an updated copy along with commands to execute.

    Attributes:
        config: Immutable configuration for the workflow and all steps
        workers: Mutable state for each step's worker pool, queues, and in-progress executions
    """

    is_running: bool
    config: BrokerConfig
    workers: dict[str, InternalStepWorkerState]
    # Monotonic counter used to mint deterministic batch ids. Incremented in the
    # reducer when a fan-out batch is opened. Because the reducer is pure and
    # replayed tick-by-tick, the same fan-out reproduces the same counter value,
    # hence the same batch id, on replay. The id derivation omits run_id so a
    # replay (run_id=None) and the live run produce identical ids.
    batch_seq: int = 0
    # Open fan-out batches keyed by batch id. A ``Batch`` models its lifecycle as
    # an explicit live set of work items (see ``Batch.live``). A batch closes
    # exactly when its live set empties. A live set rather than a scalar pending
    # counter: a counter seeded by member count and decremented per delivery and
    # per branch suffers a unit mismatch that silently truncates batches and hangs
    # the run.
    batches: dict[str, Batch] = field(default_factory=dict)

    def deepcopy(self) -> BrokerState:
        """
        Deep-ish copy. Copies fields that are considered mutable during updates.
        """
        return BrokerState(
            is_running=self.is_running,
            config=self.config,  # immutable
            workers={
                name: worker_state._deepcopy()
                for name, worker_state in self.workers.items()
            },
            batch_seq=self.batch_seq,
            batches={bid: b._copy() for bid, b in self.batches.items()},
        )

    @staticmethod
    def from_workflow(workflow: Workflow) -> BrokerState:
        return BrokerState(
            is_running=False,
            config=BrokerConfig(
                steps={
                    name: InternalStepConfig(
                        accepted_events=step_func._step_config.accepted_events,
                        retry_policy=step_func._step_config.retry_policy,
                        num_workers=step_func._step_config.num_workers,
                    )
                    for name, step_func in workflow._get_steps().items()
                },
                timeout=workflow._timeout,
                catch_error_handlers=dict(workflow._catch_error_handlers),
                handler_for_step=dict(workflow._handler_for_step),
                fan_in_bindings=_compute_fan_in_bindings(workflow),
            ),
            workers={
                name: InternalStepWorkerState(
                    queue=[],
                    config=step_func._step_config,
                    in_progress=[],
                    collected_events={},
                    collected_waiters=[],
                    batch_buffers={},
                )
                for name, step_func in workflow._get_steps().items()
            },
        )

    def rehydrate_with_ticks(self) -> list[WorkflowTick]:
        """
        Rehydrates non-serializable state by re-running commands
        """
        commands: list[WorkflowTick] = []
        for step_name, worker_state in sorted(self.workers.items(), key=lambda x: x[0]):
            for waiter in sorted(
                worker_state.collected_waiters, key=lambda x: x.waiter_id
            ):
                if waiter.has_requirements and not waiter.requirements:
                    commands.append(
                        TickAddEvent(event=waiter.event, step_name=step_name)
                    )
        return commands

    def to_serialized(self, serializer: BaseSerializer) -> SerializedContext:
        """Serialize the broker state to a SerializedContext."""

        workers_dict = {}
        for step_name, worker_state in self.workers.items():
            # Serialize queue with retry info
            queue = [
                SerializedEventAttempt(
                    event=serializer.serialize(attempt.event),
                    attempts=attempt.attempts or 0,
                    first_attempt_at=attempt.first_attempt_at,
                    last_exception=attempt.last_exception,
                    last_failed_at=attempt.last_failed_at,
                    recovery_counts=dict(attempt.recovery_counts),
                    batch_stack=list(attempt.batch_stack),
                    batch_input=[serializer.serialize(ev) for ev in attempt.batch_input]
                    if attempt.batch_input is not None
                    else None,
                )
                for attempt in worker_state.queue
            ]

            # Serialize in-progress executions with full retry + batch lineage, so
            # they re-queue on resume WITHOUT losing their batch_stack (the work
            # item's identity) — a member restored without it can't close its batch.
            in_progress = [
                SerializedEventAttempt(
                    event=serializer.serialize(ip.event),
                    attempts=ip.attempts or 0,
                    first_attempt_at=ip.first_attempt_at,
                    last_exception=ip.last_exception,
                    last_failed_at=ip.last_failed_at,
                    recovery_counts=dict(ip.recovery_counts),
                    batch_stack=list(ip.batch_stack),
                    batch_input=[
                        serializer.serialize(ev)
                        for ev in (ip.shared_state.batch_input or [])
                    ]
                    if ip.shared_state.batch_input is not None
                    else None,
                )
                for ip in worker_state.in_progress
            ]

            # Serialize collected events
            collected_events = {
                buffer_id: [serializer.serialize(ev) for ev in events]
                for buffer_id, events in worker_state.collected_events.items()
            }

            # Serialize batch-lineage fan-in buffers
            batch_buffers = {
                batch_id: [serializer.serialize(ev) for ev in events]
                for batch_id, events in worker_state.batch_buffers.items()
            }

            # Serialize waiters
            waiters = [
                SerializedWaiter(
                    waiter_id=waiter.waiter_id,
                    event=serializer.serialize(waiter.event),
                    waiting_for_event=f"{waiter.waiting_for_event.__module__}.{waiter.waiting_for_event.__name__}",
                    has_requirements=bool(len(waiter.requirements))
                    or waiter.has_requirements,
                    resolved_event=serializer.serialize(waiter.resolved_event)
                    if waiter.resolved_event
                    else None,
                )
                for waiter in worker_state.collected_waiters
            ]

            workers_dict[step_name] = SerializedStepWorkerState(
                queue=queue,
                in_progress=in_progress,
                collected_events=collected_events,
                collected_waiters=waiters,
                batch_buffers=batch_buffers,
                batch_fired=sorted(worker_state.batch_fired),
            )

        return SerializedContext(
            version=CURRENT_SERIALIZED_VERSION,
            state={},  # State is filled separately by the state store
            is_running=self.is_running,
            workers=workers_dict,
            batch_seq=self.batch_seq,
            batches={
                bid: SerializedBatch(
                    batch_id=b.batch_id,
                    producer=b.producer,
                    origin_stack=list(b.origin_stack),
                    bound_collects=list(b.bound_collects),
                    live=b.live,
                )
                for bid, b in self.batches.items()
            },
        )

    @staticmethod
    def from_serialized(
        serialized: SerializedContext,
        workflow: Workflow,
        serializer: BaseSerializer,
    ) -> BrokerState:
        """Deserialize a SerializedContext into a BrokerState."""

        serializer = serializer or JsonSerializer()

        # Start with a base state from the workflow
        base_state = BrokerState.from_workflow(workflow)
        # Unfortunately, important to preserve this state, since the workflow needs to know this to decide
        # whether to create a start_event from kwargs (it only constructs and passes a start event if not already running)
        base_state.is_running = serialized.is_running
        base_state.batch_seq = serialized.batch_seq
        base_state.batches = {
            bid: Batch(
                batch_id=b.batch_id,
                producer=b.producer,
                origin_stack=tuple(b.origin_stack),
                bound_collects=tuple(b.bound_collects),
                live=b.live,
            )
            for bid, b in serialized.batches.items()
        }

        # Restore worker state (queues, collected events, waiters)
        # We do this regardless of is_running state so workflows can resume from where they left off
        for step_name, worker_data in serialized.workers.items():
            if step_name not in base_state.workers:
                continue

            worker = base_state.workers[step_name]

            # Restore queue with retry info
            worker.queue = [
                EventAttempt(
                    event=serializer.deserialize(attempt.event),
                    attempts=attempt.attempts,
                    first_attempt_at=attempt.first_attempt_at,
                    last_exception=attempt.last_exception,
                    last_failed_at=attempt.last_failed_at,
                    recovery_counts=dict(attempt.recovery_counts),
                    batch_stack=tuple(attempt.batch_stack),
                    batch_input=[
                        serializer.deserialize(ev) for ev in attempt.batch_input
                    ]
                    if attempt.batch_input is not None
                    else None,
                )
                for attempt in worker_data.queue
            ]

            # in_progress executions are moved to the queue on deserialization and
            # restarted when the workflow runs, preserving their batch_stack so the
            # restored work item still closes its batch.
            for attempt in worker_data.in_progress:
                worker.queue.append(
                    EventAttempt(
                        event=serializer.deserialize(attempt.event),
                        attempts=attempt.attempts,
                        first_attempt_at=attempt.first_attempt_at,
                        last_exception=attempt.last_exception,
                        last_failed_at=attempt.last_failed_at,
                        recovery_counts=dict(attempt.recovery_counts),
                        batch_stack=tuple(attempt.batch_stack),
                        batch_input=[
                            serializer.deserialize(ev) for ev in attempt.batch_input
                        ]
                        if attempt.batch_input is not None
                        else None,
                    )
                )

            # Restore collected events
            worker.collected_events = {
                buffer_id: [serializer.deserialize(ev) for ev in events]
                for buffer_id, events in worker_data.collected_events.items()
            }

            # Restore batch-lineage fan-in buffers
            worker.batch_buffers = {
                batch_id: [serializer.deserialize(ev) for ev in events]
                for batch_id, events in worker_data.batch_buffers.items()
            }
            worker.batch_fired = set(worker_data.batch_fired)

            # Restore waiters
            worker.collected_waiters = []
            for waiter_data in worker_data.collected_waiters:
                # Import the event type
                waiting_for_event = _import_event_type(waiter_data.waiting_for_event)

                worker.collected_waiters.append(
                    StepWorkerWaiter(
                        waiter_id=waiter_data.waiter_id,
                        event=serializer.deserialize(waiter_data.event),
                        waiting_for_event=waiting_for_event,
                        requirements={},
                        has_requirements=waiter_data.has_requirements,
                        resolved_event=serializer.deserialize(
                            waiter_data.resolved_event
                        )
                        if waiter_data.resolved_event
                        else None,
                    )
                )

        return base_state


def _import_event_type(qualified_name: str) -> type[Event]:
    """Import an event type from a fully qualified name like 'mymodule.MyEvent'."""
    parts = qualified_name.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid qualified name: {qualified_name}")

    module_name, class_name = parts

    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _event_types(types: Any) -> list[type[Event]]:
    return [t for t in types if isinstance(t, type) and issubclass(t, Event)]


def _compute_fan_in_bindings(workflow: Workflow) -> dict[str, tuple[str, ...]]:
    """Statically bind each fan-out producer to the collect steps at its level.

    A collect step is bound to the batch a producer opens when the collect's
    element type is *produced at that level* — reachable from the producer
    without crossing into a deeper batch, plus any summary types a nested child
    batch's own collects emit back up into this level. This is the closure
    binding: when a batch closes, exactly these collects fire (once each, even
    on an empty buffer), with no runtime graph walk.
    """
    steps = {name: fn._step_config for name, fn in workflow._get_steps().items()}
    collects: dict[str, tuple[Any, ...]] = {
        name: cfg.batch_collect_param[1]
        for name, cfg in steps.items()
        if cfg.batch_collect_param is not None
    }

    def same_level_types(seed_types: Any, guard: frozenset[str]) -> set[type[Event]]:
        seen: set[type[Event]] = set()
        frontier: list[type[Event]] = list(_event_types(seed_types))
        while frontier:
            t = frontier.pop()
            if t in seen:
                continue
            seen.add(t)
            for name, cfg in steps.items():
                if t not in cfg.accepted_events:
                    continue
                if cfg.batch_collect_param is not None:
                    # A collect consumes at this level but emits at the parent
                    # level — a boundary. Do not traverse its outputs here.
                    continue
                if cfg.is_fan_out:
                    # Opens a child batch. The child's bound collects emit their
                    # summaries back into THIS level, so fold those summary types
                    # into the frontier (but not the fan-out's own outputs, which
                    # live one level deeper).
                    if name in guard:
                        continue
                    child = same_level_types(cfg.return_types, guard | {name})
                    for cname, cetypes in collects.items():
                        if any(et in child for et in cetypes):
                            frontier.extend(_event_types(steps[cname].return_types))
                    continue
                frontier.extend(_event_types(cfg.return_types))
        return seen

    bindings: dict[str, tuple[str, ...]] = {}
    for name, cfg in steps.items():
        if not cfg.is_fan_out:
            continue
        level_types = same_level_types(cfg.return_types, frozenset({name}))
        bindings[name] = tuple(
            cname
            for cname, cetypes in collects.items()
            if any(et in level_types for et in cetypes)
        )
    return bindings


@dataclass(frozen=True)
class BrokerConfig:
    """
    configuration for a workflow run.

    This contains all the static configuration that doesn't change during workflow execution.

    Attributes:
        steps: Configuration for each step indexed by step name
        timeout: Maximum seconds before the workflow times out, or None for no timeout
        catch_error_handlers: handler step name -> CatchErrorHandler descriptor
        handler_for_step: step name -> handler step name that owns it
    """

    steps: dict[str, InternalStepConfig]
    timeout: float | None
    catch_error_handlers: dict[str, CatchErrorHandler] = field(default_factory=dict)
    handler_for_step: dict[str, str] = field(default_factory=dict)
    # Static fan-in binding: fan-out producer step name -> the collect step
    # names bound to the batch level it opens. A collect is bound when its element
    # type is produced at that level (reachable from the producer without crossing
    # into a deeper batch, including types re-entering from nested child collects).
    # Computed once at build so batch closure never needs a runtime graph walk;
    # the bound collects fire exactly once on close, even with an empty buffer.
    fan_in_bindings: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass()
class InternalStepConfig:
    """
    Configuration for a single step in the workflow.

    Attributes:
        accepted_events: List of Event type classes this step can handle
        retry_policy: Policy for retrying failed executions, or None for no retries
        num_workers: Maximum number of concurrent executions of this step
    """

    accepted_events: list[Any]
    retry_policy: RetryPolicy | None
    num_workers: int


@dataclass()
class EventAttempt:
    """
    Represents an event that is being or will be processed by a step.

    Tracks retry information for events that have failed and are being retried.

    Attributes:
        event: The event to process
        attempts: Number of times this event has been attempted (0 for first attempt), or None if not yet attempted
        first_attempt_at: Unix timestamp of first attempt, or None if not yet attempted
        last_exception: Most recent exception, if this attempt is a retry.
        last_failed_at: Unix timestamp of the most recent failure, or None.
    """

    event: Event
    attempts: int | None = None
    first_attempt_at: float | None = None
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    # Batch lineage stack of the event being processed (innermost id last).
    batch_stack: tuple[str, ...] = field(default_factory=tuple)
    # Closed-batch collect payload. Normal event attempts leave this unset; when
    # a list[E] collect waits behind num_workers capacity, this carries the
    # already-buffered batch until the worker slot opens.
    batch_input: list[Event] | None = None


@dataclass()
class InternalStepWorkerState:
    """
    Runtime state for a single step's worker pool.

    This manages the queue of pending events, currently executing workers, and any
    state needed for ctx.collect_events() and ctx.wait_for_event() operations.

    Attributes:
        queue: Events waiting to be processed by this step
        config: Step configuration (includes retry policy, num_workers, etc.)
        in_progress: Currently executing workers for this step
        collected_events: Events being collected via ctx.collect_events(), keyed by buffer_id
        collected_waiters: Active waiters created by ctx.wait_for_event()
    """

    queue: list[EventAttempt]
    config: StepConfig
    in_progress: list[InProgressState]
    collected_events: dict[str, list[Event]]
    collected_waiters: list[StepWorkerWaiter]
    # Batch-lineage fan-in: events buffered per fan-out batch id, awaiting
    # the matching TickBatchClosed. Keyed by innermost batch id. Only populated
    # for steps with a ``batch_collect_param``.
    batch_buffers: dict[str, list[Event]] = field(default_factory=dict)
    # Cardinality release: batch ids this collect step has already released
    # early via a Take(n) threshold. Late siblings of a fired batch are
    # ignored, and the eventual TickBatchClosed does not re-fire the step.
    batch_fired: set[str] = field(default_factory=set)

    def _deepcopy(self) -> InternalStepWorkerState:
        return InternalStepWorkerState(
            queue=[dataclasses.replace(x) for x in self.queue],
            config=self.config,
            in_progress=[x._deepcopy() for x in self.in_progress],
            collected_events={k: list(v) for k, v in self.collected_events.items()},
            collected_waiters=[dataclasses.replace(x) for x in self.collected_waiters],
            batch_buffers={k: list(v) for k, v in self.batch_buffers.items()},
            batch_fired=set(self.batch_fired),
        )


@dataclass()
class InProgressState:
    """
    Represents a single worker execution that is currently in progress.

    Each worker gets a snapshot of the step's shared state at the time it starts.
    This enables optimistic execution - if the shared state changes during execution
    (e.g., new collected events arrive), the control loop can detect this and retry
    the worker with the updated state.

    Attributes:
        event: The event being processed by this worker
        worker_id: Numeric ID (0 to num_workers-1) identifying this worker slot
        shared_state: Snapshot of collected_events and collected_waiters at worker start time
        attempts: Number of times this event has been attempted (including current attempt)
        first_attempt_at: Unix timestamp when this event was first attempted
        last_exception: Most recent exception from the prior attempt, or None if this is the first attempt.
        last_failed_at: Unix timestamp of the most recent failure, or None.
    """

    event: Event
    worker_id: int
    shared_state: StepWorkerState
    attempts: int
    first_attempt_at: float
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    # Batch lineage stack of the trigger event for this execution. Output events
    # inherit this (1:1) or extend it (fan-out push) at result-processing time.
    batch_stack: tuple[str, ...] = field(default_factory=tuple)

    def _deepcopy(self) -> InProgressState:
        return InProgressState(
            event=self.event,
            worker_id=self.worker_id,
            shared_state=self.shared_state._deepcopy(),
            attempts=self.attempts,
            first_attempt_at=self.first_attempt_at,
            last_exception=self.last_exception,
            last_failed_at=self.last_failed_at,
            recovery_counts=dict(self.recovery_counts),
            batch_stack=self.batch_stack,
        )
