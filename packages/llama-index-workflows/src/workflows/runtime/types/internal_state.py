# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import dataclasses
import importlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from workflows._event_matching import step_accepts_type
from workflows._stream_levels import event_types, stream_level_types_by_producer
from workflows.collect import Collect, Take
from workflows.context.context_types import (
    CURRENT_SERIALIZED_VERSION,
    SerializedCollectionReleasePayload,
    SerializedCollectionReleaseState,
    SerializedCollectionStreamInstance,
    SerializedContext,
    SerializedEventAttempt,
    SerializedStepWorkerState,
    SerializedWaiter,
)
from workflows.context.serializers import JsonSerializer
from workflows.decorators import CatchErrorHandler, StepConfig
from workflows.events import Event
from workflows.retry_policy import RetryPolicy
from workflows.runtime.types.results import (
    CollectionReleasePayload,
    StepWorkerState,
    StepWorkerWaiter,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickAddEvent, WorkflowTick
from workflows.workflow import Workflow

if TYPE_CHECKING:
    from workflows.context.serializers import BaseSerializer


@dataclass(frozen=True)
class CollectionBinding:
    """
    Static typed stream binding from a finite list source to a collect step.

    Bindings are computed once from step signatures and stored in BrokerConfig.
    At runtime, each CollectionStreamInstance records the binding ids that may
    accept events produced inside that stream.
    """

    id: str
    source_step: str
    target_step: str
    item_types: tuple[type[Event], ...]
    policy: Collect


@dataclass()
class CollectionStreamInstance:
    """
    One execution of a collection-producing step.

    A returned ``list[E]`` opens one stream instance. Work emitted inside that
    stream carries ``scope_path`` so downstream completions can decrement the
    right open-work counter and close the nearest enclosing stream.
    """

    stream_id: str
    source_step: str
    scope_path: tuple[str, ...]
    open_work_items: int = 0
    accepting_binding_ids: tuple[str, ...] = ()

    def _copy(self) -> CollectionStreamInstance:
        return dataclasses.replace(self)


@dataclass()
class CollectionReleaseState:
    """
    Release state for one binding within one stream instance.

    ``buffer`` stores member events until the binding's Collect policy releases
    them. ``released`` makes the release fire-once: Take(n) sets it on the
    n-th arrival, and stream close sets it for All() (or an unmet Take).
    """

    binding_id: str
    stream_id: str
    buffer: list[Event] = field(default_factory=list)
    released: bool = False

    def _copy(self) -> CollectionReleaseState:
        return dataclasses.replace(self, buffer=list(self.buffer))


@dataclass()
class BrokerState:
    """
    Complete state of the workflow broker at a given point in time.

    This is the primary state object passed through the control loop's reducer pattern.
    Each tick processes this state and returns an updated copy along with commands to execute.

    Attributes:
        config: Immutable configuration for the workflow and all steps
        workers: Mutable state for each step's worker pool, queues, and in-progress executions
        stream_seq: Monotonic counter used to mint deterministic collection stream ids
        streams: Open collection streams keyed by stream id
        collection_release_states: Per-binding release buffers keyed by stream and binding
    """

    is_running: bool
    config: BrokerConfig
    workers: dict[str, InternalStepWorkerState]
    stream_seq: int = 0
    work_item_seq: int = 0
    streams: dict[str, CollectionStreamInstance] = field(default_factory=dict)
    collection_release_states: dict[str, CollectionReleaseState] = field(
        default_factory=dict
    )

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
            stream_seq=self.stream_seq,
            work_item_seq=self.work_item_seq,
            streams={sid: stream._copy() for sid, stream in self.streams.items()},
            collection_release_states={
                key: state._copy()
                for key, state in self.collection_release_states.items()
            },
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
                        accept_event_subclasses=step_func._step_config.accept_event_subclasses,
                    )
                    for name, step_func in workflow._get_steps().items()
                },
                timeout=workflow._timeout,
                catch_error_handlers=dict(workflow._catch_error_handlers),
                handler_for_step=dict(workflow._handler_for_step),
                collection_bindings=_compute_collection_bindings(workflow),
            ),
            workers={
                name: InternalStepWorkerState(
                    queue=[],
                    config=step_func._step_config,
                    in_progress=[],
                    collected_events={},
                    static_collect_events=[],
                    collected_waiters=[],
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
                    # Re-ping the step with its whole work record so the
                    # re-registered waiter keeps its stream scope and any
                    # collect batch.
                    commands.append(
                        TickAddEvent(
                            event=waiter.event,
                            step_id=StepId.root(step_name),
                            bound_events=waiter.bound_events,
                            scope_path=waiter.scope_path,
                            collection_release_payload=waiter.collection_release_payload,
                            work_item_id=waiter.work_item_id,
                        )
                    )
        return commands

    def to_serialized(self, serializer: BaseSerializer) -> SerializedContext:
        """Serialize the broker state to a SerializedContext."""

        workers_dict = {}
        for step_name, worker_state in self.workers.items():
            # Serialize queue with retry and stream scope info
            queue = [
                SerializedEventAttempt(
                    event=serializer.serialize(attempt.event),
                    bound_events={
                        name: serializer.serialize(event)
                        for name, event in attempt.bound_events.items()
                    }
                    if attempt.bound_events is not None
                    else None,
                    attempts=attempt.attempts or 0,
                    first_attempt_at=attempt.first_attempt_at,
                    last_exception=attempt.last_exception,
                    last_failed_at=attempt.last_failed_at,
                    recovery_counts=dict(attempt.recovery_counts),
                    not_before=attempt.not_before,
                    scope_path=list(attempt.scope_path),
                    collection_release_payload=_serialize_release_payload(
                        attempt.collection_release_payload, serializer
                    ),
                    work_item_id=attempt.work_item_id,
                )
                for attempt in worker_state.queue
            ]
            # Serialize in-progress attempts so they can be re-queued on resume.
            in_progress = [
                SerializedEventAttempt(
                    event=serializer.serialize(ip.event),
                    bound_events={
                        name: serializer.serialize(event)
                        for name, event in ip.bound_events.items()
                    }
                    if ip.bound_events is not None
                    else None,
                    attempts=ip.attempts or 0,
                    first_attempt_at=ip.first_attempt_at,
                    last_exception=ip.last_exception,
                    last_failed_at=ip.last_failed_at,
                    recovery_counts=dict(ip.recovery_counts),
                    scope_path=list(ip.scope_path),
                    collection_release_payload=_serialize_release_payload(
                        ip.shared_state.collection_release_payload, serializer
                    ),
                    work_item_id=ip.work_item_id,
                )
                for ip in worker_state.in_progress
            ]

            # Serialize collected events
            collected_events = {
                buffer_id: [serializer.serialize(ev) for ev in events]
                for buffer_id, events in worker_state.collected_events.items()
            }

            # Serialize waiters
            waiters = [
                SerializedWaiter(
                    waiter_id=waiter.waiter_id,
                    event=serializer.serialize(waiter.event),
                    bound_events={
                        name: serializer.serialize(event)
                        for name, event in waiter.bound_events.items()
                    }
                    if waiter.bound_events is not None
                    else None,
                    waiting_for_event=f"{waiter.waiting_for_event.__module__}.{waiter.waiting_for_event.__name__}",
                    has_requirements=bool(len(waiter.requirements))
                    or waiter.has_requirements,
                    resolved_event=serializer.serialize(waiter.resolved_event)
                    if waiter.resolved_event
                    else None,
                    scope_path=list(waiter.scope_path),
                    collection_release_payload=_serialize_release_payload(
                        waiter.collection_release_payload, serializer
                    ),
                    work_item_id=waiter.work_item_id,
                )
                for waiter in worker_state.collected_waiters
            ]

            workers_dict[step_name] = SerializedStepWorkerState(
                queue=queue,
                in_progress=in_progress,
                collected_events=collected_events,
                static_collect_events=[
                    serializer.serialize(ev)
                    for ev in worker_state.static_collect_events
                ],
                collected_waiters=waiters,
            )

        return SerializedContext(
            version=CURRENT_SERIALIZED_VERSION,
            state={},  # State is filled separately by the state store
            is_running=self.is_running,
            workers=workers_dict,
            stream_seq=self.stream_seq,
            work_item_seq=self.work_item_seq,
            streams={
                sid: SerializedCollectionStreamInstance(
                    stream_id=stream.stream_id,
                    source_step=stream.source_step,
                    scope_path=list(stream.scope_path),
                    open_work_items=stream.open_work_items,
                    accepting_binding_ids=list(stream.accepting_binding_ids),
                )
                for sid, stream in self.streams.items()
            },
            collection_release_states={
                key: SerializedCollectionReleaseState(
                    binding_id=release.binding_id,
                    stream_id=release.stream_id,
                    buffer=[serializer.serialize(ev) for ev in release.buffer],
                    released=release.released,
                )
                for key, release in self.collection_release_states.items()
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
        base_state.stream_seq = serialized.stream_seq
        base_state.work_item_seq = serialized.work_item_seq
        base_state.streams = {
            sid: CollectionStreamInstance(
                stream_id=stream.stream_id,
                source_step=stream.source_step,
                scope_path=tuple(stream.scope_path),
                open_work_items=stream.open_work_items,
                accepting_binding_ids=tuple(stream.accepting_binding_ids),
            )
            for sid, stream in serialized.streams.items()
        }
        base_state.collection_release_states = {
            key: CollectionReleaseState(
                binding_id=release.binding_id,
                stream_id=release.stream_id,
                buffer=[serializer.deserialize(ev) for ev in release.buffer],
                released=release.released,
            )
            for key, release in serialized.collection_release_states.items()
        }

        # Restore worker state (queues, collected events, waiters)
        # We do this regardless of is_running state so workflows can resume from where they left off
        for step_name, worker_data in serialized.workers.items():
            if step_name not in base_state.workers:
                continue

            worker = base_state.workers[step_name]

            # Restore queue with retry and stream scope info.
            # in_progress events are moved to the queue on deserialization;
            # they will be restarted when the workflow runs.
            worker.queue = [
                _deserialize_event_attempt(attempt, serializer)
                for attempt in [*worker_data.queue, *worker_data.in_progress]
            ]

            # Restore collected events
            worker.collected_events = {
                buffer_id: [serializer.deserialize(ev) for ev in events]
                for buffer_id, events in worker_data.collected_events.items()
            }
            worker.static_collect_events = [
                serializer.deserialize(ev) for ev in worker_data.static_collect_events
            ]

            # Restore waiters
            worker.collected_waiters = []
            for waiter_data in worker_data.collected_waiters:
                waiter_payload = _deserialize_release_payload(
                    waiter_data.collection_release_payload, serializer
                )
                worker.collected_waiters.append(
                    StepWorkerWaiter(
                        waiter_id=waiter_data.waiter_id,
                        bound_events={
                            name: serializer.deserialize(event)
                            for name, event in waiter_data.bound_events.items()
                        }
                        if waiter_data.bound_events is not None
                        else None,
                        # For a suspended collect invocation the payload is the
                        # authoritative record; the trigger event is derived
                        # from it so the two cannot diverge on resume.
                        event=waiter_payload.as_event()
                        if waiter_payload is not None
                        else serializer.deserialize(waiter_data.event),
                        waiting_for_event=_import_event_type(
                            waiter_data.waiting_for_event
                        ),
                        requirements={},
                        has_requirements=waiter_data.has_requirements,
                        resolved_event=serializer.deserialize(
                            waiter_data.resolved_event
                        )
                        if waiter_data.resolved_event
                        else None,
                        scope_path=tuple(waiter_data.scope_path),
                        collection_release_payload=waiter_payload,
                        work_item_id=waiter_data.work_item_id,
                    )
                )

        return base_state


def _deserialize_event_attempt(
    attempt: SerializedEventAttempt, serializer: BaseSerializer
) -> EventAttempt:
    """Restore one queued work item.

    For a collect invocation the persisted payload is the authoritative
    record; the trigger event is derived from it so the two cannot diverge
    across a serialize/resume boundary.
    """
    payload = _deserialize_release_payload(
        attempt.collection_release_payload, serializer
    )
    return EventAttempt(
        event=payload.as_event()
        if payload is not None
        else serializer.deserialize(attempt.event),
        bound_events={
            name: serializer.deserialize(event)
            for name, event in attempt.bound_events.items()
        }
        if attempt.bound_events is not None
        else None,
        attempts=attempt.attempts,
        first_attempt_at=attempt.first_attempt_at,
        last_exception=attempt.last_exception,
        last_failed_at=attempt.last_failed_at,
        recovery_counts=dict(attempt.recovery_counts),
        not_before=attempt.not_before,
        scope_path=tuple(attempt.scope_path),
        collection_release_payload=payload,
        work_item_id=attempt.work_item_id,
    )


def _import_event_type(qualified_name: str) -> type[Event]:
    """Import an event type from a fully qualified name like 'mymodule.MyEvent'."""
    parts = qualified_name.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid qualified name: {qualified_name}")

    module_name, class_name = parts

    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _binding_id(
    source_step: str,
    target_step: str,
    item_types: tuple[type[Event], ...],
    policy: Collect,
) -> str:
    """Build a stable id for one static source-to-collect-step binding."""
    type_names = ",".join(f"{t.__module__}.{t.__qualname__}" for t in item_types)
    card = policy.cardinality
    card_repr = f"Take({card.n})" if isinstance(card, Take) else type(card).__name__
    return f"{source_step}->{target_step}:{type_names}:{card_repr}:nearest"


def _compute_collection_bindings(workflow: Workflow) -> dict[str, CollectionBinding]:
    """
    Compute static list[E] stream bindings from the workflow graph.

    A fan-out source binds to a collection step when the collection step's item
    type appears at the same stream level reachable from the source (see
    :mod:`workflows._stream_levels` for the level traversal, shared with static
    validation).
    """
    steps = {name: fn._step_config for name, fn in workflow._get_steps().items()}
    collects: dict[str, tuple[Any, ...]] = {
        name: cfg.collection_param[1]
        for name, cfg in steps.items()
        if cfg.collection_param is not None
    }

    bindings: dict[str, CollectionBinding] = {}
    for source_step, level_types in stream_level_types_by_producer(steps).items():
        for target_step, collect_types in collects.items():
            item_types = tuple(event_types(collect_types))
            if not any(
                step_accepts_type(
                    produced,
                    item_types,
                    allow_subclasses=steps[target_step].accept_event_subclasses,
                )
                for produced in level_types
            ):
                continue
            policy = steps[target_step].collection_policy
            if policy is None:
                continue
            binding = CollectionBinding(
                id=_binding_id(source_step, target_step, item_types, policy),
                source_step=source_step,
                target_step=target_step,
                item_types=item_types,
                policy=policy,
            )
            bindings[binding.id] = binding
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
        collection_bindings: Static list[E] stream bindings keyed by binding id
    """

    steps: dict[str, InternalStepConfig]
    timeout: float | None
    catch_error_handlers: dict[str, CatchErrorHandler] = field(default_factory=dict)
    handler_for_step: dict[str, str] = field(default_factory=dict)
    collection_bindings: dict[str, CollectionBinding] = field(default_factory=dict)

    def bindings_for_source(self, source_step: str) -> tuple[CollectionBinding, ...]:
        return tuple(
            binding
            for binding in self.collection_bindings.values()
            if binding.source_step == source_step
        )

    def binding_for_target(
        self,
        stream_id: str,
        target_step: str,
        streams: dict[str, CollectionStreamInstance],
    ) -> CollectionBinding | None:
        stream = streams.get(stream_id)
        if stream is None:
            return None
        for binding_id in stream.accepting_binding_ids:
            binding = self.collection_bindings.get(binding_id)
            if binding is not None and binding.target_step == target_step:
                return binding
        return None


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
    accept_event_subclasses: bool = False


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
        not_before: Absolute adapter-get_now time before which this attempt
            must not be dispatched (retry delay), or None if eligible now.
        recovery_counts: Per-handler recovery counts on this event's lineage.
        scope_path: Collection stream scope path, innermost stream id last.
        collection_release_payload: Explicit payload for queued list[E] collect invocations.
    """

    event: Event
    bound_events: dict[str, Event] | None = None
    attempts: int | None = None
    first_attempt_at: float | None = None
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    not_before: float | None = None
    scope_path: tuple[str, ...] = field(default_factory=tuple)
    collection_release_payload: CollectionReleasePayload | None = None
    work_item_id: str | None = None


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
    static_collect_events: list[Event] = field(default_factory=list)

    def _deepcopy(self) -> InternalStepWorkerState:
        return InternalStepWorkerState(
            queue=[dataclasses.replace(x) for x in self.queue],
            config=self.config,
            in_progress=[x._deepcopy() for x in self.in_progress],
            collected_events={k: list(v) for k, v in self.collected_events.items()},
            static_collect_events=list(self.static_collect_events),
            collected_waiters=[dataclasses.replace(x) for x in self.collected_waiters],
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
        recovery_counts: Per-handler recovery counts on this event's lineage.
        scope_path: Collection stream scope path for the worker's current event.
    """

    event: Event
    worker_id: int
    shared_state: StepWorkerState
    attempts: int
    first_attempt_at: float
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    bound_events: dict[str, Event] | None = None
    scope_path: tuple[str, ...] = field(default_factory=tuple)
    work_item_id: str | None = None

    def _deepcopy(self) -> InProgressState:
        return InProgressState(
            event=self.event,
            bound_events=dict(self.bound_events) if self.bound_events else None,
            worker_id=self.worker_id,
            shared_state=self.shared_state._deepcopy(),
            attempts=self.attempts,
            first_attempt_at=self.first_attempt_at,
            last_exception=self.last_exception,
            last_failed_at=self.last_failed_at,
            recovery_counts=dict(self.recovery_counts),
            scope_path=self.scope_path,
            work_item_id=self.work_item_id,
        )


def _serialize_release_payload(
    payload: CollectionReleasePayload | None, serializer: BaseSerializer
) -> SerializedCollectionReleasePayload | None:
    """Serialize a queued list[E] collect invocation payload."""
    if payload is None:
        return None
    return SerializedCollectionReleasePayload(
        binding_id=payload.binding_id,
        stream_id=payload.stream_id,
        events=[serializer.serialize(ev) for ev in payload.events],
        output_scope_path=list(payload.output_scope_path),
    )


def _deserialize_release_payload(
    payload: SerializedCollectionReleasePayload | None, serializer: BaseSerializer
) -> CollectionReleasePayload | None:
    """Deserialize a queued list[E] collect invocation payload."""
    if payload is None:
        return None
    return CollectionReleasePayload(
        binding_id=payload.binding_id,
        stream_id=payload.stream_id,
        events=[serializer.deserialize(ev) for ev in payload.events],
        output_scope_path=tuple(payload.output_scope_path),
    )
