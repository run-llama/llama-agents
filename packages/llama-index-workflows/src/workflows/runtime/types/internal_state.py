# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import dataclasses
import importlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

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
from workflows.runtime.types.ticks import TickAddEvent, WorkflowTick
from workflows.workflow import Workflow

if TYPE_CHECKING:
    from workflows.context.serializers import BaseSerializer


@dataclass(frozen=True)
class CollectionBinding:
    """Static typed stream binding from a finite list source to a collect step."""

    id: str
    source_step: str
    target_step: str
    item_types: tuple[type[Event], ...]
    policy: Collect
    scope_rule: Literal["nearest"] = "nearest"


@dataclass()
class CollectionStreamInstance:
    """One execution of a collection-producing step."""

    stream_id: str
    source_step: str
    source_execution_id: str
    parent_stream_id: str | None
    scope_path: tuple[str, ...]
    open_work_items: int = 0
    accepting_binding_ids: tuple[str, ...] = ()
    closed_to_new_items: bool = True

    def _copy(self) -> CollectionStreamInstance:
        return dataclasses.replace(self)


@dataclass()
class CollectionReleaseState:
    """Release state for one binding within one stream instance."""

    binding_id: str
    stream_id: str
    buffer: list[Event] = field(default_factory=list)
    released: bool = False
    cursor: int = 0

    def _copy(self) -> CollectionReleaseState:
        return dataclasses.replace(self, buffer=list(self.buffer))


@dataclass()
class BrokerState:
    """
    Complete state of the workflow broker at a given point in time.

    This is the primary state object passed through the control loop's reducer
    pattern. Each tick processes this state and returns an updated copy along
    with commands to execute.
    """

    is_running: bool
    config: BrokerConfig
    workers: dict[str, InternalStepWorkerState]
    stream_seq: int = 0
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
            config=self.config,
            workers={
                name: worker_state._deepcopy()
                for name, worker_state in self.workers.items()
            },
            stream_seq=self.stream_seq,
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
                    collected_waiters=[],
                )
                for name, step_func in workflow._get_steps().items()
            },
        )

    def rehydrate_with_ticks(self) -> list[WorkflowTick]:
        """
        Rehydrates non-serializable state by re-running commands.
        """
        ticks: list[WorkflowTick] = []
        for step_name, worker_state in sorted(self.workers.items(), key=lambda x: x[0]):
            for waiter in sorted(
                worker_state.collected_waiters, key=lambda x: x.waiter_id
            ):
                if waiter.has_requirements and not waiter.requirements:
                    ticks.append(TickAddEvent(event=waiter.event, step_name=step_name))
        return ticks

    def to_serialized(self, serializer: BaseSerializer) -> SerializedContext:
        """Serialize the broker state to a SerializedContext."""
        workers_dict = {}
        for step_name, worker_state in self.workers.items():
            # Serialize queue with retry and stream scope info.
            queue = [
                SerializedEventAttempt(
                    event=serializer.serialize(attempt.event),
                    attempts=attempt.attempts or 0,
                    first_attempt_at=attempt.first_attempt_at,
                    last_exception=attempt.last_exception,
                    last_failed_at=attempt.last_failed_at,
                    recovery_counts=dict(attempt.recovery_counts),
                    scope_path=list(attempt.scope_path),
                    collection_release_payload=_serialize_release_payload(
                        attempt.collection_release_payload, serializer
                    ),
                )
                for attempt in worker_state.queue
            ]
            # Serialize in-progress events so they can be re-queued on resume.
            in_progress = [
                SerializedEventAttempt(
                    event=serializer.serialize(ip.event),
                    attempts=ip.attempts or 0,
                    first_attempt_at=ip.first_attempt_at,
                    last_exception=ip.last_exception,
                    last_failed_at=ip.last_failed_at,
                    recovery_counts=dict(ip.recovery_counts),
                    scope_path=list(ip.scope_path),
                    collection_release_payload=_serialize_release_payload(
                        ip.shared_state.collection_release_payload, serializer
                    ),
                )
                for ip in worker_state.in_progress
            ]
            # Serialize collected events.
            collected_events = {
                buffer_id: [serializer.serialize(ev) for ev in events]
                for buffer_id, events in worker_state.collected_events.items()
            }
            # Serialize waiters.
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
            )

        return SerializedContext(
            version=CURRENT_SERIALIZED_VERSION,
            state={},  # State is filled separately by the state store.
            is_running=self.is_running,
            workers=workers_dict,
            stream_seq=self.stream_seq,
            streams={
                sid: SerializedCollectionStreamInstance(
                    stream_id=stream.stream_id,
                    source_step=stream.source_step,
                    source_execution_id=stream.source_execution_id,
                    parent_stream_id=stream.parent_stream_id,
                    scope_path=list(stream.scope_path),
                    open_work_items=stream.open_work_items,
                    accepting_binding_ids=list(stream.accepting_binding_ids),
                    closed_to_new_items=stream.closed_to_new_items,
                )
                for sid, stream in self.streams.items()
            },
            collection_release_states={
                key: SerializedCollectionReleaseState(
                    binding_id=release.binding_id,
                    stream_id=release.stream_id,
                    buffer=[serializer.serialize(ev) for ev in release.buffer],
                    released=release.released,
                    cursor=release.cursor,
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
        # Start with a base state from the workflow.
        base_state = BrokerState.from_workflow(workflow)
        # Preserve this so the workflow knows whether to construct a StartEvent
        # from kwargs when it resumes.
        base_state.is_running = serialized.is_running
        base_state.stream_seq = serialized.stream_seq
        base_state.streams = {
            sid: CollectionStreamInstance(
                stream_id=stream.stream_id,
                source_step=stream.source_step,
                source_execution_id=stream.source_execution_id,
                parent_stream_id=stream.parent_stream_id,
                scope_path=tuple(stream.scope_path),
                open_work_items=stream.open_work_items,
                accepting_binding_ids=tuple(stream.accepting_binding_ids),
                closed_to_new_items=stream.closed_to_new_items,
            )
            for sid, stream in serialized.streams.items()
        }
        base_state.collection_release_states = {
            key: CollectionReleaseState(
                binding_id=release.binding_id,
                stream_id=release.stream_id,
                buffer=[serializer.deserialize(ev) for ev in release.buffer],
                released=release.released,
                cursor=release.cursor,
            )
            for key, release in serialized.collection_release_states.items()
        }

        # Restore worker state even when not running so resume can pick up the
        # persisted queues, collected events, and waiters.
        for step_name, worker_data in serialized.workers.items():
            if step_name not in base_state.workers:
                continue
            worker = base_state.workers[step_name]
            # Restore queue with retry and stream scope info.
            worker.queue = [
                EventAttempt(
                    event=serializer.deserialize(attempt.event),
                    attempts=attempt.attempts,
                    first_attempt_at=attempt.first_attempt_at,
                    last_exception=attempt.last_exception,
                    last_failed_at=attempt.last_failed_at,
                    recovery_counts=dict(attempt.recovery_counts),
                    scope_path=tuple(attempt.scope_path),
                    collection_release_payload=_deserialize_release_payload(
                        attempt.collection_release_payload, serializer
                    ),
                )
                for attempt in worker_data.queue
            ]
            # In-progress events are moved back to the queue on deserialization.
            for attempt in worker_data.in_progress:
                worker.queue.append(
                    EventAttempt(
                        event=serializer.deserialize(attempt.event),
                        attempts=attempt.attempts,
                        first_attempt_at=attempt.first_attempt_at,
                        last_exception=attempt.last_exception,
                        last_failed_at=attempt.last_failed_at,
                        recovery_counts=dict(attempt.recovery_counts),
                        scope_path=tuple(attempt.scope_path),
                        collection_release_payload=_deserialize_release_payload(
                            attempt.collection_release_payload, serializer
                        ),
                    )
                )
            # Restore collected events.
            worker.collected_events = {
                buffer_id: [serializer.deserialize(ev) for ev in events]
                for buffer_id, events in worker_data.collected_events.items()
            }
            # Restore waiters.
            worker.collected_waiters = []
            for waiter_data in worker_data.collected_waiters:
                worker.collected_waiters.append(
                    StepWorkerWaiter(
                        waiter_id=waiter_data.waiter_id,
                        event=serializer.deserialize(waiter_data.event),
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
                    )
                )
        return base_state


def _import_event_type(qualified_name: str) -> type[Event]:
    """Import an event type from a fully qualified name like 'mymodule.MyEvent'."""
    parts = qualified_name.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid qualified name: {qualified_name}")
    module_name, class_name = parts
    return getattr(importlib.import_module(module_name), class_name)


def _event_types(types: Any) -> list[type[Event]]:
    return [t for t in types if isinstance(t, type) and issubclass(t, Event)]


def _binding_id(
    source_step: str,
    target_step: str,
    item_types: tuple[type[Event], ...],
    policy: Collect,
) -> str:
    type_names = ",".join(f"{t.__module__}.{t.__qualname__}" for t in item_types)
    card = policy.cardinality
    card_repr = f"Take({card.n})" if isinstance(card, Take) else type(card).__name__
    return f"{source_step}->{target_step}:{type_names}:{card_repr}:nearest"


def _compute_collection_bindings(workflow: Workflow) -> dict[str, CollectionBinding]:
    steps = {name: fn._step_config for name, fn in workflow._get_steps().items()}
    collects: dict[str, tuple[Any, ...]] = {
        name: cfg.collection_param[1]
        for name, cfg in steps.items()
        if cfg.collection_param is not None
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
                if cfg.collection_param is not None:
                    continue
                if cfg.is_fan_out:
                    if name in guard:
                        continue
                    child = same_level_types(cfg.return_types, guard | {name})
                    for cname, cetypes in collects.items():
                        if any(et in child for et in cetypes):
                            frontier.extend(_event_types(steps[cname].return_types))
                    continue
                frontier.extend(_event_types(cfg.return_types))
        return seen

    bindings: dict[str, CollectionBinding] = {}
    for source_step, cfg in steps.items():
        if not cfg.is_fan_out:
            continue
        level_types = same_level_types(cfg.return_types, frozenset({source_step}))
        for target_step, collect_types in collects.items():
            item_types = tuple(_event_types(collect_types))
            if not any(et in level_types for et in item_types):
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
    accepted_events: list[Any]
    retry_policy: RetryPolicy | None
    num_workers: int


@dataclass()
class EventAttempt:
    event: Event
    attempts: int | None = None
    first_attempt_at: float | None = None
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    scope_path: tuple[str, ...] = field(default_factory=tuple)
    collection_release_payload: CollectionReleasePayload | None = None


@dataclass()
class InternalStepWorkerState:
    queue: list[EventAttempt]
    config: StepConfig
    in_progress: list[InProgressState]
    collected_events: dict[str, list[Event]]
    collected_waiters: list[StepWorkerWaiter]

    def _deepcopy(self) -> InternalStepWorkerState:
        return InternalStepWorkerState(
            queue=[dataclasses.replace(x) for x in self.queue],
            config=self.config,
            in_progress=[x._deepcopy() for x in self.in_progress],
            collected_events={k: list(v) for k, v in self.collected_events.items()},
            collected_waiters=[dataclasses.replace(x) for x in self.collected_waiters],
        )


@dataclass()
class InProgressState:
    event: Event
    worker_id: int
    shared_state: StepWorkerState
    attempts: int
    first_attempt_at: float
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    scope_path: tuple[str, ...] = field(default_factory=tuple)

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
            scope_path=self.scope_path,
        )


def _serialize_release_payload(
    payload: CollectionReleasePayload | None, serializer: BaseSerializer
) -> SerializedCollectionReleasePayload | None:
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
    if payload is None:
        return None
    return CollectionReleasePayload(
        binding_id=payload.binding_id,
        stream_id=payload.stream_id,
        events=[serializer.deserialize(ev) for ev in payload.events],
        output_scope_path=tuple(payload.output_scope_path),
    )
