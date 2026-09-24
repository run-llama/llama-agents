# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, TypeVar

from pydantic import TypeAdapter
from workflows._event_matching import step_accepts_type
from workflows._stream_levels import event_types, stream_level_types_by_producer
from workflows.collect import Collect, Take
from workflows.context.context_types import (
    CURRENT_SERIALIZED_VERSION,
    SerializedChildBroker,
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
from workflows.events import Event, SerializableEventType
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


T = TypeVar("T")


def _normalize_step_id(value: Any) -> StepId:
    return value if isinstance(value, StepId) else StepId.from_str(value)


def _normalize_step_id_keys(mapping: Mapping[Any, T]) -> dict[StepId, T]:
    return {_normalize_step_id(step_id): value for step_id, value in mapping.items()}


@dataclass(frozen=True)
class CollectionBinding:
    """
    Static typed stream binding from a finite list source to a collect step.

    Bindings are computed once from step signatures and stored in BrokerConfig.
    At runtime, each CollectionStreamInstance records the binding ids that may
    accept events produced inside that stream. Source/target are the broker-
    local (bare-namespace) StepIds, since bindings never cross a broker boundary.
    """

    id: str
    source_step: StepId
    target_step: StepId
    item_types: tuple[type[Event], ...]
    policy: Collect


@dataclass()
class CollectionStreamInstance:
    """
    One execution of a collection-producing step, local to one broker.

    A returned ``list[E]`` opens one stream instance. Work emitted inside that
    stream carries ``scope_path`` so downstream completions can decrement the
    right open-work counter and close the nearest enclosing stream.
    """

    stream_id: str
    source_step: StepId
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
class ChildBroker:
    """A child workflow invocation nested inside its parent broker.

    ``state`` is a full :class:`BrokerState` reduced by the same reducer. The
    boundary fields are the descending delivery's identity in the *parent's*
    stream accounting: the child is exactly one work item in the parent's
    enclosing stream (``boundary_scope_path``), consumed once at ascent. The
    child's own internal scope starts fresh at ``()``.
    """

    slot: str
    state: BrokerState
    boundary_scope_path: tuple[str, ...] = ()
    boundary_recovery_counts: dict[str, int] = field(default_factory=dict)

    def _deepcopy(self) -> ChildBroker:
        return ChildBroker(
            slot=self.slot,
            state=self.state.deepcopy(),
            boundary_scope_path=self.boundary_scope_path,
            boundary_recovery_counts=dict(self.boundary_recovery_counts),
        )


@dataclass()
class BrokerState:
    """State of ONE workflow invocation (root or any child).

    The reducer treats every broker uniformly: root is just the broker whose
    parent is the runner. ``workers`` and ``config.steps`` are keyed by the
    broker-local (bare-namespace) :class:`StepId` of this workflow's own steps;
    child invocations live in ``children`` as nested brokers, not as namespaced
    rows in this broker.

    Attributes:
        config: Immutable configuration for this workflow class (and, via
            ``child_configs``, its child classes).
        workers: Mutable per-step worker pools, queues, in-progress executions.
        stream_seq: Monotonic counter minting deterministic collection stream ids.
        work_item_seq: Monotonic per-broker work-item counter (waiter-id
            distinctness keys off this, so sibling invocations stay isolated).
        streams: Open collection streams keyed by stream id (local).
        collection_release_states: Per-binding release buffers.
        children: Live child invocations keyed by ``"slot#N"`` segment.
        child_seq: Per-slot mint counter (persisted for replay determinism).
        elapsed_alive: Known-alive seconds this broker has spent against its
            timeout budget. Accrues on each addressed tick from the journaled
            stamp; the session-start marker resets ``last_alive_stamp`` without
            accruing so downtime never enters the budget.
        last_alive_stamp: Accrual reference point (the last stamp seen), reset by
            the session-start marker at each resume. ``None`` before any stamp.
    """

    is_running: bool
    config: BrokerConfig
    workers: dict[StepId, InternalStepWorkerState]
    stream_seq: int = 0
    work_item_seq: int = 0
    streams: dict[str, CollectionStreamInstance] = field(default_factory=dict)
    collection_release_states: dict[str, CollectionReleaseState] = field(
        default_factory=dict
    )
    children: dict[str, ChildBroker] = field(default_factory=dict)
    child_seq: dict[str, int] = field(default_factory=dict)
    elapsed_alive: float = 0.0
    last_alive_stamp: float | None = None

    def __post_init__(self) -> None:
        self._normalize_worker_keys()

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        # Defaults for child fields absent from a pre-child pickle. Each call
        # rebuilds these literals, so the empty dicts are never shared.
        for name, default in (
            ("children", {}),
            ("child_seq", {}),
            ("elapsed_alive", 0.0),
            ("last_alive_stamp", None),
        ):
            if name not in state:
                setattr(self, name, default)
        self._normalize_worker_keys()

    def _normalize_worker_keys(self) -> None:
        self.workers = _normalize_step_id_keys(self.workers)

    def deepcopy(self) -> BrokerState:
        """
        Deep-ish copy. Copies fields that are considered mutable during updates.
        """
        return BrokerState(
            is_running=self.is_running,
            config=self.config,  # immutable
            workers={
                step_id: worker_state._deepcopy()
                for step_id, worker_state in self.workers.items()
            },
            stream_seq=self.stream_seq,
            work_item_seq=self.work_item_seq,
            streams={sid: stream._copy() for sid, stream in self.streams.items()},
            collection_release_states={
                key: state._copy()
                for key, state in self.collection_release_states.items()
            },
            children={key: child._deepcopy() for key, child in self.children.items()},
            child_seq=dict(self.child_seq),
            elapsed_alive=self.elapsed_alive,
            last_alive_stamp=self.last_alive_stamp,
        )

    def live_child_namespaces(self) -> set[tuple[str, ...]]:
        """Every live child invocation path in the broker tree (excludes root).

        Replaces the flat model's ``live_invocation_namespaces``: liveness *is*
        broker-tree presence, so a completed (popped) child is naturally absent.
        Used to bound the snapshot's per-namespace state export.
        """
        result: set[tuple[str, ...]] = set()

        def walk(state: BrokerState, path: tuple[str, ...]) -> None:
            for key, child in state.children.items():
                child_path = (*path, key)
                result.add(child_path)
                walk(child.state, child_path)

        walk(self, ())
        return result

    @staticmethod
    def from_workflow(workflow: Workflow) -> BrokerState:
        """Build the root broker for ``workflow`` with an empty children map.

        Config recurses into ``child_configs`` so the reducer can reach a child
        class's template at boundary descent without touching ``Workflow``; child
        *state* is created lazily when a slot is first triggered.
        """
        return _broker_state_from_instance(workflow, is_root=True)

    @staticmethod
    def from_config(config: BrokerConfig) -> BrokerState:
        """Mint a fresh running broker from a (child) config template."""
        return BrokerState(
            is_running=True,
            config=config,
            workers={
                step_id: InternalStepWorkerState(
                    queue=[],
                    config=step_config,
                    in_progress=[],
                    collected_events={},
                    static_collect_events=[],
                    collected_waiters=[],
                )
                for step_id, step_config in config.steps.items()
            },
        )

    def rehydrate_with_ticks(self) -> list[WorkflowTick]:
        """Re-ping suspended waiters across the whole broker tree.

        Recurses, path-qualifying each re-ping ``TickAddEvent`` with its owning
        broker's invocation path so ``descend`` routes it back to that broker.
        """
        commands: list[WorkflowTick] = []
        _rehydrate_broker(self, (), commands)
        return commands

    def to_serialized(self, serializer: BaseSerializer) -> SerializedContext:
        """Serialize this broker (and nested children) to a SerializedContext."""
        return _broker_to_serialized(self, serializer)

    @staticmethod
    def from_serialized(
        serialized: SerializedContext,
        workflow: Workflow,
        serializer: BaseSerializer,
    ) -> BrokerState:
        """Deserialize a SerializedContext into the root BrokerState.

        Nested ``child_brokers`` rebuild the child tree from the config
        templates.
        """
        serializer = serializer if serializer is not None else JsonSerializer()
        base_state = BrokerState.from_workflow(workflow)
        _load_broker_from_serialized(base_state, serialized, serializer)
        return base_state


def _broker_state_from_instance(workflow: Workflow, *, is_root: bool) -> BrokerState:
    config = _config_from_instance(workflow, is_root=is_root)
    state = BrokerState.from_config(config)
    state.is_running = False
    return state


def _config_from_instance(workflow: Workflow, *, is_root: bool) -> BrokerConfig:
    workflow._ensure_children_attached()
    steps_by_name = workflow._get_steps()
    steps: dict[StepId, InternalStepConfig] = {
        StepId((), name): _internal_step_config(func._step_config)
        for name, func in steps_by_name.items()
    }
    catch_error_handlers: dict[StepId, CatchErrorHandler] = {
        StepId((), name): handler
        for name, handler in workflow._catch_error_handlers.items()
    }
    handler_for_step: dict[StepId, StepId] = {
        StepId((), step_name): StepId((), handler_name)
        for step_name, handler_name in workflow._handler_for_step.items()
    }
    child_configs = {
        slot: _config_from_instance(child, is_root=False)
        for slot, child in workflow._child_workflows.items()
    }
    timeout = workflow._timeout if is_root else workflow._child_namespace_timeout()
    return BrokerConfig(
        steps=steps,
        timeout=timeout,
        catch_error_handlers=catch_error_handlers,
        handler_for_step=handler_for_step,
        collection_bindings=_compute_collection_bindings(
            {name: func._step_config for name, func in steps_by_name.items()}
        ),
        child_configs=child_configs,
    )


def _internal_step_config(step_config: StepConfig) -> InternalStepConfig:
    return InternalStepConfig(
        accepted_events=step_config.accepted_events,
        retry_policy=step_config.retry_policy,
        num_workers=step_config.num_workers,
        accept_event_subclasses=step_config.accept_event_subclasses,
        collection_param=step_config.collection_param,
        collect_params=step_config.collect_params,
        collection_policy=step_config.collection_policy,
    )


def _rehydrate_broker(
    state: BrokerState, path: tuple[str, ...], commands: list[WorkflowTick]
) -> None:
    for step_id, worker_state in sorted(state.workers.items(), key=lambda x: str(x[0])):
        for waiter in sorted(worker_state.collected_waiters, key=lambda x: x.waiter_id):
            if waiter.has_requirements and not waiter.requirements:
                commands.append(
                    TickAddEvent(
                        event=waiter.event,
                        step_id=step_id,
                        bound_events=waiter.bound_events,
                        scope_path=waiter.scope_path,
                        origin_namespace=path,
                        collection_release_payload=waiter.collection_release_payload,
                        work_item_id=waiter.work_item_id,
                    )
                )
    for key, child in sorted(state.children.items()):
        _rehydrate_broker(child.state, (*path, key), commands)


def _broker_to_serialized(
    state: BrokerState, serializer: BaseSerializer
) -> SerializedContext:
    workers_dict = {}
    for step_id, worker_state in state.workers.items():
        queue = [
            _serialize_event_attempt(attempt, serializer)
            for attempt in worker_state.queue
        ]
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
        collected_events = {
            buffer_id: [serializer.serialize(ev) for ev in events]
            for buffer_id, events in worker_state.collected_events.items()
        }
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
        # Workers are keyed by the bare step name within this broker (identical
        # to the pre-child root format).
        workers_dict[str(step_id)] = SerializedStepWorkerState(
            queue=queue,
            in_progress=in_progress,
            collected_events=collected_events,
            static_collect_events=[
                serializer.serialize(ev) for ev in worker_state.static_collect_events
            ],
            collected_waiters=waiters,
        )

    child_brokers = {
        key: SerializedChildBroker(
            slot=child.slot,
            boundary_scope_path=list(child.boundary_scope_path),
            boundary_recovery_counts=dict(child.boundary_recovery_counts),
            broker=_broker_to_serialized(child.state, serializer),
        )
        for key, child in state.children.items()
    }

    return SerializedContext(
        version=CURRENT_SERIALIZED_VERSION,
        state={},  # State is filled separately by the state store
        is_running=state.is_running,
        workers=workers_dict,
        stream_seq=state.stream_seq,
        work_item_seq=state.work_item_seq,
        streams={
            sid: SerializedCollectionStreamInstance(
                stream_id=stream.stream_id,
                source_step=str(stream.source_step),
                scope_path=list(stream.scope_path),
                open_work_items=stream.open_work_items,
                accepting_binding_ids=list(stream.accepting_binding_ids),
            )
            for sid, stream in state.streams.items()
        },
        collection_release_states={
            key: SerializedCollectionReleaseState(
                binding_id=release.binding_id,
                stream_id=release.stream_id,
                buffer=[serializer.serialize(ev) for ev in release.buffer],
                released=release.released,
            )
            for key, release in state.collection_release_states.items()
        },
        child_brokers=child_brokers,
        child_seq=dict(state.child_seq),
        elapsed_alive=state.elapsed_alive,
        last_alive_stamp=state.last_alive_stamp,
    )


def _serialize_event_attempt(
    attempt: EventAttempt, serializer: BaseSerializer
) -> SerializedEventAttempt:
    return SerializedEventAttempt(
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


def _load_broker_from_serialized(
    state: BrokerState, serialized: SerializedContext, serializer: BaseSerializer
) -> None:
    state.is_running = serialized.is_running
    state.stream_seq = serialized.stream_seq
    state.work_item_seq = serialized.work_item_seq
    state.child_seq = dict(serialized.child_seq)
    state.elapsed_alive = serialized.elapsed_alive
    state.last_alive_stamp = serialized.last_alive_stamp
    state.streams = {
        sid: CollectionStreamInstance(
            stream_id=stream.stream_id,
            source_step=StepId.from_str(stream.source_step),
            scope_path=tuple(stream.scope_path),
            open_work_items=stream.open_work_items,
            accepting_binding_ids=tuple(stream.accepting_binding_ids),
        )
        for sid, stream in serialized.streams.items()
    }
    state.collection_release_states = {
        key: CollectionReleaseState(
            binding_id=release.binding_id,
            stream_id=release.stream_id,
            buffer=[serializer.deserialize(ev) for ev in release.buffer],
            released=release.released,
        )
        for key, release in serialized.collection_release_states.items()
    }

    for step_key, worker_data in serialized.workers.items():
        step_id = StepId.from_str(step_key)
        if step_id not in state.workers:
            continue
        worker = state.workers[step_id]
        # in_progress events are moved to the queue on load; they restart on run.
        worker.queue = [
            _deserialize_event_attempt(attempt, serializer)
            for attempt in [*worker_data.queue, *worker_data.in_progress]
        ]
        worker.collected_events = {
            buffer_id: [serializer.deserialize(ev) for ev in events]
            for buffer_id, events in worker_data.collected_events.items()
        }
        worker.static_collect_events = [
            serializer.deserialize(ev) for ev in worker_data.static_collect_events
        ]
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
                    event=waiter_payload.as_event()
                    if waiter_payload is not None
                    else serializer.deserialize(waiter_data.event),
                    waiting_for_event=_import_event_type(
                        waiter_data.waiting_for_event, serializer
                    ),
                    requirements={},
                    has_requirements=waiter_data.has_requirements,
                    resolved_event=serializer.deserialize(waiter_data.resolved_event)
                    if waiter_data.resolved_event
                    else None,
                    scope_path=tuple(waiter_data.scope_path),
                    collection_release_payload=waiter_payload,
                    work_item_id=waiter_data.work_item_id,
                )
            )

    for key, child_entry in serialized.child_brokers.items():
        child_config = state.config.child_configs.get(child_entry.slot)
        if child_config is None:
            continue
        child_state = BrokerState.from_config(child_config)
        _load_broker_from_serialized(child_state, child_entry.broker, serializer)
        state.children[key] = ChildBroker(
            slot=child_entry.slot,
            state=child_state,
            boundary_scope_path=tuple(child_entry.boundary_scope_path),
            boundary_recovery_counts=dict(child_entry.boundary_recovery_counts),
        )


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


_event_type_adapter = TypeAdapter(SerializableEventType)


def _import_event_type(qualified_name: str, serializer: BaseSerializer) -> type[Event]:
    """Resolve an event class name through the selected serializer."""
    with serializer.validation_context():
        return _event_type_adapter.validate_python(qualified_name)


def _binding_id(
    source_step: StepId,
    target_step: StepId,
    item_types: tuple[type[Event], ...],
    policy: Collect,
) -> str:
    """Build a stable id for one static source-to-collect-step binding.

    The id embeds the *string projection* of each StepId. Bindings are broker-
    local (bare names), so a root binding keeps the exact pre-child id string.
    This id is serialized by value into stream state, so a changed root id would
    break snapshot resume.
    """
    type_names = ",".join(f"{t.__module__}.{t.__qualname__}" for t in item_types)
    card = policy.cardinality
    card_repr = f"Take({card.n})" if isinstance(card, Take) else type(card).__name__
    return f"{source_step}->{target_step}:{type_names}:{card_repr}:nearest"


def _compute_collection_bindings(
    steps: dict[str, StepConfig],
) -> dict[str, CollectionBinding]:
    """Compute static list[E] stream bindings for one broker's own steps.

    A fan-out source binds to a collection step when the collection step's item
    type appears at the same stream level reachable from the source. Fed only
    this broker's steps, so a binding can only ever form within one broker;
    source/target are bare (namespace ``()``) StepIds.
    """
    collects: dict[str, tuple[Any, ...]] = {
        name: cfg.collection_param[1]
        for name, cfg in steps.items()
        if cfg.collection_param is not None
    }
    bindings: dict[str, CollectionBinding] = {}
    for source_name, level_types in stream_level_types_by_producer(steps).items():
        for target_name, collect_types in collects.items():
            item_types = tuple(event_types(collect_types))
            if not any(
                step_accepts_type(
                    produced,
                    item_types,
                    allow_subclasses=steps[target_name].accept_event_subclasses,
                )
                for produced in level_types
            ):
                continue
            policy = steps[target_name].collection_policy
            if policy is None:
                continue
            source_step = StepId((), source_name)
            target_step = StepId((), target_name)
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
    """Static configuration for one workflow class (root or child).

    Recurses via ``child_configs`` so the reducer can reach a child's template
    at descent without a ``Workflow`` reference. Never serialized (rebuilt from
    the workflow on load).

    Attributes:
        steps: Broker-local step configs keyed by bare StepId.
        timeout: This broker's own deadline budget in seconds, or None. Root's
            deadline is scheduled by the runner; a child's arms at descent.
        catch_error_handlers: local handler StepId -> descriptor.
        handler_for_step: local covered-step StepId -> its handler StepId.
        collection_bindings: local static list[E] bindings keyed by binding id.
        child_configs: static slot name -> that child class's BrokerConfig.
    """

    steps: dict[StepId, InternalStepConfig]
    timeout: float | None
    catch_error_handlers: dict[StepId, CatchErrorHandler] = field(default_factory=dict)
    handler_for_step: dict[StepId, StepId] = field(default_factory=dict)
    collection_bindings: dict[str, CollectionBinding] = field(default_factory=dict)
    child_configs: dict[str, BrokerConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._normalize_step_id_maps()

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._normalize_step_id_maps()
        object.__setattr__(
            self,
            "collection_bindings",
            {
                binding_id: dataclasses.replace(
                    binding,
                    source_step=_normalize_step_id(binding.source_step),
                    target_step=_normalize_step_id(binding.target_step),
                )
                if not isinstance(binding.source_step, StepId)
                or not isinstance(binding.target_step, StepId)
                else binding
                for binding_id, binding in self.collection_bindings.items()
            },
        )

    def _normalize_step_id_maps(self) -> None:
        object.__setattr__(self, "steps", _normalize_step_id_keys(self.steps))
        object.__setattr__(
            self,
            "catch_error_handlers",
            _normalize_step_id_keys(self.catch_error_handlers),
        )
        object.__setattr__(
            self,
            "handler_for_step",
            {
                _normalize_step_id(step_id): _normalize_step_id(handler_id)
                for step_id, handler_id in self.handler_for_step.items()
            },
        )

    def bindings_for_source(self, source_step: StepId) -> tuple[CollectionBinding, ...]:
        return tuple(
            binding
            for binding in self.collection_bindings.values()
            if binding.source_step == source_step
        )

    def binding_for_target(
        self,
        stream_id: str,
        target_step: StepId,
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

    def child_config_for(self, slot: str) -> BrokerConfig | None:
        return self.child_configs.get(slot)


@dataclass()
class InternalStepConfig:
    """Broker-local worker configuration for a single step.

    Carries everything a worker pool needs at reduction time, so a child broker
    can be minted from its config template alone (no ``Workflow`` reference).

    Attributes:
        accepted_events: Event type classes this step handles.
        retry_policy: Retry policy, or None.
        num_workers: Max concurrent executions.
        accept_event_subclasses: Subclass-aware routing.
        collection_param: list[E] collect binding descriptor, if any.
        collect_params: static multi-parameter fan-in descriptor, if any.
        collection_policy: Collect policy for a list[E] collect step.
    """

    accepted_events: list[Any]
    retry_policy: RetryPolicy | None
    num_workers: int
    accept_event_subclasses: bool = False
    collection_param: tuple[str, tuple[Any, ...]] | None = None
    collect_params: list[tuple[str, Any]] | None = None
    collection_policy: Collect | None = None


@dataclass()
class EventAttempt:
    """
    Represents an event that is being or will be processed by a step.

    Tracks retry information for events that have failed and are being retried.
    Lives inside one broker, so it needs no invocation namespace of its own.

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

    def _copy(self) -> EventAttempt:
        return EventAttempt(
            event=self.event,
            bound_events=dict(self.bound_events) if self.bound_events else None,
            attempts=self.attempts,
            first_attempt_at=self.first_attempt_at,
            last_exception=self.last_exception,
            last_failed_at=self.last_failed_at,
            recovery_counts=dict(self.recovery_counts),
            not_before=self.not_before,
            scope_path=self.scope_path,
            collection_release_payload=self.collection_release_payload,
            work_item_id=self.work_item_id,
        )


@dataclass()
class InternalStepWorkerState:
    """
    Runtime state for a single step's worker pool, local to one broker.

    Attributes:
        queue: Events waiting to be processed by this step
        config: Step configuration (retry policy, num_workers, collect specs)
        in_progress: Currently executing workers for this step
        collected_events: Events being collected via ctx.collect_events(), keyed by buffer_id
        collected_waiters: Active waiters created by ctx.wait_for_event()
        static_collect_events: Pending static multi-parameter fan-in events
    """

    queue: list[EventAttempt]
    config: InternalStepConfig
    in_progress: list[InProgressState]
    collected_events: dict[str, list[Event]]
    collected_waiters: list[StepWorkerWaiter]
    static_collect_events: list[Event] = field(default_factory=list)

    def _deepcopy(self) -> InternalStepWorkerState:
        return InternalStepWorkerState(
            queue=[x._copy() for x in self.queue],
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

    Lives inside one broker, so it needs no invocation namespace of its own.
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
