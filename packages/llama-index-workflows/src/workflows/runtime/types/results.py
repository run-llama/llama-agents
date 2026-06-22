# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import dataclasses
import weakref
from contextvars import ContextVar
from dataclasses import dataclass
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    TypeVar,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    PlainSerializer,
    PlainValidator,
    TypeAdapter,
    model_serializer,
    model_validator,
)
from workflows.events import (
    CollectionReleaseEvent,
    Event,
    SerializableEvent,
    SerializableEventType,
    SerializableException,
    SerializableOptionalEvent,
)

EventType = TypeVar("EventType", bound=Event)

#################################################################
# State Passed to step functions and returned by step functions #
#################################################################


@dataclass(frozen=True)
class RetryAttempt:
    """Per-invocation state handed to a step worker for the currently-processed event.

    Bundles the counters the runtime needs to surface via ``Context.retry_info()``
    and to reconstruct :class:`workflows.retry_policy.RetryInfo`. ``retry_number``
    is 0-based (0 = first run, 1 = first retry). ``last_exception`` /
    ``last_failed_at`` are ``None`` on the first attempt. ``recovery_counts``
    carries the per-``@catch_error``-handler invocation counts on the running
    event's lineage so nested failures and ``ctx.send_event`` emissions route to
    the same handlers.
    """

    retry_number: int = 0
    first_attempt_at: float = 0.0
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = dataclasses.field(default_factory=dict)


@dataclass(frozen=True)
class StepWorkerContext:
    """
    Base state passed to step functions and returned by step functions.
    """

    # event currently being processed by this step invocation
    event: Event
    # immutable state of the step events at start of the step function execution
    state: StepWorkerState
    # add commands here to mutate the internal worker state after step execution
    returns: Returns
    retry: RetryAttempt = dataclasses.field(default_factory=RetryAttempt)


@dataclass(frozen=True)
class StepWorkerState:
    """
    State passed to step functions and returned by step functions.
    """

    step_name: str
    collected_events: dict[str, list[Event]]
    collected_waiters: list[StepWorkerWaiter]
    collection_release_payload: CollectionReleasePayload | None = None
    scope_path: tuple[str, ...] = ()
    work_item_id: str | None = None

    def _deepcopy(self) -> StepWorkerState:
        return StepWorkerState(
            step_name=self.step_name,
            collected_events={k: list(v) for k, v in self.collected_events.items()},
            collected_waiters=[dataclasses.replace(x) for x in self.collected_waiters],
            collection_release_payload=self.collection_release_payload._copy()
            if self.collection_release_payload is not None
            else None,
            scope_path=self.scope_path,
            work_item_id=self.work_item_id,
        )


@dataclass(frozen=True)
class CollectionReleasePayload:
    """List payload supplied to a collection step invocation."""

    binding_id: str
    stream_id: str
    events: list[Event]
    output_scope_path: tuple[str, ...]

    def _copy(self) -> CollectionReleasePayload:
        return CollectionReleasePayload(
            binding_id=self.binding_id,
            stream_id=self.stream_id,
            events=list(self.events),
            output_scope_path=self.output_scope_path,
        )

    def as_event(self) -> CollectionReleaseEvent:
        """Derive the invocation trigger event for this release.

        The payload is the authoritative work record; the event is rebuilt from
        it at every (re)delivery so the two cannot diverge across retries,
        waiter resumes, or serialize/resume.
        """
        return CollectionReleaseEvent(
            events=list(self.events),
            stream_id=self.stream_id,
            binding_id=self.binding_id,
        )

    def work_item_id(self) -> str:
        """Stable work item id for this collect invocation.

        Collect invocations are fired directly (not via a routed tick), so the
        monotonic work-item counter never sees them. Their stream+binding key is
        already unique and carried on the payload across serialize/resume, so use
        it as the work item id. This keeps two collect invocations of the same
        step distinct and lets a suspended collect invocation recreate the same
        implicit waiter id on resume.
        """
        return f"work_item_collect_{self.stream_id}:{self.binding_id}"


# Tick wire format for the payload's member events: the same SerializableEvent
# codec every other tick/result event field uses (events.py).
_payload_events_adapter: TypeAdapter[list[Event]] = TypeAdapter(list[SerializableEvent])


def _serialize_release_payload_value(
    payload: CollectionReleasePayload | None,
) -> Any:
    if payload is None:
        return None
    return {
        "binding_id": payload.binding_id,
        "stream_id": payload.stream_id,
        "events": _payload_events_adapter.dump_python(payload.events),
        "output_scope_path": list(payload.output_scope_path),
    }


def _deserialize_release_payload_value(data: Any) -> CollectionReleasePayload | None:
    if data is None or isinstance(data, CollectionReleasePayload):
        return data
    return CollectionReleasePayload(
        binding_id=data["binding_id"],
        stream_id=data["stream_id"],
        events=_payload_events_adapter.validate_python(data["events"]),
        output_scope_path=tuple(data["output_scope_path"]),
    )


SerializableCollectionReleasePayload = Annotated[
    CollectionReleasePayload | None,
    PlainSerializer(_serialize_release_payload_value, return_type=Any),
    PlainValidator(_deserialize_release_payload_value),
]


@dataclass()
class StepWorkerWaiter(Generic[EventType]):
    """
    Any current waiters for events that are or are not resolved. Upon resolution, step should provide a delete waiter command.
    """

    # the waiter id
    waiter_id: str
    # original event to replay once the condition is met
    event: Event
    # the type of event that is being waited for
    waiting_for_event: type[EventType]
    # the requirements for the waiting event to consider it met
    requirements: dict[str, Any]
    # requirements are not required to be serializable. Flag used during deserialization to re-ping the step function for the requirements
    has_requirements: bool
    # set to true when the waiting event has been resolved, such that the step can retrieve it
    resolved_event: EventType | None
    # pre-bound fan-in parameters from the original worker invocation
    bound_events: dict[str, Event] | None = None
    # set to true when the waiter has timed out, such that the step raises asyncio.TimeoutError
    timed_out: bool = False
    # Originating work record: stream scope of the suspended work item, restored
    # whole on resume so the resumed attempt still closes its stream.
    scope_path: tuple[str, ...] = ()
    # For a suspended collect invocation, the release batch to re-invoke with.
    collection_release_payload: CollectionReleasePayload | None = None
    # Stable identity for the suspended work item. Used to rebuild implicit
    # waiter IDs across retries, serialization, and resume.
    work_item_id: str | None = None


@dataclass()
class Returns:
    """
    Mutate to add return values to the step function. These are only executed after the
    step function has completed (including errors!)
    """

    return_values: list[StepFunctionResult]


class WaitingForEvent(Exception, Generic[EventType]):
    """
    Raised when a step function is called, waiting for an event, but the event is not yet available.
    Handled by the step worker to instead add a waiter rather than failing. Step is retried with the original event
    once the waiting event is available.
    """

    def __init__(self, add: AddWaiter[EventType]):
        self.add = add
        super().__init__(f"Waiting for event {add.event_type}")

    add: AddWaiter[EventType]


StepWorkerStateContextVar = ContextVar[StepWorkerContext]("step_worker")

# Holds a weakref to the Context (in internal-face state) for the currently
# executing step.  A weakref is used so that asyncio timer-handle context
# snapshots do not pin the Workflow in memory (see RunContextContainer for
# the analogous fix at the run level).  The strong reference lives as a local
# variable in as_step_worker_function(); the weakref here is only a lookup handle.
InternalContextVar: ContextVar[weakref.ref[Any]] = ContextVar("internal_context")


###################################
# Data returned by step functions #
###################################


class StepWorkerResult(BaseModel):
    """Returned after a step function has been successfully executed."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["result"] = "result"
    result: SerializableOptionalEvent = None
    # True when this execution actually returned a list (stream emission). A
    # fan-out-annotated step that takes a non-list branch (None, or a declared
    # bare union member) does not fan out: no stream is minted and downstream
    # joins do not fire. An empty list return carries fanned_out=True with
    # result=None — an empty stream whose joins fire with [].
    fanned_out: bool = False


class RetryDecision(BaseModel):
    """The retry policy's verdict for a failure, recorded at failure time.

    ``delay`` is seconds to wait before the next attempt, or None to stop
    retrying. Journaling the decision inside the failure tick makes replay
    independent of retry policy code: a policy whose parameters changed (or
    whose jitter ignores the seed) between the live run and a replay would
    otherwise recompute a different delay than the journaled TickWakeup.due,
    leaving the queued attempt permanently ineligible.
    """

    model_config = ConfigDict(frozen=True)
    delay: float | None = None


class StepWorkerFailed(BaseModel):
    """Returned after a step function has failed."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["failed"] = "failed"
    exception: SerializableException
    failed_at: float
    # None on ticks journaled before decisions were recorded; the reducer
    # falls back to recomputing via the policy (seeded jitter) for those.
    retry_decision: RetryDecision | None = None
    # When this event was first dispatched, recorded at failure time. State
    # rebuilds re-stamp dispatch times with the rebuild clock, so without
    # this the elapsed-time budget of stop conditions silently restarts on
    # every resume. None on ticks journaled before it was recorded; the
    # reducer falls back to the (possibly rebuilt) state value.
    first_attempt_at: float | None = None


class DeleteWaiter(BaseModel):
    """Returned after a waiter condition has been successfully resolved."""

    model_config = ConfigDict(frozen=True)
    type: Literal["delete_waiter"] = "delete_waiter"
    waiter_id: str


class DeleteCollectedEvent(BaseModel):
    """Returned after a collected event has been successfully resolved."""

    model_config = ConfigDict(frozen=True)
    type: Literal["delete_collected"] = "delete_collected"
    event_id: str


class AddCollectedEvent(BaseModel):
    """Returned after a collected event has been added, and is not yet resolved."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["add_collected"] = "add_collected"
    event_id: str
    event: SerializableEvent


class AddWaiter(BaseModel, Generic[EventType]):
    """Returned after a waiter has been added, and is not yet resolved."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["add_waiter"] = "add_waiter"
    waiter_id: str
    waiter_event: SerializableOptionalEvent = None
    requirements: dict[str, Any] = {}
    timeout: float | None = None
    event_type: SerializableEventType
    has_requirements: bool = False

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        # Always serialize requirements as {} and record whether they existed
        data["has_requirements"] = bool(self.requirements)
        data["requirements"] = {}
        return data

    @model_validator(mode="wrap")  # type: ignore[ty:invalid-argument-type]
    @classmethod
    def _validate(cls, data: Any, handler: Any) -> AddWaiter:
        if isinstance(data, dict):
            # Strip has_requirements before validation (it's computed)
            data = dict(data)
            data.pop("has_requirements", None)
        return handler(data)


# A step function result "command" communicates back to the workflow how the step function was resolved
# e.g. are we collecting events, waiting for an event, or just returning a result?
StepFunctionResult = (
    StepWorkerResult
    | StepWorkerFailed
    | AddCollectedEvent
    | DeleteCollectedEvent
    | AddWaiter[Event]
    | DeleteWaiter
)
