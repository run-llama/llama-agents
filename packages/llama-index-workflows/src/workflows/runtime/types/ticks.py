# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
Ticks (events) that drive the control loop.

The control loop waits for ticks to arrive, then processes them through a reducer
to produce updated state and commands. Ticks represent all the different kinds of
events that can occur during workflow execution:
  - New events added to the workflow
  - Step function execution completing
  - Timeout occurring
  - User cancellation
  - External event publishing requests
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    TypeAdapter,
)
from workflows.events import SerializableEvent, SerializableOptionalException
from workflows.runtime.types.results import (
    SerializableCollectionReleasePayload,
    StepFunctionResult,
)
from workflows.runtime.types.step_id import StepId

# Pre-StepId journals serialized this field as ``step_name``; accept both so
# persisted ticks still deserialize. New ticks serialize under ``step_id``.
_STEP_ID_ALIAS = AliasChoices("step_id", "step_name")


class TickStepResult(BaseModel):
    """When processed, executes a step function and publishes the result"""

    model_config = ConfigDict(
        frozen=True, arbitrary_types_allowed=True, populate_by_name=True
    )
    type: Literal["step_result"] = "step_result"
    step_id: StepId = Field(validation_alias=_STEP_ID_ALIAS)
    worker_id: int
    event: SerializableEvent
    result: list[Annotated[StepFunctionResult, Discriminator("type")]]


class TickAddEvent(BaseModel):
    """When sent, adds an event to the workflow's event queue"""

    model_config = ConfigDict(
        frozen=True, arbitrary_types_allowed=True, populate_by_name=True
    )
    type: Literal["add_event"] = "add_event"
    event: SerializableEvent
    step_id: StepId | None = Field(default=None, validation_alias=_STEP_ID_ALIAS)
    bound_events: dict[str, SerializableEvent] | None = None
    # Namespace of the step (or boundary) that emitted this event. Type-routing
    # is scoped to this namespace so events emitted inside a child stay in the
    # child; ``()`` (the default) is the root namespace, preserving the
    # pre-child wire format for old journals.
    origin_namespace: tuple[str, ...] = ()
    attempts: int | None = None
    first_attempt_at: float | None = None
    last_exception: SerializableOptionalException = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = Field(default_factory=dict)
    scope_path: tuple[str, ...] = Field(default_factory=tuple)
    # Collect-invocation work record. A payload-carrying tick is routed
    # directly to the binding's target step, before waiter matching and the
    # member-arrival path.
    collection_release_payload: SerializableCollectionReleasePayload = None


class TickCancelRun(BaseModel):
    """When processed, cancels the workflow run"""

    model_config = ConfigDict(frozen=True)
    type: Literal["cancel_run"] = "cancel_run"


class TickIdleRelease(BaseModel):
    """When processed, cleanly releases the workflow due to idleness"""

    model_config = ConfigDict(frozen=True)
    type: Literal["idle_release"] = "idle_release"


class TickPublishEvent(BaseModel):
    """When sent, publishes an event to workflow consumers, e.g. a UI or a callback"""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["publish_event"] = "publish_event"
    event: SerializableEvent


class TickTimeout(BaseModel):
    """When processed, times the workflow out, cancelling it"""

    model_config = ConfigDict(frozen=True)
    type: Literal["timeout"] = "timeout"
    timeout: float


class TickWaiterTimeout(BaseModel):
    """When processed, marks a specific waiter as timed out and replays the step."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)
    type: Literal["waiter_timeout"] = "waiter_timeout"
    step_id: StepId = Field(validation_alias=_STEP_ID_ALIAS)
    waiter_id: str


class TickNamespaceTimeout(BaseModel):
    """When processed, times out a single child namespace (not the whole run).

    Scheduled when the first event routes into a child namespace that declares a
    ``timeout``. On fire it expires *that child* through the namespaced failure
    path (so the child's ``@catch_error`` can recover it); only the root timeout
    (:class:`TickTimeout`) halts the whole run. ``started_at`` pins the activation
    this tick was scheduled for: if the namespace has since completed or been
    re-armed, the tick is a stale no-op.
    """

    model_config = ConfigDict(frozen=True)
    type: Literal["namespace_timeout"] = "namespace_timeout"
    namespace: tuple[str, ...]
    timeout: float
    started_at: float


class TickIdleCheck(BaseModel):
    """Scheduled after state appears idle, to re-check after async sends run.

    Appended to tick_buffer when the reducer sees quiescent state. Processed
    on the next loop iteration after asyncio.sleep(0), giving in-flight
    ctx.send_event() calls a chance to deliver.
    """

    model_config = ConfigDict(frozen=True)
    type: Literal["idle_check"] = "idle_check"


class TickWakeup(BaseModel):
    """Poke that flips due delayed attempts to eligible and re-checks queues.

    ``due`` is the absolute adapter-get_now time this wakeup was scheduled
    for (a delayed retry's ``not_before``). Reducing the tick clears
    ``not_before`` on queued attempts that are due. Eligibility is derived
    from the journaled tick, never from the current clock, so replaying a
    tick stream makes the same dispatch decisions as the live run.

    Carries no work-item data: the queued attempt lives in BrokerState, so
    dropping a wakeup can never lose work — resume re-arms it from the queue.
    """

    model_config = ConfigDict(frozen=True)
    type: Literal["wakeup"] = "wakeup"
    due: float


WorkflowTick = Annotated[
    TickStepResult
    | TickAddEvent
    | TickCancelRun
    | TickPublishEvent
    | TickTimeout
    | TickWaiterTimeout
    | TickNamespaceTimeout
    | TickIdleCheck
    | TickIdleRelease
    | TickWakeup,
    Discriminator("type"),
]

WorkflowTickAdapter: TypeAdapter[WorkflowTick] = TypeAdapter(WorkflowTick)
