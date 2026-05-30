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

from pydantic import BaseModel, ConfigDict, Discriminator, Field, TypeAdapter
from workflows.events import SerializableEvent, SerializableOptionalException
from workflows.runtime.types.results import StepFunctionResult


class TickStepResult(BaseModel):
    """When processed, executes a step function and publishes the result"""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["step_result"] = "step_result"
    step_name: str
    worker_id: int
    event: SerializableEvent
    result: list[Annotated[StepFunctionResult, Discriminator("type")]]


class TickAddEvent(BaseModel):
    """When sent, adds an event to the workflow's event queue"""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["add_event"] = "add_event"
    event: SerializableEvent
    step_name: str | None = None
    attempts: int | None = None
    first_attempt_at: float | None = None
    last_exception: SerializableOptionalException = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = Field(default_factory=dict)
    # Batch lineage stack (innermost batch id last). Empty for events outside any
    # fan-out batch. This is hidden wrapper-layer metadata, not user payload — the
    # ``Event`` model itself stays pure. A fan-out step pushes a fresh id; a
    # collect step pops the targeted id. 1:1 steps inherit their trigger's stack
    # verbatim. (Isomorphic to OpenTelemetry trace/span/parent-span nesting.)
    batch_stack: tuple[str, ...] = Field(default_factory=tuple)
    # Live-set accounting (L2): True only for reducer-emitted events (set via
    # CommandQueueEvent), whose birth into their batch was already counted at the
    # producing step's resolve. False (the default) for directly-constructed
    # ticks — ``ctx.send_event``, the initial StartEvent, rehydration — whose
    # birth is counted at routing instead.
    batch_counted: bool = False


class TickBatchClosed(BaseModel):
    """Marks a fan-out batch as fully emitted.

    Emitted after a fan-out step (``list[E]`` return) exhausts its emissions.
    Collect-mode steps keyed on ``batch_id`` fire once
    when they observe this tick. ``batch_stack`` is the *trigger* stack of the
    fan-out step (i.e. the stack the closing batch id was pushed onto), so a
    collect step's outputs inherit it after popping the closed id.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    type: Literal["batch_closed"] = "batch_closed"
    batch_id: str
    step_name: str
    batch_stack: tuple[str, ...] = Field(default_factory=tuple)


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

    model_config = ConfigDict(frozen=True)
    type: Literal["waiter_timeout"] = "waiter_timeout"
    step_name: str
    waiter_id: str


class TickIdleCheck(BaseModel):
    """Scheduled after state appears idle, to re-check after async events drain.

    Appended to tick_buffer when the reducer sees quiescent state. Processed
    on the next loop iteration after asyncio.sleep(0), giving in-flight
    ctx.send_event() calls a chance to deliver via the pull task.
    """

    model_config = ConfigDict(frozen=True)
    type: Literal["idle_check"] = "idle_check"


WorkflowTick = Annotated[
    TickStepResult
    | TickAddEvent
    | TickBatchClosed
    | TickCancelRun
    | TickPublishEvent
    | TickTimeout
    | TickWaiterTimeout
    | TickIdleCheck
    | TickIdleRelease,
    Discriminator("type"),
]

WorkflowTickAdapter: TypeAdapter[WorkflowTick] = TypeAdapter(WorkflowTick)
