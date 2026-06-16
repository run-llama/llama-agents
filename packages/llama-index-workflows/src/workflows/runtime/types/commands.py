# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""
Commands returned by the control loop's tick reducer.

The control loop follows a reducer pattern:
  1. Wait for a tick (event, step result, timeout, etc.)
  2. Reduce the tick with current state -> (new_state, commands)
  3. Execute commands (which may spawn async tasks or queue new ticks)
  4. Repeat

Commands represent imperative actions to take after processing a tick,
such as starting workers, queuing events, or completing the workflow.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from workflows.events import Event, StopEvent
from workflows.runtime.types.step_id import StepId


@dataclass(frozen=True)
class CommandRunWorker:
    step_id: StepId
    event: Event
    id: int
    bound_events: dict[str, Event] | None = None


@dataclass(frozen=True)
class CommandQueueEvent:
    event: Event
    step_id: StepId | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    scope_path: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CommandHalt:
    exception: Exception


@dataclass(frozen=True)
class CommandCompleteRun:
    result: StopEvent


@dataclass(frozen=True)
class CommandFailWorkflow:
    step_id: StepId
    exception: Exception


@dataclass(frozen=True)
class CommandPublishEvent:
    event: Event


@dataclass(frozen=True)
class CommandScheduleWaiterTimeout:
    step_id: StepId
    waiter_id: str
    timeout: float


@dataclass(frozen=True)
class CommandScheduleWakeup:
    """Schedule a contentless TickWakeup at an absolute adapter-get_now time.

    Emitted when a delayed retry is re-queued with a future ``not_before``.
    The wakeup carries no payload; it just prompts the loop to re-scan the
    queues for newly eligible attempts.
    """

    at_time: float


@dataclass(frozen=True)
class CommandScheduleIdleCheck:
    """Schedule a deferred idle check via TickIdleCheck.

    Returned by the reducer when state looks quiescent after processing a tick.
    The runner appends a TickIdleCheck to tick_buffer so that idle is confirmed
    on the next loop iteration, after an asyncio.sleep(0) yield gives in-flight
    ctx.send_event() calls a chance to deliver.
    """

    pass


WorkflowCommand = (
    CommandRunWorker
    | CommandQueueEvent
    | CommandHalt
    | CommandCompleteRun
    | CommandFailWorkflow
    | CommandPublishEvent
    | CommandScheduleIdleCheck
    | CommandScheduleWaiterTimeout
    | CommandScheduleWakeup
)


def indicates_exit(command: WorkflowCommand) -> bool:
    return (
        isinstance(command, CommandCompleteRun)
        or isinstance(command, CommandFailWorkflow)
        or isinstance(command, CommandHalt)
    )
