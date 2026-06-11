# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import dataclasses
import importlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from workflows.context.context_types import (
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
                    not_before=attempt.not_before,
                )
                for attempt in worker_state.queue
            ]

            # Serialize in-progress events (just the events, retry info tracked separately)
            in_progress = [
                serializer.serialize(ip.event) for ip in worker_state.in_progress
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
            version=1,
            state={},  # State is filled separately by the state store
            is_running=self.is_running,
            workers=workers_dict,
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
                    not_before=attempt.not_before,
                )
                for attempt in worker_data.queue
            ]

            # in_progress events are moved to the queue on deserialization
            # They will be restarted when the workflow runs
            for event_str in worker_data.in_progress:
                worker.queue.append(
                    EventAttempt(
                        event=serializer.deserialize(event_str),
                        attempts=0,
                        first_attempt_at=None,
                    )
                )

            # Restore collected events
            worker.collected_events = {
                buffer_id: [serializer.deserialize(ev) for ev in events]
                for buffer_id, events in worker_data.collected_events.items()
            }

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
    """

    event: Event
    attempts: int | None = None
    first_attempt_at: float | None = None
    last_exception: Exception | None = None
    last_failed_at: float | None = None
    recovery_counts: dict[str, int] = field(default_factory=dict)
    not_before: float | None = None


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
        )
