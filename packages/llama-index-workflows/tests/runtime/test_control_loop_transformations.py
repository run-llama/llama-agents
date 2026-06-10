# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
Unit tests for control loop transformation functions.

These tests focus on the pure transformation functions in the control loop,
testing them in isolation without running the full async control loop.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from typing import cast

import pytest
from workflows.decorators import StepConfig
from workflows.errors import WorkflowTimeoutError
from workflows.events import (
    Event,
    IdleReleasedEvent,
    InputRequiredEvent,
    StartEvent,
    StepState,
    StepStateChanged,
    StopEvent,
    UnhandledEvent,
    WorkflowIdleEvent,
)
from workflows.retry_policy import (
    ConstantDelayRetryPolicy,
    ExponentialBackoffRetryPolicy,
)
from workflows.runtime.control_loop import (
    _add_or_enqueue_event,
    _check_idle_state,
    _process_add_event_tick,
    _process_cancel_run_tick,
    _process_publish_event_tick,
    _process_step_result_tick,
    _process_timeout_tick,
    _reduce_tick,
    rebuild_state_from_ticks,
    rebuild_state_from_ticks_stream,
    replay_ticks_stream,
    rewind_in_progress,
)
from workflows.runtime.types.commands import (
    CommandCompleteRun,
    CommandFailWorkflow,
    CommandHalt,
    CommandPublishEvent,
    CommandQueueEvent,
    CommandRunWorker,
    CommandScheduleWakeup,
)
from workflows.runtime.types.internal_state import (
    BrokerConfig,
    BrokerState,
    EventAttempt,
    InProgressState,
    InternalStepConfig,
    InternalStepWorkerState,
)
from workflows.runtime.types.results import (
    AddCollectedEvent,
    AddWaiter,
    DeleteCollectedEvent,
    DeleteWaiter,
    StepFunctionResult,
    StepWorkerFailed,
    StepWorkerResult,
    StepWorkerState,
    StepWorkerWaiter,
)
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickIdleRelease,
    TickPublishEvent,
    TickStepResult,
    TickTimeout,
    TickWakeup,
    WorkflowTick,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


class MyTestEvent(Event):
    value: int


class OtherEvent(Event):
    data: str


@pytest.fixture
def base_state() -> BrokerState:
    """Create a minimal BrokerState for testing."""
    step_config = StepConfig(
        accepted_events=[MyTestEvent, StartEvent],
        event_name="ev",
        return_types=[StopEvent, OtherEvent, type(None)],
        context_parameter="ctx",
        retry_policy=None,
        num_workers=1,
        resources=[],
    )
    return BrokerState(
        is_running=True,
        config=BrokerConfig(
            steps={
                "test_step": InternalStepConfig(
                    accepted_events=[MyTestEvent, StartEvent],
                    retry_policy=None,
                    num_workers=1,
                )
            },
            timeout=None,
        ),
        workers={
            "test_step": InternalStepWorkerState(
                queue=[],
                config=step_config,
                in_progress=[],
                collected_events={},
                collected_waiters=[],
            )
        },
    )


def add_worker(state: BrokerState, event: Event, worker_id: int = 0) -> None:
    """Helper to add an in-progress worker to state."""
    state.workers["test_step"].in_progress.append(
        InProgressState(
            event=event,
            worker_id=worker_id,
            shared_state=StepWorkerState(
                step_name="test_step",
                collected_events={},
                collected_waiters=[],
            ),
            attempts=0,
            first_attempt_at=100.0,
        )
    )


def test_add_event_unhandled_emits_internal_event(base_state: BrokerState) -> None:
    """Unhandled events should emit UnhandledEvent with idle status."""
    tick = TickAddEvent(event=OtherEvent(data="unused"), step_name=None)
    state, commands = _process_add_event_tick(tick, base_state, now_seconds=0.0)

    publish_events = [c.event for c in commands if isinstance(c, CommandPublishEvent)]
    unhandled = [e for e in publish_events if isinstance(e, UnhandledEvent)]
    assert len(unhandled) == 1
    assert unhandled[0].event_type == "OtherEvent"
    assert unhandled[0].qualified_name.endswith(".OtherEvent")
    assert unhandled[0].step_name is None
    assert unhandled[0].idle == _check_idle_state(state)


class CustomInputRequired(InputRequiredEvent):
    """Custom InputRequiredEvent subclass for testing."""

    prompt: str


def test_add_event_input_required_does_not_emit_unhandled(
    base_state: BrokerState,
) -> None:
    """InputRequiredEvent subclasses should NOT emit UnhandledEvent.

    InputRequiredEvent events are designed to be handled externally by human
    consumers, not by workflow steps. They should not trigger UnhandledEvent.
    """
    tick = TickAddEvent(event=CustomInputRequired(prompt="test"), step_name=None)
    _, commands = _process_add_event_tick(tick, base_state, now_seconds=0.0)

    publish_events = [c.event for c in commands if isinstance(c, CommandPublishEvent)]
    unhandled = [e for e in publish_events if isinstance(e, UnhandledEvent)]
    assert len(unhandled) == 0


def test_add_event_base_input_required_does_not_emit_unhandled(
    base_state: BrokerState,
) -> None:
    """Base InputRequiredEvent should also NOT emit UnhandledEvent."""
    tick = TickAddEvent(event=InputRequiredEvent(), step_name=None)
    _, commands = _process_add_event_tick(tick, base_state, now_seconds=0.0)

    publish_events = [c.event for c in commands if isinstance(c, CommandPublishEvent)]
    unhandled = [e for e in publish_events if isinstance(e, UnhandledEvent)]
    assert len(unhandled) == 0


def test_add_event_matches_waiter_does_not_emit_unhandled(
    base_state: BrokerState,
) -> None:
    """Events that satisfy a waiter should not emit UnhandledEvent."""
    base_state.workers["test_step"].collected_waiters.append(
        StepWorkerWaiter(
            waiter_id="waiter-1",
            event=StartEvent(),
            waiting_for_event=OtherEvent,
            requirements={},
            has_requirements=False,
            resolved_event=None,
        )
    )
    tick = TickAddEvent(event=OtherEvent(data="hit"), step_name=None)
    _, commands = _process_add_event_tick(tick, base_state, now_seconds=0.0)

    publish_events = [c.event for c in commands if isinstance(c, CommandPublishEvent)]
    assert not any(isinstance(e, UnhandledEvent) for e in publish_events)


@pytest.mark.parametrize(
    "result,expected_commands",
    [
        (StopEvent(result="done"), [StepStateChanged, StopEvent, CommandCompleteRun]),
        (OtherEvent(data="next"), [StepStateChanged, CommandQueueEvent]),
        (
            InputRequiredEvent(),
            [StepStateChanged, InputRequiredEvent, CommandQueueEvent],
        ),
        (None, [StepStateChanged]),
    ],
)
def test_step_worker_results(
    base_state: BrokerState, result: Event | None, expected_commands: list
) -> None:
    """Test different step worker result types."""
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    tick = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerResult(result=result)],
    )

    new_state, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    # Check expected command types
    for i, expected_type in enumerate(expected_commands):
        if isinstance(expected_type, type) and issubclass(expected_type, Event):
            command = commands[i]
            assert isinstance(command, CommandPublishEvent)
            assert isinstance(command.event, expected_type)
        else:
            assert isinstance(commands[i], expected_type)

    # Worker should be removed from in_progress
    assert len(new_state.workers["test_step"].in_progress) == 0


def test_step_worker_failed_with_retry(base_state: BrokerState) -> None:
    """Test that failures with retry policy re-queue a retry in state."""
    base_state.workers["test_step"].config.retry_policy = ConstantDelayRetryPolicy(
        maximum_attempts=3, delay=1.0
    )
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerFailed(exception=ValueError("test"), failed_at=110.0)],
    )

    new_state, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    # The retry lives in the worker queue with its eligibility time
    queue = new_state.workers["test_step"].queue
    assert len(queue) == 1
    assert queue[0].attempts == 1
    assert queue[0].not_before == 111.0
    # A contentless wakeup is scheduled at the eligibility time
    wakeups = [c for c in commands if isinstance(c, CommandScheduleWakeup)]
    assert len(wakeups) == 1
    assert wakeups[0].at_time == 111.0

    # First command should be NOT_RUNNING state change before re-queue
    assert isinstance(commands[0], CommandPublishEvent)
    assert isinstance(commands[0].event, StepStateChanged)
    assert commands[0].event.step_state == StepState.NOT_RUNNING


def test_step_worker_failed_without_retry(base_state: BrokerState) -> None:
    """Test that failures without retry fail the workflow."""
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerFailed(exception=ValueError("test"), failed_at=110.0)],
    )

    new_state, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    assert new_state.is_running is False
    assert any(isinstance(c, CommandFailWorkflow) for c in commands)


def test_collected_events(base_state: BrokerState) -> None:
    """Test AddCollectedEvent and DeleteCollectedEvent."""
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    # Add event
    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[AddCollectedEvent(event_id="buf1", event=OtherEvent(data="e1"))],
    )
    new_state, _ = _process_step_result_tick(tick, base_state, now_seconds=110.0)
    assert "buf1" in new_state.workers["test_step"].collected_events

    # Delete event
    add_worker(new_state, event)
    tick = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[
            StepWorkerResult(result=StopEvent()),
            DeleteCollectedEvent(event_id="buf1"),
        ],
    )
    new_state, _ = _process_step_result_tick(tick, new_state, now_seconds=110.0)
    assert "buf1" not in new_state.workers["test_step"].collected_events


def test_waiters(base_state: BrokerState) -> None:
    """Test AddWaiter and DeleteWaiter."""
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    result = AddWaiter(
        waiter_id="w1",
        waiter_event=InputRequiredEvent(),
        requirements={},
        timeout=None,
        event_type=OtherEvent,
    )
    # Add waiter
    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[
            cast(StepFunctionResult, result),
        ],
    )
    new_state, _ = _process_step_result_tick(tick, base_state, now_seconds=110.0)
    assert len(new_state.workers["test_step"].collected_waiters) == 1

    # Delete waiter
    add_worker(new_state, event)
    tick = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[
            StepWorkerResult(result=StopEvent()),
            DeleteWaiter(waiter_id="w1"),
        ],
    )
    new_state, _ = _process_step_result_tick(tick, new_state, now_seconds=110.0)
    assert len(new_state.workers["test_step"].collected_waiters) == 0


def test_start_event_sets_running(base_state: BrokerState) -> None:
    """Test that StartEvent sets is_running to True."""
    base_state.is_running = False
    tick = TickAddEvent(event=StartEvent())
    new_state, _ = _process_add_event_tick(tick, base_state, now_seconds=100.0)
    assert new_state.is_running is True


def test_event_routing(base_state: BrokerState) -> None:
    """Test that events are routed to accepting steps."""
    tick = TickAddEvent(event=MyTestEvent(value=42))
    new_state, commands = _process_add_event_tick(tick, base_state, now_seconds=100.0)

    run_cmds = [c for c in commands if isinstance(c, CommandRunWorker)]
    assert len(run_cmds) == 1
    assert run_cmds[0].step_name == "test_step"


def test_per_step_explicit_routing_accepts_only_matching_types(
    base_state: BrokerState,
) -> None:
    """Explicit routing with step_name must still satisfy accepted event types."""
    # base_state only has test_step that accepts MyTestEvent and StartEvent
    # Explicitly target test_step with MyTestEvent → should run
    tick_ok = TickAddEvent(event=MyTestEvent(value=1), step_name="test_step")
    _, cmds_ok = _process_add_event_tick(tick_ok, base_state, now_seconds=100.0)
    assert any(isinstance(c, CommandRunWorker) for c in cmds_ok)

    # Explicitly target an unknown step → should not run anything
    tick_bad = TickAddEvent(event=MyTestEvent(value=1), step_name="unknown")
    _, cmds_bad = _process_add_event_tick(tick_bad, base_state, now_seconds=100.0)
    assert not any(isinstance(c, CommandRunWorker) for c in cmds_bad)


def test_explicit_routing_requires_acceptance(base_state: BrokerState) -> None:
    """Explicit step routing should still require accepted event types."""
    # Add a second step that does NOT accept MyTestEvent
    other_step_cfg = StepConfig(
        accepted_events=[StartEvent],
        event_name="ev",
        return_types=[StopEvent, OtherEvent, type(None)],
        context_parameter="ctx",
        retry_policy=None,
        num_workers=1,
        resources=[],
    )
    base_state.config.steps["other_step"] = InternalStepConfig(
        accepted_events=[StartEvent], retry_policy=None, num_workers=1
    )
    base_state.workers["other_step"] = InternalStepWorkerState(
        queue=[],
        config=other_step_cfg,
        in_progress=[],
        collected_events={},
        collected_waiters=[],
    )

    # Try to route MyTestEvent explicitly to non-accepting step → should not start
    tick = TickAddEvent(event=MyTestEvent(value=1), step_name="other_step")
    _, commands = _process_add_event_tick(tick, base_state, now_seconds=100.0)
    assert not any(
        isinstance(c, CommandRunWorker) and c.step_name == "other_step"
        for c in commands
    )

    # Explicitly route to accepting step → should start
    tick_ok = TickAddEvent(event=MyTestEvent(value=2), step_name="test_step")
    _, commands_ok = _process_add_event_tick(tick_ok, base_state, now_seconds=100.0)
    assert any(
        isinstance(c, CommandRunWorker) and c.step_name == "test_step"
        for c in commands_ok
    )


def test_waiter_resolution(base_state: BrokerState) -> None:
    """Test that events matching waiters trigger step re-execution."""
    original_event = MyTestEvent(value=1)
    waiter = StepWorkerWaiter(
        waiter_id="w1",
        event=original_event,
        waiting_for_event=OtherEvent,
        requirements={"data": "expected"},
        has_requirements=True,
        resolved_event=None,
    )
    base_state.workers["test_step"].collected_waiters.append(waiter)

    tick = TickAddEvent(event=OtherEvent(data="expected"))
    new_state, commands = _process_add_event_tick(tick, base_state, now_seconds=100.0)

    assert (
        new_state.workers["test_step"].collected_waiters[0].resolved_event is not None
    )
    run_cmds = [c for c in commands if isinstance(c, CommandRunWorker)]
    assert any(c.event == original_event for c in run_cmds)


def test_step_state_changed_names(base_state: BrokerState) -> None:
    """Verify input/output event names on StepStateChanged use actual event types."""
    input_ev = MyTestEvent(value=7)
    add_worker(base_state, input_ev)

    # Return a regular Event → output_event_name should be its type, and input_event_name should be str(type(input))
    tick = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=input_ev,
        result=[StepWorkerResult(result=OtherEvent(data="x"))],
    )
    _, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)
    assert isinstance(commands[0], CommandPublishEvent)
    assert isinstance(commands[0].event, StepStateChanged)
    ev = commands[0].event
    assert ev.input_event_name == str(type(input_ev))
    assert ev.output_event_name == str(type(OtherEvent(data="x")))

    # Return StopEvent → output_event_name should be None
    add_worker(base_state, input_ev)
    tick2 = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=input_ev,
        result=[StepWorkerResult(result=StopEvent(result="done"))],
    )
    _, commands2 = _process_step_result_tick(tick2, base_state, now_seconds=110.0)
    assert isinstance(commands2[0], CommandPublishEvent)
    assert isinstance(commands2[0].event, StepStateChanged)
    ev2 = commands2[0].event
    assert ev2.input_event_name == str(type(input_ev))
    assert ev2.output_event_name == "<class 'workflows.events.StopEvent'>"


def test_cancel_run(base_state: BrokerState) -> None:
    """Test that cancel sets not running and halts."""
    tick = TickCancelRun()
    new_state, commands = _process_cancel_run_tick(tick, base_state)

    # This is perhaps unintuitive, but it's important to be able to cancel and resume a workflow
    # based on this state--Workflow uses this as a signal to determine whether to pass or construct
    # a start event
    assert new_state.is_running is True
    assert len(commands) == 2
    assert isinstance(commands[0], CommandPublishEvent)
    assert isinstance(commands[1], CommandHalt)


def test_idle_release(base_state: BrokerState) -> None:
    """Test that idle release returns CommandCompleteRun with IdleReleasedEvent and no published events."""
    tick = TickIdleRelease()
    new_state, commands = _reduce_tick(tick, base_state, 0.0)

    # State is unchanged (returned early without deepcopy)
    assert new_state is base_state
    # Single command: complete run with IdleReleasedEvent
    assert len(commands) == 1
    assert isinstance(commands[0], CommandCompleteRun)
    assert isinstance(commands[0].result, IdleReleasedEvent)
    # No CommandPublishEvent — nothing written to event stream
    assert not any(isinstance(c, CommandPublishEvent) for c in commands)


def test_publish_event(base_state: BrokerState) -> None:
    """Test that publish events pass through without state changes."""
    event = MyTestEvent(value=42)
    tick = TickPublishEvent(event=event)
    new_state, commands = _process_publish_event_tick(tick, base_state)

    assert new_state is base_state
    assert len(commands) == 1
    assert isinstance(commands[0], CommandPublishEvent)


def test_timeout(base_state: BrokerState) -> None:
    """Test that timeout sets not running and halts with error."""
    tick = TickTimeout(timeout=10.0)
    new_state, commands = _process_timeout_tick(tick, base_state)

    assert new_state.is_running is False
    assert isinstance(commands[1], CommandHalt)
    assert isinstance(commands[1].exception, WorkflowTimeoutError)


def test_add_when_capacity_available(base_state: BrokerState) -> None:
    """Test that events start immediately when capacity available."""
    event = MyTestEvent(value=42)
    commands = _add_or_enqueue_event(
        EventAttempt(event=event),
        "test_step",
        base_state.workers["test_step"],
        now_seconds=100.0,
    )

    assert len(base_state.workers["test_step"].in_progress) == 1
    assert any(isinstance(c, CommandRunWorker) for c in commands)
    assert any(
        isinstance(c, CommandPublishEvent)
        and isinstance(c.event, StepStateChanged)
        and c.event.step_state == StepState.RUNNING
        for c in commands
    )


def test_enqueue_when_no_capacity(base_state: BrokerState) -> None:
    """Test that events queue when no capacity available."""
    # Fill capacity
    add_worker(base_state, MyTestEvent(value=1))

    # Try to add another
    event = MyTestEvent(value=42)
    commands = _add_or_enqueue_event(
        EventAttempt(event=event),
        "test_step",
        base_state.workers["test_step"],
        now_seconds=100.0,
    )

    assert len(base_state.workers["test_step"].queue) == 1
    # PREPARING should be published when we enqueue
    assert isinstance(commands[0], CommandPublishEvent)
    assert isinstance(commands[0].event, StepStateChanged)
    assert commands[0].event.step_state == StepState.PREPARING


def test_rewind_restarts_workers(base_state: BrokerState) -> None:
    """Test that in_progress workers are restarted."""
    base_state.workers["test_step"].config.num_workers = 2
    base_state.config.steps["test_step"].num_workers = 2

    add_worker(base_state, MyTestEvent(value=1), worker_id=0)
    add_worker(base_state, MyTestEvent(value=2), worker_id=1)

    new_state, commands = rewind_in_progress(base_state, now_seconds=120.0)

    # Both should be restarted
    run_cmds = [c for c in commands if isinstance(c, CommandRunWorker)]
    assert len(run_cmds) == 2
    assert len(new_state.workers["test_step"].in_progress) == 2


def test_add_event_tick_preserves_retry_metadata(base_state: BrokerState) -> None:
    """Test that attempts and first_attempt_at are preserved from TickAddEvent."""
    now = 200.0
    first_attempt_time = 100.0
    attempts = 3

    tick = TickAddEvent(
        event=MyTestEvent(value=42),
        attempts=attempts,
        first_attempt_at=first_attempt_time,
    )

    new_state, commands = _process_add_event_tick(tick, base_state, now_seconds=now)

    # Verify the worker was started
    run_cmds = [c for c in commands if isinstance(c, CommandRunWorker)]
    assert len(run_cmds) == 1

    # Verify retry metadata was preserved in the InProgressState
    in_progress = new_state.workers["test_step"].in_progress
    assert len(in_progress) == 1
    assert in_progress[0].attempts == attempts
    assert in_progress[0].first_attempt_at == first_attempt_time


def test_add_event_tick_uses_now_when_no_retry_metadata(
    base_state: BrokerState,
) -> None:
    """Test that fresh events get attempts=0 and first_attempt_at=now."""
    now = 200.0

    tick = TickAddEvent(event=MyTestEvent(value=42))  # No retry metadata

    new_state, _ = _process_add_event_tick(tick, base_state, now_seconds=now)

    in_progress = new_state.workers["test_step"].in_progress
    assert len(in_progress) == 1
    assert in_progress[0].attempts == 0
    assert in_progress[0].first_attempt_at == now


def test_step_worker_failed_retry_preserves_delay(base_state: BrokerState) -> None:
    """Test that the re-queued retry carries the delay as an absolute not_before."""
    retry_delay = 5.0
    base_state.workers["test_step"].config.retry_policy = ConstantDelayRetryPolicy(
        maximum_attempts=3, delay=retry_delay
    )
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerFailed(exception=ValueError("test"), failed_at=110.0)],
    )

    new_state, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    queue = new_state.workers["test_step"].queue
    assert len(queue) == 1
    assert queue[0].not_before == 110.0 + retry_delay
    assert queue[0].attempts == 1
    assert queue[0].first_attempt_at == 100.0  # from add_worker fixture
    # The retry must not be dispatched within this reduction
    assert not any(isinstance(c, CommandRunWorker) for c in commands)
    assert len(new_state.workers["test_step"].in_progress) == 0


def test_step_worker_failed_retry_preserves_first_attempt_at(
    base_state: BrokerState,
) -> None:
    """Test that first_attempt_at stays constant across retries."""
    base_state.workers["test_step"].config.retry_policy = ConstantDelayRetryPolicy(
        maximum_attempts=5, delay=1.0
    )
    event = MyTestEvent(value=42)

    original_first_attempt_at = 50.0
    # Simulate a worker that's already been retried twice
    base_state.workers["test_step"].in_progress.append(
        InProgressState(
            event=event,
            worker_id=0,
            shared_state=StepWorkerState(
                step_name="test_step",
                collected_events={},
                collected_waiters=[],
            ),
            attempts=2,  # Already retried twice
            first_attempt_at=original_first_attempt_at,
        )
    )

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerFailed(exception=ValueError("test"), failed_at=200.0)],
    )

    new_state, _ = _process_step_result_tick(tick, base_state, now_seconds=200.0)

    queue = new_state.workers["test_step"].queue
    assert len(queue) == 1
    assert queue[0].attempts == 3  # incremented from 2
    assert queue[0].first_attempt_at == original_first_attempt_at  # preserved!


def test_step_worker_failed_exponential_jitter_deterministic(
    base_state: BrokerState,
) -> None:
    """Retry delay must be identical on two calls with the same run_id (DBOS replay determinism)."""
    policy = ExponentialBackoffRetryPolicy(
        initial_delay=1.0,
        multiplier=2.0,
        max_delay=60.0,
        maximum_attempts=5,
        jitter=True,
    )
    base_state.workers["test_step"].config.retry_policy = policy
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerFailed(exception=ValueError("test"), failed_at=110.0)],
    )

    run_id = "run-determinism-test"
    failures = 1
    jitter_seed = (
        int(
            hashlib.sha256(f"{run_id}:test_step:{failures}".encode()).hexdigest(),
            16,
        )
        & 0xFFFF_FFFF
    )
    expected_delay = policy.next(
        10.0,
        failures,
        ValueError("test"),
        seed=jitter_seed,
    )
    assert expected_delay is not None

    state_first, _ = _process_step_result_tick(
        tick, base_state, now_seconds=110.0, run_id=run_id
    )
    state_second, _ = _process_step_result_tick(
        tick, base_state, now_seconds=110.0, run_id=run_id
    )

    first_not_before = state_first.workers["test_step"].queue[0].not_before
    second_not_before = state_second.workers["test_step"].queue[0].not_before
    assert first_not_before is not None
    assert first_not_before == second_not_before == 110.0 + expected_delay


# =============================================================================
# Delayed Retry (not_before) Tests
# =============================================================================


def test_wakeup_dispatches_eligible_attempt(base_state: BrokerState) -> None:
    """TickWakeup dispatches attempts whose not_before is covered by its due."""
    base_state.workers["test_step"].queue.append(
        EventAttempt(event=MyTestEvent(value=1), attempts=1, not_before=110.0)
    )

    # Wakeup due before eligibility: nothing dispatched
    state_before, commands_before = _reduce_tick(
        TickWakeup(due=105.0), base_state, 105.0
    )
    assert not any(isinstance(c, CommandRunWorker) for c in commands_before)
    assert len(state_before.workers["test_step"].queue) == 1

    # Wakeup due at eligibility: dispatched
    state_after, commands_after = _reduce_tick(TickWakeup(due=110.0), base_state, 110.0)
    run_cmds = [c for c in commands_after if isinstance(c, CommandRunWorker)]
    assert len(run_cmds) == 1
    assert len(state_after.workers["test_step"].queue) == 0
    assert state_after.workers["test_step"].in_progress[0].attempts == 1


def test_wakeup_eligibility_is_due_driven_not_clock_driven(
    base_state: BrokerState,
) -> None:
    """Dispatch decisions derive from the journaled due, not the wall clock.

    Regression: replaying a journal long after the fact must reduce each
    wakeup identically to the live run, even though the replay-time clock is
    far past every not_before.
    """
    base_state.workers["test_step"].queue.append(
        EventAttempt(event=MyTestEvent(value=1), attempts=1, not_before=110.0)
    )

    # A stale wakeup (due=105) replayed when the clock is way past 110 must
    # still NOT dispatch — the live run didn't dispatch on it either.
    state, commands = _reduce_tick(TickWakeup(due=105.0), base_state, 99_999.0)
    assert not any(isinstance(c, CommandRunWorker) for c in commands)
    queue = state.workers["test_step"].queue
    assert len(queue) == 1
    assert queue[0].not_before == 110.0


def test_ineligible_attempt_does_not_block_eligible_behind_it(
    base_state: BrokerState,
) -> None:
    """An ineligible attempt at the queue head must not starve eligible work."""
    worker_state = base_state.workers["test_step"]
    worker_state.queue.append(
        EventAttempt(event=MyTestEvent(value=1), attempts=1, not_before=999.0)
    )
    worker_state.queue.append(EventAttempt(event=MyTestEvent(value=2)))

    state, commands = _reduce_tick(TickWakeup(due=100.0), base_state, 100.0)

    run_cmds = [c for c in commands if isinstance(c, CommandRunWorker)]
    assert len(run_cmds) == 1
    assert isinstance(run_cmds[0].event, MyTestEvent)
    assert run_cmds[0].event.value == 2
    # Delayed attempt remains queued, not consuming the worker slot
    remaining = state.workers["test_step"].queue
    assert len(remaining) == 1
    assert remaining[0].not_before == 999.0


def test_retry_with_zero_delay_dispatches_immediately(base_state: BrokerState) -> None:
    """A zero-delay retry is re-dispatched within the same reduction."""
    base_state.workers["test_step"].config.retry_policy = ConstantDelayRetryPolicy(
        maximum_attempts=3, delay=0
    )
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerFailed(exception=ValueError("test"), failed_at=110.0)],
    )

    new_state, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    run_cmds = [c for c in commands if isinstance(c, CommandRunWorker)]
    assert len(run_cmds) == 1
    assert not any(isinstance(c, CommandScheduleWakeup) for c in commands)
    assert len(new_state.workers["test_step"].queue) == 0
    assert new_state.workers["test_step"].in_progress[0].attempts == 1


def test_rewind_rearms_wakeup_for_delayed_attempt(base_state: BrokerState) -> None:
    """Resume re-arms a wakeup for queued future-not_before attempts."""
    base_state.workers["test_step"].queue.append(
        EventAttempt(event=MyTestEvent(value=1), attempts=2, not_before=150.0)
    )

    new_state, commands = rewind_in_progress(base_state, now_seconds=120.0)

    # Not dispatched, stays queued with retry info intact
    assert not any(isinstance(c, CommandRunWorker) for c in commands)
    queue = new_state.workers["test_step"].queue
    assert len(queue) == 1
    assert queue[0].attempts == 2
    # Poke re-armed at the eligibility time
    wakeups = [c for c in commands if isinstance(c, CommandScheduleWakeup)]
    assert len(wakeups) == 1
    assert wakeups[0].at_time == 150.0


def test_rewind_rearms_wakeup_even_when_past_due(base_state: BrokerState) -> None:
    """A past-due not_before still goes through a wakeup tick, never a direct
    dispatch from rewind.

    The runner fires a past-due wakeup immediately, so delivery is prompt —
    but it arrives as a journaled tick, keeping replay deterministic.
    """
    base_state.workers["test_step"].queue.append(
        EventAttempt(event=MyTestEvent(value=1), attempts=2, not_before=110.0)
    )

    new_state, commands = rewind_in_progress(base_state, now_seconds=120.0)

    assert not any(isinstance(c, CommandRunWorker) for c in commands)
    wakeups = [c for c in commands if isinstance(c, CommandScheduleWakeup)]
    assert len(wakeups) == 1
    assert wakeups[0].at_time == 110.0
    queue = new_state.workers["test_step"].queue
    assert len(queue) == 1
    assert queue[0].attempts == 2

    # The (immediately fired) wakeup then delivers exactly once
    final_state, wake_commands = _reduce_tick(TickWakeup(due=110.0), new_state, 120.0)
    run_cmds = [c for c in wake_commands if isinstance(c, CommandRunWorker)]
    assert len(run_cmds) == 1
    assert len(final_state.workers["test_step"].queue) == 0
    assert final_state.workers["test_step"].in_progress[0].attempts == 2


# =============================================================================
# Idle Workflow Tracking Tests
# =============================================================================


def test_check_idle_state_not_running(base_state: BrokerState) -> None:
    """A workflow that is not running is not idle."""
    base_state.is_running = False
    assert _check_idle_state(base_state) is False


def test_check_idle_state_has_queued_events(base_state: BrokerState) -> None:
    """A workflow with queued events is not idle."""
    base_state.workers["test_step"].queue.append(
        EventAttempt(event=MyTestEvent(value=1))
    )
    assert _check_idle_state(base_state) is False


def test_check_idle_state_delayed_retry_is_pending_work(
    base_state: BrokerState,
) -> None:
    """A queued attempt waiting out a retry delay keeps the workflow non-idle.

    This is what defers idle release (e.g. DBOS) during a retry-delay window.
    """
    base_state.workers["test_step"].queue.append(
        EventAttempt(event=MyTestEvent(value=1), attempts=1, not_before=10_000.0)
    )
    assert _check_idle_state(base_state) is False


def test_check_idle_state_has_in_progress(base_state: BrokerState) -> None:
    """A workflow with in-progress workers is not idle."""
    add_worker(base_state, MyTestEvent(value=1))
    assert _check_idle_state(base_state) is False


def test_check_idle_state_no_work_is_idle(base_state: BrokerState) -> None:
    """A running workflow with empty queues and no in-progress work is idle."""
    assert _check_idle_state(base_state) is True


def test_check_idle_state_is_idle_with_waiter(base_state: BrokerState) -> None:
    """A running workflow with only waiters and no work is idle."""
    waiter = StepWorkerWaiter(
        waiter_id="w1",
        event=MyTestEvent(value=1),
        waiting_for_event=OtherEvent,
        requirements={},
        has_requirements=False,
        resolved_event=None,
    )
    base_state.workers["test_step"].collected_waiters.append(waiter)
    assert _check_idle_state(base_state) is True


def test_step_result_does_not_emit_idle(base_state: BrokerState) -> None:
    """Step result tick never emits WorkflowIdleEvent directly.

    Idle detection is handled at the runner level via TickIdleCheck, not in
    the pure reducer. This test confirms the reducer doesn't emit idle.
    """
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerResult(result=None)],
    )

    new_state, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    idle_commands = [
        c
        for c in commands
        if isinstance(c, CommandPublishEvent) and isinstance(c.event, WorkflowIdleEvent)
    ]
    assert len(idle_commands) == 0
    # State IS idle (no queued work, no in-progress), but emission is the runner's job
    assert _check_idle_state(new_state) is True


def test_check_idle_state_multi_step_not_idle_if_one_has_work(
    base_state: BrokerState,
) -> None:
    """With multiple steps, not idle if any step has work."""
    # Add a second step
    other_step_cfg = StepConfig(
        accepted_events=[OtherEvent],
        event_name="ev",
        return_types=[StopEvent, type(None)],
        context_parameter="ctx",
        retry_policy=None,
        num_workers=1,
        resources=[],
    )
    base_state.config.steps["other_step"] = InternalStepConfig(
        accepted_events=[OtherEvent], retry_policy=None, num_workers=1
    )
    base_state.workers["other_step"] = InternalStepWorkerState(
        queue=[],
        config=other_step_cfg,
        in_progress=[],
        collected_events={},
        collected_waiters=[],
    )

    # Add waiter to test_step (which alone would make it idle)
    waiter = StepWorkerWaiter(
        waiter_id="w1",
        event=MyTestEvent(value=1),
        waiting_for_event=OtherEvent,
        requirements={},
        has_requirements=False,
        resolved_event=None,
    )
    base_state.workers["test_step"].collected_waiters.append(waiter)

    # Without work in other_step, workflow is idle
    assert _check_idle_state(base_state) is True

    # Add in_progress work to other_step - now not idle
    base_state.workers["other_step"].in_progress.append(
        InProgressState(
            event=OtherEvent(data="test"),
            worker_id=0,
            shared_state=StepWorkerState(
                step_name="other_step",
                collected_events={},
                collected_waiters=[],
            ),
            attempts=0,
            first_attempt_at=100.0,
        )
    )
    assert _check_idle_state(base_state) is False

    # Or with queued work
    base_state.workers["other_step"].in_progress = []
    base_state.workers["other_step"].queue.append(
        EventAttempt(event=OtherEvent(data="queued"))
    )
    assert _check_idle_state(base_state) is False


def test_no_idle_event_when_work_remains(base_state: BrokerState) -> None:
    """WorkflowIdleEvent is not emitted if there's still work to do."""
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    # Queue another event so work remains after processing
    base_state.workers["test_step"].queue.append(
        EventAttempt(event=MyTestEvent(value=99))
    )

    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerResult(result=None)],  # Completes but queue has more
    )

    _, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    idle_commands = [
        c
        for c in commands
        if isinstance(c, CommandPublishEvent) and isinstance(c.event, WorkflowIdleEvent)
    ]
    assert len(idle_commands) == 0


def test_no_idle_event_when_workflow_completes(base_state: BrokerState) -> None:
    """WorkflowIdleEvent is not emitted when workflow completes (StopEvent)."""
    event = MyTestEvent(value=42)
    add_worker(base_state, event)

    # Add a waiter
    waiter = StepWorkerWaiter(
        waiter_id="w1",
        event=event,
        waiting_for_event=OtherEvent,
        requirements={},
        has_requirements=False,
        resolved_event=None,
    )
    base_state.workers["test_step"].collected_waiters.append(waiter)

    # Complete the workflow with StopEvent
    tick: TickStepResult = TickStepResult(
        step_name="test_step",
        worker_id=0,
        event=event,
        result=[StepWorkerResult(result=StopEvent(result="done"))],
    )

    new_state, commands = _process_step_result_tick(tick, base_state, now_seconds=110.0)

    # Workflow is no longer running
    assert new_state.is_running is False

    # No idle event should be emitted
    idle_commands = [
        c
        for c in commands
        if isinstance(c, CommandPublishEvent) and isinstance(c.event, WorkflowIdleEvent)
    ]
    assert len(idle_commands) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Tests for rebuild_state_from_ticks
# ─────────────────────────────────────────────────────────────────────────────


def test_rebuild_state_from_ticks_clears_in_progress(base_state: BrokerState) -> None:
    """
    Test that rebuild_state_from_ticks clears in_progress before replaying ticks.

    This is critical for checkpointing resumed workflows. When a workflow is resumed:
    1. The checkpoint has in_progress workers with IDs like [1, 2, 3]
    2. rewind_in_progress() clears in_progress and assigns new IDs [0, 1, 2]
    3. New ticks reference the new worker IDs [0, 1, 2]
    4. When checkpointing again, rebuild_state_from_ticks must also clear in_progress
       before replaying ticks, otherwise worker IDs won't match.

    Without the fix, this would raise: "Worker 0 not found in in_progress"
    """
    event1 = MyTestEvent(value=1)
    event2 = MyTestEvent(value=2)

    # Simulate checkpoint state with in_progress workers using IDs 1, 2
    # (as if they were mid-execution when checkpoint was taken)
    shared_state = StepWorkerState(
        step_name="test_step",
        collected_events={},
        collected_waiters=[],
    )
    base_state.workers["test_step"].in_progress = [
        InProgressState(
            event=event1,
            worker_id=1,  # Original worker ID from checkpoint
            shared_state=shared_state,
            attempts=0,
            first_attempt_at=100.0,
        ),
        InProgressState(
            event=event2,
            worker_id=2,  # Original worker ID from checkpoint
            shared_state=shared_state,
            attempts=0,
            first_attempt_at=100.0,
        ),
    ]

    # Simulate ticks from a resumed run where rewind_in_progress assigned new IDs
    # These ticks reference worker IDs 0 and 1 (not 1 and 2 from checkpoint)
    ticks: list[WorkflowTick] = [
        # Worker 0 starts (after rewind assigned new ID)
        TickAddEvent(event=event1),
        # Worker 0 completes
        TickStepResult(
            step_name="test_step",
            worker_id=0,  # New ID assigned after rewind
            event=event1,
            result=[StepWorkerResult(result=OtherEvent(data="done1"))],
        ),
        # Worker 1 starts (after rewind assigned new ID)
        TickAddEvent(event=event2),
        # Worker 1 completes
        TickStepResult(
            step_name="test_step",
            worker_id=0,  # Reuses ID 0 since previous worker completed
            event=event2,
            result=[StepWorkerResult(result=StopEvent(result="done2"))],
        ),
    ]

    # This should NOT raise "Worker 0 not found in in_progress"
    # because rebuild_state_from_ticks now clears in_progress before replaying
    final_state = rebuild_state_from_ticks(base_state, ticks)

    # Verify the workflow completed
    assert final_state.is_running is False
    assert len(final_state.workers["test_step"].in_progress) == 0


def test_rebuild_state_from_ticks_preserves_queue_order(
    base_state: BrokerState,
) -> None:
    """
    Test that rebuild_state_from_ticks applies rewind_in_progress which moves
    in_progress events to the front of the queue and then re-starts them.

    Since the base fixture has num_workers=1, only one event can be in_progress
    at a time. The originally in_progress event (event1) should be re-started
    with worker_id=0, and event2 should remain in the queue.
    """
    event1 = MyTestEvent(value=1)
    event2 = MyTestEvent(value=2)

    # State with in_progress worker
    shared_state = StepWorkerState(
        step_name="test_step",
        collected_events={},
        collected_waiters=[],
    )
    base_state.workers["test_step"].in_progress = [
        InProgressState(
            event=event1,
            worker_id=0,
            shared_state=shared_state,
            attempts=2,  # Already retried twice
            first_attempt_at=100.0,
        ),
    ]
    # Also has queued event
    base_state.workers["test_step"].queue = [
        EventAttempt(event=event2, attempts=0, first_attempt_at=None)
    ]

    # No ticks - test that rebuild applies rewind_in_progress
    result = rebuild_state_from_ticks(base_state, [])

    # rewind_in_progress re-starts workers, so event1 should be back in in_progress
    # with worker_id=0 (reassigned) and retry info preserved
    assert len(result.workers["test_step"].in_progress) == 1
    assert result.workers["test_step"].in_progress[0].event == event1
    assert result.workers["test_step"].in_progress[0].worker_id == 0
    assert result.workers["test_step"].in_progress[0].attempts == 2
    # Queue should have event2 (since num_workers=1, only 1 can be in_progress)
    assert len(result.workers["test_step"].queue) == 1
    assert result.workers["test_step"].queue[0].event == event2


async def _aiter(ticks: list[WorkflowTick]) -> AsyncIterator[WorkflowTick]:
    for t in ticks:
        yield t


def _simple_step_tick_sequence() -> list[WorkflowTick]:
    event1 = MyTestEvent(value=1)
    event2 = MyTestEvent(value=2)
    return [
        TickAddEvent(event=event1),
        TickStepResult(
            step_name="test_step",
            worker_id=0,
            event=event1,
            result=[StepWorkerResult(result=OtherEvent(data="done1"))],
        ),
        TickAddEvent(event=event2),
        TickStepResult(
            step_name="test_step",
            worker_id=0,
            event=event2,
            result=[StepWorkerResult(result=StopEvent(result="done2"))],
        ),
    ]


async def test_rebuild_state_from_ticks_stream_empty(base_state: BrokerState) -> None:
    shared_state = StepWorkerState(
        step_name="test_step", collected_events={}, collected_waiters=[]
    )
    event1 = MyTestEvent(value=1)
    base_state.workers["test_step"].in_progress = [
        InProgressState(
            event=event1,
            worker_id=0,
            shared_state=shared_state,
            attempts=1,
            first_attempt_at=100.0,
        ),
    ]

    streamed = await rebuild_state_from_ticks_stream(base_state, _aiter([]))

    # rewind_in_progress re-assigns worker_id=0 starting fresh; in_progress preserved.
    assert len(streamed.workers["test_step"].in_progress) == 1
    assert streamed.workers["test_step"].in_progress[0].worker_id == 0
    assert streamed.workers["test_step"].in_progress[0].event == event1


async def test_rebuild_state_from_ticks_stream_single_tick(
    base_state: BrokerState, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Freeze time so timestamp kludges don't diverge between paths.
    monkeypatch.setattr("workflows.runtime.control_loop.time.time", lambda: 12345.0)
    ticks: list[WorkflowTick] = [TickAddEvent(event=MyTestEvent(value=42))]
    streamed = await rebuild_state_from_ticks_stream(
        base_state.deepcopy(), _aiter(ticks)
    )
    listed = rebuild_state_from_ticks(base_state.deepcopy(), ticks)
    assert streamed == listed


async def test_rebuild_state_from_ticks_stream_multi_tick_equivalence(
    base_state: BrokerState, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("workflows.runtime.control_loop.time.time", lambda: 12345.0)
    ticks = _simple_step_tick_sequence()
    streamed = await rebuild_state_from_ticks_stream(
        base_state.deepcopy(), _aiter(list(ticks))
    )
    listed = rebuild_state_from_ticks(base_state.deepcopy(), list(ticks))
    assert streamed == listed
    assert streamed.is_running is False


async def test_rebuild_state_from_ticks_stream_large_history_equivalence(
    base_state: BrokerState, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("workflows.runtime.control_loop.time.time", lambda: 12345.0)
    ticks: list[WorkflowTick] = [
        TickAddEvent(event=MyTestEvent(value=i)) for i in range(500)
    ]
    streamed = await rebuild_state_from_ticks_stream(
        base_state.deepcopy(), _aiter(list(ticks))
    )
    listed = rebuild_state_from_ticks(base_state.deepcopy(), list(ticks))
    assert streamed == listed


async def test_rebuild_state_from_ticks_stream_clears_in_progress(
    base_state: BrokerState,
) -> None:
    event1 = MyTestEvent(value=1)
    event2 = MyTestEvent(value=2)
    shared_state = StepWorkerState(
        step_name="test_step", collected_events={}, collected_waiters=[]
    )
    base_state.workers["test_step"].in_progress = [
        InProgressState(
            event=event1,
            worker_id=1,
            shared_state=shared_state,
            attempts=0,
            first_attempt_at=100.0,
        ),
        InProgressState(
            event=event2,
            worker_id=2,
            shared_state=shared_state,
            attempts=0,
            first_attempt_at=100.0,
        ),
    ]
    ticks: list[WorkflowTick] = [
        TickAddEvent(event=event1),
        TickStepResult(
            step_name="test_step",
            worker_id=0,
            event=event1,
            result=[StepWorkerResult(result=OtherEvent(data="done1"))],
        ),
        TickAddEvent(event=event2),
        TickStepResult(
            step_name="test_step",
            worker_id=0,
            event=event2,
            result=[StepWorkerResult(result=StopEvent(result="done2"))],
        ),
    ]

    final_state = await rebuild_state_from_ticks_stream(base_state, _aiter(ticks))

    assert final_state.is_running is False
    assert len(final_state.workers["test_step"].in_progress) == 0


async def test_replay_ticks_stream_surfaces_stop_event(base_state: BrokerState) -> None:
    ticks = _simple_step_tick_sequence()
    replay = await replay_ticks_stream(base_state, _aiter(list(ticks)))
    assert replay.state.is_running is False
    assert isinstance(replay.exit_command, CommandCompleteRun)
    assert isinstance(replay.exit_command.result, StopEvent)
    assert replay.exit_command.result.result == "done2"


async def test_replay_ticks_stream_no_exit_command_when_running(
    base_state: BrokerState,
) -> None:
    ticks: list[WorkflowTick] = [TickAddEvent(event=MyTestEvent(value=1))]
    replay = await replay_ticks_stream(base_state, _aiter(ticks))
    assert replay.exit_command is None
