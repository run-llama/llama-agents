# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import Context, Workflow, step
from workflows.context.context_types import SerializedContext
from workflows.context.serializers import JsonSerializer
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.control_loop.reduce import _add_or_enqueue_event, _reduce_tick
from workflows.runtime.control_loop.runner import _ControlLoopRunner
from workflows.runtime.types.commands import (
    CommandRunWorker,
    CommandScheduleWaiterTimeout,
)
from workflows.runtime.types.internal_state import BrokerState, EventAttempt
from workflows.runtime.types.results import AddWaiter, DeleteWaiter, StepWorkerResult
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickStepResult,
    TickWaiterTimeout,
    WorkflowTickAdapter,
)

from .conftest import MockRunAdapter


class Answer(Event):
    value: str


class WaiterWorkflow(Workflow):
    @step(num_workers=1)
    async def wait(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


WAIT_STEP = StepId.root("wait")


def register_waiter(
    state: BrokerState, timeout: float | None
) -> tuple[BrokerState, list[CommandScheduleWaiterTimeout]]:
    run = next(
        c
        for c in _add_or_enqueue_event(
            EventAttempt(event=StartEvent()), WAIT_STEP, state, (), 0.0
        )
        if isinstance(c, CommandRunWorker)
    )
    state, commands = _reduce_tick(
        TickStepResult(
            step_id=WAIT_STEP,
            worker_id=run.id,
            invocation_id=run.invocation_id,
            event=run.event,
            result=[AddWaiter(waiter_id="answer", event_type=Answer, timeout=timeout)],
        ),
        state,
        0.0,
    )
    return state, [c for c in commands if isinstance(c, CommandScheduleWaiterTimeout)]


@pytest.mark.asyncio
@pytest.mark.parametrize("second_timeout", [None, 50.0])
@pytest.mark.parametrize("checkpoint", [False, True])
async def test_old_timeout_cannot_expire_reused_waiter(
    test_plugin: MockRunAdapter, second_timeout: float | None, checkpoint: bool
) -> None:
    workflow = WaiterWorkflow(timeout=None)
    state = BrokerState.from_workflow(workflow)
    state.is_running = True
    state, alarms = register_waiter(state, 5.0)
    runner = _ControlLoopRunner(workflow, test_plugin, Context(workflow), {}, state)
    now = await test_plugin.get_now()
    await runner.process_command(alarms[0])
    old_tick = runner.pop_due_ticks(now + 5.0)[0]
    # Exercise the same serialized tick shape used by journal adapters.
    old_tick = WorkflowTickAdapter.validate_python(old_tick.model_dump(mode="json"))

    state, commands = _reduce_tick(
        TickAddEvent(event=Answer(value="first")), state, 1.0
    )
    continuation = next(c for c in commands if isinstance(c, CommandRunWorker))
    state, _ = _reduce_tick(
        TickStepResult(
            step_id=WAIT_STEP,
            worker_id=continuation.id,
            invocation_id=continuation.invocation_id,
            event=continuation.event,
            result=[DeleteWaiter(waiter_id="answer"), StepWorkerResult()],
        ),
        state,
        1.0,
    )
    assert state.workers[WAIT_STEP].collected_waiters == []
    state, alarms = register_waiter(state, second_timeout)
    if checkpoint:
        serializer = JsonSerializer()
        state = BrokerState.from_serialized(
            state.to_serialized(serializer), workflow, serializer
        )

    state, commands = _reduce_tick(old_tick, state, 5.0)
    waiter = state.workers[WAIT_STEP].collected_waiters[0]
    assert not waiter.timed_out
    assert not any(isinstance(c, CommandRunWorker) for c in commands)

    if second_timeout is not None:
        await runner.process_command(alarms[0])
        current_tick = runner.pop_due_ticks(now + second_timeout)[0]
        state, commands = _reduce_tick(current_tick, state, second_timeout)
        assert state.workers[WAIT_STEP].collected_waiters[0].timed_out
        assert sum(isinstance(c, CommandRunWorker) for c in commands) == 1
        state, commands = _reduce_tick(current_tick, state, second_timeout)
        assert not any(isinstance(c, CommandRunWorker) for c in commands)
        assert state.workers[WAIT_STEP].queue == []
    else:
        state, commands = _reduce_tick(
            TickAddEvent(event=Answer(value="second")), state, 6.0
        )
        assert sum(isinstance(c, CommandRunWorker) for c in commands) == 1
        assert state.workers[WAIT_STEP].collected_waiters[0].resolved_event == Answer(
            value="second"
        )


@pytest.mark.asyncio
async def test_requirements_refresh_keeps_original_timeout(
    test_plugin: MockRunAdapter,
) -> None:
    workflow = WaiterWorkflow(timeout=None)
    state = BrokerState.from_workflow(workflow)
    state, alarms = register_waiter(state, 5.0)
    runner = _ControlLoopRunner(workflow, test_plugin, Context(workflow), {}, state)
    now = await test_plugin.get_now()
    await runner.process_command(alarms[0])
    state, alarms = register_waiter(state, 50.0)
    assert alarms == []
    state, commands = _reduce_tick(runner.pop_due_ticks(now + 5.0)[0], state, 5.0)
    assert state.workers[WAIT_STEP].collected_waiters[0].timed_out
    assert sum(isinstance(c, CommandRunWorker) for c in commands) == 1


def test_legacy_timeout_tick_still_resumes_waiter() -> None:
    workflow = WaiterWorkflow(timeout=None)
    state = BrokerState.from_workflow(workflow)
    state, _ = register_waiter(state, 5.0)
    serializer = JsonSerializer()
    snapshot = state.to_serialized(serializer).model_dump(mode="json")
    snapshot["workers"]["wait"]["collected_waiters"][0].pop("registration_id", None)
    state = BrokerState.from_serialized(
        SerializedContext.model_validate(snapshot), workflow, serializer
    )
    tick = WorkflowTickAdapter.validate_python(
        {"type": "waiter_timeout", "step_id": "wait", "waiter_id": "answer"}
    )
    assert isinstance(tick, TickWaiterTimeout)
    state, commands = _reduce_tick(tick, state, 5.0)
    assert state.workers[WAIT_STEP].collected_waiters[0].timed_out
    assert sum(isinstance(c, CommandRunWorker) for c in commands) == 1
