# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import Workflow, step
from workflows.context.serializers import JsonSerializer
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.control_loop.reduce import _add_or_enqueue_event, _reduce_tick
from workflows.runtime.types.commands import CommandRunWorker
from workflows.runtime.types.internal_state import BrokerState, EventAttempt
from workflows.runtime.types.results import (
    AddWaiter,
    DeleteWaiter,
    StepWorkerResult,
    StepWorkerWaiter,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickAddEvent, TickStepResult


class Answer(Event):
    value: str


class WaiterWorkflow(Workflow):
    @step(num_workers=2)
    async def wait(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


WAIT_STEP = StepId.root("wait")


@pytest.mark.parametrize("checkpoint", [False, True])
def test_requirements_refresh_preserves_unconsumed_answer(checkpoint: bool) -> None:
    workflow = WaiterWorkflow()
    state = BrokerState.from_workflow(workflow)
    state.is_running = True
    event = StartEvent()
    state.workers[WAIT_STEP].collected_waiters.append(
        StepWorkerWaiter(
            waiter_id="answer",
            event=event,
            waiting_for_event=Answer,
            requirements={"value": "yes"},
            has_requirements=True,
            resolved_event=None,
            work_item_id="original",
        )
    )
    refresh = next(
        c
        for c in _add_or_enqueue_event(
            EventAttempt(event=event, work_item_id="original"),
            WAIT_STEP,
            state,
            (),
            0.0,
        )
        if isinstance(c, CommandRunWorker)
    )
    state, commands = _reduce_tick(TickAddEvent(event=Answer(value="yes")), state, 1.0)
    continuation = next(c for c in commands if isinstance(c, CommandRunWorker))
    if checkpoint:
        serializer = JsonSerializer()
        state = BrokerState.from_serialized(
            state.to_serialized(serializer), workflow, serializer
        )

    state, _ = _reduce_tick(
        TickStepResult(
            step_id=WAIT_STEP,
            worker_id=refresh.id,
            invocation_id=refresh.invocation_id,
            event=event,
            result=[
                AddWaiter(
                    waiter_id="answer",
                    event_type=Answer,
                    requirements={"value": "yes"},
                )
            ],
        ),
        state,
        2.0,
    )
    waiters = state.workers[WAIT_STEP].collected_waiters
    assert len(waiters) == 1
    assert waiters[0].resolved_event == Answer(value="yes")
    state, _ = _reduce_tick(
        TickStepResult(
            step_id=WAIT_STEP,
            worker_id=continuation.id,
            invocation_id=continuation.invocation_id,
            event=event,
            result=[DeleteWaiter(waiter_id="answer"), StepWorkerResult()],
        ),
        state,
        3.0,
    )
    assert state.workers[WAIT_STEP].collected_waiters == []
