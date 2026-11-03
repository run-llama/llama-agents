# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import Workflow, step
from workflows.context.serializers import JsonSerializer
from workflows.events import Event, InputRequiredEvent, StartEvent, StopEvent
from workflows.runtime.control_loop.reduce import _add_or_enqueue_event, _reduce_tick
from workflows.runtime.types.commands import (
    CommandRunWorker,
    CommandScheduleWaiterTimeout,
)
from workflows.runtime.types.internal_state import BrokerState, EventAttempt
from workflows.runtime.types.results import (
    AddWaiter,
    DeleteWaiter,
    StepWorkerResult,
    StepWorkerWaiter,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickStepResult,
    TickWaiterTimeout,
)


class Answer(Event):
    value: str


class WaiterWorkflow(Workflow):
    @step(num_workers=2)
    async def wait(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


WAIT_STEP = StepId.root("wait")


@pytest.mark.parametrize(
    "complete_first,checkpoint,reuse_before_late",
    [
        (False, False, False),
        (True, False, False),
        (True, True, False),
        (True, True, True),
    ],
)
@pytest.mark.parametrize("resolve_with_answer", [False, True])
def test_late_registration_cannot_reset_or_recreate_completed_waiter(
    complete_first: bool,
    checkpoint: bool,
    reuse_before_late: bool,
    resolve_with_answer: bool,
) -> None:
    workflow = WaiterWorkflow()
    state = BrokerState.from_workflow(workflow)
    state.is_running = True
    event = StartEvent()
    worker = state.workers[WAIT_STEP]
    worker.collected_waiters.append(
        StepWorkerWaiter(
            waiter_id="answer",
            event=event,
            waiting_for_event=Answer,
            requirements={"value": "yes"} if resolve_with_answer else {},
            has_requirements=True,
            resolved_event=None,
            work_item_id="original",
        )
    )
    # The requirements-refresh worker is still running when resolution resumes
    # the same suspended work item on the second worker.
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
    resolution = (
        TickAddEvent(event=Answer(value="yes"))
        if resolve_with_answer
        else TickWaiterTimeout(step_id=WAIT_STEP, waiter_id="answer")
    )
    state, commands = _reduce_tick(resolution, state, 1.0)
    continuation = next(c for c in commands if isinstance(c, CommandRunWorker))
    assert continuation.id != refresh.id

    completion = TickStepResult(
        step_id=WAIT_STEP,
        worker_id=continuation.id,
        invocation_id=continuation.invocation_id,
        event=event,
        result=[DeleteWaiter(waiter_id="answer"), StepWorkerResult()],
    )
    if complete_first:
        state, _ = _reduce_tick(completion, state, 2.0)
    fresh = None
    if reuse_before_late:
        # Capture a nonzero generation while the stale worker is still active.
        fresh = next(
            c
            for c in _add_or_enqueue_event(
                EventAttempt(event=StartEvent(), work_item_id="fresh"),
                WAIT_STEP,
                state,
                (),
                2.5,
            )
            if isinstance(c, CommandRunWorker)
        )
    if checkpoint:
        serializer = JsonSerializer()
        state = BrokerState.from_serialized(
            state.to_serialized(serializer), workflow, serializer
        )

    state, commands = _reduce_tick(
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
                    timeout=1.0,
                    waiter_event=InputRequiredEvent(),
                )
            ],
        ),
        state,
        3.0,
    )
    assert not any(isinstance(c, CommandScheduleWaiterTimeout) for c in commands)
    if not complete_first:
        waiter = state.workers[WAIT_STEP].collected_waiters[0]
        if resolve_with_answer:
            assert waiter.resolved_event == Answer(value="yes")
        else:
            assert waiter.timed_out
        state, _ = _reduce_tick(completion, state, 4.0)
    assert state.workers[WAIT_STEP].collected_waiters == []
    if fresh is None:
        assert state.workers[WAIT_STEP].waiter_generations == {}
    state, commands = _reduce_tick(
        TickWaiterTimeout(step_id=WAIT_STEP, waiter_id="answer"), state, 5.0
    )
    assert not any(isinstance(c, CommandRunWorker) for c in commands)

    # A later invocation may intentionally reuse the explicit waiter ID.
    if fresh is None:
        fresh = next(
            c
            for c in _add_or_enqueue_event(
                EventAttempt(event=StartEvent(), work_item_id="fresh"),
                WAIT_STEP,
                state,
                (),
                6.0,
            )
            if isinstance(c, CommandRunWorker)
        )
    state, commands = _reduce_tick(
        TickStepResult(
            step_id=WAIT_STEP,
            worker_id=fresh.id,
            invocation_id=fresh.invocation_id,
            event=event,
            result=[AddWaiter(waiter_id="answer", event_type=Answer, timeout=1.0)],
        ),
        state,
        7.0,
    )
    assert len(state.workers[WAIT_STEP].collected_waiters) == 1
    assert state.workers[WAIT_STEP].waiter_generations == {}
    assert any(isinstance(c, CommandScheduleWaiterTimeout) for c in commands)
