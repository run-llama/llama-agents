# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Events a step returned survive a cut before their add_event tick.

A step result is journaled as a TickStepResult, but the event it returned is
only routed when the derived TickAddEvent is reduced as a later tick. The
broker keeps the event in ``pending_events`` across that gap so a snapshot or
journal cut in between still resumes the run.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest
from pydantic import TypeAdapter
from workflows.context import Context
from workflows.context.external_context import ExternalContext
from workflows.context.serializers import JsonSerializer
from workflows.decorators import step
from workflows.events import Event, StartEvent, StepFailedEvent, StopEvent
from workflows.plugins.basic import BasicRuntime
from workflows.runtime.control_loop.reduce import (
    _reduce_tick,
    pending_event_commands,
    rebuild_state_from_ticks,
)
from workflows.runtime.types.commands import CommandQueueEvent, CommandRunWorker
from workflows.runtime.types.internal_state import BrokerState, PendingEvent
from workflows.runtime.types.results import StepWorkerResult
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickAddEvent, TickStepResult, WorkflowTick
from workflows.workflow import Workflow


class MiddleEvent(Event):
    value: int


class ChainWorkflow(Workflow):
    @step
    async def first(self, ev: StartEvent) -> MiddleEvent:
        return MiddleEvent(value=1)

    @step
    async def second(self, ev: MiddleEvent) -> StopEvent:
        return StopEvent(result=f"done_{ev.value}")


FIRST = StepId.root("first")
SECOND = StepId.root("second")
SERIALIZER = JsonSerializer()
TICK_ADAPTER: TypeAdapter[WorkflowTick] = TypeAdapter(WorkflowTick)


def _reduce(state: BrokerState, tick: TickAddEvent | TickStepResult) -> BrokerState:
    state, _ = _reduce_tick(tick, state, time.time())
    return state


def _state_after_first_step() -> BrokerState:
    """Reduce the start event and ``first``'s result, but not the derived add."""
    state = BrokerState.from_workflow(ChainWorkflow())
    state = _reduce(state, TickAddEvent(event=StartEvent()))
    return _reduce(
        state,
        TickStepResult(
            step_id=FIRST,
            worker_id=0,
            event=StartEvent(),
            result=[StepWorkerResult(result=MiddleEvent(value=1))],
        ),
    )


def _round_trip(state: BrokerState) -> BrokerState:
    serialized = state.to_serialized(SERIALIZER)
    return BrokerState.from_serialized(serialized, ChainWorkflow(), SERIALIZER)


def test_step_result_event_is_pending_across_serialization() -> None:
    state = _state_after_first_step()

    restored = _round_trip(state)

    assert all(not w.queue and not w.in_progress for w in restored.workers.values())
    assert [p.event for p in restored.pending_events] == [MiddleEvent(value=1)]
    commands = pending_event_commands(restored)
    assert commands == [CommandQueueEvent(event=MiddleEvent(value=1))]


def test_derived_add_event_pops_pending() -> None:
    state = _state_after_first_step()

    state = _reduce(state, TickAddEvent(event=MiddleEvent(value=1)))

    assert state.pending_events == []
    assert len(state.workers[SECOND].in_progress) == 1


def test_external_send_equal_to_pending_queues_both() -> None:
    state = _state_after_first_step()
    commands: list[CommandRunWorker] = []

    # An external send_event equal to the pending event arrives first and
    # takes the pending entry. The derived tick then counts as external.
    for _ in range(2):
        state, emitted = _reduce_tick(
            TickAddEvent(event=MiddleEvent(value=1)), state, time.time()
        )
        commands.extend(c for c in emitted if isinstance(c, CommandRunWorker))

    assert state.pending_events == []
    assert len(commands) == 2
    assert len(state.workers[SECOND].in_progress) == 2


def test_replayed_add_event_with_exception_pops_pending() -> None:
    """Journal replay deserializes ticks separately; exceptions still match."""
    failed = StepFailedEvent(
        step_name="first",
        input_event=StartEvent(),
        exception=ValueError("boom"),
        attempts=1,
        elapsed_seconds=0.0,
        failed_at=datetime.now(timezone.utc),
    )
    state = BrokerState.from_workflow(ChainWorkflow())
    state.pending_events.append(PendingEvent(event=failed, step_id=FIRST))
    tick = TickAddEvent(event=failed, step_id=FIRST)
    replayed = TICK_ADAPTER.validate_json(TICK_ADAPTER.dump_json(tick))
    assert isinstance(replayed, TickAddEvent)
    assert replayed.event != failed

    state = _reduce(state, replayed)

    assert state.pending_events == []


@pytest.mark.asyncio
async def test_resume_from_cut_after_step_result_completes() -> None:
    wf = ChainWorkflow(timeout=5.0, runtime=BasicRuntime())
    handler = wf.run()
    assert await handler == "done_1"
    assert handler.ctx is not None
    face = handler.ctx._face
    assert isinstance(face, ExternalContext)
    ticks = face._tick_log

    # Cut the journal right after first's TickStepResult, before its add.
    cut = next(
        i
        for i, tick in enumerate(ticks)
        if isinstance(tick, TickStepResult) and tick.step_id == FIRST
    )
    state = rebuild_state_from_ticks(BrokerState.from_workflow(wf), ticks[: cut + 1])
    ctx_dict = state.to_serialized(SERIALIZER).model_dump(mode="python")
    assert all(
        not w["queue"] and not w["in_progress"] for w in ctx_dict["workers"].values()
    )
    assert len(ctx_dict["pending_events"]) == 1

    resumed = wf.run(ctx=Context.from_dict(wf, ctx_dict))

    assert await resumed == "done_1"
