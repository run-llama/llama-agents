# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Events a step sends with ctx.send_event survive a cut after its result.

The sent TickAddEvent can be reduced before or after the sending step's
TickStepResult. When the result comes first, the broker holds the send in
``pending_events`` until its tick arrives, so a snapshot or journal cut in
between still resumes the run.
"""

from __future__ import annotations

import time

import pytest
from pydantic import TypeAdapter
from workflows.context import Context
from workflows.context.external_context import ExternalContext
from workflows.context.serializers import JsonSerializer
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent
from workflows.plugins.basic import BasicRuntime
from workflows.runtime.control_loop.reduce import (
    _reduce_tick,
    pending_event_commands,
    rebuild_state_from_ticks,
)
from workflows.runtime.types.commands import CommandQueueEvent
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.results import SentBy, SentEvent, StepWorkerResult
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickAddEvent, TickStepResult, WorkflowTick
from workflows.workflow import Workflow


class SentEv(Event):
    value: int


class ReturnedEv(Event):
    value: int


class SendWorkflow(Workflow):
    @step
    async def first(self, ctx: Context, ev: StartEvent) -> SentEv | None:
        # SentEv is declared for validation. The step sends it instead.
        ctx.send_event(SentEv(value=1))
        return None

    @step
    async def second(self, ev: SentEv) -> StopEvent:
        return StopEvent(result=f"done_{ev.value}")


class SendAndReturnWorkflow(Workflow):
    @step
    async def first(self, ctx: Context, ev: StartEvent) -> SentEv | ReturnedEv:
        ctx.send_event(SentEv(value=1))
        return ReturnedEv(value=2)

    @step
    async def collect(
        self, ctx: Context, ev: SentEv | ReturnedEv
    ) -> StopEvent | None:
        events = ctx.collect_events(ev, [SentEv, ReturnedEv])
        if events is None:
            return None
        return StopEvent(result=f"done_{events[0].value}_{events[1].value}")


FIRST = StepId.root("first")
SERIALIZER = JsonSerializer()
TICK_ADAPTER: TypeAdapter[WorkflowTick] = TypeAdapter(WorkflowTick)
SENT_BY = SentBy(work_item_id="work_item_1", index=0)
SENT_ADD = TickAddEvent(event=SentEv(value=1), sent_by=SENT_BY)
FIRST_RESULT = TickStepResult(
    step_id=FIRST,
    worker_id=0,
    event=StartEvent(),
    result=[StepWorkerResult(result=None)],
    sent_events=[SentEvent(index=0, event=SentEv(value=1))],
)


def _reduce(state: BrokerState, *ticks: WorkflowTick) -> BrokerState:
    for tick in ticks:
        state, _ = _reduce_tick(tick, state, time.time())
    return state


def _started() -> BrokerState:
    return _reduce(
        BrokerState.from_workflow(SendWorkflow()), TickAddEvent(event=StartEvent())
    )


def _serialized(wf: Workflow, state: BrokerState) -> dict:
    return state.to_serialized(SERIALIZER).model_dump(mode="python")


async def _run_and_get_ticks(wf: Workflow, expected: str) -> list[WorkflowTick]:
    handler = wf.run()
    assert await handler == expected
    assert handler.ctx is not None
    face = handler.ctx._face
    assert isinstance(face, ExternalContext)
    return list(face._tick_log)


def test_send_reduced_after_result_is_pending_until_its_tick() -> None:
    state = _reduce(_started(), FIRST_RESULT)

    restored = BrokerState.from_serialized(
        state.to_serialized(SERIALIZER), SendWorkflow(), SERIALIZER
    )

    assert [(p.event, p.sent_by) for p in restored.pending_events] == [
        (SentEv(value=1), SENT_BY)
    ]
    assert pending_event_commands(restored) == [
        CommandQueueEvent(event=SentEv(value=1), sent_by=SENT_BY)
    ]
    assert _reduce(restored, SENT_ADD).pending_events == []


def test_send_reduced_before_result_is_not_pending() -> None:
    state = _reduce(_started(), SENT_ADD, FIRST_RESULT)

    assert state.pending_events == []


@pytest.mark.asyncio
async def test_resume_from_cut_after_sending_step_result_completes() -> None:
    wf = SendWorkflow(timeout=5.0, runtime=BasicRuntime())
    ticks = await _run_and_get_ticks(wf, "done_1")
    cut = next(
        i
        for i, tick in enumerate(ticks)
        if isinstance(tick, TickStepResult) and tick.step_id == FIRST
    )
    state = rebuild_state_from_ticks(BrokerState.from_workflow(wf), ticks[: cut + 1])
    # The live run reduced the send after the result, so the cut holds it.
    assert [p.event for p in state.pending_events] == [SentEv(value=1)]

    resumed = wf.run(ctx=Context.from_dict(wf, _serialized(wf, state)))

    assert await resumed == "done_1"


@pytest.mark.asyncio
async def test_send_then_wait_in_same_step_resolves() -> None:
    class SendThenWaitWorkflow(Workflow):
        @step
        async def first(self, ctx: Context, ev: StartEvent) -> StopEvent:
            ctx.send_event(SentEv(value=3))
            got = await ctx.wait_for_event(SentEv)
            return StopEvent(result=f"done_{got.value}")

    wf = SendThenWaitWorkflow(timeout=5.0, disable_validation=True)

    assert await wf.run() == "done_3"


@pytest.mark.asyncio
async def test_replay_matches_live_state_and_resumes_at_every_cut() -> None:
    wf = SendAndReturnWorkflow(timeout=5.0, runtime=BasicRuntime())
    ticks = await _run_and_get_ticks(wf, "done_1_2")
    replayed = [TICK_ADAPTER.validate_json(TICK_ADAPTER.dump_json(t)) for t in ticks]

    for cut in range(1, len(ticks)):
        live = rebuild_state_from_ticks(BrokerState.from_workflow(wf), ticks[:cut])
        replay = rebuild_state_from_ticks(
            BrokerState.from_workflow(wf), replayed[:cut]
        )
        assert _serialized(wf, replay) == _serialized(wf, live), cut
        if not live.is_running:
            continue
        resumed = wf.run(ctx=Context.from_dict(wf, _serialized(wf, live)))
        assert await resumed == "done_1_2", cut
