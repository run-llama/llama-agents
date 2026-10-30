# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Regression: journal restore must keep wait_for_event requirements (#23661)."""

from __future__ import annotations

import asyncio

import pytest
from workflows import Context, Workflow, step
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
    WorkflowIdleEvent,
)
from workflows.plugins import basic_runtime


class Answer(HumanResponseEvent):
    name: str
    value: int


class GoA(Event):
    pass


class GoB(Event):
    pass


class Got(Event):
    name: str
    value: int


class TwoWaiters(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> GoA | GoB:
        ctx.send_event(GoA())
        ctx.send_event(GoB())

    @step
    async def a(self, ctx: Context, ev: GoA) -> Got:
        answer = await ctx.wait_for_event(
            Answer,
            waiter_event=InputRequiredEvent(),
            waiter_id="a",
            requirements={"name": "a"},
            timeout=None,
        )
        return Got(name="a", value=answer.value)

    @step
    async def b(self, ctx: Context, ev: GoB) -> Got:
        answer = await ctx.wait_for_event(
            Answer,
            waiter_event=InputRequiredEvent(),
            waiter_id="b",
            requirements={"name": "b"},
            timeout=None,
        )
        return Got(name="b", value=answer.value)

    @step
    async def join(self, ctx: Context, ev: Got) -> StopEvent | None:
        events = ctx.collect_events(ev, [Got, Got])
        return (
            None
            if events is None
            else StopEvent(result=sorted((item.name, item.value) for item in events))
        )


async def _collect_journal(run_id: str):
    return [record async for record in basic_runtime.journal(run_id)]


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["snapshot", "journal"])
async def test_waiter_requirements_survive_restore(source: str) -> None:
    workflow = TwoWaiters(timeout=None)
    handler = workflow.run()
    records_task = asyncio.create_task(_collect_journal(handler.run_id))
    async for event in handler.stream_events(expose_internal=True):
        if isinstance(event, WorkflowIdleEvent):
            break
    snapshot = handler.ctx.to_dict()
    await handler.cancel_run()
    records = await records_task

    workflow = TwoWaiters(timeout=None)
    if source == "journal":
        restored = basic_runtime.restore(workflow, None, records)
    else:
        restored = Context.from_dict(workflow, snapshot)

    handler = workflow.run(ctx=restored)
    await handler.send_event(Answer(name="b", value=2))
    await handler.send_event(Answer(name="a", value=1))
    events = [
        type(event).__name__
        async for event in handler.stream_events(expose_internal=True)
    ]
    assert await handler == [("a", 1), ("b", 2)]
    assert "WorkflowIdleEvent" in events
