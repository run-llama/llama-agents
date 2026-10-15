# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Regression test: restoring at an idle point must not lose a sent event.

A run saved with ``ctx.to_dict()`` and restored with ``Context.from_dict()``
at every idle point lost a ``Reply`` that had already been sent: the idle
event was published while the reply's tick was still waiting to be reduced,
so the snapshot never contained it and cancelling the old run discarded it.
The nested fan-out below then never completed its inner join, the outer
join never received ``LeftDone``, and the restored run hung.

See https://github.com/run-llama/llama_index/issues/23515
"""

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


class Left(Event):
    pass


class Right(Event):
    n: int = 0


class A(Event):
    pass


class B(Event):
    pass


class ADone(Event):
    pass


class BDone(Event):
    pass


class LeftDone(Event):
    pass


class RightDone(Event):
    pass


class Ask(InputRequiredEvent):
    waiter: str


class Reply(HumanResponseEvent):
    waiter: str


async def _wait(ctx: Context, waiter: str) -> None:
    await ctx.wait_for_event(
        Reply,
        waiter_event=Ask(waiter=waiter),
        waiter_id=waiter,
        requirements={"waiter": waiter},
        timeout=None,
    )


class NestedFanOut(Workflow):
    @step
    async def start(self, ev: StartEvent) -> list[Left | Right]:
        return [Left(), Right()]

    @step
    async def left(self, ev: Left) -> list[A | B]:
        # A fan-out nested in the first member.
        return [A(), B()]

    @step
    async def a(self, ctx: Context, ev: A) -> ADone:
        await _wait(ctx, "a")
        return ADone()

    @step
    async def b(self, ctx: Context, ev: B) -> BDone:
        await _wait(ctx, "b")
        return BDone()

    @step
    async def inner(self, events: list[ADone | BDone]) -> LeftDone:
        return LeftDone()

    @step
    async def right(self, ctx: Context, ev: Right) -> Right | RightDone:
        await _wait(ctx, f"r{ev.n}")
        # The other member asks twice.
        return Right(n=1) if ev.n == 0 else RightDone()

    @step
    async def outer(self, events: list[LeftDone | RightDone]) -> StopEvent:
        return StopEvent(result=sorted(type(item).__name__ for item in events))


async def _run(restore: bool) -> object:
    workflow = NestedFanOut(timeout=None)
    handler = workflow.run()
    stream = handler.stream_events(expose_internal=True)
    while True:
        asks: list[str] = []
        async for item in stream:
            # Until the run is idle, waiting on its requests.
            if isinstance(item, Ask):
                asks.append(item.waiter)
            elif isinstance(item, WorkflowIdleEvent) and asks:
                break
        if not asks:
            return await handler
        if restore:
            # As after a restart: save the idle run and restore it into a
            # fresh workflow.
            saved = handler.ctx.to_dict()
            await handler.cancel_run()
            try:
                await handler
            except BaseException:
                pass
            workflow = NestedFanOut(timeout=None)
            handler = workflow.run(ctx=Context.from_dict(workflow, saved))
            stream = handler.stream_events(expose_internal=True)
            async for item in stream:
                if isinstance(item, WorkflowIdleEvent):
                    # Restored and waiting again.
                    break
        for waiter in asks:
            handler.ctx.send_event(Reply(waiter=waiter))


@pytest.mark.asyncio
async def test_nested_fanout_in_place_and_restored_at_every_idle() -> None:
    for restore in (False, True):
        result = await asyncio.wait_for(_run(restore), timeout=10)
        assert result == ["LeftDone", "RightDone"]
