# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Tests for context state preservation when passing ctx to workflow.run().

These tests verify that when a Context is passed to Workflow.run(ctx=ctx),
the original context object is updated with run state and can be used for
subsequent operations.

See: thoughts/shared/plans/2026-01-23-context-state-preservation-fix.md
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import Field
from workflows.context import Context
from workflows.context.state_store import DictState
from workflows.decorators import step
from workflows.errors import ContextStateError, WorkflowRuntimeError
from workflows.events import StartEvent, StopEvent
from workflows.workflow import Workflow


class CounterWorkflow(Workflow):
    """Simple workflow that increments a counter in state."""

    @step
    async def count(self, ctx: Context, ev: StartEvent) -> StopEvent:
        count = await ctx.store.get("count", default=0)
        count += 1
        await ctx.store.set("count", count)
        return StopEvent(result=count)


@pytest.mark.asyncio
async def test_original_ctx_is_handler_ctx() -> None:
    """ctx passed to run() should be the same object as handler.ctx"""
    wf = CounterWorkflow()
    ctx = Context(wf)
    handler = wf.run(ctx=ctx)
    await handler
    assert ctx is handler.ctx


@pytest.mark.asyncio
async def test_original_ctx_to_dict_works() -> None:
    """ctx.to_dict() should work after run (not just handler.ctx.to_dict())"""
    wf = CounterWorkflow()
    ctx = Context(wf)
    handler = wf.run(ctx=ctx)
    await handler
    ctx_dict = ctx.to_dict()  # Should NOT raise
    assert "state" in ctx_dict


@pytest.mark.asyncio
async def test_sequential_runs_accumulate_state() -> None:
    """Three sequential runs should produce 1, 2, 3"""
    wf = CounterWorkflow()
    ctx = Context(wf)
    r1 = await wf.run(ctx=ctx)  # count: 0 -> 1
    r2 = await wf.run(ctx=ctx)  # count: 1 -> 2
    r3 = await wf.run(ctx=ctx)  # count: 2 -> 3
    assert (r1, r2, r3) == (1, 2, 3)


@pytest.mark.asyncio
async def test_concurrent_runs_same_context_raises() -> None:
    """Starting a second run while first is running should raise"""

    class SlowWorkflow(Workflow):
        @step
        async def slow_step(self, ev: StartEvent) -> StopEvent:
            await asyncio.sleep(0.1)
            return StopEvent(result="done")

    wf = SlowWorkflow()
    ctx = Context(wf)
    handler1 = wf.run(ctx=ctx)
    with pytest.raises(ContextStateError, match="already running"):
        wf.run(ctx=ctx)
    await handler1  # Clean up


@pytest.mark.asyncio
async def test_concurrent_runs_different_contexts_ok() -> None:
    """Different contexts can run concurrently"""

    class SlowWorkflow(Workflow):
        @step
        async def slow_step(self, ev: StartEvent) -> StopEvent:
            await asyncio.sleep(0.01)
            return StopEvent(result="done")

    wf = SlowWorkflow()
    ctx1, ctx2 = Context(wf), Context(wf)
    h1 = wf.run(ctx=ctx1)
    h2 = wf.run(ctx=ctx2)  # Should NOT raise
    await h1
    await h2


@pytest.mark.asyncio
async def test_from_dict_then_sequential_runs() -> None:
    """Restored context should work for sequential runs"""
    wf = CounterWorkflow()
    ctx1 = Context(wf)
    await wf.run(ctx=ctx1)  # count -> 1
    ctx2 = Context.from_dict(wf, ctx1.to_dict())
    r = await wf.run(ctx=ctx2)  # count -> 2
    assert r == 2


class Session(DictState):
    """Typed state: declared fields plus DictState's dynamic keys."""

    turns: int = 0
    notes: dict[str, Any] = Field(default_factory=dict)


class ChatWorkflow(Workflow):
    """Workflow whose state is a DictState subclass."""

    @step
    async def turn(self, ctx: Context[Session], ev: StartEvent) -> StopEvent:
        async with ctx.store.edit_state() as state:
            state.turns += 1
            state.notes["last"] = "hello"
            state["extra"] = "kept"  # A dynamic key.
        return StopEvent(result=(await ctx.store.get_state()).turns)


@pytest.mark.asyncio
async def test_from_dict_preserves_dict_state_subclass_fields() -> None:
    """A DictState subclass must survive a context round trip with its fields."""
    wf = ChatWorkflow()
    handler = wf.run()
    assert await handler == 1
    live = await handler.ctx.store.get_state()
    assert type(live) is Session
    assert live.turns == 1

    restored = Context.from_dict(wf, handler.ctx.to_dict())
    back = await restored.store.get_state()
    assert type(back) is Session
    assert back.turns == 1
    assert back.notes == {"last": "hello"}
    assert back["extra"] == "kept"

    assert await wf.run(ctx=handler.ctx) == 2


@pytest.mark.asyncio
async def test_stream_events_twice_raises() -> None:
    """Streaming events twice on same handler should raise"""
    wf = CounterWorkflow()
    handler = wf.run()

    async for _ in handler.stream_events():
        pass

    with pytest.raises(WorkflowRuntimeError, match="already been consumed"):
        async for _ in handler.stream_events():
            pass
