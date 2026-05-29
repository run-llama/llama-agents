# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
# ty: ignore[unknown-argument]
# pyright: reportCallIssue=false, reportArgumentType=false, reportPrivateUsage=false
"""Phase 4: each namespace gets its own state store, so a child's state writes
are invisible to the parent (and vice versa)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel
from workflows import Context, Workflow
from workflows.decorators import step
from workflows.events import StartEvent, StopEvent
from workflows.testing import WorkflowTestRunner


class CStart(StartEvent):
    pass


class CStop(StopEvent):
    val: str = ""


class IsoChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: CStart) -> CStop:
        # The parent's key must not be visible inside the child's store.
        assert await ctx.store.get("parent_key", None) is None
        await ctx.store.set("secret", "child-only")
        return CStop(val="ok")


class IsoParent(Workflow):
    child: IsoChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> CStart:
        await ctx.store.set("parent_key", "parent-only")
        return CStart()

    @step
    async def finish(self, ctx: Context, ev: CStop) -> StopEvent:
        # The child's key must not leak into the parent's store.
        assert await ctx.store.get("secret", None) is None
        return StopEvent(result=await ctx.store.get("parent_key"))


@pytest.mark.asyncio
async def test_child_state_invisible_to_parent() -> None:
    result = await WorkflowTestRunner(IsoParent(child=IsoChild())).run()
    assert result.result == "parent-only"
    # The handler's (root) store sees only the parent's writes.
    assert await result.ctx.store.get("parent_key") == "parent-only"
    assert await result.ctx.store.get("secret", None) is None


# --- Distinct typed state per namespace ---------------------------------------


class ParentState(BaseModel):
    p: int = 0


class TypedChildState(BaseModel):
    c: str = ""


class TStart(StartEvent):
    pass


class TStop(StopEvent):
    out: str = ""


class TypedChild(Workflow):
    @step
    async def run_child(self, ctx: Context[TypedChildState], ev: TStart) -> TStop:
        async with ctx.store.edit_state() as s:
            s.c = "child-typed"
        return TStop(out="done")


class TypedParent(Workflow):
    child: TypedChild

    @step
    async def start(self, ctx: Context[ParentState], ev: StartEvent) -> TStart:
        async with ctx.store.edit_state() as s:
            s.p = 7
        return TStart()

    @step
    async def finish(self, ctx: Context[ParentState], ev: TStop) -> StopEvent:
        s = await ctx.store.get_state()
        return StopEvent(result=s.p)


@pytest.mark.asyncio
async def test_distinct_typed_state_per_namespace() -> None:
    """Parent and child carry different typed state models in one run."""
    result = await WorkflowTestRunner(TypedParent(child=TypedChild())).run()
    assert result.result == 7
    root_state = await result.ctx.store.get_state()
    assert isinstance(root_state, ParentState)
    assert root_state.p == 7
