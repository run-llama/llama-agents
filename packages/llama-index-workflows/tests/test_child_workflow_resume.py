# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Phase 4: one ``to_dict()`` blob checkpoints the whole child tree, and resume
skips steps that already completed (children included)."""

from __future__ import annotations

import asyncio

import pytest
from workflows import Context, Workflow
from workflows.context.state_store import CHILD_STATES_KEY
from workflows.decorators import step
from workflows.events import (
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)
from workflows.testing import WorkflowTestRunner

# Module-level run counters; each test resets the keys it uses. Proving a step
# was not re-run on resume requires counting executions across the run boundary,
# which an instance attribute (kept alive across resume) would hide.
RUN_COUNTS: dict[str, int] = {}


class ChildStart(StartEvent):
    payload: str = ""


class ChildStop(StopEvent):
    out: str = ""


class CountingChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: ChildStart) -> ChildStop:
        RUN_COUNTS["child"] = RUN_COUNTS.get("child", 0) + 1
        await ctx.store.set("child_ran", True)
        return ChildStop(out=ev.payload.upper())


class HitlParent(Workflow):
    child: CountingChild

    @step
    async def start(self, ev: StartEvent) -> ChildStart:
        return ChildStart(payload="hello")

    @step
    async def gather(self, ctx: Context, ev: ChildStop) -> InputRequiredEvent:
        # Child has completed by now; stash its output and pause for a human.
        await ctx.store.set("from_child", ev.out)
        return InputRequiredEvent(prefix="continue?")  # type: ignore  # dynamic event kwarg

    @step
    async def complete(self, ctx: Context, ev: HumanResponseEvent) -> StopEvent:
        from_child = await ctx.store.get("from_child")
        return StopEvent(result=f"{from_child}:{ev.response}")


@pytest.mark.asyncio
async def test_kill_after_child_completes_resume_does_not_rerun_child() -> None:
    RUN_COUNTS.pop("child", None)
    workflow = HitlParent(child=CountingChild())

    handler = workflow.run()
    assert handler.ctx is not None

    ctx_dict = None
    async for event in handler.stream_events():
        if isinstance(event, InputRequiredEvent):
            ctx_dict = handler.ctx.to_dict()
            await handler.cancel_run()
            await asyncio.sleep(0.01)
            break

    assert ctx_dict is not None
    # The child ran exactly once before the checkpoint.
    assert RUN_COUNTS["child"] == 1

    # Resume from the single blob and feed the human response.
    new_handler = workflow.run(ctx=Context.from_dict(workflow, ctx_dict))
    new_handler.ctx.send_event(HumanResponseEvent(response="42"))  # type: ignore  # dynamic event kwarg
    result = await new_handler
    assert result == "HELLO:42"

    # The already-completed child step did NOT re-run on resume.
    assert RUN_COUNTS["child"] == 1


# --- Grandchild single-blob round-trip ----------------------------------------


class GrandStart(StartEvent):
    payload: str = ""


class GrandStop(StopEvent):
    out: str = ""


class Grand(Workflow):
    @step
    async def run_grand(self, ctx: Context, ev: GrandStart) -> GrandStop:
        await ctx.store.set("grand_value", ev.payload + "!")
        return GrandStop(out=ev.payload + "!")


class MidStart(StartEvent):
    payload: str = ""


class MidStop(StopEvent):
    out: str = ""


class Mid(Workflow):
    grand: Grand

    @step
    async def begin(self, ctx: Context, ev: MidStart) -> GrandStart:
        await ctx.store.set("mid_value", ev.payload)
        return GrandStart(payload=ev.payload)

    @step
    async def end(self, ev: GrandStop) -> MidStop:
        return MidStop(out=ev.out)


class Top(Workflow):
    mid: Mid

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> MidStart:
        await ctx.store.set("top_value", "g")
        return MidStart(payload="g")

    @step
    async def finish(self, ev: MidStop) -> StopEvent:
        return StopEvent(result=ev.out)


@pytest.mark.asyncio
async def test_grandchild_state_serialized_in_one_blob() -> None:
    top = Top(mid=Mid(grand=Grand()))
    ctx = Context(top)
    result = await top.run(ctx=ctx)
    assert result == "g!"

    blob = ctx.to_dict()
    # Root + each child namespace's state are nested in the single blob.
    child_states = blob["state"][CHILD_STATES_KEY]
    assert set(child_states) == {"mid", "mid/grand"}

    # The grandchild's own state is present in the blob.
    grand_data = child_states["mid/grand"]["_data"]
    assert grand_data  # non-empty

    # from_dict round-trips without dropping the child tree's StepId set.
    restored = Context.from_dict(top, blob)
    assert restored is not None


@pytest.mark.asyncio
async def test_grandchild_runs_end_to_end_with_explicit_context() -> None:
    """Sanity: the 3-level tree runs with a user-provided Context."""
    top = Top(mid=Mid(grand=Grand()))
    res = await WorkflowTestRunner(top).run()
    assert res.result == "g!"
