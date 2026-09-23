# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Parent+child HITL fixture: the child waits for human input.

The child carries an explicit timeout so the idle-release/restart path exercises
the elapsed-alive budget (downtime is forgiven; a generous budget survives the
release window and resumes).
"""

from __future__ import annotations

from typing import Any

from pydantic import Field
from workflows.context import Context
from workflows.decorators import step
from workflows.events import (
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)
from workflows.workflow import Workflow


class AskInputEvent(InputRequiredEvent):
    prefix: str = Field(default="Enter: ")


class UserInput(HumanResponseEvent):
    response: str = Field(default="")


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    response: str = Field(default="")


class HitlChild(Workflow):
    @step
    async def ask(self, ctx: Context, ev: ChildStart) -> AskInputEvent:
        await ctx.store.set("child_asked", True)
        print("STEP:child_ask:complete", flush=True)
        return AskInputEvent()

    @step
    async def process(self, ctx: Context, ev: UserInput) -> ChildStop:
        await ctx.store.set("child_processed", ev.response)
        print("STEP:child_process:complete", flush=True)
        return ChildStop(response=ev.response)


class ChildHitlWorkflow(Workflow):
    child: HitlChild

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(timeout=None, **kwargs)
        # A generous child budget: survives the idle-release downtime (forgiven)
        # and resumes rather than firing on resume from accumulated wall time.
        self.child = HitlChild(timeout=30.0)

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        await ctx.store.set("parent_started", True)
        print("STEP:parent_start:complete", flush=True)
        return ChildStart()

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> StopEvent:
        await ctx.store.set("parent_finished", True)
        print("STEP:parent_finish:complete", flush=True)
        return StopEvent(result={"response": ev.response})
