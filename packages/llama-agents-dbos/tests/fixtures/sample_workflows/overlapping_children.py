# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Two child invocations that overlap across a process crash."""

from __future__ import annotations

import asyncio
from typing import Any

from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class FirstChildFinished(Event):
    pass


class FirstChildDone(Event):
    pass


class ChildStart(StartEvent):
    label: str


class ChildStop(StopEvent):
    label: str


class Child(Workflow):
    @step
    async def work(self, ev: ChildStart) -> ChildStop:
        if ev.label == "slow":
            await asyncio.sleep(2)
        return ChildStop(label=ev.label)


class OverlappingChildren(Workflow):
    child: Child

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.child = Child()

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        ctx.send_event(ChildStart(label="slow"))
        return ChildStart(label="fast")

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> FirstChildDone | StopEvent:
        labels = await ctx.store.get("labels", default=[])
        labels = [*labels, ev.label]
        await ctx.store.set("labels", labels)
        if len(labels) == 1:
            return FirstChildDone()
        if len(labels) == 2:
            return StopEvent(result=sorted(labels))
        raise AssertionError(f"Unexpected child result count: {labels}")

    @step
    async def mark_first_done(self, ctx: Context, ev: FirstChildDone) -> None:
        # The first finish step has returned and DBOS has recorded its result.
        ctx.write_event_to_stream(FirstChildFinished())
