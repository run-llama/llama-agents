# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import pytest
from workflows import Context, Workflow, step
from workflows.errors import WorkflowRuntimeError
from workflows.events import Event, StartEvent, StopEvent


class Task(Event):
    idx: int


class Done(Event):
    idx: int


@pytest.mark.asyncio
async def test_static_list_return_emits_each_element() -> None:
    """A step returning ``list[Task]`` emits one event per element."""

    class FanOutWorkflow(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(idx=i) for i in range(3)]

        @step(num_workers=3)
        async def process(self, ev: Task) -> Done:
            return Done(idx=ev.idx)

        @step
        async def collect(self, ctx: Context, ev: Done) -> StopEvent | None:
            done = ctx.collect_events(ev, [Done] * 3)
            if done is None:
                return None
            return StopEvent(result=sorted(d.idx for d in done))

    result = await FanOutWorkflow(timeout=10).run()
    assert result == [0, 1, 2]


@pytest.mark.asyncio
async def test_empty_list_return_emits_nothing() -> None:
    """Returning ``[]`` completes the step without emitting or raising."""

    class EmptyFanOutWorkflow(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return []

        @step
        async def stop(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="ok")

        @step
        async def process(self, ev: Task) -> StopEvent | None:
            # Should never run: the empty fan-out emits no Task.
            return StopEvent(result="should-not-happen")

    result = await EmptyFanOutWorkflow(timeout=10).run()
    assert result == "ok"


@pytest.mark.asyncio
async def test_list_with_non_event_raises() -> None:
    """A list element that is not an Event raises a runtime error."""

    class BadFanOutWorkflow(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return ["not-an-event"]  # type: ignore

        @step
        async def process(self, ev: Task) -> StopEvent | None:
            return StopEvent(result="done")

    with pytest.raises(Exception):
        await BadFanOutWorkflow(timeout=10).run()


@pytest.mark.asyncio
async def test_non_fan_out_list_return_raises() -> None:
    """A runtime list only fans out when the signature declares list[E]."""

    class BadReturnWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return [StopEvent(result="not fan-out")]  # type: ignore[return-value]  # ty: ignore[invalid-return-type]

    with pytest.raises(WorkflowRuntimeError, match="returned list"):
        await BadReturnWorkflow(timeout=10).run()
