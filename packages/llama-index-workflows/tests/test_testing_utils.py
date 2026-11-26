# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import Context, Workflow, step
from workflows.events import (
    Event,
    StartEvent,
    StepStateChanged,
    StopEvent,
)
from workflows.testing import WorkflowTestRunner
from workflows.testing.runner import WorkflowTestResult


class SecondEvent(Event):
    greeting: str


class SimpleWf(Workflow):
    @step
    async def step_one(self, ev: StartEvent, ctx: Context) -> SecondEvent:
        async with ctx.store.edit_state() as state:
            state.test = "this is a test"
        ctx.write_event_to_stream(SecondEvent(greeting="hello"))
        return SecondEvent(greeting="hello")

    @step
    async def step_two(self, ev: SecondEvent, ctx: Context) -> StopEvent:
        async with ctx.store.edit_state() as state:
            state.hello = "hello"
        return StopEvent(result="done")


class CountingStartWorkflow(Workflow):
    @step
    async def count(self, ev: StartEvent) -> StopEvent:
        ev["visits"] = ev.get("visits", 0) + 1
        return StopEvent(result=ev["visits"])


class DefaultStartEvent(StartEvent):
    greeting: str = "hello"


class DefaultStartWorkflow(Workflow):
    @step
    async def greet(self, ev: DefaultStartEvent) -> StopEvent:
        return StopEvent(result=ev.greeting)


@pytest.mark.asyncio
async def test_runner_creates_a_fresh_default_start_event() -> None:
    runner = WorkflowTestRunner(CountingStartWorkflow())
    assert (await runner.run()).result == 1
    assert (await runner.run()).result == 1
    assert (await WorkflowTestRunner(CountingStartWorkflow()).run()).result == 1


@pytest.mark.asyncio
async def test_runner_uses_the_workflows_default_start_event_type() -> None:
    workflow = DefaultStartWorkflow(timeout=0.5)
    assert await workflow.run() == "hello"
    assert (await WorkflowTestRunner(workflow).run()).result == "hello"


@pytest.mark.asyncio
async def test_runner_preserves_an_explicit_start_event() -> None:
    event = DefaultStartEvent(greeting="custom")
    result = await WorkflowTestRunner(DefaultStartWorkflow()).run(start_event=event)
    assert result.result == "custom"


@pytest.mark.asyncio
async def test_testing_utils() -> None:
    wf = SimpleWf()
    runner = WorkflowTestRunner(wf)
    wf_test_run = await runner.run(
        start_event=StartEvent(message="hi"),  # type: ignore
    )
    assert isinstance(wf_test_run, WorkflowTestResult)
    assert len(wf_test_run.collected) == sum(
        [wf_test_run.event_types[k] for k in wf_test_run.event_types]
    )
    assert wf_test_run.event_types.get(SecondEvent, 0) == 1
    assert wf_test_run.event_types.get(StopEvent, 0) == 1
    assert wf_test_run.event_types.get(StepStateChanged, 0) == len(
        wf_test_run.collected
    ) - (
        wf_test_run.event_types.get(SecondEvent, 0)
        + wf_test_run.event_types.get(StopEvent, 0)
    )
    assert str(wf_test_run.result) == "done"
