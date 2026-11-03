# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio

import pytest
from workflows import Context, Workflow, step
from workflows.events import (
    Event,
    InputRequiredEvent,
    StartEvent,
    StepState,
    StepStateChanged,
    StopEvent,
)


class WaiterAnswer(Event):
    response: str


class WaitCompleted(Event):
    response: str


@pytest.mark.asyncio
@pytest.mark.parametrize("complete_first", [False, True])
async def test_answer_survives_parallel_requirements_refresh_after_resume(
    complete_first: bool,
) -> None:
    """Exercise real worker snapshots and wait_for_event without timing sleeps."""
    refresh_started = asyncio.Event()
    continuation_started = asyncio.Event()
    answer_consumed = asyncio.Event()
    release_refresh = asyncio.Event()
    release_continuation = asyncio.Event()
    release_finish = asyncio.Event()
    attempts = 0

    class ParallelWaiterWorkflow(Workflow):
        @step(num_workers=2)
        async def ask(self, ctx: Context, ev: StartEvent) -> WaitCompleted:
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                refresh_started.set()
                await release_refresh.wait()
            elif attempts == 3:
                continuation_started.set()
                await release_continuation.wait()
            answer = await ctx.wait_for_event(
                WaiterAnswer,
                waiter_id="answer",
                waiter_event=InputRequiredEvent(),
                requirements={"response": "yes"},
                timeout=None,
            )
            return WaitCompleted(response=answer.response)

        @step
        async def finish(self, ev: WaitCompleted) -> StopEvent:
            answer_consumed.set()
            # Keep the run alive until the stale registration is processed.
            await release_finish.wait()
            return StopEvent(result=ev.response)

    workflow = ParallelWaiterWorkflow(timeout=10.0)
    original = workflow.run()
    try:
        async for event in original.stream_events():
            if isinstance(event, InputRequiredEvent):
                break
        snapshot = original.ctx.to_dict()
    finally:
        await original.cancel_run()

    resumed = workflow.run(ctx=Context.from_dict(workflow, snapshot))
    stream = resumed.stream_events(expose_internal=True)
    try:
        await asyncio.wait_for(refresh_started.wait(), 5.0)
        resumed.ctx.send_event(WaiterAnswer(response="yes"))
        await asyncio.wait_for(continuation_started.wait(), 5.0)
        if complete_first:
            release_continuation.set()
            await asyncio.wait_for(answer_consumed.wait(), 5.0)
        release_refresh.set()
        async for event in stream:
            if (
                isinstance(event, StepStateChanged)
                and event.name == "ask"
                and event.step_state == StepState.NOT_RUNNING
                and event.output_event_name is None
            ):
                break
        else:
            pytest.fail("The refresh worker did not finish")

        waiters = [
            waiter
            for worker in resumed.ctx.to_dict()["workers"].values()
            for waiter in worker["collected_waiters"]
        ]
        if complete_first:
            assert waiters == []
        else:
            assert len(waiters) == 1
            assert waiters[0]["resolved_event"] is not None
        release_continuation.set()
        release_finish.set()
        assert await resumed == "yes"
        assert attempts == 3
    finally:
        release_refresh.set()
        release_continuation.set()
        release_finish.set()
        await resumed.cancel_run()
        await stream.aclose()
