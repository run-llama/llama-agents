# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from typing import AsyncGenerator

import pytest
from workflows.context import Context
from workflows.decorators import step
from workflows.errors import WorkflowCancelledByUser
from workflows.events import Event, StartEvent, StopEvent, WorkflowCancelledEvent
from workflows.handler import WorkflowHandler
from workflows.plugins.basic import BasicRuntime
from workflows.workflow import Workflow


class StepReady(Event):
    pass


class WaitingWorkflow(Workflow):
    def __init__(self) -> None:
        super().__init__(timeout=None, runtime=BasicRuntime())
        self.release = asyncio.Event()
        self.cleaning = asyncio.Event()
        self.finish_cleanup = asyncio.Event()
        self.hold_cleanup = False

    @step
    async def work(self, ctx: Context, ev: StartEvent) -> StopEvent:
        ctx.write_event_to_stream(StepReady())
        try:
            await self.release.wait()
            return StopEvent(result="completed")
        finally:
            self.cleaning.set()
            if self.hold_cleanup:
                await self.finish_cleanup.wait()


@pytest.fixture
async def running_workflow() -> AsyncGenerator[
    tuple[WaitingWorkflow, WorkflowHandler, AsyncGenerator[Event, None]], None
]:
    workflow = WaitingWorkflow()
    handler = workflow.run()
    events = handler.stream_events()
    # Start the step before cancellation; cleanup tests also wait for cleaning.
    assert isinstance(await asyncio.wait_for(anext(events), timeout=2), StepReady)
    try:
        yield workflow, handler, events
    finally:
        workflow.release.set()
        workflow.finish_cleanup.set()
        await handler.cancel_run()
        await events.aclose()


async def _next_terminal(events: AsyncGenerator[Event, None]) -> StopEvent:
    pending = asyncio.create_task(anext(events))
    try:
        done, _ = await asyncio.wait({pending}, timeout=1)
        assert pending in done, "graceful cancellation left the event stream open"
        event = pending.result()
        assert isinstance(event, StopEvent)
        return event
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


async def _assert_graceful_result(handler: WorkflowHandler) -> None:
    observed: type[BaseException] | None = None
    try:
        await handler
    except BaseException as error:
        observed = type(error)
    assert observed is WorkflowCancelledByUser
    assert isinstance(handler.get_stop_event(), WorkflowCancelledEvent)


async def test_cancel_run_after_completion_preserves_result(
    running_workflow: tuple[
        WaitingWorkflow, WorkflowHandler, AsyncGenerator[Event, None]
    ],
) -> None:
    workflow, handler, events = running_workflow
    workflow.release.set()
    assert await handler == "completed"
    await handler.cancel_run(timeout=0)
    assert type(await _next_terminal(events)) is StopEvent
    assert await handler == "completed"


async def test_default_cancel_run_is_graceful(
    running_workflow: tuple[
        WaitingWorkflow, WorkflowHandler, AsyncGenerator[Event, None]
    ],
) -> None:
    _, handler, events = running_workflow
    await handler.cancel_run()
    assert isinstance(await _next_terminal(events), WorkflowCancelledEvent)
    await _assert_graceful_result(handler)


async def test_zero_cancel_wait_does_not_abort_workflow(
    running_workflow: tuple[
        WaitingWorkflow, WorkflowHandler, AsyncGenerator[Event, None]
    ],
) -> None:
    _, handler, events = running_workflow
    await handler.cancel_run(timeout=0)
    assert isinstance(await _next_terminal(events), WorkflowCancelledEvent)
    await _assert_graceful_result(handler)


async def test_cancel_wait_timeout_during_cleanup_preserves_result(
    running_workflow: tuple[
        WaitingWorkflow, WorkflowHandler, AsyncGenerator[Event, None]
    ],
) -> None:
    workflow, handler, events = running_workflow
    workflow.hold_cleanup = True
    cancelling = asyncio.create_task(handler.cancel_run(timeout=0.1))
    try:
        await asyncio.wait_for(workflow.cleaning.wait(), timeout=2)
        await cancelling
        workflow.finish_cleanup.set()
        assert isinstance(await _next_terminal(events), WorkflowCancelledEvent)
        await _assert_graceful_result(handler)
    finally:
        workflow.finish_cleanup.set()
        cancelling.cancel()
        await asyncio.gather(cancelling, return_exceptions=True)


async def test_cancel_run_waiter_cancellation_preserves_workflow_result(
    running_workflow: tuple[
        WaitingWorkflow, WorkflowHandler, AsyncGenerator[Event, None]
    ],
) -> None:
    workflow, handler, events = running_workflow
    workflow.hold_cleanup = True
    cancelling = asyncio.create_task(handler.cancel_run())
    try:
        await asyncio.wait_for(workflow.cleaning.wait(), timeout=2)
        cancelling.cancel()
        # Existing suppression of caller cancellation is outside this regression.
        await asyncio.gather(cancelling, return_exceptions=True)
        workflow.finish_cleanup.set()
        assert isinstance(await _next_terminal(events), WorkflowCancelledEvent)
        await _assert_graceful_result(handler)
    finally:
        workflow.finish_cleanup.set()
        cancelling.cancel()
        await asyncio.gather(cancelling, return_exceptions=True)
