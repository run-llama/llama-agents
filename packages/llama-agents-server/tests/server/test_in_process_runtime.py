# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from llama_agents.server import (
    DurableWorkflowRuntime,
    HandlerQuery,
    SqliteWorkflowStore,
)
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows import Context, Workflow, step
from workflows.events import HumanResponseEvent, StartEvent, StopEvent


class ResumeInput(HumanResponseEvent):
    response: str


class InProcessWaitingWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> None:
        await ctx.store.set("started", True)

    @step
    async def finish(self, ctx: Context, ev: ResumeInput) -> StopEvent:
        started = await ctx.store.get("started")
        return StopEvent(result=f"{started}:{ev.response}")


class InProcessPayloadWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="payload")


async def _wait_for_running_tick(
    runtime: DurableWorkflowRuntime, handler_id: str
) -> str:
    async def check() -> str:
        found = await runtime.query_handlers(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].status == "running"
        assert found[0].run_id is not None
        ticks = await runtime.store.get_ticks(found[0].run_id)
        assert ticks
        return found[0].run_id

    return await wait_for_passing(check, max_duration=5.0, interval=0.05)


async def _wait_for_completed(
    runtime: DurableWorkflowRuntime, handler_id: str
) -> StopEvent:
    async def check() -> StopEvent:
        handler = await runtime.get_handler_status(handler_id)
        assert handler.status == "completed"
        assert handler.result is not None
        return handler.result

    return await wait_for_passing(check, max_duration=5.0, interval=0.05)


async def _wait_for_idle(runtime: DurableWorkflowRuntime, handler_id: str) -> None:
    async def check() -> None:
        found = await runtime.query_handlers(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].idle_since is not None

    await wait_for_passing(check, max_duration=5.0, interval=0.05)


@pytest.mark.asyncio
async def test_durable_workflow_runtime_resumes_sqlite_run(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "resume-in-process"

    runtime1 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    started = await runtime1.run("waiting", handler_id=handler_id)
    run_id = await _wait_for_running_tick(runtime1, handler_id)
    assert started.run_id == run_id
    assert not started.is_done()
    await runtime1.stop()
    assert started.is_done()

    runtime2 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    await runtime2.send_event(handler_id, ResumeInput(response="done"))

    result = await _wait_for_completed(runtime2, handler_id)
    assert result.result == "True:done"
    await runtime2.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_can_skip_startup_resume(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "skip-resume-in-process"

    runtime1 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    await runtime1.run("waiting", handler_id=handler_id)
    await _wait_for_running_tick(runtime1, handler_id)
    await runtime1.stop()

    runtime2 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01),
        resume_existing=False,
    )
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    with pytest.raises(RuntimeError):
        await runtime2.send_event(handler_id, ResumeInput(response="early"))
    with pytest.raises(RuntimeError, match="not active"):
        await runtime2.load_active_handler(handler_id)
    still_running = await runtime2.get_handler_status(handler_id)
    assert still_running.status == "running"
    await runtime2.stop()

    runtime3 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime3.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime3.start()
    await runtime3.send_event(handler_id, ResumeInput(response="late"))
    result = await _wait_for_completed(runtime3, handler_id)
    assert result.result == "True:late"
    await runtime3.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_can_use_resume_grace(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "fresh-grace-in-process"

    runtime1 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    await runtime1.run("waiting", handler_id=handler_id)
    await _wait_for_running_tick(runtime1, handler_id)
    await runtime1.stop()

    runtime2 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01),
        resume_fresh_handler_grace=timedelta(days=1),
    )
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    with pytest.raises(RuntimeError, match="not active"):
        await runtime2.load_active_handler(handler_id)
    skipped = await runtime2.get_handler_status(handler_id)
    assert skipped.status == "running"
    await runtime2.stop()

    runtime3 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime3.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime3.start()
    await runtime3.send_event(handler_id, ResumeInput(response="after-grace"))
    result = await _wait_for_completed(runtime3, handler_id)
    assert result.result == "True:after-grace"
    await runtime3.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_reloads_idle_handler_on_event(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "idle-reload-in-process"

    runtime1 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01),
        idle_timeout=0.01,
    )
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    await runtime1.run("waiting", handler_id=handler_id)
    await _wait_for_idle(runtime1, handler_id)
    await runtime1.stop()

    runtime2 = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01),
    )
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    with pytest.raises(RuntimeError, match="not active"):
        await runtime2.load_active_handler(handler_id)

    await runtime2.send_event(handler_id, ResumeInput(response="woke-up"))
    result = await _wait_for_completed(runtime2, handler_id)
    assert result.result == "True:woke-up"
    await runtime2.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_rejects_duplicate_active_handler_id(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "duplicate-in-process"

    runtime = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime.start()
    await runtime.run("waiting", handler_id=handler_id)
    await _wait_for_running_tick(runtime, handler_id)

    with pytest.raises(RuntimeError, match="already running"):
        await runtime.run("waiting", handler_id=handler_id)

    await runtime.stop()


@pytest.mark.asyncio
async def test_durable_workflow_handler_result_shapes(tmp_path: Path) -> None:
    db_path = tmp_path / "workflow.db"

    runtime = DurableWorkflowRuntime(
        workflow_store=SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    )
    runtime.add_workflow("payload", InProcessPayloadWorkflow())
    await runtime.start()

    handler = await runtime.run("payload", handler_id="payload-in-process")
    assert await handler.result() == "payload"
    stop_event = await handler.stop_event_result()
    assert isinstance(stop_event, StopEvent)
    assert stop_event.result == "payload"

    await runtime.stop()
