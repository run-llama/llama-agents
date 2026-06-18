# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from llama_agents.server import HandlerQuery, SqliteWorkflowStore
from llama_agents.server._store.abstract_workflow_store import AbstractWorkflowStore
from llama_agents.server.runtime import _DurableWorkflowRuntime
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


async def _wait_for_running_tick(store: AbstractWorkflowStore, handler_id: str) -> str:
    async def check() -> str:
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].status == "running"
        assert found[0].run_id is not None
        ticks = await store.get_ticks(found[0].run_id)
        assert ticks
        return found[0].run_id

    return await wait_for_passing(check, max_duration=5.0, interval=0.05)


async def _wait_for_completed(
    store: AbstractWorkflowStore, handler_id: str
) -> StopEvent:
    async def check() -> StopEvent:
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        handler = found[0]
        assert handler.status == "completed"
        assert handler.result is not None
        return handler.result

    return await wait_for_passing(check, max_duration=5.0, interval=0.05)


async def _wait_for_idle(store: AbstractWorkflowStore, handler_id: str) -> None:
    async def check() -> None:
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].idle_since is not None

    await wait_for_passing(check, max_duration=5.0, interval=0.05)


@pytest.mark.asyncio
async def test_durable_workflow_runtime_resumes_sqlite_run(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "resume-in-process"

    store1 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime1 = _DurableWorkflowRuntime(workflow_store=store1)
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    started = await runtime1.run("waiting", handler_id=handler_id)
    run_id = await _wait_for_running_tick(store1, handler_id)
    assert started.run_id == run_id
    await runtime1.stop()

    store2 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime2 = _DurableWorkflowRuntime(workflow_store=store2)
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    await runtime2.send_event(handler_id, ResumeInput(response="done"))

    result = await _wait_for_completed(store2, handler_id)
    assert result.result == "True:done"
    await runtime2.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_can_skip_startup_resume(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "skip-resume-in-process"

    store1 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime1 = _DurableWorkflowRuntime(workflow_store=store1)
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    await runtime1.run("waiting", handler_id=handler_id)
    await _wait_for_running_tick(store1, handler_id)
    await runtime1.stop()

    store2 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime2 = _DurableWorkflowRuntime(
        workflow_store=store2,
        resume_existing=False,
    )
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    with pytest.raises(RuntimeError):
        await runtime2.send_event(handler_id, ResumeInput(response="early"))
    still_running = await store2.query(HandlerQuery(handler_id_in=[handler_id]))
    assert len(still_running) == 1
    assert still_running[0].status == "running"
    await runtime2.stop()

    store3 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime3 = _DurableWorkflowRuntime(workflow_store=store3)
    runtime3.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime3.start()
    await runtime3.send_event(handler_id, ResumeInput(response="late"))
    result = await _wait_for_completed(store3, handler_id)
    assert result.result == "True:late"
    await runtime3.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_run_returns_after_replayable_start(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "immediate-stop-in-process"

    store1 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime1 = _DurableWorkflowRuntime(workflow_store=store1)
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    await runtime1.run("waiting", handler_id=handler_id)
    await runtime1.stop()

    store2 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime2 = _DurableWorkflowRuntime(workflow_store=store2)
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    await runtime2.send_event(handler_id, ResumeInput(response="after-stop"))
    result = await _wait_for_completed(store2, handler_id)
    assert result.result == "True:after-stop"
    await runtime2.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_can_use_resume_grace(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "fresh-grace-in-process"

    store1 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime1 = _DurableWorkflowRuntime(workflow_store=store1)
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    await runtime1.run("waiting", handler_id=handler_id)
    await _wait_for_running_tick(store1, handler_id)
    await runtime1.stop()

    store2 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime2 = _DurableWorkflowRuntime(
        workflow_store=store2,
        resume_fresh_handler_grace=timedelta(days=1),
    )
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    skipped = await store2.query(HandlerQuery(handler_id_in=[handler_id]))
    assert len(skipped) == 1
    assert skipped[0].status == "running"
    await runtime2.stop()

    store3 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime3 = _DurableWorkflowRuntime(workflow_store=store3)
    runtime3.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime3.start()
    await runtime3.send_event(handler_id, ResumeInput(response="after-grace"))
    result = await _wait_for_completed(store3, handler_id)
    assert result.result == "True:after-grace"
    await runtime3.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_reloads_idle_handler_on_event(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "idle-reload-in-process"

    store1 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime1 = _DurableWorkflowRuntime(
        workflow_store=store1,
        idle_timeout=0.01,
    )
    runtime1.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime1.start()
    await runtime1.run("waiting", handler_id=handler_id)
    await _wait_for_idle(store1, handler_id)
    await runtime1.stop()

    store2 = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime2 = _DurableWorkflowRuntime(
        workflow_store=store2,
    )
    runtime2.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime2.start()
    await runtime2.send_event(handler_id, ResumeInput(response="woke-up"))
    result = await _wait_for_completed(store2, handler_id)
    assert result.result == "True:woke-up"
    await runtime2.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_rejects_duplicate_active_handler_id(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"
    handler_id = "duplicate-in-process"

    store = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime = _DurableWorkflowRuntime(workflow_store=store)
    runtime.add_workflow("waiting", InProcessWaitingWorkflow())
    await runtime.start()
    await runtime.run("waiting", handler_id=handler_id)
    await _wait_for_running_tick(store, handler_id)

    with pytest.raises(RuntimeError, match="already running"):
        await runtime.run("waiting", handler_id=handler_id)

    await runtime.stop()


@pytest.mark.asyncio
async def test_durable_workflow_runtime_returns_initial_handler_data(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "workflow.db"

    store = SqliteWorkflowStore(str(db_path), poll_interval=0.01)
    runtime = _DurableWorkflowRuntime(workflow_store=store)
    runtime.add_workflow("payload", InProcessPayloadWorkflow())
    await runtime.start()

    data = await runtime.run("payload", handler_id="payload-in-process")
    assert data.handler_id == "payload-in-process"
    result = await _wait_for_completed(store, "payload-in-process")
    assert result.result == "payload"

    await runtime.stop()
