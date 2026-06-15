# ty: ignore[invalid-argument-type, invalid-assignment]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for idle workflow release and reload functionality."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from llama_agents.server import (
    AbstractWorkflowStore,
    HandlerQuery,
    MemoryWorkflowStore,
    PersistentHandler,
    SqliteWorkflowStore,
    WorkflowServer,
)
from llama_agents.server._runtime.idle_release_runtime import IdleReleaseDecorator
from llama_agents.server._runtime.persistence_runtime import (
    PersistenceDecorator,
)
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows import Context, Workflow, step
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import DictState, serialize_dict_state_data
from workflows.events import Event, HumanResponseEvent, StartEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerState, EventAttempt


class WaitableExternalEvent(HumanResponseEvent):
    response: str


class WaitingWorkflow(Workflow):
    """Workflow that uses ctx.wait_for_event() to become idle."""

    @step
    async def start_and_wait(self, ctx: Context, ev: StartEvent) -> None:
        pass

    @step
    async def end(self, ctx: Context, ev: WaitableExternalEvent) -> StopEvent:
        return StopEvent(result=f"received: {ev.response}")


def _get_idle_release(server: WorkflowServer) -> IdleReleaseDecorator:
    """Extract the IdleReleaseDecorator from the server's runtime stack."""
    inner = server._runtime._decorated
    assert isinstance(inner, IdleReleaseDecorator)
    return inner


def _get_persistence(server: WorkflowServer) -> PersistenceDecorator:
    """Extract the PersistenceDecorator from the server's runtime stack."""
    idle_release = _get_idle_release(server)
    assert isinstance(idle_release._persistence, PersistenceDecorator)
    return idle_release._persistence


async def wait_handler_status(
    store: AbstractWorkflowStore,
    handler_id: str,
    status: str,
    max_duration: float = 5.0,
    interval: float = 0.05,
) -> PersistentHandler:
    """Wait until a handler reaches the expected status."""

    async def check() -> PersistentHandler:
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].status == status
        return found[0]

    return await wait_for_passing(check, max_duration=max_duration, interval=interval)


async def wait_handler_idle(
    store: AbstractWorkflowStore,
    handler_id: str,
    max_duration: float = 5.0,
    interval: float = 0.05,
) -> PersistentHandler:
    """Wait until a handler has idle_since set."""

    async def check() -> PersistentHandler:
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].idle_since is not None
        return found[0]

    return await wait_for_passing(check, max_duration=max_duration, interval=interval)


async def wait_run_released(
    idle_release: IdleReleaseDecorator,
    run_id: str,
    max_duration: float = 2.0,
    interval: float = 0.01,
) -> None:
    """Wait until a run_id is no longer in the active run set."""

    async def check() -> None:
        assert run_id not in idle_release._active_run_ids

    await wait_for_passing(check, max_duration=max_duration, interval=interval)


async def wait_handler_idle_and_released(
    store: AbstractWorkflowStore,
    handler_id: str,
    idle_release: IdleReleaseDecorator,
    run_id: str,
    max_duration: float = 5.0,
) -> PersistentHandler:
    """Wait for the durable idle marker before asserting memory release."""
    handler = await wait_handler_idle(store, handler_id, max_duration=max_duration)
    await wait_run_released(idle_release, run_id, max_duration=max_duration)
    return handler


async def wait_state_value(
    store: AbstractWorkflowStore,
    run_id: str,
    key: str,
    expected: object,
    max_duration: float = 5.0,
    interval: float = 0.05,
) -> None:
    """Wait until the run state store contains the expected value."""

    async def check() -> None:
        state_store = store.create_state_store(run_id)
        assert await state_store.get(key) == expected

    await wait_for_passing(check, max_duration=max_duration, interval=interval)


@pytest.fixture
def waiting_workflow() -> WaitingWorkflow:
    return WaitingWorkflow()


@pytest.mark.asyncio
async def test_idle_handler_released_from_memory(
    memory_store: MemoryWorkflowStore, waiting_workflow: WaitingWorkflow
) -> None:
    """When a workflow becomes idle, its handler is released from memory."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", waiting_workflow)

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(
            waiting_workflow, "idle-release-1"
        )
        run_id = handler_data.run_id
        assert run_id is not None

        idle_release = _get_idle_release(server)

        handler = await wait_handler_idle_and_released(
            memory_store, "idle-release-1", idle_release, run_id
        )

        # Should still exist in store with status running.
        assert handler.status == "running"


@pytest.mark.asyncio
async def test_released_handler_reloaded_on_event(
    memory_store: MemoryWorkflowStore, waiting_workflow: WaitingWorkflow
) -> None:
    """A released idle handler is reloaded when an event is sent to it."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", waiting_workflow)

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(
            waiting_workflow, "reload-test-1"
        )
        run_id = handler_data.run_id
        assert run_id is not None

        idle_release = _get_idle_release(server)

        await wait_handler_idle_and_released(
            memory_store, "reload-test-1", idle_release, run_id
        )

        # Send event to wake it up
        await server._service.send_event(
            "reload-test-1", WaitableExternalEvent(response="hello")
        )

        # Handler should complete
        await wait_handler_status(
            memory_store, "reload-test-1", "completed", max_duration=2.0, interval=0.01
        )


@pytest.mark.asyncio
async def test_idle_since_cleared_on_reload(
    memory_store: MemoryWorkflowStore, waiting_workflow: WaitingWorkflow
) -> None:
    """idle_since is cleared in the store when a handler is reloaded."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", waiting_workflow)

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(
            waiting_workflow, "idle-clear-1"
        )
        run_id = handler_data.run_id
        assert run_id is not None

        idle_release = _get_idle_release(server)

        handler = await wait_handler_idle_and_released(
            memory_store, "idle-clear-1", idle_release, run_id
        )

        # Verify idle_since is set
        assert handler.idle_since is not None

        # Send event to trigger reload
        await server._service.send_event(
            "idle-clear-1", WaitableExternalEvent(response="wake")
        )

        # idle_since should be cleared after reload
        async def idle_since_cleared() -> None:
            found = await memory_store.query(
                HandlerQuery(handler_id_in=["idle-clear-1"])
            )
            assert found[0].idle_since is None

        await wait_for_passing(idle_since_cleared, max_duration=2.0, interval=0.01)


class FailingResumeWorkflow(Workflow):
    """Workflow that fails when resumed - used to test error handling in _on_server_start."""

    @step
    async def start_and_fail(self, ev: StartEvent) -> StopEvent:
        raise ValueError("Resume failed intentionally")


@pytest.mark.asyncio
async def test_on_server_start_marks_no_ticks_handler_as_failed(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """A handler with no persisted ticks cannot safely fresh-start.

    Such a row represents a zombie: the handler crashed before its first
    tick landed. Attempting a fresh run builds a StartEvent from empty
    kwargs, which fails for any workflow with required fields. Mark the
    handler as failed so it is not retried on every boot.
    """
    handler_id = "no-ticks-1"
    run_id = "run-no-ticks-1"
    await memory_store.update(
        PersistentHandler(
            handler_id=handler_id,
            workflow_name="test",
            status="running",
            run_id=run_id,
        )
    )

    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        handler = await wait_handler_status(memory_store, handler_id, "failed")
        assert handler.error is not None
        assert "crashed before persisting any state" in handler.error


@pytest.mark.asyncio
async def test_on_server_start_skips_handlers_created_within_resume_grace(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """A fresh request can create a running row before its first tick lands.

    Startup resume should not classify recently-created rows as crashed just
    because the first tick is not persisted yet. The grace window also covers
    small clock drift around the launch cutoff.
    """
    handler_id = "fresh-no-ticks-1"
    run_id = "run-fresh-no-ticks-1"
    resume_started_at = datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
    await memory_store.update(
        PersistentHandler(
            handler_id=handler_id,
            workflow_name="test",
            status="running",
            run_id=run_id,
            started_at=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        )
    )

    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)
    persistence = _get_persistence(server)

    await persistence._on_server_start(
        {"test": simple_test_workflow}, resume_started_at
    )

    found = await memory_store.query(HandlerQuery(handler_id_in=[handler_id]))
    assert found[0].status == "running"
    assert found[0].error is None


@pytest.mark.asyncio
async def test_on_server_start_finalizes_terminal_replay_as_completed(
    memory_store: MemoryWorkflowStore,
) -> None:
    """A handler whose ticks replay to a terminal StopEvent is marked completed."""
    from server_test_fixtures import SimpleTestWorkflow  # type: ignore[import]

    handler_id = "terminal-replay-1"

    # Phase 1: run the workflow to completion so ticks are persisted.
    server1 = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server1.add_workflow("test", SimpleTestWorkflow())
    async with server1.contextmanager():
        wf1 = server1._service._runtime.get_workflow("test")
        assert wf1 is not None
        await server1._service.start_workflow(wf1, handler_id)
        await wait_handler_status(memory_store, handler_id, "completed")

    # Phase 2: force the handler back to 'running' so the resume loop picks it
    # up on next boot — simulates the zombie-row scenario where ticks reached a
    # terminal state but the handler row was never updated.
    found = await memory_store.query(HandlerQuery(handler_id_in=[handler_id]))
    zombie = found[0]
    zombie.status = "running"
    zombie.result = None
    zombie.completed_at = None
    zombie.started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await memory_store.update(zombie)

    server2 = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server2.add_workflow("test", SimpleTestWorkflow())
    async with server2.contextmanager():
        handler = await wait_handler_status(memory_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "processed: default"


@pytest.mark.asyncio
async def test_on_server_start_finalizes_terminal_replay_as_failed(
    memory_store: MemoryWorkflowStore,
) -> None:
    """A handler whose ticks replay to a terminal failure is marked failed."""
    from server_test_fixtures import ErrorWorkflow  # type: ignore[import]

    handler_id = "terminal-replay-fail-1"

    server1 = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server1.add_workflow("failing", ErrorWorkflow())
    async with server1.contextmanager():
        wf1 = server1._service._runtime.get_workflow("failing")
        assert wf1 is not None
        await server1._service.start_workflow(wf1, handler_id)
        await wait_handler_status(memory_store, handler_id, "failed")

    # Force back to running to simulate the zombie state.
    found = await memory_store.query(HandlerQuery(handler_id_in=[handler_id]))
    zombie = found[0]
    zombie.status = "running"
    zombie.error = None
    zombie.completed_at = None
    zombie.started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await memory_store.update(zombie)

    server2 = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server2.add_workflow("failing", ErrorWorkflow())
    async with server2.contextmanager():
        handler = await wait_handler_status(memory_store, handler_id, "failed")
        assert handler.error is not None


@pytest.mark.asyncio
async def test_on_server_start_ignores_unregistered_workflows(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """Only handlers for registered workflows should be touched by resume.

    Both handlers have no ticks. The known-workflow handler gets classified
    and marked failed by the resume loop; the unknown-workflow handler is
    skipped entirely and stays untouched.
    """

    # Seed handlers for both registered and unregistered workflow
    await memory_store.update(
        PersistentHandler(
            handler_id="known-1",
            workflow_name="test",
            status="running",
            run_id="run-known-1",
        )
    )
    await memory_store.update(
        PersistentHandler(
            handler_id="unknown-1",
            workflow_name="not_registered",
            status="running",
            run_id="run-unknown-1",
        )
    )

    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        idle_release = _get_idle_release(server)

        # Known handler is classified and marked failed (no ticks → zombie)
        await wait_handler_status(memory_store, "known-1", "failed")

        # Unknown handler's run_id should NOT be in active runs
        assert "run-unknown-1" not in idle_release._active_run_ids

        # Unknown handler is untouched — still reports running
        unknown = await memory_store.query(HandlerQuery(handler_id_in=["unknown-1"]))
        assert unknown[0].status == "running"


@pytest.mark.asyncio
async def test_on_server_start_marks_failed_handler_on_error(
    memory_store: MemoryWorkflowStore,
) -> None:
    """If resume fails, handler should be marked as 'failed' in store."""
    handler_id = "fail-resume-1"
    run_id = "run-fail-resume-1"
    await memory_store.update(
        PersistentHandler(
            handler_id=handler_id,
            workflow_name="failing",
            status="running",
            run_id=run_id,
        )
    )

    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("failing", FailingResumeWorkflow())

    async with server.contextmanager():
        handler = await wait_handler_status(memory_store, handler_id, "failed")
        assert handler.error is not None


@pytest.mark.asyncio
async def test_on_server_start_ignores_idle_handlers(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """Idle handlers should NOT be resumed on server start."""
    handler_id = "idle-1"
    run_id = "run-idle-1"
    await memory_store.update(
        PersistentHandler(
            handler_id=handler_id,
            workflow_name="test",
            status="running",
            run_id=run_id,
            idle_since=datetime.now(timezone.utc),
        )
    )

    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        idle_release = _get_idle_release(server)
        persistence = _get_persistence(server)

        async def resume_done() -> None:
            assert persistence.resume_task is not None
            assert persistence.resume_task.done()

        await wait_for_passing(resume_done, max_duration=2.0, interval=0.05)
        assert run_id not in idle_release._active_run_ids


@pytest.mark.asyncio
async def test_destroy_cancels_resume_task(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """destroy() should cancel the resume_task."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        assert persistence.resume_task is not None

    # After contextmanager exits, destroy() was called.
    async def resume_task_stopped() -> None:
        resume_task = persistence.resume_task
        assert resume_task is not None
        assert resume_task.cancelled() or resume_task.done()

    await wait_for_passing(resume_task_stopped, max_duration=2.0, interval=0.01)


@pytest.mark.asyncio
async def test_destroy_aborts_active_runs(
    memory_store: MemoryWorkflowStore, waiting_workflow: WaitingWorkflow
) -> None:
    """destroy() should abort all active runs via _on_server_stop."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", waiting_workflow)

    async with server.contextmanager():
        idle_release = _get_idle_release(server)

        # Start a workflow that will stay running (waiting for event)
        await server._service.start_workflow(waiting_workflow, "destroy-test-1")

        async def run_is_active() -> None:
            assert len(idle_release._active_run_ids) > 0

        await wait_for_passing(run_is_active, max_duration=2.0, interval=0.01)

    # After exit, active runs should be cleared.
    async def active_runs_cleared() -> None:
        assert len(idle_release._active_run_ids) == 0

    await wait_for_passing(active_runs_cleared, max_duration=2.0, interval=0.01)


@pytest.mark.asyncio
async def test_ensure_active_run_handler_not_found(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """_ensure_active_run raises ValueError when no handler exists for run_id."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        idle_release = _get_idle_release(server)
        with pytest.raises(
            ValueError, match="Expected 1 handler for run nonexistent-run-id, got 0"
        ):
            await idle_release._ensure_active_run("nonexistent-run-id")


@pytest.mark.asyncio
async def test_ensure_active_run_workflow_not_found(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """_ensure_active_run raises ValueError when handler references unregistered workflow."""
    await memory_store.update(
        PersistentHandler(
            handler_id="h1",
            workflow_name="unregistered",
            status="running",
            run_id="run-unregistered",
        )
    )

    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        idle_release = _get_idle_release(server)
        with pytest.raises(ValueError, match="Workflow unregistered not found"):
            await idle_release._ensure_active_run("run-unregistered")


@pytest.mark.asyncio
async def test_context_from_ticks_empty_ticks(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """_context_from_ticks returns None when there are no ticks for the run_id."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        result = await persistence.context_from_ticks(
            simple_test_workflow, "run-id-with-no-ticks"
        )
        assert result is None


@pytest.mark.asyncio
async def test_persistence_retries_on_failure(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """Workflow completes despite transient store write failures thanks to retries."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)
    # Use instant retries
    server._runtime._persistence_backoff = [0, 0]

    original_update = memory_store.update_handler_status
    fail_count = 0

    async def flaky_update(*args: object, **kwargs: object) -> None:
        nonlocal fail_count
        fail_count += 1
        if fail_count <= 2:
            raise RuntimeError("transient store failure")
        await original_update(*args, **kwargs)  # type: ignore[arg-type]

    memory_store.update_handler_status = flaky_update  # type: ignore[assignment]

    async with server.contextmanager():
        await server._service.start_workflow(simple_test_workflow, "retry-ok-1")

        await wait_handler_status(memory_store, "retry-ok-1", "completed")

    assert fail_count > 2


@pytest.mark.asyncio
async def test_workflow_cancelled_after_all_retries_fail(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """When store writes always fail, handler never reaches completed status."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)
    # Use instant retries with only 2 attempts
    server._runtime._persistence_backoff = [0, 0]
    fail_count = 0

    async def always_fail(*args: object, **kwargs: object) -> None:
        nonlocal fail_count
        fail_count += 1
        raise RuntimeError("permanent store failure")

    memory_store.update_handler_status = always_fail  # type: ignore[assignment]

    async with server.contextmanager():
        await server._service.start_workflow(simple_test_workflow, "retry-fail-1")

        async def retries_exhausted() -> None:
            assert fail_count > 2

        await wait_for_passing(retries_exhausted, max_duration=2.0, interval=0.01)

        # The handler should NOT have reached "completed" status
        found = await memory_store.query(HandlerQuery(handler_id_in=["retry-fail-1"]))
        assert len(found) == 1
        assert found[0].status != "completed"


# --- Legacy ctx migration tests ---


def _make_legacy_ctx_v1(
    workflow: Workflow, *, state: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Build a minimal V1 SerializedContext dict with a StartEvent queued."""
    serializer = JsonSerializer()
    init_state = BrokerState.from_workflow(workflow)
    init_state.is_running = True
    # Queue a StartEvent for the first step
    first_step = next(iter(init_state.workers.keys()))
    init_state.workers[first_step].queue.append(
        EventAttempt(event=StartEvent(), attempts=0, first_attempt_at=None)
    )
    serialized = init_state.to_serialized(serializer)
    data = serialized.model_dump()
    if state:
        data["state"] = state
    return data


def _insert_handler_with_ctx(
    db_path: str,
    handler_id: str,
    run_id: str,
    workflow_name: str,
    ctx_data: dict[str, Any] | None = None,
) -> None:
    """Insert a handler row with optional ctx data directly via SQL."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO handlers (handler_id, workflow_name, status, run_id, ctx) VALUES (?, ?, ?, ?, ?)",
            (
                handler_id,
                workflow_name,
                "running",
                run_id,
                json.dumps(ctx_data) if ctx_data else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_legacy_ctx_no_ticks_resumes_workflow(
    sqlite_store: SqliteWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """Handler with old ctx data and no ticks should resume from old broker state."""
    ctx_data = _make_legacy_ctx_v1(simple_test_workflow)
    _insert_handler_with_ctx(
        sqlite_store.db_path, "legacy-1", "run-legacy-1", "test", ctx_data
    )

    server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        await wait_handler_status(sqlite_store, "legacy-1", "completed")


@pytest.mark.asyncio
async def test_legacy_ctx_seeds_user_state(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """Handler with old ctx containing user state should seed the state table."""

    class StatefulWorkflow(Workflow):
        @step
        async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
            val = await ctx.store.get("my_key", None)
            return StopEvent(result=f"my_key={val}")

    wf = StatefulWorkflow()
    serializer = JsonSerializer()

    # Build state data in the InMemory format
    dict_state = DictState()
    dict_state["my_key"] = "hello"
    state_data = {
        "store_type": "in_memory",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": serialize_dict_state_data(dict_state, serializer, ()),
    }
    ctx_data = _make_legacy_ctx_v1(wf, state=state_data)
    _insert_handler_with_ctx(
        sqlite_store.db_path, "state-1", "run-state-1", "test", ctx_data
    )

    server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server.add_workflow("test", wf)

    async with server.contextmanager():
        handler = await wait_handler_status(sqlite_store, "state-1", "completed")
        assert handler.result is not None
        assert handler.result.result == "my_key=hello"


@pytest.mark.asyncio
async def test_no_legacy_ctx_no_ticks_marked_failed(
    sqlite_store: SqliteWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """Handler with no ctx and no ticks is a zombie; mark it failed on resume."""
    _insert_handler_with_ctx(
        sqlite_store.db_path, "fresh-1", "run-fresh-1", "test", None
    )

    server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)

    async with server.contextmanager():
        handler = await wait_handler_status(sqlite_store, "fresh-1", "failed")
        assert handler.error is not None
        assert "crashed before persisting any state" in handler.error


@pytest.mark.asyncio
async def test_legacy_ctx_state_not_overwritten_on_second_resume(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """If state table already has data, legacy ctx should not overwrite it."""

    class CheckStateWorkflow(Workflow):
        @step
        async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
            val = await ctx.store.get("my_key", None)
            return StopEvent(result=f"my_key={val}")

    wf = CheckStateWorkflow()
    serializer = JsonSerializer()

    # Legacy ctx has old_value
    old_state = DictState()
    old_state["my_key"] = "old_value"
    state_data = {
        "store_type": "in_memory",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": serialize_dict_state_data(old_state, serializer, ()),
    }
    ctx_data = _make_legacy_ctx_v1(wf, state=state_data)
    _insert_handler_with_ctx(
        sqlite_store.db_path, "nooverwrite-1", "run-nooverwrite-1", "test", ctx_data
    )

    # Pre-seed the state table with new_value (simulating a previous partial run)
    state_store = sqlite_store.create_state_store("run-nooverwrite-1")
    await state_store.set("my_key", "new_value")

    server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server.add_workflow("test", wf)

    async with server.contextmanager():
        handler = await wait_handler_status(sqlite_store, "nooverwrite-1", "completed")
        # Should have the pre-seeded value, not the legacy ctx value
        assert handler.result is not None
        assert handler.result.result == "my_key=new_value"


# --- Multi-step HITL broker state resumption tests ---


class Step1Done(Event):
    value: str


class Step2Done(Event):
    value: str


class HumanInput1(Event):
    answer: str


class HumanInput2(Event):
    answer: str


class MultiStepHITLWorkflow(Workflow):
    """Three-step workflow with two human-in-the-loop wait points."""

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> Step1Done:
        await ctx.store.set("step", "started")
        return Step1Done(value="step1_complete")

    @step
    async def wait_for_human_1(self, ctx: Context, ev: Step1Done) -> HumanInput1:
        await ctx.store.set("step", "waiting_for_human_1")
        await ctx.store.set("step1_value", ev.value)
        human = await ctx.wait_for_event(HumanInput1)
        return human

    @step
    async def process_human_1(self, ctx: Context, ev: HumanInput1) -> Step2Done:
        await ctx.store.set("step", "processed_human_1")
        await ctx.store.set("human1_answer", ev.answer)
        return Step2Done(value="step2_complete")

    @step
    async def wait_for_human_2(self, ctx: Context, ev: Step2Done) -> HumanInput2:
        await ctx.store.set("step", "waiting_for_human_2")
        await ctx.store.set("step2_value", ev.value)
        human = await ctx.wait_for_event(HumanInput2)
        return human

    @step
    async def finalize(self, ctx: Context, ev: HumanInput2) -> StopEvent:
        await ctx.store.set("step", "finalized")
        step1 = await ctx.store.get("step1_value", "")
        human1 = await ctx.store.get("human1_answer", "")
        step2 = await ctx.store.get("step2_value", "")
        return StopEvent(result=f"{step1}|{human1}|{step2}|{ev.answer}")


HITL_EXTRA_EVENTS = [HumanInput1, HumanInput2]


@pytest.mark.asyncio
async def test_simple_hitl_cross_server_restart(
    sqlite_store: SqliteWorkflowStore,
    waiting_workflow: WaitingWorkflow,
) -> None:
    """Simple single-wait HITL workflow survives a full server restart."""
    handler_id = "simple-restart-1"

    # Server 1: start workflow, let it idle
    server1 = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server1.add_workflow("test", WaitingWorkflow())

    async with server1.contextmanager():
        wf1 = server1._service._runtime.get_workflow("test")
        assert wf1 is not None
        await server1._service.start_workflow(wf1, handler_id)

        await wait_handler_idle(sqlite_store, handler_id)

    # Server 2: send event, expect completion
    server2 = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server2.add_workflow("test", WaitingWorkflow())

    async with server2.contextmanager():
        await server2._service.send_event(
            handler_id, WaitableExternalEvent(response="hello")
        )

        await wait_handler_status(sqlite_store, handler_id, "completed")


@pytest.mark.asyncio
async def test_multistep_hitl_broker_state_survives_restart(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """Multi-step HITL workflow interrupted at each wait point, server restarted,
    resumes correctly with broker state and user state preserved."""
    handler_id = "hitl-1"

    def _make_server() -> WorkflowServer:
        wf = MultiStepHITLWorkflow()
        server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
        server.add_workflow("test", wf, additional_events=HITL_EXTRA_EVENTS)
        return server

    # Phase 1: Start workflow, let it reach first wait point, then stop server
    server1 = _make_server()

    async with server1.contextmanager():
        wf1 = server1._service._runtime.get_workflow("test")
        assert wf1 is not None
        await server1._service.start_workflow(wf1, handler_id)

        # Wait for handler to become idle (waiting for HumanInput1)
        await wait_handler_idle(sqlite_store, handler_id)

    # Verify state was persisted at first wait point
    run_id = (await sqlite_store.query(HandlerQuery(handler_id_in=[handler_id])))[
        0
    ].run_id
    assert run_id is not None
    state_store = sqlite_store.create_state_store(run_id)
    await wait_state_value(sqlite_store, run_id, "step", "waiting_for_human_1")
    assert await state_store.get("step1_value") == "step1_complete"

    # Phase 2: Restart server, send first human input, let it reach second wait
    server2 = _make_server()

    async with server2.contextmanager():
        await server2._service.send_event(handler_id, HumanInput1(answer="answer1"))

        async def handler_idle_2() -> None:
            found = await sqlite_store.query(HandlerQuery(handler_id_in=[handler_id]))
            assert len(found) == 1
            assert found[0].idle_since is not None
            # Verify we progressed past the first wait
            ss = sqlite_store.create_state_store(run_id)
            assert await ss.get("step") == "waiting_for_human_2"

        await wait_for_passing(handler_idle_2, max_duration=5.0, interval=0.05)

    # Verify state after second wait
    state_store2 = sqlite_store.create_state_store(run_id)
    assert await state_store2.get("human1_answer") == "answer1"
    assert await state_store2.get("step2_value") == "step2_complete"

    # Phase 3: Restart server again, send second human input, verify completion
    server3 = _make_server()

    async with server3.contextmanager():
        await server3._service.send_event(handler_id, HumanInput2(answer="answer2"))

        handler = await wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "step1_complete|answer1|step2_complete|answer2"


@pytest.mark.asyncio
async def test_multistep_hitl_multiple_restarts_at_same_wait_point(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """Server can be restarted multiple times while idle at the same wait point
    without losing state."""
    handler_id = "hitl-multi-restart"

    def _make_server() -> WorkflowServer:
        wf = MultiStepHITLWorkflow()
        server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
        server.add_workflow("test", wf, additional_events=HITL_EXTRA_EVENTS)
        return server

    # Start workflow, let it reach first wait point
    server1 = _make_server()

    async with server1.contextmanager():
        wf1 = server1._service._runtime.get_workflow("test")
        assert wf1 is not None
        await server1._service.start_workflow(wf1, handler_id)

        await wait_handler_idle(sqlite_store, handler_id)

    run_id = (await sqlite_store.query(HandlerQuery(handler_id_in=[handler_id])))[
        0
    ].run_id
    assert run_id is not None

    # Restart server 3 times without sending any events - state should be preserved
    for _ in range(3):
        server_n = _make_server()

        async with server_n.contextmanager():
            persistence = _get_persistence(server_n)

            # Wait for resume task to complete
            async def resume_done() -> None:
                assert persistence.resume_task is not None
                assert persistence.resume_task.done()

            await wait_for_passing(resume_done, max_duration=2.0, interval=0.05)

            # Handler should still be idle (not resumed by _on_server_start).
            handler = await wait_handler_idle(sqlite_store, handler_id)
            assert handler.status == "running"

    # State should still be intact after multiple restarts
    await wait_state_value(sqlite_store, run_id, "step", "waiting_for_human_1")

    # Now actually send the events and complete the workflow
    server_final = _make_server()

    async with server_final.contextmanager():
        await server_final._service.send_event(handler_id, HumanInput1(answer="final1"))

        async def idle_at_2() -> None:
            found = await sqlite_store.query(HandlerQuery(handler_id_in=[handler_id]))
            assert found[0].idle_since is not None
            ss = sqlite_store.create_state_store(run_id)
            assert await ss.get("step") == "waiting_for_human_2"

        await wait_for_passing(idle_at_2, max_duration=5.0, interval=0.05)

        await server_final._service.send_event(handler_id, HumanInput2(answer="final2"))

        handler = await wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "step1_complete|final1|step2_complete|final2"


@pytest.mark.asyncio
async def test_tick_content_after_multistep_workflow(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """After a multi-step workflow reaches idle, ticks are stored with expected structure."""
    handler_id = "tick-verify-1"

    wf = MultiStepHITLWorkflow()
    server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server.add_workflow("test", wf, additional_events=HITL_EXTRA_EVENTS)

    async with server.contextmanager():
        wf_ref = server._service._runtime.get_workflow("test")
        assert wf_ref is not None
        handler_data = await server._service.start_workflow(wf_ref, handler_id)
        run_id = handler_data.run_id
        assert run_id is not None

        # Wait for handler to become idle at first wait point
        await wait_handler_idle(sqlite_store, handler_id)

        # Verify ticks have expected structure
        ticks = await sqlite_store.get_ticks(run_id)
        assert len(ticks) > 0, "Expected at least one tick after workflow steps run"

        for tick in ticks:
            assert tick.run_id == run_id
            assert isinstance(tick.sequence, int)
            assert tick.sequence >= 0
            assert tick.timestamp is not None
            assert isinstance(tick.tick_data, dict)


@pytest.mark.asyncio
async def test_concurrent_send_event_to_idle_handler(
    memory_store: MemoryWorkflowStore, waiting_workflow: WaitingWorkflow
) -> None:
    """Two concurrent send_event calls to the same idle handler cause no unhandled exceptions."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", waiting_workflow)

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(
            waiting_workflow, "concurrent-1"
        )
        run_id = handler_data.run_id
        assert run_id is not None

        idle_release = _get_idle_release(server)

        await wait_handler_idle_and_released(
            memory_store, "concurrent-1", idle_release, run_id
        )

        # Fire two send_event calls concurrently
        results = await asyncio.gather(
            server._service.send_event(
                "concurrent-1", WaitableExternalEvent(response="first")
            ),
            server._service.send_event(
                "concurrent-1", WaitableExternalEvent(response="second")
            ),
            return_exceptions=True,
        )

        # At least one should succeed without error; the other may error or succeed
        exceptions = [r for r in results if isinstance(r, Exception)]
        successes = [r for r in results if not isinstance(r, Exception)]
        assert len(successes) >= 1, (
            f"Expected at least one success, got exceptions: {exceptions}"
        )

        # Handler should eventually complete (not hang or crash)
        await wait_handler_status(memory_store, "concurrent-1", "completed")


class FailAfterWaitEvent(Event):
    value: str


class FailAfterWaitWorkflow(Workflow):
    """Workflow that waits for an event then raises an error."""

    @step
    async def start_and_wait(self, ctx: Context, ev: StartEvent) -> StopEvent:
        external = await ctx.wait_for_event(FailAfterWaitEvent)
        if external.value == "error":
            raise RuntimeError("Error response received")
        return StopEvent(result=f"received: {external.value}")


@pytest.mark.asyncio
async def test_failed_workflow_after_reload(
    memory_store: MemoryWorkflowStore,
) -> None:
    """Workflow that raises after reload ends up with status='failed' and error message."""
    wf = FailAfterWaitWorkflow()
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", wf, additional_events=[FailAfterWaitEvent])

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(wf, "fail-reload-1")
        run_id = handler_data.run_id
        assert run_id is not None

        idle_release = _get_idle_release(server)

        await wait_handler_idle_and_released(
            memory_store, "fail-reload-1", idle_release, run_id
        )

        # Send error event to trigger RuntimeError in the workflow
        await server._service.send_event(
            "fail-reload-1", FailAfterWaitEvent(value="error")
        )

        # Handler should end up failed with error message
        handler = await wait_handler_status(memory_store, "fail-reload-1", "failed")
        assert handler.error is not None
        assert "Error response received" in handler.error


# --- State persistence across handler runs (counter pattern) ---


class IncrementEvent(HumanResponseEvent):
    pass


class CounterWorkflow(Workflow):
    """Workflow that increments a persistent counter each time it receives an event."""

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> None:
        count = await ctx.store.get("count", 0)
        await ctx.store.set("count", count + 1)

    @step
    async def wait_and_increment(self, ctx: Context, ev: IncrementEvent) -> StopEvent:
        count = await ctx.store.get("count", 0)
        new_count = count + 1
        await ctx.store.set("count", new_count)
        return StopEvent(result=f"count={new_count}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "store_factory",
    [
        pytest.param(lambda tmp: MemoryWorkflowStore(), id="memory"),
        pytest.param(
            lambda tmp: SqliteWorkflowStore(str(tmp / "test.db")), id="sqlite"
        ),
    ],
)
async def test_counter_state_persists_across_idle_reload(
    tmp_path: Path,
    store_factory: Any,
) -> None:
    """A counter workflow increments on start, goes idle, reloads on event,
    and the count reflects both increments."""
    store = store_factory(tmp_path)
    handler_id = "counter-1"

    wf = CounterWorkflow()
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow("counter", wf, additional_events=[IncrementEvent])

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(wf, handler_id)
        run_id = handler_data.run_id
        assert run_id is not None

        idle_release = _get_idle_release(server)
        await wait_handler_idle_and_released(store, handler_id, idle_release, run_id)

        # Verify count=1 after the start step
        await wait_state_value(store, run_id, "count", 1)

        # Send event to reload and increment again
        await server._service.send_event(handler_id, IncrementEvent())

        handler = await wait_handler_status(store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "count=2"
