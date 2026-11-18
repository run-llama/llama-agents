# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""A failed durable journal must not be reported as a successful workflow."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from llama_agents.server import (
    AbstractWorkflowStore,
    HandlerQuery,
    MemoryWorkflowStore,
    SqliteWorkflowStore,
    WorkflowServer,
)
from llama_agents_integration_tests.fake_agent_data import (
    FakeAgentDataBackend,
    create_agent_data_store,
)
from server_test_fixtures import (
    CumulativeWorkflow,
    InteractiveWorkflow,
    SimpleTestWorkflow,
)
from workflows.context.serializers import JsonSerializer
from workflows.runtime.types.ticks import TickAddEvent, TickStepResult


@pytest.fixture(params=["memory", "sqlite"])
def journal_store(
    request: pytest.FixtureRequest, tmp_path: Path
) -> AbstractWorkflowStore:
    if request.param == "sqlite":
        return SqliteWorkflowStore(str(tmp_path / "journal.db"), poll_interval=0.01)
    return MemoryWorkflowStore()


@pytest.mark.parametrize("stage", ["append", "barrier"])
async def test_journal_failure_marks_handler_failed_and_terminates_event_stream(
    journal_store: AbstractWorkflowStore,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    workflow = SimpleTestWorkflow() if stage == "append" else InteractiveWorkflow()
    server = WorkflowServer(workflow_store=journal_store, idle_timeout=60.0)
    server.add_workflow("test", workflow)
    append = journal_store.append_tick
    after_tick = journal_store.after_tick
    failed = False

    async def fail_append(run_id: str, tick_data: dict[str, Any]) -> None:
        nonlocal failed
        tick = JsonSerializer().deserialize(tick_data["serialized_tick"])
        if stage == "append" and isinstance(tick, TickStepResult) and not failed:
            failed = True
            raise OSError("journal append failed")
        await append(run_id, tick_data)

    async def fail_barrier(run_id: str, tick_data: dict[str, Any]) -> None:
        nonlocal failed
        if stage == "barrier" and not failed:
            failed = True
            raise OSError("journal barrier failed")
        await after_tick(run_id, tick_data)

    monkeypatch.setattr(journal_store, "append_tick", fail_append)
    monkeypatch.setattr(journal_store, "after_tick", fail_barrier)
    async with server.contextmanager():
        started = await server._service.start_workflow(workflow, "handler")
        assert started.run_id is not None
        result = await asyncio.wait_for(
            server._service.await_workflow(started), timeout=3
        )
        assert failed
        assert result.status == "failed"
        assert result.error is not None and f"journal {stage} failed" in result.error
        events = [
            event async for event in journal_store.subscribe_events(started.run_id)
        ]
        assert events[-1].event.type == "WorkflowFailedEvent"
        assert not any(event.event.type == "StopEvent" for event in events)
        persisted = await journal_store.query(HandlerQuery(handler_id_in=["handler"]))
        assert persisted[0].status == "failed"


@pytest.mark.parametrize("terminal_step", [False, True])
async def test_background_agent_data_tick_failure_fails_run(
    monkeypatch: pytest.MonkeyPatch,
    terminal_step: bool,
) -> None:
    backend = FakeAgentDataBackend()
    store = create_agent_data_store(backend, monkeypatch)
    original_create = store._client.create
    failed = False

    async def fail_tick(collection: str, data: dict[str, Any]) -> dict[str, Any]:
        nonlocal failed
        if collection == store._ticks_collection:
            tick = JsonSerializer().deserialize(data["tick_data"]["serialized_tick"])
            if isinstance(tick, TickStepResult) and not failed:
                failed = True
                raise OSError("background tick write failed")
        return await original_create(collection, data)

    monkeypatch.setattr(store._client, "create", fail_tick)
    workflow = SimpleTestWorkflow() if terminal_step else InteractiveWorkflow()
    server = WorkflowServer(workflow_store=store, idle_timeout=60.0)
    server.add_workflow("test", workflow)
    async with server.contextmanager():
        started = await server._service.start_workflow(workflow, "handler")
        assert started.run_id is not None
        result = await asyncio.wait_for(
            server._service.await_workflow(started), timeout=3
        )
        assert failed
        assert result.status == "failed"
        assert (
            result.error is not None and "background tick write failed" in result.error
        )
        events = [event async for event in store.subscribe_events(started.run_id)]
        assert events[-1].event.type == "WorkflowFailedEvent"


async def test_failed_start_tick_does_not_execute_workflow_step(
    journal_store: AbstractWorkflowStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow = CumulativeWorkflow()
    server = WorkflowServer(workflow_store=journal_store, idle_timeout=60.0)
    server.add_workflow("test", workflow)
    append = journal_store.append_tick
    attempts = 0

    async def fail_start(run_id: str, tick_data: dict[str, Any]) -> None:
        nonlocal attempts
        tick = JsonSerializer().deserialize(tick_data["serialized_tick"])
        if isinstance(tick, TickAddEvent):
            attempts += 1
            raise OSError("start tick unavailable")
        await append(run_id, tick_data)

    monkeypatch.setattr(journal_store, "append_tick", fail_start)
    async with server.contextmanager():
        started = await server._service.start_workflow(workflow, "handler")
        assert started.run_id is not None
        result = await asyncio.wait_for(
            server._service.await_workflow(started), timeout=3
        )
        assert result.status == "failed"
        assert attempts == 1
        state = journal_store.create_state_store(started.run_id)
        assert await state.get("count", None) is None


async def test_ambiguous_append_failure_does_not_retry_or_report_success(
    journal_store: AbstractWorkflowStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=journal_store, idle_timeout=60.0)
    server.add_workflow("test", workflow)
    append = journal_store.append_tick
    attempts = 0

    async def write_then_fail(run_id: str, tick_data: dict[str, Any]) -> None:
        nonlocal attempts
        tick = JsonSerializer().deserialize(tick_data["serialized_tick"])
        if isinstance(tick, TickStepResult):
            attempts += 1
            await append(run_id, tick_data)
            raise OSError("acknowledgement lost")
        await append(run_id, tick_data)

    monkeypatch.setattr(journal_store, "append_tick", write_then_fail)
    async with server.contextmanager():
        started = await server._service.start_workflow(workflow, "handler")
        assert started.run_id is not None
        result = await asyncio.wait_for(
            server._service.await_workflow(started), timeout=3
        )
        assert result.status == "failed"
        assert attempts == 1
        ticks = await journal_store.get_ticks(started.run_id)
        step_results = [
            JsonSerializer().deserialize(tick.tick_data["serialized_tick"])
            for tick in ticks
            if isinstance(
                JsonSerializer().deserialize(tick.tick_data["serialized_tick"]),
                TickStepResult,
            )
        ]
        assert len(step_results) == 1


async def test_failed_journal_is_not_replayed_after_sqlite_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "journal.db"
    store = SqliteWorkflowStore(str(path), poll_interval=0.01)
    original = store.append_tick
    failed = False

    async def lose_result(run_id: str, tick_data: dict[str, Any]) -> None:
        nonlocal failed
        if (
            isinstance(
                JsonSerializer().deserialize(tick_data["serialized_tick"]),
                TickStepResult,
            )
            and not failed
        ):
            failed = True
            raise OSError("journal unavailable")
        await original(run_id, tick_data)

    monkeypatch.setattr(store, "append_tick", lose_result)
    workflow = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=store, idle_timeout=60.0)
    server.add_workflow("test", workflow)
    async with server.contextmanager():
        started = await server._service.start_workflow(workflow, "handler")
        assert started.run_id is not None
        result = await asyncio.wait_for(
            server._service.await_workflow(started), timeout=3
        )
        assert result.status == "failed"
        before = await store.get_ticks(started.run_id)

    restarted_store = SqliteWorkflowStore(str(path), poll_interval=0.01)
    restarted = WorkflowServer(workflow_store=restarted_store, idle_timeout=60.0)
    restarted.add_workflow("test", SimpleTestWorkflow())
    async with restarted.contextmanager():
        persistence = restarted._runtime_core._persistence
        assert persistence is not None and persistence.resume_task is not None
        await asyncio.wait_for(persistence.resume_task, timeout=3)
        found = await restarted_store.query(HandlerQuery(handler_id_in=["handler"]))
        assert found[0].status == "failed"
        after = await restarted_store.get_ticks(started.run_id)
        assert [(item.sequence, item.tick_data) for item in after] == [
            (item.sequence, item.tick_data) for item in before
        ]


async def test_agent_data_tick_acknowledgement_precedes_workflow_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeAgentDataBackend()
    store = create_agent_data_store(backend, monkeypatch)
    original_create = store._client.create
    entered, release = asyncio.Event(), asyncio.Event()
    held = False

    async def gate_start(collection: str, data: dict[str, Any]) -> dict[str, Any]:
        nonlocal held
        if collection == store._ticks_collection:
            tick = JsonSerializer().deserialize(data["tick_data"]["serialized_tick"])
            if isinstance(tick, TickAddEvent) and not held:
                held = True
                entered.set()
                await release.wait()
        return await original_create(collection, data)

    monkeypatch.setattr(store._client, "create", gate_start)
    workflow = CumulativeWorkflow()
    server = WorkflowServer(workflow_store=store, idle_timeout=60.0)
    server.add_workflow("test", workflow)
    async with server.contextmanager():
        started = await server._service.start_workflow(workflow, "handler")
        assert started.run_id is not None
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            state = store.create_state_store(started.run_id)
            assert await state.get("count", None) is None
            handlers = await store.query(HandlerQuery(handler_id_in=["handler"]))
            assert handlers[0].status == "running"
            assert not any(
                event.event.type == "StopEvent"
                for event in await store.query_events(started.run_id)
            )
        finally:
            release.set()
        result = await asyncio.wait_for(
            server._service.await_workflow(started), timeout=3
        )
        assert result.status == "completed"
        assert await state.get("count") == 1
        ticks = await store.get_ticks(started.run_id)
        assert any(
            isinstance(
                JsonSerializer().deserialize(tick.tick_data["serialized_tick"]),
                TickAddEvent,
            )
            for tick in ticks
        )
