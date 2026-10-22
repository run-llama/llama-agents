# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""End-to-end HTTP tests matrix-tested across workflow stores (memory, sqlite)."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Callable

import httpx
import pytest
from llama_agents.client.client import WorkflowClient
from llama_agents.server import (
    MemoryWorkflowStore,
    SqliteWorkflowStore,
    WorkflowServer,
)
from llama_agents.server._store.postgres_workflow_store import PostgresWorkflowStore
from llama_agents_integration_tests.fake_agent_data import (
    FakeAgentDataBackend,
    create_agent_data_store,
)
from llama_agents_integration_tests.postgres import get_asyncpg_dsn
from llama_agents_integration_tests.server_test_utils import (
    ExternalEvent,
    InteractiveWorkflow,
    SimpleTestWorkflow,
    StreamEvent,
    StreamingWorkflow,
    live_server,
    wait_for_passing,
    wait_for_requested_external_event_stream,
)
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent

# -- Utilities --


async def _get_handler_raw(base_url: str, handler_id: str) -> dict[str, Any]:
    """Get handler data without raising on non-200 status (server returns 500 for failed/cancelled)."""
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        resp = await client.get(f"/handlers/{handler_id}")
        return resp.json()  # type: ignore[no-any-return]


# -- Workflow definitions (test-specific) --


class ErrorWorkflow(Workflow):
    @step
    async def error_step(self, ev: StartEvent) -> StopEvent:
        raise ValueError("Test error")


# -- Fixtures --


@pytest.fixture(
    params=[
        "memory",
        "sqlite",
        "agent_data",
        pytest.param("postgres", marks=pytest.mark.docker),
    ]
)
async def server_with_store(
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[tuple[str, WorkflowServer, str], None]:
    store_type: str = request.param

    pg_store: PostgresWorkflowStore | None = None

    if store_type == "postgres":
        postgres_container = request.getfixturevalue("postgres_container")
        dsn = get_asyncpg_dsn(postgres_container)
        pg_store = PostgresWorkflowStore(dsn=dsn)
        await pg_store.start()

    def make_server() -> WorkflowServer:
        if store_type == "memory":
            store = MemoryWorkflowStore()
        elif store_type == "sqlite":
            db_path = tmp_path_factory.mktemp("sqlite") / "test.db"
            store = SqliteWorkflowStore(str(db_path))
        elif store_type == "postgres":
            assert pg_store is not None
            store = pg_store
        else:
            store = create_agent_data_store(FakeAgentDataBackend(), monkeypatch)
        server = WorkflowServer(workflow_store=store)
        server.add_workflow("test", SimpleTestWorkflow())
        server.add_workflow("streaming", StreamingWorkflow())
        server.add_workflow(
            "interactive", InteractiveWorkflow(), additional_events=[ExternalEvent]
        )
        server.add_workflow("error", ErrorWorkflow())
        return server

    async with live_server(make_server) as (base_url, server):
        try:
            yield base_url, server, store_type
        finally:
            if pg_store is not None:
                await pg_store.close()


# -- Tests --


@pytest.mark.asyncio
async def test_sync_run(server_with_store: tuple[str, WorkflowServer, str]) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)
    result = await client.run_workflow("test", start_event=StartEvent())
    assert result.status == "completed"
    assert result.result is not None
    assert result.result.value.get("result") == "processed: default"


@pytest.mark.asyncio
async def test_async_run_and_poll(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)
    started = await client.run_workflow_nowait("test")
    handler_id = started.handler_id

    async def check_completed() -> None:
        data = await client.get_handler(handler_id)
        assert data.status == "completed"
        assert data.result is not None
        assert data.result.value.get("result") == "processed: default"

    await wait_for_passing(check_completed)


@pytest.mark.asyncio
async def test_sse_event_streaming(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)
    started = await client.run_workflow_nowait("streaming")
    handler_id = started.handler_id

    events_seen: list[StreamEvent] = []
    async for ev in client.get_workflow_events(handler_id):
        event = ev.load_event([StreamEvent])
        if isinstance(event, StreamEvent):
            events_seen.append(event)

    assert len(events_seen) == 3
    assert [e.sequence for e in events_seen] == [0, 1, 2]


@pytest.mark.asyncio
async def test_send_external_event(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, store_type = server_with_store
    client = WorkflowClient(base_url=base_url)
    started = await client.run_workflow_nowait("interactive")
    handler_id = started.handler_id

    await wait_for_requested_external_event_stream(client, handler_id, label=store_type)
    sent = await client.send_event(handler_id, ExternalEvent(response="pong"))
    assert sent.status == "sent"

    async def check_completed() -> None:
        data = await client.get_handler(handler_id)
        assert data.status == "completed", (
            f"{store_type}: handler {handler_id} did not complete after external event"
        )
        assert data.result is not None
        assert data.result.value.get("result") == "received: pong"

    await wait_for_passing(check_completed)


@pytest.mark.asyncio
async def test_cancel_handler(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)
    started = await client.run_workflow_nowait("interactive")
    handler_id = started.handler_id

    # Wait for the workflow to reach the waiting state by polling
    async def is_running() -> None:
        data = await client.get_handler(handler_id)
        assert data.status == "running"

    await wait_for_passing(is_running)

    result = await client.cancel_handler(handler_id)
    assert result.status == "cancelled"

    async def check_cancelled() -> None:
        data = await _get_handler_raw(base_url, handler_id)
        assert data["status"] == "cancelled"

    await wait_for_passing(check_cancelled)


@pytest.mark.asyncio
async def test_list_handlers(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)

    # Run two workflows
    await client.run_workflow("test", start_event=StartEvent())
    await client.run_workflow("test", start_event=StartEvent())

    handlers = await client.get_handlers()
    assert len(handlers.handlers) >= 2


@pytest.mark.asyncio
async def test_error_workflow_status(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)
    started = await client.run_workflow_nowait("error")
    handler_id = started.handler_id

    async def check_failed() -> None:
        data = await _get_handler_raw(base_url, handler_id)
        assert data["status"] == "failed"
        assert data["error"] is not None
        assert "Test error" in data["error"]

    await wait_for_passing(check_failed)


@pytest.mark.asyncio
async def test_streaming_workflow_events(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)
    started = await client.run_workflow_nowait("streaming")
    handler_id = started.handler_id

    stream_events: list[StreamEvent] = []
    saw_stop = False
    async for ev in client.get_workflow_events(handler_id):
        event = ev.load_event([StreamEvent])
        if isinstance(event, StreamEvent):
            stream_events.append(event)
        elif isinstance(event, StopEvent):
            saw_stop = True

    assert len(stream_events) == 3
    assert saw_stop


@pytest.mark.asyncio
async def test_cursor_resume(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)

    # Run a streaming workflow synchronously first so all events are stored
    result = await client.run_workflow("streaming", start_event=StartEvent())
    assert result.status == "completed"
    handler_id = result.handler_id

    # Fetch events after sequence 1 using client API
    events = []
    async for ev in client.get_workflow_events(handler_id, after_sequence=1):
        events.append(ev)

    # Should have events after sequence 1 (i.e. sequence 2+ and StopEvent)
    assert len(events) >= 1


@pytest.mark.asyncio
async def test_concurrent_workflows(
    server_with_store: tuple[str, WorkflowServer, str],
) -> None:
    base_url, _server, _store_type = server_with_store
    client = WorkflowClient(base_url=base_url)

    # Start multiple workflows concurrently
    tasks = [client.run_workflow_nowait("test") for _ in range(5)]
    started = await asyncio.gather(*tasks)
    handler_ids = [s.handler_id for s in started]

    completed = await asyncio.gather(
        *(client.wait_for_handler(handler_id) for handler_id in handler_ids)
    )
    assert all(handler.status == "completed" for handler in completed)


# -- Durability tests (parameterized across durable stores) --


@pytest.fixture(
    params=[
        "sqlite",
        "agent_data",
        pytest.param("postgres", marks=pytest.mark.docker),
    ]
)
def durable_server_factory(
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[], WorkflowServer]:
    """Factory that creates a WorkflowServer with a durable store.

    Calling the factory multiple times reuses the same underlying storage,
    which is essential for restart/reload tests.
    """
    store_type: str = request.param

    if store_type == "sqlite":
        db_path = str(tmp_path_factory.mktemp("sqlite_durable") / "test.db")

        def make_server() -> WorkflowServer:
            store = SqliteWorkflowStore(db_path)
            server = WorkflowServer(workflow_store=store)
            server.add_workflow(
                "interactive", InteractiveWorkflow(), additional_events=[ExternalEvent]
            )
            return server

    elif store_type == "agent_data":
        backend = FakeAgentDataBackend()

        def make_server() -> WorkflowServer:
            store = create_agent_data_store(backend, monkeypatch)
            server = WorkflowServer(workflow_store=store)
            server.add_workflow(
                "interactive", InteractiveWorkflow(), additional_events=[ExternalEvent]
            )
            return server

    else:
        postgres_container = request.getfixturevalue("postgres_container")
        dsn = get_asyncpg_dsn(postgres_container)

        def make_server() -> WorkflowServer:
            pg_store = PostgresWorkflowStore(dsn=dsn)
            server = WorkflowServer(workflow_store=pg_store)
            server.add_workflow(
                "interactive", InteractiveWorkflow(), additional_events=[ExternalEvent]
            )
            return server

    return make_server


@pytest.mark.asyncio
async def test_server_restart_resumes_workflow(
    durable_server_factory: Callable[[], WorkflowServer],
) -> None:
    # Start workflow, then stop server
    async with live_server(durable_server_factory) as (base_url, _server):
        client = WorkflowClient(base_url=base_url)
        started = await client.run_workflow_nowait("interactive")
        handler_id = started.handler_id

        await wait_for_requested_external_event_stream(
            client, handler_id, label="durable-restart"
        )

    # Restart with same store - workflow should resume
    async with live_server(durable_server_factory) as (base_url2, _server2):
        client2 = WorkflowClient(base_url=base_url2)

        # Send the event to complete the workflow
        sent = await client2.send_event(
            handler_id, ExternalEvent(response="after_restart")
        )
        assert sent.status == "sent"

        async def check_completed() -> None:
            data = await client2.get_handler(handler_id)
            assert data.status == "completed"
            assert data.result is not None
            assert data.result.value.get("result") == "received: after_restart"

        await wait_for_passing(check_completed)


@pytest.mark.asyncio
async def test_idle_release_and_reload(
    durable_server_factory: Callable[[], WorkflowServer],
) -> None:
    async with live_server(durable_server_factory) as (base_url, _server):
        client = WorkflowClient(base_url=base_url)
        started = await client.run_workflow_nowait("interactive")
        handler_id = started.handler_id

        # Wait for handler to be running (idle internally)
        async def is_running() -> None:
            data = await client.get_handler(handler_id)
            assert data.status == "running"

        await wait_for_passing(is_running, max_duration=3.0)

        # Give time for idle release to happen
        await asyncio.sleep(0.5)

        # Send event via HTTP to trigger reload from idle state
        sent = await client.send_event(handler_id, ExternalEvent(response="after_idle"))
        assert sent.status == "sent"

        async def check_completed() -> None:
            data = await client.get_handler(handler_id)
            assert data.status == "completed"
            assert data.result is not None
            assert data.result.value.get("result") == "received: after_idle"

        await wait_for_passing(check_completed)
