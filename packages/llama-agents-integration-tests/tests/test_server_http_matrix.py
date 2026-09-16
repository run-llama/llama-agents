# ty: ignore[unknown-argument]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Parameterized live HTTP server integration tests across storage backends."""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path
from typing import Any, AsyncGenerator

import httpx
import pytest
import uvicorn
from dbos import DBOS, DBOSConfig
from llama_agents.client.client import WorkflowClient
from llama_agents.dbos import DBOSRuntime
from llama_agents.server import MemoryWorkflowStore, SqliteWorkflowStore, WorkflowServer
from llama_agents_integration_tests.server_test_utils import (
    ExternalEvent,
    InteractiveWorkflow,
    SimpleTestWorkflow,
    StreamingWorkflow,
    live_server,
    wait_for_passing,
    wait_for_requested_external_event_stream,
)
from testcontainers.postgres import PostgresContainer
from workflows import Context, Workflow, step
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
)


# Workflow definitions (test-specific)
class CumulativeWorkflow(Workflow):
    @step
    async def accumulate(self, ctx: Context, ev: StartEvent) -> StopEvent:
        current_count = await ctx.store.get("count", 0)
        increment = getattr(ev, "increment", 1)
        new_count = current_count + increment
        await ctx.store.set("count", new_count)
        run_history = await ctx.store.get("run_history", [])
        run_history.append(f"run_{len(run_history) + 1}")
        await ctx.store.set("run_history", run_history)
        return StopEvent(result=f"count: {new_count}, runs: {len(run_history)}")


class WaitableExternalEvent(Event):
    response: str


class WaitingWorkflow(Workflow):
    @step
    async def start_and_wait(self, ctx: Context, ev: StartEvent) -> StopEvent:
        external = await ctx.wait_for_event(WaitableExternalEvent)
        return StopEvent(result=f"received: {external.response}")


def _get_backend_params() -> list[Any]:
    return [
        pytest.param("memory", id="memory"),
        pytest.param("sqlite", id="sqlite"),
        pytest.param("postgres", marks=pytest.mark.docker, id="postgres"),
    ]


@pytest.fixture(scope="module")
def postgres_container(request: Any) -> Any:
    # Only start the container when docker-marked tests will actually run
    if not any(
        item.get_closest_marker("docker")
        for item in request.session.items
        if item.module is request.module
    ):
        yield None
        return
    with PostgresContainer("postgres:16", driver=None) as container:
        yield container


def _add_all_workflows(server: WorkflowServer) -> None:
    server.add_workflow("SimpleTestWorkflow", SimpleTestWorkflow())
    server.add_workflow("StreamingWorkflow", StreamingWorkflow())
    server.add_workflow("InteractiveWorkflow", InteractiveWorkflow())
    server.add_workflow("CumulativeWorkflow", CumulativeWorkflow())
    server.add_workflow("WaitingWorkflow", WaitingWorkflow())


_pg_server_state: dict[str, Any] = {}


async def _start_postgres_server(
    postgres_container: Any,
) -> tuple[str, WorkflowServer]:
    """Start a persistent postgres-backed server (called once per module)."""
    connection_url = postgres_container.get_connection_url()
    dbos_config: DBOSConfig = {
        "name": "wf-server-http-pg",
        "system_database_url": connection_url,
        "run_admin_server": False,
        "notification_listener_polling_interval_sec": 0.01,
    }
    DBOS(config=dbos_config)
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    await runtime.launch()
    store = runtime.create_workflow_store()
    server_runtime = runtime.build_server_runtime()

    server = WorkflowServer(workflow_store=store, runtime=server_runtime)
    _add_all_workflows(server)
    await server.start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]

    config = uvicorn.Config(
        server.app, host="127.0.0.1", port=port, log_level="error", loop="asyncio"
    )
    uv_server = uvicorn.Server(config)
    serve_task = asyncio.create_task(uv_server.serve(sockets=[sock]))

    base_url = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient(base_url=base_url, timeout=1.0) as client:
        for _ in range(50):
            try:
                resp = await client.get("/health")
                if resp.status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(0.01)
        else:
            uv_server.should_exit = True
            await serve_task
            raise RuntimeError("Postgres live server did not start in time")

    _pg_server_state["runtime"] = runtime
    _pg_server_state["uv_server"] = uv_server
    _pg_server_state["serve_task"] = serve_task
    _pg_server_state["sock"] = sock
    _pg_server_state["server"] = server
    _pg_server_state["base_url"] = base_url
    return base_url, server


async def _stop_postgres_server() -> None:
    if "uv_server" in _pg_server_state:
        _pg_server_state["uv_server"].should_exit = True
        try:
            await _pg_server_state["serve_task"]
        except Exception:
            pass
        _pg_server_state["sock"].close()
        await _pg_server_state["server"].stop()
        _pg_server_state.clear()


@pytest.fixture(scope="module")
async def postgres_server(
    postgres_container: Any,
) -> AsyncGenerator[tuple[str, WorkflowServer] | None, None]:
    """Module-scoped postgres server — DBOS is created and destroyed once."""
    if postgres_container is None:
        yield None
        return
    result = await _start_postgres_server(postgres_container)
    yield result
    await _stop_postgres_server()


@pytest.fixture
async def backend_server(
    request: Any,
    tmp_path: Path,
    postgres_server: tuple[str, WorkflowServer] | None,
) -> AsyncGenerator[tuple[str, WorkflowServer], None]:
    backend = request.param

    if backend == "memory":

        def factory() -> WorkflowServer:
            store = MemoryWorkflowStore()
            server = WorkflowServer(workflow_store=store)
            _add_all_workflows(server)
            return server

        async with live_server(factory) as (base_url, server):
            yield base_url, server

    elif backend == "sqlite":
        db_path = tmp_path / "test.db"

        def factory() -> WorkflowServer:
            store = SqliteWorkflowStore(db_path=str(db_path))
            server = WorkflowServer(workflow_store=store)
            _add_all_workflows(server)
            return server

        async with live_server(factory) as (base_url, server):
            yield base_url, server

    elif backend == "postgres":
        assert postgres_server is not None
        yield postgres_server

    else:
        raise ValueError(f"Unknown backend: {backend}")


# Tests
@pytest.mark.asyncio
@pytest.mark.parametrize("backend_server", _get_backend_params(), indirect=True)
async def test_basic_run_and_result(
    backend_server: tuple[str, WorkflowServer],
) -> None:
    base_url, server = backend_server
    client = WorkflowClient(base_url=base_url)

    start_event = StartEvent(message="test_message")  # type: ignore[call-arg]
    result = await client.run_workflow("SimpleTestWorkflow", start_event=start_event)
    assert result.result is not None
    assert result.result.value["result"] == "processed: test_message"


@pytest.mark.asyncio
@pytest.mark.parametrize("backend_server", _get_backend_params(), indirect=True)
async def test_streaming_and_interactive(
    backend_server: tuple[str, WorkflowServer],
) -> None:
    base_url, server = backend_server
    client = WorkflowClient(base_url=base_url)

    started = await client.run_workflow_nowait("InteractiveWorkflow")
    handler_id = started.handler_id

    await wait_for_requested_external_event_stream(
        client, handler_id, label=type(server._workflow_store).__name__
    )
    sent = await client.send_event(handler_id, ExternalEvent(response="pong"))
    assert sent.status == "sent"

    async def check_completed() -> str:
        handler = await client.get_handler(handler_id)
        assert handler.status == "completed"
        assert handler.result is not None
        return handler.result.value["result"]

    result = await wait_for_passing(check_completed, max_duration=5.0)
    assert result == "received: pong"


@pytest.mark.asyncio
@pytest.mark.parametrize("backend_server", _get_backend_params(), indirect=True)
async def test_reconnect_stream(
    backend_server: tuple[str, WorkflowServer],
) -> None:
    base_url, server = backend_server
    client = WorkflowClient(base_url=base_url)

    started = await client.run_workflow_nowait("InteractiveWorkflow")
    handler_id = started.handler_id

    prompt_cursor = await wait_for_requested_external_event_stream(
        client, handler_id, label=type(server._workflow_store).__name__
    )

    stop_seen = asyncio.Event()

    async def consume_again() -> None:
        async for ev in client.get_workflow_events(
            handler_id, after_sequence=prompt_cursor
        ):
            if ev.type != "StopEvent":
                continue
            event = ev.load_event([StopEvent])
            if isinstance(event, StopEvent):
                stop_seen.set()
                break

    consume_task = asyncio.create_task(consume_again())

    await asyncio.sleep(0.05)

    sent = await client.send_event(handler_id, ExternalEvent(response="reconnect_test"))
    assert sent.status == "sent"

    await asyncio.wait_for(stop_seen.wait(), timeout=5.0)
    consume_task.cancel()

    async def check_completed() -> str:
        handler = await client.get_handler(handler_id)
        assert handler.status == "completed"
        assert handler.result is not None
        return handler.result.value["result"]

    result = await wait_for_passing(check_completed, max_duration=5.0)
    assert result == "received: reconnect_test"


@pytest.mark.asyncio
@pytest.mark.parametrize("backend_server", _get_backend_params(), indirect=True)
async def test_cumulative_rerun(
    backend_server: tuple[str, WorkflowServer],
) -> None:
    base_url, server = backend_server
    client = WorkflowClient(base_url=base_url)

    start_event1 = StartEvent(increment=5)  # type: ignore[call-arg]
    result1 = await client.run_workflow("CumulativeWorkflow", start_event=start_event1)
    assert result1.result is not None
    result_str1 = result1.result.value["result"]
    assert "count: 5" in result_str1
    assert "runs: 1" in result_str1

    handler_id = result1.handler_id

    start_event2 = StartEvent(increment=3)  # type: ignore[call-arg]
    result2 = await client.run_workflow(
        "CumulativeWorkflow", handler_id=handler_id, start_event=start_event2
    )
    assert result2.result is not None
    result_str2 = result2.result.value["result"]
    assert "count: 8" in result_str2
    assert "runs: 2" in result_str2
