# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from llama_agents.server._store.abstract_workflow_store import (
    HandlerQuery,
    PersistentHandler,
)
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent


class BrokenWorkflow(Workflow):
    @step
    async def explode(self, ev: StartEvent) -> StopEvent:
        raise RuntimeError("something went wrong internally")


class OkWorkflow(Workflow):
    @step
    async def run_step(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


class CrashingStore(MemoryWorkflowStore):
    """Store that raises on specific methods to simulate persistence failures."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_update: bool = False
        self.fail_query: bool = False

    async def update(self, handler: PersistentHandler) -> None:
        if self.fail_update:
            raise ConnectionError("database connection lost")
        return await super().update(handler)

    async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
        if self.fail_query:
            raise ConnectionError("database connection lost")
        return await super().query(query)


@pytest.fixture
def server() -> WorkflowServer:
    server = WorkflowServer(workflow_store=MemoryWorkflowStore())
    server.add_workflow("broken", BrokenWorkflow())
    return server


@pytest_asyncio.fixture
async def client(server: WorkflowServer) -> AsyncGenerator[AsyncClient, None]:
    async with server.contextmanager():
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.mark.asyncio
async def test_malformed_json_returns_400(client: AsyncClient) -> None:
    # Start a workflow to get a handler_id
    response = await client.post(
        "/workflows/broken/run-nowait", json={"start_event": "{}"}
    )
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    # Send malformed JSON body
    response = await client.post(
        f"/events/{handler_id}",
        content=b"not json",
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 400, (
        f"Expected 400, got {response.status_code}: {response.text}"
    )
    assert "detail" in response.json()


@pytest.mark.asyncio
async def test_unhandled_exception_returns_json_500(
    client: AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.ERROR, logger="llama_agents.server._api"):
        response = await client.post(
            "/workflows/broken/run", json={"start_event": "{}"}
        )
    assert response.status_code == 500, (
        f"Expected 500, got {response.status_code}: {response.text}"
    )
    data = response.json()
    # Workflow failure returns handler data with error field
    assert data["error"] == "something went wrong internally"
    assert any("finished with status=failed" in r.message for r in caplog.records)


# -- Workflow not found errors --


@pytest.mark.asyncio
async def test_run_workflow_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.post(
        "/workflows/nonexistent/run", json={"start_event": "{}"}
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Workflow not found"


@pytest.mark.asyncio
async def test_run_nowait_workflow_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.post(
        "/workflows/nonexistent/run-nowait", json={"start_event": "{}"}
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Workflow not found"


@pytest.mark.asyncio
async def test_workflow_events_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.get("/workflows/nonexistent/events")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_workflow_schema_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.get("/workflows/nonexistent/schema")
    assert response.status_code == 404
    assert response.json()["detail"] == "Workflow not found"


@pytest.mark.asyncio
async def test_workflow_representation_not_found_returns_404(
    client: AsyncClient,
) -> None:
    response = await client.get("/workflows/nonexistent/representation")
    assert response.status_code == 404
    assert response.json()["detail"] == "Workflow not found"


# -- Handler not found errors --


@pytest.mark.asyncio
async def test_get_handler_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.get("/handlers/nonexistent-id")
    assert response.status_code == 404
    assert response.json()["detail"] == "Handler not found"


@pytest.mark.asyncio
async def test_get_result_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.get("/results/nonexistent-id")
    assert response.status_code == 404
    assert response.json()["detail"] == "Handler not found"


@pytest.mark.asyncio
async def test_stream_events_handler_not_found_returns_404(
    client: AsyncClient,
) -> None:
    response = await client.get("/events/nonexistent-id")
    assert response.status_code == 404
    assert response.json()["detail"] == "Handler not found"


@pytest.mark.asyncio
async def test_cancel_handler_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.post("/handlers/nonexistent-id/cancel")
    assert response.status_code == 404
    assert response.json()["detail"] == "Handler not found"


# -- Invalid request parameter errors --


@pytest.mark.asyncio
async def test_stream_events_invalid_after_sequence_returns_400(
    client: AsyncClient,
) -> None:
    # Start a workflow to get a valid handler_id
    response = await client.post(
        "/workflows/broken/run-nowait", json={"start_event": "{}"}
    )
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    response = await client.get(
        f"/events/{handler_id}", params={"after_sequence": "not-a-number"}
    )
    assert response.status_code == 400
    assert "after_sequence" in response.json()["detail"]


@pytest.mark.asyncio
async def test_post_event_missing_event_data_returns_400(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/workflows/broken/run-nowait", json={"start_event": "{}"}
    )
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    response = await client.post(f"/events/{handler_id}", json={})
    assert response.status_code == 400
    assert response.json()["detail"] == "Event data is required"


@pytest.mark.asyncio
async def test_post_event_handler_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.post(
        "/events/nonexistent-id", json={"event": {"type": "SomeEvent"}}
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Handler not found"


@pytest.mark.asyncio
async def test_run_workflow_invalid_json_returns_400(client: AsyncClient) -> None:
    response = await client.post(
        "/workflows/broken/run",
        content=b"not json",
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 400
    assert "Invalid JSON body" in response.json()["detail"]


@pytest.mark.asyncio
async def test_run_nowait_invalid_json_returns_400(client: AsyncClient) -> None:
    response = await client.post(
        "/workflows/broken/run-nowait",
        content=b"not json",
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 400
    assert "Invalid JSON body" in response.json()["detail"]


# -- Post event to completed workflow --


@pytest.mark.asyncio
async def test_post_event_to_completed_workflow_returns_409(
    client: AsyncClient,
) -> None:
    import asyncio

    response = await client.post(
        "/workflows/broken/run-nowait", json={"start_event": "{}"}
    )
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    # Poll until the handler reaches terminal status
    for _ in range(50):
        result = await client.get(f"/handlers/{handler_id}")
        if result.status_code == 200 and result.json().get("status") in (
            "completed",
            "failed",
        ):
            break
        await asyncio.sleep(0.1)

    response = await client.post(
        f"/events/{handler_id}",
        json={"event": {"type": "StartEvent", "data": "{}"}},
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "Workflow already completed"


# -- 500 errors: store/runtime failures --


def _make_crashing_server(
    store: CrashingStore,
) -> WorkflowServer:
    server = WorkflowServer(
        workflow_store=store,
        persistence_backoff=[],  # no retries, fail fast
    )
    server.add_workflow("ok", OkWorkflow())
    server.add_workflow("broken", BrokenWorkflow())
    return server


@pytest_asyncio.fixture
async def crashing_store_and_client() -> AsyncGenerator[
    tuple[CrashingStore, AsyncClient], None
]:
    store = CrashingStore()
    server = _make_crashing_server(store)
    async with server.contextmanager():
        # raise_app_exceptions=False lets Starlette's exception handlers return
        # JSON 500 responses instead of httpx re-raising the exception
        transport = ASGITransport(app=server.app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield store, client


_API_LOGGER = "llama_agents.server._api"
_SERVICE_LOGGER = "llama_agents.server._service"


@pytest.mark.asyncio
async def test_run_workflow_store_crash_returns_500(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Store fails during initial handler persistence -> 500."""
    store, client = crashing_store_and_client
    store.fail_update = True
    with caplog.at_level(logging.ERROR, logger=_API_LOGGER):
        response = await client.post("/workflows/ok/run", json={"start_event": "{}"})
    assert response.status_code == 500
    assert "detail" in response.json()
    assert any("Error running workflow" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_run_nowait_store_crash_returns_500(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
) -> None:
    """Store fails during initial handler persistence on run-nowait -> 500."""
    store, client = crashing_store_and_client
    store.fail_update = True
    response = await client.post("/workflows/ok/run-nowait", json={"start_event": "{}"})
    assert response.status_code == 500
    assert "Initial persistence failed" in response.json()["detail"]


@pytest.mark.asyncio
async def test_run_workflow_step_crash_returns_500_with_error(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Workflow step raises -> sync run returns 500 with error in body."""
    _, client = crashing_store_and_client
    with caplog.at_level(logging.ERROR):
        response = await client.post(
            "/workflows/broken/run", json={"start_event": "{}"}
        )
    assert response.status_code == 500
    data = response.json()
    assert data["status"] == "failed"
    assert data["error"] == "something went wrong internally"
    # The service layer logs the exception
    assert any("raised an exception" in r.message for r in caplog.records)
    # The API layer logs the failed status
    assert any("finished with status=failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_run_nowait_step_crash_still_returns_200(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
) -> None:
    """run-nowait returns 200 immediately; failure is only visible via handler poll."""
    _, client = crashing_store_and_client
    response = await client.post(
        "/workflows/broken/run-nowait", json={"start_event": "{}"}
    )
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    # Poll until the handler reaches terminal status
    for _ in range(50):
        result = await client.get(f"/handlers/{handler_id}")
        if result.json().get("status") == "failed":
            break
        await asyncio.sleep(0.1)
    else:
        pytest.fail("handler never reached 'failed' status")
    assert result.json()["status"] == "failed"
    assert result.json()["error"] == "something went wrong internally"


_RUNTIME_LOGGER = "llama_agents.server._runtime.server_runtime"


@pytest.mark.asyncio
async def test_run_nowait_step_crash_logs_error(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Async workflow failure is logged by the server runtime adapter."""
    _, client = crashing_store_and_client
    with caplog.at_level(logging.ERROR, logger=_RUNTIME_LOGGER):
        response = await client.post(
            "/workflows/broken/run-nowait", json={"start_event": "{}"}
        )
        assert response.status_code == 200
        handler_id = response.json()["handler_id"]

        # Wait for the workflow to fail
        for _ in range(50):
            result = await client.get(f"/handlers/{handler_id}")
            if result.json().get("status") == "failed":
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("handler never reached 'failed' status")

    runtime_records = [r for r in caplog.records if r.name == _RUNTIME_LOGGER]
    assert any(
        "something went wrong internally" in r.message for r in runtime_records
    ), f"Expected runtime error log, got: {[r.message for r in runtime_records]}"


@pytest.mark.asyncio
async def test_get_handler_store_crash_returns_500(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Store.query() fails when fetching handler -> unhandled exception -> 500."""
    store, client = crashing_store_and_client
    response = await client.post("/workflows/ok/run-nowait", json={"start_event": "{}"})
    handler_id = response.json()["handler_id"]

    store.fail_query = True
    with caplog.at_level(logging.ERROR, logger=_API_LOGGER):
        response = await client.get(f"/handlers/{handler_id}")
    assert response.status_code == 500
    assert "detail" in response.json()
    assert any("Unhandled exception" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cancel_handler_store_crash_returns_500(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Store.query() fails when cancelling handler -> unhandled exception -> 500."""
    store, client = crashing_store_and_client
    response = await client.post("/workflows/ok/run-nowait", json={"start_event": "{}"})
    handler_id = response.json()["handler_id"]

    store.fail_query = True
    with caplog.at_level(logging.ERROR, logger=_API_LOGGER):
        response = await client.post(f"/handlers/{handler_id}/cancel")
    assert response.status_code == 500
    assert "detail" in response.json()
    assert any("Unhandled exception" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_post_event_store_crash_returns_500(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Store.query() fails when resolving handler for event send -> 500."""
    store, client = crashing_store_and_client
    response = await client.post("/workflows/ok/run-nowait", json={"start_event": "{}"})
    handler_id = response.json()["handler_id"]

    store.fail_query = True
    with caplog.at_level(logging.ERROR, logger=_API_LOGGER):
        response = await client.post(
            f"/events/{handler_id}",
            json={"event": {"type": "StartEvent", "data": "{}"}},
        )
    assert response.status_code == 500
    assert "detail" in response.json()
    assert any("Unhandled exception" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_stream_events_store_crash_returns_500(
    crashing_store_and_client: tuple[CrashingStore, AsyncClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Store.query() fails when resolving event stream -> 500."""
    store, client = crashing_store_and_client
    response = await client.post("/workflows/ok/run-nowait", json={"start_event": "{}"})
    handler_id = response.json()["handler_id"]

    store.fail_query = True
    with caplog.at_level(logging.ERROR, logger=_API_LOGGER):
        response = await client.get(f"/events/{handler_id}")
    assert response.status_code == 500
    assert "detail" in response.json()
    assert any("Unhandled exception" in r.message for r in caplog.records)
