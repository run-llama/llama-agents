# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import json
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator, AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient, Response
from llama_agents.server import (
    HandlerQuery,
    MemoryWorkflowStore,
    PersistentHandler,
    WorkflowServer,
)
from llama_index_instrumentation.dispatcher import active_instrument_tags
from server_test_fixtures import (
    ExternalEvent,  # type: ignore[import]
    wait_for_passing,  # type: ignore[import]
    wait_for_requested_external_event,  # type: ignore[import]
)
from workflows import Context, step

# Prepare the event to send
from workflows.context.context_types import SerializedContext
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import DictState, InMemoryStateStore
from workflows.events import Event, StartEvent, StopEvent
from workflows.workflow import Workflow


class CustomStopEvent(StopEvent):
    message: str


class CustomStopWorkflow(Workflow):
    @step
    async def finish(self, ev: StartEvent) -> CustomStopEvent:
        return CustomStopEvent(message="custom-completed")


async def serialize_context(state_dict: dict[str, Any]) -> SerializedContext:
    ser_context = SerializedContext()
    state = InMemoryStateStore(DictState())
    for key, value in state_dict.items():
        await state.set(key, value)
    ser_context.state = state.to_dict(JsonSerializer())
    return ser_context


@pytest.fixture
def server(
    simple_test_workflow: Workflow,
    error_workflow: Workflow,
    streaming_workflow: Workflow,
    interactive_workflow: Workflow,
) -> WorkflowServer:
    # Use MemoryWorkflowStore so get_handlers() can retrieve from persistence
    server = WorkflowServer(workflow_store=MemoryWorkflowStore(), idle_timeout=0.01)
    server.add_workflow("test", simple_test_workflow)
    server.add_workflow("error", error_workflow)
    server.add_workflow("streaming", streaming_workflow)
    server.add_workflow("interactive", interactive_workflow)
    return server


@pytest.fixture
def context_server(
    simple_test_workflow: Workflow,
) -> WorkflowServer:
    server = WorkflowServer(
        workflow_store=MemoryWorkflowStore(),
        idle_timeout=0.01,
        accept_context_api=True,
    )
    server.add_workflow("test", simple_test_workflow)
    return server


@pytest_asyncio.fixture
async def client(
    server: WorkflowServer,
) -> AsyncGenerator:
    async with server.contextmanager():
        transport = ASGITransport(app=server.app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest_asyncio.fixture
async def context_client(
    context_server: WorkflowServer,
) -> AsyncGenerator:
    async with context_server.contextmanager():
        transport = ASGITransport(app=context_server.app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@asynccontextmanager
async def server_with_persisted_handlers(
    interactive_workflow: Workflow,
    *,
    persisted_handlers: list[PersistentHandler] | None = None,
) -> AsyncIterator[tuple[WorkflowServer, AsyncClient, MemoryWorkflowStore]]:
    store = MemoryWorkflowStore()
    if persisted_handlers is not None:
        for handler in persisted_handlers:
            await store.update(handler)

    server_with_store = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server_with_store.add_workflow("interactive", interactive_workflow)

    async with server_with_store.contextmanager():
        transport = ASGITransport(app=server_with_store.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield server_with_store, client, store


@pytest.mark.asyncio
async def test_run_workflow_with_start_event_str_plain(
    client: AsyncClient,
) -> None:
    # Provide start_event as a plain JSON string (no discriminators)
    start_event_json = json.dumps({"message": "plain string start"})
    response = await client.post(
        "/workflows/test/run", json={"start_event": start_event_json}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["result"]["value"]["result"] == "processed: plain string start"


@pytest.mark.asyncio
async def test_run_workflow_with_start_event_dict_with_discriminators(
    client: AsyncClient,
) -> None:
    # Provide start_event as a dict with pydantic discriminators
    start_event_dict = {
        "__is_pydantic": True,
        "value": {"_data": {"message": "dict with discriminators"}},
        "qualified_name": "workflows.events.StartEvent",
    }
    response = await client.post(
        "/workflows/test/run", json={"start_event": start_event_dict}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["result"]["value"]["result"] == "processed: dict with discriminators"


@pytest.mark.asyncio
async def test_run_workflow_with_start_event_dict_plain(
    client: AsyncClient,
) -> None:
    # Provide start_event as a plain dict (no discriminators)
    start_event_dict = {"message": "plain dict start"}
    response = await client.post(
        "/workflows/test/run", json={"start_event": start_event_dict}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["result"]["value"]["result"] == "processed: plain dict start"


@pytest.mark.asyncio
async def test_run_workflow_with_nonconforming_start_event_type(
    client: AsyncClient,
) -> None:
    # Provide start_event of a different event type than the workflow's StartEvent
    wrong_event_dict = {
        "__is_pydantic": True,
        "value": {"_data": {"message": "should fail"}},
        "qualified_name": "workflows.events.StopEvent",
    }
    response = await client.post(
        "/workflows/test/run", json={"start_event": wrong_event_dict}
    )
    assert response.status_code == 400
    assert "Start event must be an instance of" in response.text


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


@pytest.mark.asyncio
async def test_health_check_returns_503_when_not_launched(
    server: WorkflowServer,
) -> None:
    """Health endpoint returns 503 when runtime is not launched."""
    from unittest.mock import PropertyMock, patch

    with patch.object(
        type(server._service._runtime),
        "is_launched",
        new_callable=PropertyMock,
        return_value=False,
    ):
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health")
    assert response.status_code == 503
    assert response.json() == {"status": "unhealthy"}


@pytest.mark.asyncio
async def test_list_workflows(client: AsyncClient) -> None:
    response = await client.get("/workflows")
    assert response.status_code == 200
    data = response.json()
    assert "workflows" in data
    assert set(data["workflows"]) == {"test", "error", "streaming", "interactive"}


@pytest.mark.asyncio
async def test_run_workflow_success(client: AsyncClient) -> None:
    response = await client.post(
        "/workflows/test/run", json={"kwargs": {"message": "hello"}}
    )
    assert response.status_code == 200
    data = response.json()
    assert "result" in data
    assert data["result"]["value"]["result"] == "processed: hello"


@pytest.mark.asyncio
async def test_run_workflow_no_kwargs(client: AsyncClient) -> None:
    response = await client.post("/workflows/test/run", json={})
    assert response.status_code == 200
    data = response.json()
    assert data["result"]["value"]["result"] == "processed: default"


@pytest.mark.asyncio
async def test_run_workflow_with_context(context_client: AsyncClient) -> None:
    ctx_dict = (
        await serialize_context({"test_param": "message from context"})
    ).model_dump(mode="python")
    response = await context_client.post(
        "/workflows/test/run", json={"context": ctx_dict}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["result"]["value"]["result"] == "processed: message from context"


@pytest.mark.asyncio
async def test_run_workflow_context_rejected_by_default(client: AsyncClient) -> None:
    ctx_dict = (
        await serialize_context({"test_param": "message from context"})
    ).model_dump(mode="python")
    response = await client.post("/workflows/test/run", json={"context": ctx_dict})
    assert response.status_code == 400
    assert "Context API is disabled" in response.json()["detail"]


@pytest.mark.asyncio
async def test_run_workflow_with_start_event(client: AsyncClient) -> None:
    # Test with simple start event containing message
    start_event_json = '{"__is_pydantic": true, "value": {"_data": {"message": "start event message"}}, "qualified_name": "workflows.events.StartEvent"}'
    response = await client.post(
        "/workflows/test/run", json={"start_event": start_event_json}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["result"]["value"]["result"] == "processed: start event message"


@pytest.mark.asyncio
async def test_run_workflow_not_found(client: AsyncClient) -> None:
    response = await client.post("/workflows/nonexistent/run", json={})
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_run_workflow_error(client: AsyncClient) -> None:
    response = await client.post("/workflows/error/run", json={})
    assert response.status_code == 500
    data = response.json()
    assert data["error"] is not None
    assert "Test error" in data["error"]
    assert data["status"] == "failed"


@pytest.mark.asyncio
async def test_run_workflow_invalid_json(client: AsyncClient) -> None:
    response = await client.post(
        "/workflows/test/run",
        content="invalid json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_run_workflow_invalid_start_event(client: AsyncClient) -> None:
    # Test with invalid JSON for start_event
    response = await client.post(
        "/workflows/test/run", json={"start_event": "invalid json"}
    )
    assert response.status_code == 400
    assert "Validation error for 'start_event'" in response.text


@pytest.mark.asyncio
async def test_run_workflow_nowait_invalid_start_event(
    client: AsyncClient,
) -> None:
    # Test with invalid JSON for start_event in nowait endpoint
    response = await client.post(
        "/workflows/test/run-nowait", json={"start_event": "invalid json"}
    )
    assert response.status_code == 400
    assert "Validation error for 'start_event'" in response.text


@pytest.mark.asyncio
async def test_structured_start_event_empty_object_validated(
    client: AsyncClient,
    server: WorkflowServer,
    structured_start_workflow: Workflow,
) -> None:
    # Register workflow with required StartEvent fields
    server.add_workflow("structured", structured_start_workflow)

    # Empty object should be validated and rejected with 400
    response = await client.post(
        "/workflows/structured/run",
        json={"start_event": {}},
    )
    assert response.status_code == 400
    assert "Validation error for 'start_event'" in response.text


@pytest.mark.asyncio
async def test_structured_start_event_missing_value_treated_as_empty_and_validated(
    client: AsyncClient,
    server: WorkflowServer,
    structured_start_workflow: Workflow,
) -> None:
    # Register workflow with required StartEvent fields
    server.add_workflow("structured", structured_start_workflow)

    response = await client.post(
        "/workflows/structured/run",
        json={},  # testing no start_event whatsoever
    )
    assert response.status_code == 400
    assert "Validation error for 'start_event'" in response.text


@pytest.mark.asyncio
async def test_run_workflow_with_start_event_and_kwargs(
    client: AsyncClient,
) -> None:
    # Test that start_event takes precedence over kwargs
    start_event_json = '{"__is_pydantic": true, "value": {"_data": {"message": "start event priority"}}, "qualified_name": "workflows.events.StartEvent"}'
    response = await client.post(
        "/workflows/test/run",
        json={
            "start_event": start_event_json,
            "kwargs": {"message": "kwargs message"},
        },
    )
    assert response.status_code == 200
    data = response.json()
    # start_event should take precedence
    assert data["result"]["value"]["result"] == "processed: start event priority"


@pytest.mark.asyncio
async def test_run_workflow_nowait_success(client: AsyncClient) -> None:
    response = await client.post(
        "/workflows/test/run-nowait", json={"kwargs": {"message": "async"}}
    )
    assert response.status_code == 200
    data = response.json()
    assert "handler_id" in data
    assert "status" in data
    assert data["status"] == "running"
    assert len(data["handler_id"]) == 10  # Default nanoid length


@pytest.mark.asyncio
async def test_run_workflow_nowait_with_start_event(client: AsyncClient) -> None:
    # Test with start event containing message
    start_event_json = '{"__is_pydantic": true, "value": {"_data": {"message": "async start event"}}, "qualified_name": "workflows.events.StartEvent"}'
    response = await client.post(
        "/workflows/test/run-nowait", json={"start_event": start_event_json}
    )
    assert response.status_code == 200
    data = response.json()
    assert "handler_id" in data
    assert "status" in data
    assert data["status"] == "running"
    assert len(data["handler_id"]) == 10  # Default nanoid length


@pytest.mark.asyncio
async def test_run_workflow_nowait_not_found(client: AsyncClient) -> None:
    response = await client.post("/workflows/nonexistent/run-nowait", json={})
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_workflow_result(context_client: AsyncClient) -> None:
    # Setup a context to test all the code paths
    ctx_dict = (
        await serialize_context({"test_param": "message from context"})
    ).model_dump(mode="python")
    # run no-wait
    response = await context_client.post(
        "/workflows/test/run-nowait", json={"context": ctx_dict}
    )
    assert response.status_code == 200
    data = response.json()
    assert "handler_id" in data
    handler_id = data["handler_id"]

    await asyncio.sleep(0.1)

    # get result
    response = await context_client.get(f"/handlers/{handler_id}")
    assert response.status_code == 200

    # Verify the result content
    result_data = response.json()
    assert "result" in result_data
    assert result_data["result"]["value"]["result"] == "processed: message from context"


@pytest.mark.asyncio
async def test_get_workflow_result_error(
    client: AsyncClient, server: WorkflowServer
) -> None:
    # run no-wait
    response = await client.post("/workflows/error/run-nowait", json={})
    assert response.status_code == 200
    data = response.json()
    assert "handler_id" in data
    handler_id = data["handler_id"]

    # get result
    async def _wait_failed() -> dict[str, Any]:
        response = await client.get(f"/handlers/{handler_id}")
        assert response.status_code == 500
        return response.json()

    data = await wait_for_passing(_wait_failed)
    assert "error" in data
    assert "Test error" in data["error"]
    assert data["status"] == "failed"


@pytest.mark.asyncio
async def test_get_workflow_result_not_found(client: AsyncClient) -> None:
    response = await client.get("/handlers/nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_stream_events_success(client: AsyncClient) -> None:
    """Test streaming events from a workflow."""
    # Start streaming workflow
    response = await client.post(
        "/workflows/streaming/run-nowait", json={"kwargs": {"count": 3}}
    )
    assert response.status_code == 200
    data = response.json()
    handler_id = data["handler_id"]

    # Stream events (after_sequence=-1 to get all from beginning)
    response = await client.get(f"/events/{handler_id}?after_sequence=-1")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"

    # Collect streamed events
    events: list[dict[str, Any]] = []
    async for line in response.aiter_lines():
        line = line.strip()
        if line.startswith("data: "):
            event_data = json.loads(line.removeprefix("data: "))
            assert isinstance(event_data, dict)
            if event_data:
                events.append(event_data)

    stream_events = [
        e for e in events if e["qualified_name"] == "server_test_fixtures.StreamEvent"
    ]
    assert len(stream_events) == 3
    for i, event in enumerate(stream_events):
        assert "qualified_name" in event
        assert event["value"]["message"] == f"event_{i}"
        assert event["value"]["sequence"] == i


@pytest.mark.asyncio
async def test_stream_events_sse(client: AsyncClient) -> None:
    """Test streaming events using Server-Sent Events format."""
    # Start streaming workflow
    response = await client.post(
        "/workflows/streaming/run-nowait", json={"kwargs": {"count": 2}}
    )
    assert response.status_code == 200
    data = response.json()
    handler_id = data["handler_id"]

    # Stream events in SSE format (after_sequence=-1 to get all from beginning)
    response = await client.get(f"/events/{handler_id}?sse=true&after_sequence=-1")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    # Collect streamed events
    events = []
    current_event = {}
    async for line in response.aiter_lines():
        line = line.strip()
        if line.startswith("event: "):
            # Extract event type
            current_event["event_type"] = line.removeprefix("event: ")
        elif line.startswith("data: "):
            # Extract JSON from SSE data line
            event_json = line.removeprefix("data: ")
            event_data = json.loads(event_json)
            # Filter out empty events
            if event_data:
                current_event["data"] = event_data
                events.append(current_event.copy())
                current_event = {}

    # Verify we got event values (not full event objects)
    # SSE format returns event data with event_type field
    stream_events = [
        e
        for e in events
        if e["data"]["qualified_name"] == "server_test_fixtures.StreamEvent"
    ]
    assert len(stream_events) == 2

    for i, event in enumerate(stream_events):
        # SSE format returns event type and data separately
        assert "event_type" not in event
        assert event["data"]["value"]["message"] == f"event_{i}"
        assert event["data"]["value"]["sequence"] == i

    # reconnect with after_sequence beyond last event returns 204
    response = await client.get(f"/events/{handler_id}?sse=true&after_sequence=999999")
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_stream_events_not_found(client: AsyncClient) -> None:
    """Test streaming events from non-existent handler."""
    response = await client.get("/events/nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_stream_events_multiple_consumers(client: AsyncClient) -> None:
    """Multiple concurrent consumers can stream the same handler's events."""
    # Start a streaming workflow
    response = await client.post(
        "/workflows/streaming/run-nowait", json={"kwargs": {"count": 2}}
    )
    handler_id = response.json()["handler_id"]

    # Two concurrent stream requests (after_sequence=-1 to get all from beginning)
    a = asyncio.create_task(client.get(f"/events/{handler_id}?after_sequence=-1"))
    b = asyncio.create_task(client.get(f"/events/{handler_id}?after_sequence=-1"))

    response_a, response_b = await asyncio.gather(a, b)

    assert response_a.status_code == 200
    assert response_b.status_code == 200

    # Both consumers should receive the same events
    def parse_events(text: str) -> list[dict[str, Any]]:
        events = []
        for line in text.strip().split("\n"):
            line = line.strip()
            if line.startswith("data: "):
                data = json.loads(line.removeprefix("data: "))
                if data:
                    events.append(data)
        return events

    events_a = parse_events(response_a.text)
    events_b = parse_events(response_b.text)

    # Both should have the same event types
    types_a = [e["type"] for e in events_a]
    types_b = [e["type"] for e in events_b]
    assert types_a == types_b


@pytest.mark.asyncio
async def test_stream_events_no_events_default_hides_internal(
    client: AsyncClient,
) -> None:
    """Test streaming from workflow that emits no events. Default excludes internal events."""
    # Start simple workflow that doesn't emit user events
    response = await client.post(
        "/workflows/test/run-nowait", json={"kwargs": {"message": "test"}}
    )
    assert response.status_code == 200
    data = response.json()
    handler_id = data["handler_id"]

    # Stream without include_internal (after_sequence=-1 to get all from beginning)
    response = await client.get(f"/events/{handler_id}?after_sequence=-1")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"

    # Collect events
    events = []
    async for line in response.aiter_lines():
        line = line.strip()
        if line.startswith("data: "):
            event_data = json.loads(line.removeprefix("data: "))
            if event_data:
                events.append(event_data)

    # Only StopEvent should be present because internal events are hidden by default
    event_types = [e["qualified_name"] for e in events]
    assert set(event_types) == {"workflows.events.StopEvent"}
    assert event_types[-1] == "workflows.events.StopEvent"
    assert Counter(event_types)["workflows.events.StopEvent"] == 1


@pytest.mark.asyncio
async def test_stream_events_include_internal_true(client: AsyncClient) -> None:
    """When include_internal=true, internal events should be included."""
    # Start simple workflow that doesn't emit user events
    response = await client.post(
        "/workflows/test/run-nowait", json={"kwargs": {"message": "test"}}
    )
    assert response.status_code == 200
    data = response.json()
    handler_id = data["handler_id"]

    # Stream with include_internal=true (after_sequence=-1 to get all from beginning)
    response = await client.get(
        f"/events/{handler_id}?include_internal=true&after_sequence=-1"
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"

    # Collect events
    events = []
    async for line in response.aiter_lines():
        line = line.strip()
        if line.startswith("data: "):
            event_data = json.loads(line.removeprefix("data: "))
            if event_data:
                events.append(event_data)

    event_types = [e["qualified_name"] for e in events]
    # Expect internal event types to be present along with StopEvent
    assert "workflows.events.StopEvent" in event_types
    assert "workflows.events.StepStateChanged" in event_types


@pytest.mark.asyncio
async def test_get_handlers_empty(client: AsyncClient) -> None:
    response = await client.get("/handlers")
    assert response.status_code == 200
    assert response.json() == {"handlers": []}


@pytest.mark.asyncio
async def test_get_handlers_with_running_workflows(client: AsyncClient) -> None:
    # Start multiple workflows
    response1 = await client.post("/workflows/test/run-nowait", json={})
    handler_id1 = response1.json()["handler_id"]

    response2 = await client.post("/workflows/test/run-nowait", json={})
    handler_id2 = response2.json()["handler_id"]

    # Get handlers
    response = await client.get("/handlers")
    assert response.status_code == 200
    handlers = response.json()["handlers"]

    # Should have 2 handlers
    assert len(handlers) == 2
    handler_ids = {handler["handler_id"] for handler in handlers}
    assert handler_id1 in handler_ids
    assert handler_id2 in handler_ids

    # Check all required fields are present
    for handler in handlers:
        assert "handler_id" in handler
        assert "status" in handler
        assert "result" in handler
        assert "error" in handler
        assert handler["status"] == "running"
        assert handler["result"] is None  # Running workflows don't have results yet
        assert handler["error"] is None  # Running workflows don't have errors

    # Wait for workflows to complete to avoid warnings
    for handler_id in [handler_id1, handler_id2]:
        response = await client.get(f"/handlers/{handler_id}")
        while response.status_code == 202:
            await asyncio.sleep(0.01)
            response = await client.get(f"/handlers/{handler_id}")


async def validate_result_response(
    handler_id: str, client: AsyncClient, expected_status: int = 200
) -> Any:
    response = await client.get(f"/handlers/{handler_id}")
    assert response.status_code == expected_status
    return response.json() if expected_status == 200 else response.text


@pytest.mark.asyncio
async def test_get_handlers_with_completed_workflow(client: AsyncClient) -> None:
    # Start a workflow and wait for it to complete
    response = await client.post("/workflows/test/run-nowait", json={})
    handler_id = response.json()["handler_id"]

    await wait_for_passing(lambda: validate_result_response(handler_id, client))
    # Get handlers
    response = await client.get("/handlers")
    assert response.status_code == 200
    handlers = response.json()["handlers"]

    # Find our handler
    handler = next(h for h in handlers if h["handler_id"] == handler_id)
    assert handler["status"] == "completed"
    assert handler["result"] == {
        "type": "StopEvent",
        "value": {"result": "processed: default"},
        "qualified_name": "workflows.events.StopEvent",
        "types": None,
    }
    assert handler["error"] is None


@pytest.mark.asyncio
async def test_custom_stop_event_serialization_in_run_and_handlers(
    client: AsyncClient, server: WorkflowServer
) -> None:
    # Register custom workflow that returns a CustomStopEvent
    server.add_workflow("custom", CustomStopWorkflow())

    # Synchronous run returns a handler with result immediately
    resp_run = await client.post("/workflows/custom/run", json={})
    assert resp_run.status_code == 200
    run_data = resp_run.json()
    assert run_data["status"] == "completed"
    assert isinstance(run_data.get("result"), dict)
    assert run_data["result"]["type"] == "CustomStopEvent"
    # Minimal value check
    assert run_data["result"]["value"]["message"] == "custom-completed"

    # No-wait run then observe via handlers
    resp_nowait = await client.post("/workflows/custom/run-nowait", json={})
    assert resp_nowait.status_code == 200
    handler_id = resp_nowait.json()["handler_id"]

    # Wait for completion via results endpoint
    async def _wait_done() -> dict[str, Any]:
        r = await client.get(f"/handlers/{handler_id}")
        if r.status_code == 200:
            return r.json()
        raise AssertionError("not done")

    result_data = await wait_for_passing(_wait_done)
    assert result_data["result"]["type"] == "CustomStopEvent"
    assert result_data["result"]["value"]["message"] == "custom-completed"

    # Handlers list should reflect the same serialized result
    resp_handlers = await client.get("/handlers")
    assert resp_handlers.status_code == 200
    handlers = resp_handlers.json()["handlers"]
    custom = next(h for h in handlers if h["handler_id"] == handler_id)
    assert custom["status"] == "completed"
    assert custom["result"]["type"] == "CustomStopEvent"
    assert custom["result"]["value"]["message"] == "custom-completed"


@pytest.mark.asyncio
async def test_get_handlers_with_failed_workflow(client: AsyncClient) -> None:
    # Start an error workflow
    response = await client.post("/workflows/error/run-nowait", json={})
    handler_id = response.json()["handler_id"]

    # Wait a bit for workflow to fail
    await asyncio.sleep(0.1)

    result = await wait_for_passing(
        lambda: validate_result_response(handler_id, client, 500)
    )
    assert "Test error" in result

    # Get handlers
    response = await client.get("/handlers")
    assert response.status_code == 200
    handlers = response.json()["handlers"]

    # Find our handler
    handler = next(h for h in handlers if h["handler_id"] == handler_id)
    assert handler["status"] == "failed"
    assert handler["error"] is not None  # Should have an error
    assert "Test error" in handler["error"]  # Check error message
    assert handler["result"] is None  # Failed workflows don't have results


@pytest.mark.asyncio
async def test_get_handlers_filters_status_and_workflow_name(
    interactive_workflow: Workflow,
) -> None:
    # Seed persistence with mixed handlers
    persisted = [
        PersistentHandler(
            handler_id="h1", workflow_name="interactive", status="running"
        ),
        PersistentHandler(
            handler_id="h2", workflow_name="interactive", status="completed"
        ),
        PersistentHandler(handler_id="h3", workflow_name="other", status="failed"),
    ]

    async with server_with_persisted_handlers(
        interactive_workflow, persisted_handlers=persisted
    ) as (_server, client, _store):
        # Filter by single status
        r1 = await client.get("/handlers?status=completed")
        assert r1.status_code == 200
        ids1 = {h["handler_id"] for h in r1.json()["handlers"]}
        assert ids1 == {"h2"}

        # Filter by workflow name
        r2 = await client.get("/handlers?workflow_name=interactive")
        assert r2.status_code == 200
        ids2 = {h["handler_id"] for h in r2.json()["handlers"]}
        assert ids2 == {"h1", "h2"}

        # Filter by both
        r3 = await client.get("/handlers?workflow_name=interactive&status=running")
        assert r3.status_code == 200
        ids3 = {h["handler_id"] for h in r3.json()["handlers"]}
        assert ids3 == {"h1"}


@pytest.mark.asyncio
async def test_get_handlers_filters_multiple_status_params(
    interactive_workflow: Workflow,
) -> None:
    persisted = [
        PersistentHandler(
            handler_id="ha", workflow_name="interactive", status="completed"
        ),
        PersistentHandler(
            handler_id="hb", workflow_name="interactive", status="failed"
        ),
        PersistentHandler(
            handler_id="hc", workflow_name="interactive", status="running"
        ),
    ]

    async with server_with_persisted_handlers(
        interactive_workflow, persisted_handlers=persisted
    ) as (_server, client, _store):
        r = await client.get("/handlers?status=completed&status=failed")
        assert r.status_code == 200
        ids = {h["handler_id"] for h in r.json()["handlers"]}
        assert ids == {"ha", "hb"}


@pytest.mark.asyncio
async def test_post_event_to_running_workflow(
    client: AsyncClient, server: WorkflowServer
) -> None:
    # Start an interactive workflow
    response = await client.post("/workflows/interactive/run-nowait", json={})
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    await wait_for_requested_external_event(server._service.store, handler_id)

    serializer = JsonSerializer()
    event = ExternalEvent(response="Hello from test")
    event_str = serializer.serialize(event)

    # Send the event
    response = await client.post(f"/events/{handler_id}", json={"event": event_str})
    assert response.status_code == 200
    assert response.json() == {"status": "sent"}

    result = await wait_for_passing(
        lambda: validate_result_response(handler_id, client)
    )

    assert result["result"]["value"]["result"] == "received: Hello from test"


@pytest.mark.asyncio
async def test_post_event_simple_schema_to_running_workflow(
    client: AsyncClient, server: WorkflowServer
) -> None:
    # Start an interactive workflow
    response = await client.post("/workflows/interactive/run-nowait", json={})
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    await wait_for_requested_external_event(server._service.store, handler_id)

    # Send the event using type/data dict format
    event_str = '{"type": "ExternalEvent", "data": {"response": "Hello from test"}}'
    response = await client.post(f"/events/{handler_id}", json={"event": event_str})
    assert response.status_code == 200
    assert response.json() == {"status": "sent"}

    result = await wait_for_passing(
        lambda: validate_result_response(handler_id, client)
    )

    assert result["result"]["value"]["result"] == "received: Hello from test"


@pytest.mark.asyncio
async def test_post_event_with_discriminators_to_running_workflow(
    client: AsyncClient, server: WorkflowServer
) -> None:
    """Test posting event using JSON serializer dict format with discriminators."""
    # Start an interactive workflow
    response = await client.post("/workflows/interactive/run-nowait", json={})
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    await wait_for_requested_external_event(server._service.store, handler_id)

    # Send event as a dict with discriminators (not as a string)
    # This is the format returned by JsonSerializer().serialize_value()
    serializer = JsonSerializer()
    event = ExternalEvent(response="Hello with discriminators")
    event_dict = serializer.serialize_value(event)

    response = await client.post(f"/events/{handler_id}", json={"event": event_dict})
    assert response.status_code == 200
    assert response.json() == {"status": "sent"}

    result = await wait_for_passing(
        lambda: validate_result_response(handler_id, client)
    )

    assert result["result"]["value"]["result"] == "received: Hello with discriminators"


@pytest.mark.asyncio
async def test_get_workflow_result_returns_202_when_pending(
    client: AsyncClient,
) -> None:
    # Start workflow that waits for an external event and thus remains pending
    response = await client.post("/workflows/interactive/run-nowait", json={})
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    response = await client.get(f"/handlers/{handler_id}")
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_get_workflow_result_multiple_times(
    client: AsyncClient,
) -> None:
    # Start and wait for completion
    response = await client.post(
        "/workflows/test/run-nowait", json={"kwargs": {"message": "cache-me"}}
    )
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    # First fetch populates cache
    first = await wait_for_passing(lambda: validate_result_response(handler_id, client))
    assert first["result"]["value"]["result"] == "processed: cache-me"

    second = await validate_result_response(handler_id, client)
    assert second == first


@pytest.mark.asyncio
async def test_post_event_handler_not_found(client: AsyncClient) -> None:
    response = await client.post("/events/nonexistent_handler", json={"event": "{}"})
    assert response.status_code == 404
    assert "Handler not found" in response.text


@pytest.mark.asyncio
async def test_post_event_to_completed_workflow(client: AsyncClient) -> None:
    # Start and wait for a simple workflow to complete
    response = await client.post("/workflows/test/run-nowait", json={})
    handler_id = response.json()["handler_id"]

    # Wait for workflow to complete
    response = await client.get(f"/handlers/{handler_id}")
    while response.status_code == 202:
        await asyncio.sleep(0.01)
        response = await client.get(f"/handlers/{handler_id}")

    # Try to send event to completed workflow
    response = await client.post(f"/events/{handler_id}", json={"event": "{}"})
    assert response.status_code == 409
    assert "Workflow already completed" in response.text


@pytest.mark.asyncio
async def test_post_event_invalid_event_data(client: AsyncClient) -> None:
    # Start an interactive workflow
    response = await client.post("/workflows/interactive/run-nowait", json={})
    handler_id = response.json()["handler_id"]

    # Send invalid event data
    response = await client.post(
        f"/events/{handler_id}", json={"event": "invalid json"}
    )
    assert response.status_code == 400
    assert "Failed to deserialize event" in response.text


@pytest.mark.asyncio
async def test_post_event_body_parsing_error(client: AsyncClient) -> None:
    # Start interactive workflow which waits for an event (keeps running)
    response = await client.post("/workflows/interactive/run-nowait", json={})
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    # Send invalid JSON body (not JSON), triggers 400 from body parsing
    response = await client.post(
        f"/events/{handler_id}",
        content="not json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    assert "Error processing request" in response.json()["detail"]


@pytest.mark.asyncio
async def test_post_event_missing_event_data(client: AsyncClient) -> None:
    # Start an interactive workflow
    response = await client.post("/workflows/interactive/run-nowait", json={})
    handler_id = response.json()["handler_id"]

    # Send request without event data
    response = await client.post(f"/events/{handler_id}", json={})
    assert response.status_code == 400
    assert "Event data is required" in response.text


@pytest.mark.asyncio
async def test_handler_datetime_fields_progress(
    client: AsyncClient, server: WorkflowServer
) -> None:
    # Start interactive workflow which waits for an external event
    response = await client.post("/workflows/interactive/run-nowait", json={})
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    # Snapshot initial times
    resp = await client.get("/handlers")
    assert resp.status_code == 200
    handlers = resp.json()["handlers"]
    item = next(h for h in handlers if h["handler_id"] == handler_id)
    started_at_1 = datetime.fromisoformat(item["started_at"])  # ISO 8601
    updated_at_1 = datetime.fromisoformat(item["updated_at"])  # ISO 8601
    assert started_at_1 <= updated_at_1
    assert item["completed_at"] is None

    await wait_for_requested_external_event(server._service.store, handler_id)

    # Send an external event to progress the workflow and update timestamps
    serializer = JsonSerializer()
    event = ExternalEvent(response="ts-check")
    event_str = serializer.serialize(event)
    send = await client.post(f"/events/{handler_id}", json={"event": event_str})
    assert send.status_code == 200

    # Check updated_at increased
    resp2 = await client.get("/handlers")
    assert resp2.status_code == 200
    item2 = next(h for h in resp2.json()["handlers"] if h["handler_id"] == handler_id)
    updated_at_2 = datetime.fromisoformat(item2["updated_at"])  # ISO 8601
    assert updated_at_2 >= updated_at_1

    # Wait for completion and check completed_at
    async def _wait_done() -> Response:
        r = await client.get(f"/handlers/{handler_id}")
        if r.status_code == 200:
            return r
        raise AssertionError("not done")

    await wait_for_passing(_wait_done)

    resp3 = await client.get("/handlers")
    item3 = next(h for h in resp3.json()["handlers"] if h["handler_id"] == handler_id)
    assert item3["status"] in {"completed", "failed"}
    if item3["status"] == "completed":
        assert item3["completed_at"] is not None
        completed_at = datetime.fromisoformat(item3["completed_at"])  # ISO 8601
        assert completed_at >= updated_at_2


@pytest.mark.asyncio
async def test_cancel_handler_persists_cancelled_status(
    interactive_workflow: Workflow,
) -> None:
    async with server_with_persisted_handlers(interactive_workflow) as (
        _server,
        client,
        store,
    ):
        response = await client.post(
            "/workflows/interactive/run-nowait",
            json={},
        )
        handler_id = response.json()["handler_id"]
        resp_cancel = await client.post(f"/handlers/{handler_id}/cancel?purge=false")
        assert resp_cancel.status_code == 200
        assert resp_cancel.json() == {"status": "cancelled"}
        persisted_cancelled = await store.query(
            HandlerQuery(handler_id_in=[handler_id])
        )
        assert len(persisted_cancelled) == 1
        assert persisted_cancelled[0].status == "cancelled"

        resp_delete2 = await client.post(f"/handlers/{handler_id}/cancel?purge=false")
        assert resp_delete2.status_code == 404


@pytest.mark.asyncio
async def test_delete_persisted_handler_removes_from_store(
    interactive_workflow: Workflow,
) -> None:
    async with server_with_persisted_handlers(
        interactive_workflow,
        persisted_handlers=[
            PersistentHandler(
                handler_id="persist-only",
                workflow_name="interactive",
                status="completed",
            )
        ],
    ) as (_server, client, store):
        resp_delete_store = await client.post(
            "/handlers/persist-only/cancel?purge=true"
        )
        assert resp_delete_store.status_code == 200
        assert resp_delete_store.json() == {"status": "deleted"}

        persisted_handlers = await store.query(
            HandlerQuery(handler_id_in=["persist-only"])
        )
        assert persisted_handlers == []


@pytest.mark.asyncio
async def test_stop_only_persisted_handler_without_removal_returns_not_found(
    interactive_workflow: Workflow,
) -> None:
    async with server_with_persisted_handlers(
        interactive_workflow,
        persisted_handlers=[
            PersistentHandler(
                handler_id="store-only",
                workflow_name="interactive",
                status="completed",
            )
        ],
    ) as (_server, client, store):
        resp_cancel_store_only = await client.post("/handlers/store-only/cancel")
        assert resp_cancel_store_only.status_code == 404

        resp_cancel_store_only = await client.post("/handlers/store-only/cancel")
        assert resp_cancel_store_only.status_code == 404

        persisted = await store.query(HandlerQuery(handler_id_in=["store-only"]))
        assert persisted and persisted[0].status == "completed"


@pytest.mark.asyncio
async def test_legacy_results_endpoint_still_works(client: AsyncClient) -> None:
    # Start a workflow without waiting
    response = await client.post("/workflows/test/run-nowait", json={})
    assert response.status_code == 200
    handler_id = response.json()["handler_id"]

    # Poll the deprecated endpoint until completion
    async def _wait_done() -> Response:
        r = await client.get(f"/results/{handler_id}")
        if r.status_code == 200:
            return r
        raise AssertionError("not done")

    r = await wait_for_passing(_wait_done)
    data = r.json()
    # Ensure it returns an object and includes an untyped result field
    assert isinstance(data, dict)
    assert data.get("handler_id") == handler_id
    assert data.get("result") is not None


@pytest.mark.asyncio
async def test_stream_events_after_completion_should_return_unconsumed_events(
    client: AsyncClient,
) -> None:
    # Start streaming workflow that emits 3 events and completes
    start_resp = await client.post(
        "/workflows/streaming/run-nowait", json={"kwargs": {"count": 3}}
    )
    assert start_resp.status_code == 200
    handler_id = start_resp.json()["handler_id"]

    # Wait for completion via results endpoint
    async def _wait_done() -> Response:
        r = await client.get(f"/handlers/{handler_id}")
        if r.status_code == 200:
            return r
        raise AssertionError("not done")

    await wait_for_passing(_wait_done)

    # Now fetch events AFTER completion. Expect all events to be retrievable.
    # Use NDJSON for easier parsing. after_sequence=-1 to get all from beginning.
    resp = await client.get(f"/events/{handler_id}?sse=false&after_sequence=-1")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-ndjson")

    # Collect NDJSON lines
    lines: list[str] = []
    async for line in resp.aiter_lines():
        data = line.strip()
        if data:
            lines.append(data)

    assert len(lines) == 4


@pytest.mark.asyncio
async def test_stream_events_sse_includes_id_field(client: AsyncClient) -> None:
    """SSE events include an id: field with the event sequence number."""
    response = await client.post(
        "/workflows/streaming/run-nowait", json={"kwargs": {"count": 2}}
    )
    handler_id = response.json()["handler_id"]

    response = await client.get(f"/events/{handler_id}?sse=true&after_sequence=-1")
    assert response.status_code == 200

    # Parse raw SSE frames and extract id fields
    ids: list[int] = []
    for line in response.text.strip().split("\n"):
        line = line.strip()
        if line.startswith("id: "):
            ids.append(int(line.removeprefix("id: ")))

    # Every SSE event should have an id
    assert len(ids) >= 2
    # Ids should be monotonically increasing
    assert ids == sorted(ids)


@pytest.mark.asyncio
async def test_stream_events_last_event_id_header(client: AsyncClient) -> None:
    """SSE Last-Event-ID header takes priority over after_sequence query param."""
    response = await client.post(
        "/workflows/streaming/run-nowait", json={"kwargs": {"count": 3}}
    )
    handler_id = response.json()["handler_id"]

    # First, stream all events to get the sequence numbers
    response = await client.get(f"/events/{handler_id}?sse=true&after_sequence=-1")
    assert response.status_code == 200

    ids: list[int] = []
    for line in response.text.strip().split("\n"):
        line = line.strip()
        if line.startswith("id: "):
            ids.append(int(line.removeprefix("id: ")))
    assert len(ids) >= 3

    # Reconnect with Last-Event-ID header set to skip past all events.
    # The query param says after_sequence=-1 (from beginning), but the header
    # should override it.
    response = await client.get(
        f"/events/{handler_id}?sse=true&after_sequence=-1",
        headers={"last-event-id": str(ids[-1])},
    )
    # Should get 204 because Last-Event-ID is past the last event and the run
    # is complete.
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_stream_events_after_sequence_now(client: AsyncClient) -> None:
    """after_sequence=now skips historical events, only receives new ones."""
    # Start a streaming workflow
    response = await client.post(
        "/workflows/streaming/run-nowait", json={"kwargs": {"count": 3}}
    )
    handler_id = response.json()["handler_id"]

    # Wait for completion so all events are stored
    async def _wait_done() -> None:
        r = await client.get(f"/handlers/{handler_id}")
        if r.status_code != 200:
            raise AssertionError("not done")

    await wait_for_passing(_wait_done)

    # Now request with after_sequence=now. Since the workflow is already complete,
    # "now" resolves to the last sequence, and there are no remaining events, so
    # we should get 204.
    response = await client.get(f"/events/{handler_id}?after_sequence=now")
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_stream_events_after_sequence_now_receives_future_events(
    interactive_workflow: Workflow,
) -> None:
    """after_sequence=now on a running workflow receives only events appended after the request."""
    async with server_with_persisted_handlers(interactive_workflow) as (
        _server,
        client,
        store,
    ):
        # Start the interactive workflow (it waits for an external event)
        start_resp = await client.post("/workflows/interactive/run-nowait", json={})
        handler_id = start_resp.json()["handler_id"]

        await wait_for_requested_external_event(store, handler_id)

        # Count events currently in the store
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        run_id = found[0].run_id
        assert run_id is not None
        events_before = await store.query_events(run_id)
        assert len(events_before) > 0

        # Start streaming with after_sequence=now — should skip all existing events
        stream_task = asyncio.create_task(
            client.get(f"/events/{handler_id}?sse=false&after_sequence=now")
        )

        # Give the streaming request time to start
        await asyncio.sleep(0.05)

        # Send an external event to progress the workflow
        serializer = JsonSerializer()
        event = ExternalEvent(response="after-now")
        event_str = serializer.serialize(event)
        await client.post(f"/events/{handler_id}", json={"event": event_str})

        response = await stream_task
        assert response.status_code == 200

        # Parse NDJSON lines
        events = []
        for line in response.text.strip().split("\n"):
            line = line.strip()
            if line:
                events.append(json.loads(line))

        # The events should include at minimum the StopEvent from completion.
        # They should NOT include any of the events that existed before "now".
        event_types = [e["type"] for e in events]
        assert "StopEvent" in event_types

        # Verify we got fewer events than the total stored — the historical ones were skipped
        all_events = await store.query_events(run_id)
        assert len(events) < len(all_events)


@pytest.mark.asyncio
async def test_instrument_tags_contains_handler_id_in_server_context() -> None:
    seen_handler_id: dict[str, str | None] = {"handler_id": None}

    class TagReadingWorkflow(Workflow):
        @step
        async def read_tags(self, ctx: Context, ev: StartEvent) -> StopEvent:
            # Read handler_id set by the server while streaming events
            hid = active_instrument_tags.get().get("llamaindex.handler_id")
            seen_handler_id["handler_id"] = hid
            return StopEvent()

    server = WorkflowServer(workflow_store=MemoryWorkflowStore(), idle_timeout=0.01)
    server.add_workflow("tags", TagReadingWorkflow())

    async with server.contextmanager():
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Start without waiting
            start = await client.post("/workflows/tags/run-nowait", json={})
            assert start.status_code == 200
            handler_id = start.json()["handler_id"]

            # Wait for completion and fetch result
            async def _wait_done() -> dict[str, Any]:
                r = await client.get(f"/handlers/{handler_id}")
                if r.status_code == 200:
                    return r.json()
                raise AssertionError("not done")

            data = await wait_for_passing(_wait_done)
            assert data["status"] == "completed"
            assert seen_handler_id["handler_id"] is not None
            assert seen_handler_id["handler_id"] == handler_id


# --- Subclass-aware event routing (accept_event_subclasses) over HTTP ---


class SubclassParentEvent(Event):
    payload: str


class SubclassChildEvent(SubclassParentEvent):
    pass


class SubclassRoutingWorkflow(Workflow):
    """A producer emits a subclass; a consumer accepts the parent with opt-in."""

    @step
    async def start(self, ev: StartEvent) -> SubclassChildEvent:
        return SubclassChildEvent(payload="from-start")

    @step(accept_event_subclasses=True)
    async def consume(self, ev: SubclassParentEvent) -> StopEvent:
        return StopEvent(result=f"consumed {type(ev).__name__}: {ev.payload}")


@pytest_asyncio.fixture
async def subclass_client() -> AsyncGenerator:
    server = WorkflowServer(workflow_store=MemoryWorkflowStore(), idle_timeout=0.01)
    server.add_workflow("subclass", SubclassRoutingWorkflow())
    async with server.contextmanager():
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.mark.asyncio
async def test_subclass_routing_runs_over_http(subclass_client: AsyncClient) -> None:
    response = await subclass_client.post("/workflows/subclass/run", json={})
    assert response.status_code == 200
    data = response.json()
    # The child event must have been routed to the parent-accepting opted-in step.
    assert (
        data["result"]["value"]["result"] == "consumed SubclassChildEvent: from-start"
    )


@pytest.mark.asyncio
async def test_subclass_routing_representation_over_http(
    subclass_client: AsyncClient,
) -> None:
    response = await subclass_client.get("/workflows/subclass/representation")
    assert response.status_code == 200
    graph = response.json()["graph"]
    edges = {(e["source"], e["target"]) for e in graph["edges"]}
    # Both the declared parent edge and the subclass fan-out edge are present.
    assert ("SubclassParentEvent", "consume") in edges
    assert ("SubclassChildEvent", "consume") in edges

    child = next(n for n in graph["nodes"] if n["id"] == "SubclassChildEvent")
    # The inheritance chain is serialized so the UI can classify the node.
    assert "SubclassParentEvent" in child["event_types"]
    assert child["event_schema"] is not None
