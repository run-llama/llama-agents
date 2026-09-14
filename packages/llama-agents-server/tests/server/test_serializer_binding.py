# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import base64
import pickle
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from llama_agents.server import WorkflowServer
from llama_agents.server._store.abstract_workflow_store import (
    PersistentHandler,
    stream_workflow_ticks,
)
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from workflows import Context, Workflow, step
from workflows.context.serializers import (
    BaseSerializer,
    JsonSerializer,
    PickleSerializer,
)
from workflows.events import Event, StartEvent, StopEvent


class PythonEvent(Event):
    pass


class PythonWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> PythonEvent:
        await ctx.store.set("python_value", complex(1, 2))
        return PythonEvent.model_validate({"value": complex(3, 4)})

    @step
    async def finish(self, ctx: Context, ev: PythonEvent) -> StopEvent:
        assert await ctx.store.get("python_value") == complex(1, 2)
        assert ev.value == complex(3, 4)
        return StopEvent(result="ok")


class CustomSerializer(BaseSerializer):
    def __init__(self) -> None:
        self.encodes = 0
        self.decodes = 0

    def serialize(self, value: Any) -> str:
        self.encodes += 1
        return "custom:" + base64.b64encode(pickle.dumps(value)).decode()

    def deserialize(self, value: str) -> Any:
        self.decodes += 1
        assert value.startswith("custom:")
        return pickle.loads(base64.b64decode(value.removeprefix("custom:")))


@pytest.mark.parametrize("use_workflow_override", [True, False])
@pytest.mark.parametrize("serializer_type", [PickleSerializer, CustomSerializer])
async def test_bound_serializer_handles_internal_python_values(
    tmp_path: Path,
    use_workflow_override: bool,
    serializer_type: type[BaseSerializer],
) -> None:
    selected = serializer_type()
    workflow = PythonWorkflow(serializer=selected if use_workflow_override else None)
    store = SqliteWorkflowStore(db_path=str(tmp_path / "workflows.db"))
    server = WorkflowServer(
        workflow_store=store,
        serializer=JsonSerializer() if use_workflow_override else selected,
    )
    server.add_workflow("python", workflow)
    assert workflow.runtime.get_serializer(workflow) is selected
    async with server._runtime_core.contextmanager():
        result = await server._runtime_core.run("python")
        completed = await server._service.await_workflow(result)
        assert completed.result is not None
        assert completed.result.value["result"] == "ok"
        assert result.run_id is not None
        ticks = [
            tick
            async for tick in stream_workflow_ticks(
                store, result.run_id, serializer=selected
            )
        ]
        assert ticks
        assert server._runtime_core._persistence is not None
        replayed = await server._runtime_core._persistence.context_from_ticks(
            workflow, result.run_id
        )
        assert replayed is not None
    if isinstance(selected, CustomSerializer):
        assert selected.encodes > 0
        assert selected.decodes > 0


async def test_unreadable_persisted_result_is_reported_over_http(
    tmp_path: Path,
) -> None:
    store = SqliteWorkflowStore(db_path=str(tmp_path / "results.db"))
    await store.update(
        PersistentHandler(
            handler_id="unreadable",
            workflow_name="missing",
            status="completed",
            result=StopEvent(result="stored"),
        )
    )
    with store._connect() as connection:
        connection.execute(
            """UPDATE handlers
               SET result = '{"__is_pydantic": true,
                              "qualified_name": "missing.result.StoredResult",
                              "value": {}}'
               WHERE handler_id = 'unreadable'"""
        )
        connection.commit()

    server = WorkflowServer(workflow_store=store)
    async with server.contextmanager():
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            handler = await client.get("/handlers/unreadable")
            result = await client.get("/results/unreadable")
            listing = await client.get("/handlers")

    assert handler.status_code == 422
    assert result.status_code == 422
    assert listing.status_code == 200
    assert listing.json()["handlers"][0]["result"] is None
