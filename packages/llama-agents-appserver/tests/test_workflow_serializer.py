# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import json
import sys
from types import ModuleType
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from llama_agents.appserver.deployment import Deployment
from llama_agents.appserver.routers.deployments import create_deployments_router
from llama_agents.appserver.settings import ApiserverSettings
from llama_agents.appserver.workflow_loader import load_workflow_server
from llama_agents.core.deployment_config import DeploymentConfig
from llama_agents.server import WorkflowServer
from workflows import Workflow, step
from workflows.context import Context, JsonSerializer
from workflows.context.serializers import BaseSerializer, PickleSerializer
from workflows.events import Event, SerializableEvent, StartEvent, StopEvent


class ExampleWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


def test_source_server_options_survive_hosted_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = PickleSerializer()
    json_serializer = JsonSerializer(allowed_types=[StartEvent, StopEvent])
    source = WorkflowServer(serializer=serializer, json_serializer=json_serializer)
    workflow = ExampleWorkflow()
    source.add_workflow("example", workflow)
    module = ModuleType("configured_server")
    setattr(module, "app", source)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    config = DeploymentConfig(name="test", app="configured_server:app")
    loaded = load_workflow_server(config)
    assert loaded is source
    deployment = Deployment(
        loaded.get_workflows(),
        serializer=loaded.serializer,
        json_serializer=loaded.json_serializer,
    )
    hosted = deployment.create_workflow_server(
        config, ApiserverSettings(persistence="memory")
    )
    assert hosted.serializer is serializer
    assert hosted.json_serializer is json_serializer
    assert hosted.get_workflows()["example"] is workflow
    assert workflow.runtime.get_serializer(workflow) is serializer


@pytest.mark.parametrize("use_default", [False, True])
@pytest.mark.parametrize("allow_event", [False, True])
def test_legacy_event_route_uses_hosted_json_decoder(
    monkeypatch: pytest.MonkeyPatch, allow_event: bool, use_default: bool
) -> None:
    json_serializer = JsonSerializer(allowed_types=[StartEvent] if allow_event else [])
    deployment = Deployment(
        {"example": ExampleWorkflow()},
        serializer=PickleSerializer(),
        json_serializer=None if use_default else json_serializer,
    )
    hosted = deployment.create_workflow_server(
        DeploymentConfig(name="test"), ApiserverSettings(persistence="memory")
    )
    app = FastAPI()
    app.include_router(
        create_deployments_router(
            "test", deployment, json_serializer=hosted.json_serializer
        )
    )
    deployment._contexts["session"] = Context(hosted.get_workflows()["example"])
    delivered: list[Event] = []

    def receive(self: Context, event: Event, step: str | None = None) -> None:
        delivered.append(event)

    def forbid_import(name: str, package: str | None = None) -> None:
        pytest.fail(f"Metadata attempted import: {name}")

    monkeypatch.setattr(Context, "send_event", receive)
    monkeypatch.setattr("workflows.context.utils.import_module", forbid_import)
    payload = JsonSerializer().serialize_value(StartEvent.model_validate({"value": 42}))
    if use_default and not allow_event:
        payload["qualified_name"] = "unknown_metadata.Event"
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            "/deployments/test/tasks/task/events",
            params={"session_id": "session"},
            json={
                "service_id": "example",
                "event_obj_str": json.dumps(payload),
            },
        )
    assert response.status_code == (200 if allow_event else 500)
    assert len(delivered) == int(allow_event)
    if allow_event:
        assert delivered[0].value == 42


class CustomSerializer(BaseSerializer):
    def serialize(self, value: Any) -> str:
        return "custom:" + PickleSerializer().serialize(value)

    def deserialize(self, value: str) -> Any:
        assert value.startswith("custom:")
        return PickleSerializer().deserialize(value.removeprefix("custom:"))


class PythonEvent(Event):
    pass


class PythonWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> PythonEvent:
        await ctx.store.set("value", complex(1, 2))
        return PythonEvent.model_validate({"value": complex(3, 4)})

    @step
    async def finish(self, ctx: Context, ev: PythonEvent) -> StopEvent:
        assert await ctx.store.get("value") == complex(1, 2)
        assert ev.value == complex(3, 4)
        return StopEvent(result="ok")


@pytest.mark.asyncio
@pytest.mark.parametrize("serializer_type", [PickleSerializer, CustomSerializer])
@pytest.mark.parametrize("workflow_override", [False, True])
async def test_hosted_explicit_codec_preserves_internal_python_values(
    serializer_type: type[BaseSerializer], workflow_override: bool
) -> None:
    serializer = serializer_type()
    workflow = PythonWorkflow(serializer=serializer if workflow_override else None)
    deployment = Deployment(
        {"python": workflow}, serializer=None if workflow_override else serializer
    )
    server = deployment.create_workflow_server(
        DeploymentConfig(name="test"), ApiserverSettings(persistence="memory")
    )
    assert workflow.runtime.get_serializer(workflow) is serializer
    async with server.contextmanager():
        handler = await server._runtime_core.run("python")
        completed = await server._service.await_workflow(handler)
        assert completed.result is not None
        assert completed.result.value["result"] == "ok"


class HostedExtraEvent(Event):
    pass


class HostedStartEvent(StartEvent):
    extra: SerializableEvent


class ExtraWorkflow(Workflow):
    @step
    async def start(self, ev: HostedStartEvent) -> StopEvent:
        assert isinstance(ev.extra, HostedExtraEvent)
        return StopEvent(result="ok")


@pytest.mark.asyncio
async def test_source_additional_event_snapshot_survives_hosted_transfer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = WorkflowServer()
    workflow = ExtraWorkflow()
    source.add_workflow("extra", workflow, additional_events=[HostedExtraEvent])
    json_serializer = source.json_serializer
    deployment = Deployment(source.get_workflows(), json_serializer=json_serializer)
    hosted = deployment.create_workflow_server(
        DeploymentConfig(name="test"), ApiserverSettings(persistence="memory")
    )
    assert source.get_workflows() == {}
    assert hosted.json_serializer is json_serializer
    selected = workflow.runtime.get_serializer(workflow)

    def forbid_import(name: str) -> Any:
        pytest.fail(f"Unexpected metadata import: {name}")

    monkeypatch.setattr("workflows.context.utils.import_module", forbid_import)
    event = HostedExtraEvent()
    assert isinstance(selected.deserialize(selected.serialize(event)), HostedExtraEvent)
    async with (
        hosted.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=hosted.app), base_url="http://test"
        ) as client,
    ):
        response = await client.post(
            "/workflows/extra/run",
            json={
                "start_event": {
                    "qualified_name": f"{HostedStartEvent.__module__}.{HostedStartEvent.__qualname__}",
                    "value": {"extra": hosted.json_serializer.serialize_value(event)},
                }
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["result"]["value"]["result"] == "ok"

    app = FastAPI()
    app.include_router(
        create_deployments_router(
            "test", deployment, json_serializer=hosted.json_serializer
        )
    )
    deployment._contexts["session"] = Context(workflow)
    delivered: list[Event] = []

    def receive(self: Context, event: Event, step: str | None = None) -> None:
        delivered.append(event)

    monkeypatch.setattr(Context, "send_event", receive)
    with TestClient(app) as legacy:
        response = legacy.post(
            "/deployments/test/tasks/task/events",
            params={"session_id": "session"},
            json={
                "service_id": "extra",
                "event_obj_str": hosted.json_serializer.serialize(event),
            },
        )
    assert response.status_code == 200, response.text
    assert len(delivered) == 1
    assert isinstance(delivered[0], HostedExtraEvent)
