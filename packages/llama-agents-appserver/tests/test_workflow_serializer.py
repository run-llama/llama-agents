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
from pydantic import BaseModel
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
    source = WorkflowServer(serializer=serializer)
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
        extra_types=loaded.extra_types,
        additional_events=loaded.additional_events,
    )
    hosted = deployment.create_workflow_server(
        config, ApiserverSettings(persistence="memory")
    )
    assert hosted.serializer is serializer
    assert hosted.get_workflows()["example"] is workflow
    assert workflow.runtime.get_serializer(workflow) is serializer


class CustomSerializer(BaseSerializer):
    def serialize(self, value: Any) -> str:
        return "custom:" + PickleSerializer().serialize(value)

    def deserialize(self, value: str) -> Any:
        assert value.startswith("custom:")
        return PickleSerializer().deserialize(value.removeprefix("custom:"))


@pytest.mark.parametrize("serializer_type", [PickleSerializer, CustomSerializer])
@pytest.mark.parametrize("workflow_override", [False, True])
def test_hosted_explicit_serializer_is_preserved(
    serializer_type: type[BaseSerializer], workflow_override: bool
) -> None:
    serializer = serializer_type()
    workflow = ExampleWorkflow(serializer=serializer if workflow_override else None)
    deployment = Deployment(
        {"example": workflow}, serializer=None if workflow_override else serializer
    )
    server = deployment.create_workflow_server(
        DeploymentConfig(name="test"), ApiserverSettings(persistence="memory")
    )
    assert workflow.runtime.get_serializer(workflow) is serializer
    assert server.get_workflows()["example"] is workflow


class HostedModel(BaseModel):
    value: int = 0


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
    source = WorkflowServer(extra_types=[HostedModel])
    workflow = ExtraWorkflow()
    source.add_workflow("extra", workflow, additional_events=[HostedExtraEvent])
    deployment = Deployment(
        source.get_workflows(),
        extra_types=source.extra_types,
        additional_events=source.additional_events,
    )
    hosted = deployment.create_workflow_server(
        DeploymentConfig(name="test"), ApiserverSettings(persistence="memory")
    )
    assert source.get_workflows() == {}
    selected = workflow.runtime.get_serializer(workflow)
    model = HostedModel(value=5)
    assert selected.deserialize(selected.serialize(model)) == model

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
                    "value": {"extra": JsonSerializer().serialize_value(event)},
                }
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["result"]["value"]["result"] == "ok"

    app = FastAPI()
    app.include_router(create_deployments_router("test", deployment))
    deployment._contexts["session"] = Context(workflow)
    delivered: list[Event] = []

    def receive(self: Context, event: Event, step: str | None = None) -> None:
        delivered.append(event)

    monkeypatch.setattr(Context, "send_event", receive)
    with TestClient(app, raise_server_exceptions=False) as legacy:
        response = legacy.post(
            "/deployments/test/tasks/task/events",
            params={"session_id": "session"},
            json={
                "service_id": "extra",
                "event_obj_str": JsonSerializer().serialize(event),
            },
        )
        assert response.status_code == 200, response.text
        for payload in [
            {
                "__is_pydantic": True,
                "qualified_name": "unknown_metadata.Event",
                "value": {},
            },
            JsonSerializer().serialize_value(model),
        ]:
            undeclared = legacy.post(
                "/deployments/test/tasks/task/events",
                params={"session_id": "session"},
                json={"service_id": "extra", "event_obj_str": json.dumps(payload)},
            )
            assert undeclared.status_code == 500
    assert len(delivered) == 1
    assert isinstance(delivered[0], HostedExtraEvent)
