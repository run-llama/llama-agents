from __future__ import annotations

import sys
from types import ModuleType

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from llama_agents.appserver.deployment import Deployment
from llama_agents.appserver.routers.deployments import create_deployments_router
from llama_agents.appserver.settings import ApiserverSettings
from llama_agents.appserver.workflow_loader import load_workflow_server, load_workflows
from llama_agents.core.deployment_config import DeploymentConfig
from llama_agents.server import WorkflowServer
from workflows import Workflow, step
from workflows.context import Context, JsonSerializer
from workflows.context.serializers import PickleSerializer
from workflows.events import Event, StartEvent, StopEvent


class ExampleWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


def test_loader_and_deployment_preserve_explicit_workflow_serializer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = PickleSerializer()
    workflow = ExampleWorkflow(serializer=serializer)
    module = ModuleType("configured_workflow")
    setattr(module, "workflow", workflow)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    config = DeploymentConfig(
        name="test", workflows={"example": "configured_workflow:workflow"}
    )
    loaded = load_workflows(config)
    deployment = Deployment(loaded)
    assert loaded["example"] is workflow
    assert deployment._workflow_services["example"] is workflow
    assert workflow.serializer is serializer


def test_source_server_options_survive_hosted_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = PickleSerializer()
    json_serializer = JsonSerializer(
        allowed_types=[StartEvent, StopEvent], dynamic_import=False
    )
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


@pytest.mark.parametrize("allow_event", [False, True])
def test_legacy_event_route_uses_hosted_json_decoder(
    monkeypatch: pytest.MonkeyPatch, allow_event: bool
) -> None:
    json_serializer = JsonSerializer(
        allowed_types=[StartEvent] if allow_event else [], dynamic_import=False
    )
    deployment = Deployment(
        {"example": ExampleWorkflow()},
        serializer=PickleSerializer(),
        json_serializer=json_serializer,
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
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            "/deployments/test/tasks/task/events",
            params={"session_id": "session"},
            json={
                "service_id": "example",
                "event_obj_str": JsonSerializer().serialize(
                    StartEvent.model_validate({"value": 42})
                ),
            },
        )
    assert response.status_code == (200 if allow_event else 500)
    assert len(delivered) == int(allow_event)
    if allow_event:
        assert delivered[0].value == 42
