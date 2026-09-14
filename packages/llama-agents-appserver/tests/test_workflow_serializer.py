# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import sys
from types import ModuleType

import pytest
from llama_agents.appserver.deployment import Deployment
from llama_agents.appserver.settings import ApiserverSettings
from llama_agents.appserver.workflow_loader import load_workflow_server
from llama_agents.core.deployment_config import DeploymentConfig
from llama_agents.server import WorkflowServer
from workflows import Workflow, step
from workflows.context.serializers import PickleSerializer
from workflows.events import StartEvent, StopEvent


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
    deployment = Deployment(loaded.get_workflows(), serializer=loaded.serializer)
    hosted = deployment.create_workflow_server(
        config, ApiserverSettings(persistence="memory")
    )
    assert hosted.serializer is serializer
    assert hosted.get_workflows()["example"] is workflow
    assert workflow.runtime.get_serializer(workflow) is serializer
