from typing import cast

import pytest
from llama_agents.agentcore._runtime_decorator import AgentCoreRuntimeDecorator
from llama_agents.agentcore._service import AgentCoreService, WorkflowNotFoundError
from llama_agents.server._service import _WorkflowService
from llama_agents.server._store.memory_workflow_store import MemoryWorkflowStore
from workflows import Workflow
from workflows.events import StartEvent, StopEvent

from .conftest import (
    DummyFileWorkflow,
    DummyMetadataWorkflow,
    DummyWorkflow,
    DummyWorkflowWithError,
    FileEvent,
    MockBedrockApp,
)


@pytest.fixture()
def workflows() -> dict[str, Workflow]:
    return {
        "default": DummyWorkflow(),
        "metadata": DummyMetadataWorkflow(),
        "process-file": DummyFileWorkflow(),
    }


@pytest.fixture()
def store() -> MemoryWorkflowStore:
    return MemoryWorkflowStore()


def test_init(store: MemoryWorkflowStore) -> None:
    app = MockBedrockApp()
    service = AgentCoreService(store=store, app=app)  # type: ignore
    assert isinstance(service._workflow_store, MemoryWorkflowStore)
    assert isinstance(service._runtime, AgentCoreRuntimeDecorator)
    assert isinstance(service._service, _WorkflowService)


def test_add_workflows(
    store: MemoryWorkflowStore, workflows: dict[str, Workflow]
) -> None:
    workflows_cp = workflows.copy()
    app = MockBedrockApp()
    service = AgentCoreService(store=store, app=app)  # type: ignore
    for name in workflows_cp:
        service.add_workflow(name, workflows_cp[name])
        wf = service._service.get_workflow(name)
        assert wf is not None
        assert wf.workflow_name == name
    names = service._service.get_workflow_names()
    assert len(names) == len(workflows_cp)
    assert all(name in workflows_cp for name in names)


@pytest.mark.asyncio
async def test_run_workflow_simple_success(
    store: MemoryWorkflowStore, workflows: dict[str, Workflow]
) -> None:
    workflows_cp = workflows.copy()
    app = MockBedrockApp()
    service = AgentCoreService(store=store, app=app)  # type: ignore
    for name in workflows_cp:
        service.add_workflow(name, workflows_cp[name])
    result = await service.run_workflow(
        workflow_name="default", start_event=StartEvent()
    )
    assert result.result is not None
    event = cast(StopEvent, result.result.load_event([StopEvent]))
    assert event.result == "hello"
    assert app.added == 1  # 1 step -> add_async_task called once
    assert app.completed == 1  # 1 step -> complete_async_task called once


@pytest.mark.asyncio
async def test_run_workflow_file_success(
    store: MemoryWorkflowStore, workflows: dict[str, Workflow]
) -> None:
    workflows_cp = workflows.copy()
    app = MockBedrockApp()
    service = AgentCoreService(store=store, app=app)  # type: ignore
    for name in workflows_cp:
        service.add_workflow(name, workflows_cp[name])
    result = await service.run_workflow(
        workflow_name="process-file", start_event=FileEvent(file_id="1")
    )
    assert result.result is not None
    event = cast(StopEvent, result.result.load_event([StopEvent]))
    assert event.result == "1"
    assert app.added == 1  # 1 step -> add_async_task called once
    assert app.completed == 1  # 1 step -> complete_async_task called once


@pytest.mark.asyncio
async def test_run_workflow_retains_error(
    store: MemoryWorkflowStore, workflows: dict[str, Workflow]
) -> None:
    app = MockBedrockApp()
    service = AgentCoreService(store=store, app=app)  # type: ignore
    workflows_cp = workflows.copy()
    workflows_cp["with-error"] = DummyWorkflowWithError()
    for name in workflows_cp:
        service.add_workflow(name, workflows_cp[name])
    data = await service.run_workflow(
        workflow_name="with-error", start_event=StartEvent()
    )
    assert data.error is not None
    assert "You shall not pass!" == data.error
    assert app.added == 1  # 1 step -> add_async_task called once
    assert (
        app.completed == 1
    )  # Step raises, but should be completed thanks to the `try ... finally` block


@pytest.mark.asyncio
async def test_run_workflow_not_found_error(
    store: MemoryWorkflowStore, workflows: dict[str, Workflow]
) -> None:
    workflows_cp = workflows.copy()
    app = MockBedrockApp()
    service = AgentCoreService(store=store, app=app)  # type: ignore
    for name in workflows_cp:
        service.add_workflow(name, workflows_cp[name])
    with pytest.raises(WorkflowNotFoundError):
        await service.run_workflow(workflow_name="notexist", start_event=StartEvent())
