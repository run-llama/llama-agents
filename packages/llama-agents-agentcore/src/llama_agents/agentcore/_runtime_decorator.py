from __future__ import annotations

from typing import Any, Awaitable, Callable, ParamSpec, TypeVar

from bedrock_agentcore.runtime import BedrockAgentCoreApp
from llama_agents.server._runtime.server_runtime import ServerRuntimeDecorator
from llama_agents.server._store.abstract_workflow_store import AbstractWorkflowStore
from workflows import Workflow
from workflows.events import StartEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    RegisteredWorkflow,
    Runtime,
    WorkflowRunFunction,
)
from workflows.runtime.types.step_function import (
    StepWorkerFunction,
    as_step_worker_functions,
    create_workflow_run_function,
)
from workflows.runtime.types.step_id import StepId

_P = ParamSpec("_P")
_R = TypeVar("_R")


def as_agentcore_async_task(
    app: BedrockAgentCoreApp,
    name: str,
    fn: Callable[_P, Awaitable[_R]],
) -> Callable[_P, Awaitable[_R]]:
    # Opaque pass-through typed via ParamSpec so the wrapper matches whatever
    # signature the underlying step worker exposes.
    async def step_worker(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        task_id = app.add_async_task(name)
        try:
            return await fn(*args, **kwargs)
        finally:
            app.complete_async_task(task_id)

    return step_worker


def as_agentcore_workflow_run(
    app: BedrockAgentCoreApp, name: str, fn: WorkflowRunFunction
) -> WorkflowRunFunction:
    """Wrap the entire workflow run as an async task.

    Keeps the container alive for the full workflow duration, not just
    individual steps.
    """

    async def wrapper(
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        tags: dict[str, Any] | None = None,
    ) -> StopEvent:
        task_id = app.add_async_task(f"{name}.run")
        try:
            return await fn(init_state, start_event, tags)
        finally:
            app.complete_async_task(task_id)

    return wrapper


class AgentCoreRuntimeDecorator(ServerRuntimeDecorator):
    def __init__(
        self,
        decorated: Runtime,
        store: AbstractWorkflowStore,
        app: BedrockAgentCoreApp,
        *,
        persistence_backoff: list[float] | None = None,
    ) -> None:
        super().__init__(decorated, store, persistence_backoff=persistence_backoff)
        self.app = app
        self._tracked: dict[str, Workflow] = {}
        self._registered: dict[int, RegisteredWorkflow] = {}

    def register(self, workflow: Workflow) -> RegisteredWorkflow:
        name = workflow.workflow_name
        if name not in self._tracked:
            self._tracked[name] = workflow
        wrapped_steps: dict[StepId, StepWorkerFunction] = {
            step_name: as_agentcore_async_task(self.app, f"{name}.{step_name}", step)
            for step_name, step in as_step_worker_functions(workflow).items()
        }
        run_fn = create_workflow_run_function(workflow)
        registered = RegisteredWorkflow(
            steps=wrapped_steps,
            workflow_run_fn=as_agentcore_workflow_run(self.app, name, run_fn),
            workflow=workflow,
        )
        id_ = id(workflow)
        self._registered[id_] = registered
        return registered

    def track_workflow(self, workflow: Workflow) -> None:
        self._tracked[workflow.workflow_name] = workflow
        super().track_workflow(workflow)

    def untrack_workflow(self, workflow: Workflow) -> None:
        self._tracked.pop(workflow.workflow_name, None)
        super().untrack_workflow(workflow)

    def get_registered(self, workflow: Workflow) -> RegisteredWorkflow | None:
        return self._registered.get(id(workflow))

    def get_workflow(self, name: str) -> Workflow | None:
        return self._tracked.get(name)

    def get_workflow_names(self) -> list[str]:
        return list(self._tracked.keys())
