import asyncio
import json
import logging
import os
from collections.abc import Iterable, Mapping
from typing import Any, Tuple
from urllib.parse import quote_plus

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from llama_agents.appserver.deployment_config_parser import get_deployment_config
from llama_agents.appserver.settings import ApiserverSettings, settings
from llama_agents.appserver.types import generate_id
from llama_agents.appserver.workflow_loader import DEFAULT_SERVICE_ID
from llama_agents.core.deployment_config import DeploymentConfig
from llama_agents.server import (
    AbstractWorkflowStore,
    AgentDataStore,
    MemoryWorkflowStore,
    SqliteWorkflowStore,
    WorkflowServer,
)
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.routing import Route
from workflows import Context, Workflow
from workflows.context.serializers import BaseSerializer
from workflows.events import Event
from workflows.handler import WorkflowHandler

logger = logging.getLogger()


class DeploymentError(Exception): ...


class Deployment:
    def __init__(
        self,
        workflows: dict[str, Workflow],
        *,
        serializer: BaseSerializer | None = None,
        extra_types: Iterable[type[Any]] = (),
        additional_events: Mapping[str, Iterable[type[Event]]] | None = None,
    ) -> None:
        """Creates a Deployment instance.

        Args:
            config: The configuration object defining this deployment
            root_path: The path on the filesystem used to store deployment data
            local: Whether the deployment is local. If true, sources won't be synced
        """

        self._serializer = serializer
        self._extra_types = tuple(extra_types)
        self._additional_events = {
            name: tuple(events) for name, events in (additional_events or {}).items()
        }
        self._default_service: Workflow | None = workflows.get(DEFAULT_SERVICE_ID)
        self._service_tasks: list[asyncio.Task] = []
        # Ready to load services
        self._workflow_services: dict[str, Workflow] = workflows
        self._contexts: dict[str, Context] = {}
        self._handlers: dict[str, WorkflowHandler] = {}
        self._handler_inputs: dict[str, str] = {}

    @property
    def default_service(self) -> Workflow | None:
        """Return the default workflow, if any."""
        return self._default_service

    @property
    def service_names(self) -> list[str]:
        """Returns the list of service names in this deployment."""
        return list(self._workflow_services.keys())

    async def run_workflow(
        self, service_id: str, session_id: str | None = None, **run_kwargs: Any
    ) -> Any:
        workflow = self._workflow_services[service_id]
        if session_id is not None:
            context = self._contexts[session_id]
            return await workflow.run(context=context, **run_kwargs)

        if run_kwargs:
            return await workflow.run(**run_kwargs)

        return await workflow.run()

    def run_workflow_no_wait(
        self, service_id: str, session_id: str | None = None, **run_kwargs: Any
    ) -> Tuple[str, str]:
        workflow = self._workflow_services[service_id]
        if session_id is not None:
            context = self._contexts[session_id]
            handler = workflow.run(context=context, **run_kwargs)
        else:
            handler = workflow.run(**run_kwargs)
            session_id = generate_id()
            self._contexts[session_id] = handler.ctx or Context(workflow)

        handler_id = generate_id()
        self._handlers[handler_id] = handler
        self._handler_inputs[handler_id] = json.dumps(run_kwargs)
        assert session_id is not None
        return handler_id, session_id

    def create_workflow_server(
        self, deployment_config: DeploymentConfig, settings: ApiserverSettings
    ) -> WorkflowServer:
        persistence: AbstractWorkflowStore = MemoryWorkflowStore()
        if settings.persistence == "local":
            logger.info("Using local sqlite persistence for workflows")
            persistence = SqliteWorkflowStore(
                settings.local_persistence_path or "workflows.db"
            )
        elif settings.persistence == "cloud" or (
            # default to cloud if api key is present to use
            settings.persistence is None and os.getenv("LLAMA_CLOUD_API_KEY")
        ):
            logger.info("Using agent data cloud persistence for workflows")
            cloud_name = settings.cloud_persistence_name
            collection = "workflow_contexts"
            if cloud_name is not None:
                parts = cloud_name.split(":")
                if len(parts) > 1:
                    collection = parts[1]
                deployment_name = parts[0]
            else:
                deployment_name = deployment_config.name
            persistence = AgentDataStore(
                base_url=os.getenv(
                    "LLAMA_CLOUD_BASE_URL",
                    "https://api.cloud.llamaindex.ai",
                ),
                api_key=os.getenv("LLAMA_CLOUD_API_KEY", ""),
                project_id=os.getenv("LLAMA_DEPLOY_PROJECT_ID", ""),
                deployment_name=deployment_name,
                collection=collection,
            )
        else:
            logger.info("Not persisting workflows")
        server = WorkflowServer(
            workflow_store=persistence,
            serializer=self._serializer,
            extra_types=self._extra_types,
        )
        for service_id, workflow in self._workflow_services.items():
            additional_events = self._additional_events.get(service_id)
            server.add_workflow(
                service_id,
                workflow,
                additional_events=list(additional_events)
                if additional_events is not None
                else None,
            )
        return server

    def mount_workflow_server(self, app: FastAPI) -> WorkflowServer:
        config = get_deployment_config()
        server = self.create_workflow_server(config, settings)

        for route in server.app.routes:
            # add routes directly rather than mounting, so that we can share a root (only one ASGI app can be mounted at a path)
            if isinstance(route, Route):
                app.add_api_route(
                    f"/deployments/{{deployment_name}}{route.path}",
                    route.endpoint,
                    name=f"{config.name}_{route.name}",
                    methods=list(route.methods) if route.methods else None,
                    include_in_schema=True,  # change to false when schemas are added to workflow server
                    tags=["workflows"],
                )

        @app.get("/debugger", include_in_schema=False)
        @app.get("/debugger/", include_in_schema=False)
        def redirect_to_debugger(request: Request) -> RedirectResponse:
            return RedirectResponse(
                "/debugger/index.html?api=" + quote_plus("/deployments/" + config.name)
            )

        @app.get("/debugger/index.html", include_in_schema=False, response_model=None)
        def serve_debugger(api: str | None = None) -> RedirectResponse | HTMLResponse:
            if not api:
                return RedirectResponse(
                    "/debugger/index.html?api="
                    + quote_plus("/deployments/" + config.name)
                )
            else:
                return HTMLResponse("""<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>workflow-debugger</title>
    <script type="module" crossorigin src="https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@latest/dist/app.js"></script>
    <link rel="stylesheet" crossorigin href="https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@latest/dist/app.css">
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
""")

        return server
