# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import uvicorn
from llama_agents.server._diagnostics import (
    PressureDiagnosticsConfig,
    PressureDiagnosticsDecorator,
    PressureDiagnosticsRecorder,
)
from llama_agents.server._runtime.idle_release_runtime import IdleReleaseDecorator
from llama_agents.server._runtime.persistence_runtime import PersistenceDecorator
from llama_agents.server._runtime.server_runtime import ServerRuntimeDecorator
from starlette.middleware import Middleware
from workflows import Workflow
from workflows.events import Event
from workflows.plugins.basic import basic_runtime
from workflows.runtime.types.plugin import Runtime

from ._api import _WorkflowAPI
from ._service import _WorkflowService
from ._store.abstract_workflow_store import AbstractWorkflowStore
from ._store.memory_workflow_store import MemoryWorkflowStore

logger = logging.getLogger(__name__)


class WorkflowServer:
    """HTTP server that exposes workflows as REST APIs.

    Wraps one or more ``Workflow`` instances behind an HTTP API with endpoints
    for running workflows, streaming events, and sending human-in-the-loop
    input. Includes a built-in debugging UI served at the root path.

    Example:

        from workflows import Workflow, step
        from workflows.events import StartEvent, StopEvent
        from llama_agents.server import WorkflowServer

        class GreetingWorkflow(Workflow):
            @step
            async def greet(self, ev: StartEvent) -> StopEvent:
                name = ev.get("name", "World")
                return StopEvent(result=f"Hello, {name}!")

        server = WorkflowServer()
        server.add_workflow("greet", GreetingWorkflow())

        # Run with: python -m workflows.server my_server.py
        # Or programmatically:
        # await server.serve(host="0.0.0.0", port=8080)

    The ASGI application is available as ``server.app`` for embedding in a
    larger application or mounting behind a reverse proxy.
    """

    def __init__(
        self,
        *,
        middleware: list[Middleware] | None = None,
        exception_handlers: Mapping[Any, Any] | None = None,
        workflow_store: AbstractWorkflowStore | None = None,
        persistence_backoff: list[float] = [0.5, 3],
        runtime: Runtime | None = None,
        idle_timeout: float = 60.0,
        sse_heartbeat_interval: float | None = 25.0,
        accept_context_api: bool = False,
        diagnostics: PressureDiagnosticsConfig | None = None,
    ):
        """Create a new workflow server.

        Args:
            middleware: Starlette middleware to apply to the ASGI app. Defaults
                to a permissive CORS configuration. Passing a custom list
                replaces the default entirely.
            exception_handlers: Starlette exception handlers mapping exception
                types to handler callables. Defaults to JSON error responses
                with logging. Passing a custom mapping replaces the default
                entirely.
            workflow_store: Persistence backend for handler state, events, and
                ticks. Defaults to ``MemoryWorkflowStore``. Use
                ``SqliteWorkflowStore`` or ``PostgresWorkflowStore`` for
                durable persistence across restarts.
            persistence_backoff: Retry delays (in seconds) when writing handler
                state to the store fails. Each entry is a sleep duration before
                the next attempt. Defaults to ``[0.5, 3]`` (two retries).
            runtime: Custom workflow runtime. When ``None`` (the default), the
                server builds a runtime stack that handles persistence and
                idle-release automatically. Only override this if you need a
                custom execution backend.
            idle_timeout: Seconds to wait after a workflow becomes idle before
                releasing it from memory. The workflow is automatically
                reloaded when new events arrive. Defaults to ``60.0``.
            sse_heartbeat_interval: Seconds between SSE keep-alive comments
                (``: heartbeat``) on idle connections. Defaults to ``25.0``.
                Set to ``None`` to disable heartbeats. Only applies to SSE
                mode; NDJSON streams are unaffected.
            accept_context_api: Allow the ``"context"`` field in run request
                bodies. Defaults to ``False``. Context deserialization can
                instantiate arbitrary Pydantic objects via ``importlib``, so
                only enable this on trusted networks.
            diagnostics: Optional pressure diagnostics configuration. Diagnostics
                are disabled by default and only start sampler tasks when this
                is provided with ``enabled=True``.
        """
        self.diagnostics = (
            diagnostics if diagnostics is not None else PressureDiagnosticsConfig()
        )
        self.pressure_diagnostics_recorder: PressureDiagnosticsRecorder | None = None
        self._workflow_store = (
            workflow_store if workflow_store is not None else MemoryWorkflowStore()
        )
        inner: Runtime = (
            runtime
            if runtime is not None
            else IdleReleaseDecorator(
                PersistenceDecorator(basic_runtime, store=self._workflow_store),
                store=self._workflow_store,
                idle_timeout=idle_timeout,
            )
        )
        if self.diagnostics.enabled:
            self.pressure_diagnostics_recorder = PressureDiagnosticsRecorder(
                self.diagnostics
            )
            inner = PressureDiagnosticsDecorator(
                inner, self.pressure_diagnostics_recorder
            )
        self._runtime: ServerRuntimeDecorator = ServerRuntimeDecorator(
            inner,
            store=self._workflow_store,
            persistence_backoff=list(persistence_backoff),
        )
        self._service = _WorkflowService(
            runtime=self._runtime, store=self._workflow_store
        )

        self._api = _WorkflowAPI(
            self._service,
            middleware=middleware,
            exception_handlers=dict(exception_handlers) if exception_handlers else None,
            sse_heartbeat_interval=sse_heartbeat_interval,
            accept_context_api=accept_context_api,
        )
        self.app = self._api.app

    # ------------------------------------------------------------------
    # Workflow registration
    # ------------------------------------------------------------------

    def add_workflow(
        self,
        name: str,
        workflow: Workflow,
        additional_events: list[type[Event]] | None = None,
    ) -> None:
        """Register a workflow under the given name.

        The workflow becomes available at ``/workflows/{name}/run`` and
        ``/workflows/{name}/run-nowait``.

        Args:
            name: URL-safe name for the workflow.
            workflow: The workflow instance to serve.
            additional_events: Extra event types to expose in the debugger UI
                and ``Send Event`` functionality. Use this for events that
                aren't discoverable from step signatures alone (e.g. events
                consumed via ``ctx.wait_for_event()``).
        """
        workflow._switch_workflow_name(name)
        workflow._switch_runtime(self._runtime)

        if additional_events is not None:
            self._api.register_additional_events(name, additional_events)

    def get_workflows(self) -> dict[str, Workflow]:
        """Return registered workflows as a dict by name. Only available after start()."""
        return {
            n: wf
            for n in self._service.get_workflow_names()
            if (wf := self._service.get_workflow(n)) is not None
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> WorkflowServer:
        """Resumes previously running workflows, if they were not complete at last shutdown.

        Idle workflows are not resumed - they remain released and will be
        loaded on-demand when events arrive for them.
        """
        if self.pressure_diagnostics_recorder is not None:
            await self.pressure_diagnostics_recorder.start()
        try:
            await self._service.start()
            return self
        except Exception:
            if self.pressure_diagnostics_recorder is not None:
                await self.pressure_diagnostics_recorder.stop()
            raise

    @asynccontextmanager
    async def contextmanager(self) -> AsyncGenerator[WorkflowServer, None]:
        """Use this server as a context manager to start and stop it"""
        await self.start()
        try:
            yield self
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully shut down all running workflow handlers."""
        try:
            await self._service.stop()
        finally:
            if self.pressure_diagnostics_recorder is not None:
                await self.pressure_diagnostics_recorder.stop()

    # ------------------------------------------------------------------
    # Serve
    # ------------------------------------------------------------------

    async def serve(
        self,
        host: str = "localhost",
        port: int = 80,
        uvicorn_config: dict[str, Any] | None = None,
    ) -> None:
        """Start the HTTP server and block until shutdown.

        Calls ``start()`` internally before serving.

        Args:
            host: Bind address. Defaults to ``"localhost"``.
            port: Bind port. Defaults to ``80``.
            uvicorn_config: Additional keyword arguments forwarded to
                ``uvicorn.Config`` (e.g. ``root_path``, ``log_level``).
        """
        uvicorn_config = uvicorn_config or {}

        config = uvicorn.Config(self.app, host=host, port=port, **uvicorn_config)
        server = uvicorn.Server(config)
        logger.info(
            f"Starting Workflow server at http://{host}:{port}{uvicorn_config.get('root_path', '/')}"
        )

        await server.serve()

    def openapi_schema(self) -> dict:
        return self._api.openapi_schema()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate OpenAPI schema")
    parser.add_argument(
        "--output", type=str, default="openapi.json", help="Output file path"
    )
    args = parser.parse_args()

    server = WorkflowServer()
    dict_schema = server.openapi_schema()
    with open(args.output, "w") as f:
        json.dump(dict_schema, indent=2, fp=f)
    print(f"OpenAPI schema written to {args.output}")  # noqa: T201
