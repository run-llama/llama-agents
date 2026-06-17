# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from importlib.metadata import version
from pathlib import Path
from typing import Any, AsyncGenerator, cast

from llama_agents.client.protocol import (
    CancelHandlerResponse,
    HandlerData,
    HandlersListResponse,
    HealthResponse,
    SendEventResponse,
    WorkflowEventsListResponse,
    WorkflowGraphResponse,
    WorkflowSchemaResponse,
)
from llama_agents.client.protocol.serializable_events import (
    EventEnvelope,
    EventEnvelopeWithMetadata,
    EventValidationError,
)
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route
from starlette.schemas import SchemaGenerator
from starlette.staticfiles import StaticFiles
from workflows import Context, Workflow
from workflows.events import Event, InternalDispatchEvent, StartEvent
from workflows.representation import get_workflow_representation
from workflows.utils import _nanoid as nanoid

from ._service import (
    EventSendError,
    HandlerCompletedError,
    HandlerNotFoundError,
    _WorkflowService,
)
from ._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    Status,
    is_terminal_status,
)

logger = logging.getLogger(__name__)


async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)


async def _unhandled_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    logger.error(
        "Unhandled exception on %s %s", request.method, request.url.path, exc_info=exc
    )
    return JSONResponse({"detail": f"Internal server error: {exc}"}, status_code=500)


_DEFAULT_ASSETS_PATH = Path(__file__).parent / "static"


class _WorkflowAPI:
    def __init__(
        self,
        service: _WorkflowService,
        *,
        middleware: list[Middleware] | None = None,
        exception_handlers: dict[Any, Any] | None = None,
        assets_path: Path = _DEFAULT_ASSETS_PATH,
        sse_heartbeat_interval: float | None = None,
        accept_context_api: bool = False,
    ) -> None:
        self._service = service
        self._additional_events: dict[str, list[type[Event]]] = {}
        self._sse_heartbeat_interval = sse_heartbeat_interval
        self._accept_context_api = accept_context_api

        exception_handlers = exception_handlers or {
            HTTPException: _http_exception_handler,
            Exception: _unhandled_exception_handler,
        }

        middleware = middleware or [
            Middleware(
                CORSMiddleware,  # type: ignore[arg-type]
                # regex echoes the origin header back, which some browsers require (rather than "*") when credentials are required
                allow_origin_regex=".*",
                allow_methods=["*"],
                allow_headers=["*"],
                allow_credentials=True,
            )
        ]

        @asynccontextmanager
        async def lifespan(app: Starlette) -> AsyncGenerator[None, None]:
            await self._service.start()
            try:
                yield
            finally:
                await self._service.stop()

        self.app = Starlette(
            routes=self._routes(),
            middleware=middleware,
            lifespan=lifespan,
            exception_handlers=exception_handlers,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        )
        self.app.mount(
            "/", app=StaticFiles(directory=assets_path, html=True), name="ui"
        )

    def register_additional_events(self, name: str, events: list[type[Event]]) -> None:
        self._additional_events[name] = events

    def get_workflow_events(self, workflow_name: str) -> list[type[Event]]:
        workflow = self._service.get_workflow(workflow_name)
        if workflow is None:
            return []
        return workflow.events + (self._additional_events.get(workflow_name) or [])

    def event_registry(self, workflow_name: str) -> dict[str, type[Event]]:
        """Return a name→type mapping of events for the given workflow."""
        return {e.__name__: e for e in self.get_workflow_events(workflow_name)}

    def _routes(self) -> list[Route]:
        return [
            Route("/workflows", self._list_workflows, methods=["GET"]),
            Route("/workflows/{name}/run", self._run_workflow, methods=["POST"]),
            Route(
                "/workflows/{name}/run-nowait",
                self._run_workflow_nowait,
                methods=["POST"],
            ),
            Route(
                "/workflows/{name}/schema",
                self._get_events_schema,
                methods=["GET"],
            ),
            Route(
                "/results/{handler_id}",
                self._get_workflow_result,
                methods=["GET"],
            ),
            Route("/events/{handler_id}", self._stream_events, methods=["GET"]),
            Route("/events/{handler_id}", self._post_event, methods=["POST"]),
            Route("/health", self._health_check, methods=["GET"]),
            Route("/handlers", self._get_handlers, methods=["GET"]),
            Route(
                "/handlers/{handler_id}",
                self._get_workflow_handler,
                methods=["GET"],
            ),
            Route(
                "/handlers/{handler_id}/cancel",
                self._cancel_handler,
                methods=["POST"],
            ),
            Route(
                "/workflows/{name}/representation",
                self._get_workflow_representation,
                methods=["GET"],
            ),
            Route(
                "/workflows/{name}/events",
                self._list_workflow_events,
                methods=["GET"],
            ),
        ]

    def openapi_schema(self) -> dict:
        gen = SchemaGenerator(
            {
                "openapi": "3.0.0",
                "info": {
                    "title": "Workflows API",
                    "version": version("llama-agents-server"),
                },
                "components": {
                    "schemas": {
                        "EventEnvelopeWithMetadata": {
                            "type": "object",
                            "properties": {
                                "value": {"type": "object"},
                                "types": {"type": "array", "items": {"type": "string"}},
                                "type": {"type": "string"},
                                "qualified_name": {"type": "string"},
                            },
                            "required": ["value", "type"],
                        },
                        "Handler": {
                            "type": "object",
                            "properties": {
                                "handler_id": {"type": "string"},
                                "workflow_name": {"type": "string"},
                                "run_id": {"type": "string", "nullable": True},
                                "status": {
                                    "type": "string",
                                    "enum": [
                                        "running",
                                        "completed",
                                        "failed",
                                        "cancelled",
                                    ],
                                },
                                "started_at": {"type": "string", "format": "date-time"},
                                "updated_at": {
                                    "type": "string",
                                    "format": "date-time",
                                    "nullable": True,
                                },
                                "completed_at": {
                                    "type": "string",
                                    "format": "date-time",
                                    "nullable": True,
                                },
                                "error": {"type": "string", "nullable": True},
                                "result": {
                                    "description": "Workflow result value",
                                    "oneOf": [
                                        {
                                            "$ref": "#/components/schemas/EventEnvelopeWithMetadata"
                                        },
                                        {"type": "null"},
                                    ],
                                },
                            },
                            "required": [
                                "handler_id",
                                "workflow_name",
                                "status",
                                "started_at",
                            ],
                        },
                        "HandlersList": {
                            "type": "object",
                            "properties": {
                                "handlers": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Handler"},
                                }
                            },
                            "required": ["handlers"],
                        },
                    }
                },
            }
        )

        return gen.get_schema(self.app.routes)

    #
    # HTTP endpoints
    #

    async def _health_check(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Health check
        description: Returns the server health status and workflow counts.
        responses:
          200:
            description: Successful health check
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    status:
                      type: string
                      example: healthy
                  required: [status]
        """
        if not self._service._runtime.is_launched:
            return JSONResponse(
                HealthResponse(status="unhealthy").model_dump(),
                status_code=503,
            )
        return JSONResponse(HealthResponse(status="healthy").model_dump())

    async def _list_workflows(self, request: Request) -> JSONResponse:
        """
        ---
        summary: List workflows
        description: Returns the list of registered workflow names.
        responses:
          200:
            description: List of workflows
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    workflows:
                      type: array
                      items:
                        type: string
                  required: [workflows]
        """
        workflow_names = self._service.get_workflow_names()
        return JSONResponse({"workflows": workflow_names})

    async def _list_workflow_events(self, request: Request) -> JSONResponse:
        """
        ---
        summary: List workflow events
        description: Returns the list of registered workflow event schemas.
        parameters:
          - in: path
            name: name
            required: true
            schema:
              type: string
            description: Registered workflow name.
        responses:
          200:
            description: List of workflow event schemas
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    events:
                      type: array
                      description: List of workflow event JSON schemas
                      items:
                        type: object
                  required: [events]
        """
        if "name" not in request.path_params:
            raise HTTPException(status_code=400, detail="name param is required")

        name = request.path_params["name"]
        if self._service.get_workflow(name) is None:
            raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")

        events = self.get_workflow_events(name)

        return JSONResponse(
            WorkflowEventsListResponse(
                events=[event.model_json_schema() for event in events]
            ).model_dump()
        )

    async def _run_workflow(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Run workflow (wait)
        description: |
          Runs the specified workflow synchronously and returns the final result.
          The request body may include an optional serialized start event and optional
          keyword arguments passed to the workflow run.
        parameters:
          - in: path
            name: name
            required: true
            schema:
              type: string
            description: Registered workflow name.
        requestBody:
          required: false
          content:
            application/json:
              schema:
                type: object
                properties:
                  start_event:
                    type: object
                    description: 'Plain JSON object representing the start event (e.g., {"message": "..."}).'
                  handler_id:
                    type: string
                    description: Workflow handler identifier to continue from a previous completed run.
                  kwargs:
                    type: object
                    description: Additional keyword arguments for the workflow.
        responses:
          200:
            description: Workflow completed successfully
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Handler'
          400:
            description: Invalid start_event payload
          404:
            description: Workflow or handler identifier not found
          500:
            description: Error running workflow or invalid request body
        """
        workflow = self._extract_workflow(request)
        context, start_event, handler_id = await self._extract_run_params(
            request, workflow, workflow.workflow_name
        )

        if start_event is not None:
            input_ev = workflow.start_event_class.model_validate(start_event)
        else:
            input_ev = None

        try:
            started = await self._service.start_workflow(
                workflow=workflow,
                handler_id=handler_id,
                context=context,
                start_event=input_ev,
            )
        except Exception as e:
            logger.error(f"Error running workflow: {e}", exc_info=True)
            raise HTTPException(detail=f"Error running workflow: {e}", status_code=500)

        try:
            handler_data = await self._service.await_workflow(started)
            if handler_data.status == "completed":
                status = 200
            else:
                logger.error(
                    "Workflow %s finished with status=%s error=%s (handler=%s, run=%s)",
                    handler_data.workflow_name,
                    handler_data.status,
                    handler_data.error,
                    handler_data.handler_id,
                    handler_data.run_id,
                )
                status = 500
        except Exception as e:
            logger.error(f"Error running workflow: {e}", exc_info=True)
            handler_data = await self._service.load_handler(handler_id)
            status = 500

        return JSONResponse(
            handler_data.model_dump() if handler_data else {}, status_code=status
        )

    async def _get_events_schema(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Get JSON schema for start event
        description: |
          Gets the JSON schema of the start and stop events from the specified workflow and returns it under "start" (start event) and "stop" (stop event)
        parameters:
          - in: path
            name: name
            required: true
            schema:
              type: string
            description: Registered workflow name.
        requestBody:
          required: false
        responses:
          200:
            description: JSON schema successfully retrieved for start event
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    start:
                      description: JSON schema for the start event
                    stop:
                      description: JSON schema for the stop event
                  required: [start, stop]
          404:
            description: Workflow not found
          500:
            description: Error while getting the JSON schema for the start or stop event
        """
        workflow = self._extract_workflow(request)
        try:
            start_event_schema = workflow.start_event_class.model_json_schema()
        except Exception as e:
            raise HTTPException(
                detail=f"Error getting schema of start event for workflow: {e}",
                status_code=500,
            )
        try:
            stop_event_schema = workflow.stop_event_class.model_json_schema()
        except Exception as e:
            raise HTTPException(
                detail=f"Error getting schema of stop event for workflow: {e}",
                status_code=500,
            )

        return JSONResponse(
            WorkflowSchemaResponse(
                start=start_event_schema, stop=stop_event_schema
            ).model_dump()
        )

    async def _get_workflow_representation(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Get the representation of the workflow
        description: |
          Get the representation of the workflow as a directed graph in JSON format
        parameters:
          - in: path
            name: name
            required: true
            schema:
              type: string
            description: Registered workflow name.
        requestBody:
          required: false
        responses:
          200:
            description: JSON representation successfully retrieved
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    graph:
                      description: the elements of the JSON representation of the workflow
                  required: [graph]
          404:
            description: Workflow not found
          500:
            description: Error while getting JSON workflow representation
        """
        workflow = self._extract_workflow(request)
        try:
            workflow_graph = get_workflow_representation(workflow)
        except Exception as e:
            raise HTTPException(
                detail=f"Error while getting JSON workflow representation: {e}",
                status_code=500,
            )
        return JSONResponse(WorkflowGraphResponse(graph=workflow_graph).model_dump())

    async def _run_workflow_nowait(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Run workflow (no-wait)
        description: |
          Starts the specified workflow asynchronously and returns a handler identifier
          which can be used to query results or stream events.
        parameters:
          - in: path
            name: name
            required: true
            schema:
              type: string
            description: Registered workflow name.
        requestBody:
          required: false
          content:
            application/json:
              schema:
                type: object
                properties:
                  start_event:
                    type: object
                    description: 'Plain JSON object representing the start event (e.g., {"message": "..."}).'
                  handler_id:
                    type: string
                    description: Workflow handler identifier to continue from a previous completed run.
                  kwargs:
                    type: object
                    description: Additional keyword arguments for the workflow.
        responses:
          200:
            description: Workflow started
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Handler'
          400:
            description: Invalid start_event payload
          404:
            description: Workflow or handler identifier not found
        """
        workflow = self._extract_workflow(request)
        context, start_event, handler_id = await self._extract_run_params(
            request, workflow, workflow.workflow_name
        )

        if start_event is not None:
            input_ev = workflow.start_event_class.model_validate(start_event)
        else:
            input_ev = None

        try:
            handler_data = await self._service.start_workflow(
                workflow=workflow,
                handler_id=handler_id,
                context=context,
                start_event=input_ev,
            )
        except Exception as e:
            raise HTTPException(
                detail=f"Initial persistence failed: {e}", status_code=500
            )
        return JSONResponse(handler_data.model_dump())

    async def _load_handler(self, handler_id: str) -> HandlerData:
        handler_data = await self._service.load_handler(handler_id)
        if handler_data is None:
            raise HTTPException(detail="Handler not found", status_code=404)
        return handler_data

    async def _resolve_event_stream(
        self,
        handler_id: str,
        *,
        after_sequence: int | None,
        include_internal: bool,
        include_children: bool,
        include_qualified_name: bool,
    ) -> AsyncGenerator[tuple[int, EventEnvelopeWithMetadata], None] | None:
        """Resolve a handler to an event stream.

        Args:
            handler_id: The handler to stream events for.
            after_sequence: Resume after this sequence number. None means "now"
                (skip historical events).
            include_internal: Whether to include internal dispatch events.
            include_children: Whether to include child-origin events.
            include_qualified_name: Whether to include qualified_name in envelopes.

        Returns:
            An async generator of (sequence, envelope) tuples, or None if the
            handler is completed and all events have been consumed.

        Raises:
            HTTPException: 404 if handler not found or has no run.
        """
        store = self._service.store

        # Resolve handler_id → run_id via persistence
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        if not found:
            raise HTTPException(detail="Handler not found", status_code=404)

        persistent = found[0]
        run_id = persistent.run_id
        if run_id is None:
            raise HTTPException(detail="Handler has no associated run", status_code=404)

        # Resolve "now" cursor to current max sequence
        if after_sequence is None:
            all_current = await store.query_events(run_id)
            after_sequence = all_current[-1].sequence if all_current else -1

        # Check if already fully consumed
        remaining_events = await store.query_events(
            run_id, after_sequence=after_sequence
        )
        if not remaining_events:
            all_events = await store.query_events(run_id)
            run_is_complete = is_terminal_status(persistent.status) or (
                bool(all_events)
                and AbstractWorkflowStore._is_terminal_event(all_events[-1])
            )
            if run_is_complete:
                return None

        _INTERNAL_EVENT_TYPE = InternalDispatchEvent.__name__

        async def event_gen() -> AsyncGenerator[
            tuple[int, EventEnvelopeWithMetadata], None
        ]:
            async for stored_event in store.subscribe_events(
                run_id,
                after_sequence=after_sequence,  # type: ignore[arg-type]
            ):
                envelope = stored_event.event
                types = (envelope.types or []) + [envelope.type]
                if not include_internal and _INTERNAL_EVENT_TYPE in types:
                    continue
                if envelope.origin_namespace and not include_children:
                    continue
                if not include_qualified_name:
                    envelope = envelope.model_copy(update={"qualified_name": None})
                yield stored_event.sequence, envelope

        return event_gen()

    async def _get_workflow_result(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Get workflow result (deprecated)
        description: |
          Deprecated. Use GET /handlers/{handler_id} instead. Returns the final result of an asynchronously started workflow, if available.
        parameters:
          - in: path
            name: handler_id
            required: true
            schema:
              type: string
            description: Workflow run identifier returned from the no-wait run endpoint.
        deprecated: true
        responses:
          200:
            description: Result is available
            content:
              application/json:
                schema:
                  type: object
          202:
            description: Result not ready yet
            content:
              application/json:
                schema:
                  type: object
          404:
            description: Handler not found
          500:
            description: Error computing result
            content:
              text/plain:
                schema:
                  type: string
        """
        handler_id = request.path_params["handler_id"]
        if not handler_id:
            raise HTTPException(detail="Handler ID is required", status_code=400)

        handler_data = await self._load_handler(handler_id)
        status = (
            202
            if handler_data.status == "running"
            else 200
            if handler_data.status == "completed"
            else 500
        )
        response_model = handler_data.model_dump()

        # compatibility. Use handler api instead
        if not handler_data.result:
            response_model["result"] = None
        else:
            type = handler_data.result.qualified_name
            response_model["result"] = (
                handler_data.result.value.get("result")
                if type == "workflows.events.StopEvent"
                else handler_data.result.value
            )
        return JSONResponse(response_model, status_code=status)

    async def _get_workflow_handler(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Get workflow handler
        description: Returns the final result of an asynchronously started workflow, if available
        parameters:
          - in: path
            name: handler_id
            required: true
            schema:
              type: string
            description: Workflow run identifier returned from the no-wait run endpoint.
        responses:
          200:
            description: Result is available
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Handler'
          202:
            description: Result not ready yet
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Handler'
          404:
            description: Handler not found
          500:
            description: Error computing result
            content:
              text/plain:
                schema:
                  type: string
        """
        handler_id = request.path_params["handler_id"]
        if not handler_id:
            raise HTTPException(detail="Handler ID is required", status_code=400)

        handler_data = await self._load_handler(handler_id)
        status = (
            202
            if handler_data.status == "running"
            else 200
            if handler_data.status == "completed"
            else 500
        )
        return JSONResponse(handler_data.model_dump(), status_code=status)

    async def _stream_events(self, request: Request) -> StreamingResponse:
        """
        ---
        summary: Stream workflow events
        description: |
          Streams events produced by a workflow execution. Events are emitted as
          newline-delimited JSON by default, or as Server-Sent Events when `sse=true`.
          Multiple clients can stream the same handler concurrently. Disconnected
          clients can resume from their last-seen position via `after_sequence`.

          Event data is returned as an envelope:
          {
            "value": <pydantic serialized value>,
            "types": [<class names from MRO excluding the event class and base Event>],
            "type": <class name>,
            "qualified_name": <python module path + class name>,
          }

        parameters:
          - in: path
            name: handler_id
            required: true
            schema:
              type: string
            description: Identifier returned from the no-wait run endpoint.
          - in: query
            name: sse
            required: false
            schema:
              type: boolean
              default: true
            description: If false, as NDJSON instead of Server-Sent Events.
          - in: query
            name: include_internal
            required: false
            schema:
              type: boolean
              default: false
            description: If true, include internal workflow events (e.g., step state changes).
          - in: query
            name: include_children
            required: false
            schema:
              type: boolean
              default: false
            description: If true, include events emitted from child workflow namespaces.
          - in: query
            name: after_sequence
            required: false
            schema:
              oneOf:
                - type: integer
                - type: string
                  enum: [now]
              default: now
            description: >
              Resume streaming after this event sequence number. Use -1
              to start from the beginning, or "now" (default) to skip historical events and
              only receive events appended after the request is made.
              In SSE mode, the Last-Event-ID request header takes priority over
              this parameter.
          - in: query
            name: include_qualified_name
            required: false
            schema:
              type: boolean
              default: true
            description: If true, include the qualified name of the event in the response body.
        responses:
          200:
            description: Streaming started
            content:
              text/event-stream:
                schema:
                  type: object
                  description: Server-Sent Events stream of event data.
                  properties:
                    value:
                      type: object
                      description: The event value.
                    type:
                      type: string
                      description: The class name of the event.
                    types:
                      type: array
                      description: Superclass names from MRO (excluding the event class and base Event).
                      items:
                        type: string
                    qualified_name:
                      type: string
                      description: The qualified name of the event.
                  required: [value, type]
          204:
            description: Handler completed and all events already consumed
          404:
            description: Handler not found
        """
        handler_id = request.path_params["handler_id"]
        include_internal = (
            request.query_params.get("include_internal", "false").lower() == "true"
        )
        include_children = (
            request.query_params.get("include_children", "false").lower() == "true"
        )
        include_qualified_name = (
            request.query_params.get("include_qualified_name", "true").lower() == "true"
        )
        sse = request.query_params.get("sse", "true").lower() == "true"
        after_sequence_str = request.query_params.get("after_sequence", "now")
        after_sequence_is_now = after_sequence_str.lower() == "now"
        if after_sequence_is_now:
            after_sequence: int | None = None  # resolved by helper
        else:
            try:
                after_sequence = int(after_sequence_str)
            except ValueError:
                raise HTTPException(
                    detail=f"Invalid after_sequence: '{after_sequence_str}'",
                    status_code=400,
                )

        # SSE Last-Event-ID header overrides after_sequence
        if sse:
            last_event_id = request.headers.get("last-event-id")
            if last_event_id is not None:
                try:
                    after_sequence = int(last_event_id)
                except ValueError:
                    pass  # Ignore non-integer Last-Event-ID

        gen = await self._resolve_event_stream(
            handler_id,
            after_sequence=after_sequence,
            include_internal=include_internal,
            include_children=include_children,
            include_qualified_name=include_qualified_name,
        )
        if gen is None:
            raise HTTPException(detail="Handler is completed", status_code=204)

        media_type = "text/event-stream" if sse else "application/x-ndjson"
        heartbeat_interval = self._sse_heartbeat_interval if sse else None

        async def format_stream() -> AsyncGenerator[str, None]:
            # We use a queue + feeder task so we can apply wait_for on
            # queue.get() without corrupting async-generator state (which
            # happens when asyncio.wait_for cancels __anext__).
            _SENTINEL = object()
            queue: asyncio.Queue[tuple[int, EventEnvelopeWithMetadata] | object] = (
                asyncio.Queue()
            )

            async def _feed() -> None:
                async for item in gen:
                    await queue.put(item)
                await queue.put(_SENTINEL)

            feeder = asyncio.ensure_future(_feed())
            try:
                while True:
                    try:
                        item = await asyncio.wait_for(
                            queue.get(), timeout=heartbeat_interval
                        )
                    except asyncio.TimeoutError:
                        yield ": heartbeat\n\n"
                        await asyncio.sleep(0)
                        continue
                    if item is _SENTINEL:
                        break
                    sequence, envelope = cast(
                        tuple[int, EventEnvelopeWithMetadata], item
                    )
                    payload = envelope.model_dump_json()
                    if sse:
                        yield f"id: {sequence}\ndata: {payload}\n\n"
                    else:
                        yield f"{payload}\n"
                    await asyncio.sleep(0)
            finally:
                feeder.cancel()

        return StreamingResponse(format_stream(), media_type=media_type)

    async def _get_handlers(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Get handlers
        description: Returns workflow handlers, optionally filtered by query parameters.
        parameters:
          - in: query
            name: status
            required: false
            schema:
              type: array
              items:
                type: string
                enum: [running, completed, failed, cancelled]
            style: form
            explode: true
            description: |
              Filter by handler status. Can be provided multiple times (e.g., status=running&status=failed)
          - in: query
            name: workflow_name
            required: false
            schema:
              type: array
              items:
                type: string
            style: form
            explode: true
            description: |
              Filter by workflow name. Can be provided multiple times (e.g., workflow_name=test&workflow_name=other)
        responses:
          200:
            description: List of handlers
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/HandlersList'
        """

        def _parse_list_param(param_name: str) -> list[str] | None:
            # parse repeated params
            values = list(request.query_params.getlist(param_name))
            if not values:
                single = request.query_params.get(param_name) or ""
                values = [single]
            values = [value.strip() for value in values if value.strip()]
            if not values:
                return None
            return values

        # Parse filters
        status_values = _parse_list_param("status")
        workflow_name_in = _parse_list_param("workflow_name")

        # Narrow types for status to match HandlerQuery expectations
        allowed_status_values: set[Status] = {
            "running",
            "completed",
            "failed",
            "cancelled",
        }

        status_in: list[Status] | None = (
            list(set(allowed_status_values).intersection(status_values))
            if status_values is not None
            else None
        )
        persistent_handlers = await self._service.query_handlers(
            HandlerQuery(status_in=status_in, workflow_name_in=workflow_name_in)
        )
        items = [
            HandlerData(
                handler_id=h.handler_id,
                workflow_name=h.workflow_name,
                run_id=h.run_id,
                status=h.status,
                started_at=h.started_at.isoformat() if h.started_at else "",
                updated_at=h.updated_at.isoformat() if h.updated_at else None,
                completed_at=h.completed_at.isoformat() if h.completed_at else None,
                error=h.error,
                result=EventEnvelopeWithMetadata.from_event(h.result)
                if h.result
                else None,
            )
            for h in persistent_handlers
        ]
        return JSONResponse(HandlersListResponse(handlers=items).model_dump())

    async def _post_event(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Send event to workflow
        description: Sends an event to a running workflow's context.
        parameters:
          - in: path
            name: handler_id
            required: true
            schema:
              type: string
            description: Workflow handler identifier.
        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
                properties:
                  event:
                    description: Serialized event. Accepts object or JSON-encoded string for backward compatibility.
                    oneOf:
                      - type: string
                        description: JSON string of the event envelope or value.
                        examples:
                          - '{"type": "ExternalEvent", "value": {"response": "hi"}}'
                      - type: object
                        properties:
                          type:
                            type: string
                            description: The class name of the event.
                          value:
                            type: object
                            description: The event value object (preferred over data).
                        additionalProperties: true
                  step:
                    type: string
                    description: Optional target step name. If not provided, event is sent to all steps.
                required: [event]
        responses:
          200:
            description: Event sent successfully
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    status:
                      type: string
                      enum: [sent]
                  required: [status]
          400:
            description: Invalid event data
          404:
            description: Handler not found
          409:
            description: Workflow already completed
        """
        handler_id = request.path_params["handler_id"]

        # Parse request body
        try:
            body = await request.json()
        except Exception as e:
            raise HTTPException(
                detail=f"Error processing request: {e}", status_code=400
            )

        event_data = body.get("event")
        step = body.get("step")

        if not event_data:
            raise HTTPException(detail="Event data is required", status_code=400)

        try:
            handler_data = await self._service.resolve_handler(handler_id)
        except HandlerNotFoundError:
            raise HTTPException(detail="Handler not found", status_code=404)
        except HandlerCompletedError:
            raise HTTPException(detail="Workflow already completed", status_code=409)

        try:
            event = EventEnvelope.parse(
                event_data, self.event_registry(handler_data.workflow_name)
            )
        except EventValidationError as e:
            raise HTTPException(detail=str(e), status_code=400)
        except Exception as e:
            raise HTTPException(
                detail=f"Failed to deserialize event: {e}", status_code=400
            )

        try:
            await self._service.send_event(handler_id, event, step=step)
        except HandlerNotFoundError:
            raise HTTPException(detail="Handler not found", status_code=404)
        except HandlerCompletedError:
            raise HTTPException(detail="Workflow already completed", status_code=409)
        except EventSendError as e:
            raise HTTPException(detail=str(e), status_code=500)

        return JSONResponse(SendEventResponse(status="sent").model_dump())

    async def _cancel_handler(self, request: Request) -> JSONResponse:
        """
        ---
        summary: Stop and delete handler
        description: |
          Stops a running workflow handler by cancelling its tasks. Optionally removes the
          handler from the persistence store if purge=true.
        parameters:
          - in: path
            name: handler_id
            required: true
            schema:
              type: string
            description: Workflow handler identifier.
          - in: query
            name: purge
            required: false
            schema:
              type: boolean
              default: false
            description: If true, also deletes the handler from the store, otherwise updates the status to cancelled.
        responses:
          200:
            description: Handler cancelled and deleted or cancelled only
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    status:
                      type: string
                      enum: [deleted, cancelled]
                  required: [status]
          404:
            description: Handler not found
        """
        handler_id = request.path_params["handler_id"]
        # Simple boolean parsing aligned with other APIs (e.g., `sse`): only "true" enables
        purge = request.query_params.get("purge", "false").lower() == "true"

        result = await self._service.cancel_handler(handler_id, purge=purge)
        if result is None:
            raise HTTPException(detail="Handler not found", status_code=404)

        return JSONResponse(CancelHandlerResponse(status=result).model_dump())

    #
    # Private methods
    #
    def _extract_workflow(self, request: Request) -> Workflow:
        if "name" not in request.path_params:
            raise HTTPException(detail="'name' parameter missing", status_code=400)
        name = request.path_params["name"]

        workflow = self._service.get_workflow(name)
        if workflow is None:
            raise HTTPException(detail="Workflow not found", status_code=404)

        return workflow

    async def _extract_run_params(
        self, request: Request, workflow: Workflow, workflow_name: str
    ) -> tuple[Context | None, StartEvent | None, str]:
        try:
            try:
                body = await request.json()
            except Exception as e:
                raise HTTPException(detail=f"Invalid JSON body: {e}", status_code=400)
            context_data = body.get("context")
            run_kwargs = body.get("kwargs", {})
            start_event_data = body.get("start_event", run_kwargs)
            handler_id = body.get("handler_id")

            # Extract custom StartEvent if present
            start_event = None
            if start_event_data is not None:
                try:
                    start_event = EventEnvelope.parse(
                        start_event_data,
                        self.event_registry(workflow_name),
                        explicit_event=workflow.start_event_class,
                    )

                except Exception as e:
                    raise HTTPException(
                        detail=f"Validation error for 'start_event': {e}",
                        status_code=400,
                    )
                if start_event is not None and not isinstance(
                    start_event, workflow.start_event_class
                ):
                    raise HTTPException(
                        detail=f"Start event must be an instance of {workflow.start_event_class}",
                        status_code=400,
                    )

            # Extract custom Context if present
            context = None
            if context_data:
                if not self._accept_context_api:
                    raise HTTPException(
                        detail="Context API is disabled. Set accept_context_api=True on WorkflowServer to enable it.",
                        status_code=400,
                    )
                context = Context.from_dict(workflow=workflow, data=context_data)

            handler_id = handler_id or nanoid()
            return (context, start_event, handler_id)

        except HTTPException:
            # Re-raise HTTPExceptions as-is (like start_event validation errors)
            raise
        except Exception as e:
            raise HTTPException(
                detail=f"Error processing request body: {e}", status_code=400
            )
