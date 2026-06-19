# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Literal,
    overload,
)

import httpx
from workflows import Context
from workflows.events import Event, StartEvent

from .protocol import (
    CancelHandlerResponse,
    HandlerData,
    HandlersListResponse,
    HealthResponse,
    SendEventResponse,
    Status,
    WorkflowsListResponse,
)
from .protocol.serializable_events import (
    EventEnvelope,
    EventEnvelopeWithMetadata,
)


def _raise_for_status_with_body(response: httpx.Response) -> None:
    """
    Raise an HTTPStatusError with the first 200 characters of the response body
    for 400 and 500 level errors.
    """
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if 400 <= e.response.status_code < 600:
            body_preview = e.response.text[:200]
            method = e.request.method
            url = e.request.url
            status_code = e.response.status_code
            raise httpx.HTTPStatusError(
                f"{status_code} {e.response.reason_phrase} for {method} {url}. Response: {body_preview}",
                request=e.request,
                response=e.response,
            ) from e
        raise


@dataclass(frozen=True)
class _QueuedEvent:
    sequence: int | Literal["now"]
    event: EventEnvelopeWithMetadata


@dataclass(frozen=True)
class _QueuedError:
    error: BaseException


@dataclass(frozen=True)
class _QueuedDone:
    pass


_QueueItem = _QueuedEvent | _QueuedError | _QueuedDone


class EventStream:
    """Async iterator over workflow events that exposes the current stream position.

    Returned by ``WorkflowClient.get_workflow_events()``. Use
    ``last_sequence`` to capture the cursor for resuming later::

        stream = client.get_workflow_events(handler_id)
        async for event in stream:
            print(event.type, stream.last_sequence)

        # Resume from where we left off:
        stream = client.get_workflow_events(
            handler_id, after_sequence=stream.last_sequence
        )
    """

    def __init__(
        self,
        queue: asyncio.Queue[_QueueItem],
        task: asyncio.Task[None] | None,
        initial_sequence: int | Literal["now"],
    ) -> None:
        self._queue = queue
        self._task = task
        self._last_sequence: int | Literal["now"] = initial_sequence
        self._iter_started = False

    @property
    def last_sequence(self) -> int | Literal["now"]:
        """The sequence number of the most recently yielded event, or the
        initial ``after_sequence`` value if no events have been yielded yet."""
        return self._last_sequence

    def __aiter__(self) -> AsyncIterator[EventEnvelopeWithMetadata]:
        if self._iter_started:
            raise RuntimeError("EventStream can only be iterated once")
        self._iter_started = True
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[EventEnvelopeWithMetadata, None]:
        try:
            while True:
                item = await self._queue.get()
                if isinstance(item, _QueuedDone):
                    return
                if isinstance(item, _QueuedError):
                    raise item.error
                self._last_sequence = item.sequence
                yield item.event
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        """Cancel the background reader and release resources."""
        if self._task is None:
            return
        task, self._task = self._task, None
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


class WorkflowClient:
    """Python client for interacting with a ``WorkflowServer``.

    Provides methods for listing workflows, running them synchronously or
    asynchronously, streaming events, and sending events for
    human-in-the-loop workflows.

    Example:

        from llama_agents.client import WorkflowClient
        from workflows.events import StartEvent

        client = WorkflowClient(base_url="http://localhost:8080")

        # Run synchronously
        result = await client.run_workflow("greet", start_event=StartEvent(name="Ada"))
        print(result.result)

        # Run async and stream events
        handler = await client.run_workflow_nowait("greet")
        stream = client.get_workflow_events(handler.handler_id)
        async for event in stream:
            print(event.type, event.value)

    Args:
        base_url: Base URL of the workflow server (e.g. ``"http://localhost:8080"``).
        httpx_client: Pre-configured ``httpx.AsyncClient``. Use this for
            custom auth headers, timeouts, or transport configuration.

    Provide exactly one of ``base_url`` or ``httpx_client``.
    """

    @overload
    def __init__(self, *, httpx_client: httpx.AsyncClient): ...
    @overload
    def __init__(
        self,
        *,
        base_url: str,
    ): ...

    def __init__(
        self,
        *,
        httpx_client: httpx.AsyncClient | None = None,
        base_url: str | None = None,
    ):
        if httpx_client is None and base_url is None:
            raise ValueError("Either httpx_client or base_url must be provided")
        if httpx_client is not None and base_url is not None:
            raise ValueError("Only one of httpx_client or base_url must be provided")
        self.httpx_client = httpx_client
        self.base_url = base_url

    @asynccontextmanager
    async def _get_client(self) -> AsyncIterator[httpx.AsyncClient]:
        if self.httpx_client:
            yield self.httpx_client
        else:
            async with httpx.AsyncClient(base_url=self.base_url or "") as client:
                yield client

    async def is_healthy(self) -> HealthResponse:
        """Check whether the workflow server is healthy.

        Raises:
            httpx.HTTPStatusError: If the server returns an error status.
        """
        async with self._get_client() as client:
            response = await client.get("/health")
            _raise_for_status_with_body(response)
            return HealthResponse.model_validate(response.json())

    async def list_workflows(self) -> WorkflowsListResponse:
        """List the names of all workflows registered on the server."""
        async with self._get_client() as client:
            response = await client.get("/workflows")

            _raise_for_status_with_body(response)

            return WorkflowsListResponse.model_validate(response.json())

    async def run_workflow(
        self,
        workflow_name: str,
        handler_id: str | None = None,
        start_event: StartEvent | dict[str, Any] | None = None,
        context: Context | dict[str, Any] | None = None,
    ) -> HandlerData:
        """Run the workflow and block until completion.

        Args:
            workflow_name: Name of the registered workflow to run.
            start_event: Input event for the workflow. Can be a ``StartEvent``
                instance or a plain dict.
            context: Workflow context to restore, for continuing a previous run.
            handler_id: Handler identifier to continue from a previous
                completed run.

        Returns:
            HandlerData: Handler metadata including the final result.
        """
        if start_event is not None:
            try:
                start_event = _serialize_event(start_event, bare=True)
            except Exception as e:
                raise ValueError(
                    f"Impossible to serialize the start event because of: {e}"
                )
        if isinstance(context, Context):
            try:
                context = context.to_dict()
            except Exception as e:
                raise ValueError(f"Impossible to serialize the context because of: {e}")
        request_body: dict[str, Any] = {
            "start_event": start_event
            if start_event is not None
            else _serialize_event(StartEvent(), bare=True),
            "context": context if context is not None else {},
        }
        if handler_id:
            request_body["handler_id"] = handler_id
        async with self._get_client() as client:
            response = await client.post(
                f"/workflows/{workflow_name}/run", json=request_body
            )

            _raise_for_status_with_body(response)

            return HandlerData.model_validate(response.json())

    async def run_workflow_nowait(
        self,
        workflow_name: str,
        handler_id: str | None = None,
        start_event: StartEvent | dict[str, Any] | None = None,
        context: Context | dict[str, Any] | None = None,
    ) -> HandlerData:
        """Start the workflow without waiting for completion.

        Use the returned ``handler_id`` to stream events, poll for results,
        or send events.

        Args:
            workflow_name: Name of the registered workflow to run.
            start_event: Input event for the workflow. Can be a ``StartEvent``
                instance or a plain dict.
            context: Workflow context to restore, for continuing a previous run.
            handler_id: Handler identifier to continue from a previous
                completed run.

        Returns:
            HandlerData: Handler metadata including the ``handler_id``.
        """
        if start_event is not None:
            try:
                start_event = _serialize_event(start_event)
            except Exception as e:
                raise ValueError(
                    f"Impossible to serialize the start event because of: {e}"
                )
        if isinstance(context, Context):
            try:
                context = context.to_dict()
            except Exception as e:
                raise ValueError(f"Impossible to serialize the context because of: {e}")
        request_body: dict[str, Any] = {
            "start_event": start_event
            if start_event is not None
            else _serialize_event(StartEvent()),
            "context": context if context is not None else {},
        }
        if handler_id:
            request_body["handler_id"] = handler_id
        async with self._get_client() as client:
            response = await client.post(
                f"/workflows/{workflow_name}/run-nowait", json=request_body
            )

            _raise_for_status_with_body(response)

            return HandlerData.model_validate(response.json())

    def get_workflow_events(
        self,
        handler_id: str,
        include_internal_events: bool = False,
        include_children: bool = False,
        after_sequence: int | Literal["now"] = -1,
        max_reconnect_attempts: int = 3,
    ) -> EventStream:
        """Stream events as they are produced by the workflow.

        Returns an ``EventStream`` whose ``last_sequence`` property tracks
        the sequence number of the most recently yielded event. Uses SSE
        and automatically reconnects from the last received event on
        connection drops.

        Example:

            stream = client.get_workflow_events(handler_id)
            async for event in stream:
                print(event.type, stream.last_sequence)

        Args:
            handler_id: ID of the handler running the workflow.
            include_internal_events: Include internal dispatch events.
                Defaults to ``False``.
            include_children: Include events emitted from child workflow namespaces.
                Defaults to ``False``.
            after_sequence: Where to start streaming. ``-1`` (default) streams
                all events from the beginning. ``"now"`` skips existing events
                and only delivers new ones. An integer ``N`` streams events
                after sequence ``N``.
            max_reconnect_attempts: Maximum reconnect attempts on connection
                drop. Defaults to ``3``.
        """
        queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
        stream = EventStream(queue, None, after_sequence)

        async def reader() -> None:
            incl_inter = "true" if include_internal_events else "false"
            incl_children = "true" if include_children else "false"
            url = f"/events/{handler_id}"
            last_sequence: int | Literal["now"] = after_sequence
            attempts = 0
            try:
                while True:
                    async with self._get_client() as client:
                        try:
                            async with client.stream(
                                "GET",
                                url,
                                params={
                                    "sse": "true",
                                    "include_internal": incl_inter,
                                    "include_children": incl_children,
                                    "after_sequence": str(last_sequence),
                                },
                                headers={"Connection": "keep-alive"},
                                timeout=None,
                            ) as response:
                                if response.status_code == 404:
                                    raise ValueError("Handler not found")
                                elif response.status_code == 204:
                                    await queue.put(_QueuedDone())
                                    return

                                _raise_for_status_with_body(response)

                                # Reset attempts on successful connection
                                attempts = 0

                                # Parse SSE stream: "id: N\ndata: {...}\n\n"
                                current_id: str | None = None
                                async for line in response.aiter_lines():
                                    stripped = line.strip()
                                    if not stripped:
                                        # Empty line = end of SSE event
                                        continue
                                    if stripped.startswith("id:"):
                                        current_id = stripped[3:].strip()
                                    elif stripped.startswith("data:"):
                                        data = stripped[5:].strip()
                                        event = EventEnvelopeWithMetadata.model_validate_json(
                                            data
                                        )
                                        if current_id is not None:
                                            try:
                                                last_sequence = int(current_id)
                                            except ValueError:
                                                pass
                                        await queue.put(
                                            _QueuedEvent(
                                                sequence=last_sequence,
                                                event=event,
                                            )
                                        )
                                        current_id = None

                            # Stream ended normally (server closed connection)
                            await queue.put(_QueuedDone())
                            return

                        except httpx.TimeoutException:
                            raise TimeoutError(
                                f"Timeout waiting for events from handler {handler_id}"
                            )
                        except (httpx.RequestError, ConnectionError):
                            attempts += 1
                            if attempts > max_reconnect_attempts:
                                raise ConnectionError(
                                    f"Failed to connect to event stream after {max_reconnect_attempts} attempts"
                                )
                            # Retry from last received sequence
            except asyncio.CancelledError:
                await queue.put(_QueuedDone())
            except BaseException as exc:
                await queue.put(_QueuedError(exc))

        stream._task = asyncio.create_task(reader())
        return stream

    async def send_event(
        self,
        handler_id: str,
        event: Event | dict[str, Any],
        step: str | None = None,
    ) -> SendEventResponse:
        """Send an event to a running workflow.

        Useful for human-in-the-loop workflows that wait for external input.

        Args:
            handler_id: ID of the handler running the workflow.
            event: Event to send, as an ``Event`` instance or a dict.
            step: Target a specific workflow step. When ``None``, the event
                is broadcast to all waiting steps.
        """
        try:
            serialized_event: dict[str, Any] = _serialize_event(event)
        except Exception as e:
            raise ValueError(f"Error while serializing the provided event: {e}")
        request_body: dict[str, Any] = {"event": serialized_event}
        if step:
            request_body["step"] = step
        async with self._get_client() as client:
            response = await client.post(f"/events/{handler_id}", json=request_body)
            _raise_for_status_with_body(response)

            return SendEventResponse.model_validate(response.json())

    async def get_result(self, handler_id: str) -> HandlerData:
        """
        Deprecated. Use get_handler instead.
        """
        return await self.get_handler(handler_id)

    async def get_handlers(
        self,
        status: list[Status] | None = None,
        workflow_name: list[str] | None = None,
    ) -> HandlersListResponse:
        """List all workflow handlers.

        Args:
            status: Filter by handler status (e.g. ``"running"``,
                ``"completed"``).
            workflow_name: Filter by workflow name.
        """
        async with self._get_client() as client:
            response = await client.get(
                "/handlers",
                params={
                    "status": status,
                    "workflow_name": workflow_name,
                },
            )
            _raise_for_status_with_body(response)

            return HandlersListResponse.model_validate(response.json())

    async def get_handler(self, handler_id: str) -> HandlerData:
        """Get a workflow handler by ID.

        Returns handler metadata including status, result (if completed),
        and timestamps.

        Args:
            handler_id: ID of the handler.
        """
        async with self._get_client() as client:
            response = await client.get(f"/handlers/{handler_id}")
            _raise_for_status_with_body(response)

            return HandlerData.model_validate(response.json())

    async def cancel_handler(
        self, handler_id: str, purge: bool = False
    ) -> CancelHandlerResponse:
        """Cancel a running workflow.

        Args:
            handler_id: ID of the handler to cancel.
            purge: Also remove the handler from the persistence store.
                Defaults to ``False``.
        """
        async with self._get_client() as client:
            response = await client.post(
                f"/handlers/{handler_id}/cancel",
                params={"purge": "true" if purge else "false"},
            )
            _raise_for_status_with_body(response)

            return CancelHandlerResponse.model_validate(response.json())


def _serialize_event(
    event: Event | dict[str, Any], bare: bool = False
) -> dict[str, Any]:
    if isinstance(event, dict):
        return event  # assumes you know what you are doing. In many cases this needs to be a dict that contains type metadata and the value
    return (
        event.model_dump()
        if bare
        else EventEnvelope.from_event(event=event).model_dump()
    )
