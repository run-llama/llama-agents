# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Generic,
    TypeVar,
    cast,
)

from llama_index_instrumentation.dispatcher import (
    active_instrument_tags,
    instrument_tags,
)

from workflows.context.external_context import ExternalContext
from workflows.context.internal_context import InternalContext
from workflows.context.pre_context import PreContext
from workflows.errors import (
    ContextSerdeError,
    ContextStateError,
    WorkflowRuntimeError,
)
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
)
from workflows.handler import WorkflowHandler
from workflows.retry_policy import RetryInfo
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
)
from workflows.runtime.types.results import InternalContextVar
from workflows.types import RunResultT
from workflows.utils import _nanoid as nanoid

from .serializers import BaseSerializer, JsonSerializer
from .state_store import MODEL_T, StateStore

if TYPE_CHECKING:  # pragma: no cover
    from workflows import Workflow


T = TypeVar("T", bound=Event)
EventBuffer = dict[str, list[Event]]


# TODO(v3) remove this class, and replace with direct references to the pre/internal/external contexts
class Context(Generic[MODEL_T]):
    """
    Global, per-run context for a `Workflow`. Provides an interface into the
    underlying broker run, for both external (workflow run oberservers) and
    internal consumption by workflow steps.

    The `Context` coordinates event delivery between steps, tracks in-flight work,
    exposes a global state store, and provides utilities for streaming and
    synchronization. It is created by a `Workflow` at run time and can be
    persisted and restored.

    Args:
        workflow (Workflow): The owning workflow instance. Used to infer
            step configuration and instrumentation.
        previous_context: A previous context snapshot to resume from.
        serializer: A serializer to use for serializing and deserializing the current and previous context snapshots.

    Attributes:
        is_running (bool): Whether the workflow is currently running.
        store (StateStore[MODEL_T]): Type-safe, async state store shared
            across steps. See also
            [StateStore][workflows.context.state_store.StateStore].

    Examples:
        Basic usage inside a step:

        ```python
        from workflows import step
        from workflows.events import StartEvent, StopEvent

        @step
        async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
            await ctx.store.set("query", ev.topic)
            ctx.write_event_to_stream(ev)  # surface progress to UI
            return StopEvent(result="ok")
        ```

        Persisting the state of a workflow across runs:

        ```python
        from workflows import Context

        # Create a context and run the workflow with the same context
        ctx = Context(my_workflow)
        result_1 = await my_workflow.run(..., ctx=ctx)
        result_2 = await my_workflow.run(..., ctx=ctx)

        # Serialize the context and restore it
        ctx_dict = ctx.to_dict()
        restored_ctx = Context.from_dict(my_workflow, ctx_dict)
        result_3 = await my_workflow.run(..., ctx=restored_ctx)
        ```


    See Also:
        - [Workflow][workflows.Workflow]
        - [Event][workflows.events.Event]
        - [InMemoryStateStore][workflows.context.state_store.InMemoryStateStore]
    """

    # Current face - context is in exactly one state at a time
    _face: (
        PreContext[MODEL_T] | ExternalContext[MODEL_T, Any] | InternalContext[MODEL_T]
    )

    def __init__(
        self,
        workflow: Workflow,
        previous_context: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> None:
        # Start in pre-run (config) state - PreContext handles deserialization
        pre_context: PreContext[MODEL_T] = PreContext(
            workflow=workflow,
            previous_context=previous_context,
            serializer=serializer,
        )
        self._face = pre_context

    @classmethod
    def _create_face(
        cls,
        face: PreContext[MODEL_T]
        | ExternalContext[MODEL_T, Any]
        | InternalContext[MODEL_T],
    ) -> Context[MODEL_T]:
        new_ctx = cast(Context[MODEL_T], object.__new__(cls))
        new_ctx._face = face
        return new_ctx

    @staticmethod
    def get_step_context() -> Context:
        """Return the `Context` for the currently executing step.

        This is useful for decorators or wrappers around step functions that
        need access to the step context without requiring the user-defined
        step to declare a ``ctx: Context`` parameter.

        Returns:
            Context: The context instance (in internal-face state) for the
            running step.

        Raises:
            WorkflowRuntimeError: If called outside of a step function.

        Examples:
            ```python
            from workflows import Context

            # Inside a decorator that wraps a step function
            ctx = Context.get_step_context()
            ctx.send_event(ProgressEvent(msg="step starting"))
            ```
        """
        try:
            ref = InternalContextVar.get()
        except LookupError:
            raise WorkflowRuntimeError(
                "Context.get_step_context() may only be called from within a step function"
            )
        ctx = ref()
        if ctx is None:
            raise WorkflowRuntimeError(
                "Context.get_step_context() may only be called from within a step function"
            )
        return ctx

    @property
    def is_running(self) -> bool:
        """Whether the workflow is currently running."""
        if isinstance(self._face, PreContext):
            return self._face.is_running
        elif isinstance(self._face, ExternalContext):
            return self._face.is_running
        else:
            _warn_is_running_in_step()
            return True

    def _require_pre(self, fn: str) -> PreContext[MODEL_T]:
        """Require context to be in pre-run state. Raises ContextStateError if not."""
        if isinstance(self._face, PreContext):
            return self._face  # type: ignore[ty:invalid-return-type]
        raise ContextStateError(
            f"{fn} requires a pre-run context. The workflow has already started."
        )

    def _require_external(self, fn: str) -> ExternalContext[MODEL_T, Any]:
        """Require context to be in external state. Raises ContextStateError if not."""
        if isinstance(self._face, ExternalContext):
            return self._face
        if isinstance(self._face, PreContext):
            raise ContextStateError(
                f"{fn} requires a running workflow. Call workflow.run() first."
            )
        raise ContextStateError(
            f"{fn} is only available from handler code, not from within steps."
        )

    def _require_internal(self, fn: str) -> InternalContext[MODEL_T]:
        """Require context to be in internal state. Raises ContextStateError if not."""
        if isinstance(self._face, InternalContext):
            return self._face  # type: ignore[ty:invalid-return-type]
        if isinstance(self._face, PreContext):
            raise ContextStateError(
                f"{fn} requires a running workflow. Call workflow.run() first."
            )
        raise ContextStateError(f"{fn} is only available from within step functions.")

    @classmethod
    def _create_internal(
        cls,
        workflow: Workflow,
    ) -> Context[MODEL_T]:
        """Create a Context directly in internal face state.

        Requires a current run context (via with_current_run_id) to be set.
        """
        internal_adapter = workflow._runtime.get_internal_adapter(workflow)
        new_ctx = cast(Context[MODEL_T], object.__new__(cls))
        new_ctx._face = cast(
            InternalContext[MODEL_T],
            InternalContext(
                internal_adapter=internal_adapter,
                workflow=workflow,
            ),
        )
        return new_ctx

    @classmethod
    def _create_external(
        cls,
        workflow: Workflow,
        external_adapter: ExternalRunAdapter,
        serializer: BaseSerializer = JsonSerializer(),
    ) -> Context[MODEL_T]:
        """Create a Context directly in external face state with a broker."""

        new_ctx = cast(Context[MODEL_T], object.__new__(cls))

        # Set external face
        new_ctx._face = cast(
            ExternalContext[MODEL_T, Any],
            ExternalContext(
                workflow=workflow,
                external_adapter=external_adapter,
                serializer=serializer,
            ),
        )
        return new_ctx

    def _workflow_run(
        self,
        workflow: Workflow,
        start_event: StartEvent
        | None,  # None only when resuming a workflow from a snapshotted context
        run_id: str | None = None,
    ) -> WorkflowHandler:
        """
        called by package internally from the workflow to run it
        """
        run_id = run_id or nanoid()
        with instrument_tags(
            {**active_instrument_tags.get(), "llamaindex.run_id": run_id}
        ):
            # Get or create PreContext for initialization
            if isinstance(self._face, PreContext):
                pre = self._face
            elif isinstance(self._face, ExternalContext):
                # Check for concurrent run
                if self._face.is_running:
                    raise ContextStateError(
                        "Cannot start a new run while context is already running. "
                        "Wait for completion or use a new Context."
                    )
                # Continuation: create fresh PreContext from current state
                pre = PreContext(
                    workflow=workflow,
                    previous_context=self._face.to_dict(),
                    serializer=self._face._serializer,
                )
            else:
                raise ContextStateError(
                    "Cannot start workflow from a step function context"
                )

            # Compute state from serialized snapshot
            init_state = BrokerState.from_serialized(
                pre.init_snapshot, workflow, pre._serializer
            )

            # TODO(v3) - make this async
            external_adapter = workflow._runtime.run_workflow(
                run_id=run_id,
                workflow=workflow,
                init_state=init_state,
                start_event=start_event,
                serialized_state=pre.serialized_state,
                serializer=pre.serializer,
            )

            # TODO(v3): Remove mutation. Handler will just be the external face.
            self._face = cast(
                ExternalContext[MODEL_T, Any],
                ExternalContext(
                    workflow=workflow,
                    external_adapter=external_adapter,
                    serializer=pre._serializer,
                ),
            )

            return WorkflowHandler(
                workflow=workflow,
                external_adapter=external_adapter,
                ctx=self,
            )

    def _workflow_cancel_run(self) -> None:
        """Called internally from the handler to cancel a context's run."""
        if isinstance(self._face, ExternalContext):
            self._face.cancel()
        elif isinstance(self._face, PreContext):
            _warn_cancel_before_start()
        else:
            _warn_cancel_in_step()

    @property
    def store(self) -> StateStore[MODEL_T]:
        """Typed, process-local state store shared across steps.

        If no state was initialized yet, a default
        [DictState][workflows.context.state_store.DictState] store is created.

        Returns:
            StateStore[MODEL_T]: The state store instance.
        """
        return self._face.store

    def to_dict(self, serializer: BaseSerializer | None = None) -> dict[str, Any]:
        """Serialize the context to a JSON-serializable dict.

        Persists the global state store, event queues, buffers, accepted events,
        broker log, and running flag. This payload can be fed to
        [from_dict][workflows.context.context.Context.from_dict] to resume a run
        or carry state across runs.

        Args:
            serializer (BaseSerializer | None): Value serializer used for state
                and event payloads. Defaults to
                [JsonSerializer][workflows.context.serializers.JsonSerializer].

        Returns:
            dict[str, Any]: A dict suitable for JSON encoding and later
            restoration via `from_dict`.

        See Also:
            - [InMemoryStateStore.to_dict][workflows.context.state_store.InMemoryStateStore.to_dict]

        Examples:
            ```python
            ctx_dict = ctx.to_dict()
            my_db.set("key", json.dumps(ctx_dict))

            ctx_dict = my_db.get("key")
            restored_ctx = Context.from_dict(my_workflow, json.loads(ctx_dict))
            result = await my_workflow.run(..., ctx=restored_ctx)
            ```
        """
        return self._require_external(fn="to_dict").to_dict(serializer)

    @classmethod
    def from_dict(
        cls,
        workflow: Workflow,
        data: dict[str, Any],
        serializer: BaseSerializer | None = None,
    ) -> Context[MODEL_T]:
        """Reconstruct a `Context` from a serialized payload.

        Args:
            workflow (Workflow): The workflow instance that will own this
                context.
            data (dict[str, Any]): Payload produced by
                [to_dict][workflows.context.context.Context.to_dict].
            serializer (BaseSerializer | None): Serializer used to decode state
                and events. Defaults to JSON.

        Returns:
            Context[MODEL_T]: A context instance initialized with the persisted
                state and queues.

        Raises:
            ContextSerdeError: If the payload is missing required fields or is
                in an incompatible format.

        Examples:
            ```python
            ctx_dict = ctx.to_dict()
            my_db.set("key", json.dumps(ctx_dict))

            ctx_dict = my_db.get("key")
            restored_ctx = Context.from_dict(my_workflow, json.loads(ctx_dict))
            result = await my_workflow.run(..., ctx=restored_ctx)
            ```
        """
        try:
            return cls(workflow, previous_context=data, serializer=serializer)
        except KeyError as e:
            msg = "Error creating a Context instance: the provided payload has a wrong or old format."
            raise ContextSerdeError(msg) from e

    async def running_steps(self) -> list[str]:
        """Return the list of currently running step names.

        Returns:
            list[str]: Names of steps that have at least one active worker.
        """
        return await self._require_external(fn="running_steps").running_steps()

    def collect_events(
        self, ev: Event, expected: list[type[Event]], buffer_id: str | None = None
    ) -> list[Event] | None:
        """
        Buffer events until all expected types are available, then return them.

        This utility is helpful when a step can receive multiple event types
        and needs to proceed only when it has a full set. The returned list is
        ordered according to `expected`.

        Args:
            ev (Event): The incoming event to add to the buffer.
            expected (list[Type[Event]]): Event types to collect, in order.
            buffer_id (str | None): Optional stable key to isolate buffers across
                steps or workers. Defaults to an internal key derived from the
                task name or expected types.

        Returns:
            list[Event] | None: The events in the requested order when complete,
            otherwise `None`.

        Examples:
            ```python
            @step
            async def synthesize(
                self, ctx: Context, ev: QueryEvent | RetrieveEvent
            ) -> StopEvent | None:
                events = ctx.collect_events(ev, [QueryEvent, RetrieveEvent])
                if events is None:
                    return None
                query_ev, retrieve_ev = events
                # ... proceed with both inputs present ...
            ```

        See Also:
            - [Event][workflows.events.Event]
        """
        return self._require_internal(fn="collect_events").collect_events(
            ev, expected, buffer_id
        )

    def send_event(self, message: Event, step: str | None = None) -> None:
        """Dispatch an event to one or all workflow steps.

        If `step` is omitted, the event is broadcast to all step queues and
        non-matching steps will ignore it. When `step` is provided, the target
        step must accept the event type or a
        [WorkflowRuntimeError][workflows.errors.WorkflowRuntimeError] is raised.

        Args:
            message (Event): The event to enqueue.
            step (str | None): Optional step name to target.

        Raises:
            WorkflowRuntimeError: If the target step does not exist or does not
                accept the event type.

        Examples:
            It's common to use this method to fan-out events:

            ```python
            @step
            async def my_step(self, ctx: Context, ev: StartEvent) -> WorkerEvent | GatherEvent:
                for i in range(10):
                    ctx.send_event(WorkerEvent(msg=i))
                return GatherEvent()
            ```

            You also see this method used from the caller side to send events into the workflow:

            ```python
            handler = my_workflow.run(...)
            async for ev in handler.stream_events():
                if isinstance(ev, SomeEvent):
                    handler.ctx.send_event(SomeOtherEvent(msg="Hello!"))

            result = await handler
            ```
        """
        # send_event can be called from internal (steps) or external (handler) contexts
        if isinstance(self._face, InternalContext):
            self._face.send_event(message, step)
        elif isinstance(self._face, ExternalContext):
            self._face.send_event(message, step)
        else:
            raise ContextStateError(
                "send_event() called before workflow started. "
                "Call workflow.run() first."
            )

    async def wait_for_event(
        self,
        event_type: type[T],
        waiter_event: Event | None = None,
        waiter_id: str | None = None,
        requirements: dict[str, Any] | None = None,
        timeout: float | None = 2000,
    ) -> T:
        """Wait for the next matching event of type `event_type`.

        The runtime pauses by throwing an internal control-flow exception and replays
        the entire step when the event arrives, so keep this call near the top of the
        step and make any preceding work safe to repeat.

        Optionally emits a `waiter_event` to the event stream once per `waiter_id` to
        inform callers that the workflow is waiting for external input.
        This helps to prevent duplicate waiter events from being sent to the event stream.

        Args:
            event_type (type[T]): Concrete event class to wait for.
            waiter_event (Event | None): Optional event to write to the stream
                once when the wait begins.
            waiter_id (str | None): Stable identifier to avoid emitting multiple
                waiter events for the same logical wait.
            requirements (dict[str, Any] | None): Key/value filters that must be
                satisfied by the event via `event.get(key) == value`.
            timeout (float | None): Max seconds to wait. `None` means no
                timeout. Defaults to 2000 seconds.

        Returns:
            T: The received event instance of the requested type.

        Raises:
            asyncio.TimeoutError: If the timeout elapses.

        Examples:
            ```python
            @step
            async def my_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
                response = await ctx.wait_for_event(
                    HumanResponseEvent,
                    waiter_event=InputRequiredEvent(msg="What's your name?"),
                    waiter_id="user_name",
                    timeout=60,
                )
                return StopEvent(result=response.response)
            ```
        """
        return await self._require_internal(fn="wait_for_event").wait_for_event(
            event_type, waiter_event, waiter_id, requirements, timeout
        )

    def retry_info(self) -> RetryInfo:
        """Return a snapshot of the currently-executing step's retry state.

        Returns:
            RetryInfo: 0-based retry number (0 on first run, 1 on first retry),
            seconds since the first attempt, the most recent prior exception
            (or `None`), and the timezone-aware UTC datetime of that failure
            (or `None`).

        Raises:
            WorkflowRuntimeError: If called outside of a step function.

        Examples:
            ```python
            @step(retry_policy=ConstantDelay(maximum_attempts=3, delay=0))
            async def flaky(self, ctx: Context, ev: StartEvent) -> StopEvent:
                info = ctx.retry_info()
                if info.last_exception is not None:
                    logger.info(
                        "retry %d: %s",
                        info.retry_number,
                        str(info.last_exception),
                    )
                ...
            ```
        """
        return self._require_internal(fn="retry_info").retry_info()

    def write_event_to_stream(self, ev: Event | None) -> None:
        """Enqueue an event for streaming to [WorkflowHandler]](workflows.handler.WorkflowHandler).

        Args:
            ev (Event | None): The event to stream. `None` can be used as a
                sentinel in some streaming modes.

        Examples:
            ```python
            @step
            async def my_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
                ctx.write_event_to_stream(ev)
                return StopEvent(result="ok")
            ```
        """
        self._require_internal(fn="write_event_to_stream").write_event_to_stream(ev)

    async def _finalize_step(self) -> None:
        """Finalize step execution by awaiting background tasks.

        Called after a step function completes to ensure all fire-and-forget
        operations (e.g., write_event_to_stream, send_event) complete before
        returning control to the control loop.
        """
        await self._require_internal(fn="_finalize_step")._finalize_step()

    def get_result(self) -> RunResultT:
        """Return the final result of the workflow run.

        Deprecated:
            This method is deprecated and will be removed in a future release.
            Prefer awaiting the handler returned by `Workflow.run`, e.g.:
            `result = await workflow.run(..., ctx=ctx)`.

        Examples:
            ```python
            # Preferred
            result = await my_workflow.run(..., ctx=ctx)

            # Deprecated
            result_agent = ctx.get_result()
            ```

        Returns:
            RunResultT: The value provided via a `StopEvent`.

        Raises:
            ContextStateError: If called before the workflow is running or
                from within a step function.
        """
        _warn_get_result()
        stop_event = self._require_external(fn="get_result").get_result()
        return stop_event.result if type(stop_event) is StopEvent else stop_event

    def stream_events(self) -> AsyncGenerator[Event, None]:
        """Stream events published by the workflow.

        Returns an async generator that yields events as they are published
        by steps via `write_event_to_stream()`.

        Returns:
            AsyncGenerator[Event, None]: Stream of published events.

        Raises:
            ContextStateError: If called before the workflow is running or
                from within a step function.
        """
        return self._require_external(fn="stream_events").stream_events()

    @property
    def streaming_queue(self) -> asyncio.Queue:
        """Deprecated queue-based event stream.

        Returns an asyncio.Queue that is populated by iterating this context's
        stream_events(). A deprecation warning is emitted once per process.
        """
        _warn_streaming_queue()
        self._require_external(fn="streaming_queue")
        q: asyncio.Queue[Event] = asyncio.Queue()

        async def _pump() -> None:
            async for ev in self.stream_events():
                await q.put(ev)
                if isinstance(ev, StopEvent):
                    break

        try:
            asyncio.create_task(_pump())
        except RuntimeError:
            loop = asyncio.get_event_loop()
            loop.create_task(_pump())
        return q


@functools.lru_cache(maxsize=1)
def _warn_get_result() -> None:
    warnings.warn(
        (
            "Context.get_result() is deprecated and will be removed in a future "
            "release. Prefer awaiting the WorkflowHandler returned by "
            "Workflow.run: `result = await workflow.run(..., ctx=ctx)`."
        ),
        DeprecationWarning,
        stacklevel=2,
    )


@functools.lru_cache(maxsize=1)
def _warn_streaming_queue() -> None:
    warnings.warn(
        (
            "Context.streaming_queue is deprecated and will be removed in a future "
            "release. Prefer iterating Context.stream_events(): "
            "`async for ev in ctx.stream_events(): ...`"
        ),
        DeprecationWarning,
        stacklevel=2,
    )


@functools.lru_cache(maxsize=1)
def _warn_is_running_in_step() -> None:
    warnings.warn(
        "is_running called from within a step; the workflow is always "
        "running inside a step. This usage is deprecated.",
        DeprecationWarning,
        stacklevel=3,
    )


@functools.lru_cache(maxsize=1)
def _warn_cancel_before_start() -> None:
    warnings.warn(
        "cancel() called before workflow started; nothing to cancel.",
        stacklevel=3,
    )


@functools.lru_cache(maxsize=1)
def _warn_cancel_in_step() -> None:
    warnings.warn(
        "cancel() called from within a step; use send_event() instead.",
        stacklevel=3,
    )
