# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from _collections_abc import dict_items, dict_keys, dict_values
from datetime import datetime
from enum import Enum
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    PlainValidator,
    PrivateAttr,
    model_serializer,
)

from workflows.context.serializers import JsonSerializer
from workflows.context.utils import import_module_from_qualified_name


class DictLikeModel(BaseModel):
    """
    Base Pydantic model class that mimics a dict interface for dynamic fields.

    Known, typed fields behave like regular Pydantic attributes. Any extra
    keyword arguments are stored in an internal dict and can be accessed through
    both attribute and mapping semantics. This hybrid model enables flexible
    event payloads while preserving validation for declared fields.

    PrivateAttr:
        _data (dict[str, Any]): Underlying Python dict for dynamic fields.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    _data: dict[str, Any] = PrivateAttr(default_factory=dict)

    def __init__(self, **params: Any):
        """
        __init__.

        NOTE: fields and private_attrs are pulled from params by name.
        """
        # extract and set fields, private attrs and remaining shove in _data
        fields = {}
        private_attrs = {}
        data = {}
        for k, v in params.items():
            if k in self.__class__.model_fields:
                fields[k] = v
            elif k in self.__private_attributes__:
                private_attrs[k] = v
            else:
                data[k] = v
        super().__init__(**fields)
        for private_attr, value in private_attrs.items():
            super().__setattr__(private_attr, value)
        if data:
            self._data.update(data)

    def __getattr__(self, __name: str) -> Any:
        if (
            __name in self.__private_attributes__
            or __name in self.__class__.model_fields
        ):
            return super().__getattr__(__name)  # type: ignore
        else:
            if __name not in self._data:
                raise AttributeError(
                    f"'{self.__class__.__name__}' object has no attribute '{__name}'"
                )
            return self._data[__name]

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__private_attributes__ or name in self.__class__.model_fields:
            super().__setattr__(name, value)
        else:
            self._data.__setitem__(name, value)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def keys(self) -> "dict_keys[str, Any]":
        return self._data.keys()

    def values(self) -> "dict_values[str, Any]":
        return self._data.values()

    def items(self) -> "dict_items[str, Any]":
        return self._data.items()

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Any:
        return iter(self._data)

    def to_dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self._data

    def __bool__(self) -> bool:
        """Make test `if event:` pass on Event instances."""
        return True

    @model_serializer(mode="wrap")
    def custom_model_dump(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        # include _data in serialization
        if self._data:
            data["_data"] = self._data
        return data


class Event(DictLikeModel):
    """
    Base class for all workflow events.

    Events are light-weight, serializable payloads passed between steps.
    They support both attribute and mapping access to dynamic fields.

    Examples:
        Subclassing with typed fields:

        ```python
        from pydantic import Field

        class CustomEv(Event):
            score: int = Field(ge=0)

        e = CustomEv(score=10)
        print(e.score)
        ```

    See Also:
        - [StartEvent][workflows.events.StartEvent]
        - [StopEvent][workflows.events.StopEvent]
        - [InputRequiredEvent][workflows.events.InputRequiredEvent]
        - [HumanResponseEvent][workflows.events.HumanResponseEvent]
    """

    def __init__(self, **params: Any):
        super().__init__(**params)


_json_serializer = JsonSerializer()


def _serialize_event(event: Event) -> Any:
    return _json_serializer.serialize_value(event)


def _deserialize_event(data: Any) -> Event:
    return _json_serializer.deserialize_value(data)


SerializableEvent = Annotated[
    Event,
    PlainSerializer(_serialize_event, return_type=Any),
    PlainValidator(_deserialize_event),
]


def _serialize_optional_event(event: Event | None) -> Any:
    if event is None:
        return None
    return _json_serializer.serialize_value(event)


def _deserialize_optional_event(data: Any) -> Event | None:
    if data is None:
        return None
    return _json_serializer.deserialize_value(data)


SerializableOptionalEvent = Annotated[
    Event | None,
    PlainSerializer(_serialize_optional_event, return_type=Any),
    PlainValidator(_deserialize_optional_event),
]


def _serialize_exception(exc: Exception) -> dict[str, Any]:
    exc_type = type(exc)
    qualified_name = f"{exc_type.__module__}.{exc_type.__qualname__}"
    return {
        "exception_type": qualified_name,
        "exception_message": str(exc),
    }


def _deserialize_exception(data: Any) -> Exception:
    if isinstance(data, Exception):
        return data
    exc_message = data["exception_message"]
    try:
        exc_cls = import_module_from_qualified_name(data["exception_type"])
        return exc_cls(exc_message)
    except (ImportError, AttributeError, ValueError):
        return Exception(exc_message)


SerializableException = Annotated[
    Exception,
    PlainSerializer(_serialize_exception, return_type=dict[str, Any]),
    PlainValidator(_deserialize_exception),
]


def _serialize_optional_exception(exc: Exception | None) -> Any:
    if exc is None:
        return None
    return _serialize_exception(exc)


def _deserialize_optional_exception(data: Any) -> Exception | None:
    if data is None:
        return None
    return _deserialize_exception(data)


SerializableOptionalException = Annotated[
    Exception | None,
    PlainSerializer(_serialize_optional_exception, return_type=Any),
    PlainValidator(_deserialize_optional_exception),
]


def _serialize_event_type(event_type: type[Event]) -> str:
    return f"{event_type.__module__}.{event_type.__qualname__}"


def _deserialize_event_type(data: Any) -> type[Event]:
    if isinstance(data, type):
        return data
    return import_module_from_qualified_name(data)


SerializableEventType = Annotated[
    type[Event],
    PlainSerializer(_serialize_event_type, return_type=str),
    PlainValidator(_deserialize_event_type),
]


class StartEvent(Event):
    """Implicit entry event sent to kick off a `Workflow.run()`."""


class StopEvent(Event):
    """Terminal event that signals the workflow has completed.

    The `result` property contains the return value of the workflow run. When a
    custom stop event subclass is used, the workflow result is that event
    instance itself.

    Examples:
        ```python
        # default stop event: result holds the value
        return StopEvent(result={"answer": 42})
        ```

        Subclassing to provide a custom result:

        ```python
        class MyStopEv(StopEvent):
            pass

        @step
        async def my_step(self, ctx: Context, ev: StartEvent) -> MyStopEv:
            return MyStopEv(result={"answer": 42})
    """

    _result: Any = PrivateAttr(default=None)

    def __init__(self, result: Any = None, **kwargs: Any) -> None:
        # forces the user to provide a result
        super().__init__(_result=result, **kwargs)

    def _get_result(self) -> Any:
        """This can be overridden by subclasses to return the desired result."""
        return self._result

    @property
    def result(self) -> Any:
        return self._get_result()

    @model_serializer(mode="wrap")
    def custom_model_dump(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        # include _result in serialization for base StopEvent
        if self._result is not None:
            data["result"] = self._result
        return data

    def __repr__(self) -> str:
        dict_items = {**self._data, **self.model_dump()}
        # Format as key=value pairs
        parts = [f"{k}={v!r}" for k, v in dict_items.items()]
        dict_str = ", ".join(parts)
        return f"{self.__class__.__name__}({dict_str})"

    def __str__(self) -> str:
        return str(self._result)


class WorkflowTimedOutEvent(StopEvent):
    """Published when a workflow exceeds its configured timeout.

    This event is published to the event stream when a workflow times out,
    allowing consumers to understand why the workflow ended before the
    WorkflowTimeoutError exception is raised.

    Attributes:
        timeout: The timeout duration in seconds that was exceeded.
        active_steps: List of step names that were still active when the timeout occurred.

    Examples:
        ```python
        async for event in handler.stream_events():
            if isinstance(event, WorkflowTimedOutEvent):
                print(f"Workflow timed out after {event.timeout}s")
                print(f"Active steps: {event.active_steps}")
        ```
    """

    timeout: float
    active_steps: list[str]


class WorkflowCancelledEvent(StopEvent):
    """Published when a workflow is cancelled by the user.

    This event is published to the event stream when a workflow is cancelled
    via the handler or programmatically, allowing consumers to understand why
    the workflow ended before the WorkflowCancelledByUser exception is raised.

    Examples:
        ```python
        async for event in handler.stream_events():
            if isinstance(event, WorkflowCancelledEvent):
                print("Workflow was cancelled by user")
        ```
    """


class IdleReleasedEvent(StopEvent):
    """Sentinel returned when a workflow is cleanly released due to idleness.

    Unlike WorkflowCancelledEvent, this does not publish to the event stream
    and does not raise an exception — the control loop simply returns this
    event as the workflow result.
    """


class WorkflowFailedEvent(StopEvent):
    """Published when a workflow step fails permanently.

    Published when a step fails and all retries are exhausted (or no retry
    policy permits a retry, or a catch_error handler itself raised).

    Attributes:
        step_name: The name of the step that failed.
        exception: The raised exception. ``__traceback__`` is present only
            in-process; ``None`` after a replay.
        attempts: The total number of attempts made before giving up.
        elapsed_seconds: Time in seconds from first attempt to final failure.

    Examples:
        ```python
        async for event in handler.stream_events():
            if isinstance(event, WorkflowFailedEvent):
                print(f"Step '{event.step_name}' failed after {event.attempts} attempts")
                print(f"{type(event.exception).__name__}: {event.exception}")
        ```
    """

    step_name: str
    exception: SerializableException
    attempts: int
    elapsed_seconds: float


class CollectionReleaseEvent(Event):
    """Internal: invokes a collect step with one released batch.

    This is the trigger event of a collect-step invocation work item — never a
    member of the stream itself. The authoritative record of the batch is the
    work item's collection release payload; the runtime derives this event from
    that payload at every (re)delivery so the two cannot diverge.

    Surfaces publicly as `StepFailedEvent.input_event` when a collect step
    fails into a `@catch_error` handler: ``events`` is the real released batch
    (possibly empty for an empty stream).
    """

    events: list[SerializableEvent] = Field(default_factory=list)
    stream_id: str = ""
    binding_id: str = ""


class StepFailedEvent(Event):
    """Delivered to a `@catch_error` handler when a step exhausts its retries.

    The handler may inspect the fields to decide how to recover. Returning a
    `StopEvent` completes the workflow successfully; raising from the handler
    propagates the new exception and fails the workflow.

    Attributes:
        step_name: The name of the step that failed.
        input_event: The triggering event instance that caused the failure.
        exception: The raised exception. ``__traceback__`` is present in-process
            but ``None`` after the event has crossed a serialization boundary
            (e.g., a replay).
        attempts: Total number of attempts made before giving up.
        elapsed_seconds: Seconds from first attempt to final failure.
        failed_at: Timezone-aware UTC datetime of the final failure.
    """

    step_name: str
    input_event: SerializableEvent
    exception: SerializableException
    attempts: int
    elapsed_seconds: float
    failed_at: datetime


class InputRequiredEvent(Event):
    """Emitted when human input is required to proceed.

    Automatically written to the event stream if returned from a step.

    If returned from a step, it does not need to be consumed by other steps and will pass validation.
    It's expected that the caller will respond to this event and send back a [HumanResponseEvent][workflows.events.HumanResponseEvent].

    Use this directly or subclass it.

    Typical flow: a step returns `InputRequiredEvent`, callers consume it from
    the stream and send back a [HumanResponseEvent][workflows.events.HumanResponseEvent].

    Examples:
        ```python
        from workflows.events import InputRequiredEvent, HumanResponseEvent

        class HITLWorkflow(Workflow):
            @step
            async def my_step(self, ev: StartEvent) -> InputRequiredEvent:
                return InputRequiredEvent(prefix="What's your name? ")

            @step
            async def my_step(self, ev: HumanResponseEvent) -> StopEvent:
                return StopEvent(result=ev.response)
        ```
    """


class HumanResponseEvent(Event):
    """Carries a human's response for a prior input request.

    If consumed by a step and not returned by another, it will still pass validation.

    Examples:
        ```python
        from workflows.events import InputRequiredEvent, HumanResponseEvent

        class HITLWorkflow(Workflow):
            @step
            async def my_step(self, ev: StartEvent) -> InputRequiredEvent:
                return InputRequiredEvent(prefix="What's your name? ")

            @step
            async def my_step(self, ev: HumanResponseEvent) -> StopEvent:
                return StopEvent(result=ev.response)
        ```
    """


class InternalDispatchEvent(Event):
    """
    InternalDispatchEvent is a special event type that exposes processes running inside workflow, even if the user did not explicitly expose them by setting, e.g., `ctx.write_event_to_stream(`.

    Examples:
        ```python
        wf = ExampleWorkflow()
        handler = wf.run(message="Hello, who are you?")

        async for ev in handler.stream_event(expose_internal=True):
            if isinstance(ev, InternalDispatchEvent):
                print(type(ev), ev)
        ```
    """

    pass


class WorkflowIdleEvent(InternalDispatchEvent):
    """Emitted when workflow transitions to idle (waiting on external input).

    A workflow is idle when:
    1. The workflow is running (hasn't completed/failed/cancelled)
    2. All steps have no pending events in their queues
    3. All steps have no workers currently executing
    4. At least one step has an active waiter (from ctx.wait_for_event())

    This event is intentionally minimal - no metadata beyond the event type.
    Resumption from idle is signaled by StepStateChanged with StepState.RUNNING.
    """

    pass


class UnhandledEvent(InternalDispatchEvent):
    """Emitted when an incoming event is not handled by any step or waiter.

    This helps callers understand when an external event is ignored and whether
    the workflow is idle after processing the event.
    """

    event_type: str = Field(description="Class name of the unhandled event.")
    qualified_name: str = Field(description="Fully qualified name of the event type.")
    step_name: str | None = Field(
        default=None,
        description="Target step name if the event was addressed to a step.",
    )
    idle: bool = Field(description="Whether the workflow is idle after processing.")


class StepState(Enum):
    # is enqueued, but no capacity yet available to run
    PREPARING = "preparing"
    # is running now on a worker. Skips PREPARING if there is capacity available.
    RUNNING = "running"
    # is no longer running.
    NOT_RUNNING = "not_running"


class StepStateChanged(InternalDispatchEvent):
    """
    StepStateChanged is a special event type that exposes internal changes in the state of the event, including whether the step is running or in progress, what worker it is running on and what events it takes as input and output, as well as changes in the workflow state.

    Attributes:
        name (str): Name of the step
        step_state (StepState): State of the step ("running", "not_running", "in_progress", "not_in_progress", "exited")
        worker_id (str): ID of the worker that the step is running on
        input_event_name (str): Name of the input event
        output_event_name (Optional[str]): Name of the output event
        context_state (dict[str, Any]): Snapshot of the current workflow state
    """

    name: str = Field(description="Name of the step")
    step_state: StepState = Field(
        description="State of the step ('running', 'not_running', 'in_progress', 'not_in_progress', 'exited')"
    )
    worker_id: str = Field(description="ID of the worker that the step is running on")
    input_event_name: str = Field(description="Name of the input event")
    output_event_name: str | None = Field(
        description="Name of the output event", default=None
    )


EventType = type[Event]
