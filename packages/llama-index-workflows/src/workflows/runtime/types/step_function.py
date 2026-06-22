# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import inspect
import time
import uuid
import weakref
from contextvars import copy_context
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, TypeVar

from llama_index_instrumentation import get_dispatcher
from llama_index_instrumentation.base import BaseEvent
from llama_index_instrumentation.dispatcher import (
    active_instrument_tags,
    instrument_tags,
)
from llama_index_instrumentation.events.span import SpanDropEvent
from llama_index_instrumentation.span import active_span_id
from workflows._event_summary import summarize_event
from workflows.decorators import P, StepConfig
from workflows.errors import WorkflowCancelledByUser, WorkflowRuntimeError
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
)
from workflows.runtime.control_loop import control_loop
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ControlLoopFunction,
    RunContext,
    WorkflowRunFunction,
    run_context,
)
from workflows.runtime.types.results import (
    InternalContextVar,
    RetryAttempt,
    Returns,
    StepFunctionResult,
    StepWorkerContext,
    StepWorkerFailed,
    StepWorkerResult,
    StepWorkerState,
    StepWorkerStateContextVar,
    WaitingForEvent,
)
from workflows.workflow import Workflow

if TYPE_CHECKING:
    from workflows.context.context import Context

_dispatcher = get_dispatcher(__name__)

StepReturnT = TypeVar("StepReturnT", bound=Event | None)


class SpanCancelledEvent(BaseEvent):
    """Instrumentation event emitted when a span exits due to cancellation."""

    reason: str

    @classmethod
    def class_name(cls) -> str:
        return "SpanCancelledEvent"


class WorkflowStepOutputEvent(BaseEvent):
    """Instrumentation event emitted with output summary when a step returns."""

    output: str

    @classmethod
    def class_name(cls) -> str:
        return "step.output"


class WorkflowRunOutputEvent(BaseEvent):
    """Instrumentation event emitted with output summary when a workflow run completes."""

    output: str

    @classmethod
    def class_name(cls) -> str:
        return "workflow.output"


def _emit_output_event(event: BaseEvent) -> None:
    """Fire an instrumentation event, silently ignoring failures."""
    try:
        _dispatcher.event(event)
    except Exception:
        pass


def _run_with_tags(tags: dict[str, Any], func: Callable[[], Any]) -> Any:
    """Run a callable inside an instrument_tags context (for sync/executor use)."""
    with instrument_tags(tags):
        return func()


class StepWorkerFunction(Protocol):
    def __call__(
        self,
        state: StepWorkerState,
        step_name: str,
        event: Event,
        workflow: Workflow,
        bound_events: dict[str, Event] | None = None,
        retry: RetryAttempt = RetryAttempt(),
    ) -> Awaitable[list[StepFunctionResult]]: ...


async def partial(
    func: Callable[..., Any],
    step_config: StepConfig,
    event: Event,
    context: Context,
    workflow: Workflow,
    collected_events: dict[str, Event] | None = None,
    collection_events: dict[str, list[Event]] | None = None,
) -> Callable[[], Any]:
    kwargs: dict[str, Any] = {}
    if collection_events is not None:
        kwargs.update(collection_events)
    elif collected_events is not None:
        # Collect-mode (multi-slot fan-in): bind each declared event
        # parameter to its collected event instead of a single trigger event.
        kwargs.update(collected_events)
    else:
        kwargs[step_config.event_name] = event
    if step_config.context_parameter:
        # Convert to internal face for step execution
        kwargs[step_config.context_parameter] = context
    with workflow._resource_manager.resolution_scope():
        for resource_def in step_config.resources:
            descriptor = resource_def.resource
            descriptor.set_type_annotation(resource_def.type_annotation)
            # Unified resolution through ResourceManager
            resource_value = await workflow._resource_manager.get(resource=descriptor)
            kwargs[resource_def.name] = resource_value
    return functools.partial(func, **kwargs)


def as_step_worker_functions(workflow: Workflow) -> dict[str, StepWorkerFunction]:
    step_funcs = workflow._get_steps()
    step_workers: dict[str, StepWorkerFunction] = {
        name: as_step_worker_function(getattr(func, "__func__", func))
        for name, func in step_funcs.items()
    }
    return step_workers


def as_step_worker_function(
    func: Callable[P, Awaitable[StepReturnT]],
) -> StepWorkerFunction:
    """
    Wrap a step function, setting context variables and handling exceptions to instead
    return the appropriate StepFunctionResult.
    """

    # Keep original function reference for free-function steps; for methods we
    # will resolve the currently-bound method from the provided workflow at call time.
    original_func: Callable[..., Awaitable[StepReturnT]] = func

    # Avoid functools.wraps here because it would set __wrapped__ to the bound
    # method (when present), which would strongly reference the workflow
    # instance and prevent garbage collection under high churn.
    async def wrapper(
        state: StepWorkerState,
        step_name: str,
        event: Event,
        workflow: Workflow,
        bound_events: dict[str, Event] | None = None,
        retry: RetryAttempt = RetryAttempt(),
    ) -> list[StepFunctionResult]:
        from workflows.context.context import Context

        internal_context = Context._create_internal(workflow=workflow)
        returns = Returns(return_values=[])

        step_ctx = StepWorkerContext(
            event=event,
            state=state,
            returns=returns,
            retry=retry,
        )
        token = StepWorkerStateContextVar.set(step_ctx)
        ctx_token = InternalContextVar.set(weakref.ref(internal_context))

        try:
            config = workflow._get_steps()[step_name]._step_config
            collected_binding: dict[str, Event] | None = None
            collection_binding: dict[str, list[Event]] | None = None
            if config.collection_param is not None:
                param_name, _ = config.collection_param
                payload = state.collection_release_payload
                if payload is None:
                    # A collect invocation without its release payload is
                    # corrupt state — fail loudly instead of re-running the
                    # step with a fabricated empty batch.
                    raise WorkflowRuntimeError(
                        f"Collect step {step_name!r} was invoked without a "
                        "collection release payload. This indicates a lost "
                        "work record (runtime bug or corrupted serialized "
                        "state)."
                    )
                collection_binding = {param_name: list(payload.events)}
            else:
                collected_binding = bound_events
            # Resolve callable at call time:
            # - If the workflow has an attribute with the step name, use it
            #   (this yields a bound method for instance-defined steps).
            # - Otherwise, fall back to the original function (free function step).
            try:
                call_func = getattr(workflow, step_name)
            except AttributeError:
                call_func = original_func
            # For async steps, intercept WaitingForEvent and CancelledError before
            # they reach dispatcher.span() to prevent them from being recorded as
            # error spans.
            captured_waiting: WaitingForEvent | None = None
            captured_cancelled: BaseException | None = None
            if asyncio.iscoroutinefunction(call_func):

                @functools.wraps(call_func)
                async def span_safe_call(*args: Any, **kwargs: Any) -> Any:
                    nonlocal captured_waiting, captured_cancelled
                    try:
                        step_result = await call_func(*args, **kwargs)
                        if step_result is not None and isinstance(step_result, Event):
                            _emit_output_event(
                                WorkflowStepOutputEvent(
                                    output=summarize_event(step_result)
                                )
                            )
                        return step_result
                    except WaitingForEvent as e:
                        captured_waiting = e
                        return None
                    except asyncio.CancelledError as e:
                        _dispatcher.event(SpanCancelledEvent(reason="step cancelled"))
                        captured_cancelled = e
                        return None

                span_target = span_safe_call
            else:

                @functools.wraps(call_func)
                def span_safe_sync_call(*args: Any, **kwargs: Any) -> Any:
                    step_result = call_func(*args, **kwargs)
                    if step_result is not None and isinstance(step_result, Event):
                        _emit_output_event(
                            WorkflowStepOutputEvent(output=summarize_event(step_result))
                        )
                    return step_result

                span_target = span_safe_sync_call

            # Prepare input event tags — these become span attributes when the
            # span is entered (inside partial_func), not when the wrapper is created.
            try:
                input_tags = {
                    "llamaindex.step.input_event": type(event).__name__,
                    "llamaindex.step.input_summary": summarize_event(event),
                }
            except Exception:
                input_tags = {}
            merged_tags = {**active_instrument_tags.get(), **input_tags}

            partial_func = await partial(
                func=workflow._dispatcher.span(span_target),
                step_config=config,
                event=event,
                context=internal_context,
                workflow=workflow,
                collected_events=collected_binding,
                collection_events=collection_binding,
            )

            try:
                # coerce to coroutine function
                if not asyncio.iscoroutinefunction(call_func):
                    # run_in_executor doesn't accept **kwargs, so we need to use partial
                    copy = copy_context()

                    result: StepReturnT = (
                        await asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: copy.run(
                                lambda: _run_with_tags(merged_tags, partial_func)
                            ),
                        )
                    )
                else:
                    with instrument_tags(merged_tags):
                        result = await partial_func()
                    if captured_cancelled is not None:
                        raise captured_cancelled
                    if captured_waiting is not None:
                        raise captured_waiting
                if isinstance(result, list) and config.is_fan_out:
                    # A step that actually returned a list fans out: each
                    # element is emitted as its own event into a fresh stream.
                    # An empty list emits nothing but still opens (and
                    # immediately closes) the stream, so joins fire with [].
                    for item in result:
                        if not isinstance(item, Event):
                            msg = (
                                f"Step function {step_name} returned a list "
                                f"containing {type(item).__name__} instead of an "
                                "Event instance."
                            )
                            raise WorkflowRuntimeError(msg)
                    if result:
                        for item in result:
                            returns.return_values.append(
                                StepWorkerResult(result=item, fanned_out=True)
                            )
                    else:
                        returns.return_values.append(
                            StepWorkerResult(result=None, fanned_out=True)
                        )
                elif result is not None and not isinstance(result, Event):
                    msg = f"Step function {step_name} returned {type(result).__name__} instead of an Event instance."
                    raise WorkflowRuntimeError(msg)
                elif (
                    config.is_fan_out
                    and result is not None
                    and not any(
                        isinstance(t, type) and isinstance(result, t)
                        for t in config.bare_return_types
                    )
                ):
                    # A bare event under a list-returning annotation. A type
                    # declared as a non-list union member (-> list[A] | B
                    # returning B) is ordinary dispatch and handled below; an
                    # undeclared bare element is an error, not a silent
                    # one-member stream.
                    msg = (
                        f"Step function {step_name} returned a bare "
                        f"{type(result).__name__} but its return annotation only "
                        "declares it inside a list. Return a one-element list to "
                        "fan out, or declare the bare type as a union member "
                        "(e.g. -> list[A] | B) for ordinary dispatch."
                    )
                    raise WorkflowRuntimeError(msg)
                else:
                    # Ordinary dispatch — including a fan-out step's declared
                    # non-list branch and its None (no emission, no stream)
                    # branch.
                    returns.return_values.append(StepWorkerResult(result=result))
            except WaitingForEvent as e:
                await asyncio.sleep(0)
                returns.return_values.append(e.add)
            except Exception as e:
                returns.return_values.append(
                    StepWorkerFailed(exception=e, failed_at=time.time())
                )

            await internal_context._finalize_step()
            return returns.return_values
        finally:
            try:
                InternalContextVar.reset(ctx_token)
            except Exception:
                pass
            try:
                StepWorkerStateContextVar.reset(token)
            except Exception:
                pass

    # Manually set minimal metadata without retaining bound instance references.
    try:
        unbound_for_wrapped = getattr(func, "__func__", func)
        wrapper.__name__ = getattr(func, "__name__", wrapper.__name__)
        wrapper.__qualname__ = getattr(func, "__qualname__", wrapper.__qualname__)
        # Point __wrapped__ to the unbound function when available to avoid
        # strong refs to the instance via a bound method object.
        setattr(wrapper, "__wrapped__", unbound_for_wrapped)
    except Exception:
        # Best-effort; lack of these attributes is non-fatal.
        pass

    return wrapper


def create_workflow_run_function(
    workflow: Workflow, control_loop_fn: ControlLoopFunction = control_loop
) -> WorkflowRunFunction:
    async def run_workflow(
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        tags: dict[str, Any] | None = None,
    ) -> StopEvent:
        from workflows.context.context import Context
        from workflows.context.internal_context import InternalContext

        registered = workflow._runtime.get_or_register(workflow)
        # Set run_id context before creating internal context
        internal_ctx = Context._create_internal(workflow=workflow)
        internal_adapter = workflow._runtime.get_internal_adapter(workflow)

        # Restore propagation context (otel trace, instrument tags, etc.)
        # before creating any spans so they parent correctly.
        get_dispatcher().restore_propagation_context(tags or {})

        # defer execution to make sure the task can be captured and passed
        # to the handler as async exception, protecting against exceptions from before_start
        await asyncio.sleep(0)

        run_ctx = RunContext(
            workflow=workflow,
            run_adapter=internal_adapter,
            context=internal_ctx,
            steps=registered.steps,
        )
        # Create a wrapping span so that all step spans have a parent.
        # The caller's span context is captured via propagation context (tags)
        # and restored above, so this span parents correctly under the caller.
        cls_name = workflow.__class__.__name__
        span_id = f"{cls_name}.run-{uuid.uuid4()}"
        outer_parent_span_id = active_span_id.get()
        span_token = active_span_id.set(span_id)

        bound_args = inspect.signature(run_workflow).bind(init_state, start_event, tags)

        # Set start event info as instrument tags for the run span
        if start_event is not None:
            try:
                run_input_tags = {
                    "llamaindex.start_event": summarize_event(start_event),
                }
            except Exception:
                run_input_tags = {}
        else:
            run_input_tags = {}

        with instrument_tags({**active_instrument_tags.get(), **run_input_tags}):
            _dispatcher.span_enter(
                id_=span_id,
                bound_args=bound_args,
                instance=workflow,
                parent_id=outer_parent_span_id,
            )

        try:
            try:
                with run_context(run_ctx):
                    result = await control_loop_fn(
                        start_event,
                        init_state,
                        internal_adapter.run_id,
                    )

                    _emit_output_event(
                        WorkflowRunOutputEvent(output=summarize_event(result))
                    )

                    _dispatcher.span_exit(
                        id_=span_id,
                        bound_args=bound_args,
                        instance=workflow,
                        result=result,
                    )

                    return result
            finally:
                # Cancel any background tasks from InternalContext on completion or cancellation
                if isinstance(internal_ctx._face, InternalContext):
                    internal_ctx._face.cancel_background_tasks()
        except WorkflowCancelledByUser:
            # User-initiated cancellation is not an error — exit the span
            # cleanly so it shows as OK rather than ERROR in traces.
            _dispatcher.event(SpanCancelledEvent(reason="workflow cancelled by user"))
            _dispatcher.span_exit(
                id_=span_id,
                bound_args=bound_args,
                instance=workflow,
                result=None,
            )
            raise
        except BaseException as e:
            _dispatcher.event(SpanDropEvent(span_id=span_id, err_str=str(e)))
            _dispatcher.span_drop(
                id_=span_id, bound_args=bound_args, instance=workflow, err=e
            )
            raise
        finally:
            try:
                active_span_id.reset(span_token)
            except ValueError:
                pass

    return run_workflow
