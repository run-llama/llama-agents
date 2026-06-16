# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import dataclasses
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Literal,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
    overload,
)

from pydantic import BaseModel

from .collect import Collect
from .errors import WorkflowValidationError
from .events import StepFailedEvent
from .resource import ResourceDefinition
from .utils import (
    inspect_signature,
    is_free_function,
    validate_step_signature,
)

if TYPE_CHECKING:  # pragma: no cover
    from .workflow import Workflow
from .retry_policy import RetryPolicy

WorkflowGraphCheck = Literal["reachability", "terminal_event", "dead_end"]
StepGraphCheck = Literal["reachability", "dead_end"]


StepRole = Literal["step", "catch_error"]


@dataclasses.dataclass
class StepConfig:
    accepted_events: list[Any]
    event_name: str
    return_types: list[Any]
    context_parameter: str | None
    num_workers: int
    retry_policy: RetryPolicy | None
    resources: list[ResourceDefinition]
    context_state_type: type[BaseModel] | None = None
    skip_graph_checks: list[StepGraphCheck] = dataclasses.field(default_factory=list)
    # Heterogeneous fan-in: when a step declares more than one event parameter,
    # this is the ordered list of (parameter_name, event_type) it collects. The
    # step fires once when one event of each type has arrived. None for the
    # ordinary single-event-trigger model.
    collect_params: list[tuple[str, Any]] | None = None
    # Fan-out producer: True when the return annotation is ``list[E]``. Such
    # a step MAY mint a fresh stream per execution — whether it does is a
    # runtime fact: only an actual list return opens a stream. Computed at
    # decoration time from the return annotation; used for binding computation
    # and validation.
    is_fan_out: bool = False
    # Non-list members of a fan-out return union (``-> list[A] | B`` -> (B,)).
    # A bare return of one of these types is ordinary dispatch; any other bare
    # event under a list-returning annotation is a runtime error.
    bare_return_types: tuple[Any, ...] = ()
    # Collection-stream fan-in: set to ``(parameter_name, element_event_types)``
    # when the step declares a single ``list[E]`` parameter. The element types are
    # a tuple — ``list[Done]`` -> ``(Done,)``; a union flat list ``list[A | B]`` ->
    # ``(A, B)`` (every member routes to the step). The step buffers incoming
    # members by innermost stream id and releases per ``collection_policy``.
    collection_param: tuple[str, tuple[Any, ...]] | None = None
    # The resolved ``Collect`` marker for the collection parameter. A bare
    # ``list[E]`` parameter resolves to ``Collect()`` (``All``). None for steps
    # without a collection parameter.
    collection_policy: Collect | None = None
    role: StepRole = "step"
    # Only meaningful when role == "catch_error".
    # None means wildcard — covers any step not claimed by a scoped handler.
    catch_error_for_steps: list[str] | None = None
    catch_error_max_recoveries: int = 1
    accept_event_subclasses: bool = False


@dataclasses.dataclass(frozen=True)
class CatchErrorHandler:
    """Runtime descriptor for a ``@catch_error`` handler.

    Precomputed by ``Workflow._validate()`` from the handler's ``StepConfig``;
    consumed by the control loop's failure-routing branch and by
    ``BrokerState.from_workflow``.
    """

    step_name: str
    for_steps: list[str] | None
    max_recoveries: int


P = ParamSpec("P")
R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)


class StepFunction(Protocol, Generic[P, R_co]):
    """A decorated function, that has some _step_config metadata from the @step decorator"""

    _step_config: StepConfig

    __name__: str
    __qualname__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co: ...


@overload
def step(func: Callable[P, R]) -> StepFunction[P, R]: ...


@overload
def step(
    *,
    workflow: type["Workflow"] | None = None,
    num_workers: int = 4,
    retry_policy: RetryPolicy | None = None,
    skip_graph_checks: list[StepGraphCheck] | None = None,
    accept_event_subclasses: bool = False,
) -> Callable[[Callable[P, R]], StepFunction[P, R]]: ...


def step(
    func: Callable[P, R] | None = None,
    *,
    workflow: type["Workflow"] | None = None,
    num_workers: int = 4,
    retry_policy: RetryPolicy | None = None,
    skip_graph_checks: list[StepGraphCheck] | None = None,
    accept_event_subclasses: bool = False,
) -> Callable[[Callable[P, R]], StepFunction[P, R]] | StepFunction[P, R]:
    """
    Decorate a callable to declare it as a workflow step.

    The decorator inspects the function signature to infer the accepted event
    type, return event types, optional `Context` parameter (optionally with a
    typed state model), and any resource injections via `typing.Annotated`.

    When applied to free functions, provide the workflow class via
    `workflow=MyWorkflow`. For instance methods, the association is automatic.

    Args:
        workflow (type[Workflow] | None): Workflow class to attach the free
            function step to. Not required for methods.
        num_workers (int): Number of workers for this step. Defaults to 4.
        retry_policy (RetryPolicy | None): Optional retry policy for failures.
        skip_graph_checks (list[str] | None): Graph validation checks to skip
            for this step. Currently supports ``"reachability"`` to allow
            intentionally unreachable steps.
        accept_event_subclasses (bool): If True, enable subclass-aware event routing.

    Returns:
        Callable: The original function, annotated with internal step metadata.

    Raises:
        WorkflowValidationError: If signature validation fails or when decorating
            a free function without specifying `workflow`.

    Examples:
        Method step:

        ```python
        class MyFlow(Workflow):
            @step
            async def start(self, ev: StartEvent) -> StopEvent:
                return StopEvent(result="done")
        ```

        Free function step:

        ```python
        class MyWorkflow(Workflow):
            pass

        @step(workflow=MyWorkflow)
        async def generate(ev: StartEvent) -> NextEvent: ...
        ```
    """

    def decorator(func: Callable[P, R]) -> StepFunction[P, R]:
        localns = _capture_decorator_localns()
        return _apply_step_decorator(
            func,
            num_workers=num_workers,
            retry_policy=retry_policy,
            workflow=workflow,
            localns=localns,
            skip_graph_checks=skip_graph_checks or [],
            accept_event_subclasses=accept_event_subclasses,
        )

    if func is not None:
        # The decorator was used without parentheses, like `@step`
        localns = _capture_callsite_localns()
        return _apply_step_decorator(
            func,
            num_workers=num_workers,
            retry_policy=retry_policy,
            workflow=workflow,
            localns=localns,
            skip_graph_checks=skip_graph_checks or [],
            accept_event_subclasses=accept_event_subclasses,
        )
    return decorator


def make_step_function(
    func: Callable[P, R],
    num_workers: int = 4,
    retry_policy: RetryPolicy | None = None,
    localns: dict[str, Any] | None = None,
    skip_graph_checks: list[StepGraphCheck] | None = None,
    accept_event_subclasses: bool = False,
) -> StepFunction[P, R]:
    # This will raise providing a message with the specific validation failure
    spec = inspect_signature(func, localns=localns)
    validate_step_signature(spec)

    event_name, accepted_events = next(iter(spec.accepted_events.items()))

    # Collect-mode (multi-slot fan-in): more than one event parameter. The step
    # accepts every declared event type for routing, then collects by
    # declaration order before firing once.
    collect_params: list[tuple[str, Any]] | None = None
    if len(spec.accepted_events) > 1:
        collect_params = [
            (name, param_types[0]) for name, param_types in spec.accepted_events.items()
        ]
        accepted_events = [event_type for _, event_type in collect_params]

    casted = cast(StepFunction[P, R], func)
    casted._step_config = StepConfig(
        accepted_events=accepted_events,
        event_name=event_name,
        return_types=spec.return_types,
        context_parameter=spec.context_parameter,
        context_state_type=spec.context_state_type,
        num_workers=num_workers,
        retry_policy=retry_policy,
        resources=spec.resources,
        skip_graph_checks=skip_graph_checks or [],
        collect_params=collect_params,
        is_fan_out=spec.is_fan_out,
        bare_return_types=tuple(spec.bare_return_types),
        collection_param=spec.collection_param,
        collection_policy=spec.collection_policy,
        accept_event_subclasses=accept_event_subclasses,
    )

    return casted


def _apply_step_decorator(
    func: Callable[P, R],
    *,
    num_workers: int,
    retry_policy: RetryPolicy | None,
    workflow: type["Workflow"] | None,
    localns: dict[str, Any] | None,
    skip_graph_checks: list[StepGraphCheck],
    accept_event_subclasses: bool,
) -> StepFunction[P, R]:
    if not isinstance(num_workers, int) or num_workers <= 0:
        raise WorkflowValidationError("num_workers must be an integer greater than 0")

    func = make_step_function(
        func,
        num_workers=num_workers,
        retry_policy=retry_policy,
        localns=localns,
        skip_graph_checks=skip_graph_checks,
        accept_event_subclasses=accept_event_subclasses,
    )

    # If this is a free function, call add_step() explicitly.
    if is_free_function(func.__qualname__):
        if workflow is None:
            msg = f"To decorate {func.__name__} please pass a workflow class to the @step decorator."
            raise WorkflowValidationError(msg)
        workflow.add_step(func)

    return func


@overload
def catch_error(func: Callable[P, R]) -> StepFunction[P, R]: ...


@overload
def catch_error(
    *,
    for_steps: list[str] | None = None,
    max_recoveries: int = 1,
) -> Callable[[Callable[P, R]], StepFunction[P, R]]: ...


def catch_error(
    func: Callable[P, R] | None = None,
    *,
    for_steps: list[str] | None = None,
    max_recoveries: int = 1,
) -> Callable[[Callable[P, R]], StepFunction[P, R]] | StepFunction[P, R]:
    """Mark a method as a handler for steps that exhaust their retries.

    Handlers can be scoped to specific steps via `for_steps`, or left as
    wildcards (default) to cover any step not claimed by a scoped handler.
    Each handler has a per-lineage recovery budget (`max_recoveries`): when the
    budget is exceeded the workflow fails instead of re-entering the handler.

    A handler may return any event type — the graph validator checks that the
    handler's sub-graph eventually terminates at a `StopEvent`.

    Args:
        for_steps: Step names this handler covers. `None` means wildcard.
        max_recoveries: How many times this handler may be invoked per lineage
            before the workflow fails. Must be >= 1. Defaults to 1.

    Examples:
        ```python
        from workflows import Workflow, catch_error, step, Context
        from workflows.events import StartEvent, StepFailedEvent, StopEvent

        class MyFlow(Workflow):
            @step(retry_policy=...)
            async def fetch(self, ev: StartEvent) -> FetchedEvent: ...

            @catch_error(for_steps=["fetch"], max_recoveries=2)
            async def handle_fetch(self, ctx: Context, ev: StepFailedEvent) -> FallbackEvent:
                return FallbackEvent(...)

            @catch_error  # wildcard; covers any step not owned by a scoped handler
            async def handle_default(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
                return StopEvent(result={"failed": ev.step_name})
        ```
    """

    if not isinstance(max_recoveries, int) or max_recoveries < 1:
        raise WorkflowValidationError(
            "@catch_error max_recoveries must be an integer >= 1"
        )
    if for_steps is not None:
        if not isinstance(for_steps, list) or not all(
            isinstance(s, str) for s in for_steps
        ):
            raise WorkflowValidationError(
                "@catch_error for_steps must be None or a list of step name strings"
            )

    def _apply(inner: Callable[P, R], localns: dict[str, Any]) -> StepFunction[P, R]:
        step_fn = make_step_function(
            inner,
            num_workers=1,
            retry_policy=None,
            localns=localns,
        )
        accepted = step_fn._step_config.accepted_events
        if len(accepted) != 1 or accepted[0] is not StepFailedEvent:
            name = getattr(inner, "__name__", repr(inner))
            raise WorkflowValidationError(
                f"@catch_error handler '{name}' must accept StepFailedEvent "
                f"as its event parameter."
            )
        step_fn._step_config.role = "catch_error"
        step_fn._step_config.catch_error_for_steps = (
            list(for_steps) if for_steps is not None else None
        )
        step_fn._step_config.catch_error_max_recoveries = max_recoveries
        return step_fn

    if func is not None:
        # bare usage: `@catch_error`
        return _apply(func, _capture_callsite_localns())

    def decorator(inner: Callable[P, R]) -> StepFunction[P, R]:
        return _apply(inner, _capture_decorator_localns())

    return decorator


def _capture_decorator_localns() -> dict[str, Any]:
    frame = inspect.currentframe()
    if frame is None or frame.f_back is None:
        return {}

    try:
        return _merge_ancestor_locals(frame.f_back, depth=4)
    finally:
        del frame


def _capture_callsite_localns() -> dict[str, Any]:
    frame = inspect.currentframe()
    if frame is None or frame.f_back is None or frame.f_back.f_back is None:
        return {}

    try:
        return _merge_ancestor_locals(frame.f_back.f_back, depth=3)
    finally:
        del frame


def _merge_ancestor_locals(start: Any, depth: int) -> dict[str, Any]:
    frames: list[Any] = []
    f = start
    for _ in range(depth):
        if f is None:
            break
        frames.append(f)
        f = f.f_back
    localns: dict[str, Any] = {}
    for fr in reversed(frames):
        localns.update(fr.f_locals)
    return localns
