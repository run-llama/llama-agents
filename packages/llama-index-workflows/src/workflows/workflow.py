# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import logging
import sys
import warnings
from inspect import Parameter, Signature
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    cast,
    get_args,
)

from llama_index_instrumentation import get_dispatcher
from pydantic import ValidationError
from typing_extensions import dataclass_transform

if TYPE_CHECKING:  # pragma: no cover
    from .context import Context
    from .runtime.types.plugin import Runtime
from ._event_matching import step_accepts_event
from .decorators import CatchErrorHandler, StepConfig, StepFunction, WorkflowGraphCheck
from .errors import (
    WorkflowRuntimeError,
    WorkflowValidationError,
)
from .events import Event, StartEvent, StopEvent
from .handler import WorkflowHandler
from .resource import ResourceManager
from .runtime.types.step_id import StepId
from .types import RunResultT
from .utils import get_steps_from_class, get_steps_from_instance

dispatcher = get_dispatcher(__name__)
logger = logging.getLogger(__name__)

# Default run timeout. Referenced by the constructor and type-checker field.
DEFAULT_TIMEOUT = 45.0


class _UnsetTimeout:
    """Sentinel for an unspecified ``timeout``.

    Lets the constructor tell "user passed nothing" apart from "user passed a
    value" without knowing yet whether this instance is a root or a child. A
    root resolves an unset timeout to ``DEFAULT_TIMEOUT``; a child defers to its
    parent and gets no deadline of its own.
    """

    def __repr__(self) -> str:
        return "<unset>"


_UNSET_TIMEOUT = _UnsetTimeout()


def _resolve_child_slots(cls: type) -> dict[str, type]:
    """Resolve workflow-typed annotations from ``cls`` and its bases."""
    workflow_cls = globals().get("Workflow")
    if workflow_cls is None:
        return {}
    slots: dict[str, type] = {}
    for klass in reversed(cls.__mro__):
        annotations = klass.__dict__.get("__annotations__") or getattr(
            klass, "__annotations__", None
        )
        if not annotations:
            continue
        module = sys.modules.get(klass.__module__)
        globalns = getattr(module, "__dict__", {})
        localns = dict(vars(klass))
        for field_name, annotation in annotations.items():
            resolved: Any = annotation
            if isinstance(annotation, str):
                try:
                    resolved = eval(annotation, globalns, localns)  # noqa: S307
                except Exception:
                    continue
            if isinstance(resolved, type) and issubclass(resolved, workflow_cls):
                slots[field_name] = resolved
    return slots


def _synthesized_workflow_init(self: Workflow, *args: Any, **kwargs: Any) -> None:
    """Constructor synthesized for classes that only declare child slots."""
    slots = type(self)._get_child_workflow_slots()
    children: dict[str, Any] = {}
    for slot_name in slots:
        if slot_name in kwargs:
            children[slot_name] = kwargs.pop(slot_name)
    Workflow.__init__(self, *args, **kwargs)
    for slot_name, child in children.items():
        self._attach_child(slot_name, child)


def _validate_includable_child(child: Workflow, slot_name: str) -> None:
    """A workflow is includable as a child only if it declares custom
    ``StartEvent`` and ``StopEvent`` subclasses — the typed IO is the routing
    contract that maps each child's events to exactly one child.
    """
    cls_name = type(child).__name__
    if child._start_event_class is StartEvent:
        raise WorkflowValidationError(
            f"Child workflow '{cls_name}' (slot '{slot_name}') must declare a "
            "custom StartEvent subclass; a bare StartEvent cannot be routed to a "
            "child unambiguously."
        )
    if child._stop_event_class is StopEvent:
        raise WorkflowValidationError(
            f"Child workflow '{cls_name}' (slot '{slot_name}') must declare a "
            "custom StopEvent subclass; a bare StopEvent cannot be routed back "
            "to the parent unambiguously."
        )


def _warn_ignored_child_config(child: Workflow, slot_name: str) -> None:
    ignored: list[str] = []
    if child._verbose:
        ignored.append("verbose=True")
    if child._num_concurrent_runs is not None:
        ignored.append(f"num_concurrent_runs={child._num_concurrent_runs!r}")
    if child._workflow_name is not None:
        ignored.append(f"workflow_name={child._workflow_name!r}")
    if not ignored:
        return
    warnings.warn(
        f"Child workflow slot '{slot_name}' on '{type(child).__name__}' has "
        f"run-level config ignored when nested: {', '.join(ignored)}.",
        UserWarning,
        stacklevel=3,
    )


def _has_custom_workflow_init(cls: type) -> bool:
    return bool(getattr(cls, "_custom_workflow_init", False))


def _config_field(*, alias: str, default: Any = None) -> Any:
    """dataclass_transform field specifier for Workflow config params."""
    return default


@dataclass_transform(kw_only_default=True, field_specifiers=(_config_field,))
class WorkflowMeta(type):
    # Defined only at runtime, hidden from type checkers. As a metaclass
    # __call__ it is the single entry point for every instantiation and sits
    # above the whole __init__/super() chain: type.__call__ drives __new__ and
    # the complete chain as one unit, so when it returns — every derived
    # __init__ done and all children assigned — _finalize_construction runs at
    # the true outermost point of construction. Registration then sees the full
    # child tree regardless of init style, subclassing depth, or launch timing.
    #
    # It is hidden behind ``if not TYPE_CHECKING`` because a typed metaclass
    # __call__ can't satisfy both type checkers at once: basedpyright needs a
    # generic ``cls: type[T] -> T`` self-type to preserve the constructed type,
    # but that exact annotation makes ty reject the ``super().__call__`` as not
    # bound to the metaclass. Hiding it lets both fall back to the correct
    # dataclass_transform constructor typing (``Parent(...)`` -> ``Parent``).
    if not TYPE_CHECKING:

        def __call__(cls, *args: Any, **kwargs: Any) -> Any:
            obj = super().__call__(*args, **kwargs)
            obj._finalize_construction()
            return obj

    def __init__(cls, name: str, bases: tuple[type, ...], dct: dict[str, Any]) -> None:
        super().__init__(name, bases, dct)
        cls._step_functions: dict[str, StepFunction] = {}
        cls._custom_workflow_init = False

        # A user-defined __init__ always wins.
        workflow_cls = globals().get("Workflow")
        if workflow_cls is None:
            return
        if "__init__" in dct:
            cls._custom_workflow_init = cls is not workflow_cls
            return
        if any(getattr(base, "_custom_workflow_init", False) for base in bases):
            cls._custom_workflow_init = True
            return
        if not _resolve_child_slots(cls):
            return
        inherited_init = getattr(cls, "__init__", None)
        if (
            inherited_init is workflow_cls.__init__
            or inherited_init is _synthesized_workflow_init
        ):
            setattr(cls, "__init__", _synthesized_workflow_init)
            cls.__signature__ = _child_slot_signature(cls)


def _child_slot_signature(cls: type) -> Signature:
    params = [
        Parameter(
            name,
            kind=Parameter.KEYWORD_ONLY,
            default=Parameter.empty,
            annotation=slot_type,
        )
        for name, slot_type in _resolve_child_slots(cls).items()
    ]
    params.extend(
        [
            Parameter(
                "timeout",
                kind=Parameter.KEYWORD_ONLY,
                default=_UNSET_TIMEOUT,
                annotation=float | None | _UnsetTimeout,
            ),
            Parameter(
                "disable_validation",
                kind=Parameter.KEYWORD_ONLY,
                default=False,
                annotation=bool,
            ),
            Parameter(
                "verbose",
                kind=Parameter.KEYWORD_ONLY,
                default=False,
                annotation=bool,
            ),
            Parameter(
                "resource_manager",
                kind=Parameter.KEYWORD_ONLY,
                default=None,
                annotation=ResourceManager | None,
            ),
            Parameter(
                "num_concurrent_runs",
                kind=Parameter.KEYWORD_ONLY,
                default=None,
                annotation=int | None,
            ),
            Parameter(
                "runtime",
                kind=Parameter.KEYWORD_ONLY,
                default=None,
                annotation=Any,
            ),
            Parameter(
                "workflow_name",
                kind=Parameter.KEYWORD_ONLY,
                default=None,
                annotation=str | None,
            ),
            Parameter(
                "skip_graph_checks",
                kind=Parameter.KEYWORD_ONLY,
                default=None,
                annotation=set[WorkflowGraphCheck] | None,
            ),
        ]
    )
    return Signature(params)


class Workflow(metaclass=WorkflowMeta):
    """
    Event-driven orchestrator to define and run application flows using typed steps.

    A `Workflow` is composed of `@step`-decorated callables that accept and emit
    typed [Event][workflows.events.Event]s. Steps can be declared as instance
    methods or as free functions registered via the decorator.

    Key features:
    - Validation of step signatures and event graph before running
    - Typed start/stop events
    - Streaming of intermediate events
    - Optional human-in-the-loop events
    - Retry policies per step
    - Resource injection

    Examples:
        Basic usage:

        ```python
        from workflows import Workflow, step
        from workflows.events import StartEvent, StopEvent

        class MyFlow(Workflow):
            @step
            async def start(self, ev: StartEvent) -> StopEvent:
                return StopEvent(result="done")

        result = await MyFlow(timeout=60).run(topic="Pirates")
        ```

        Custom start/stop events and streaming:

        ```python
        handler = MyFlow().run()
        async for ev in handler.stream_events():
            ...
        result = await handler
        ```

    See Also:
        - [step][workflows.decorators.step]
        - [Event][workflows.events.Event]
        - [Context][workflows.context.context.Context]
        - [WorkflowHandler][workflows.handler.WorkflowHandler]
        - [RetryPolicy][workflows.retry_policy.RetryPolicy]
    """

    # Class-level state (metaclass / per-class), NOT constructor fields.
    _step_functions: ClassVar[dict[str, StepFunction]]
    _step_functions_version: ClassVar[int] = 0
    _child_workflow_slots_cache: ClassVar[dict[str, type] | None] = None

    # Phantom dataclass_transform fields for typed subclass constructors.
    _timeout_arg: float | None = _config_field(alias="timeout", default=_UNSET_TIMEOUT)
    _disable_validation_arg: bool = _config_field(
        alias="disable_validation", default=False
    )
    _verbose_arg: bool = _config_field(alias="verbose", default=False)
    _resource_manager_arg: ResourceManager | None = _config_field(
        alias="resource_manager", default=None
    )
    _num_concurrent_runs_arg: int | None = _config_field(
        alias="num_concurrent_runs", default=None
    )
    _runtime_arg: Runtime | None = _config_field(alias="runtime", default=None)
    _workflow_name_arg: str | None = _config_field(alias="workflow_name", default=None)
    _skip_graph_checks_arg: set[WorkflowGraphCheck] | None = _config_field(
        alias="skip_graph_checks", default=None
    )

    def __init__(
        self,
        timeout: float | None | _UnsetTimeout = _UNSET_TIMEOUT,
        disable_validation: bool = False,
        verbose: bool = False,
        resource_manager: ResourceManager | None = None,
        num_concurrent_runs: int | None = None,
        runtime: Runtime | None = None,
        workflow_name: str | None = None,
        skip_graph_checks: set[WorkflowGraphCheck] | None = None,
    ) -> None:
        """
        Initialize a workflow instance.

        Args:
            timeout (float | None): Max seconds to wait for completion. `None`
                disables the timeout. When unset, a root workflow defaults to
                45s; a child workflow defaults to no timeout of its own and is
                bounded by its parent. An explicit value (including `None`) is
                always honored.
            disable_validation (bool): Skip pre-run validation of the event graph
                (not recommended).
            verbose (bool): If True, print step activity.
            resource_manager (ResourceManager | None): Custom resource manager
                for dependency injection.
            num_concurrent_runs (int | None): Limit on concurrent `run()` calls.
            runtime (Runtime | None): Optional runtime to use for this workflow.
                If not provided, uses the current context-scoped runtime or
                falls back to basic_runtime.
            workflow_name (str | None): Optional explicit name for this workflow.
                If not provided, a module-qualified name is computed from
                the class's `__module__` and `__qualname__` attributes.
            skip_graph_checks (set[str] | None): Optional set of graph validation
                checks to skip (e.g. "reachability", "terminal_event"). Use to
                allow intentional patterns that would otherwise fail validation.
        """
        # Inline imports: every module below imports ``Workflow`` transitively,
        # so deferring to call time breaks the cycle.
        from workflows.plugins._context import get_current_runtime
        from workflows.runtime.verbose import VerboseDecorator

        from .representation.validate import (
            _collect_events,
            _ensure_start_event_class,
            _ensure_stop_event_class,
        )

        # Configuration. Resolve an unset timeout to the root default so the
        # runner and `_timeout` readers see a concrete value; `_timeout_set`
        # records whether the user supplied one, which lets a child suppress its
        # own deadline (see `_child_namespace_timeout`).
        if isinstance(timeout, _UnsetTimeout):
            self._timeout_set = False
            self._timeout: float | None = DEFAULT_TIMEOUT
        else:
            self._timeout_set = True
            self._timeout = timeout
        self._verbose = verbose
        self._disable_validation = disable_validation
        self._num_concurrent_runs = num_concurrent_runs
        # Store explicit name (None means use computed name)
        self._workflow_name: str | None = workflow_name

        step_configs = self._step_configs()
        cls_name = self.__class__.__name__
        # Detect StartEvent issues before StopEvent for clearer guidance
        self._start_event_class = _ensure_start_event_class(step_configs, cls_name)
        self._stop_event_class = _ensure_stop_event_class(step_configs, cls_name)
        # Populated by _validate(); empty until a successful validation runs.
        self._catch_error_handlers: dict[str, CatchErrorHandler] = {}
        self._handler_for_step: dict[str, str] = {}
        # Attached child-workflow instances, keyed by declared field name.
        self._child_workflows: dict[str, Workflow] = {}
        self._events = _collect_events(step_configs)
        # Resource management
        self._resource_manager = resource_manager or ResourceManager()
        # Instrumentation
        self._dispatcher = dispatcher
        self._runtime_locked = False
        # Validation cache: set after first successful _validate(); skip re-validation on run() until invalidated.
        # _validated_version tracks which _step_functions_version was validated so add_step() invalidates the cache.
        self._validation_result: bool | None = None
        self._validated_version: int = -1
        checks = skip_graph_checks or set()
        valid_checks = set(get_args(WorkflowGraphCheck))
        unknown = checks - valid_checks
        if unknown:
            raise WorkflowValidationError(
                f"Unknown graph check names: {', '.join(sorted(unknown))}. "
                f"Valid names are: {', '.join(sorted(valid_checks))}"
            )
        self._skip_graph_checks: set[WorkflowGraphCheck] = checks

        # Runtime registration: explicit > context-scoped > basic_runtime
        self._runtime: Runtime = (
            runtime if runtime is not None else get_current_runtime()
        )
        if self._verbose:
            self._runtime = VerboseDecorator(self._runtime)
        # Tracking is deferred until subclass construction has finished.

    def _finalize_construction(self) -> None:
        """Attach declared children and track the workflow once."""
        if not hasattr(self, "_runtime"):
            # _runtime is the last attribute set by Workflow.__init__, so its
            # absence means a subclass __init__ skipped super().__init__().
            # __call__ runs this for every instance, so without the guard the
            # next lines raise an opaque AttributeError on a private attribute.
            raise WorkflowValidationError(
                f"{type(self).__name__}.__init__ did not call super().__init__(). "
                "Workflow subclasses with a custom __init__ must call "
                "super().__init__(...) so the base workflow is initialized."
            )
        self._ensure_children_attached()
        # Register with runtime for tracking (no-op for BasicRuntime)
        self._runtime.track_workflow(self)

    def _validate_valid_step_message(self, step: str, message: Event) -> None:
        """Validate that a step name exists in the workflow and accepts ``message``."""
        self._resolve_target_step(step, message)

    def _resolve_target_step(
        self, step: str, message: Event, base_namespace: tuple[str, ...] = ()
    ) -> StepId:
        """Resolve a ``send_event(step=...)`` target to a validated ``StepId``.

        ``step`` is resolved relative to ``base_namespace`` — root (``()``) for
        external sends, the emitting step's namespace for internal sends — so a
        bare name lands in that namespace and a ``"child/answer"`` path descends
        from it. The single resolver shared by ``Context.send_event`` and
        ``ExternalContext.send_event``: it validates against the full namespaced
        step set (not the root-only one) and that the target accepts the message,
        naming the valid steps when it rejects.
        """
        parsed = StepId.from_str(step)
        target = StepId(base_namespace + parsed.namespace, parsed.name)
        namespaced = self._get_namespaced_steps()
        step_func = namespaced.get(target)
        if step_func is None:
            valid = ", ".join(sorted(str(s) for s in namespaced))
            raise WorkflowRuntimeError(
                f"Step {step} does not exist. Valid steps: {valid}"
            )
        step_config = step_func._step_config
        if not step_accepts_event(
            message,
            step_config.accepted_events,
            allow_subclasses=step_config.accept_event_subclasses,
        ):
            raise WorkflowRuntimeError(
                f"Step {step} does not accept event of type {type(message)}"
            )
        return target

    @property
    def runtime(self) -> Runtime:
        """The runtime this workflow is registered with."""
        return self._runtime

    def _switch_runtime(self, new_runtime: Runtime, *, register: bool = True) -> None:
        """Reassign this workflow's runtime, propagating into children."""
        if new_runtime is not self._runtime:
            if self._runtime_locked:
                raise RuntimeError(
                    "Cannot reassign runtime after workflow has been launched"
                )
            self._runtime.untrack_workflow(self)
            self._runtime = new_runtime
            if register:
                new_runtime.track_workflow(self)
        # Descendants may have been attached after this node last switched.
        for child in self._child_workflows.values():
            was_locked = child._runtime_locked
            child._runtime_locked = False
            try:
                child._switch_runtime(new_runtime, register=False)
            finally:
                child._runtime_locked = was_locked

    @classmethod
    def _get_child_workflow_slots(cls) -> dict[str, type]:
        """Return ``{field_name: child_workflow_type}`` declared on this class.

        Resolved from class annotations whose type is a ``Workflow`` subclass.
        Cached per-class on first access.
        """
        cached = cls.__dict__.get("_child_workflow_slots_cache")
        if cached is None:
            cached = _resolve_child_slots(cls)
            cls._child_workflow_slots_cache = cached
        return cached

    @property
    def child_workflows(self) -> dict[str, Workflow]:
        """Attached child-workflow instances, keyed by declared field name."""
        return dict(self._child_workflows)

    def _attach_child(self, name: str, child: Workflow) -> None:
        """Wire a child workflow instance into this parent."""
        if self._child_workflows.get(name) is child:
            return
        if not isinstance(child, Workflow):
            raise WorkflowValidationError(
                f"Child workflow slot '{name}' on '{type(self).__name__}' must be "
                f"a Workflow instance, got {type(child).__name__}."
            )
        expected = type(self)._get_child_workflow_slots().get(name)
        if expected is not None and not isinstance(child, expected):
            raise WorkflowValidationError(
                f"Child workflow slot '{name}' on '{type(self).__name__}' expects "
                f"{expected.__name__}, got {type(child).__name__}."
            )
        _validate_includable_child(child, name)
        _warn_ignored_child_config(child, name)
        register_child = getattr(self._runtime, "_register_child_workflows", True)
        if child._runtime is self._runtime:
            if not register_child:
                child._runtime.untrack_workflow(child)
        else:
            child._runtime.untrack_workflow(child)
            child._switch_runtime(self._runtime, register=register_child)
        child._runtime_locked = True
        setattr(self, name, child)
        self._child_workflows[name] = child

    def _ensure_children_attached(self) -> None:
        """Attach any declared child instances not yet wired in."""
        for name, expected in type(self)._get_child_workflow_slots().items():
            if name in self._child_workflows:
                continue
            if name not in self.__dict__ and isinstance(
                getattr(type(self), name, None), Workflow
            ):
                raise WorkflowValidationError(
                    f"Child workflow slot '{name}' on '{type(self).__name__}' uses "
                    "a shared class-body Workflow instance. Pass a fresh child "
                    "instance to the constructor instead."
                )
            child = getattr(self, name, None)
            if child is None:
                if _has_custom_workflow_init(type(self)) and not (
                    self._missing_child_is_used(name, expected)
                ):
                    continue
                # Inline import: representation validation imports Workflow.
                from .representation.validate import _validate_child_type_graph

                _validate_child_type_graph(cast(Any, type(self)))
                raise WorkflowValidationError(
                    f"Missing child workflow for slot '{name}' on "
                    f"'{type(self).__name__}'. Pass a {expected.__name__} "
                    "instance to the constructor."
                )
            if isinstance(child, Workflow):
                if _has_custom_workflow_init(type(self)) and (
                    child._start_event_class is StartEvent
                    or child._stop_event_class is StopEvent
                ):
                    continue
                self._attach_child(name, child)

    def _missing_child_is_used(self, name: str, expected: type) -> bool:
        """Whether a missing declared child forms a parent graph boundary."""
        # Inline import: representation validation imports Workflow.
        from .representation.validate import (
            _ensure_start_event_class,
            _ensure_stop_event_class,
        )

        expected_workflow = cast("type[Workflow]", expected)
        child_step_configs = {
            step_name: step_func._step_config
            for step_name, step_func in expected_workflow._get_steps_from_class().items()
        }
        child_start = _ensure_start_event_class(child_step_configs, expected.__name__)
        child_stop = _ensure_stop_event_class(child_step_configs, expected.__name__)
        if child_start is StartEvent or child_stop is StopEvent:
            return False
        for cfg in self._step_configs().values():
            if child_start in cfg.return_types or child_stop in cfg.accepted_events:
                return True
        return False

    @property
    def workflow_name(self) -> str:
        """
        The workflow name.

        If an explicit name was provided at construction, returns that.
        Otherwise, returns a module-qualified name based on the class's
        __module__ and __qualname__ attributes.

        Examples:
            - Explicit: `Workflow(workflow_name="my-workflow")` → `"my-workflow"`
            - Module-level class: `"mymodule.MyWorkflow"`
            - Nested class: `"mymodule.Outer.Inner"`
            - Function-scoped: `"mymodule.func.<locals>.LocalWorkflow"`
        """
        if self._workflow_name is not None:
            return self._workflow_name
        cls = self.__class__
        return f"{cls.__module__}.{cls.__qualname__}"

    def _switch_workflow_name(self, name: str) -> None:
        if self._runtime_locked and name != self._workflow_name:
            raise RuntimeError(
                "Cannot change workflow_name after workflow has been launched"
            )
        self._workflow_name = name

    def _step_configs(self) -> dict[str, StepConfig]:
        """Return ``{step_name: StepConfig}`` for every registered step."""
        return {name: func._step_config for name, func in self._get_steps().items()}

    @property
    def start_event_class(self) -> type[StartEvent]:
        """The `StartEvent` subclass accepted by this workflow.

        Determined by inspecting step input types.
        """
        return self._start_event_class

    @property
    def events(self) -> list[type[Event]]:
        """Returns all known events emitted by this workflow.

        Determined by inspecting step input/output types.
        """
        return self._events

    @property
    def stop_event_class(self) -> type[RunResultT]:
        """The `StopEvent` subclass produced by this workflow.

        Determined by inspecting step return annotations.
        """
        return self._stop_event_class

    @classmethod
    def _get_steps_from_class(cls) -> dict[str, StepFunction]:
        """Returns all the steps, whether defined as methods or free functions."""
        return {**get_steps_from_class(cls), **cls._step_functions}

    @classmethod
    def _get_namespaced_steps_from_class(cls) -> dict[StepId, StepFunction]:
        """Return class-declared steps keyed by namespaced ``StepId``."""
        result: dict[StepId, StepFunction] = {
            StepId((), name): func for name, func in cls._get_steps_from_class().items()
        }
        for field_name, child_type in cls._get_child_workflow_slots().items():
            child_cls = cast("type[Workflow]", child_type)
            for child_id, func in child_cls._get_namespaced_steps_from_class().items():
                result[StepId((field_name, *child_id.namespace), child_id.name)] = func
        return result

    @classmethod
    def add_step(cls, func: StepFunction) -> None:
        """
        Adds a free function as step for this workflow instance.

        It raises an exception if a step with the same name was already added to the workflow.
        """
        step_config: StepConfig | None = getattr(func, "_step_config", None)
        if not step_config:
            msg = f"Step function {func.__name__} is missing the `@step` decorator."
            raise WorkflowValidationError(msg)

        if func.__name__ in cls._get_steps_from_class():
            msg = f"A step {func.__name__} is already part of this workflow, please choose another name."
            raise WorkflowValidationError(msg)

        cls._step_functions[func.__name__] = func
        cls._step_functions_version += 1

    def _get_steps(self) -> dict[str, StepFunction]:
        """Returns all the steps, whether defined as methods or free functions."""
        return {**get_steps_from_instance(self), **self.__class__._step_functions}

    def _get_namespaced_steps(self) -> dict[StepId, StepFunction]:
        """Return this workflow's steps keyed by namespaced ``StepId``."""
        self._ensure_children_attached()
        result: dict[StepId, StepFunction] = {
            StepId((), name): func for name, func in self._get_steps().items()
        }
        for field_name, child in self._child_workflows.items():
            for child_id, func in child._get_namespaced_steps().items():
                result[StepId((field_name, *child_id.namespace), child_id.name)] = func
        return result

    def _child_namespace_timeout(self) -> float | None:
        """Per-namespace deadline to arm when this instance runs as a child.

        An unset child defers to its parent and gets no deadline of its own; an
        explicit timeout — including ``None`` (no deadline) — is honored.
        """
        return self._timeout if self._timeout_set else None

    def _namespace_instances(self) -> dict[tuple[str, ...], Workflow]:
        """Map each namespace path to the workflow instance that owns it."""
        self._ensure_children_attached()
        result: dict[tuple[str, ...], Workflow] = {(): self}
        for field_name, child in self._child_workflows.items():
            for ns, inst in child._namespace_instances().items():
                result[(field_name, *ns)] = inst
        return result

    def _get_start_event_instance(
        self, start_event: StartEvent | None, **kwargs: Any
    ) -> StartEvent:
        if start_event is not None:
            # start_event was used wrong
            if not isinstance(start_event, StartEvent):
                msg = "The 'start_event' argument must be an instance of 'StartEvent'."
                raise ValueError(msg)

            # start_event is ok but point out that additional kwargs will be ignored in this case
            if kwargs:
                msg = (
                    "Keyword arguments are not supported when 'run()' is invoked with the 'start_event' parameter."
                    f" These keyword arguments will be ignored: {kwargs}"
                )
                logger.warning(msg)
            return start_event

        # Old style start event creation, with kwargs used to create an instance of self._start_event_class
        try:
            return self._start_event_class(**kwargs)
        except ValidationError as e:
            ev_name = self._start_event_class.__name__
            msg = f"Failed creating a start event of type '{ev_name}' with the keyword arguments: {kwargs}"
            logger.debug(e)
            raise WorkflowRuntimeError(msg)

    def run(
        self,
        ctx: Context | None = None,
        start_event: StartEvent | None = None,
        **kwargs: Any,
    ) -> WorkflowHandler:
        """Run the workflow and return a handler for results and streaming.

        This schedules the workflow execution in the background and returns a
        [WorkflowHandler][workflows.handler.WorkflowHandler] that can be awaited
        for the final result or used to stream intermediate events.

        You may pass either a concrete `start_event` instance or keyword
        arguments that will be used to construct the inferred
        [StartEvent][workflows.events.StartEvent] subclass.

        Args:
            ctx (Context | None): Optional context to resume or share state
                across runs. If omitted, a fresh context is created.
            start_event (StartEvent | None): Optional explicit start event.
            **kwargs (Any): Keyword args to initialize the start event when
                `start_event` is not provided.

        Returns:
            WorkflowHandler: A future-like object to await the final result and
            stream events.

        Raises:
            WorkflowValidationError: If validation fails and validation is
                enabled.
            WorkflowRuntimeError: If the start event cannot be created from kwargs.
            WorkflowTimeoutError: If execution exceeds the configured timeout.

        Examples:
            ```python
            # Create and run with kwargs
            handler = MyFlow().run(topic="Pirates")

            # Stream events
            async for ev in handler.stream_events():
                ...

            # Await final result
            result = await handler
            ```

            If you subclassed the start event, you can also directly pass it in:

            ```python
            result = await my_workflow.run(start_event=MyStartEvent(topic="Pirates"))
            ```
        """
        from workflows.context import Context

        self._ensure_children_attached()

        if not self._runtime_locked:
            # don't allow switching runtime after a workflow has been launched
            self._runtime_locked = True

        # Validate the workflow
        self._validate()

        # Extract run_id before passing remaining kwargs to start event
        run_id = kwargs.pop("run_id", None)

        # If a previous context is provided, pass its serialized form
        ctx = ctx if ctx is not None else Context(self)
        # TODO(v3) - remove dependency on is running for choosing whether to send a StartEvent.
        # Is not an easily synchronously queryable property.
        start_event_instance: StartEvent | None = (
            None
            if ctx.is_running
            else self._get_start_event_instance(start_event, **kwargs)
        )
        return ctx._workflow_run(
            workflow=self, start_event=start_event_instance, run_id=run_id
        )

    def validate(
        self,
        *,
        validate_resource_configs: bool = True,
        validate_resources: bool = False,
    ) -> bool:
        """
        Validate the workflow to ensure it's well-formed.

        This method validates the event graph and optionally validates resources:
        - Event production/consumption (set-based checks)
        - Graph structure: all steps reachable from an input event (StartEvent or HumanResponseEvent),
          and only output events (StopEvent, InputRequiredEvent) may be terminal
        - Resource configs (JSON files with Pydantic validation) are validated by default
        - Resource factories are not validated by default (may require env vars)
        - Circular resource dependencies are caught when validate_resources=True

        Validation result is cached after the first successful run(); subsequent run() calls
        skip re-validation. Calling validate() explicitly always re-runs all checks.

        Args:
            validate_resource_configs: If True (default), validate that resource
                config files exist and contain valid data for their Pydantic models.
            validate_resources: If False (default), skip resolving resource factories
                during validation. Set to True to also validate that resource
                factories can be resolved and detect circular dependencies
                (may require environment variables or external connections).

        Returns:
            True if the workflow uses human-in-the-loop, False otherwise.
        """
        return self._validate(
            validate_resource_configs=validate_resource_configs,
            validate_resources=validate_resources,
            force=True,  # Explicit validate() call should always run
        )

    def _validate(
        self,
        *,
        validate_resource_configs: bool = True,
        validate_resources: bool = False,
        force: bool = False,
    ) -> bool:
        self._ensure_children_attached()
        if self._disable_validation and not force:
            return False
        stale = self._validated_version != self.__class__._step_functions_version
        if not force and not stale and self._validation_result is not None:
            return self._validation_result

        # Inline import: ``representation`` transitively imports ``Workflow``.
        from .representation.validate import (
            _validate_child_workflow_declarations,
            _validate_resource_configs,
            _validate_resources,
            _validate_workflow,
        )

        step_configs = self._step_configs()
        # A child is "triggered" when some parent step emits its StartEvent; only
        # then does it form a boundary in the parent graph (StartEvent crosses
        # out, StopEvent crosses back in). Children attached but never triggered
        # are inert and excluded so they don't trip reachability.
        parent_return_types: set[type] = set()
        for cfg in step_configs.values():
            parent_return_types.update(cfg.return_types)
        child_boundaries = [
            (child._start_event_class, child._stop_event_class)
            for child in self._child_workflows.values()
            if child._start_event_class in parent_return_types
        ]
        result = _validate_workflow(
            step_configs,
            self.__class__.__name__,
            self._skip_graph_checks,
            child_boundaries=child_boundaries,
        )
        self._start_event_class = result.start_event_class
        self._stop_event_class = result.stop_event_class
        self._catch_error_handlers = result.catch_error_handlers
        self._handler_for_step = result.handler_for_step

        if validate_resource_configs:
            if errors := _validate_resource_configs(step_configs):
                raise WorkflowValidationError(
                    "Resource config validation failed:\n"
                    + "\n".join(f"  - {e}" for e in errors)
                )

        if validate_resources:
            errors = asyncio.run(
                _validate_resources(step_configs, self._resource_manager)
            )
            if errors:
                raise WorkflowValidationError(
                    "Resource validation failed:\n"
                    + "\n".join(f"  - {e}" for e in errors)
                )

        _validate_child_workflow_declarations(self)
        for child in self._child_workflows.values():
            child._validate(
                validate_resource_configs=validate_resource_configs,
                validate_resources=validate_resources,
                force=force,
            )

        self._validation_result = result.uses_hitl
        self._validated_version = self.__class__._step_functions_version
        return self._validation_result
