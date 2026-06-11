# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    get_args,
)

from llama_index_instrumentation import get_dispatcher
from pydantic import ValidationError

if TYPE_CHECKING:  # pragma: no cover
    from .context import Context
    from .runtime.types.plugin import Runtime
from ._event_matching import step_accepts_event
from .decorators import CatchErrorHandler, StepConfig, StepFunction, WorkflowGraphCheck
from .errors import (
    WorkflowRuntimeError,
    WorkflowValidationError,
)
from .events import Event, StartEvent
from .handler import WorkflowHandler
from .resource import ResourceManager
from .types import RunResultT
from .utils import get_steps_from_class, get_steps_from_instance

dispatcher = get_dispatcher(__name__)
logger = logging.getLogger(__name__)


class WorkflowMeta(type):
    def __init__(cls, name: str, bases: tuple[type, ...], dct: dict[str, Any]) -> None:
        super().__init__(name, bases, dct)
        cls._step_functions: dict[str, StepFunction] = {}


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

    # Populated by the metaclass; declared here for type checkers.
    _step_functions: dict[str, StepFunction]
    _step_functions_version: int = 0

    _runtime: Runtime
    _workflow_name: str | None

    def __init__(
        self,
        timeout: float | None = 45.0,
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
                disables the timeout.
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

        # Configuration
        self._timeout = timeout
        self._verbose = verbose
        self._disable_validation = disable_validation
        self._num_concurrent_runs = num_concurrent_runs
        # Store explicit name (None means use computed name)
        self._workflow_name = workflow_name

        step_configs = self._step_configs()
        cls_name = self.__class__.__name__
        # Detect StartEvent issues before StopEvent for clearer guidance
        self._start_event_class = _ensure_start_event_class(step_configs, cls_name)
        self._stop_event_class = _ensure_stop_event_class(step_configs, cls_name)
        # Populated by _validate(); empty until a successful validation runs.
        self._catch_error_handlers: dict[str, CatchErrorHandler] = {}
        self._handler_for_step: dict[str, str] = {}
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
        self._runtime = runtime if runtime is not None else get_current_runtime()
        if self._verbose:
            self._runtime = VerboseDecorator(self._runtime)
        # Register with runtime for tracking (no-op for BasicRuntime)
        self._runtime.track_workflow(self)

    def _validate_valid_step_message(self, step: str, message: Event) -> None:
        """Validate that a step name exists in the workflow."""
        if step not in self._get_steps():
            raise WorkflowRuntimeError(f"Step {step} does not exist")

        step_func = self._get_steps()[step]
        step_config = step_func._step_config
        is_accepted = step_accepts_event(
            message,
            step_config.accepted_events,
            allow_subclasses=step_config.accept_event_subclasses,
        )
        if not is_accepted:
            raise WorkflowRuntimeError(
                f"Step {step} does not accept event of type {type(message)}"
            )

    @property
    def runtime(self) -> Runtime:
        """The runtime this workflow is registered with."""
        return self._runtime

    def _switch_runtime(self, new_runtime: Runtime) -> None:
        if new_runtime is self._runtime:
            return
        if self._runtime_locked:
            raise RuntimeError(
                "Cannot reassign runtime after workflow has been launched"
            )
        old = self._runtime
        old.untrack_workflow(self)
        self._runtime = new_runtime
        new_runtime.track_workflow(self)

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
        if self._disable_validation and not force:
            return False
        stale = self._validated_version != self.__class__._step_functions_version
        if not force and not stale and self._validation_result is not None:
            return self._validation_result

        # Inline import: ``representation`` transitively imports ``Workflow``.
        from .representation.validate import (
            _validate_resource_configs,
            _validate_resources,
            _validate_workflow,
        )

        step_configs = self._step_configs()
        result = _validate_workflow(
            step_configs, self.__class__.__name__, self._skip_graph_checks
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

        self._validation_result = result.uses_hitl
        self._validated_version = self.__class__._step_functions_version
        return self._validation_result
