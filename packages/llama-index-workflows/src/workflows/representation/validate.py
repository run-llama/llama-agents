# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from workflows._event_matching import is_subclass, step_accepts_type, type_matches
from workflows._stream_levels import (
    event_types,
    same_level_types,
    stream_level_types_by_producer,
)
from workflows.decorators import CatchErrorHandler, StepConfig, WorkflowGraphCheck
from workflows.errors import WorkflowConfigurationError, WorkflowValidationError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StepFailedEvent,
    StopEvent,
)
from workflows.resource import ResourceDescriptor, ResourceManager, _ResourceConfig

# Graph nodes: step names (str) for steps, event classes (type) for events.
GraphNode = str | type


@dataclass
class StepGraph:
    """Lightweight adjacency-list representation of a workflow's step/event graph.

    Nodes are step names (``str``) for steps and event classes (``type``) for
    events.  An edge from an event node to a step node means the step accepts
    that event; an edge from a step node to an event node means the step returns
    that event type.
    """

    outgoing: dict[GraphNode, list[GraphNode]] = field(default_factory=dict)
    """Adjacency list: node -> list of successor nodes."""

    event_types: set[type] = field(default_factory=set)
    """All event classes seen in the graph."""

    step_names: set[str] = field(default_factory=set)
    """Names of all steps in the graph."""

    forward_reachable: set[GraphNode] = field(default_factory=set)
    """Nodes reachable from input seeds (StartEvent, HumanResponseEvent subclasses)."""

    reverse_reachable: set[GraphNode] = field(default_factory=set)
    """Nodes that can reach an output event (StopEvent, InputRequiredEvent) via reverse traversal."""


def build_step_graph(
    steps: dict[str, StepConfig],
    start_event_class: type,
    catch_error_steps: list[str] | None = None,
) -> StepGraph:
    """Build a StepGraph from step configs and a start event class.

    Constructs the adjacency list, then computes forward reachability from input
    events (StartEvent + HumanResponseEvent subclasses + any catch_error handler
    step names) and reverse reachability from output events (StopEvent +
    InputRequiredEvent).
    """
    outgoing: dict[GraphNode, list[GraphNode]] = {}
    event_types: set[type] = {start_event_class} if steps else set()
    step_names: set[str] = set()

    for name, cfg in steps.items():
        step_names.add(name)
        for rt in cfg.return_types:
            if rt is type(None):
                continue
            event_types.add(rt)
            outgoing.setdefault(name, []).append(rt)
        for ev in cfg.accepted_events:
            event_types.add(ev)

    # Build event→step edges. A step consumes an event when its type matches one
    # of the step's accepted events (subclass-aware when the step opted in).
    # StopEvent is excluded from subclass expansion: a returned StopEvent
    # terminates the run instead of routing, so a broad accepted base (e.g.
    # ``Event``) must not produce a StopEvent→step edge.
    for ev_type in event_types:
        for name, cfg in steps.items():
            allow_subclasses = cfg.accept_event_subclasses and not is_subclass(
                ev_type, StopEvent
            )
            if step_accepts_type(
                ev_type,
                cfg.accepted_events,
                allow_subclasses=allow_subclasses,
            ):
                outgoing.setdefault(ev_type, []).append(name)

    # Forward DFS from StartEvent + HumanResponseEvent subclasses +
    # catch_error handler step names (their sub-graphs are reachable via
    # runtime routing of StepFailedEvent, not via any event in the graph).
    seeds: list[GraphNode] = [start_event_class]
    for ev_type in event_types:
        if is_subclass(ev_type, HumanResponseEvent) and ev_type not in seeds:
            seeds.append(ev_type)
    for handler_name in catch_error_steps or []:
        if handler_name not in seeds:
            seeds.append(handler_name)

    forward_reachable = _dfs(seeds, outgoing)

    # Reverse DFS from output events
    incoming: dict[GraphNode, list[GraphNode]] = {}
    for source, targets in outgoing.items():
        for target in targets:
            incoming.setdefault(target, []).append(source)

    output_seeds: list[GraphNode] = [
        ev_type
        for ev_type in event_types
        if is_subclass(ev_type, (StopEvent, InputRequiredEvent))
    ]
    reverse_reachable = _dfs(output_seeds, incoming)

    return StepGraph(
        outgoing=outgoing,
        event_types=event_types,
        step_names=step_names,
        forward_reachable=forward_reachable,
        reverse_reachable=reverse_reachable,
    )


def _dfs(
    seeds: list[GraphNode], adjacency: dict[GraphNode, list[GraphNode]]
) -> set[GraphNode]:
    """Depth-first search returning all reachable nodes from seeds."""
    visited: set[GraphNode] = set()
    stack = list(seeds)
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for target in adjacency.get(node, []):
            if target not in visited:
                stack.append(target)
    return visited


@dataclass
class GraphValidationError:
    """A single graph validation error."""

    check: WorkflowGraphCheck
    message: str
    hint: str
    step_names: list[str] = field(default_factory=list)


def validate_graph(
    steps: dict[str, StepConfig],
    start_event_class: type,
    skip_checks: set[WorkflowGraphCheck] | None = None,
    catch_error_steps: list[str] | None = None,
) -> list[GraphValidationError]:
    """Validate the graph structure of a workflow, accumulating all errors.

    Builds a ``StepGraph`` from step configs and runs three checks:
    1. Reachability: all steps are reachable from input events
    2. Terminal events: events with no consumer must be output events
    3. Dead ends: every step producing events must reach an output event

    Args:
        steps: Mapping of step name to StepConfig.
        start_event_class: The StartEvent subclass for this workflow.
        skip_checks: Workflow-level checks to skip entirely.
        catch_error_steps: Names of catch_error handler steps; their sub-graphs
            are forward-reachable via runtime routing rather than by
            connection to an event in the main graph.

    Returns:
        List of GraphValidationError (empty if the graph is valid).
    """
    skip_checks = skip_checks or set()
    errors: list[GraphValidationError] = []

    graph = build_step_graph(steps, start_event_class, catch_error_steps)

    # Check 1: Reachability
    if "reachability" not in skip_checks:
        step_skip = {
            name
            for name, cfg in steps.items()
            if "reachability" in cfg.skip_graph_checks
        }
        unreachable_steps = sorted(
            name
            for name in graph.step_names - step_skip
            if name not in graph.forward_reachable
        )
        if unreachable_steps:
            names = ", ".join(unreachable_steps)
            errors.append(
                GraphValidationError(
                    check="reachability",
                    message=f"Unreachable steps: {names}",
                    hint="Steps must be reachable from StartEvent or HumanResponseEvent.",
                    step_names=unreachable_steps,
                )
            )

    # Check 2: Terminal events — events with no step consumer must be output events
    if "terminal_event" not in skip_checks:
        dangling: list[type] = []
        for ev_type in graph.event_types:
            targets = graph.outgoing.get(ev_type, [])
            if any(t in graph.step_names for t in targets):
                continue
            if is_subclass(ev_type, (StopEvent, InputRequiredEvent)):
                continue
            dangling.append(ev_type)
        if dangling:
            names = ", ".join(sorted(t.__name__ for t in dangling))
            errors.append(
                GraphValidationError(
                    check="terminal_event",
                    message=f"Events produced but never consumed: {names}",
                    hint="Only StopEvent and InputRequiredEvent may be terminal.",
                    step_names=[],
                )
            )

    # Check 3: Dead-end detection
    if "dead_end" not in skip_checks:
        steps_producing_events = {
            s
            for s in graph.step_names
            if any(isinstance(t, type) for t in graph.outgoing.get(s, []))
        }

        step_skip = {
            name for name, cfg in steps.items() if "dead_end" in cfg.skip_graph_checks
        }
        dead_end_steps = sorted(
            name
            for name in steps_producing_events - step_skip
            if name not in graph.reverse_reachable
        )
        if dead_end_steps:
            names = ", ".join(dead_end_steps)
            errors.append(
                GraphValidationError(
                    check="dead_end",
                    message=f"Dead-end steps: {names}",
                    hint="Steps must have a path to StopEvent or InputRequiredEvent.",
                    step_names=dead_end_steps,
                )
            )

    return errors


def validate_catch_error_handlers(
    handlers: Iterable[CatchErrorHandler],
    step_names: set[str],
) -> list[str]:
    """Validate structural invariants of ``@catch_error`` handlers.

    Returns a list of error messages; empty when the handler set is valid.
    Callers are responsible for raising.
    """
    errors: list[str] = []

    handlers = list(handlers)
    wildcard_handlers = [h for h in handlers if h.for_steps is None]
    if len(wildcard_handlers) > 1:
        names = ", ".join(sorted(h.step_name for h in wildcard_handlers))
        errors.append(
            f"Only one wildcard @catch_error handler is allowed per workflow, "
            f"found {len(wildcard_handlers)}: {names}"
        )

    handler_step_names = {h.step_name for h in handlers}
    claim_owner: dict[str, str] = {}
    for handler in handlers:
        if handler.for_steps is None:
            continue
        for target in handler.for_steps:
            if target not in step_names:
                errors.append(
                    f"@catch_error handler '{handler.step_name}' lists "
                    f"unknown step '{target}' in for_steps."
                )
                continue
            if target == handler.step_name or target in handler_step_names:
                errors.append(
                    f"@catch_error handler '{handler.step_name}' cannot "
                    f"cover another handler step '{target}'."
                )
                continue
            if target in claim_owner:
                errors.append(
                    f"Step '{target}' is claimed by two @catch_error "
                    f"handlers: '{claim_owner[target]}' and '{handler.step_name}'."
                )
                continue
            claim_owner[target] = handler.step_name

    return errors


def _ensure_start_event_class(
    steps: dict[str, StepConfig], workflow_cls_name: str
) -> type[StartEvent]:
    """Infer and validate the single StartEvent subclass accepted by a workflow.

    Inspects every step's accepted events and returns the unique StartEvent
    subclass. Raises ``WorkflowConfigurationError`` if zero or more than one
    are found.
    """
    start_events_found: set[type[StartEvent]] = set()
    for cfg in steps.values():
        for event_type in cfg.accepted_events:
            if is_subclass(event_type, StartEvent):
                start_events_found.add(event_type)

    num_found = len(start_events_found)
    if num_found == 0:
        raise WorkflowConfigurationError(
            "At least one Event of type StartEvent must be received by any step. "
            f"(Workflow '{workflow_cls_name}' has no @step that accepts StartEvent.)"
        )
    if num_found > 1:
        raise WorkflowConfigurationError(
            f"Only one type of StartEvent is allowed per workflow, found {num_found}: "
            f"{start_events_found} in workflow '{workflow_cls_name}'."
        )
    return start_events_found.pop()


def _ensure_stop_event_class(
    steps: dict[str, StepConfig], workflow_cls_name: str
) -> type[StopEvent]:
    """Infer and validate the single StopEvent subclass produced by a workflow.

    Inspects every step's return types and returns the unique StopEvent
    subclass. Raises ``WorkflowConfigurationError`` if zero or more than one
    are found.
    """
    stop_events_found: set[type[StopEvent]] = set()
    for cfg in steps.values():
        for event_type in cfg.return_types:
            if is_subclass(event_type, StopEvent):
                stop_events_found.add(event_type)

    num_found = len(stop_events_found)
    if num_found == 0:
        raise WorkflowConfigurationError(
            "At least one Event of type StopEvent must be returned by any step. "
            f"(Workflow '{workflow_cls_name}' has no @step that returns StopEvent.)"
        )
    if num_found > 1:
        raise WorkflowConfigurationError(
            f"Only one type of StopEvent is allowed per workflow, found {num_found}: "
            f"{stop_events_found} in workflow '{workflow_cls_name}'."
        )
    return stop_events_found.pop()


def _collect_events(steps: dict[str, StepConfig]) -> list[type[Event]]:
    """Return every ``Event`` subclass touched by the workflow's steps.

    Skips the runtime-injected ``_done`` step so only user-facing events are
    reported. Walks both accepted and returned types of each step.
    """
    events_found: set[type[Event]] = set()
    for cfg in steps.values():
        for event_type in cfg.return_types:
            if is_subclass(event_type, Event):
                events_found.add(event_type)
        for event_type in cfg.accepted_events:
            if is_subclass(event_type, Event):
                events_found.add(event_type)
    return list(events_found)


def _collect_catch_error_handlers(
    steps: dict[str, StepConfig],
) -> tuple[dict[str, CatchErrorHandler], dict[str, str]]:
    """Discover ``@catch_error`` handlers and build the step->handler routing table.

    Validates the handler set (via :func:`validate_catch_error_handlers`) and
    each handler's ``max_recoveries``; raises ``WorkflowValidationError`` on
    any problem.

    Returns ``(catch_error_handlers, handler_for_step)`` where
    ``catch_error_handlers`` maps handler step name to its descriptor and
    ``handler_for_step`` maps each covered step name to the handler that owns
    it (scoped claims first, then the wildcard fills).
    """
    all_step_names = set(steps.keys())
    handlers: list[CatchErrorHandler] = []
    for name, cfg in steps.items():
        if cfg.role != "catch_error":
            continue
        max_recoveries = cfg.catch_error_max_recoveries
        if not isinstance(max_recoveries, int) or max_recoveries < 1:
            raise WorkflowValidationError(
                f"@catch_error handler '{name}' has max_recoveries="
                f"{max_recoveries!r}; must be an integer >= 1."
            )
        handlers.append(
            CatchErrorHandler(
                step_name=name,
                for_steps=(
                    list(cfg.catch_error_for_steps)
                    if cfg.catch_error_for_steps is not None
                    else None
                ),
                max_recoveries=max_recoveries,
            )
        )

    handler_errors = validate_catch_error_handlers(handlers, all_step_names)
    if handler_errors:
        raise WorkflowValidationError("\n".join(handler_errors))

    handler_step_names = {h.step_name for h in handlers}
    handler_for_step: dict[str, str] = {}
    for handler in handlers:
        if handler.for_steps is None:
            continue
        for target in handler.for_steps:
            handler_for_step[target] = handler.step_name

    wildcards = [h for h in handlers if h.for_steps is None]
    wildcard = wildcards[0] if wildcards else None
    if wildcard is not None:
        for step_name in all_step_names:
            if step_name in handler_step_names:
                continue
            if step_name in handler_for_step:
                continue
            handler_for_step[step_name] = wildcard.step_name

    return {h.step_name: h for h in handlers}, handler_for_step


def _validate_event_connectivity(
    steps: dict[str, StepConfig],
    start_event_class: type[StartEvent],
) -> bool:
    """Validate event production/consumption across the step graph.

    Checks that:
    - No user step accepts ``StopEvent``.
    - Every consumed event is either produced or crosses the workflow
      boundary (``InputRequiredEvent``/``HumanResponseEvent``/``StopEvent``/
      ``StepFailedEvent``).
    - Every produced event is consumed, except for
      ``InputRequiredEvent``/``HumanResponseEvent``/``StopEvent`` subclasses.

    Returns ``True`` if the workflow uses human-in-the-loop
    (``InputRequiredEvent`` produced or ``HumanResponseEvent`` consumed).
    Raises ``WorkflowValidationError`` on any violation.
    """
    produced_events: set[type] = {start_event_class}
    consumed_events: set[type] = set()
    steps_accepting_stop_event: list[str] = []

    for name, cfg in steps.items():
        for event_type in cfg.accepted_events:
            if is_subclass(event_type, StopEvent):
                steps_accepting_stop_event.append(name)
                break
        for event_type in cfg.accepted_events:
            consumed_events.add(event_type)
        for event_type in cfg.return_types:
            if event_type is type(None):
                continue
            produced_events.add(event_type)

    if steps_accepting_stop_event:
        step_names = "', '".join(steps_accepting_stop_event)
        plural = "" if len(steps_accepting_stop_event) == 1 else "s"
        raise WorkflowValidationError(
            f"Step{plural} '{step_names}' cannot accept StopEvent. "
            "StopEvent signals the end of the workflow. "
            "Use a different Event type instead."
        )

    # An accepted event is satisfied when some produced event matches it (under
    # the consuming step's matching mode); otherwise it is consumed but never
    # produced.
    boundary_in = (InputRequiredEvent, HumanResponseEvent, StopEvent, StepFailedEvent)
    unconsumed_events = set()
    for cfg in steps.values():
        for event_type in cfg.accepted_events:
            if is_subclass(event_type, boundary_in):
                continue
            satisfied = any(
                type_matches(
                    p, event_type, allow_subclasses=cfg.accept_event_subclasses
                )
                for p in produced_events
            )
            if not satisfied:
                unconsumed_events.add(event_type)

    if unconsumed_events:
        names = ", ".join(ev.__name__ for ev in unconsumed_events)
        raise WorkflowValidationError(
            f"The following events are consumed but never produced: {names}"
        )

    # A produced event is unused when no step accepts it (under that step's
    # matching mode). Boundary-out events may legitimately leave the workflow.
    boundary_out = (InputRequiredEvent, HumanResponseEvent, StopEvent)
    unused_events = set()
    for x in produced_events:
        if is_subclass(x, boundary_out):
            continue
        consumed = any(
            step_accepts_type(
                x, cfg.accepted_events, allow_subclasses=cfg.accept_event_subclasses
            )
            for cfg in steps.values()
        )
        if not consumed:
            unused_events.add(x)

    if unused_events:
        names = ", ".join(ev.__name__ for ev in unused_events)
        raise WorkflowValidationError(
            f"The following events are produced but never consumed: {names}"
        )

    return any(is_subclass(p, InputRequiredEvent) for p in produced_events) or any(
        is_subclass(c, HumanResponseEvent) for c in consumed_events
    )


@dataclass
class _ResourceValidationContext:
    """Tracks context for resource validation to provide clear error messages."""

    resource: ResourceDescriptor
    step_name: str
    param_name: str
    resource_chain: list[str] = field(default_factory=list)

    def format_location(self) -> str:
        if len(self.resource_chain) > 1:
            chain_str = " -> ".join(self.resource_chain)
            return (
                f"step '{self.step_name}', parameter '{self.param_name}' ({chain_str})"
            )
        return f"step '{self.step_name}', parameter '{self.param_name}'"

    def with_dependency(self, dep: ResourceDescriptor) -> _ResourceValidationContext:
        return _ResourceValidationContext(
            resource=dep,
            step_name=self.step_name,
            param_name=self.param_name,
            resource_chain=[*self.resource_chain, dep.name],
        )


def _validate_resource_configs(steps: dict[str, StepConfig]) -> list[str]:
    """Validate every resource config (and nested configs) by loading it.

    Returns a list of human-readable error messages; empty if all configs load
    cleanly. Callers decide whether to raise.
    """
    errors: list[str] = []
    seen: set[str] = set()

    stack: list[_ResourceValidationContext] = []
    for step_name, cfg in steps.items():
        for res_def in cfg.resources:
            res_def.resource.set_type_annotation(res_def.type_annotation)
            stack.append(
                _ResourceValidationContext(
                    resource=res_def.resource,
                    step_name=step_name,
                    param_name=res_def.name,
                    resource_chain=[res_def.resource.name],
                )
            )

    while stack:
        ctx = stack.pop()
        if ctx.resource.name in seen:
            continue
        seen.add(ctx.resource.name)

        for _dep_param, dep, type_ann in ctx.resource.get_dependencies():
            dep.set_type_annotation(type_ann)
            stack.append(ctx.with_dependency(dep))

        if isinstance(ctx.resource, _ResourceConfig):
            try:
                ctx.resource.call()
            except Exception as e:
                errors.append(f"In {ctx.format_location()}: {e}")

    return errors


async def _validate_resources(
    steps: dict[str, StepConfig], resource_manager: ResourceManager
) -> list[str]:
    """Resolve every resource via ``resource_manager``.

    Surfaces circular dependencies and factory-time failures. Returns a list of
    error messages; empty if all resources resolve.
    """
    errors: list[str] = []
    for step_name, cfg in steps.items():
        for res_def in cfg.resources:
            res_def.resource.set_type_annotation(res_def.type_annotation)
            try:
                await resource_manager.get(res_def.resource)
            except Exception as e:
                errors.append(f"In step '{step_name}', parameter '{res_def.name}': {e}")
    return errors


@dataclass
class _WorkflowValidationResult:
    """Derived workflow state produced by :func:`_validate_workflow`."""

    start_event_class: type[StartEvent]
    stop_event_class: type[StopEvent]
    catch_error_handlers: dict[str, CatchErrorHandler]
    handler_for_step: dict[str, str]
    uses_hitl: bool


def _validate_collection_bindings(steps: dict[str, StepConfig]) -> None:
    """Require list[E] fan-in to bind to a static returned-list producer."""

    level_types_by_producer = stream_level_types_by_producer(steps)
    errors: list[str] = []
    for step_name, cfg in steps.items():
        if cfg.collection_param is None:
            continue
        param_name, element_types = cfg.collection_param
        missing = [
            event_type
            for event_type in event_types(element_types)
            if not any(
                type_matches(
                    produced,
                    event_type,
                    allow_subclasses=cfg.accept_event_subclasses,
                )
                for level_types in level_types_by_producer.values()
                for produced in level_types
            )
        ]
        if missing:
            names = ", ".join(sorted(t.__name__ for t in missing))
            errors.append(
                f"Step '{step_name}' parameter '{param_name}' collects "
                f"list[{names}], but no returned-list producer creates a stream "
                "for those event types. list[E] fan-in only consumes events "
                "from steps annotated as returning list[E]; use ctx.collect_events "
                "for unstreamed ctx.send_event flows."
            )
    if errors:
        raise WorkflowValidationError("\n".join(errors))


def _validate_multi_slot_levels(
    steps: dict[str, StepConfig],
    start_event_class: type[StartEvent],
) -> None:
    """Reject multi-slot joins whose slots cannot fill at one stream level.

    A multi-slot step fires when one event of each declared type has arrived,
    and its output continues at the scope of its inputs. That is only
    well-defined when every slot type is producible at a common stream level
    (the top level, or one returned-list producer's stream). Mixing levels
    would strand work items: a slot filled at one level waits forever for a
    slot that only fills at another.

    Rejected shape:

        fan_out(StartEvent) -> list[A]      # A is inside fan_out's stream
        emit_b(StartEvent) -> B             # B is top-level
        join(a: A, b: B) -> C               # no single level contains A and B
    """
    multi_slot = {
        name: cfg.collect_params
        for name, cfg in steps.items()
        if cfg.collect_params is not None
    }
    if not multi_slot:
        return

    external_seeds = [
        event_type
        for cfg in steps.values()
        for event_type in cfg.accepted_events
        if isinstance(event_type, type) and issubclass(event_type, HumanResponseEvent)
    ]
    levels: dict[str, set[type[Event]]] = {
        "the top level": same_level_types(
            steps, [start_event_class, *external_seeds], frozenset()
        )
    }
    for producer, level_types in stream_level_types_by_producer(steps).items():
        levels[f"the stream of step '{producer}'"] = level_types

    errors: list[str] = []
    for step_name, slots in multi_slot.items():
        slot_types = {slot_type for _, slot_type in slots or []}
        if any(slot_types <= level_types for level_types in levels.values()):
            continue
        slot_levels = "; ".join(
            f"'{param_name}' ({slot_type.__name__}) is produced at "
            + (
                ", ".join(
                    sorted(
                        level_name
                        for level_name, level_types in levels.items()
                        if slot_type in level_types
                    )
                )
                or "no level"
            )
            for param_name, slot_type in slots or []
        )
        errors.append(
            f"Step '{step_name}' joins event parameters that are never produced "
            f"at a common stream level: {slot_levels}. A multi-slot join only "
            "fires when all of its inputs originate at the same level. To gate "
            "in-stream work on input from another level, use ctx.wait_for_event "
            "inside the producing step instead."
        )
    if errors:
        raise WorkflowValidationError("\n".join(errors))


def _validate_workflow(
    steps: dict[str, StepConfig],
    workflow_cls_name: str,
    skip_graph_checks: set[WorkflowGraphCheck],
) -> _WorkflowValidationResult:
    """Run every structural check on a workflow's step set.

    Orders checks so the most actionable errors surface first (missing steps,
    then StartEvent, then StopEvent, then event connectivity, then catch_error
    handlers, then graph reachability/dead-ends).

    Raises ``WorkflowConfigurationError`` or ``WorkflowValidationError`` on any
    violation. Resource validation is handled separately via
    :func:`_validate_resource_configs` and :func:`_validate_resources`.
    """
    if not steps:
        raise WorkflowConfigurationError(
            f"Workflow '{workflow_cls_name}' has no configured steps. "
            "Did you forget to annotate methods with @step or to register "
            "free-function steps via @step(workflow=...)?"
        )

    start_event_class = _ensure_start_event_class(steps, workflow_cls_name)
    stop_event_class = _ensure_stop_event_class(steps, workflow_cls_name)

    _validate_collection_bindings(steps)
    _validate_multi_slot_levels(steps, start_event_class)

    uses_hitl = _validate_event_connectivity(steps, start_event_class)

    catch_error_handlers, handler_for_step = _collect_catch_error_handlers(steps)

    graph_errors = validate_graph(
        steps=steps,
        start_event_class=start_event_class,
        skip_checks=skip_graph_checks,
        catch_error_steps=list(catch_error_handlers.keys()),
    )
    if graph_errors:
        detail = "\n".join(
            f"  - [{e.check}] {e.message}\n    {e.hint}" for e in graph_errors
        )
        raise WorkflowValidationError(f"Graph validation failed:\n{detail}")

    return _WorkflowValidationResult(
        start_event_class=start_event_class,
        stop_event_class=stop_event_class,
        catch_error_handlers=catch_error_handlers,
        handler_for_step=handler_for_step,
        uses_hitl=uses_hitl,
    )
