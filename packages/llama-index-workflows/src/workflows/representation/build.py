from __future__ import annotations

import hashlib
import inspect
from typing import Any

from pydantic import BaseModel

from workflows import Workflow
from workflows._event_matching import is_subclass
from workflows.decorators import StepFunction
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StopEvent,
)
from workflows.representation.types import (
    WorkflowEventNode,
    WorkflowExternalNode,
    WorkflowGraph,
    WorkflowGraphEdge,
    WorkflowGraphNode,
    WorkflowResourceConfigNode,
    WorkflowResourceNode,
    WorkflowStepNode,
)
from workflows.resource import (
    ResourceDefinition,
    ResourceDescriptor,
    _get_resource_config_data,
    _Resource,
    _ResourceConfig,
)


def _get_type_name(type_annotation: type | None) -> str | None:
    """Extract a readable type name from a type annotation."""
    if type_annotation is None:
        return None
    if hasattr(type_annotation, "__name__"):
        return type_annotation.__name__
    return str(type_annotation)


def _event_schema(type_annotation: Any) -> dict[str, Any] | None:
    """JSON schema for a Pydantic event type, or ``None`` for non-model annotations."""
    if isinstance(type_annotation, type) and issubclass(type_annotation, BaseModel):
        return type_annotation.model_json_schema()
    return None


def _get_resource_identity(resource: ResourceDescriptor) -> int:
    """Get a unique identifier for resource deduplication.

    For _Resource, uses the factory function identity.
    For _ResourceConfig, uses (config_file, path_selector) hash.
    """
    if isinstance(resource, _Resource):
        return id(resource._factory)
    if isinstance(resource, _ResourceConfig):
        # Use hash of config_file + path_selector for deduplication
        hash_input = f"{resource.config_file}:{resource.path_selector or ''}"
        return hash(hash_input)
    return id(resource)


def _get_event_type_chain(cls: type) -> list[str]:
    """Get the event type inheritance chain including the class itself.

    Returns a list starting with the class name, followed by parent Event
    subclasses up to (but not including) Event itself.
    """
    if not is_subclass(cls, Event):
        name = _get_type_name(cls)
        return [name] if name else []

    names: list[str] = [cls.__name__]
    for parent in cls.mro()[1:]:
        if parent is Event:
            break
        if is_subclass(parent, Event):
            names.append(parent.__name__)
    return names


def _create_resource_config_node(
    resource_config: _ResourceConfig,
    type_annotation: type | None,
) -> WorkflowResourceConfigNode:
    """Create a WorkflowResourceConfigNode from a _ResourceConfig."""
    # Compute unique hash for deduplication based on config file and path selector
    hash_input = f"{resource_config.config_file}:{resource_config.path_selector or ''}"
    unique_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:12]

    node_id = f"resource_config_{unique_hash}"
    type_name = _get_type_name(type_annotation)
    # Prefer explicit label, then type name, then config file path
    label = resource_config.label or type_name or resource_config.config_file

    # Extract JSON schema if type is a BaseModel
    config_schema: dict[str, Any] | None = None
    if (
        type_annotation is not None
        and isinstance(type_annotation, type)
        and issubclass(type_annotation, BaseModel)
    ):
        model_cls: type[BaseModel] = type_annotation
        config_schema = model_cls.model_json_schema()

    # Read config value using existing infrastructure
    config_value = _get_resource_config_data(
        resource_config.config_file, resource_config.path_selector
    )

    return WorkflowResourceConfigNode(
        id=node_id,
        label=label,
        type_name=type_name,
        config_file=resource_config.config_file,
        path_selector=resource_config.path_selector,
        config_schema=config_schema,
        config_value=config_value,
        description=resource_config.description,
    )


def _create_resource_node(resource_def: ResourceDefinition) -> WorkflowResourceNode:
    """Create a WorkflowResourceNode from a ResourceDefinition.

    Extracts metadata (source file, line number, docstring) lazily here
    rather than at Resource creation time for performance.
    """
    resource = resource_def.resource
    type_name = _get_type_name(resource_def.type_annotation)

    # Extract source metadata lazily - only available for _Resource with factory
    source_file: str | None = None
    source_line: int | None = None
    resource_description: str | None = None

    if isinstance(resource, _Resource):
        factory = resource._factory
        source_file = inspect.getfile(factory)  # type: ignore[arg-type]
        _, source_line = inspect.getsourcelines(factory)  # type: ignore[arg-type]
        resource_description = inspect.getdoc(factory)

    # Compute unique hash for deduplication
    hash_input = f"{resource.name}:{source_file or 'unknown'}"
    unique_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:12]

    # Label: prefer type_name, then getter_name, then id
    node_id = f"resource_{unique_hash}"
    label = type_name or resource.name or node_id

    return WorkflowResourceNode(
        id=node_id,
        label=label,
        type_name=type_name,
        getter_name=resource.name,
        source_file=source_file,
        source_line=source_line,
        description=resource_description,
    )


def get_workflow_representation(workflow: Workflow | type[Workflow]) -> WorkflowGraph:
    """Build a graph representation of a workflow's structure.

    Extracts the workflow's steps, events, and resources into a WorkflowGraph
    that can be used for visualization or analysis.

    Args:
        workflow: A workflow instance or workflow class to build a representation for.

    Returns:
        A WorkflowGraph containing nodes for steps, events, resources,
        and external interactions, with edges showing the data flow.
    """
    # Get workflow steps
    workflow_cls = workflow if isinstance(workflow, type) else type(workflow)
    steps: dict[str, StepFunction] = workflow_cls._get_steps_from_class()

    # Map each produced event type (by class name) to the steps that declare it
    # in their return signature. Collection returns (list[E]) are already
    # flattened into return_types upstream.
    produced_by_map: dict[str, list[str]] = {}
    for step_name, step_func in steps.items():
        for return_type in step_func._step_config.return_types:
            if return_type is type(None):
                continue
            return_type_name = _get_type_name(return_type)
            if return_type_name is None:
                continue
            producers = produced_by_map.setdefault(return_type_name, [])
            if step_name not in producers:
                producers.append(step_name)

    nodes: list[WorkflowGraphNode] = []
    edges: list[WorkflowGraphEdge] = []
    added_nodes: set[str] = set()  # Track added node IDs to avoid duplicates
    # Track resource nodes by identity (factory id for _Resource)
    added_resource_nodes: dict[int, WorkflowResourceNode] = {}
    # Track resource config nodes by config_file>path_selector
    added_resource_config_nodes: dict[str, WorkflowResourceConfigNode] = {}
    # Track descriptor nodes by identity for step edges (_Resource or _ResourceConfig)
    added_descriptor_nodes: dict[
        int, WorkflowResourceNode | WorkflowResourceConfigNode
    ] = {}
    # Track which resources have had their dependencies expanded
    expanded_resources: set[int] = set()
    expanding_resources: set[int] = set()
    # Track resource dependency edges to avoid duplicates
    resource_edge_keys: set[tuple[str, str, str | None]] = set()

    def _ensure_resource_config_node(
        resource_config: _ResourceConfig,
        type_annotation: type | None,
    ) -> WorkflowResourceConfigNode:
        selector = resource_config.path_selector
        config_key = resource_config.config_file + (
            (">" + selector) if selector else ""
        )
        if config_key in added_resource_config_nodes:
            return added_resource_config_nodes[config_key]
        node = _create_resource_config_node(resource_config, type_annotation)
        nodes.append(node)
        added_resource_config_nodes[config_key] = node
        return node

    def _ensure_resource_node(
        resource: _Resource,
        type_annotation: type | None,
        param_name: str,
    ) -> WorkflowResourceNode:
        resource_id = _get_resource_identity(resource)
        if resource_id in added_resource_nodes:
            return added_resource_nodes[resource_id]
        node = _create_resource_node(
            ResourceDefinition(
                name=param_name,
                resource=resource,
                type_annotation=type_annotation,
            )
        )
        nodes.append(node)
        added_resource_nodes[resource_id] = node
        return node

    def _track_resource_edge(
        source: str,
        target: str,
        label: str | None,
    ) -> None:
        key = (source, target, label)
        if key in resource_edge_keys:
            return
        resource_edge_keys.add(key)
        edges.append(WorkflowGraphEdge(source=source, target=target, label=label))

    def _ensure_descriptor_node(
        descriptor: ResourceDescriptor,
        type_annotation: type | None,
        param_name: str,
    ) -> WorkflowResourceNode | WorkflowResourceConfigNode:
        descriptor_id = _get_resource_identity(descriptor)
        if isinstance(descriptor, _ResourceConfig):
            node = _ensure_resource_config_node(descriptor, type_annotation)
            added_descriptor_nodes[descriptor_id] = node
            return node
        if isinstance(descriptor, _Resource):
            node = _ensure_resource_node(descriptor, type_annotation, param_name)
            added_descriptor_nodes[descriptor_id] = node
            resource_id = _get_resource_identity(descriptor)
            if resource_id in expanded_resources:
                return node
            if resource_id in expanding_resources:
                return node
            expanding_resources.add(resource_id)
            for dep_name, dep_descriptor, dep_type in descriptor.get_dependencies():
                dep_node = _ensure_descriptor_node(dep_descriptor, dep_type, dep_name)
                _track_resource_edge(source=node.id, target=dep_node.id, label=dep_name)
            expanding_resources.remove(resource_id)
            expanded_resources.add(resource_id)
            return node
        raise TypeError(
            f"Unsupported resource descriptor type: {type(descriptor).__name__}"
        )

    # Only one kind of `StopEvent` is allowed in a `Workflow`.
    # Assuming that `Workflow` is validated before drawing, it's enough to find the first one.
    current_stop_event = None
    for step_func in steps.values():
        for return_type in step_func._step_config.return_types:
            if is_subclass(return_type, StopEvent):
                current_stop_event = return_type
                break
        if current_stop_event:
            break

    # First pass: Add all nodes
    graph_event_types: set[type] = set()
    for step_name, step_func in steps.items():
        step_config = step_func._step_config

        # Add step node
        if step_name not in added_nodes:
            step_description = inspect.getdoc(step_func)
            nodes.append(
                WorkflowStepNode(
                    id=step_name, label=step_name, description=step_description
                )
            )
            added_nodes.add(step_name)

        # Add event nodes for accepted events
        for event_type in step_config.accepted_events:
            if event_type == StopEvent and event_type != current_stop_event:
                continue

            graph_event_types.add(event_type)
            event_type_name = _get_type_name(event_type) or "UnknownType"
            if event_type_name not in added_nodes:
                nodes.append(
                    WorkflowEventNode(
                        id=event_type_name,
                        label=event_type_name,
                        event_type=event_type_name,
                        event_types=_get_event_type_chain(event_type),
                        event_schema=_event_schema(event_type),
                        produced_by=list(produced_by_map.get(event_type_name, [])),
                    )
                )
                added_nodes.add(event_type_name)

        # Add event nodes for return types
        for return_type in step_config.return_types:
            if return_type is type(None):
                continue

            graph_event_types.add(return_type)
            return_type_name = _get_type_name(return_type) or "UnknownType"
            if return_type_name not in added_nodes:
                nodes.append(
                    WorkflowEventNode(
                        id=return_type_name,
                        label=return_type_name,
                        event_type=return_type_name,
                        event_types=_get_event_type_chain(return_type),
                        event_schema=_event_schema(return_type),
                        produced_by=list(produced_by_map.get(return_type_name, [])),
                    )
                )
                added_nodes.add(return_type_name)

            # Add external_step node when InputRequiredEvent is found
            if (
                is_subclass(return_type, InputRequiredEvent)
                and "external_step" not in added_nodes
            ):
                nodes.append(
                    WorkflowExternalNode(id="external_step", label="external_step")
                )
                added_nodes.add("external_step")

        # Add resource nodes (deduplicated by resource identity)
        for resource_def in step_config.resources:
            _ensure_descriptor_node(
                resource_def.resource,
                resource_def.type_annotation,
                resource_def.name,
            )

    # Second pass: Add edges
    for step_name, step_func in steps.items():
        step_config = step_func._step_config

        # Edges from steps to return types
        for return_type in step_config.return_types:
            if return_type is not type(None):
                return_type_name = _get_type_name(return_type) or "UnknownType"
                edges.append(
                    WorkflowGraphEdge(source=step_name, target=return_type_name)
                )

            if is_subclass(return_type, InputRequiredEvent):
                return_type_name = _get_type_name(return_type) or "UnknownType"
                edges.append(
                    WorkflowGraphEdge(source=return_type_name, target="external_step")
                )

        # Edges from events to steps. With subclass routing a single accepted
        # base maps to every produced subclass present in the graph; otherwise
        # each accepted event maps to itself. De-duplicate so the fan-out never
        # emits the same source twice for one step. StopEvent is excluded from
        # the fan-out: a returned StopEvent terminates the run instead of
        # routing, so a broad accepted base (e.g. ``Event``) must not draw a
        # StopEvent→step edge.
        matching_events: list[Any] = []
        seen_sources: set[Any] = set()
        for event_type in step_config.accepted_events:
            if step_config.accept_event_subclasses and isinstance(event_type, type):
                candidates = [
                    ev
                    for ev in graph_event_types
                    if is_subclass(ev, event_type)
                    and (ev is event_type or not is_subclass(ev, StopEvent))
                ]
            else:
                candidates = [event_type]
            for ev in candidates:
                if ev not in seen_sources:
                    seen_sources.add(ev)
                    matching_events.append(ev)

        for ev in matching_events:
            ev_name = _get_type_name(ev) or "UnknownType"
            if step_name == "_done" and is_subclass(ev, StopEvent):
                if current_stop_event:
                    stop_event_name = _get_type_name(current_stop_event) or "StopEvent"
                    edges.append(
                        WorkflowGraphEdge(source=stop_event_name, target=step_name)
                    )
            else:
                edges.append(WorkflowGraphEdge(source=ev_name, target=step_name))

            if is_subclass(ev, HumanResponseEvent) and "external_step" in added_nodes:
                edges.append(WorkflowGraphEdge(source="external_step", target=ev_name))

        # Edges from steps to resources (with variable name as label)
        for resource_def in step_config.resources:
            resource_id = _get_resource_identity(resource_def.resource)
            resource_node = added_descriptor_nodes[resource_id]
            edges.append(
                WorkflowGraphEdge(
                    source=step_name,
                    target=resource_node.id,
                    label=resource_def.name,  # The variable name
                )
            )

    workflow_name = workflow_cls.__name__
    workflow_description = inspect.getdoc(workflow_cls)
    return WorkflowGraph(
        name=workflow_name, nodes=nodes, edges=edges, description=workflow_description
    )


__all__ = ["get_workflow_representation"]
