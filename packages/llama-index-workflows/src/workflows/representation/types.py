from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class WorkflowNodeBase(BaseModel):
    """Base class for all workflow graph nodes."""

    id: str = Field(description="Unique identifier for the node")
    label: str = Field(description="Display text for the node")

    def truncated_label(self, max_length: int) -> str:
        """Get truncated label for visualization (adds * suffix if truncated)."""
        if len(self.label) <= max_length:
            return self.label
        return f"{self.label[: max_length - 1]}*"


class WorkflowStepNode(WorkflowNodeBase):
    """A workflow step node representing a function decorated with @step."""

    node_type: Literal["step"] = Field(
        default="step", description="Discriminator field for node type"
    )
    description: str | None = Field(
        default=None,
        description="Documentation string extracted from the step function",
    )


class WorkflowEventNode(WorkflowNodeBase):
    """An event node representing an Event class that flows between steps."""

    node_type: Literal["event"] = Field(
        default="event", description="Discriminator field for node type"
    )
    event_type: str = Field(
        description="The event class name (e.g., 'StartEvent', 'MyCustomEvent')"
    )
    event_types: list[str] = Field(
        description="Event class inheritance chain for subclass checking. "
        "First element is the class itself, followed by parent Event subclasses."
    )
    event_schema: dict[str, Any] | None = Field(
        default=None,
        description="Pydantic JSON schema for the event type",
    )
    produced_by: list[str] = Field(
        default_factory=list,
        description="Names of steps whose declared return signature emits this "
        "event type (including via list[E] returns).",
    )

    def is_subclass_of(self, *type_names: str) -> bool:
        """Check if this node's event_type is a subclass of any of the given types."""
        return any(name in self.event_types for name in type_names)


class WorkflowExternalNode(WorkflowNodeBase):
    """An external node representing human-in-the-loop or external system interaction."""

    node_type: Literal["external"] = Field(
        default="external", description="Discriminator field for node type"
    )


class WorkflowResourceNode(WorkflowNodeBase):
    """A resource node representing an injected dependency (e.g., database client, API client)."""

    node_type: Literal["resource"] = Field(
        default="resource", description="Discriminator field for node type"
    )
    type_name: str | None = Field(
        default=None,
        description="The type annotation of the resource (e.g., 'DatabaseClient', 'AsyncLlamaCloud')",
    )
    getter_name: str | None = Field(
        default=None,
        description="Name of the factory function that creates the resource",
    )
    source_file: str | None = Field(
        default=None,
        description="Absolute path to the source file containing the getter function",
    )
    source_line: int | None = Field(
        default=None, description="Line number where the getter function is defined"
    )
    description: str | None = Field(
        default=None,
        description="Documentation string extracted from the getter function",
    )


class WorkflowResourceConfigNode(WorkflowNodeBase):
    """A resource config node representing a configuration loaded from a JSON file."""

    node_type: Literal["resource_config"] = Field(
        default="resource_config", description="Discriminator field for node type"
    )
    type_name: str | None = Field(
        default=None,
        description="The Pydantic BaseModel type that the config is validated against",
    )
    config_file: str | None = Field(
        default=None,
        description="Path to the JSON configuration file",
    )
    path_selector: str | None = Field(
        default=None,
        description="Dot-separated path selector for nested configuration values",
    )
    config_schema: dict[str, Any] | None = Field(
        default=None,
        description="Pydantic JSON schema for the config type",
    )
    config_value: dict[str, Any] | None = Field(
        default=None,
        description="The configuration value read from the file (if readable)",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable description of the config's purpose and contents",
    )


class WorkflowGenericNode(WorkflowNodeBase):
    """A generic node for custom visualization types not covered by standard node types.

    Used for agent visualization (node_type='agent', 'tool', 'workflow_agent', etc.)
    and other custom extensions. Supports optional event_type fields for type checking.
    """

    node_type: str = Field(
        description="Custom node type string (e.g., 'agent', 'tool', 'workflow_base')"
    )
    event_type: str | None = Field(
        default=None,
        description="Optional type name for nodes that support inheritance checking (e.g., agent types)",
    )
    event_types: list[str] | None = Field(
        default=None,
        description="Optional inheritance chain for subclass checking, similar to WorkflowEventNode",
    )

    def is_subclass_of(self, *type_names: str) -> bool:
        """Check if this node's event_type is a subclass of any of the given types."""
        if not self.event_types:
            return False
        return any(name in self.event_types for name in type_names)


# Union type for workflow graph nodes
# Pydantic will try to match against types in order; WorkflowGenericNode is last as catch-all
WorkflowGraphNode = (
    WorkflowStepNode
    | WorkflowEventNode
    | WorkflowExternalNode
    | WorkflowResourceNode
    | WorkflowResourceConfigNode
    | WorkflowGenericNode
)


class WorkflowGraphEdge(BaseModel):
    """A directed edge connecting two nodes in the workflow graph."""

    source: str = Field(description="ID of the source node (where the edge originates)")
    target: str = Field(description="ID of the target node (where the edge points to)")
    label: str | None = Field(
        default=None,
        description="Optional edge label, used for resource edges to show the variable name",
    )


class WorkflowGraph(BaseModel):
    """Complete workflow graph structure containing all nodes and edges."""

    name: str = Field(description="Name of the workflow class")
    nodes: list[WorkflowGraphNode] = Field(
        description="All nodes in the workflow graph"
    )
    edges: list[WorkflowGraphEdge] = Field(
        description="All directed edges connecting the nodes"
    )
    description: str | None = Field(
        default=None,
        description="Documentation string extracted from the workflow class",
    )

    def filter_by_node_type(self, *node_types: str) -> WorkflowGraph:
        """Create a simplified graph by removing nodes of specified types.

        Edges passing through filtered nodes are resolved:
        Node1 -> FilteredNode -> Node2 becomes Node1 -> Node2

        Args:
            *node_types: One or more node type strings to filter out
                        (e.g., "event", "resource", "step", "external")

        Returns:
            A new WorkflowGraph with the specified node types removed
            and edges resolved through them.
        """
        filter_types = set(node_types)

        # Identify nodes to filter out
        filtered_node_ids: set[str] = set()
        for node in self.nodes:
            if node.node_type in filter_types:
                filtered_node_ids.add(node.id)

        # Keep remaining nodes
        remaining_nodes = [n for n in self.nodes if n.id not in filtered_node_ids]
        remaining_node_ids = {n.id for n in remaining_nodes}

        # Build outgoing edge map and node lookup
        outgoing_map: dict[str, list[WorkflowGraphEdge]] = {}
        for edge in self.edges:
            outgoing_map.setdefault(edge.source, []).append(edge)

        node_by_id: dict[str, WorkflowGraphNode] = {n.id: n for n in self.nodes}

        def resolve_targets(
            from_id: str,
            first_filtered_label: str | None,
            visited: set[str],
        ) -> list[tuple[str, str | None]]:
            """Find remaining nodes reachable from from_id, through filtered nodes."""
            results: list[tuple[str, str | None]] = []
            for edge in outgoing_map.get(from_id, []):
                target = edge.target
                if target in visited:
                    continue

                if target in remaining_node_ids:
                    # Use the first filtered node's label, or the edge label if direct
                    label = (
                        first_filtered_label
                        if first_filtered_label is not None
                        else edge.label
                    )
                    results.append((target, label))
                elif target in filtered_node_ids:
                    # Follow through filtered node, capturing its label if first
                    visited.add(target)
                    filtered_node = node_by_id[target]
                    label = (
                        first_filtered_label
                        if first_filtered_label is not None
                        else filtered_node.label
                    )
                    results.extend(resolve_targets(target, label, visited))
            return results

        # Build new edges
        new_edges: list[WorkflowGraphEdge] = []
        seen_edges: set[tuple[str, str]] = set()

        for source_id in remaining_node_ids:
            for target_id, label in resolve_targets(source_id, None, set()):
                edge_key = (source_id, target_id)
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    new_edges.append(
                        WorkflowGraphEdge(
                            source=source_id,
                            target=target_id,
                            label=label,
                        )
                    )

        return WorkflowGraph(
            name=self.name,
            nodes=remaining_nodes,
            edges=new_edges,
            description=self.description,
        )


__all__ = [
    "WorkflowNodeBase",
    "WorkflowStepNode",
    "WorkflowEventNode",
    "WorkflowExternalNode",
    "WorkflowResourceNode",
    "WorkflowResourceConfigNode",
    "WorkflowGenericNode",
    "WorkflowGraphNode",
    "WorkflowGraphEdge",
    "WorkflowGraph",
]
