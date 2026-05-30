# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import ast
import inspect
import json
import textwrap
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Tuple, cast

# Guard imports behind TYPE_CHECKING so the module
# loads without pulling the full llama-index-core import chain.
if TYPE_CHECKING:
    from llama_index.core.agent.workflow import (
        AgentWorkflow,
        BaseWorkflowAgent,
    )

from pyvis.network import Network
from workflows import Workflow
from workflows.context.external_context import ExternalContext
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
)
from workflows.handler import WorkflowHandler
from workflows.representation import (
    WorkflowGenericNode,
    WorkflowGraph,
    WorkflowGraphEdge,
    WorkflowGraphNode,
    WorkflowResourceConfigNode,
    WorkflowResourceNode,
)
from workflows.representation import (
    get_workflow_representation as _get_workflow_representation,
)
from workflows.runtime.types.results import AddCollectedEvent, StepWorkerResult
from workflows.runtime.types.ticks import TickAddEvent, TickStepResult, WorkflowTick


def _truncate_label(label: str, max_length: int) -> str:
    """Truncate long labels for visualization."""
    return label if len(label) <= max_length else f"{label[: max_length - 1]}*"


def _get_node_color(node: WorkflowGraphNode) -> str:
    """Determine color for a node based on its type and event_type."""
    if node.node_type == "step":
        return "#ADD8E6"  # Light blue for steps
    elif node.node_type == "external":
        return "#BEDAE4"  # Light blue-gray for external
    elif node.node_type == "child_connector":
        return "#E0E0E0"  # Light gray for child workflow connectors
    elif node.node_type == "resource":
        return "#DDA0DD"  # Plum/light purple for resources
    elif node.node_type == "resource_config":
        return "#B2DFDB"  # Light teal for resource configs
    elif node.node_type == "event":
        if node.is_subclass_of("StartEvent"):  # type: ignore[possibly-missing-attribute]
            return "#E27AFF"  # Pink for start events
        elif node.is_subclass_of("StopEvent"):  # type: ignore[possibly-missing-attribute]
            return "#FFA07A"  # Orange for stop events
        return "#90EE90"  # Light green for other events
    elif node.node_type == "agent":
        if node.is_subclass_of("ReActAgent"):  # type: ignore[possibly-missing-attribute]
            return "#E27AFF"
        elif node.is_subclass_of("CodeActAgent"):  # type: ignore[possibly-missing-attribute]
            return "#66ccff"
        return "#90EE90"
    elif node.node_type == "tool":
        return "#ff9966"  # Orange for tools
    elif node.node_type == "workflow_base":
        return "#90EE90"  # Light green for workflow base
    elif node.node_type == "workflow_agent":
        return "#66ccff"  # Light blue for workflow agents
    elif node.node_type == "workflow_tool":
        return "#ff9966"  # Orange for workflow tools
    elif node.node_type == "workflow_handoff":
        return "#E27AFF"  # Pink for handoff nodes
    else:
        return "#90EE90"  # Default light green


def _get_node_shape(node: WorkflowGraphNode) -> str:
    """Determine shape for a node based on its type."""
    if node.node_type in ("step", "external"):
        return "box"
    elif node.node_type == "child_connector":
        return "ellipse"
    elif node.node_type == "event":
        return "ellipse"
    elif node.node_type == "resource":
        return "hexagon"
    elif node.node_type == "resource_config":
        return "box"
    elif node.node_type in ("agent", "tool", "workflow_agent", "workflow_handoff"):
        return "ellipse"
    elif node.node_type == "workflow_base":
        return "diamond"
    elif node.node_type == "workflow_tool":
        return "box"
    else:
        return "box"


def _render_pyvis(
    graph: WorkflowGraph,
    filename: str,
    notebook: bool = False,
    max_label_length: int | None = None,
) -> None:
    """Render workflow graph using Pyvis."""

    net = Network(directed=True, height="750px", width="100%")

    # Add nodes
    for node in graph.nodes:
        color = _get_node_color(node)
        shape = _get_node_shape(node)

        # Compute display label (with optional truncation)
        display_label = node.label
        if max_label_length:
            display_label = _truncate_label(node.label, max_label_length)

        # Build title - show full label if truncated, plus resource metadata
        title: str | None = None
        if max_label_length and len(node.label) > max_label_length:
            title = node.label  # Show full label on hover
        if isinstance(node, WorkflowResourceNode):
            title_parts = [f"Type: {node.type_name or 'Unknown'}"]
            if node.getter_name:
                title_parts.append(f"Getter: {node.getter_name}")
            if node.source_file:
                location = node.source_file
                if node.source_line:
                    location += f":{node.source_line}"
                title_parts.append(f"Source: {location}")
            if node.description:
                title_parts.append(f"Doc: {node.description[:100]}...")
            title = "\n".join(title_parts)
        elif isinstance(node, WorkflowResourceConfigNode):
            title_parts = []
            if node.type_name:
                title_parts.append(f"Type: {node.type_name}")
            if node.config_file:
                title_parts.append(f"File: {node.config_file}")
            if node.path_selector:
                title_parts.append(f"Path: {node.path_selector}")
            title = "\n".join(title_parts) if title_parts else title

        net.add_node(
            node.id,
            label=display_label,
            title=title,
            color=color,
            shape=shape,
        )

    # Add edges
    for edge in graph.edges:
        if edge.label:
            net.add_edge(edge.source, edge.target, label=edge.label)
        else:
            net.add_edge(edge.source, edge.target)

    net.show(filename, notebook=notebook)


def _determine_event_color(event_type: type) -> str:
    """Determine color for an event type."""
    if issubclass(event_type, StartEvent):
        # Pink for start events
        event_color = "#E27AFF"
    elif issubclass(event_type, StopEvent):
        # Orange for stop events
        event_color = "#FFA07A"
    else:
        # Light green for other events
        event_color = "#90EE90"
    return event_color


def _clean_id_for_mermaid(name: str) -> str:
    """Convert a name to a valid Mermaid ID."""
    return name.replace(" ", "_").replace("-", "_").replace(".", "_")


def _get_mermaid_css_class(node: WorkflowGraphNode) -> str:
    """Determine CSS class for a node in Mermaid based on its type and event_type."""
    if node.node_type == "step":
        return "stepStyle"
    elif node.node_type == "external":
        return "externalStyle"
    elif node.node_type == "child_connector":
        return "childConnectorStyle"
    elif node.node_type == "resource":
        return "resourceStyle"
    elif node.node_type == "resource_config":
        return "resourceConfigStyle"
    elif node.node_type == "event":
        if node.is_subclass_of("StartEvent"):  # type: ignore[possibly-missing-attribute]
            return "startEventStyle"
        elif node.is_subclass_of("StopEvent"):  # type: ignore[possibly-missing-attribute]
            return "stopEventStyle"
        return "defaultEventStyle"
    elif node.node_type == "agent":
        if node.is_subclass_of("ReActAgent"):  # type: ignore[possibly-missing-attribute]
            return "reactAgentStyle"
        elif node.is_subclass_of("CodeActAgent"):  # type: ignore[possibly-missing-attribute]
            return "codeActAgentStyle"
        return "defaultAgentStyle"
    elif node.node_type == "tool":
        return "toolStyle"
    elif node.node_type == "workflow_base":
        return "workflowBaseStyle"
    elif node.node_type == "workflow_agent":
        return "workflowAgentStyle"
    elif node.node_type == "workflow_tool":
        return "workflowToolStyle"
    elif node.node_type == "workflow_handoff":
        return "workflowHandoffStyle"
    else:
        return "defaultEventStyle"


def _get_clean_node_id(node: WorkflowGraphNode) -> str:
    """Get a clean Mermaid-compatible ID for a node."""
    return f"{node.node_type}_{_clean_id_for_mermaid(node.id)}"


def _get_mermaid_shape(shape: str) -> tuple[str, str]:
    """Get Mermaid shape delimiters for a given shape."""
    if shape == "box":
        return "[", "]"
    elif shape == "ellipse":
        return "([", "])"
    elif shape == "diamond":
        return "{", "}"
    elif shape == "hexagon":
        return "{{", "}}"
    else:
        return "[", "]"


def _render_mermaid(
    graph: WorkflowGraph, filename: str, max_label_length: int | None = None
) -> str:
    """Render workflow graph using Mermaid."""
    mermaid_lines = ["flowchart TD"]
    added_nodes: set[str] = set()
    added_edges: set[str] = set()

    # Build lookup dictionary for all nodes
    node_by_id: dict[str, WorkflowGraphNode] = {node.id: node for node in graph.nodes}

    # Add nodes
    for node in graph.nodes:
        clean_id = _get_clean_node_id(node)

        if clean_id not in added_nodes:
            added_nodes.add(clean_id)

            # Compute display label (with optional truncation)
            display_label = node.label
            if max_label_length:
                display_label = _truncate_label(node.label, max_label_length)

            shape = _get_node_shape(node)
            shape_start, shape_end = _get_mermaid_shape(shape)

            css_class = _get_mermaid_css_class(node)
            mermaid_lines.append(
                f'    {clean_id}{shape_start}"{display_label}"{shape_end}:::{css_class}'
            )

    # Add edges
    for edge in graph.edges:
        source_node = node_by_id.get(edge.source)
        target_node = node_by_id.get(edge.target)

        if source_node is None or target_node is None:
            continue

        source_id = _get_clean_node_id(source_node)
        target_id = _get_clean_node_id(target_node)

        # Handle edge labels (e.g., variable names for resources)
        if edge.label:
            edge_str = f'{source_id} -->|"{edge.label}"| {target_id}'
        else:
            edge_str = f"{source_id} --> {target_id}"

        if edge_str not in added_edges:
            added_edges.add(edge_str)
            mermaid_lines.append(f"    {edge_str}")

    # Add style definitions
    mermaid_lines.extend(
        [
            "    classDef stepStyle fill:#ADD8E6,color:#000000,line-height:1.2",
            "    classDef externalStyle fill:#BEDAE4,color:#000000,line-height:1.2",
            "    classDef resourceStyle fill:#DDA0DD,color:#000000,line-height:1.2",
            "    classDef resourceConfigStyle fill:#B2DFDB,color:#000000,line-height:1.2",
            "    classDef startEventStyle fill:#E27AFF,color:#000000",
            "    classDef stopEventStyle fill:#FFA07A,color:#000000",
            "    classDef defaultEventStyle fill:#90EE90,color:#000000",
            "    classDef reactAgentStyle fill:#E27AFF,color:#000000",
            "    classDef codeActAgentStyle fill:#66ccff,color:#000000",
            "    classDef defaultAgentStyle fill:#90EE90,color:#000000",
            "    classDef toolStyle fill:#ff9966,color:#000000",
            "    classDef workflowBaseStyle fill:#90EE90,color:#000000",
            "    classDef workflowAgentStyle fill:#66ccff,color:#000000",
            "    classDef workflowToolStyle fill:#ff9966,color:#000000",
            "    classDef workflowHandoffStyle fill:#E27AFF,color:#000000",
            "    classDef childConnectorStyle fill:#E0E0E080,color:#555555,stroke-width:0px",
        ]
    )

    diagram_string = "\n".join(mermaid_lines)

    if filename:
        with open(filename, "w") as f:
            f.write(diagram_string)

    return diagram_string


def _get_type_chain(cls: type, base: type) -> list[str]:
    """Get type inheritance chain up to (but not including) base class."""
    names: list[str] = [cls.__name__]
    for parent in cls.mro()[1:]:
        if parent is base:
            break
        if isinstance(parent, type) and issubclass(parent, base):
            names.append(parent.__name__)
    return names


def _extract_single_agent_structure(agent: BaseWorkflowAgent) -> WorkflowGraph:
    """Extract the structure of a single agent."""
    # Deferred import: llama-index-core uses PEP 604 union syntax (`X | Y`) at
    # runtime which crashes on Python 3.9.  Importing here avoids pulling the
    # whole llama-index-core import chain at module-load time.
    from llama_index.core.agent.workflow import BaseWorkflowAgent as _BaseWorkflowAgent
    from llama_index.core.tools import AsyncBaseTool, BaseTool

    nodes: List[WorkflowGraphNode] = []
    edges: List[WorkflowGraphEdge] = []

    # Add agent node
    agent_type = type(agent)
    agent_node = WorkflowGenericNode(
        id="agent",
        label=agent.name,
        node_type="agent",
        event_type=agent_type.__name__,
        event_types=_get_type_chain(agent_type, _BaseWorkflowAgent),
    )
    nodes.append(agent_node)

    # Add tool nodes and edges
    tools = cast(List[BaseTool | AsyncBaseTool] | None, agent.tools)
    if tools is not None and len(tools) > 0:
        for i, tool in enumerate(tools):
            tool_id = f"tool_{i}"
            tool_node = WorkflowGenericNode(
                id=tool_id,
                label=f"Tool {i + 1}: {tool.metadata.get_name()}",
                node_type="tool",
            )
            nodes.append(tool_node)

            # Add edge from agent to tool
            edges.append(WorkflowGraphEdge(source="agent", target=tool_id))

    return WorkflowGraph(name=agent.name, nodes=nodes, edges=edges)


def _process_tools_and_handoffs(
    agent: BaseWorkflowAgent,
    processed_agents: List[str],
    all_agents: Dict[str, BaseWorkflowAgent],
    nodes: List[WorkflowGraphNode],
    edges: List[WorkflowGraphEdge],
    root_agent: str,
) -> Tuple[List[WorkflowGraphNode], List[WorkflowGraphEdge], List[str]]:
    from llama_index.core.tools import BaseTool

    if agent.name not in processed_agents:
        nodes.append(
            WorkflowGenericNode(
                id=agent.name, label=agent.name, node_type="workflow_agent"
            )
        )
        if agent.name == root_agent:
            edges.append(WorkflowGraphEdge(source="user", target=root_agent))
        for t in agent.tools or []:
            if isinstance(t, BaseTool):
                fn_name = t.metadata.get_name()
            else:
                # Fallback for non-BaseTool callables or objects without __name__
                fn_name = getattr(t, "__name__", type(t).__name__)
            node_id = f"{agent.name}_{fn_name}"
            nodes.append(
                WorkflowGenericNode(
                    id=node_id,
                    label=fn_name,
                    node_type="workflow_tool",
                )
            )
            edges.append(WorkflowGraphEdge(source=agent.name, target=node_id))
        if agent.can_handoff_to:
            for a in agent.can_handoff_to:
                edges.append(WorkflowGraphEdge(source=agent.name, target=a))
        else:
            edges.append(WorkflowGraphEdge(source=agent.name, target="output"))
        processed_agents.append(agent.name)

    if agent.can_handoff_to:
        for a in agent.can_handoff_to:
            if a not in processed_agents:
                _process_tools_and_handoffs(
                    all_agents[a],
                    processed_agents=processed_agents,
                    all_agents=all_agents,
                    nodes=nodes,
                    edges=edges,
                    root_agent=root_agent,
                )

    return nodes, edges, processed_agents


def _extract_agent_workflow_structure(
    agent_workflow: AgentWorkflow,
) -> WorkflowGraph:
    """Extract the structure of an agent workflow."""
    nodes: List[WorkflowGraphNode] = []
    edges: List[WorkflowGraphEdge] = []

    # Add base workflow node
    user_node = WorkflowGenericNode(
        id="user",
        label="User",
        node_type="workflow_base",
    )
    output_node = WorkflowGenericNode(
        id="output", label="Output", node_type="workflow_base"
    )
    nodes.extend([user_node, output_node])

    agents = agent_workflow.agents
    processed_agents: List[str] = []
    for v in agents.values():
        nodes, edges, processed_agents = _process_tools_and_handoffs(
            agent=v,
            processed_agents=processed_agents,
            all_agents=agents,
            nodes=nodes,
            edges=edges,
            root_agent=agent_workflow.root_agent,
        )
    if all(edge.target != "output" for edge in edges):
        agent_nodes = [n for n in nodes if n.node_type == "workflow_agent"]
        edges.append(WorkflowGraphEdge(source=agent_nodes[-1].id, target="output"))

    return WorkflowGraph(name=type(agent_workflow).__name__, nodes=nodes, edges=edges)


def _extract_execution_graph(
    handler: WorkflowHandler, max_label_length: int | None = None
) -> Tuple[Dict[str, Tuple[str, str, type | None]], List[Tuple[str, str]]]:
    """Helper to extract nodes and edges from the workflow handler's tick log."""

    ticks: List[WorkflowTick] = []
    if handler.ctx is not None:
        face = handler.ctx._face
        if isinstance(face, ExternalContext):
            ticks = face._tick_log
    nodes: Dict[str, Tuple[str, str, type | None]] = {}
    edges: List[Tuple[str, str]] = []
    event_node_by_identity: Dict[int, str] = {}
    step_seq: Dict[str, int] = {}

    external_node_id = "external_step"
    nodes[external_node_id] = ("external_step", "external", None)

    def ensure_event_node(ev: Event) -> str:
        key = id(ev)
        if key in event_node_by_identity:
            return event_node_by_identity[key]
        label = type(ev).__name__
        node_id = f"event:{label}#{len(event_node_by_identity)}"
        display_label = (
            _truncate_label(label, max_label_length) if max_label_length else label
        )
        nodes[node_id] = (display_label, "event", type(ev))
        event_node_by_identity[key] = node_id
        return node_id

    def iter_emitted_events(step_tick: TickStepResult[Any]) -> List[Event]:
        emitted: List[Event] = []
        for r in step_tick.result:
            if isinstance(r, StepWorkerResult) and isinstance(r.result, Event):
                emitted.append(r.result)
            elif isinstance(r, AddCollectedEvent):
                emitted.append(r.event)
        return emitted

    for t in ticks:
        if isinstance(t, TickAddEvent):
            ev_id = ensure_event_node(t.event)
            edges.append((external_node_id, ev_id))
        elif isinstance(t, TickStepResult):
            step_name = str(t.step_id)
            step_seq[step_name] = step_seq.get(step_name, 0) + 1
            seq = step_seq[step_name]
            step_node_id = f"step:{step_name}#{seq}"
            step_label = f"{step_name}#{seq}"
            display_label = (
                _truncate_label(step_label, max_label_length)
                if max_label_length
                else step_label
            )
            nodes[step_node_id] = (display_label, "step", None)

            in_event_node_id = ensure_event_node(t.event)
            edges.append((in_event_node_id, step_node_id))

            for out_ev in iter_emitted_events(t):
                out_event_node_id = ensure_event_node(out_ev)
                edges.append((step_node_id, out_event_node_id))

    return nodes, edges


def _get_workflow_classes_from_step(method_callable: Callable | Any) -> list[str]:
    """
    Finds classes instantiated within a method that inherit from Workflow.
    Resolves names against the method's actual global namespace.
    """
    if method_callable is None:
        return []

    workflow_classes = []
    try:
        # Get source and module context
        source = inspect.getsource(method_callable)
        clean_source = textwrap.dedent(source)
        tree = ast.parse(clean_source)

        # Use __globals__ to get the actual namespace where the function was defined.
        # This is more robust than sys.modules.get(module_name) because it works
        # correctly when multiple modules have the same name (e.g., multiple
        # conftest.py files in different packages during test collection).
        func_globals = getattr(method_callable, "__globals__", None)
        if not func_globals:
            return []

        for node in ast.walk(tree):
            # We are looking for instantiations: e.g., MySubWorkflow()
            if isinstance(node, ast.Call):
                # Handle direct calls: Name()
                if isinstance(node.func, ast.Name):
                    class_name = node.func.id
                # Handle attribute calls: module.Name()
                elif isinstance(node.func, ast.Attribute):
                    class_name = node.func.attr
                else:
                    continue

                # Look up the name in the function's actual global namespace
                obj = func_globals.get(class_name)

                # Robust check: Is it a class, and is it a Workflow subclass?
                if (
                    inspect.isclass(obj)
                    and issubclass(obj, Workflow)
                    and obj is not Workflow  # Don't include the base class itself
                ):
                    if class_name not in workflow_classes:
                        workflow_classes.append(class_name)

    except Exception:
        # Fallback to empty list if source cannot be parsed or objects resolved
        return []

    return workflow_classes


def _get_nested_workflow_representation(
    workflow: Workflow | type[Workflow], include_child_workflows: bool = False
) -> WorkflowGraph:
    """
    Introspects a workflow and builds a unified WorkflowGraph.

    If include_child_workflows is True, it performs a 1-level deep scan of
    step source code to find instantiated sub-workflows and merges their
    graphs into the parent graph.
    """
    parent_graph = _get_workflow_representation(workflow)
    if not include_child_workflows:
        return parent_graph

    # Define the helper AFTER parent_graph is created so it can
    # modify parent_graph directly via closure (i.e. parent_graph is now in scope for this helper function.
    def _merge_subgraph_into_parent(
        child_graph: WorkflowGraph, parent_step_id: str, class_name: str
    ) -> None:
        """Internal helper to handle ID prefixing and edge stitching."""
        prefix = f"{parent_step_id}_{class_name}_"

        # 1. Merge Nodes
        for c_node in list(child_graph.nodes):
            c_node_id = getattr(c_node, "id", str(c_node))
            new_node = WorkflowGenericNode(
                id=f"{prefix}{c_node_id}",
                label=getattr(c_node, "label", c_node_id),
                node_type=getattr(c_node, "node_type", "step"),
                event_type=getattr(c_node, "event_type", None),
            )
            parent_graph.nodes.append(new_node)

        # 2. Merge Edges
        for edge in child_graph.edges:
            parent_graph.edges.append(
                WorkflowGraphEdge(
                    source=f"{prefix}{edge.source}",
                    target=f"{prefix}{edge.target}",
                    label=edge.label,
                )
            )

        # 3. Stitch: Parent Step -> "calls" node -> Child Start
        child_start_id = next(
            (
                n.id
                for n in child_graph.nodes
                if getattr(n, "event_type", None) == "StartEvent"
            ),
            None,
        )
        if child_start_id:
            calls_node_id = f"{prefix}calls"
            parent_graph.nodes.append(
                WorkflowGenericNode(
                    id=calls_node_id,
                    label=f"calls: {class_name}",
                    node_type="child_connector",
                )
            )
            parent_graph.edges.append(
                WorkflowGraphEdge(source=parent_step_id, target=calls_node_id)
            )
            parent_graph.edges.append(
                WorkflowGraphEdge(
                    source=calls_node_id, target=f"{prefix}{child_start_id}"
                )
            )

        # 4. Stitch: Child Stop -> "returns" node -> Parent Step
        child_stop_id = next(
            (
                n.id
                for n in child_graph.nodes
                if getattr(n, "event_type", None) == "StopEvent"
            ),
            None,
        )
        if child_stop_id:
            returns_node_id = f"{prefix}returns"
            parent_graph.nodes.append(
                WorkflowGenericNode(
                    id=returns_node_id,
                    label=f"returns: {class_name}",
                    node_type="child_connector",
                )
            )
            parent_graph.edges.append(
                WorkflowGraphEdge(
                    source=f"{prefix}{child_stop_id}", target=returns_node_id
                )
            )
            parent_graph.edges.append(
                WorkflowGraphEdge(source=returns_node_id, target=parent_step_id)
            )

    # --- Discovery and Execution Loop ---
    workflow_cls = workflow if isinstance(workflow, type) else type(workflow)
    steps_lookup = workflow_cls._get_steps_from_class()

    for node in list(parent_graph.nodes):
        step_id = getattr(node, "id", str(node))
        if getattr(node, "node_type", None) == "step":
            step_method = steps_lookup.get(step_id)
            nested_wf_classnames = _get_workflow_classes_from_step(step_method)

            for nested_wf_classname in nested_wf_classnames:
                try:
                    # Use __globals__ to get the class from the actual namespace
                    # where the step method was defined. This handles cases where
                    # multiple modules share the same name (e.g., conftest.py).
                    func_globals = getattr(step_method, "__globals__", {})
                    wf_class = func_globals.get(nested_wf_classname)
                    if wf_class is None:
                        raise LookupError(
                            f"Could not find workflow class '{nested_wf_classname}' "
                            f"in step method's namespace"
                        )

                    child_instance = wf_class()
                    child_graph = _get_workflow_representation(child_instance)

                    # Executes the merge using the closure above
                    _merge_subgraph_into_parent(
                        child_graph, step_id, nested_wf_classname
                    )
                except Exception:
                    continue

    return parent_graph


def draw_all_possible_flows(
    workflow: Workflow | type[Workflow],
    filename: str = "workflow_all_flows.html",
    notebook: bool = False,
    max_label_length: int | None = None,
    include_child_workflows: bool = True,
) -> None:
    """
    Draws all possible flows of the workflow using Pyvis.

    Args:
        workflow: The workflow instance or class to visualize
        filename: Output HTML filename
        notebook: Whether running in notebook environment
        max_label_length: Maximum label length before truncation (None = no limit)
        include_child_workflows: Whether to include child workflow graphs

    """
    graph = _get_nested_workflow_representation(
        workflow, include_child_workflows=include_child_workflows
    )
    _render_pyvis(graph, filename, notebook, max_label_length)


def draw_all_possible_flows_mermaid(
    workflow: Workflow | type[Workflow],
    filename: str = "workflow_all_flows.mermaid",
    max_label_length: int | None = None,
    include_child_workflows: bool = True,
) -> str:
    """
    Draws all possible flows of the workflow as a Mermaid diagram.
    """
    # Use the new helper to get the full graph structure
    full_graph = _get_nested_workflow_representation(
        workflow, include_child_workflows=include_child_workflows
    )

    # Render to Mermaid format
    return _render_mermaid(full_graph, filename, max_label_length)


def draw_agent_with_tools(
    agent: BaseWorkflowAgent,
    filename: str = "agent_with_tools.html",
    notebook: bool = False,
) -> str:
    """
    > **NOTE**: *PyVis is needed for this function*.
    Draw an agent with its tool as a flowchart.

    Args:
        agent (BaseWorkflowAgent): agent workflow.
        filename (str): name of the HTML file to save the flowchart to. Defaults to 'agent_workflow.html'.
        notebook (bool): whether or not this is displayed within a notebook (.ipynb). Defaults to False.

    Returns:
        str: the path to the file where the flowchart was saved.

    """
    graph = _extract_single_agent_structure(agent)
    _render_pyvis(graph, filename, notebook)
    return filename


def draw_agent_workflow(
    agent_workflow: AgentWorkflow,
    filename: str = "agent_workflow.html",
    notebook: bool = False,
) -> str:
    """
    > **NOTE**: *PyVis is needed for this function*.
    Draw an agent workflow as a flowchart.

    Args:
        agent_workflow (AgentWorkflow): agent workflow.
        filename (str): name of the HTML file to save the flowchart to. Defaults to 'agent_workflow.html'.
        notebook (bool): whether or not this is displayed within a notebook (.ipynb). Defaults to False.

    Returns:
        str: the path to the file where the flowchart was saved.

    """
    graph = _extract_agent_workflow_structure(agent_workflow)
    _render_pyvis(graph, filename, notebook)
    return filename


def draw_agent_with_tools_mermaid(
    agent: BaseWorkflowAgent,
    filename: str = "agent_with_tools.mermaid",
) -> str:
    """
    Draw an agent with its tools as a Mermaid diagram.

    Args:
        agent (BaseWorkflowAgent): agent workflow.
        filename (str): name of the Mermaid file to save the diagram to. Defaults to 'agent_with_tools.mermaid'.

    Returns:
        str: the Mermaid diagram as a string.

    """
    graph = _extract_single_agent_structure(agent)
    return _render_mermaid(graph, filename)


def draw_agent_workflow_mermaid(
    agent_workflow: AgentWorkflow,
    filename: str = "agent_workflow.mermaid",
) -> str:
    """
    Draw an agent workflow as a Mermaid diagram.

    Args:
        agent_workflow (AgentWorkflow): agent workflow.
        filename (str): name of the Mermaid file to save the diagram to. Defaults to 'agent_workflow.mermaid'.

    Returns:
        str: the Mermaid diagram as a string.

    """
    graph = _extract_agent_workflow_structure(agent_workflow)
    return _render_mermaid(graph, filename)


def draw_most_recent_execution(
    handler: WorkflowHandler,
    filename: str = "workflow_recent_execution.html",
    notebook: bool = False,
    max_label_length: int | None = None,
) -> None:
    """Draws the most recent execution of the workflow using Pyvis."""
    nodes, edges = _extract_execution_graph(handler, max_label_length)
    net = Network(directed=True, height="750px", width="100%")

    for node_id, (label, node_type, ev_type) in nodes.items():
        if node_type == "step" or node_type == "external":
            color = "#ADD8E6" if node_type == "step" else "#BEDAE4"
            shape = "box"
        else:
            color = _determine_event_color(ev_type if ev_type else Event)
            shape = "ellipse"
        net.add_node(node_id, label=label, color=color, shape=shape)

    for src, dst in edges:
        net.add_edge(src, dst)

    options = {
        "layout": {
            "hierarchical": {
                "enabled": True,
                "direction": "LR",
                "nodeSpacing": 150,
                "levelSeparation": 120,
            }
        },
        "physics": {"enabled": False},
    }
    try:
        net.set_options(json.dumps(options))
    except Exception:
        pass

    net.show(filename, notebook=notebook)


def draw_most_recent_execution_mermaid(
    handler: WorkflowHandler,
    filename: str = "workflow_recent_execution.mermaid",
    max_label_length: int | None = None,
) -> str:
    """Draws the most recent execution of the workflow as a Mermaid diagram."""
    nodes, edges = _extract_execution_graph(handler, max_label_length)
    mermaid_lines = ["flowchart TD"]

    cleaned_ids = {
        # Child-workflow steps carry a namespaced StepId whose string form is
        # ``child/step`` -- the ``/`` is not a valid Mermaid node-id character,
        # so it must be sanitized alongside ``:`` and ``#``.
        node_id: node_id.replace(":", "_").replace("#", "_").replace("/", "_")
        for node_id in nodes.keys()
    }

    for node_id, (label, node_type, ev_type) in nodes.items():
        clean_id = cleaned_ids[node_id]
        shape_start, shape_end = (
            ("[", "]") if node_type in ["step", "external"] else ("([", "])")
        )

        css_class = "defaultEventStyle"
        if node_type == "step":
            css_class = "stepStyle"
        elif node_type == "external":
            css_class = "externalStyle"
        elif node_type == "event" and ev_type:
            if issubclass(ev_type, StartEvent):
                css_class = "startEventStyle"
            elif issubclass(ev_type, StopEvent):
                css_class = "stopEventStyle"

        mermaid_lines.append(
            f'    {clean_id}{shape_start}"{label}"{shape_end}:::{css_class}'
        )

    for src, dst in edges:
        mermaid_lines.append(f"    {cleaned_ids[src]} --> {cleaned_ids[dst]}")

    styles = [
        "classDef stepStyle fill:#ADD8E6,color:#000000,line-height:1.2",
        "classDef externalStyle fill:#BEDAE4,color:#000000,line-height:1.2",
        "classDef resourceStyle fill:#DDA0DD,color:#000000,line-height:1.2",
        "classDef resourceConfigStyle fill:#B2DFDB,color:#000000,line-height:1.2",
        "classDef startEventStyle fill:#E27AFF,color:#000000",
        "classDef stopEventStyle fill:#FFA07A,color:#000000",
        "classDef defaultEventStyle fill:#90EE90,color:#000000",
        "classDef reactAgentStyle fill:#E27AFF,color:#000000",
        "classDef codeActAgentStyle fill:#66ccff,color:#000000",
        "classDef defaultAgentStyle fill:#90EE90,color:#000000",
        "classDef toolStyle fill:#ff9966,color:#000000",
        "classDef workflowBaseStyle fill:#90EE90,color:#000000",
        "classDef workflowAgentStyle fill:#66ccff,color:#000000",
        "classDef workflowToolStyle fill:#ff9966,color:#000000",
        "classDef workflowHandoffStyle fill:#E27AFF,color:#000000",
    ]
    mermaid_lines.extend([f"    {s}" for s in styles])

    diagram_string = "\n".join(mermaid_lines)
    if filename:
        with open(filename, "w") as f:
            f.write(diagram_string)

    return diagram_string
