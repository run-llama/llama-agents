import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Annotated

import pytest
from pydantic import BaseModel
from workflows.decorators import step
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)
from workflows.representation import (
    WorkflowEventNode,
    WorkflowExternalNode,
    WorkflowGraph,
    WorkflowGraphEdge,
    WorkflowGraphNode,
    WorkflowResourceConfigNode,
    WorkflowResourceNode,
    WorkflowStepNode,
    get_workflow_representation,
)
from workflows.resource import Resource, ResourceConfig
from workflows.workflow import Workflow

from .conftest import DummyWorkflow  # type: ignore[import]


def _nodes_of_type(graph: WorkflowGraph, node_type: str) -> list[WorkflowGraphNode]:
    return [node for node in graph.nodes if node.node_type == node_type]


def _resource_nodes(graph: WorkflowGraph) -> list[WorkflowResourceNode]:
    return [node for node in graph.nodes if isinstance(node, WorkflowResourceNode)]


def _resource_config_nodes(graph: WorkflowGraph) -> list[WorkflowResourceConfigNode]:
    return [
        node for node in graph.nodes if isinstance(node, WorkflowResourceConfigNode)
    ]


def _edges_as_tuples(graph: WorkflowGraph) -> set[tuple[str, str, str | None]]:
    return {(edge.source, edge.target, edge.label) for edge in graph.edges}


def _find_edges(
    graph: WorkflowGraph,
    *,
    source: str | None = None,
    target_prefix: str | None = None,
    label: str | None = None,
) -> list[WorkflowGraphEdge]:
    edges: Iterable[WorkflowGraphEdge] = graph.edges
    if source is not None:
        edges = [edge for edge in edges if edge.source == source]
    if target_prefix is not None:
        edges = [edge for edge in edges if edge.target.startswith(target_prefix)]
    if label is not None:
        edges = [edge for edge in edges if edge.label == label]
    return list(edges)


@pytest.fixture()
def ground_truth_repr() -> WorkflowGraph:
    return WorkflowGraph(
        name="DummyWorkflow",
        nodes=[
            WorkflowStepNode(
                id="end_step",
                label="end_step",
            ),
            WorkflowEventNode(
                id="LastEvent",
                label="LastEvent",
                event_type="LastEvent",
                event_types=["LastEvent"],
            ),
            WorkflowEventNode(
                id="StopEvent",
                label="StopEvent",
                event_type="StopEvent",
                event_types=["StopEvent"],
            ),
            WorkflowStepNode(
                id="middle_step",
                label="middle_step",
            ),
            WorkflowEventNode(
                id="OneTestEvent",
                label="OneTestEvent",
                event_type="OneTestEvent",
                event_types=["OneTestEvent"],
            ),
            WorkflowStepNode(
                id="start_step",
                label="start_step",
            ),
            WorkflowEventNode(
                id="StartEvent",
                label="StartEvent",
                event_type="StartEvent",
                event_types=["StartEvent"],
            ),
        ],
        edges=[
            WorkflowGraphEdge(source="end_step", target="StopEvent"),
            WorkflowGraphEdge(source="LastEvent", target="end_step"),
            WorkflowGraphEdge(source="middle_step", target="LastEvent"),
            WorkflowGraphEdge(source="OneTestEvent", target="middle_step"),
            WorkflowGraphEdge(source="start_step", target="OneTestEvent"),
            WorkflowGraphEdge(source="StartEvent", target="start_step"),
        ],
    )


def test_get_workflow_representation(ground_truth_repr: WorkflowGraph) -> None:
    wf = DummyWorkflow()
    graph = get_workflow_representation(workflow=wf)
    assert isinstance(graph, WorkflowGraph)
    assert sorted(
        node.id for node in _nodes_of_type(ground_truth_repr, "step")
    ) == sorted(node.id for node in _nodes_of_type(graph, "step"))
    assert sorted(
        node.id for node in _nodes_of_type(ground_truth_repr, "event")
    ) == sorted(node.id for node in _nodes_of_type(graph, "event"))
    assert _edges_as_tuples(graph) >= _edges_as_tuples(ground_truth_repr)


def test_representation_hitl_includes_external_step_bridge() -> None:
    """HITL workflows get external_step node and bridging edges for graph validation."""

    class HITLWorkflow(Workflow):
        @step
        async def ask(self, ev: StartEvent) -> InputRequiredEvent:
            return InputRequiredEvent()

        @step
        async def handle(self, ev: HumanResponseEvent) -> StopEvent:
            return StopEvent(result="ok")

    wf = HITLWorkflow()
    graph = get_workflow_representation(workflow=wf)
    node_ids = {n.id for n in graph.nodes}
    assert "external_step" in node_ids
    edges = _edges_as_tuples(graph)
    assert ("InputRequiredEvent", "external_step", None) in edges or any(
        e[0] == "InputRequiredEvent" and e[1] == "external_step" for e in edges
    )
    assert ("external_step", "HumanResponseEvent", None) in edges or any(
        e[0] == "external_step" and e[1] == "HumanResponseEvent" for e in edges
    )


def test_truncated_label() -> None:
    """Test that truncated_label method works correctly."""
    node = WorkflowStepNode(id="my_step", label="my_long_step_name")
    assert node.truncated_label(5) == "my_l*"
    assert node.truncated_label(20) == "my_long_step_name"
    assert node.truncated_label(17) == "my_long_step_name"


class FannedOutTask(Event):
    idx: int


def test_produced_by_lists_producing_step() -> None:
    """A step returning list[Task] records itself as Task's producer."""

    class FanWorkflow(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[FannedOutTask]:
            return [FannedOutTask(idx=i) for i in range(3)]

        @step
        async def collect(self, ev: FannedOutTask) -> StopEvent | None:
            return StopEvent(result=ev.idx)

    graph = get_workflow_representation(FanWorkflow)

    task_nodes = [
        node
        for node in graph.nodes
        if isinstance(node, WorkflowEventNode) and node.event_type == "FannedOutTask"
    ]
    assert len(task_nodes) == 1
    assert "fan_out" in task_nodes[0].produced_by


def test_graph_serialization() -> None:
    """Test that WorkflowGraph serializes and restores node types."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="test", label="test"),
            WorkflowEventNode(
                id="OneTestEvent",
                label="OneTestEvent",
                event_type="OneTestEvent",
                event_types=["OneTestEvent"],
            ),
        ],
        edges=[WorkflowGraphEdge(source="test", target="OneTestEvent")],
    )

    data = graph.model_dump()
    restored = WorkflowGraph.model_validate(data)

    assert len(restored.nodes) == 2
    event_node = next(
        node for node in restored.nodes if isinstance(node, WorkflowEventNode)
    )
    assert event_node.event_type == "OneTestEvent"
    assert event_node.is_subclass_of("OneTestEvent")


# --- Resource node tests ---


class DatabaseClient:
    """A mock database client for testing resources."""

    pass


def get_database_client() -> DatabaseClient:
    """Factory function to create a database client.

    This docstring should appear in the resource metadata.
    """
    return DatabaseClient()


class MiddleEvent(Event):
    pass


class WorkflowWithResources(Workflow):
    @step
    async def start_step(self, ev: StartEvent) -> MiddleEvent:
        return MiddleEvent()

    @step
    async def step_with_resource(
        self,
        ev: MiddleEvent,
        db_client: Annotated[DatabaseClient, Resource(get_database_client)],
    ) -> StopEvent:
        return StopEvent(result="done")


def test_get_workflow_representation_with_resources() -> None:
    """Resource node metadata and step -> resource edge label are derived from factory."""
    wf = WorkflowWithResources()
    graph = get_workflow_representation(workflow=wf)

    resource_nodes = _resource_nodes(graph)
    assert len(resource_nodes) == 1
    resource_node = resource_nodes[0]
    assert resource_node.type_name == "DatabaseClient"
    assert resource_node.getter_name == "get_database_client"
    assert resource_node.description is not None
    assert resource_node.source_file is not None
    assert resource_node.source_line is not None
    edges = _find_edges(
        graph,
        source="step_with_resource",
        target_prefix="resource_",
        label="db_client",
    )
    assert len(edges) == 1


def test_resource_nodes_are_deduplicated() -> None:
    """Test that the same resource used in multiple steps appears only once."""

    class StepEvent(Event):
        pass

    class WorkflowWithSharedResource(Workflow):
        @step
        async def start_step(self, ev: StartEvent) -> StepEvent:
            return StepEvent()

        @step
        async def step_one(
            self,
            ev: StepEvent,
            db: Annotated[DatabaseClient, Resource(get_database_client)],
        ) -> MiddleEvent:
            return MiddleEvent()

        @step
        async def step_two(
            self,
            ev: MiddleEvent,
            db: Annotated[DatabaseClient, Resource(get_database_client)],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithSharedResource()
    graph = get_workflow_representation(workflow=wf)

    # Should have only one resource node (deduplicated)
    assert len(_resource_nodes(graph)) == 1

    # But should have two edges (one from each step)
    resource_edges = _find_edges(graph, target_prefix="resource_", label="db")
    assert len(resource_edges) == 2


def test_multiple_different_resources() -> None:
    """Test workflow with multiple different resources."""

    class CacheClient:
        pass

    def get_cache_client() -> CacheClient:
        return CacheClient()

    class WorkflowWithMultipleResources(Workflow):
        @step
        async def start_step(
            self,
            ev: StartEvent,
            db: Annotated[DatabaseClient, Resource(get_database_client)],
            cache: Annotated[CacheClient, Resource(get_cache_client)],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithMultipleResources()
    graph = get_workflow_representation(workflow=wf)

    # Should have two different resource nodes
    resource_nodes = _resource_nodes(graph)
    assert len(resource_nodes) == 2

    type_names = {rn.type_name for rn in resource_nodes}
    assert type_names == {"DatabaseClient", "CacheClient"}

    # Should have two edges with different labels
    resource_edges = _find_edges(graph, target_prefix="resource_")
    assert len(resource_edges) == 2

    labels = {e.label for e in resource_edges}
    assert labels == {"db", "cache"}


def test_edge_with_label() -> None:
    """Test that WorkflowGraphEdge with label works correctly."""
    edge = WorkflowGraphEdge(source="resource_123", target="my_step", label="my_var")

    assert edge.source == "resource_123"
    assert edge.target == "my_step"
    assert edge.label == "my_var"


def test_edge_without_label() -> None:
    """Test that WorkflowGraphEdge without label works correctly."""
    edge = WorkflowGraphEdge(source="event_A", target="step_B")

    assert edge.source == "event_A"
    assert edge.target == "step_B"
    assert edge.label is None


def test_graph_with_all_node_types_serialization() -> None:
    """Test full graph serialization/deserialization with all node types."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowEventNode(
                id="StartEvent",
                label="StartEvent",
                event_type="StartEvent",
                event_types=["StartEvent"],
            ),
            WorkflowExternalNode(id="external", label="External"),
            WorkflowResourceNode(
                id="resource_123",
                label="DB",
                type_name="DatabaseClient",
                getter_name="get_db",
            ),
        ],
        edges=[
            WorkflowGraphEdge(source="StartEvent", target="step1"),
            WorkflowGraphEdge(source="step1", target="resource_123", label="db"),
        ],
    )

    # Serialize
    data = graph.model_dump()
    assert len(data["nodes"]) == 4
    assert len(data["edges"]) == 2

    # Check discriminator values are present
    node_types = {n["node_type"] for n in data["nodes"]}
    assert node_types == {"step", "event", "external", "resource"}

    # Deserialize
    restored = WorkflowGraph.model_validate(data)
    assert len(restored.nodes) == 4
    assert len(restored.edges) == 2

    # Check correct types restored
    step_nodes = [n for n in restored.nodes if isinstance(n, WorkflowStepNode)]
    event_nodes = [n for n in restored.nodes if isinstance(n, WorkflowEventNode)]
    external_nodes = [n for n in restored.nodes if isinstance(n, WorkflowExternalNode)]
    resource_nodes = [n for n in restored.nodes if isinstance(n, WorkflowResourceNode)]

    assert len(step_nodes) == 1
    assert len(event_nodes) == 1
    assert len(external_nodes) == 1
    assert len(resource_nodes) == 1

    # Verify event node has its method
    assert event_nodes[0].is_subclass_of("StartEvent")

    # Verify resource node has its fields
    assert resource_nodes[0].type_name == "DatabaseClient"
    assert resource_nodes[0].getter_name == "get_db"


def test_graph_deserialization_from_raw_json() -> None:
    """Test that graph can be deserialized from raw JSON dict."""
    raw_data = {
        "name": "TestWorkflow",
        "nodes": [
            {"id": "step1", "label": "Step 1", "node_type": "step"},
            {
                "id": "MyEvent",
                "label": "MyEvent",
                "node_type": "event",
                "event_type": "MyEvent",
                "event_types": ["MyEvent"],
            },
            {"id": "external", "label": "External", "node_type": "external"},
            {
                "id": "resource_xyz",
                "label": "Resource",
                "node_type": "resource",
                "type_name": "SomeType",
            },
            {
                "id": "resource_config_456",
                "label": "ConfigModel",
                "node_type": "resource_config",
                "type_name": "ConfigModel",
                "config_file": "config.json",
                "path_selector": "settings",
                "config_schema": {
                    "type": "object",
                    "properties": {"key": {"type": "string"}},
                },
                "config_value": {"key": "value"},
            },
        ],
        "edges": [{"source": "MyEvent", "target": "step1"}],
    }

    graph = WorkflowGraph.model_validate(raw_data)

    assert len(graph.nodes) == 5
    node_types = {node.node_type for node in graph.nodes}
    assert node_types == {"step", "event", "external", "resource", "resource_config"}


# --- filter_by_node_type tests ---


def test_filter_by_node_type_removes_nodes() -> None:
    """Test that filter_by_node_type removes specified node types."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowEventNode(
                id="EventA",
                label="EventA",
                event_type="EventA",
                event_types=["EventA"],
            ),
            WorkflowStepNode(id="step2", label="Step 2"),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="EventA"),
            WorkflowGraphEdge(source="EventA", target="step2"),
        ],
    )

    filtered = graph.filter_by_node_type("event")

    # Event nodes should be removed
    assert len(filtered.nodes) == 2
    assert all(n.node_type == "step" for n in filtered.nodes)
    node_ids = {n.id for n in filtered.nodes}
    assert node_ids == {"step1", "step2"}


def test_filter_by_node_type_resolves_edges() -> None:
    """Test that edges through filtered nodes are resolved."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowEventNode(
                id="EventA",
                label="EventA",
                event_type="EventA",
                event_types=["EventA"],
            ),
            WorkflowStepNode(id="step2", label="Step 2"),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="EventA"),
            WorkflowGraphEdge(source="EventA", target="step2"),
        ],
    )

    filtered = graph.filter_by_node_type("event")

    # Edge should be resolved: step1 -> step2
    assert len(filtered.edges) == 1
    assert filtered.edges[0].source == "step1"
    assert filtered.edges[0].target == "step2"


def test_filter_by_node_type_chain_of_filtered_nodes() -> None:
    """Test filtering handles chains of filtered nodes."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowEventNode(
                id="EventA",
                label="First Filtered Node",
                event_type="EventA",
                event_types=["EventA"],
            ),
            WorkflowEventNode(
                id="EventB",
                label="Second Filtered Node",
                event_type="EventB",
                event_types=["EventB"],
            ),
            WorkflowStepNode(id="step2", label="Step 2"),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="EventA"),
            WorkflowGraphEdge(source="EventA", target="EventB"),
            WorkflowGraphEdge(source="EventB", target="step2"),
        ],
    )

    filtered = graph.filter_by_node_type("event")

    # Chain resolved: step1 -> step2, with first filtered node's label
    assert len(filtered.nodes) == 2
    assert len(filtered.edges) == 1
    assert filtered.edges[0].source == "step1"
    assert filtered.edges[0].target == "step2"
    assert filtered.edges[0].label == "First Filtered Node"


def test_filter_by_node_type_multiple_types() -> None:
    """Test filtering multiple node types at once."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowEventNode(
                id="EventA",
                label="EventA",
                event_type="EventA",
                event_types=["EventA"],
            ),
            WorkflowResourceNode(id="resource1", label="Resource"),
            WorkflowStepNode(id="step2", label="Step 2"),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="EventA"),
            WorkflowGraphEdge(source="step1", target="resource1", label="db"),
            WorkflowGraphEdge(source="EventA", target="step2"),
        ],
    )

    filtered = graph.filter_by_node_type("event", "resource")

    # Only step nodes remain
    assert len(filtered.nodes) == 2
    assert all(n.node_type == "step" for n in filtered.nodes)
    # step1 -> step2 edge remains (resolved through EventA)
    assert len(filtered.edges) == 1
    assert filtered.edges[0].source == "step1"
    assert filtered.edges[0].target == "step2"


def test_filter_by_node_type_preserves_direct_edges() -> None:
    """Test that direct edges between remaining nodes are preserved."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowStepNode(id="step2", label="Step 2"),
            WorkflowEventNode(
                id="EventA",
                label="EventA",
                event_type="EventA",
                event_types=["EventA"],
            ),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="step2"),  # Direct edge
            WorkflowGraphEdge(source="step2", target="EventA"),
        ],
    )

    filtered = graph.filter_by_node_type("event")

    # Direct edge should be preserved
    assert len(filtered.edges) == 1
    assert filtered.edges[0].source == "step1"
    assert filtered.edges[0].target == "step2"


def test_filter_by_node_type_uses_filtered_node_label() -> None:
    """Test that the first filtered node's label becomes the new edge label."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowEventNode(
                id="EventA",
                label="My Event Label",
                event_type="EventA",
                event_types=["EventA"],
            ),
            WorkflowStepNode(id="step2", label="Step 2"),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="EventA"),
            WorkflowGraphEdge(source="EventA", target="step2"),
        ],
    )

    filtered = graph.filter_by_node_type("event")

    # Label from filtered node should be on the new edge
    assert len(filtered.edges) == 1
    assert filtered.edges[0].label == "My Event Label"


def test_filter_by_node_type_preserves_direct_edge_labels() -> None:
    """Test that labels on direct edges are preserved."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowResourceNode(id="resource1", label="Resource"),
            WorkflowEventNode(
                id="EventA",
                label="EventA",
                event_type="EventA",
                event_types=["EventA"],
            ),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="resource1", label="db"),
            WorkflowGraphEdge(source="step1", target="EventA"),
        ],
    )

    filtered = graph.filter_by_node_type("event")

    # Resource edge label should be preserved (it's a direct edge)
    resource_edge = next(e for e in filtered.edges if e.target == "resource1")
    assert resource_edge.label == "db"


def test_filter_by_node_type_no_matching_types() -> None:
    """Test filtering with types that don't exist in graph."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowStepNode(id="step2", label="Step 2"),
        ],
        edges=[WorkflowGraphEdge(source="step1", target="step2")],
    )

    filtered = graph.filter_by_node_type("nonexistent")

    # Graph should be unchanged
    assert len(filtered.nodes) == 2
    assert len(filtered.edges) == 1


def test_filter_by_node_type_preserves_description() -> None:
    """Test that the workflow description is preserved."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[WorkflowStepNode(id="step1", label="Step 1")],
        edges=[],
        description="My workflow description",
    )

    filtered = graph.filter_by_node_type("event")

    assert filtered.description == "My workflow description"


def test_filter_by_node_type_deduplicates_edges() -> None:
    """Test that duplicate edges are not created."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowEventNode(
                id="EventA",
                label="EventA",
                event_type="EventA",
                event_types=["EventA"],
            ),
            WorkflowEventNode(
                id="EventB",
                label="EventB",
                event_type="EventB",
                event_types=["EventB"],
            ),
            WorkflowStepNode(id="step2", label="Step 2"),
        ],
        edges=[
            # Both events lead to step2 from step1
            WorkflowGraphEdge(source="step1", target="EventA"),
            WorkflowGraphEdge(source="step1", target="EventB"),
            WorkflowGraphEdge(source="EventA", target="step2"),
            WorkflowGraphEdge(source="EventB", target="step2"),
        ],
    )

    filtered = graph.filter_by_node_type("event")

    # Should only have one edge: step1 -> step2 (deduplicated)
    assert len(filtered.edges) == 1
    assert filtered.edges[0].source == "step1"
    assert filtered.edges[0].target == "step2"


# --- Resource config node tests ---


class ConfigData(BaseModel):
    """A config model for testing resource configs."""

    setting: str
    value: int


def _write_config(tmp_path: Path, filename: str, data: Mapping[str, object]) -> str:
    config_path = tmp_path / filename
    with open(config_path, "w") as f:
        json.dump(data, f)
    return str(config_path)


def test_resource_config_nested_in_resource_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nested ResourceConfig should create resource + config nodes and an edge."""
    monkeypatch.chdir(tmp_path)

    config_data = {"setting": "test", "value": 42}
    config_path = _write_config(tmp_path, "config.json", config_data)

    def get_configured_client(
        my_config: Annotated[ConfigData, ResourceConfig(config_file="config.json")],
    ) -> DatabaseClient:
        return DatabaseClient()

    class WorkflowWithResourceConfig(Workflow):
        @step
        async def step_with_config(
            self,
            ev: StartEvent,
            client: Annotated[DatabaseClient, Resource(get_configured_client)],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithResourceConfig()
    graph = get_workflow_representation(workflow=wf)

    assert len(_resource_nodes(graph)) == 1
    resource_config_nodes = _resource_config_nodes(graph)
    assert len(resource_config_nodes) == 1

    config_node = resource_config_nodes[0]
    assert config_node.type_name == "ConfigData"
    assert config_node.config_file == config_path
    assert config_node.path_selector is None
    assert config_node.config_schema is not None
    assert {"setting", "value"} <= set(config_node.config_schema.get("properties", {}))
    assert config_node.config_value == config_data

    edges = _find_edges(graph, target_prefix="resource_config_", label="my_config")
    assert len(edges) == 1
    assert edges[0].source.startswith("resource_")


def test_recursive_resource_dependencies_with_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nested resources should create resource->resource and resource->config edges."""
    monkeypatch.chdir(tmp_path)

    config_data = {"setting": "nested", "value": 7}
    config_path = _write_config(tmp_path, "config.json", config_data)

    class DBConnection:
        def __init__(self, config: ConfigData) -> None:
            self.config = config

    class Repository:
        def __init__(self, db: DBConnection) -> None:
            self.db = db

    def get_db_connection(
        config: Annotated[ConfigData, ResourceConfig(config_file="config.json")],
    ) -> DBConnection:
        return DBConnection(config=config)

    def get_repository(
        db: Annotated[DBConnection, Resource(get_db_connection)],
    ) -> Repository:
        return Repository(db=db)

    class WorkflowWithRecursiveResources(Workflow):
        @step
        async def start_step(
            self,
            ev: StartEvent,
            repo: Annotated[Repository, Resource(get_repository)],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithRecursiveResources()
    graph = get_workflow_representation(workflow=wf)

    assert len(_resource_nodes(graph)) == 2
    resource_config_nodes = _resource_config_nodes(graph)
    assert len(resource_config_nodes) == 1
    assert resource_config_nodes[0].config_file == config_path
    assert resource_config_nodes[0].config_value == config_data

    def _resource_node_for_getter(suffix: str) -> WorkflowResourceNode:
        return next(
            node
            for node in _resource_nodes(graph)
            if node.getter_name is not None and node.getter_name.endswith(suffix)
        )

    repo_node = _resource_node_for_getter("get_repository")
    db_node = _resource_node_for_getter("get_db_connection")

    repo_edges = _find_edges(graph, source=repo_node.id, label="db")
    assert len(repo_edges) == 1
    assert repo_edges[0].target == db_node.id

    config_edges = _find_edges(graph, source=db_node.id, label="config")
    assert len(config_edges) == 1
    assert config_edges[0].target.startswith("resource_config_")


def test_resource_config_direct_in_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ResourceConfig used directly in a step should only create a config node."""
    monkeypatch.chdir(tmp_path)

    config_data = {"setting": "direct", "value": 99}
    config_path = _write_config(tmp_path, "direct_config.json", config_data)

    class WorkflowWithDirectConfig(Workflow):
        @step
        async def step_with_direct_config(
            self,
            ev: StartEvent,
            config: Annotated[
                ConfigData, ResourceConfig(config_file="direct_config.json")
            ],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithDirectConfig()
    graph = get_workflow_representation(workflow=wf)

    assert len(_resource_nodes(graph)) == 0
    resource_config_nodes = _resource_config_nodes(graph)
    assert len(resource_config_nodes) == 1
    config_node = resource_config_nodes[0]
    assert config_node.type_name == "ConfigData"
    assert config_node.config_file == config_path
    assert config_node.config_value == config_data

    edges = _find_edges(
        graph,
        source="step_with_direct_config",
        target_prefix="resource_config_",
        label="config",
    )
    assert len(edges) == 1


def test_resource_config_with_path_selector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ResourceConfig path selector is preserved in graph nodes."""
    monkeypatch.chdir(tmp_path)

    config_data = {"database": {"setting": "test", "value": 42}}
    config_path = _write_config(tmp_path, "config.json", config_data)

    def get_configured_client(
        config: Annotated[
            ConfigData,
            ResourceConfig(config_file="config.json", path_selector="database"),
        ],
    ) -> DatabaseClient:
        return DatabaseClient()

    class WorkflowWithResourceConfig(Workflow):
        @step
        async def step_with_config(
            self,
            ev: StartEvent,
            client: Annotated[DatabaseClient, Resource(get_configured_client)],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithResourceConfig()
    graph = get_workflow_representation(workflow=wf)

    resource_config_nodes = _resource_config_nodes(graph)
    assert len(resource_config_nodes) == 1
    config_node = resource_config_nodes[0]
    assert config_node.config_file == config_path
    assert config_node.path_selector == "database"


def test_resource_config_nodes_are_deduplicated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same resource config used by multiple resources appears once."""
    monkeypatch.chdir(tmp_path)

    _write_config(tmp_path, "config.json", {"setting": "test", "value": 42})

    def get_client_one(
        config: Annotated[ConfigData, ResourceConfig(config_file="config.json")],
    ) -> DatabaseClient:
        return DatabaseClient()

    def get_client_two(
        config: Annotated[ConfigData, ResourceConfig(config_file="config.json")],
    ) -> DatabaseClient:
        return DatabaseClient()

    class WorkflowWithSharedConfig(Workflow):
        @step
        async def step_one(
            self,
            ev: StartEvent,
            client: Annotated[DatabaseClient, Resource(get_client_one)],
        ) -> MiddleEvent:
            return MiddleEvent()

        @step
        async def step_two(
            self,
            ev: MiddleEvent,
            client: Annotated[DatabaseClient, Resource(get_client_two)],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithSharedConfig()
    graph = get_workflow_representation(workflow=wf)

    assert len(_resource_config_nodes(graph)) == 1
    assert len(_resource_nodes(graph)) == 2
    assert len(_find_edges(graph, target_prefix="resource_config_")) == 2


def test_multiple_different_resource_configs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple configs create distinct config nodes."""
    monkeypatch.chdir(tmp_path)

    db_path = _write_config(tmp_path, "db_config.json", {"setting": "db", "value": 1})
    cache_path = _write_config(
        tmp_path, "cache_config.json", {"setting": "cache", "value": 2}
    )

    def get_db_client(
        config: Annotated[ConfigData, ResourceConfig(config_file="db_config.json")],
    ) -> DatabaseClient:
        return DatabaseClient()

    class CacheClient:
        pass

    def get_cache_client(
        config: Annotated[ConfigData, ResourceConfig(config_file="cache_config.json")],
    ) -> CacheClient:
        return CacheClient()

    class WorkflowWithMultipleConfigs(Workflow):
        @step
        async def step_with_both(
            self,
            ev: StartEvent,
            db: Annotated[DatabaseClient, Resource(get_db_client)],
            cache: Annotated[CacheClient, Resource(get_cache_client)],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithMultipleConfigs()
    graph = get_workflow_representation(workflow=wf)

    config_files = {node.config_file for node in _resource_config_nodes(graph)}
    assert config_files == {db_path, cache_path}


def test_filter_by_node_type_with_resource_config() -> None:
    """Test that filter_by_node_type works with resource_config nodes."""
    graph = WorkflowGraph(
        name="TestWorkflow",
        nodes=[
            WorkflowStepNode(id="step1", label="Step 1"),
            WorkflowResourceNode(id="resource_123", label="Resource"),
            WorkflowResourceConfigNode(
                id="resource_config_456",
                label="Config",
                config_file="config.json",
            ),
        ],
        edges=[
            WorkflowGraphEdge(source="step1", target="resource_123", label="client"),
            WorkflowGraphEdge(
                source="resource_123", target="resource_config_456", label="config"
            ),
        ],
    )

    # Filter out resource_config nodes
    filtered = graph.filter_by_node_type("resource_config")

    assert len(filtered.nodes) == 2
    node_types = {n.node_type for n in filtered.nodes}
    assert node_types == {"step", "resource"}

    # Edge from resource to config should be removed
    # (no remaining node to connect to)
    assert len(filtered.edges) == 1
    assert filtered.edges[0].source == "step1"
    assert filtered.edges[0].target == "resource_123"


def test_resource_config_label_and_description(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ResourceConfig label and description are preserved in graph nodes."""
    monkeypatch.chdir(tmp_path)

    config_data = {"categories": ["invoice", "resume", "contract"]}
    _write_config(tmp_path, "classify.json", config_data)

    class WorkflowWithLabeledConfig(Workflow):
        @step
        async def classify_step(
            self,
            ev: StartEvent,
            config: Annotated[
                ConfigData,
                ResourceConfig(
                    config_file="classify.json",
                    label="Document Classifier",
                    description="Configuration for document type classification",
                ),
            ],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithLabeledConfig()
    graph = get_workflow_representation(workflow=wf)

    config_nodes = _resource_config_nodes(graph)
    assert len(config_nodes) == 1
    config_node = config_nodes[0]

    # Label should be used instead of type name
    assert config_node.label == "Document Classifier"
    assert config_node.description == "Configuration for document type classification"
    # Type name should still be preserved
    assert config_node.type_name == "ConfigData"


def test_resource_config_label_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ResourceConfig without label falls back to type name."""
    monkeypatch.chdir(tmp_path)

    config_data = {"value": 123}
    _write_config(tmp_path, "config.json", config_data)

    class WorkflowWithUnlabeledConfig(Workflow):
        @step
        async def step(
            self,
            ev: StartEvent,
            config: Annotated[ConfigData, ResourceConfig(config_file="config.json")],
        ) -> StopEvent:
            return StopEvent(result="done")

    wf = WorkflowWithUnlabeledConfig()
    graph = get_workflow_representation(workflow=wf)

    config_nodes = _resource_config_nodes(graph)
    assert len(config_nodes) == 1
    config_node = config_nodes[0]

    # Label should fall back to type name when not specified
    assert config_node.label == "ConfigData"
    assert config_node.description is None
