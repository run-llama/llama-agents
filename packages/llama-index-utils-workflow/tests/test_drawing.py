import json
from pathlib import Path
from typing import Annotated, Any, cast
from unittest.mock import MagicMock, mock_open, patch

import pytest
from conftest import DummyWorkflow, ParentWorkflow, ResourceWorkflow
from llama_index.utils.workflow import (
    draw_all_possible_flows,
    draw_all_possible_flows_mermaid,
    draw_most_recent_execution,
    draw_most_recent_execution_mermaid,
)
from pydantic import BaseModel
from workflows.decorators import step
from workflows.events import StartEvent, StopEvent
from workflows.resource import Resource, ResourceConfig
from workflows.runtime.types.results import StepWorkerResult
from workflows.runtime.types.ticks import TickStepResult
from workflows.workflow import Workflow


@pytest.mark.asyncio
async def test_workflow_draw_methods(workflow: Workflow) -> None:
    with patch("llama_index.utils.workflow.Network") as mock_network:
        draw_all_possible_flows(workflow, filename="test_all_flows.html")
        mock_network.assert_called_once()
        mock_network.return_value.show.assert_called_once_with(
            "test_all_flows.html", notebook=False
        )

    handler = workflow.run()
    await handler
    with patch("llama_index.utils.workflow.Network") as mock_network:
        draw_most_recent_execution(handler, filename="test_recent_execution.html")
        mock_network.assert_called_once()
        mock_network.return_value.show.assert_called_once_with(
            "test_recent_execution.html", notebook=False
        )


def test_draw_all_possible_flows_with_max_label_length(
    workflow: Workflow,
) -> None:
    """Test the max_label_length parameter."""
    with patch("llama_index.utils.workflow.Network") as mock_network:
        mock_net_instance = MagicMock()
        mock_network.return_value = mock_net_instance

        # Test with max_label_length=10
        draw_all_possible_flows(
            workflow, filename="test_truncated.html", max_label_length=10
        )

        # Extract actual label mappings from add_node calls
        label_mappings = {}
        for call in mock_net_instance.add_node.call_args_list:
            _, kwargs = call
            label = kwargs.get("label")
            title = kwargs.get("title")

            # For items with titles (truncated), map title->label
            if title:
                label_mappings[title] = label
            # For items without titles (not truncated), map label->label
            elif label:
                label_mappings[label] = label

        # Test cases using actual events from DummyWorkflow fixture
        test_cases = [
            ("OneTestEvent", "OneTestEv*"),  # 12 chars -> truncated to 10
            ("LastEvent", "LastEvent"),  # 9 chars -> no truncation
            (
                "StartEvent",
                "StartEvent",
            ),  # 10 chars -> no truncation (exactly at limit)
            ("StopEvent", "StopEvent"),  # 9 chars -> no truncation
        ]

        # Verify actual results match expected for available test cases
        for original, expected_label in test_cases:
            if original in label_mappings:
                actual_label = label_mappings[original]
                assert actual_label == expected_label, (
                    f"Expected '{original}' to become '{expected_label}', but got '{actual_label}'"
                )
                assert len(actual_label) <= 10, (
                    f"Label '{actual_label}' exceeds max_label_length=10"
                )


def test_draw_all_possible_flows_mermaid_basic(workflow: Workflow) -> None:
    """Test basic Mermaid diagram generation."""
    with patch("builtins.open", mock_open()) as mock_file:
        result = draw_all_possible_flows_mermaid(
            workflow, filename="test_mermaid.mermaid"
        )

        # Verify file was written
        mock_file.assert_called_once_with("test_mermaid.mermaid", "w")

        # Verify basic structure
        assert isinstance(result, str)
        assert result.startswith("flowchart TD")

        # Verify contains style definitions
        assert "classDef stepStyle fill:#ADD8E6" in result
        assert "classDef startEventStyle fill:#E27AFF" in result
        assert "classDef stopEventStyle fill:#FFA07A" in result
        assert "classDef defaultEventStyle fill:#90EE90" in result
        assert "classDef externalStyle fill:#BEDAE4" in result


def test_draw_all_possible_flows_mermaid_no_file(workflow: Workflow) -> None:
    """Test Mermaid diagram generation without file output."""
    result = draw_all_possible_flows_mermaid(workflow)

    # Should still return the diagram string
    assert isinstance(result, str)
    assert result.startswith("flowchart TD")


def test_mermaid_node_shapes_and_styles(workflow: Workflow) -> None:
    """Test that Mermaid nodes have correct shapes and styles."""
    result = draw_all_possible_flows_mermaid(workflow)

    lines = result.split("\n")

    # Check for step nodes (should use box shape [...] and stepStyle)
    step_nodes = [line for line in lines if "step_" in line and ":::stepStyle" in line]
    for step_line in step_nodes:
        assert "[" in step_line and "]" in step_line, (
            f"Step node should use box shape: {step_line}"
        )
        assert ":::stepStyle" in step_line, (
            f"Step node should use stepStyle: {step_line}"
        )

    # Check for event nodes (should use ellipse shape ([...]) and event styles)
    event_nodes = [
        line
        for line in lines
        if "event_" in line
        and (
            ":::startEventStyle" in line
            or ":::stopEventStyle" in line
            or ":::defaultEventStyle" in line
        )
    ]
    for event_line in event_nodes:
        assert "([" in event_line and ")" in event_line, (
            f"Event node should use ellipse shape: {event_line}"
        )


def test_mermaid_edges_generation(workflow: Workflow) -> None:
    """Test that Mermaid edges are properly generated."""
    result = draw_all_possible_flows_mermaid(workflow)

    lines = result.split("\n")
    edge_lines = [line.strip() for line in lines if " --> " in line]

    # Should have at least some edges
    assert len(edge_lines) > 0, "Should generate at least some edges"

    # All edge lines should follow the pattern: source --> target
    for edge_line in edge_lines:
        assert edge_line.count(" --> ") == 1, (
            f"Edge should have exactly one arrow: {edge_line}"
        )
        source, target = edge_line.split(" --> ")
        assert source.strip(), f"Edge source should not be empty: {edge_line}"
        assert target.strip(), f"Edge target should not be empty: {edge_line}"


def test_mermaid_id_cleaning(workflow: Workflow) -> None:
    """Test that Mermaid IDs are properly cleaned for validity."""
    result = draw_all_possible_flows_mermaid(workflow)

    lines = result.split("\n")

    # Check that all node IDs are valid (no spaces, special chars that would break Mermaid)
    for line in lines:
        if line.strip().startswith(("step_", "event_", "external_step")):
            # Extract the ID (first word)
            parts = line.strip().split()
            if parts:
                node_id = parts[0]
                # Should not contain spaces, dots, or hyphens
                assert " " not in node_id, (
                    f"Node ID should not contain spaces: {node_id}"
                )
                assert "." not in node_id, f"Node ID should not contain dots: {node_id}"
                # Note: We allow underscores as they're valid in Mermaid


def test_mermaid_vs_pyvis_consistency(workflow: Workflow) -> None:
    """Test that Mermaid and Pyvis generate consistent node/edge counts."""
    # Generate Pyvis version
    with patch("llama_index.utils.workflow.Network") as mock_network:
        mock_net_instance = MagicMock()
        mock_network.return_value = mock_net_instance

        draw_all_possible_flows(workflow, filename="test.html")

        # Count unique nodes (Pyvis deduplicates automatically)
        pyvis_unique_nodes = set()
        for call in mock_net_instance.add_node.call_args_list:
            args, kwargs = call
            node_id = args[0]  # First argument is the node ID
            pyvis_unique_nodes.add(node_id)

        pyvis_edge_calls = len(mock_net_instance.add_edge.call_args_list)

    # Generate Mermaid version
    mermaid_result = draw_all_possible_flows_mermaid(workflow)
    lines = mermaid_result.split("\n")

    # Count Mermaid nodes (lines with node definitions, but NOT edge lines)
    mermaid_node_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Node lines start with step_, event_, or external_step BUT don't contain -->
        if " --> " not in line and line.startswith(
            ("step_", "event_", "external_step")
        ):
            mermaid_node_lines.append(line)

    # Count Mermaid edges (lines with arrows)
    mermaid_edge_lines = [line for line in lines if " --> " in line]

    # Should have same number of nodes and edges
    assert len(mermaid_node_lines) == len(pyvis_unique_nodes), (
        f"Mermaid nodes ({len(mermaid_node_lines)}) should match Pyvis unique nodes ({len(pyvis_unique_nodes)})"
    )
    assert len(mermaid_edge_lines) == pyvis_edge_calls, (
        f"Mermaid edges ({len(mermaid_edge_lines)}) should match Pyvis edges ({pyvis_edge_calls})"
    )


def test_mermaid_file_writing(workflow: Workflow) -> None:
    """Test that Mermaid diagram is correctly written to file."""
    mock_file_handle = mock_open()

    with patch("builtins.open", mock_file_handle):
        result = draw_all_possible_flows_mermaid(
            workflow, filename="test_output.mermaid"
        )

        # Verify file was opened for writing
        mock_file_handle.assert_called_once_with("test_output.mermaid", "w")

        # Verify content was written
        written_content = "".join(
            call.args[0] for call in mock_file_handle().write.call_args_list
        )

        assert written_content == result, "File content should match returned string"
        assert written_content.startswith("flowchart TD"), (
            "File should contain valid Mermaid syntax"
        )


def test_mermaid_empty_filename(workflow: Workflow) -> None:
    """Test that Mermaid works with empty/None filename."""
    # Test without filename (defaults internally)
    result1 = draw_all_possible_flows_mermaid(workflow)
    assert isinstance(result1, str)
    assert result1.startswith("flowchart TD")

    # Test with empty string
    result2 = draw_all_possible_flows_mermaid(workflow, filename="")
    assert isinstance(result2, str)
    assert result2.startswith("flowchart TD")

    # Both should be identical
    assert result1 == result2


@pytest.mark.asyncio
async def test_draw_most_recent_execution_mermaid(workflow: Workflow) -> None:
    """Test Mermaid diagram generation for the most recent execution."""
    handler = workflow.run()
    await handler

    with patch("builtins.open", mock_open()) as mock_file:
        result = draw_most_recent_execution_mermaid(
            handler, filename="test_recent.mermaid"
        )

        # Verify file was written
        mock_file.assert_called_once_with("test_recent.mermaid", "w")

        # Verify basic structure
        assert isinstance(result, str)
        assert result.startswith("flowchart TD")

        # Verify it contains style definitions
        assert "classDef stepStyle fill:#ADD8E6" in result
        assert "classDef startEventStyle fill:#E27AFF" in result
        assert "classDef stopEventStyle fill:#FFA07A" in result
        assert "classDef defaultEventStyle fill:#90EE90" in result
        assert "classDef externalStyle fill:#BEDAE4" in result

        # Verify it contains nodes and edges
        lines = result.split("\n")
        node_lines = [line for line in lines if ":::" in line]
        edge_lines = [line for line in lines if " --> " in line]
        assert len(node_lines) > 0
        assert len(edge_lines) > 0


@pytest.mark.asyncio
async def test_draw_most_recent_execution_mermaid_sanitizes_slash_step_ids(
    workflow: Workflow,
) -> None:
    handler = workflow.run()
    await handler

    adapter = cast(Any, handler.ctx._face)._external_adapter
    adapter._queues.ticks = [
        TickStepResult(
            step_name="parent/child",
            worker_id=0,
            event=StartEvent(),
            result=[StepWorkerResult(result=StopEvent())],
        )
    ]

    result = draw_most_recent_execution_mermaid(handler, filename="")

    assert 'step_parent_child_1["parent/child#1"]:::stepStyle' in result
    assert "step_parent/child_1" not in result


# --- Resource node rendering tests ---


def test_mermaid_resource_nodes_rendered(
    workflow_with_resources: Workflow,
) -> None:
    """Test that resource nodes are rendered in Mermaid output."""
    result = draw_all_possible_flows_mermaid(workflow_with_resources)

    # Verify resource style is defined
    assert "classDef resourceStyle fill:#DDA0DD" in result

    # Verify resource nodes are present (hexagon shape with {{}})
    lines = result.split("\n")
    resource_lines = [line for line in lines if "resource_" in line and ":::" in line]
    assert len(resource_lines) > 0

    # Check resource nodes use hexagon shape
    for line in resource_lines:
        assert "{{" in line and "}}" in line, (
            f"Resource node should use hexagon shape: {line}"
        )
        assert ":::resourceStyle" in line, (
            f"Resource node should use resourceStyle: {line}"
        )


def test_mermaid_resource_edges_have_labels(
    workflow_with_resources: Workflow,
) -> None:
    """Test that edges from resources to steps have labels (variable names)."""
    result = draw_all_possible_flows_mermaid(workflow_with_resources)

    lines = result.split("\n")
    # Look for edges with labels: resource_xxx -->|"var_name"| step_yyy
    labeled_edge_lines = [line for line in lines if '-->|"' in line]

    # Should have labeled edges for resource connections
    assert len(labeled_edge_lines) > 0

    # Check that the labels are variable names
    expected_labels = {"db_client", "db", "cache"}
    found_labels = set()
    for line in labeled_edge_lines:
        # Extract label from -->|"label"|
        if '-->|"' in line:
            start = line.index('-->|"') + 5
            end = line.index('"|', start)
            label = line[start:end]
            found_labels.add(label)

    assert found_labels.intersection(expected_labels), (
        f"Expected some of {expected_labels}, found {found_labels}"
    )


def test_pyvis_resource_nodes_rendered(workflow_with_resources: Workflow) -> None:
    """Test that resource nodes are rendered in Pyvis output."""
    with patch("llama_index.utils.workflow.Network") as mock_network:
        mock_net_instance = MagicMock()
        mock_network.return_value = mock_net_instance

        draw_all_possible_flows(workflow_with_resources, filename="test.html")

        # Extract all add_node calls
        node_calls = mock_net_instance.add_node.call_args_list

        # Find resource nodes (should have hexagon shape and plum color)
        resource_nodes = []
        for call in node_calls:
            args, kwargs = call
            node_id = args[0]
            if "resource_" in node_id:
                resource_nodes.append((node_id, kwargs))

        assert len(resource_nodes) > 0, "Should have resource nodes"

        for node_id, kwargs in resource_nodes:
            assert kwargs.get("shape") == "hexagon", (
                f"Resource node {node_id} should be hexagon"
            )
            assert kwargs.get("color") == "#DDA0DD", (
                f"Resource node {node_id} should be plum color"
            )
            # Should have a title with metadata
            assert kwargs.get("title") is not None, (
                f"Resource node {node_id} should have title"
            )


def test_pyvis_resource_edges_have_labels(
    workflow_with_resources: Workflow,
) -> None:
    """Test that Pyvis edges from resources have labels."""
    with patch("llama_index.utils.workflow.Network") as mock_network:
        mock_net_instance = MagicMock()
        mock_network.return_value = mock_net_instance

        draw_all_possible_flows(workflow_with_resources, filename="test.html")

        # Extract all add_edge calls
        edge_calls = mock_net_instance.add_edge.call_args_list

        # Find edges with labels
        labeled_edges = []
        for call in edge_calls:
            args, kwargs = call
            if "label" in kwargs:
                labeled_edges.append((args, kwargs["label"]))

        assert len(labeled_edges) > 0, "Should have labeled edges"

        # Check that labels are variable names
        labels = {label for _, label in labeled_edges}
        expected_labels = {"db_client", "db", "cache"}
        assert labels.intersection(expected_labels), (
            f"Expected some of {expected_labels}, found {labels}"
        )


def test_mermaid_resource_style_always_defined(workflow: Workflow) -> None:
    """Test that resourceStyle is always defined even for workflows without resources."""
    result = draw_all_possible_flows_mermaid(workflow)

    # resourceStyle should be defined even if not used
    assert "classDef resourceStyle fill:#DDA0DD" in result


def test_mermaid_resource_config_nodes_rendered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that resource config nodes are rendered in Mermaid output."""
    monkeypatch.chdir(tmp_path)

    config_data = {"setting": "test", "value": 1}
    with open("config.json", "w") as f:
        json.dump(config_data, f)

    class ConfigData(BaseModel):
        setting: str
        value: int

    class Client:
        pass

    def get_client(
        config: Annotated[ConfigData, ResourceConfig(config_file="config.json")],
    ) -> Client:
        return Client()

    class WorkflowWithConfig(Workflow):
        @step
        async def start_step(
            self,
            ev: StartEvent,
            client: Annotated[Client, Resource(get_client)],
        ) -> StopEvent:
            return StopEvent(result="done")

    result = draw_all_possible_flows_mermaid(WorkflowWithConfig())

    assert "classDef resourceConfigStyle fill:#B2DFDB" in result
    lines = result.split("\n")
    resource_config_lines = [
        line
        for line in lines
        if "resource_config_" in line and ":::" in line and " --> " not in line
    ]
    assert len(resource_config_lines) == 1
    assert ":::resourceConfigStyle" in resource_config_lines[0]


def test_pyvis_resource_config_nodes_rendered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that resource config nodes are rendered in Pyvis output."""
    monkeypatch.chdir(tmp_path)

    config_data = {"setting": "test", "value": 1}
    with open("config.json", "w") as f:
        json.dump(config_data, f)

    class ConfigData(BaseModel):
        setting: str
        value: int

    class Client:
        pass

    def get_client(
        config: Annotated[ConfigData, ResourceConfig(config_file="config.json")],
    ) -> Client:
        return Client()

    class WorkflowWithConfig(Workflow):
        @step
        async def start_step(
            self,
            ev: StartEvent,
            client: Annotated[Client, Resource(get_client)],
        ) -> StopEvent:
            return StopEvent(result="done")

    with patch("llama_index.utils.workflow.Network") as mock_network:
        mock_net_instance = MagicMock()
        mock_network.return_value = mock_net_instance

        draw_all_possible_flows(WorkflowWithConfig(), filename="test.html")

        node_calls = mock_net_instance.add_node.call_args_list
        resource_config_nodes = []
        for call in node_calls:
            args, kwargs = call
            node_id = args[0]
            if "resource_config_" in node_id:
                resource_config_nodes.append((node_id, kwargs))

        assert len(resource_config_nodes) == 1, "Should have resource config node"
        node_id, kwargs = resource_config_nodes[0]
        assert kwargs.get("shape") == "box", (
            f"Resource config node {node_id} should be box"
        )
        assert kwargs.get("color") == "#B2DFDB", (
            f"Resource config node {node_id} should be light teal"
        )


def test_resource_node_deduplication_in_rendering(
    workflow_with_resources: Workflow,
) -> None:
    """Test that deduplicated resource nodes render correctly."""
    result = draw_all_possible_flows_mermaid(workflow_with_resources)

    lines = result.split("\n")

    # Count unique resource node definitions (not edges)
    resource_node_defs = [
        line
        for line in lines
        if "resource_" in line
        and ":::" in line
        and " --> " not in line
        and "-->|" not in line
    ]

    # The workflow has 2 unique resources (DatabaseClient used twice, CacheClient once)
    # So we should see exactly 2 resource node definitions
    assert len(resource_node_defs) == 2, (
        f"Expected 2 unique resource nodes, found {len(resource_node_defs)}: {resource_node_defs}"
    )


def test_draw_all_possible_flows_with_child_workflow_mermaid(
    nested_workflow: Workflow,
) -> None:
    """Test Mermaid diagram generation for nested workflows."""
    result = draw_all_possible_flows_mermaid(
        nested_workflow, include_child_workflows=True
    )

    # Basic checks
    assert isinstance(result, str)
    assert result.startswith("flowchart TD")

    # Check for parent workflow nodes
    assert "step_parent_start" in result
    assert "step_parent_end" in result

    # Check for child workflow nodes (prefixed)
    assert "step_parent_start_ChildWorkflowA_child_start" in result
    assert "event_parent_start_ChildWorkflowA_StartEvent" in result
    assert "event_parent_start_ChildWorkflowA_StopEvent" in result

    # Check for connector nodes (calls/returns are now nodes, not edge labels)
    assert "child_connector_parent_start_ChildWorkflowA_calls" in result
    assert "calls: ChildWorkflowA" in result
    assert "child_connector_parent_start_ChildWorkflowA_returns" in result
    assert "returns: ChildWorkflowA" in result

    # Parent step -> calls node -> child StartEvent
    assert (
        "step_parent_start --> child_connector_parent_start_ChildWorkflowA_calls"
        in result
    )
    assert (
        "child_connector_parent_start_ChildWorkflowA_calls --> event_parent_start_ChildWorkflowA_StartEvent"
        in result
    )
    # Child StopEvent -> returns node -> parent step
    assert (
        "event_parent_start_ChildWorkflowA_StopEvent --> child_connector_parent_start_ChildWorkflowA_returns"
        in result
    )
    assert (
        "child_connector_parent_start_ChildWorkflowA_returns --> step_parent_start"
        in result
    )


def test_draw_all_possible_flows_with_child_workflow_pyvis(
    nested_workflow: Workflow,
) -> None:
    """Test Pyvis diagram generation includes nested child workflow nodes and edges."""
    with patch("llama_index.utils.workflow.Network") as mock_network:
        mock_net_instance = MagicMock()
        mock_network.return_value = mock_net_instance

        draw_all_possible_flows(
            nested_workflow, filename="test.html", include_child_workflows=True
        )

        node_ids = [call[0][0] for call in mock_net_instance.add_node.call_args_list]

        # Parent nodes present
        assert "parent_start" in node_ids
        assert "parent_end" in node_ids

        # Child workflow nodes present (prefixed)
        assert "parent_start_ChildWorkflowA_child_start" in node_ids
        assert "parent_start_ChildWorkflowA_StartEvent" in node_ids
        assert "parent_start_ChildWorkflowA_StopEvent" in node_ids

        # Connector nodes present
        assert "parent_start_ChildWorkflowA_calls" in node_ids
        assert "parent_start_ChildWorkflowA_returns" in node_ids

        # Check stitching edges through connector nodes
        edge_pairs = [
            (call[0][0], call[0][1])
            for call in mock_net_instance.add_edge.call_args_list
        ]
        # parent_start -> calls node -> child StartEvent
        assert ("parent_start", "parent_start_ChildWorkflowA_calls") in edge_pairs
        assert (
            "parent_start_ChildWorkflowA_calls",
            "parent_start_ChildWorkflowA_StartEvent",
        ) in edge_pairs
        # child StopEvent -> returns node -> parent_start
        assert (
            "parent_start_ChildWorkflowA_StopEvent",
            "parent_start_ChildWorkflowA_returns",
        ) in edge_pairs
        assert ("parent_start_ChildWorkflowA_returns", "parent_start") in edge_pairs


# --- Tests using workflow classes (not instances) ---


def test_draw_all_possible_flows_mermaid_with_class() -> None:
    """Test that draw_all_possible_flows_mermaid accepts a workflow class."""
    result = draw_all_possible_flows_mermaid(DummyWorkflow)

    assert isinstance(result, str)
    assert result.startswith("flowchart TD")
    assert "start_step" in result
    assert "middle_step" in result
    assert "end_step" in result


def test_draw_all_possible_flows_with_class() -> None:
    """Test that draw_all_possible_flows accepts a workflow class."""
    with patch("llama_index.utils.workflow.Network") as mock_network:
        draw_all_possible_flows(DummyWorkflow, filename="test_class.html")
        mock_network.assert_called_once()
        mock_network.return_value.show.assert_called_once_with(
            "test_class.html", notebook=False
        )


def test_class_and_instance_produce_same_mermaid() -> None:
    """Test that passing a class or an instance produces the same diagram."""
    result_class = draw_all_possible_flows_mermaid(DummyWorkflow)
    result_instance = draw_all_possible_flows_mermaid(DummyWorkflow())

    assert result_class == result_instance


def test_draw_all_possible_flows_mermaid_with_resource_class() -> None:
    """Test that draw_all_possible_flows_mermaid accepts a workflow class with resources."""
    result = draw_all_possible_flows_mermaid(ResourceWorkflow)

    assert "classDef resourceStyle fill:#DDA0DD" in result
    lines = result.split("\n")
    resource_lines = [line for line in lines if "resource_" in line and ":::" in line]
    assert len(resource_lines) > 0


def test_draw_all_possible_flows_with_child_workflow_class_mermaid() -> None:
    """Test Mermaid diagram generation for nested workflows using class."""
    result = draw_all_possible_flows_mermaid(
        ParentWorkflow, include_child_workflows=True
    )

    assert isinstance(result, str)
    assert "step_parent_start" in result
    assert "step_parent_end" in result
