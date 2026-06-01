# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows.context import Context
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent
from workflows.representation.validate import build_step_graph
from workflows.workflow import Workflow

# ── Shared event hierarchy ──────────────────────────────────────────────────


class ParentEvent(Event):
    value: str


class ChildEvent(ParentEvent):
    pass


# ── Shared workflow: step_a emits ChildEvent, step_b expects ParentEvent ──


class SubclassRoutingWorkflow(Workflow):
    @step
    async def step_a(self, ev: StartEvent) -> ChildEvent:
        return ChildEvent(value="subclass works")

    @step
    async def step_b(self, ev: ParentEvent) -> StopEvent:
        return StopEvent(result=ev.value)


@pytest.mark.asyncio
async def test_subclass_event_routes_to_step_accepting_parent_event() -> None:
    """Validation should recognize ChildEvent satisfies ParentEvent consumption."""
    workflow = SubclassRoutingWorkflow(timeout=1)
    result = await workflow.run()
    assert result == "subclass works"


@pytest.mark.asyncio
async def test_subclass_runtime_routing() -> None:
    """Runtime should deliver ChildEvent to a step accepting ParentEvent."""
    workflow = SubclassRoutingWorkflow(timeout=2, disable_validation=True)
    result = await workflow.run()
    assert result == "subclass works"


def test_subclass_targeted_step_validation_accepts_child() -> None:
    """_validate_valid_step_message should accept ChildEvent for ParentEvent step."""
    workflow = SubclassRoutingWorkflow(disable_validation=True)
    workflow._validate_valid_step_message("step_b", ChildEvent(value="test"))


def test_subclass_graph_edges_connect_child_to_parent_step() -> None:
    """build_step_graph should create edges from ChildEvent to steps accepting ParentEvent."""
    workflow = SubclassRoutingWorkflow(disable_validation=True)
    steps = {name: func._step_config for name, func in workflow._get_steps().items()}
    graph = build_step_graph(steps, StartEvent)
    child_event_targets = graph.outgoing.get(ChildEvent, [])
    assert "step_b" in child_event_targets, (
        f"ChildEvent should connect to step_b, got: {child_event_targets}"
    )


@pytest.mark.asyncio
async def test_exact_type_event_routing_still_works() -> None:
    """Regression: workflows using exact event types must work unchanged."""

    class ExactTypeWorkflow(Workflow):
        @step
        async def step_a(self, ev: StartEvent) -> ParentEvent:
            return ParentEvent(value="exact match")

        @step
        async def step_b(self, ev: ParentEvent) -> StopEvent:
            return StopEvent(result=ev.value)

    workflow = ExactTypeWorkflow(timeout=1)
    result = await workflow.run()
    assert result == "exact match"


@pytest.mark.asyncio
async def test_subclass_waiter_accepts_child_event_for_parent_wait() -> None:
    """A waiter waiting for a ParentEvent should be resolved by a ChildEvent."""

    class WaiterWorkflow(Workflow):
        @step
        async def step_a(self, ev: StartEvent, ctx: Context) -> StopEvent:
            waiter = ctx.wait_for_event(ParentEvent)
            ctx.send_event(ChildEvent(value="waiter works"))
            result = await waiter
            return StopEvent(result=result.value)

    workflow = WaiterWorkflow(timeout=2, disable_validation=True)
    result = await workflow.run()
    assert result == "waiter works"
