# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows.context import Context
from workflows.decorators import step
from workflows.errors import (
    WorkflowRuntimeError,
    WorkflowTimeoutError,
    WorkflowValidationError,
)
from workflows.events import Event, StartEvent, StopEvent
from workflows.representation.validate import build_step_graph
from workflows.workflow import Workflow

# ── Shared event hierarchy ──────────────────────────────────────────────────


class ParentEvent(Event):
    value: str


class ChildEvent(ParentEvent):
    pass


# ── Shared workflows for testing subclass-aware routing ───────────────────────


class DefaultExactWorkflow(Workflow):
    @step
    async def step_a(self, ev: StartEvent) -> ChildEvent:
        return ChildEvent(value="x")

    @step
    async def step_b(self, ev: ParentEvent) -> StopEvent:
        return StopEvent(result=ev.value)


class OptInSubclassWorkflow(Workflow):
    @step
    async def step_a(self, ev: StartEvent) -> ChildEvent:
        return ChildEvent(value="subclass works")

    @step(accept_event_subclasses=True)
    async def step_b(self, ev: ParentEvent) -> StopEvent:
        return StopEvent(result=ev.value)


class MixedWorkflow(Workflow):
    @step
    async def step_a(self, ev: StartEvent) -> ChildEvent:
        return ChildEvent(value="x")

    @step
    async def step_b(self, ev: ParentEvent) -> StopEvent:
        return StopEvent(result=ev.value)

    @step(accept_event_subclasses=True)
    async def step_c(self, ev: ParentEvent) -> StopEvent:
        return StopEvent(result=ev.value)


# ── Test 1: Default step rejects subclass event (Strict validation & timeout) ─


def test_default_step_validation_rejects_subclass_event() -> None:
    """Without accept_event_subclasses=True, validation detects ParentEvent as unproduced."""
    workflow = DefaultExactWorkflow(timeout=1)
    with pytest.raises(WorkflowValidationError) as excinfo:
        workflow._validate()
    assert "consumed but never produced: ParentEvent" in str(excinfo.value)


@pytest.mark.asyncio
async def test_default_step_runtime_ignores_subclass_event() -> None:
    """Without accept_event_subclasses=True, runtime routing ignores ChildEvent and times out."""
    workflow = DefaultExactWorkflow(timeout=1, disable_validation=True)
    with pytest.raises(WorkflowTimeoutError):
        await workflow.run()


# ── Test 2: Opt-in step accepts subclass event ───────────────────────────────


@pytest.mark.asyncio
async def test_opt_in_step_accepts_subclass_event() -> None:
    """With accept_event_subclasses=True, workflow successfully routes ChildEvent."""
    workflow = OptInSubclassWorkflow(timeout=2)
    result = await workflow.run()
    assert result == "subclass works"


# ── Test 3: Graph edges follow opt-in subclass matching ──────────────────────


def test_graph_edges_follow_opt_in_subclass_matching() -> None:
    """build_step_graph only builds subclass edges for steps that opt-in."""
    workflow = MixedWorkflow(disable_validation=True)
    steps = {name: func._step_config for name, func in workflow._get_steps().items()}
    graph = build_step_graph(steps, StartEvent)

    child_event_targets = graph.outgoing.get(ChildEvent, [])
    # Should connect to step_c (opt-in) but NOT to step_b (default)
    assert "step_c" in child_event_targets
    assert "step_b" not in child_event_targets


# ── Test 4: Targeted step validation respects opt-in subclass matching ────────


def test_targeted_step_validation_respects_opt_in_subclass_matching() -> None:
    """_validate_valid_step_message rejects subclass event by default, accepts on opt-in."""
    workflow = MixedWorkflow(disable_validation=True)

    # Default step_b rejects ChildEvent
    with pytest.raises(WorkflowRuntimeError) as excinfo:
        workflow._validate_valid_step_message("step_b", ChildEvent(value="x"))
    assert "does not accept event of type" in str(excinfo.value)

    # Opt-in step_c accepts ChildEvent without error
    workflow._validate_valid_step_message("step_c", ChildEvent(value="x"))


# ── Test 5: Exact-type event routing still works (Regression Guard) ──────────


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


# ── Test 6: Waiter respects opt-in subclass matching ──────────────────────────


@pytest.mark.asyncio
async def test_waiter_respects_opt_in_subclass_matching_default_ignores() -> None:
    """A waiter created inside a default step ignores subclass event and times out."""

    class WaiterDefaultWorkflow(Workflow):
        @step
        async def step_a(self, ev: StartEvent, ctx: Context) -> StopEvent:
            waiter = ctx.wait_for_event(ParentEvent)
            ctx.send_event(ChildEvent(value="waiter default"))
            result = await waiter
            return StopEvent(result=result.value)

    workflow = WaiterDefaultWorkflow(timeout=1, disable_validation=True)
    with pytest.raises(WorkflowTimeoutError):
        await workflow.run()


@pytest.mark.asyncio
async def test_waiter_respects_opt_in_subclass_matching_opt_in_resolves() -> None:
    """A waiter created inside an opted-in step resolves with subclass event."""

    class WaiterOptInWorkflow(Workflow):
        @step(accept_event_subclasses=True)
        async def step_a(self, ev: StartEvent, ctx: Context) -> StopEvent:
            waiter = ctx.wait_for_event(ParentEvent)
            ctx.send_event(ChildEvent(value="waiter opt-in"))
            result = await waiter
            return StopEvent(result=result.value)

    workflow = WaiterOptInWorkflow(timeout=2, disable_validation=True)
    result = await workflow.run()
    assert result == "waiter opt-in"


# ── Test 7: StopEvent is excluded from subclass graph expansion ───────────────


def test_step_graph_excludes_stop_event_from_subclass_expansion() -> None:
    """A catch-all opted-in step (accepting ``Event``) gets edges from regular
    events but not from StopEvent: a returned StopEvent terminates the run
    instead of routing."""

    class CatchAllWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> ChildEvent:
            return ChildEvent(value="x")

        @step(accept_event_subclasses=True)
        async def observe(self, ev: Event) -> StopEvent | None:
            if isinstance(ev, ChildEvent):
                return StopEvent(result=ev.value)
            return None

    workflow = CatchAllWorkflow(disable_validation=True)
    steps = {name: func._step_config for name, func in workflow._get_steps().items()}
    graph = build_step_graph(steps, StartEvent)

    assert "observe" in graph.outgoing.get(ChildEvent, [])
    assert "observe" in graph.outgoing.get(StartEvent, [])
    assert "observe" not in graph.outgoing.get(StopEvent, [])
