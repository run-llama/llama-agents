# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import Context, Workflow, step
from workflows.errors import WorkflowRuntimeError, WorkflowValidationError
from workflows.events import StartEvent, StopEvent
from workflows.runtime.control_loop.streams import _child_slots_accepting
from workflows.runtime.types.internal_state import BrokerState


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    pass


class OtherChildStop(StopEvent):
    pass


class FirstChild(Workflow):
    @step
    async def first(self, ev: ChildStart) -> ChildStop:
        return ChildStop()


class SecondChild(Workflow):
    @step
    async def second(self, ev: ChildStart) -> OtherChildStop:
        return OtherChildStop()


class ParentWithDuplicateChildStarts(Workflow):
    first: FirstChild
    second: SecondChild

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


def test_duplicate_direct_child_start_event_validation_error() -> None:
    wf = ParentWithDuplicateChildStarts(first=FirstChild(), second=SecondChild())

    with pytest.raises(
        WorkflowValidationError,
        match=(
            "Child workflows 'first' and 'second'.*"
            "both accept StartEvent type 'ChildStart'"
        ),
    ):
        wf.validate()


class DerivedChildStart(ChildStart):
    pass


class BaseStartChild(Workflow):
    @step(accept_event_subclasses=True)
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop()


class DerivedStartChild(Workflow):
    @step
    async def run_child(self, ev: DerivedChildStart) -> OtherChildStop:
        return OtherChildStop()


class ParentWithOverlappingStarts(Workflow):
    first: BaseStartChild
    second: DerivedStartChild

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


class ParentWithReversedOverlappingStarts(Workflow):
    first: DerivedStartChild
    second: BaseStartChild

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


@pytest.mark.parametrize("reverse", [False, True])
def test_child_start_inheritance_overlap_is_rejected(reverse: bool) -> None:
    wf = (
        ParentWithReversedOverlappingStarts(
            first=DerivedStartChild(), second=BaseStartChild()
        )
        if reverse
        else ParentWithOverlappingStarts(
            first=BaseStartChild(), second=DerivedStartChild()
        )
    )
    with pytest.raises(WorkflowValidationError, match="overlapping StartEvent types"):
        wf.validate()


def test_runtime_rejects_multiple_accepting_children_without_validation() -> None:
    wf = ParentWithOverlappingStarts(first=BaseStartChild(), second=DerivedStartChild())
    config = BrokerState.from_workflow(wf).config
    with pytest.raises(WorkflowRuntimeError, match="matches multiple child workflows"):
        _child_slots_accepting(config, DerivedChildStart())


class CycleAStart(StartEvent):
    pass


class CycleAStop(StopEvent):
    pass


class CycleBStart(StartEvent):
    pass


class CycleBStop(StopEvent):
    pass


class CycleA(Workflow):
    @step
    async def a_step(self, ev: CycleAStart) -> CycleAStop:
        return CycleAStop()


class CycleB(Workflow):
    a: CycleA

    @step
    async def b_step(self, ev: CycleBStart) -> CycleBStop:
        return CycleBStop()


class BoundaryChildStart(StartEvent):
    pass


class BoundaryChildStop(StopEvent):
    answer: str = ""


class SubBoundaryChildStart(BoundaryChildStart):
    pass


class BoundaryChild(Workflow):
    @step(accept_event_subclasses=True)
    async def run_child(self, ev: BoundaryChildStart) -> BoundaryChildStop:
        return BoundaryChildStop(answer="ok")


class SubclassTriggerParent(Workflow):
    """Parent triggers the child by returning an accepted *subclass* of its
    StartEvent — the boundary is not the exact declared StartEvent type."""

    child: BoundaryChild

    @step
    async def start(self, ev: StartEvent) -> SubBoundaryChildStart:
        return SubBoundaryChildStart()

    @step
    async def finish(self, ev: BoundaryChildStop) -> StopEvent:
        return StopEvent(result=ev.answer)


class SendEventTriggerParent(Workflow):
    """Parent triggers the child via ``ctx.send_event`` rather than a bare
    return. The child's StopEvent is still consumed by a parent step."""

    child: BoundaryChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> BoundaryChildStart | None:
        ctx.send_event(BoundaryChildStart())
        return None

    @step
    async def finish(self, ev: BoundaryChildStop) -> StopEvent:
        return StopEvent(result=ev.answer)


@pytest.mark.asyncio
async def test_child_boundary_detected_for_subclass_start_trigger() -> None:
    """A parent consuming the child's StopEvent validates and runs when the
    child is triggered by an accepted subclass of its StartEvent."""
    wf = SubclassTriggerParent(child=BoundaryChild())
    wf.validate()
    assert await wf.run() == "ok"


@pytest.mark.asyncio
async def test_child_boundary_detected_for_send_event_trigger() -> None:
    """A parent consuming the child's StopEvent validates and runs when the
    child is triggered via ctx.send_event."""
    wf = SendEventTriggerParent(child=BoundaryChild())
    wf.validate()
    assert await wf.run() == "ok"


def test_child_workflow_type_cycle_validation_error() -> None:
    CycleA.__annotations__["b"] = CycleB
    CycleA._child_workflow_slots_cache = None
    try:
        with pytest.raises(
            WorkflowValidationError,
            match=("Child workflow type cycle detected: CycleA -> CycleB -> CycleA"),
        ):
            CycleA().validate()
    finally:
        CycleA.__annotations__.pop("b", None)
        CycleA._child_workflow_slots_cache = None
