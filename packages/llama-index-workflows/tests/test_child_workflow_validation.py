# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from inspect import signature
from typing import Annotated, Any, cast
from typing import Annotated as A

import pytest
from workflows import ChildWorkflow, Workflow, step
from workflows import ChildWorkflow as CW
from workflows.errors import WorkflowRuntimeError, WorkflowValidationError
from workflows.events import StartEvent, StopEvent
from workflows.plugins.basic import BasicRuntime


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
    first: Annotated[FirstChild, ChildWorkflow]
    second: Annotated[SecondChild, ChildWorkflow]

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


def test_duplicate_direct_child_start_event_validation_error() -> None:
    wf = cast(Any, ParentWithDuplicateChildStarts)(
        first=FirstChild(), second=SecondChild()
    )

    with pytest.raises(
        WorkflowValidationError,
        match=(
            "Child workflows 'first' and 'second'.*"
            "both accept StartEvent type 'ChildStart'"
        ),
    ):
        wf.validate()


class ParentWithChild(Workflow):
    child: Annotated[FirstChild, ChildWorkflow]

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


def test_declared_child_execution_fails_before_recursive_runtime() -> None:
    wf = cast(Any, ParentWithChild)(child=FirstChild())

    with pytest.raises(WorkflowRuntimeError, match="recursive child runtime"):
        wf.run()


@pytest.mark.asyncio
async def test_plain_workflow_helper_and_unrelated_annotations_keep_run_compatible() -> (
    None
):
    class WorkflowWithHelperFields(Workflow):
        helper: FirstChild
        unrelated: int

        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="done")

    WorkflowWithHelperFields.__annotations__ = {
        "helper": "FirstChild",
        "unrelated": "1 / 0",
    }
    wf = WorkflowWithHelperFields()
    wf.helper = FirstChild()
    assert wf.child_workflows == {}
    assert await wf.run() == "done"


def test_annotated_and_marker_aliases_ignore_unrelated_unresolved_annotation() -> None:
    class Parent(Workflow):
        child: A[FirstChild, CW]
        unrelated: int

        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent()

    Parent.__annotations__["unrelated"] = "MissingHelper"
    child = FirstChild()
    wf = cast(Any, Parent)(child=child)
    assert wf.child_workflows == {"child": child}
    assert signature(Parent).parameters["child"].annotation is FirstChild


def test_invalid_marked_child_type_fails_loudly() -> None:
    with pytest.raises(WorkflowValidationError, match="must name a Workflow subclass"):

        class InvalidChild(Workflow):
            child: Annotated[int, ChildWorkflow]


def test_unresolved_marked_child_fails_loudly() -> None:
    class UnresolvedChild(Workflow):
        child: Annotated[FirstChild, ChildWorkflow]

        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent()

    UnresolvedChild.__annotations__ = {
        "child": "Annotated[MissingChildType, ChildWorkflow]"
    }
    with pytest.raises(
        WorkflowValidationError, match="Could not resolve child workflow annotation"
    ):
        UnresolvedChild()


class CycleAStart(StartEvent):
    pass


class CycleAStop(StopEvent):
    pass


class CycleBStart(StartEvent):
    pass


class CycleBStop(StopEvent):
    pass


class CycleA(Workflow):
    b: Annotated["CycleB", ChildWorkflow]

    @step
    async def a_step(self, ev: CycleAStart) -> CycleAStop:
        return CycleAStop()


class CycleB(Workflow):
    a: Annotated[CycleA, ChildWorkflow]

    @step
    async def b_step(self, ev: CycleBStart) -> CycleBStop:
        return CycleBStop()


def test_child_workflow_type_cycle_validation_error() -> None:
    with pytest.raises(
        WorkflowValidationError,
        match=("Child workflow type cycle detected: CycleA -> CycleB -> CycleA"),
    ):
        CycleA().validate()


def test_missing_declared_child_fails_before_execution() -> None:
    with pytest.raises(WorkflowValidationError, match="Missing child workflow.*child"):
        cast(Any, ParentWithChild)()


def test_non_workflow_declared_child_fails_validation() -> None:
    with pytest.raises(WorkflowValidationError, match="must be a Workflow instance"):
        cast(Any, ParentWithChild)(child=42)


def test_attached_child_reassignment_is_rejected() -> None:
    original = FirstChild()
    wf = cast(Any, ParentWithChild)(child=original)
    with pytest.raises(WorkflowValidationError, match="cannot be reassigned"):
        wf.child = FirstChild()
    assert wf.child_workflows["child"] is original
    assert wf.validate() is False


class ForwardParent(Workflow):
    child: Annotated["ForwardChild", ChildWorkflow]

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


class ForwardChild(FirstChild):
    pass


def test_forward_string_child_annotation_gets_synthesized_constructor() -> None:
    child = ForwardChild()
    wf = cast(Any, ForwardParent)(child=child)
    assert wf.child_workflows == {"child": child}
    assert wf._timeout == 45.0
    assert signature(ForwardParent).parameters["child"].annotation is ForwardChild
    assert wf.validate() is False


class ParentWithCustomInit(Workflow):
    child: Annotated[FirstChild, ChildWorkflow]

    def __init__(self, child: FirstChild) -> None:
        super().__init__()
        self.child = child

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


def test_custom_constructor_attaches_child_after_super_init() -> None:
    child = FirstChild()
    wf = ParentWithCustomInit(child)
    assert wf.child_workflows == {"child": child}
    assert wf.validate() is False


class MiddleStart(StartEvent):
    pass


class MiddleStop(StopEvent):
    pass


class MiddleChild(Workflow):
    leaf: Annotated[FirstChild, ChildWorkflow]

    @step
    async def middle(self, ev: MiddleStart) -> MiddleStop:
        return MiddleStop()


class RootWithMiddle(Workflow):
    middle: Annotated[MiddleChild, ChildWorkflow]

    @step
    async def root(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


def test_moving_nested_tree_registers_grandchild_with_new_runtime() -> None:
    old_runtime = BasicRuntime()
    new_runtime = BasicRuntime()
    leaf = FirstChild(runtime=old_runtime)
    middle = cast(Any, MiddleChild)(leaf=leaf, runtime=old_runtime)
    root = cast(Any, RootWithMiddle)(middle=middle, runtime=new_runtime)

    assert root.child_workflows["middle"].child_workflows["leaf"] is leaf
    assert all(wf.runtime is new_runtime for wf in (root, middle, leaf))
    assert all(wf in new_runtime._pending for wf in (root, middle, leaf))
    assert all(wf not in old_runtime._pending for wf in (middle, leaf))


class BoundaryStart(StartEvent):
    pass


class BoundaryStop(StopEvent):
    pass


class BoundaryChild(Workflow):
    @step
    async def child_step(self, ev: BoundaryStart) -> BoundaryStop:
        return BoundaryStop()


class BoundaryParent(Workflow):
    child: Annotated[BoundaryChild, ChildWorkflow]

    @step
    async def start(self, ev: StartEvent) -> BoundaryStart:
        return BoundaryStart()

    @step
    async def finish(self, ev: BoundaryStop) -> StopEvent:
        return StopEvent()


def test_declared_child_boundary_validates_without_execution() -> None:
    wf = cast(Any, BoundaryParent)(child=BoundaryChild())
    assert wf.validate() is False
    with pytest.raises(WorkflowRuntimeError, match="recursive child runtime"):
        wf.run()
