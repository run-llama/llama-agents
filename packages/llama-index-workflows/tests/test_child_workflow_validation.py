# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import Workflow, step
from workflows.errors import WorkflowValidationError
from workflows.events import StartEvent, StopEvent


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
