# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
# ty: ignore[unknown-argument]
# pyright: reportCallIssue=false, reportArgumentType=false, reportPrivateUsage=false
"""Phase 4: namespaced child-workflow execution and the StopEvent boundary.

A parent triggers a child by emitting the child's ``StartEvent``; the child runs
namespaced inside the parent's single broker state, and its ``StopEvent`` surfaces
back to the parent as an ordinary routable event. Only the *root* StopEvent
completes the run.
"""

from __future__ import annotations

import pytest
from workflows import Workflow
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent
from workflows.testing import WorkflowTestRunner


class ChildStart(StartEvent):
    payload: str = ""


class ChildStop(StopEvent):
    out: str = ""


class Child(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop(out=ev.payload.upper())


class Parent(Workflow):
    child: Child

    @step
    async def start(self, ev: StartEvent) -> ChildStart:
        return ChildStart(payload="hello")

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result=ev.out)


def test_parent_with_boundary_child_validates() -> None:
    Parent(child=Child()).validate()


@pytest.mark.asyncio
async def test_child_stop_event_surfaces_as_parent_event() -> None:
    """Parent emits a child StartEvent; the child's StopEvent comes back as a
    routable parent event, and the parent completes on its own StopEvent."""
    result = await WorkflowTestRunner(Parent(child=Child())).run()
    assert result.result == "HELLO"


@pytest.mark.asyncio
async def test_child_runs_standalone_unchanged() -> None:
    """A child workflow still runs on its own."""
    result = await WorkflowTestRunner(Child()).run(start_event=ChildStart(payload="hi"))
    # ChildStop is a StopEvent *subclass*, so the handler returns the event itself.
    assert isinstance(result.result, ChildStop)
    assert result.result.out == "HI"


# --- Grandchild (3-level) boundary --------------------------------------------


class GrandStart(StartEvent):
    payload: str = ""


class GrandStop(StopEvent):
    out: str = ""


class Grand(Workflow):
    @step
    async def run_grand(self, ev: GrandStart) -> GrandStop:
        return GrandStop(out=ev.payload + "!")


class MidStart(StartEvent):
    payload: str = ""


class MidStop(StopEvent):
    out: str = ""


class Mid(Workflow):
    grand: Grand

    @step
    async def begin(self, ev: MidStart) -> GrandStart:
        return GrandStart(payload=ev.payload)

    @step
    async def end(self, ev: GrandStop) -> MidStop:
        return MidStop(out=ev.out)


class Top(Workflow):
    mid: Mid

    @step
    async def start(self, ev: StartEvent) -> MidStart:
        return MidStart(payload="g")

    @step
    async def finish(self, ev: MidStop) -> StopEvent:
        return StopEvent(result=ev.out)


@pytest.mark.asyncio
async def test_grandchild_boundary_runs_namespaced() -> None:
    result = await WorkflowTestRunner(Top(mid=Mid(grand=Grand()))).run()
    assert result.result == "g!"


# --- Namespace isolation of routing -------------------------------------------


class SharedMid(Event):
    """An intermediate event type *reused* by both parent and child namespaces.

    Routing must keep each namespace's copy within that namespace.
    """

    tag: str = ""


class IsoChildStart(StartEvent):
    pass


class IsoChildStop(StopEvent):
    out: str = ""


class IsoChild(Workflow):
    @step
    async def begin(self, ev: IsoChildStart) -> SharedMid:
        return SharedMid(tag="child")

    @step
    async def end(self, ev: SharedMid) -> IsoChildStop:
        return IsoChildStop(out=ev.tag)


class IsoParent(Workflow):
    child: IsoChild

    @step
    async def start(self, ev: StartEvent) -> SharedMid:
        return SharedMid(tag="parent")

    @step
    async def middle(self, ev: SharedMid) -> IsoChildStart:
        # Parent's own SharedMid must route here, not into the child.
        assert ev.tag == "parent"
        return IsoChildStart()

    @step
    async def finish(self, ev: IsoChildStop) -> StopEvent:
        return StopEvent(result=ev.out)


@pytest.mark.asyncio
async def test_shared_event_type_routes_within_namespace() -> None:
    result = await WorkflowTestRunner(IsoParent(child=IsoChild())).run()
    # The child's SharedMid stays in the child; its end step sees tag="child".
    assert result.result == "child"
