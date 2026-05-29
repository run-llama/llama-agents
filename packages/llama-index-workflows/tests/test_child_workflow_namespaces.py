# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
# ty: ignore[unknown-argument]
# pyright: reportCallIssue=false, reportArgumentType=false, reportPrivateUsage=false
"""Tests for namespaced step enumeration across a child-workflow tree.

The child-aware constructor is synthesized by ``WorkflowMeta`` at runtime, so
the file-level pragmas suppress the expected static-typing diagnostics on the
``child=`` keyword (see ``test_child_workflow_declaration.py``).
"""

from __future__ import annotations

from workflows import Workflow
from workflows.decorators import step
from workflows.events import StartEvent, StopEvent
from workflows.runtime.types.step_id import StepId


class GrandStart(StartEvent):
    payload: str = "g"


class GrandStop(StopEvent):
    out: str = "g"


class GrandChild(Workflow):
    @step
    async def run_grand(self, ev: GrandStart) -> GrandStop:
        return GrandStop(out=ev.payload)


class MidStart(StartEvent):
    payload: str = "m"


class MidStop(StopEvent):
    out: str = "m"


class Mid(Workflow):
    grand: GrandChild

    @step
    async def run_mid(self, ev: MidStart) -> MidStop:
        return MidStop(out=ev.payload)


class Root(Workflow):
    mid: Mid

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="root done")


class ChildStart(StartEvent):
    payload: str = "c"


class ChildStop(StopEvent):
    out: str = "c"


class Child(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop(out=ev.payload)


class Parent(Workflow):
    child: Child

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="parent done")


def test_namespaced_steps_includes_child_under_field_path() -> None:
    parent = Parent(child=Child())
    assert set(parent._get_namespaced_steps()) == {
        StepId.root("start"),
        StepId(("child",), "run_child"),
    }


def test_namespace_instances_maps_paths_to_owning_instances() -> None:
    child = Child()
    parent = Parent(child=child)
    instances = parent._namespace_instances()
    assert instances[()] is parent
    assert instances[("child",)] is child


def test_grandchild_namespaced_as_flat_compound_tuple() -> None:
    grand = GrandChild()
    mid = Mid(grand=grand)
    root = Root(mid=mid)

    assert set(root._get_namespaced_steps()) == {
        StepId.root("start"),
        StepId(("mid",), "run_mid"),
        StepId(("mid", "grand"), "run_grand"),
    }
    instances = root._namespace_instances()
    assert instances[()] is root
    assert instances[("mid",)] is mid
    assert instances[("mid", "grand")] is grand


def test_static_class_path_matches_runtime_set() -> None:
    """The static (no-instantiation) walk derives the same StepId set."""
    root = Root(mid=Mid(grand=GrandChild()))
    assert set(Root._get_namespaced_steps_from_class()) == set(
        root._get_namespaced_steps()
    )


def test_childless_workflow_only_root_namespace() -> None:
    assert set(Child()._get_namespaced_steps()) == {StepId.root("run_child")}
    assert set(GrandChild._get_namespaced_steps_from_class()) == {
        StepId.root("run_grand")
    }
