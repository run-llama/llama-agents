# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for declaring child workflows as typed class fields.

``WorkflowMeta`` is a ``dataclass_transform``, so a parent's child fields and the
base config kwargs both type-check on the constructor (``Parent(child=Child(),
timeout=30)``) with no suppression. The only per-line ignore below is on a test
that deliberately passes the wrong child type to exercise the runtime check.
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest
from workflows import Workflow
from workflows.decorators import step
from workflows.errors import WorkflowValidationError
from workflows.events import StartEvent, StopEvent
from workflows.plugins import BasicRuntime


class ChildStart(StartEvent):
    payload: str = "x"


class ChildStop(StopEvent):
    out: str = "y"


class Child(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop(out=ev.payload)


class Parent(Workflow):
    child: Child

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="parent done")


def test_synthesized_init_constructs_and_forwards_kwargs() -> None:
    parent = Parent(child=Child(), timeout=30)
    assert isinstance(parent.child, Child)
    assert parent._timeout == 30
    assert parent.child_workflows == {"child": parent.child}


def test_child_slots_resolved_from_annotations() -> None:
    slots = Parent._get_child_workflow_slots()
    assert slots == {"child": Child}


def test_child_adopts_parent_runtime_and_is_tracked() -> None:
    runtime = BasicRuntime()
    with runtime.registering():
        parent = Parent(child=Child())
    assert parent.runtime is runtime
    assert parent.child.runtime is runtime
    assert parent.child in runtime._pending


def test_child_constructed_under_different_runtime_is_reparented() -> None:
    parent_rt = BasicRuntime()
    other_rt = BasicRuntime()
    with other_rt.registering():
        child = Child()
    assert child.runtime is other_rt
    with parent_rt.registering():
        parent = Parent(child=child)
    assert parent.child.runtime is parent_rt
    assert child not in other_rt._pending
    assert child in parent_rt._pending


def test_child_runtime_override_is_blocked() -> None:
    parent = Parent(child=Child())
    with pytest.raises(RuntimeError, match="Cannot reassign runtime"):
        parent.child._switch_runtime(BasicRuntime())


class PlainStartChild(Workflow):
    @step
    async def go(self, ev: StartEvent) -> ChildStop:
        return ChildStop()


class PlainStopChild(Workflow):
    @step
    async def go(self, ev: ChildStart) -> StopEvent:
        return StopEvent()


class ParentBadStart(Workflow):
    child: PlainStartChild

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


class ParentBadStop(Workflow):
    child: PlainStopChild

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


def test_child_without_custom_start_event_rejected() -> None:
    with pytest.raises(WorkflowValidationError, match="custom StartEvent"):
        ParentBadStart(child=PlainStartChild())


def test_child_without_custom_stop_event_rejected() -> None:
    with pytest.raises(WorkflowValidationError, match="custom StopEvent"):
        ParentBadStop(child=PlainStopChild())


def test_wrong_child_type_rejected() -> None:
    class OtherStart(StartEvent):
        pass

    class OtherStop(StopEvent):
        pass

    class Other(Workflow):
        @step
        async def go(self, ev: OtherStart) -> OtherStop:
            return OtherStop()

    with pytest.raises(WorkflowValidationError, match="expects Child"):
        Parent(child=Other())  # type: ignore  # wrong child type, rejected at runtime


class ParentWithUserInit(Workflow):
    child: Child

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.child = Child()

    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


def test_user_defined_init_attaches_children_on_validate() -> None:
    parent = ParentWithUserInit()
    # User-init path: not attached at construction, wired in at validate/run.
    assert parent._child_workflows == {}
    parent.validate()
    assert isinstance(parent.child, Child)
    assert parent._child_workflows == {"child": parent.child}
    assert parent.child.runtime is parent.runtime


def test_childless_workflow_unaffected() -> None:
    class NoChildren(Workflow):
        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="ok")

    # No synthesized init: still inherits Workflow.__init__.
    assert NoChildren.__init__ is Workflow.__init__
    wf = NoChildren(timeout=10)
    assert wf._timeout == 10
    assert wf.child_workflows == {}


def test_dead_child_config_warns() -> None:
    """Run-level config set on a child is ignored once nested, so attaching it
    emits a warning naming the dead params."""
    with pytest.warns(UserWarning, match="timeout=10.*verbose=True"):
        Parent(child=Child(timeout=10, verbose=True))


def test_honored_child_config_does_not_warn() -> None:
    """A child with default run-level config attaches silently."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Parent(child=Child())
