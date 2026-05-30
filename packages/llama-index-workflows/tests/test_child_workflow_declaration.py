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
from workflows.runtime.types.step_id import StepId


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


def test_user_defined_init_attaches_children_at_construction() -> None:
    parent = ParentWithUserInit()
    # User-init path: the child is assigned as a plain attribute inside the
    # user __init__, then WorkflowMeta.__call__ runs _finalize_construction
    # (-> _ensure_children_attached) once the outermost __init__ returns, so
    # the child is wired in by the time construction completes.
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
    emits a warning naming the dead params. ``timeout`` is NOT dead -- it bounds
    the child's own execution -- so only ``verbose`` is flagged here."""
    with pytest.warns(UserWarning, match="verbose=True"):
        Parent(child=Child(timeout=10, verbose=True))


def test_child_timeout_does_not_warn() -> None:
    """A child ``timeout`` is honored (per-namespace deadline), not ignored, so
    setting it alone attaches silently."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Parent(child=Child(timeout=10))


def test_honored_child_config_does_not_warn() -> None:
    """A child with default run-level config attaches silently."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Parent(child=Child())


# ---------------------------------------------------------------------------
# Tracking ordering: registration must see the full child tree.
#
# WorkflowMeta.__call__ runs _finalize_construction (-> _ensure_children_attached
# then track_workflow) only after the outermost __init__ returns, so by the time
# the runtime tracks a workflow its children are already attached. These tests
# capture the namespaced step set at track time to prove the child steps are
# present -- the precise condition a DBOS launch later freezes into the step set.
# ---------------------------------------------------------------------------


class _RecordingRuntime(BasicRuntime):
    """Records the namespaced step set observed at each ``track_workflow``."""

    def __init__(self) -> None:
        super().__init__()
        self.track_calls: list[tuple[int, frozenset[StepId]]] = []

    def track_workflow(self, workflow: Workflow) -> None:
        self.track_calls.append(
            (id(workflow), frozenset(workflow._get_namespaced_steps().keys()))
        )
        super().track_workflow(workflow)


def _steps_at_track(rt: _RecordingRuntime, wf: Workflow) -> list[frozenset[StepId]]:
    """The step sets captured each time ``wf`` was tracked (one per call)."""
    return [steps for wf_id, steps in rt.track_calls if wf_id == id(wf)]


def _has_child_steps(steps: frozenset[StepId]) -> bool:
    return any(sid.namespace == ("child",) for sid in steps)


def test_synthesized_init_tracks_after_children_attached() -> None:
    rt = _RecordingRuntime()
    with rt.registering():
        parent = Parent(child=Child())
    captures = _steps_at_track(rt, parent)
    assert len(captures) == 1
    assert _has_child_steps(captures[0])


def test_user_init_tracks_after_children_attached() -> None:
    rt = _RecordingRuntime()
    with rt.registering():
        parent = ParentWithUserInit()
    captures = _steps_at_track(rt, parent)
    assert len(captures) == 1
    assert _has_child_steps(captures[0])


def test_subclass_of_user_init_tracks_after_children_attached() -> None:
    class SubOfUserInit(ParentWithUserInit):
        """No own __init__: still instantiated through WorkflowMeta.__call__,
        finalize resolves child slots from ``type(self)``."""

    rt = _RecordingRuntime()
    with rt.registering():
        # dataclass_transform synthesizes a `child`-requiring __init__ for the
        # subclass (it declares no __init__ of its own), but at runtime the
        # inherited user __init__ constructs the child, so no arg is needed.
        parent = SubOfUserInit()  # type: ignore  # synthesized init wants child; inherited user init supplies it
    captures = _steps_at_track(rt, parent)
    assert len(captures) == 1
    assert _has_child_steps(captures[0])
    assert isinstance(parent.child, Child)


def test_childless_user_init_tracked_exactly_once() -> None:
    class ChildlessUserInit(Workflow):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            # Work after super().__init__() must not disturb tracking.
            self.marker = "after-super"

        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="ok")

    rt = _RecordingRuntime()
    with rt.registering():
        wf = ChildlessUserInit()
    assert wf.marker == "after-super"
    assert len(_steps_at_track(rt, wf)) == 1
    assert wf in rt._pending


def test_track_fires_once_per_workflow_in_tree() -> None:
    rt = _RecordingRuntime()
    with rt.registering():
        child = Child()
        parent = Parent(child=child)
    # Parent and child each tracked exactly once; no double-track from
    # _attach_child re-homing a same-runtime child.
    assert len(_steps_at_track(rt, parent)) == 1
    assert len(_steps_at_track(rt, child)) == 1


def test_init_raises_skips_finalize() -> None:
    class Boom(Workflow):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            raise ValueError("boom")

        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent()

    rt = _RecordingRuntime()
    with rt.registering():
        with pytest.raises(ValueError, match="boom"):
            Boom()
    # __call__ propagates the exception; _finalize_construction never runs.
    assert rt.track_calls == []


def test_missing_super_init_raises_clear_error() -> None:
    class NoSuper(Workflow):
        def __init__(self, **kwargs: Any) -> None:
            # Intentionally skips super().__init__(): _runtime never gets set,
            # so finalize would otherwise raise an opaque private-attr error.
            pass

        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent()

    with pytest.raises(WorkflowValidationError, match="did not call super"):
        NoSuper()
