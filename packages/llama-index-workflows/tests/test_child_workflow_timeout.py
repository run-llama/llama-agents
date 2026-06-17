# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Per-child-namespace timeouts.

A child's ``timeout`` bounds that child's execution on its own clock: expiry
fails the child through the namespaced catch-error path (so the child's
``@catch_error`` can recover it), and only an uncaught child timeout — or the
root timeout — fails the whole run. The parent's (longer) timeout never
pre-empts a child's shorter one, and vice versa.
"""

from __future__ import annotations

import asyncio

import pytest
from workflows import Workflow
from workflows.decorators import catch_error, step
from workflows.errors import WorkflowTimeoutError
from workflows.events import StartEvent, StepFailedEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerState
from workflows.workflow import DEFAULT_TIMEOUT


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    pass


class SlowChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        await asyncio.sleep(5)  # far exceeds the child's own timeout
        return ChildStop()


class ParentOfSlowChild(Workflow):
    child: SlowChild

    @step
    async def begin(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_child_times_out_on_its_own_clock_and_fails_run_when_uncaught() -> None:
    # Child bound to 0.1s; parent bound to a much longer 30s. The child must time
    # out on its own clock (well before its 5s step and before the parent's 30s),
    # and with no handler the uncaught timeout fails the whole run.
    handler = ParentOfSlowChild(child=SlowChild(timeout=0.1), timeout=30).run()
    with pytest.raises(WorkflowTimeoutError):
        await handler


class RecoveringSlowChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        await asyncio.sleep(5)
        return ChildStop()

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> ChildStop:
        # Catches the WorkflowTimeoutError routed through the namespaced path and
        # emits the child's StopEvent, which crosses back into the parent.
        assert isinstance(ev.exception, WorkflowTimeoutError)
        return ChildStop()


class ParentOfRecoveringSlowChild(Workflow):
    child: RecoveringSlowChild

    @step
    async def begin(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result="recovered")


@pytest.mark.asyncio
async def test_child_timeout_is_caught_by_child_catch_error() -> None:
    handler = ParentOfRecoveringSlowChild(
        child=RecoveringSlowChild(timeout=0.1), timeout=30
    ).run()
    result = await handler
    assert result == "recovered"


class FastDoneChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop()


class ParentOfFastChild(Workflow):
    child: FastDoneChild

    @step
    async def begin(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result="ok")


@pytest.mark.asyncio
async def test_child_under_its_timeout_completes_normally() -> None:
    # Regression guard: a child that finishes well within its timeout is not
    # spuriously failed, and the per-namespace deadline tick is a no-op.
    handler = ParentOfFastChild(child=FastDoneChild(timeout=5), timeout=30).run()
    assert await handler == "ok"


# --- Grandchild: a compound namespace times out on its own clock --------------


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    pass


class MidStart(StartEvent):
    pass


class MidStop(StopEvent):
    pass


class SlowGrandchild(Workflow):
    @step
    async def run_grand(self, ev: GrandStart) -> GrandStop:
        await asyncio.sleep(5)
        return GrandStop()


class MidWithSlowGrand(Workflow):
    grand: SlowGrandchild

    @step
    async def begin(self, ev: MidStart) -> GrandStart:
        return GrandStart()

    @step
    async def finish(self, ev: GrandStop) -> MidStop:
        return MidStop()


class TopOfSlowGrand(Workflow):
    mid: MidWithSlowGrand

    @step
    async def begin(self, ev: StartEvent) -> MidStart:
        return MidStart()

    @step
    async def finish(self, ev: MidStop) -> StopEvent:
        return StopEvent(result="never")


# --- Default resolution: root keeps 45s, an unset child gets no deadline -------


def test_unset_child_arms_no_deadline_root_keeps_default() -> None:
    # A child constructed without a timeout defers to its parent: no per-namespace
    # deadline is armed, while the root still resolves to the 45s default.
    state = BrokerState.from_workflow(ParentOfSlowChild(child=SlowChild()))
    assert state.config.namespace_timeouts == {}
    assert state.config.timeout == DEFAULT_TIMEOUT


def test_explicit_child_timeout_arms_namespace_deadline() -> None:
    state = BrokerState.from_workflow(ParentOfSlowChild(child=SlowChild(timeout=0.1)))
    assert state.config.namespace_timeouts == {("child",): 0.1}


def test_explicit_child_none_timeout_arms_no_deadline() -> None:
    # Explicit None means "no deadline" — same armed state as unset, but chosen.
    state = BrokerState.from_workflow(ParentOfSlowChild(child=SlowChild(timeout=None)))
    assert state.config.namespace_timeouts == {}


def test_root_timeout_resolution() -> None:
    # Unset root → 45s default; explicit None → no root deadline.
    assert (
        BrokerState.from_workflow(ParentOfSlowChild(child=SlowChild())).config.timeout
        == DEFAULT_TIMEOUT
    )
    explicit_none = ParentOfSlowChild(child=SlowChild(), timeout=None)
    assert BrokerState.from_workflow(explicit_none).config.timeout is None


@pytest.mark.asyncio
async def test_grandchild_times_out_on_its_own_clock() -> None:
    # The grandchild (compound namespace ("mid", "grand")) is bound to 0.1s while
    # mid and top run with longer timeouts; the grandchild deadline fires first.
    handler = TopOfSlowGrand(
        mid=MidWithSlowGrand(grand=SlowGrandchild(timeout=0.1))
    ).run()
    with pytest.raises(WorkflowTimeoutError):
        await handler
