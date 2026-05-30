# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Public string projections of a child step's identity.

A child step's :class:`StepId` stringifies to a ``/``-joined namespace path
(``child/run_child``, ``mid/grand/run_grand``). The events that expose step
identity as a plain ``str`` -- notably ``WorkflowFailedEvent.step_name`` -- now
carry that compound form for child steps. These tests pin that contract so a
consumer (logging, telemetry, a UI) knows what to expect, and so the projection
is not silently changed back to a bare name.
"""

from __future__ import annotations

import pytest
from workflows import Context, Workflow
from workflows.decorators import step
from workflows.decorators import catch_error
from workflows.events import (
    Event,
    StartEvent,
    StepFailedEvent,
    StopEvent,
    WorkflowFailedEvent,
)
from workflows.handler import WorkflowHandler


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    pass


class FailingChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        raise ValueError("boom-in-child")


class ParentOfFailingChild(Workflow):
    child: FailingChild

    @step
    async def begin(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result="never")


async def _collect_until_done(handler: WorkflowHandler) -> list[Event]:
    events: list[Event] = []
    async for ev in handler.stream_events():
        events.append(ev)
    return events


@pytest.mark.asyncio
async def test_child_step_failure_event_carries_namespaced_step_name() -> None:
    """A failing child step surfaces a (root-origin) WorkflowFailedEvent whose
    ``step_name`` is the slash-joined ``child/run_child`` -- not the bare name."""
    handler = ParentOfFailingChild(child=FailingChild()).run()
    events = await _collect_until_done(handler)

    with pytest.raises(ValueError, match="boom-in-child"):
        await handler

    failed = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert len(failed) == 1
    assert failed[0].step_name == "child/run_child"


# --- Grandchild: compound namespace in the projection ------------------------


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    pass


class MidStart(StartEvent):
    pass


class MidStop(StopEvent):
    pass


class FailingGrandChild(Workflow):
    @step
    async def run_grand(self, ev: GrandStart) -> GrandStop:
        raise ValueError("boom-in-grandchild")


class MidWithGrandChild(Workflow):
    grand: FailingGrandChild

    @step
    async def begin(self, ev: MidStart) -> GrandStart:
        return GrandStart()

    @step
    async def finish(self, ev: GrandStop) -> MidStop:
        return MidStop()


class TopWithGrandChild(Workflow):
    mid: MidWithGrandChild

    @step
    async def begin(self, ctx: Context, ev: StartEvent) -> MidStart:
        return MidStart()

    @step
    async def finish(self, ev: MidStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_grandchild_step_failure_event_carries_compound_namespace() -> None:
    """A failing grandchild step's ``step_name`` is the full compound path
    ``mid/grand/run_grand``."""
    handler = TopWithGrandChild(
        mid=MidWithGrandChild(grand=FailingGrandChild())
    ).run()
    events = await _collect_until_done(handler)

    with pytest.raises(ValueError, match="boom-in-grandchild"):
        await handler

    failed = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert len(failed) == 1
    assert failed[0].step_name == "mid/grand/run_grand"


# --- @catch_error on a child is a silent no-op: warn, and prove it ------------


class RecoveringChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        raise ValueError("boom-in-child")

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> ChildStop:
        # Never reached when nested: catch-error is root-only.
        return ChildStop()


class ParentOfRecoveringChild(Workflow):
    child: RecoveringChild

    @step
    async def begin(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result="recovered")


def test_child_catch_error_handler_warns_on_attach() -> None:
    """Declaring @catch_error on a child warns that it won't fire when nested."""
    with pytest.warns(UserWarning, match="catch-error recovery only applies"):
        ParentOfRecoveringChild(child=RecoveringChild())


@pytest.mark.asyncio
async def test_child_catch_error_handler_does_not_recover_when_nested() -> None:
    """The child's @catch_error handler never runs; the child failure fails the
    whole run rather than being recovered."""
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        handler = ParentOfRecoveringChild(child=RecoveringChild()).run()
    events = await _collect_until_done(handler)

    with pytest.raises(ValueError, match="boom-in-child"):
        await handler

    # The handler did not recover the run: a WorkflowFailedEvent for the child
    # step was emitted (the catch-error handler was bypassed).
    failed = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert len(failed) == 1
    assert failed[0].step_name == "child/run_child"
