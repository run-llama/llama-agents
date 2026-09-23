# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Namespace-aware ``@catch_error`` recovery for nested child workflows.

A child's ``@catch_error`` handler recovers the child's own failing steps,
within the child's namespace, on a recovery budget distinct from a same-named
root handler. These tests pin that contract across the boundary and at the
grandchild level.
"""

from __future__ import annotations

import pytest
from workflows import Workflow
from workflows.decorators import catch_error, step
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


async def _collect_until_done(handler: WorkflowHandler) -> list[Event]:
    events: list[Event] = []
    async for ev in handler.stream_events():
        events.append(ev)
    return events


# --- Same handler name in two namespaces keeps separate recovery budgets ------
#
# ``recovery_counts`` travels with the event lineage and crosses the child
# StopEvent boundary into the parent. If the budget were keyed by the bare
# handler name, the child's ``recover`` would consume the root's ``recover``
# budget and the parent failure would no longer be recoverable. StepId keys
# keep the budgets distinct.


class SharedNameChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        raise ValueError("child-boom")

    @catch_error(max_recoveries=1)
    async def recover(self, ev: StepFailedEvent) -> ChildStop:
        return ChildStop()


class SharedNameParent(Workflow):
    child: SharedNameChild

    @step
    async def begin(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def after_child(self, ev: ChildStop) -> StopEvent:
        raise ValueError("parent-boom")

    @catch_error(max_recoveries=1)
    async def recover(self, ev: StepFailedEvent) -> StopEvent:
        return StopEvent(result="parent-recovered")


@pytest.mark.asyncio
async def test_same_handler_name_across_namespaces_has_separate_budgets() -> None:
    handler = SharedNameParent(child=SharedNameChild()).run()
    events = await _collect_until_done(handler)

    result = await handler
    # Child recovered its own failure (max 1) and the parent then recovered its
    # own failure (max 1) on a separate budget despite the shared name.
    assert result == "parent-recovered"
    failed = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert failed == []


# --- Grandchild handler recovers a grandchild step ----------------------------


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    pass


class MidStart(StartEvent):
    pass


class MidStop(StopEvent):
    pass


class RecoveringGrandChild(Workflow):
    @step
    async def run_grand(self, ev: GrandStart) -> GrandStop:
        raise ValueError("grand-boom")

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> GrandStop:
        return GrandStop()


class MidPassthrough(Workflow):
    grand: RecoveringGrandChild

    @step
    async def begin(self, ev: MidStart) -> GrandStart:
        return GrandStart()

    @step
    async def finish(self, ev: GrandStop) -> MidStop:
        return MidStop()


class TopRecover(Workflow):
    mid: MidPassthrough

    @step
    async def begin(self, ev: StartEvent) -> MidStart:
        return MidStart()

    @step
    async def finish(self, ev: MidStop) -> StopEvent:
        return StopEvent(result="grand-recovered")


@pytest.mark.asyncio
async def test_grandchild_catch_error_recovers_in_compound_namespace() -> None:
    handler = TopRecover(mid=MidPassthrough(grand=RecoveringGrandChild())).run()
    events = await _collect_until_done(handler)

    result = await handler
    assert result == "grand-recovered"
    failed = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert failed == []
