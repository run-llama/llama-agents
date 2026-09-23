# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Phase-1 recursive-broker behaviors.

Boundary crossing (descent births exactly one parent work item; ascent consumes
it once), broker-local sends, dead-invocation loudness, sibling isolation across
``slot#N``, and replay determinism of the counter-minted invocation ids.
"""

from __future__ import annotations

import asyncio
import logging
import time

import pytest
from workflows import Context, Workflow
from workflows.context.external_context import ExternalContext
from workflows.context.serializers import JsonSerializer
from workflows.decorators import step
from workflows.errors import WorkflowRuntimeError
from workflows.events import Event, StartEvent, StopEvent, UnhandledEvent
from workflows.runtime.control_loop.reduce import (
    _dispatch_add_event,
    _reduce_tick,
    rebuild_state_from_ticks,
    rewind_in_progress,
)
from workflows.runtime.control_loop.streams import _count_accepting_steps
from workflows.runtime.types.commands import (
    CommandPublishEvent,
    CommandRunWorker,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.results import StepWorkerResult, StepWorkerWaiter
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickAddEvent, TickStepResult
from workflows.testing import WorkflowTestRunner


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    value: int = 0


class _Late(Event):
    pass


class _SimpleChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop(value=1)

    @step
    async def also_late(self, ev: _Late) -> None:
        return None


class _SimpleParent(Workflow):
    child: _SimpleChild

    @step
    async def start(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result=ev.value)


# --- Dead-invocation targeted send is loud ------------------------------------


def test_dead_invocation_targeted_send_publishes_unhandled() -> None:
    """A targeted send to a concrete-but-absent child invocation publishes an
    origin-tagged UnhandledEvent instead of silently re-entering a torn-down
    broker."""
    root = BrokerState.from_workflow(_SimpleParent(child=_SimpleChild()))
    tick = TickAddEvent(
        event=_Late(),
        step_id=StepId((), "also_late"),
        origin_namespace=("child#0",),
    )
    _, commands = _dispatch_add_event(tick, root, 0.0)
    unhandled = [
        c
        for c in commands
        if isinstance(c, CommandPublishEvent) and isinstance(c.event, UnhandledEvent)
    ]
    assert len(unhandled) == 1
    assert unhandled[0].origin_namespace == ("child#0",)


# --- Multi-start-step child descends as exactly one work item -----------------


class _MultiStartChild(Workflow):
    @step
    async def one(self, ev: ChildStart) -> ChildStop:
        return ChildStop(value=1)

    @step
    async def two(self, ev: ChildStart) -> ChildStop:
        return ChildStop(value=2)


class _MultiStartParent(Workflow):
    child: _MultiStartChild

    @step
    async def start(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result=ev.value)


def test_child_descent_counts_as_one_regardless_of_start_steps() -> None:
    """A child slot descended into is exactly one work item in the parent's
    enclosing stream, however many of the child's start steps accept the
    event."""
    root = BrokerState.from_workflow(_MultiStartParent(child=_MultiStartChild()))
    # No parent step accepts ChildStart; two child start steps do — the birth
    # count (fan-out factor) is 1 (the single child slot), not 2.
    assert _count_accepting_steps(root, ChildStart()) == 1


# --- Child inside a parent fan-out: stream closes on ascent -------------------


class _Trigger(Event):
    k: int


class _FanChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop(value=1)


class _FanOverChildrenParent(Workflow):
    child: _FanChild

    @step
    async def fan(self, ev: StartEvent) -> list[_Trigger]:
        return [_Trigger(k=i) for i in range(3)]

    @step
    async def relay(self, ev: _Trigger) -> ChildStart:
        return ChildStart()

    @step
    async def collect(self, ctx: Context, ev: ChildStop) -> StopEvent | None:
        got = ctx.collect_events(ev, [ChildStop, ChildStop, ChildStop])
        if got is None:
            return None
        return StopEvent(result=sum(c.value for c in got))


class _MixedSignal(StartEvent):
    pass


class _GateDone(Event):
    pass


class _MixedChild(Workflow):
    @step
    async def run_child(self, ev: _MixedSignal) -> ChildStop:
        return ChildStop(value=9)


class _MixedParent(Workflow):
    child: _MixedChild

    @step
    async def gate(self, ctx: Context, ev: StartEvent) -> _GateDone:
        await ctx.wait_for_event(_MixedSignal)
        return _GateDone()

    @step
    async def finish(self, ev: _GateDone | ChildStop) -> StopEvent:
        return StopEvent(result="done")


def test_mixed_descend_and_waiter_resolve_in_one_tick() -> None:
    """One event that both resolves a parent waiter and is a child's StartEvent
    does both in the same tick: the waiter's step re-runs and a child is born
    (the descent is an independent boundary work item)."""
    root = BrokerState.from_workflow(_MixedParent(child=_MixedChild()))
    gate = StepId((), "gate")
    root.workers[gate].collected_waiters.append(
        StepWorkerWaiter(
            waiter_id="w1",
            event=StartEvent(),
            waiting_for_event=_MixedSignal,
            requirements={},
            has_requirements=False,
            resolved_event=None,
            scope_path=(),
            work_item_id="work_item_1",
        )
    )

    new_state, commands = _dispatch_add_event(
        TickAddEvent(event=_MixedSignal(), origin_namespace=()), root, 0.0
    )
    assert any(isinstance(c, CommandRunWorker) and c.step_id == gate for c in commands)
    assert set(new_state.children) == {"child#0"}


@pytest.mark.asyncio
async def test_child_inside_parent_fan_out_completes() -> None:
    """Three children born inside a parent fan-out each complete; the parent
    stream balances (each child is one work item, consumed once at ascent) and
    the join releases."""
    result = await WorkflowTestRunner(_FanOverChildrenParent(child=_FanChild())).run()
    assert result.result == 3


# --- Sibling isolation across slot#0 / slot#1 ---------------------------------


class _CountingStart(StartEvent):
    pass


class _CountingStop(StopEvent):
    count: int = 0


class _CountingChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: _CountingStart) -> _CountingStop:
        count = await ctx.store.get("count", default=0) + 1
        await ctx.store.set("count", count)
        return _CountingStop(count=count)


class _TwiceParent(Workflow):
    child: _CountingChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> _CountingStart:
        ctx.send_event(_CountingStart())
        return _CountingStart()

    @step
    async def finish(self, ctx: Context, ev: _CountingStop) -> StopEvent | None:
        got = ctx.collect_events(ev, [_CountingStop, _CountingStop])
        if got is None:
            return None
        # Both invocations are fresh slot#0/slot#1 brokers with their own store,
        # so each counts to exactly 1 (no leaked count across siblings).
        return StopEvent(result=sorted(c.count for c in got))


@pytest.mark.asyncio
async def test_overlapping_siblings_have_isolated_state() -> None:
    result = await WorkflowTestRunner(_TwiceParent(child=_CountingChild())).run()
    assert result.result == [1, 1]


# --- Broker-local send rule ---------------------------------------------------


class _CrossSendChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop()


class _CrossSendParent(Workflow):
    child: _CrossSendChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
        ctx.send_event(ChildStart(), step="child/run_child")
        return StopEvent(result="unreachable")


@pytest.mark.asyncio
async def test_internal_targeted_cross_broker_send_is_rejected() -> None:
    """A step targeting another broker (a child's step by path) is a loud
    WorkflowRuntimeError; broker-local sends use a bare step name."""
    with pytest.raises(WorkflowRuntimeError, match="another broker"):
        await WorkflowTestRunner(_CrossSendParent(child=_CrossSendChild())).run()


# --- Replay determinism: counter ids are stable across snapshot boundary ------


@pytest.mark.asyncio
async def test_snapshot_plus_suffix_replay_matches_full_replay_ids() -> None:
    """A mid-run snapshot + journal suffix reconstructs the same invocation ids
    and child_seq as a full-journal replay (counter minting is deterministic)."""
    handler = _TwiceParent(child=_CountingChild()).run()
    await handler
    assert handler.ctx is not None
    face = handler.ctx._face
    assert isinstance(face, ExternalContext)
    journal = face._tick_log

    workflow = _TwiceParent(child=_CountingChild())

    # Full replay from a canonical initial state.
    full = rebuild_state_from_ticks(BrokerState.from_workflow(workflow), list(journal))

    # Split replay: rebuild a prefix, snapshot it, resume on the suffix.
    split_at = len(journal) // 2
    prefix = rebuild_state_from_ticks(
        BrokerState.from_workflow(workflow), list(journal[:split_at])
    )
    serializer = JsonSerializer()
    restored = BrokerState.from_serialized(
        prefix.to_serialized(serializer), workflow, serializer
    )
    restored, _ = rewind_in_progress(restored, time.time())
    for tick in journal[split_at:]:
        restored, _ = _reduce_tick(tick, restored, time.time())

    assert full.child_seq == restored.child_seq
    assert set(full.children) == set(restored.children)


# --- Orphan: task outliving cancel is logged; late result is loud -------------


class _OrphanChildStart(StartEvent):
    pass


class _OrphanChildStop(StopEvent):
    pass


def test_late_result_to_dead_broker_publishes_unhandled() -> None:
    """A worker result descending to an already-popped broker publishes an
    UnhandledEvent (the loud orphan burn-off) rather than crashing the loop."""
    root = BrokerState.from_workflow(_SimpleParent(child=_SimpleChild()))
    tick = TickStepResult(
        step_id=StepId((), "run_child"),
        worker_id=0,
        invocation_namespace=("child#0",),
        event=ChildStart(),
        result=[StepWorkerResult(result=ChildStop(value=1))],
    )
    _, commands = _reduce_tick(tick, root, 0.0)
    unhandled = [
        c
        for c in commands
        if isinstance(c, CommandPublishEvent) and isinstance(c.event, UnhandledEvent)
    ]
    assert len(unhandled) == 1
    assert unhandled[0].origin_namespace == ("child#0",)


class _SlowSibling(Event):
    pass


class _StopFast(Event):
    pass


class _TwoBranchChild(Workflow):
    @step
    async def begin(
        self, ctx: Context, ev: _OrphanChildStart
    ) -> _StopFast | _SlowSibling:
        ctx.send_event(_SlowSibling())
        return _StopFast()

    @step
    async def stop_fast(self, ev: _StopFast) -> _OrphanChildStop:
        return _OrphanChildStop()

    @step
    async def slow(self, ev: _SlowSibling) -> _OrphanChildStop:
        # Swallow the cancellation and keep running past the grace window, so the
        # task becomes a tracked orphan the runner must log and let go of.
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(1.0)
        return _OrphanChildStop()


class _TwoBranchParent(Workflow):
    child: _TwoBranchChild

    @step
    async def start(self, ev: StartEvent) -> _OrphanChildStart:
        return _OrphanChildStart()

    @step
    async def finish(self, ev: _OrphanChildStop) -> StopEvent:
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_orphan_task_outliving_cancel_is_handled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A child StopEvent tears the subtree down; a sibling coroutine that
    outlives the cancel grace cannot crash the loop — the run still completes
    cleanly via the child's StopEvent boundary, and the orphan is logged."""
    with caplog.at_level(logging.WARNING, logger="workflows.runtime.control_loop"):
        result = await asyncio.wait_for(
            _TwoBranchParent(child=_TwoBranchChild()).run(), timeout=10
        )
    assert result == "done"

    orphan_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "outlived" in r.getMessage()
    ]
    assert orphan_warnings, "expected a warning for the orphaned task"
    message = orphan_warnings[0].getMessage()
    assert "slow" in message
    assert "child#0" in message
