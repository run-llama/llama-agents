# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Phase-2 unified semantics on the recursive broker.

Two features, one meaning at every nesting level:

- **Elapsed-alive timeout budget** (root and every child): a broker may spend
  ``timeout`` seconds of *known-alive* time. Alive gaps within a session count;
  inter-session downtime is forgiven (the session-start marker resets the accrual
  reference without accruing). Snapshot-resume and full-journal replay reconstruct
  identical ``elapsed_alive``.
- **Boundary failure ascent**: a child broker whose step fails uncaught locally,
  or whose deadline fires uncaught, surfaces to its parent as a ``StepFailedEvent``
  named for the child's path, eligible for the parent's ``@catch_error`` with
  normal ``max_recoveries`` accounting. It recurses upward; only root-uncaught
  fails the run.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
import workflows.runtime.control_loop.reduce as reduce_mod
from workflows import Context, Workflow
from workflows.context.serializers import JsonSerializer
from workflows.decorators import catch_error, step
from workflows.errors import WorkflowTimeoutError
from workflows.events import (
    Event,
    StartEvent,
    StepFailedEvent,
    StopEvent,
)
from workflows.runtime.control_loop.reduce import (
    _reduce_tick,
    rebuild_state_from_ticks,
    rewind_in_progress,
)
from workflows.runtime.types.commands import (
    CommandCancelNamespace,
    CommandFailWorkflow,
    CommandHalt,
    WorkflowCommand,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickSessionStart,
    WorkflowTick,
)
from workflows.testing import WorkflowTestRunner


class _Ping(Event):
    pass


class _BudgetRoot(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")

    @step
    async def ping(self, ev: _Ping) -> None:
        return None


# --- Elapsed-alive accrual: alive counts, downtime is forgiven ----------------


def test_root_alive_budget_accrues_and_forgives_downtime() -> None:
    """The root accrues alive time from journaled stamps; a session-start marker
    resets the reference without accruing, so downtime never enters the budget."""
    state = BrokerState.from_workflow(_BudgetRoot())

    # Session 1: 5s of alive time counts.
    state, _ = _reduce_tick(TickSessionStart(stamped_at=0.0), state, 0.0)
    state, _ = _reduce_tick(
        TickAddEvent(event=StartEvent(), stamped_at=5.0), state, 5.0
    )
    assert state.elapsed_alive == pytest.approx(5.0)

    # Session 2 after 100s downtime: the marker forgives the gap (no accrual).
    state, _ = _reduce_tick(TickSessionStart(stamped_at=105.0), state, 105.0)
    assert state.elapsed_alive == pytest.approx(5.0)

    # Alive time in session 2 resumes counting from the marker, not from t=5.
    state, _ = _reduce_tick(TickAddEvent(event=_Ping(), stamped_at=107.0), state, 107.0)
    assert state.elapsed_alive == pytest.approx(7.0)


def test_unstamped_ticks_never_accrue() -> None:
    """Legacy (unstamped) ticks accrue nothing, so an old markerless journal
    replays with an empty budget and can never fire a spurious timeout."""
    state = BrokerState.from_workflow(_BudgetRoot())
    state, _ = _reduce_tick(TickAddEvent(event=StartEvent()), state, 123.0)
    state, _ = _reduce_tick(TickAddEvent(event=_Ping()), state, 456.0)
    assert state.elapsed_alive == 0.0
    assert state.last_alive_stamp is None


# --- Snapshot-resume vs full-tick-replay reconstruct identical elapsed_alive --


class _BudgetChildStart(StartEvent):
    pass


class _BudgetChildStop(StopEvent):
    n: int = 0


class _BudgetChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: _BudgetChildStart) -> _BudgetChildStop:
        n = await ctx.store.get("n", default=0) + 1
        await ctx.store.set("n", n)
        return _BudgetChildStop(n=n)


class _BudgetParent(Workflow):
    child: _BudgetChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> _BudgetChildStart:
        ctx.send_event(_BudgetChildStart())
        return _BudgetChildStart()

    @step
    async def finish(self, ctx: Context, ev: _BudgetChildStop) -> StopEvent | None:
        got = ctx.collect_events(ev, [_BudgetChildStop, _BudgetChildStop])
        if got is None:
            return None
        return StopEvent(result=sorted(c.n for c in got))


def _alive_map(state: BrokerState) -> dict[tuple[str, ...], tuple[float, float | None]]:
    result: dict[tuple[str, ...], tuple[float, float | None]] = {}

    def walk(broker: BrokerState, path: tuple[str, ...]) -> None:
        result[path] = (broker.elapsed_alive, broker.last_alive_stamp)
        for key, child in broker.children.items():
            walk(child.state, (*path, key))

    walk(state, ())
    return result


@pytest.mark.asyncio
async def test_snapshot_resume_matches_full_replay_elapsed_alive() -> None:
    """A mid-run snapshot + journal suffix reconstructs the same elapsed-alive
    budget (root and any live child) as a full-journal replay: the snapshot
    captures ``elapsed_alive``/``last_alive_stamp`` exactly, so both paths agree."""
    from workflows.context.external_context import ExternalContext

    handler = _BudgetParent(child=_BudgetChild()).run()
    await handler
    assert handler.ctx is not None
    face = handler.ctx._face
    assert isinstance(face, ExternalContext)
    journal = list(face._tick_log)

    workflow = _BudgetParent(child=_BudgetChild())
    full = rebuild_state_from_ticks(BrokerState.from_workflow(workflow), journal)

    split_at = len(journal) // 2
    prefix = rebuild_state_from_ticks(
        BrokerState.from_workflow(workflow), journal[:split_at]
    )
    serializer = JsonSerializer()
    restored = BrokerState.from_serialized(
        prefix.to_serialized(serializer), workflow, serializer
    )
    restored, _ = rewind_in_progress(restored, time.time())
    for tick in journal[split_at:]:
        restored, _ = _reduce_tick(tick, restored, time.time())

    assert _alive_map(full) == _alive_map(restored)


# --- Boundary failure ascent: parent @catch_error catches a child failure -----


class _ChildStart(StartEvent):
    pass


class _ChildStop(StopEvent):
    pass


class _BoomChild(Workflow):
    @step
    async def run_child(self, ev: _ChildStart) -> _ChildStop:
        raise ValueError("boom-in-child")


class _ParentCatchesChildFailure(Workflow):
    child: _BoomChild

    @step
    async def begin(self, ev: StartEvent) -> _ChildStart:
        return _ChildStart()

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> StopEvent:
        # The boundary StepFailedEvent is named for the child's path, not the
        # internal step, and carries the child's exception.
        assert isinstance(ev.exception, ValueError)
        return StopEvent(result=f"caught:{ev.step_name}")

    @step
    async def finish(self, ev: _ChildStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_parent_catch_error_catches_child_step_failure() -> None:
    result = await WorkflowTestRunner(
        _ParentCatchesChildFailure(child=_BoomChild())
    ).run()
    assert result.result == "caught:child"


class _SlowChild(Workflow):
    @step
    async def run_child(self, ev: _ChildStart) -> _ChildStop:
        await asyncio.sleep(5)
        return _ChildStop()


class _ParentCatchesChildTimeout(Workflow):
    child: _SlowChild

    @step
    async def begin(self, ev: StartEvent) -> _ChildStart:
        return _ChildStart()

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> StopEvent:
        assert isinstance(ev.exception, WorkflowTimeoutError)
        return StopEvent(result=f"caught-timeout:{ev.step_name}")

    @step
    async def finish(self, ev: _ChildStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_parent_catch_error_catches_child_timeout() -> None:
    # The child has no handler of its own, so its timeout ascends to the parent's
    # @catch_error rather than firing the whole run.
    handler = _ParentCatchesChildTimeout(
        child=_SlowChild(timeout=0.1), timeout=30
    ).run()
    assert await handler == "caught-timeout:child"


# --- Grandparent catches when the parent lacks a handler ----------------------


class _GrandStart(StartEvent):
    pass


class _GrandStop(StopEvent):
    pass


class _MidStart(StartEvent):
    pass


class _MidStop(StopEvent):
    pass


class _BoomGrand(Workflow):
    @step
    async def run_grand(self, ev: _GrandStart) -> _GrandStop:
        raise ValueError("boom-in-grand")


class _MidNoHandler(Workflow):
    grand: _BoomGrand

    @step
    async def begin(self, ev: _MidStart) -> _GrandStart:
        return _GrandStart()

    @step
    async def finish(self, ev: _GrandStop) -> _MidStop:
        return _MidStop()


class _TopCatchesGrand(Workflow):
    mid: _MidNoHandler

    @step
    async def begin(self, ev: StartEvent) -> _MidStart:
        return _MidStart()

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> StopEvent:
        return StopEvent(result=f"top-caught:{ev.step_name}")

    @step
    async def finish(self, ev: _MidStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_grandparent_catches_when_parent_lacks_handler() -> None:
    # grand fails uncaught -> ascends through mid (no handler) -> top catches it,
    # named for the top-level child ("mid") whose subtree failed.
    result = await WorkflowTestRunner(
        _TopCatchesGrand(mid=_MidNoHandler(grand=_BoomGrand()))
    ).run()
    assert result.result == "top-caught:mid"


def _spy_boundary_cancels(
    monkeypatch: pytest.MonkeyPatch,
) -> list[CommandCancelNamespace]:
    """Record every CommandCancelNamespace produced by boundary-failure ascent."""
    cancels: list[CommandCancelNamespace] = []
    original = reduce_mod._ascend_boundary_failure

    def spy(*args: Any, **kwargs: Any) -> list[WorkflowCommand]:
        commands = original(*args, **kwargs)
        cancels.extend(c for c in commands if isinstance(c, CommandCancelNamespace))
        return commands

    monkeypatch.setattr(reduce_mod, "_ascend_boundary_failure", spy)
    return cancels


@pytest.mark.asyncio
async def test_grandchild_failure_caught_emits_single_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A grandchild failure ascending to a catching top-level parent emits exactly
    one namespace cancel — the top-level child's subtree — not one per level."""
    cancels = _spy_boundary_cancels(monkeypatch)
    result = await WorkflowTestRunner(
        _TopCatchesGrand(mid=_MidNoHandler(grand=_BoomGrand()))
    ).run()
    assert result.result == "top-caught:mid"
    assert [c.namespace for c in cancels] == [("mid#0",)]


@pytest.mark.asyncio
async def test_root_uncaught_grandchild_failure_emits_single_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child failure ascending uncaught to the root emits exactly one namespace
    cancel for the failing child's subtree."""
    cancels = _spy_boundary_cancels(monkeypatch)
    handler = _ParentNoHandler(child=_BoomChild()).run()
    with pytest.raises(ValueError, match="boom-in-child"):
        await handler
    assert [c.namespace for c in cancels] == [("child#0",)]


# --- Root-uncaught boundary failure fails the whole run -----------------------


class _ParentNoHandler(Workflow):
    child: _BoomChild

    @step
    async def begin(self, ev: StartEvent) -> _ChildStart:
        return _ChildStart()

    @step
    async def finish(self, ev: _ChildStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_root_uncaught_child_failure_fails_run() -> None:
    handler = _ParentNoHandler(child=_BoomChild()).run()
    with pytest.raises(ValueError, match="boom-in-child"):
        await handler


# --- Recovery budget enforced across the boundary chain -----------------------


CHILD_ATTEMPTS = 0


class _AlwaysBoomChild(Workflow):
    @step
    async def run_child(self, ev: _ChildStart) -> _ChildStop:
        global CHILD_ATTEMPTS
        CHILD_ATTEMPTS += 1
        raise ValueError("boom-again")


class _ParentLimitedRecovery(Workflow):
    child: _AlwaysBoomChild

    @step
    async def begin(self, ev: StartEvent) -> _ChildStart:
        return _ChildStart()

    @catch_error(max_recoveries=2)
    async def recover(self, ev: StepFailedEvent) -> _ChildStart:
        # Re-trigger the child (a fresh invocation) until the recovery budget is
        # spent; the child fails every time, so the budget is what bounds this.
        return _ChildStart()

    @step
    async def finish(self, ev: _ChildStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_boundary_recovery_budget_is_enforced_across_chain() -> None:
    global CHILD_ATTEMPTS
    CHILD_ATTEMPTS = 0
    handler = _ParentLimitedRecovery(child=_AlwaysBoomChild()).run()
    with pytest.raises(ValueError, match="boom-again"):
        await handler
    # First attempt + 2 recoveries = 3 child invocations, then the budget is spent
    # and the failure ascends uncaught to the root.
    assert CHILD_ATTEMPTS == 3


# --- Legacy journal replay: no accrual, no spurious timeout command -----------


def test_legacy_unstamped_replay_emits_no_timeout() -> None:
    """A hand-built unstamped journal replays under the legacy clock fallback with
    an empty budget and no timeout/halt command."""
    workflow = _BudgetRoot()
    state = BrokerState.from_workflow(workflow)
    ticks: list[WorkflowTick] = [TickAddEvent(event=StartEvent())]
    state, _ = rewind_in_progress(state, 1000.0)
    exit_commands: list[object] = []
    for tick in ticks:
        state, commands = _reduce_tick(tick, state, 1_000_000.0)
        exit_commands.extend(
            c for c in commands if isinstance(c, (CommandFailWorkflow, CommandHalt))
        )
    assert state.elapsed_alive == 0.0
    assert exit_commands == []
