# ty: ignore[unknown-argument]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Regression tests for the child-workflow namespace consolidation.

The child-workflow feature added a ``namespace`` dimension but left several
runtime consumers modelling a flat step space. These tests pin child
``list[E]`` fan-in, cross-namespace stream accounting, child human-input
round-trips, namespace teardown, and the ``max_recoveries`` bound on namespace
timeout recovery.
"""

from __future__ import annotations

import asyncio

import pytest
from workflows import Context, Workflow
from workflows.decorators import catch_error, step
from workflows.errors import WorkflowRuntimeError, WorkflowTimeoutError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StepFailedEvent,
    StopEvent,
    get_event_origin_namespace,
)
from workflows.runtime.control_loop.reduce import terminate_namespace
from workflows.runtime.types.internal_state import (
    BrokerState,
    CollectionStreamInstance,
    EventAttempt,
    _binding_id,
)
from workflows.runtime.types.step_id import StepId
from workflows.testing import WorkflowTestRunner

# --- Phase 1: child fan-in + cross-namespace accounting -----------------------


class _Item(Event):
    n: int


class _ChildFanStart(StartEvent):
    pass


class _ChildFanStop(StopEvent):
    total: int = 0


class _FanChild(Workflow):
    @step
    async def fan(self, ev: _ChildFanStart) -> list[_Item]:
        return [_Item(n=1), _Item(n=2), _Item(n=3)]

    @step
    async def collect(self, events: list[_Item]) -> _ChildFanStop:
        return _ChildFanStop(total=sum(e.n for e in events))


class _FanParent(Workflow):
    child: _FanChild

    @step
    async def start(self, ev: StartEvent) -> _ChildFanStart:
        return _ChildFanStart()

    @step
    async def finish(self, ev: _ChildFanStop) -> StopEvent:
        return StopEvent(result=ev.total)


@pytest.mark.asyncio
async def test_child_list_fan_in_joins_all_items() -> None:
    """A ``list[E]`` fan-out/fan-in *inside a child* binds within the child
    namespace and joins every item — previously the binding was computed
    root-only, so the child collect never bound and every item was dropped.
    """
    result = await WorkflowTestRunner(_FanParent(child=_FanChild())).run()
    assert result.result == 6


class _Shared(Event):
    pass


class _ReuseChildStart(StartEvent):
    pass


class _ReuseChildStop(StopEvent):
    pass


class _ReuseChild(Workflow):
    @step
    async def run_child(self, ev: _ReuseChildStart) -> _Shared:
        return _Shared()

    @step
    async def also_accepts_shared(self, ev: _Shared) -> _ReuseChildStop:
        return _ReuseChildStop()


class _ReuseParent(Workflow):
    child: _ReuseChild

    @step
    async def fan(self, ev: StartEvent) -> list[_Shared]:
        return [_Shared()]

    @step
    async def collect(self, events: list[_Shared]) -> StopEvent:
        return StopEvent(result=len(events))


@pytest.mark.asyncio
async def test_root_stream_accounting_ignores_same_typed_child_step() -> None:
    """A root collection stream must not wedge when a child step happens to
    accept the streamed event type. Birth-count is namespace-scoped, so the
    child's ``also_accepts_shared`` is not counted into the root stream's open
    work items (which previously left a phantom item open forever).
    """
    result = await WorkflowTestRunner(_ReuseParent(child=_ReuseChild())).run()
    assert result.result == 1


def test_root_binding_id_is_byte_identical_to_pre_namespace_format() -> None:
    """Root binding ids embed the bare step name (no namespace prefix), so a
    snapshot written before the StepId conversion still resolves on resume.
    """
    state = BrokerState.from_workflow(_ReuseParent(child=_ReuseChild()))
    root_bindings = [
        b for b in state.config.collection_bindings.values() if b.source_step.is_root
    ]
    assert root_bindings, "expected a root fan->collect binding"
    for binding in root_bindings:
        expected = _binding_id(
            StepId.root(binding.source_step.name),
            StepId.root(binding.target_step.name),
            binding.item_types,
            binding.policy,
        )
        assert binding.id == expected
        # The id string carries the bare names, never a "ns/name" projection.
        assert binding.id.startswith(
            f"{binding.source_step.name}->{binding.target_step.name}:"
        )
        assert "/" not in binding.id.split(":", 1)[0]


# --- Grandchild (3-level) fan-in ----------------------------------------------


class _GrandStart(StartEvent):
    pass


class _GrandStop(StopEvent):
    total: int = 0


class _GrandFanChild(Workflow):
    @step
    async def fan(self, ev: _GrandStart) -> list[_Item]:
        return [_Item(n=2), _Item(n=5)]

    @step
    async def collect(self, events: list[_Item]) -> _GrandStop:
        return _GrandStop(total=sum(e.n for e in events))


class _MidStart(StartEvent):
    pass


class _MidStop(StopEvent):
    total: int = 0


class _MidChild(Workflow):
    grand: _GrandFanChild

    @step
    async def start(self, ev: _MidStart) -> _GrandStart:
        return _GrandStart()

    @step
    async def finish(self, ev: _GrandStop) -> _MidStop:
        return _MidStop(total=ev.total)


class _GrandParent(Workflow):
    mid: _MidChild

    @step
    async def start(self, ev: StartEvent) -> _MidStart:
        return _MidStart()

    @step
    async def finish(self, ev: _MidStop) -> StopEvent:
        return StopEvent(result=ev.total)


@pytest.mark.asyncio
async def test_grandchild_fan_in_binds_within_its_own_namespace() -> None:
    """A ``list[E]`` join three levels deep binds within the grandchild
    namespace ``(mid, grand)``."""
    result = await WorkflowTestRunner(
        _GrandParent(mid=_MidChild(grand=_GrandFanChild()))
    ).run()
    assert result.result == 7


# --- Phase 2: targeted child human-input round-trip ---------------------------


class _HitlChildStart(StartEvent):
    pass


class _HitlChildStop(StopEvent):
    answer: str = ""


class _HitlChild(Workflow):
    @step
    async def ask(self, ev: _HitlChildStart) -> InputRequiredEvent:
        return InputRequiredEvent(prefix="child?")  # type: ignore[reportCallIssue]

    @step
    async def answer(self, ev: HumanResponseEvent) -> _HitlChildStop:
        return _HitlChildStop(answer=ev.response)


class _HitlParent(Workflow):
    child: _HitlChild

    @step
    async def start(self, ev: StartEvent) -> _HitlChildStart:
        return _HitlChildStart()

    @step
    async def finish(self, ev: _HitlChildStop) -> StopEvent:
        return StopEvent(result=ev.answer)


@pytest.mark.asyncio
async def test_child_human_input_resolves_via_targeted_send() -> None:
    """A child surfaces an InputRequiredEvent; the caller answers it with a
    targeted ``send_event(resp, step="child/answer")`` that descends into the
    child namespace, and the run completes instead of timing out.
    """
    handler = _HitlParent(child=_HitlChild()).run()
    saw_request = False
    async for ev in handler.stream_events(include_children=True):
        if isinstance(ev, InputRequiredEvent):
            saw_request = True
            child_origin = get_event_origin_namespace(ev)
            handler.ctx.send_event(
                HumanResponseEvent(response="ok"),  # type: ignore[reportCallIssue]
                step=f"{child_origin[0]}/answer",
            )
    assert saw_request
    result = await handler
    assert result == "ok"


def test_resolve_target_rejects_unknown_child_step_with_actionable_error() -> None:
    """A bad child target names the valid namespaced steps, not just 'does not
    exist'."""
    wf = _HitlParent(child=_HitlChild())
    with pytest.raises(WorkflowRuntimeError) as excinfo:
        wf._resolve_target_step(
            "child/nope",
            HumanResponseEvent(response="x"),  # type: ignore[reportCallIssue]
        )
    message = str(excinfo.value)
    assert "child/nope does not exist" in message
    assert "child/answer" in message


class _RootHitlParent(Workflow):
    @step
    async def ask(self, ev: StartEvent) -> InputRequiredEvent:
        return InputRequiredEvent(prefix="root?")  # type: ignore[reportCallIssue]

    @step
    async def answer(self, ev: HumanResponseEvent) -> StopEvent:
        return StopEvent(result=ev.response)


@pytest.mark.asyncio
async def test_root_targeted_send_unchanged() -> None:
    """A bare step name still targets a root step (no namespace regression)."""
    handler = _RootHitlParent().run()
    async for ev in handler.stream_events():
        if isinstance(ev, InputRequiredEvent):
            handler.ctx.send_event(
                HumanResponseEvent(response="root-ok"),  # type: ignore[reportCallIssue]
                step="answer",
            )
    result = await handler
    assert result == "root-ok"


# --- Phase 3: one namespace lifecycle (teardown + bounded timeout recovery) ----


def test_terminate_namespace_prefix_matches_descendants_only() -> None:
    """One teardown clears a namespace and every descendant (prefix match),
    leaving sibling/ancestor namespaces untouched."""
    state = BrokerState.from_workflow(
        _GrandParent(mid=_MidChild(grand=_GrandFanChild()))
    )
    root = StepId((), "start")
    mid = StepId(("mid",), "start")
    grand = StepId(("mid", "grand"), "fan")
    mid_invocation = ("mid#abc",)
    grand_invocation = ("mid#abc", "grand#def")
    state.workers[root].queue.append(EventAttempt(event=_Item(n=1)))
    state.workers[root].collected_events["buf"] = [_Item(n=1)]
    state.workers[mid].queue.append(
        EventAttempt(event=_Item(n=1), invocation_namespace=mid_invocation)
    )
    state.workers[mid].collected_events_by_invocation[mid_invocation] = {
        "buf": [_Item(n=1)]
    }
    state.workers[grand].queue.append(
        EventAttempt(event=_Item(n=1), invocation_namespace=grand_invocation)
    )
    state.workers[grand].collected_events_by_invocation[grand_invocation] = {
        "buf": [_Item(n=1)]
    }
    state.streams["s-grand"] = CollectionStreamInstance(
        stream_id="s-grand",
        source_step=grand,
        scope_path=(),
        source_invocation_namespace=grand_invocation,
        open_work_items=1,
    )
    state.namespace_started[()] = 1.0
    state.namespace_started[mid_invocation] = 1.0
    state.namespace_started[grand_invocation] = 1.0

    terminate_namespace(state, mid_invocation)

    # The child and grandchild are fully cleared.
    for sid in (mid, grand):
        assert not state.workers[sid].queue
        assert not state.workers[sid].collected_events_by_invocation
    assert mid_invocation not in state.namespace_started
    assert grand_invocation not in state.namespace_started
    assert "s-grand" not in state.streams
    # The root (ancestor) is untouched.
    assert state.workers[root].queue
    assert state.workers[root].collected_events
    assert () in state.namespace_started


class _OrphanChildStart(StartEvent):
    pass


class _OrphanChildStop(StopEvent):
    pass


class _RecoveringSlowChild(Workflow):
    @step
    async def run_child(self, ev: _OrphanChildStart) -> _OrphanChildStop:
        # Orphaned coroutine: still running well after the child's timeout fires.
        await asyncio.sleep(2)
        return _OrphanChildStop()

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> _OrphanChildStop:
        assert isinstance(ev.exception, WorkflowTimeoutError)
        return _OrphanChildStop()


class _SlowBranch(Event):
    pass


class _BranchDone(Event):
    pass


class _OrphanParent(Workflow):
    child: _RecoveringSlowChild

    @step
    async def begin(
        self, ctx: Context, ev: StartEvent
    ) -> _OrphanChildStart | _SlowBranch:
        # Fan out: trigger the child AND a parent branch that outlives the orphan.
        ctx.send_event(_SlowBranch())
        return _OrphanChildStart()

    @step
    async def slow_branch(self, ev: _SlowBranch) -> _BranchDone:
        await asyncio.sleep(0.5)
        return _BranchDone()

    @step
    async def finish(
        self, ctx: Context, ev: _OrphanChildStop | _BranchDone
    ) -> StopEvent | None:
        got = ctx.collect_events(ev, [_OrphanChildStop, _BranchDone])
        if got is None:
            return None
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_caught_child_timeout_cancels_orphan_no_loop_crash() -> None:
    """A caught child timeout cancels the still-running child coroutine, so it
    cannot later report into a torn-down worker slot and crash the loop with
    'Worker not found in in_progress'. The run completes via recovery + the
    parent branch."""
    handler = _OrphanParent(child=_RecoveringSlowChild(timeout=0.05), timeout=30).run()
    result = await asyncio.wait_for(handler, timeout=10)
    assert result == "done"


class _LeakChildStart(StartEvent):
    pass


class _LeakChildStop(StopEvent):
    tag: str = ""


class _FastPath(Event):
    pass


class _SlowPath(Event):
    pass


class _LeakChild(Workflow):
    @step
    async def begin(self, ctx: Context, ev: _LeakChildStart) -> _FastPath | _SlowPath:
        ctx.send_event(_SlowPath())
        return _FastPath()

    @step
    async def fast(self, ev: _FastPath) -> _LeakChildStop:
        return _LeakChildStop(tag="fast")

    @step
    async def slow(self, ev: _SlowPath) -> _LeakChildStop:
        await asyncio.sleep(0.3)
        return _LeakChildStop(tag="slow")


class _KeepAlive(Event):
    pass


class _LeakParent(Workflow):
    child: _LeakChild

    @step
    async def begin(self, ctx: Context, ev: StartEvent) -> _LeakChildStart | _KeepAlive:
        ctx.send_event(_KeepAlive())
        return _LeakChildStart()

    @step
    async def observe(self, ctx: Context, ev: _LeakChildStop) -> None:
        seen = await ctx.store.get("stops", default=0)
        await ctx.store.set("stops", seen + 1)
        return None

    @step
    async def keep_alive(self, ctx: Context, ev: _KeepAlive) -> StopEvent:
        # Outlive the slow child sibling (0.3s); if a leak existed its second
        # ChildStop would have surfaced by now.
        await asyncio.sleep(0.6)
        return StopEvent(result=await ctx.store.get("stops", default=0))


@pytest.mark.asyncio
async def test_child_stop_terminates_sibling_no_double_fire() -> None:
    """A child that returns its StopEvent while a sibling branch is in flight
    surfaces exactly one StopEvent to the parent — the boundary terminates the
    whole child namespace, cancelling the slow sibling before it can fire a
    second StopEvent."""
    handler = _LeakParent(child=_LeakChild(), timeout=30).run()
    result = await asyncio.wait_for(handler, timeout=10)
    assert result == 1


class _GTeardownGrandStart(StartEvent):
    pass


class _GTeardownGrandStop(StopEvent):
    pass


class _GTeardownGrand(Workflow):
    @step
    async def work(self, ev: _GTeardownGrandStart) -> _GTeardownGrandStop:
        await asyncio.sleep(0.4)
        return _GTeardownGrandStop()


class _GTeardownChildStart(StartEvent):
    pass


class _GTeardownChildStop(StopEvent):
    pass


class _GTeardownFast(Event):
    pass


class _GTeardownChild(Workflow):
    grand: _GTeardownGrand

    @step
    async def begin(
        self, ctx: Context, ev: _GTeardownChildStart
    ) -> _GTeardownGrandStart | _GTeardownFast:
        # Trigger the slow grandchild AND a fast path that stops the child.
        ctx.send_event(_GTeardownFast())
        return _GTeardownGrandStart()

    @step
    async def stop_fast(self, ev: _GTeardownFast) -> _GTeardownChildStop:
        return _GTeardownChildStop()

    @step
    async def absorb_grand(self, ctx: Context, ev: _GTeardownGrandStop) -> None:
        # Would only fire if the grandchild leaked past the child's teardown.
        await ctx.store.set("grand_leaked", True)
        return None


class _GTeardownParent(Workflow):
    child: _GTeardownChild

    @step
    async def begin(self, ev: StartEvent) -> _GTeardownChildStart:
        return _GTeardownChildStart()

    @step
    async def finish(self, ev: _GTeardownChildStop) -> StopEvent:
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_child_stop_tears_down_inflight_grandchild() -> None:
    """When a child returns its StopEvent, an in-flight grandchild is torn down
    with it (prefix teardown + task cancellation), so the grandchild's later
    completion never surfaces and the run completes cleanly."""
    handler = _GTeardownParent(child=_GTeardownChild(grand=_GTeardownGrand())).run()
    result = await asyncio.wait_for(handler, timeout=10)
    assert result == "done"
    # Give a cancelled grandchild's 0.4s sleep time to (not) fire.
    await asyncio.sleep(0.6)


class _ReArmChildStart(StartEvent):
    pass


class _ReArmChildStop(StopEvent):
    pass


class _ReArmingChild(Workflow):
    @step
    async def run_child(self, ev: _ReArmChildStart) -> _ReArmChildStop:
        await asyncio.sleep(2)  # always exceeds the child's own timeout
        return _ReArmChildStop()

    @catch_error(max_recoveries=2)
    async def recover(self, ev: StepFailedEvent) -> _ReArmChildStart:
        # Re-arm: re-trigger the child instead of stopping, so it times out
        # again. Bounded by max_recoveries; otherwise it would loop forever.
        return _ReArmChildStart()


class _ReArmParent(Workflow):
    child: _ReArmingChild

    @step
    async def begin(self, ev: StartEvent) -> _ReArmChildStart:
        return _ReArmChildStart()

    @step
    async def finish(self, ev: _ReArmChildStop) -> StopEvent:
        return StopEvent(result="never")


@pytest.mark.asyncio
async def test_rearming_child_timeout_bounded_by_max_recoveries() -> None:
    """A child whose @catch_error re-arms the timeout fails the run after
    max_recoveries, instead of looping forever."""
    handler = _ReArmParent(child=_ReArmingChild(timeout=0.05), timeout=30).run()
    with pytest.raises(WorkflowTimeoutError):
        await asyncio.wait_for(handler, timeout=10)
