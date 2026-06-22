# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Regression tests for the typed ``list[E]`` fan-out/fan-in stream accounting.

Each case below is a minimal reproduction for a bug class that can silently
truncate a joined stream or leave it waiting forever. Assertions stay on
observable behavior: the run completes, the join sees the expected stream, and
capacity limits are honored.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Callable

import pytest
from workflows import Context, Workflow, catch_error, step
from workflows.collect import Collect, Take
from workflows.errors import WorkflowValidationError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StepFailedEvent,
    StopEvent,
)
from workflows.retry_policy import retry_policy, stop_after_attempt, wait_fixed

_CONTROL_LOOP_LOGGER = "workflows.runtime.control_loop"

# Counter-drift smells: a premature close shows up as decrements against an
# already-closed stream or a negative counter, even when the run "succeeds".
_ACCOUNTING_SMELLS = ("already-closed stream", "went negative")


def _assert_clean_accounting(caplog: pytest.LogCaptureFixture) -> None:
    smells = [
        r.getMessage()
        for r in caplog.records
        if r.name == _CONTROL_LOOP_LOGGER
        and any(needle in r.getMessage() for needle in _ACCOUNTING_SMELLS)
    ]
    assert not smells, smells


class Task(Event):
    n: int


class Done(Event):
    n: int


async def _run(wf: Workflow, timeout: float = 6.0) -> object:
    """Run to completion, failing loudly (not hanging) if a stream never closes."""
    return await asyncio.wait_for(wf.run(), timeout=timeout)


# ---------------------------------------------------------------------------
# Member accounting: an event accepted by two steps is two work items. The join
# must still see the full stream.
# ---------------------------------------------------------------------------


async def test_two_collects_same_type_see_full_stream() -> None:
    """Two `list[Done]` joins on the same element type each see the whole stream."""
    a_calls: list[list[int]] = []
    b_calls: list[list[int]] = []

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def collect_a(self, events: list[Done]) -> StopEvent:
            a_calls.append(sorted(e.n for e in events))
            return StopEvent(result=sorted(e.n for e in events))

        @step
        async def collect_b(self, events: list[Done]) -> None:
            b_calls.append(sorted(e.n for e in events))
            return None

    await _run(WF(timeout=8))
    assert a_calls == [[0, 1, 2]], a_calls
    assert b_calls == [[0, 1, 2]], b_calls


async def test_event_routed_to_step_and_join_keeps_full_stream() -> None:
    """A fanned-out event consumed by both a 1:1 step and a join loses no members."""

    class Echo(Event):
        n: int

    join_calls: list[list[int]] = []
    echoed: list[int] = []

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(5)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step(skip_graph_checks=["dead_end"])
        async def passthrough(self, ev: Done) -> Echo:
            return Echo(n=ev.n)

        @step(skip_graph_checks=["dead_end"])
        async def sink(self, ev: Echo) -> None:
            echoed.append(ev.n)
            return None

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            join_calls.append(sorted(e.n for e in events))
            return StopEvent(result=sorted(e.n for e in events))

    await _run(WF(timeout=8))
    assert join_calls == [[0, 1, 2, 3, 4]], join_calls
    assert sorted(echoed) == [0, 1, 2, 3, 4], echoed


async def test_list_collect_honors_accept_event_subclasses() -> None:
    class BaseDone(Event):
        n: int

    class DerivedDone(BaseDone):
        pass

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[DerivedDone]:
            return [DerivedDone(n=1), DerivedDone(n=2)]

        @step(accept_event_subclasses=True)
        async def join(self, events: list[BaseDone]) -> StopEvent:
            return StopEvent(result=[type(e).__name__ for e in events])

    result = await _run(WF(timeout=8))
    assert result == ["DerivedDone", "DerivedDone"]


async def test_distinct_fan_out_sources_share_collect_target_without_merging() -> None:
    class Batch(Event):
        nums: list[int]

    batches: list[list[int]] = []

    class WF(Workflow):
        @step
        async def fan_a(self, ev: StartEvent) -> list[Done]:
            return [Done(n=0), Done(n=1)]

        @step
        async def fan_b(self, ev: StartEvent) -> list[Done]:
            return [Done(n=10), Done(n=11)]

        @step
        async def join(self, events: list[Done]) -> Batch:
            return Batch(nums=sorted(e.n for e in events))

        @step
        async def finish(self, ev: Batch) -> StopEvent | None:
            batches.append(ev.nums)
            if len(batches) < 2:
                return None
            return StopEvent(result=sorted(batches))

    result = await _run(WF(timeout=8))
    assert result == [[0, 1], [10, 11]]


@pytest.mark.asyncio
async def test_concurrent_collect_invocations_keep_distinct_implicit_waiters() -> None:
    """Two collect invocations of the same join step (one per fan-out source)
    each wait for the same event type with an implicit waiter id. Collect
    invocations are fired directly rather than through a routed tick, so they
    must still get distinct work item ids — otherwise both waiters collapse onto
    one and only one invocation resumes."""

    class Batch(Event):
        nums: list[int]

    batches: list[list[int]] = []

    class WF(Workflow):
        @step
        async def fan_a(self, ev: StartEvent) -> list[Done]:
            return [Done(n=0), Done(n=1)]

        @step
        async def fan_b(self, ev: StartEvent) -> list[Done]:
            return [Done(n=10), Done(n=11)]

        @step
        async def join(self, ctx: Context, events: list[Done]) -> Batch:
            response = await ctx.wait_for_event(
                HumanResponseEvent,
                waiter_event=InputRequiredEvent(),
            )
            return Batch(nums=sorted(e.n for e in events) + [int(response.response)])

        @step
        async def finish(self, ev: Batch) -> StopEvent | None:
            batches.append(ev.nums)
            if len(batches) < 2:
                return None
            return StopEvent(result=sorted(batches))

    workflow = WF(timeout=8.0)
    handler = workflow.run()
    prompts = 0
    async for ev in handler.stream_events():
        if isinstance(ev, InputRequiredEvent):
            prompts += 1
            if prompts == 2:
                handler.ctx.send_event(HumanResponseEvent(response="7"))  # type: ignore
                break

    assert prompts == 2
    result = await handler
    assert result == [[0, 1, 7], [10, 11, 7]]


# ---------------------------------------------------------------------------
# Error paths keep the work item's stream stack across retry and recovery.
# ---------------------------------------------------------------------------


async def test_catch_error_recovery_rejoins_stream() -> None:
    """A member recovered by @catch_error rejoins the stream and makes the join.

    The handler runs in the failed member's scope: the work item travels whole
    to the handler invocation, and the handler's member-typed emission stays
    in-stream. The join therefore always sees the recovered member — never a
    truncated batch from the stream closing first.
    """

    class WF(Workflow):
        @step
        async def start(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            if ev.n == 1:
                raise RuntimeError("boom on member 1")
            return Done(n=ev.n)

        @catch_error(for_steps=["work"], max_recoveries=2)
        async def recover(self, ev: StepFailedEvent) -> Done:
            return Done(n=1000 + getattr(ev.input_event, "n", -1))

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(WF(timeout=8), timeout=6)
    assert result == [0, 2, 1001], result


async def test_catch_error_successor_can_fan_out_inside_stream() -> None:
    class RecoveryPiece(Event):
        n: int

    class WF(Workflow):
        @step
        async def start(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            if ev.n == 1:
                raise RuntimeError("boom on member 1")
            return Done(n=ev.n)

        @catch_error(for_steps=["work"])
        async def recover(self, ev: StepFailedEvent) -> list[RecoveryPiece]:
            return [RecoveryPiece(n=1000), RecoveryPiece(n=1)]

        @step
        async def collect_recovery(self, events: list[RecoveryPiece]) -> Done:
            return Done(n=sum(e.n for e in events))

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(WF(timeout=8), timeout=6)
    assert result == [0, 2, 1001]


# ---------------------------------------------------------------------------
# Collection releases use the normal worker capacity path. Overlapping releases
# must queue distinct payloads instead of aliasing in-progress state.
# ---------------------------------------------------------------------------


async def test_num_workers_1_collect_overlapping_streams() -> None:
    class Seed(Event):
        gid: int

    class Leaf(Event):
        gid: int
        k: int

    class Collected(Event):
        gid: int
        n: int

    collected: list[tuple[int, int]] = []
    active_collects = 0
    max_active_collects = 0

    class WF(Workflow):
        @step
        async def seed(self, ev: StartEvent) -> list[Seed]:
            return [Seed(gid=0), Seed(gid=1)]

        @step(num_workers=2)
        async def fan_inner(self, ev: Seed) -> list[Leaf]:
            return [Leaf(gid=ev.gid, k=k) for k in range(3)]

        @step(num_workers=1)
        async def collect(self, stream: list[Leaf]) -> Collected:
            nonlocal active_collects, max_active_collects
            active_collects += 1
            max_active_collects = max(max_active_collects, active_collects)
            await asyncio.sleep(0.2)
            try:
                gid = next(iter({b.gid for b in stream}))
                return Collected(gid=gid, n=len(stream))
            finally:
                active_collects -= 1

        @step
        async def finish(self, ev: Collected) -> StopEvent | None:
            collected.append((ev.gid, ev.n))
            if len(collected) < 2:
                return None
            return StopEvent(result=sorted(collected))

    await _run(WF(timeout=10), timeout=8)
    assert sorted(collected) == [(0, 3), (1, 3)], collected
    assert max_active_collects == 1


# ---------------------------------------------------------------------------
# Nested fan-out still summarizes when inner joins drop some or all branches.
# ---------------------------------------------------------------------------


class InnerTask(Event):
    outer: int
    inner: int


class InnerDone(Event):
    outer: int
    inner: int


class InnerSummary(Event):
    outer: int
    total: int


def _nested_workflow(per_inner_drops: Callable[[int], bool]) -> type[Workflow]:
    class Nested(Workflow):
        @step
        async def outer(self, ev: StartEvent) -> list[Task]:
            return [Task(n=o) for o in range(3)]

        @step
        async def inner(self, ev: Task) -> list[InnerTask]:
            return [InnerTask(outer=ev.n, inner=i) for i in range(2)]

        @step
        async def inner_work(self, ev: InnerTask) -> InnerDone:
            return InnerDone(outer=ev.outer, inner=ev.inner)

        @step
        async def per_inner(self, events: list[InnerDone]) -> InnerSummary | None:
            outer = events[0].outer
            if per_inner_drops(outer):
                return None
            return InnerSummary(outer=outer, total=len(events))

        @step
        async def per_outer(self, events: list[InnerSummary]) -> StopEvent:
            return StopEvent(result=sorted((s.outer, s.total) for s in events))

    return Nested


async def test_nested_partial_inner_drop_sees_subset() -> None:
    """One inner join drops, the outer join sees the survivors."""
    wf = _nested_workflow(lambda outer: outer == 1)
    result = await _run(wf(timeout=8))
    assert result == [(0, 2), (2, 2)], result


async def test_nested_all_inner_dropped_terminates() -> None:
    """Every inner join drops; the outer join must still fire once with []."""
    wf = _nested_workflow(lambda outer: True)
    result = await _run(wf(timeout=8))
    assert result == [], result


# ---------------------------------------------------------------------------
# Persistence: a snapshot taken mid-fan-out can resume without losing stream ids
# or in-progress member scope.
# ---------------------------------------------------------------------------


_RESUME_GATE = asyncio.Event()
_RESUME_SEEN: list[int] = []


def _gated_fan_out_workflow() -> type[Workflow]:
    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(5)]

        @step(num_workers=8)
        async def work(self, ev: Task) -> Done:
            _RESUME_SEEN.append(ev.n)
            await _RESUME_GATE.wait()  # hold the stream open at snapshot time
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    return FanOut


async def test_resume_mid_open_stream_completes() -> None:
    _RESUME_GATE.clear()
    _RESUME_SEEN.clear()

    wf = _gated_fan_out_workflow()(timeout=30)
    handler = wf.run()
    for _ in range(300):
        if _RESUME_SEEN:
            break
        await asyncio.sleep(0.02)
    assert _RESUME_SEEN, "workers never started"
    await asyncio.sleep(0.2)  # let the rest of the stream settle into the queue

    snapshot = handler.ctx.to_dict()
    await handler.cancel_run()
    try:
        await asyncio.wait_for(handler, timeout=2)
    except BaseException:
        pass

    _RESUME_GATE.set()
    wf2 = _gated_fan_out_workflow()(timeout=30)
    restored = Context.from_dict(wf2, snapshot)
    result = await asyncio.wait_for(wf2.run(ctx=restored), timeout=10)
    assert result == [0, 1, 2, 3, 4], result


# ---------------------------------------------------------------------------
# Signature validation rejects unsupported shapes with useful errors, while
# supported multi-slot joins must consume one event per slot without hanging.
# ---------------------------------------------------------------------------


async def test_same_type_multi_slot_join_consumes_one_event_per_slot() -> None:
    class A(Event):
        value: str

    class WF(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> A | None:
            ctx.send_event(A(value="one"))
            ctx.send_event(A(value="two"))
            return None

        @step
        async def join(self, a: A, b: A) -> StopEvent:
            return StopEvent(result=f"{a.value}+{b.value}")

    assert await _run(WF(timeout=3), timeout=3) == "one+two"


def test_optional_list_collect_param_not_generic_error() -> None:
    """Fan-out return unwraps Optional/Union; the fan-in param side should too."""

    def _build() -> None:
        class WF(Workflow):
            @step
            async def fan(self, ev: StartEvent) -> Done:
                return Done(n=0)

            @step
            async def collect(self, events: list[Done] | None) -> StopEvent:
                return StopEvent(result=len(events or []))

    try:
        _build()
    except WorkflowValidationError as e:
        assert "at least one parameter annotated as type Event" not in str(e), str(e)


# ---------------------------------------------------------------------------
# ctx.send_event is ordinary dispatch, not stream membership. list[E] fan-in is
# only for returned-list producer streams.
# ---------------------------------------------------------------------------


class Item(Event):
    idx: int


async def test_send_event_into_take_collect_rejected() -> None:
    """Unstreamed send_event flows must use ctx.collect_events, not list[E]."""

    class WF(Workflow):
        @step
        async def start(self, ctx: Context, ev: StartEvent) -> StopEvent | None:
            for i in range(3):
                ctx.send_event(Item(idx=i))
            return None

        @step
        async def collect(
            self, events: Annotated[list[Item], Collect(Take(3))]
        ) -> StopEvent:
            return StopEvent(result=sorted(e.idx for e in events))

    with pytest.raises(WorkflowValidationError, match="returned-list producer"):
        await _run(WF(timeout=10))


async def test_send_event_into_all_collect_rejected() -> None:
    class WF(Workflow):
        @step
        async def start(self, ctx: Context, ev: StartEvent) -> StopEvent | None:
            for i in range(3):
                ctx.send_event(Item(idx=i))
            return None

        @step
        async def collect(self, events: list[Item]) -> StopEvent:
            return StopEvent(result=sorted(e.idx for e in events))

    with pytest.raises(WorkflowValidationError, match="returned-list producer"):
        await _run(WF(timeout=10), timeout=5)


async def test_send_event_only_inside_collection_param_rejected() -> None:
    """A list[E] collect needs a returned-list producer binding."""

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ctx: Context, ev: Task) -> None:
            ctx.send_event(Done(n=ev.n))
            return None

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    with pytest.raises(WorkflowValidationError, match="returned-list producer"):
        await _run(WF(timeout=6), timeout=5)


# ---------------------------------------------------------------------------
# Resume mid-stream with a member retrying. Combines the persist/resume path with
# an in-flight retry: the snapshot must preserve both the open stream's live count
# and the retrying member's scope_path, and the resumed run must not double- or
# under-count the member when it re-runs.
# ---------------------------------------------------------------------------


_RETRY_RESUME_GATE = asyncio.Event()
_RETRY_RESUME_SEEN: list[int] = []
_RETRY_RESUME_FAILED: dict[int, bool] = {}


def _gated_retry_fan_out_workflow() -> type[Workflow]:
    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(5)]

        @step(
            num_workers=8,
            retry_policy=retry_policy(
                wait=wait_fixed(0.01), stop=stop_after_attempt(3)
            ),
        )
        async def work(self, ev: Task) -> Done:
            _RETRY_RESUME_SEEN.append(ev.n)
            if ev.n == 3 and not _RETRY_RESUME_FAILED.get(3):
                _RETRY_RESUME_FAILED[3] = True
                raise RuntimeError("transient on member 3")
            await _RETRY_RESUME_GATE.wait()  # hold the stream open at snapshot time
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    return FanOut


async def test_resume_mid_stream_with_retry_in_flight_completes() -> None:
    _RETRY_RESUME_GATE.clear()
    _RETRY_RESUME_SEEN.clear()
    _RETRY_RESUME_FAILED.clear()

    wf = _gated_retry_fan_out_workflow()(timeout=30)
    handler = wf.run()
    # Wait until member 3 has failed once (its retry is now scheduled/in flight).
    for _ in range(300):
        if _RETRY_RESUME_FAILED.get(3):
            break
        await asyncio.sleep(0.02)
    assert _RETRY_RESUME_FAILED.get(3), "member 3 never failed"
    await asyncio.sleep(0.2)  # let the stream settle with the retry pending

    snapshot = handler.ctx.to_dict()
    await handler.cancel_run()
    try:
        await asyncio.wait_for(handler, timeout=2)
    except BaseException:
        pass

    _RETRY_RESUME_GATE.set()
    wf2 = _gated_retry_fan_out_workflow()(timeout=30)
    restored = Context.from_dict(wf2, snapshot)
    result = await asyncio.wait_for(wf2.run(ctx=restored), timeout=10)
    assert result == [0, 1, 2, 3, 4], result


# ---------------------------------------------------------------------------
# Consume-once accounting for merge-shaped steps inside a stream. Each shape
# below resolves every birth-counted delivery exactly once: a stale-snapshot
# rerun is the same live work item (must not consume), a slot-buffering
# invocation consumes its trigger, and a waiter that steals a delivery consumes
# it on behalf of the step it woke.
# ---------------------------------------------------------------------------


async def test_in_stream_collect_events_default_workers_full_batch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """ctx.collect_events inside a fan-out branch sees the whole batch.

    With the default num_workers, concurrent buffering invocations rerun on
    stale snapshots; counting each rerun closed the stream early and released
    the join with [] (silent data loss).
    """

    class Summed(Event):
        total: int

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step  # num_workers default 4: buffering invocations overlap
        async def gather(self, ctx: Context, ev: Task) -> Summed | None:
            await asyncio.sleep(0.01)  # encourage overlapping snapshots
            events = ctx.collect_events(ev, [Task] * 3)
            if events is None:
                return None
            return Summed(total=sum(e.n for e in events))

        @step
        async def join(self, events: list[Summed]) -> StopEvent:
            return StopEvent(result=[e.total for e in events])

    with caplog.at_level(logging.WARNING, logger=_CONTROL_LOOP_LOGGER):
        result = await _run(WF(timeout=8))

    assert result == [3], result
    _assert_clean_accounting(caplog)


def _pair_join_workflow(task_count: int) -> tuple[type[Workflow], type[Event]]:
    class A(Event):
        n: int

    class B(Event):
        n: int

    class C(Event):
        n: int

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(task_count)]

        @step
        async def worker_a(self, ev: Task) -> A:
            return A(n=ev.n)

        @step
        async def worker_b(self, ev: Task) -> B:
            return B(n=ev.n + 10)

        @step
        async def pair_join(self, a: A, b: B) -> C:
            return C(n=a.n + b.n)

        @step
        async def join(self, events: list[C]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    return WF, C


async def test_in_stream_multi_slot_join_single_pair(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A two-slot join fed from inside a fan-out stream completes the stream.

    The slot-buffering invocation must consume its work item; otherwise an
    N-slot join leaks N-1 items per firing and the run dies with a stuck-stream
    failure.
    """
    wf_cls, _ = _pair_join_workflow(1)

    with caplog.at_level(logging.WARNING, logger=_CONTROL_LOOP_LOGGER):
        result = await _run(wf_cls(timeout=8))

    assert result == [10], result
    _assert_clean_accounting(caplog)


async def test_in_stream_multi_slot_join_many_members_completes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Multiple same-type members racing into a two-slot join still complete.

    Slot pairing is by arrival order and a duplicate arrival for an
    already-filled slot is dropped, so the batch size can vary — but every
    delivery must be consumed exactly once: the run completes (no stuck-stream
    failure) and the accounting stays clean.
    """
    wf_cls, _ = _pair_join_workflow(3)

    with caplog.at_level(logging.WARNING, logger=_CONTROL_LOOP_LOGGER):
        result = await _run(wf_cls(timeout=8))

    assert isinstance(result, list)
    assert 1 <= len(result) <= 3, result
    _assert_clean_accounting(caplog)


_STEAL_GATE = asyncio.Event()


class _StealWaiting(Event):
    pass


async def test_waiter_steal_of_in_stream_member_completes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A member that resolves a waiter on a step that also accepts it.

    Waiter matching swallows the member's normal delivery to the woken step;
    that birth-counted delivery must still be consumed or the stream never
    closes. The stolen member never joins the woken step's processing — the
    waiter consumed it — but the list[E] join still sees every member.
    """
    _STEAL_GATE.clear()

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            if ev.n == 2:
                # Hold the resolving member until the waiter is registered.
                await _STEAL_GATE.wait()
            return Done(n=ev.n)

        @step
        async def observe(self, ctx: Context, ev: Done) -> None:
            if ev.n == 0:
                await ctx.wait_for_event(
                    Done,
                    requirements={"n": 2},
                    waiter_event=_StealWaiting(),
                    timeout=6,
                )
            return None

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    with caplog.at_level(logging.WARNING, logger=_CONTROL_LOOP_LOGGER):
        handler = WF(timeout=10).run()
        async for ev in handler.stream_events():
            if isinstance(ev, _StealWaiting) and not _STEAL_GATE.is_set():
                _STEAL_GATE.set()
        result = await asyncio.wait_for(handler, timeout=8)

    assert result == [0, 1, 2], result
    _assert_clean_accounting(caplog)
