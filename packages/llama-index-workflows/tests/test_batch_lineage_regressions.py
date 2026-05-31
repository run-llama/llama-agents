# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Regression tests for the typed ``list[E]`` fan-out/fan-in batch accounting.

Each case below is a minimal, user-facing reproduction of a batch-lineage
defect found while de-risking the feature: silent data loss (a join fires with
a truncated batch) or an indefinite hang (a batch never closes, the join never
fires). All assertions are on observable behavior (the run completes / the join
sees the full batch), not on internal reducer state, so they remain valid
across a control-loop rewrite and flip to green when the accounting is fixed.

Known-broken cases are marked ``xfail(strict=True)``: they keep CI green today
and fail loudly the moment the underlying bug is fixed, which is the signal to
delete the marker. Every run is wrapped in ``asyncio.wait_for`` so a hang fails
the test instead of stalling the suite.
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Callable

import pytest
from workflows import Collect, Context, Take, Workflow, catch_error, step
from workflows.errors import WorkflowValidationError
from workflows.events import Event, StartEvent, StepFailedEvent, StopEvent
from workflows.retry_policy import retry_policy, stop_after_attempt, wait_fixed


class Task(Event):
    n: int


class Done(Event):
    n: int


async def _run(wf: Workflow, timeout: float = 6.0) -> object:
    """Run to completion, failing loudly (not hanging) if a batch never closes."""
    return await asyncio.wait_for(wf.run(), timeout=timeout)


# ---------------------------------------------------------------------------
# Member accounting: batch_pending is decremented per (member x consumer)
# instead of per delivered member, so batches close early and drop members.
# ---------------------------------------------------------------------------


async def test_two_collects_same_type_see_full_batch() -> None:
    """Two `list[Done]` joins on the same element type each see the whole batch."""
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


async def test_event_routed_to_step_and_join_keeps_full_batch() -> None:
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


# ---------------------------------------------------------------------------
# Error paths drop batch_stack from the re-queued event, so the recovered /
# retried member never decrements its batch and the join hangs forever.
# ---------------------------------------------------------------------------


async def test_retried_batch_member_keeps_lineage() -> None:
    """A member that fails once and succeeds on retry still closes the batch."""
    attempts = {"n2": 0}

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(5)]

        @step(
            retry_policy=retry_policy(wait=wait_fixed(0.01), stop=stop_after_attempt(3))
        )
        async def work(self, ev: Task) -> Done:
            if ev.n == 2 and attempts["n2"] == 0:
                attempts["n2"] += 1
                raise RuntimeError("transient on member 2")
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(WF(timeout=10), timeout=8)
    assert result == [0, 1, 2, 3, 4], result


async def test_catch_error_recovery_closes_batch() -> None:
    """A member recovered by @catch_error must still let the batch close."""

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
    # recovered member counted -> [0, 2, 1001]; dead branch -> [0, 2]
    assert result in ([0, 2], [0, 2, 1001]), result


# ---------------------------------------------------------------------------
# num_workers=1 collect: when two batches' closes overlap, the worker-id-0
# fallback aliases their in-progress state and one batch's result is lost.
# ---------------------------------------------------------------------------


async def test_num_workers_1_collect_overlapping_batches() -> None:
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
        async def collect(self, batch: list[Leaf]) -> Collected:
            nonlocal active_collects, max_active_collects
            active_collects += 1
            max_active_collects = max(max_active_collects, active_collects)
            await asyncio.sleep(0.2)
            try:
                gid = next(iter({b.gid for b in batch}))
                return Collected(gid=gid, n=len(batch))
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
# Nested fan-out: the empty-batch firing gate stops one level too early, so a
# middle batch that closes empty never fires its join and the run hangs.
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
    """Cleared behavior: one inner join drops, the outer join sees the survivors."""
    wf = _nested_workflow(lambda outer: outer == 1)
    result = await _run(wf(timeout=8))
    assert result == [(0, 2), (2, 2)], result


async def test_nested_all_inner_dropped_terminates() -> None:
    """Every inner join drops; the outer join must still fire once with []."""
    wf = _nested_workflow(lambda outer: True)
    result = await _run(wf(timeout=8))
    assert result == [], result


# ---------------------------------------------------------------------------
# Persistence: a workflow snapshotted mid-fan-out cannot resume (batch ids are
# re-minted under run_id=None and in_progress lineage is dropped), so the
# resumed run hangs. This is the durable/server resume path.
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
            await _RESUME_GATE.wait()  # hold the batch open at snapshot time
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    return FanOut


async def test_resume_mid_open_batch_completes() -> None:
    _RESUME_GATE.clear()
    _RESUME_SEEN.clear()

    wf = _gated_fan_out_workflow()(timeout=30)
    handler = wf.run()
    for _ in range(300):
        if _RESUME_SEEN:
            break
        await asyncio.sleep(0.02)
    assert _RESUME_SEEN, "workers never started"
    await asyncio.sleep(0.2)  # let the rest of the batch settle into the queue

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
# Signature validation: unsatisfiable joins are accepted then deadlock, and an
# Optional list collect param is rejected with a misleading generic error.
# ---------------------------------------------------------------------------


async def test_same_type_multi_slot_join_rejected() -> None:
    class A(Event):
        value: str

    class WF(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> A | None:
            ctx.send_event(A(value="one"))
            return None

        @step
        async def join(self, a: A, b: A) -> StopEvent:
            return StopEvent(result=f"{a.value}+{b.value}")

    try:
        await asyncio.wait_for(WF(timeout=3).run(), timeout=3)
    except WorkflowValidationError:
        return  # correct: rejected before it can deadlock
    except asyncio.TimeoutError:
        pytest.fail("join(a: A, b: A) was accepted then deadlocked")
    pytest.fail("workflow completed unexpectedly")


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
# Old/new API boundary: ctx.send_event is ordinary dispatch, not batch
# membership. list[E] fan-in is only for returned-list producer batches.
# ---------------------------------------------------------------------------


class Item(Event):
    idx: int


async def test_send_event_into_take_collect_rejected() -> None:
    """Unbatched send_event flows must use ctx.collect_events, not list[E]."""

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


# ---------------------------------------------------------------------------
# send_event from INSIDE a fan-out branch remains outside batch membership.
# ---------------------------------------------------------------------------


async def test_send_event_extra_member_inside_batch_is_not_joined() -> None:
    """A sent event from a branch does not become a member of the return batch."""

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ctx: Context, ev: Task) -> Done:
            if ev.n == 2:
                ctx.send_event(Done(n=99))
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(WF(timeout=6), timeout=5)
    assert result == [0, 1, 2], result


async def test_send_event_only_inside_batch_collect_rejected() -> None:
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
# Resume mid-batch with a member retrying. Combines the persist/resume path with
# an in-flight retry: the snapshot must preserve both the open batch's live count
# and the retrying member's batch_stack, and the resumed run must not double- or
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
            await _RETRY_RESUME_GATE.wait()  # hold the batch open at snapshot time
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    return FanOut


async def test_resume_mid_batch_with_retry_in_flight_completes() -> None:
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
    await asyncio.sleep(0.2)  # let the batch settle with the retry pending

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
