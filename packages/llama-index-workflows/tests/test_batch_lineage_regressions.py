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


@pytest.mark.xfail(
    reason="batch_pending decremented per accepting collect step; batch truncates",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="event routed to a 1:1 step and a join double-debits the batch; closes early",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="retry CommandQueueEvent omits batch_stack; retried member never closes the batch",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="catch_error recovery re-enters at top level with no batch_stack; join hangs",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="_fire_batch_collect worker_id-0 fallback aliases overlapping batches; one is lost",
    strict=True,
)
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

    class WF(Workflow):
        @step
        async def seed(self, ev: StartEvent) -> list[Seed]:
            return [Seed(gid=0), Seed(gid=1)]

        @step(num_workers=2)
        async def fan_inner(self, ev: Seed) -> list[Leaf]:
            return [Leaf(gid=ev.gid, k=k) for k in range(3)]

        @step(num_workers=1)
        async def collect(self, batch: list[Leaf]) -> Collected:
            await asyncio.sleep(0.2)
            gid = next(iter({b.gid for b in batch}))
            return Collected(gid=gid, n=len(batch))

        @step
        async def finish(self, ev: Collected) -> StopEvent | None:
            collected.append((ev.gid, ev.n))
            if len(collected) < 2:
                return None
            return StopEvent(result=sorted(collected))

    await _run(WF(timeout=10), timeout=8)
    assert sorted(collected) == [(0, 3), (1, 3)], collected


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


@pytest.mark.xfail(
    reason="empty-batch firing gate halts at the fan-out boundary; outer join never fires",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="resume re-mints batch ids under run_id=None and drops in_progress lineage; hangs",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="join(a: A, b: A) accepted at validation then deadlocks; should reject at decoration",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="Optional[list[E]] collect param rejected with a misleading generic error",
    strict=True,
)
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
# Heterogeneous one-of-each join downstream of a fan-out: the legacy collect
# buffer is lineage-blind, so pairs mis-match across batches and events strand.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="one-of-each join downstream of fan-out is lineage-blind; mis-pairs and strands events",
    strict=True,
)
async def test_heterogeneous_join_downstream_fan_out_pairs_correctly() -> None:
    class A(Event):
        v: int

    class B(Event):
        v: int

    class Paired(Event):
        a: int
        b: int

    joins: list[tuple[int, int]] = []
    n = 3

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(n)]

        @step
        async def make_a(self, ev: Task) -> A:
            return A(v=ev.n)

        @step
        async def make_b(self, ev: Task) -> B:
            await asyncio.sleep(0.02 * (n - ev.n))  # reverse B arrival order
            return B(v=ev.n)

        @step
        async def join(self, a: A, b: B) -> Paired:
            joins.append((a.v, b.v))
            return Paired(a=a.v, b=b.v)

        @step
        async def collect(self, ctx: Context, ev: Paired) -> StopEvent | None:
            done = ctx.collect_events(ev, [Paired] * n)
            return None if done is None else StopEvent(result="done")

    await _run(WF(timeout=5), timeout=8)
    assert sorted(joins) == [(i, i) for i in range(n)], joins


# ---------------------------------------------------------------------------
# Old/new API boundary: events from ctx.send_event carry no batch_stack, so a
# typed list[E] All() join keyed on batch close never fires (hangs).
# ---------------------------------------------------------------------------


class Item(Event):
    idx: int


async def test_send_event_into_take_collect_fires() -> None:
    """Cleared behavior: send_event into a Take(n) join fires on the n-th arrival."""

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

    result = await _run(WF(disable_validation=True, timeout=10))
    assert result == [0, 1, 2], result


@pytest.mark.xfail(
    reason="send_event events land in the never-closed '' batch bucket; All() join hangs",
    strict=True,
)
async def test_send_event_into_all_collect_fires() -> None:
    class WF(Workflow):
        @step
        async def start(self, ctx: Context, ev: StartEvent) -> StopEvent | None:
            for i in range(3):
                ctx.send_event(Item(idx=i))
            return None

        @step
        async def collect(self, events: list[Item]) -> StopEvent:
            return StopEvent(result=sorted(e.idx for e in events))

    result = await _run(WF(disable_validation=True, timeout=10), timeout=5)
    assert result == [0, 1, 2], result
