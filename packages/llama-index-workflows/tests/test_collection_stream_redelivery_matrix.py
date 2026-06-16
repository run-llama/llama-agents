# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Re-delivery matrix for collection-stream work items.

A work item's record (event, scope_path, collection release payload, retry
lineage) must travel WHOLE through every re-delivery path. This file crosses
the three re-delivery kinds — retry, @catch_error routing, and wait_for_event
resume — with the three places a work item can live: a top-level collect step,
a nested (inner) collect step, and a 1:1 worker inside a fan-out branch. It
then covers serialize/resume round-trips taken mid-flight and two interaction
cells (Take(n) + retry, num_workers>1 overlapping releases + retry).

Every cell asserts two things: the run completes with the correct batch, and
each delivery of the target work item observed the *same* payload.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

import pytest
from workflows import Context, Workflow, catch_error, step
from workflows.collect import Collect, Take
from workflows.events import (
    CollectionReleaseEvent,
    Event,
    StartEvent,
    StepFailedEvent,
    StopEvent,
)
from workflows.handler import WorkflowHandler
from workflows.retry_policy import retry_policy, stop_after_attempt, wait_fixed


class Task(Event):
    n: int


class Done(Event):
    n: int


class InnerTask(Event):
    outer: int
    inner: int


class InnerDone(Event):
    outer: int
    inner: int


class InnerSummary(Event):
    outer: int
    total: int


class Approval(Event):
    token: str


class ApprovalRequired(Event):
    """Waiter marker written to the event stream by a parked step."""

    token: str


_FAST_RETRY = retry_policy(wait=wait_fixed(0.01), stop=stop_after_attempt(3))

_KINDS = ["retry", "catch_error", "waiter"]


async def _run(wf: Workflow, timeout: float = 8.0) -> object:
    """Run to completion, answering each ApprovalRequired marker exactly once.

    Draining the published-event stream lets the in-memory runtime's pull task
    finish cleanly; for waiter cells the drain doubles as the external approver.
    """
    handler = wf.run()

    async def drive() -> object:
        answered: set[str] = set()
        async for ev in handler.stream_events():
            if isinstance(ev, ApprovalRequired) and ev.token not in answered:
                answered.add(ev.token)
                handler.ctx.send_event(Approval(token=ev.token))
        return await handler

    return await asyncio.wait_for(drive(), timeout=timeout)


async def _drain(handler: WorkflowHandler) -> None:
    async for _ in handler.stream_events():
        pass


async def _snapshot_cancel(handler: WorkflowHandler) -> dict:
    """Snapshot a live run, then cancel it (ignoring the cancellation error)."""
    snapshot = handler.ctx.to_dict()
    await handler.cancel_run()
    try:
        await asyncio.wait_for(handler, timeout=2)
    except BaseException:
        pass
    return snapshot


# ---------------------------------------------------------------------------
# Matrix: re-delivery kind x scope. Each factory builds a workflow whose target
# step is re-delivered once via `kind`, recording the payload seen by every
# delivery so identity across deliveries is asserted, not just completion.
# ---------------------------------------------------------------------------


class _TopLevelBase(Workflow):
    """Shared producers: fan out 4 members, 1:1 work, join defined per kind."""

    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(4)]

    @step
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)


def _make_top_level_join_wf(kind: str, batches: list[list[int]]) -> type[Workflow]:
    """The re-delivered step is the top-level collect step itself."""
    if kind == "retry":

        class RetryWF(_TopLevelBase):
            @step(retry_policy=_FAST_RETRY)
            async def join(self, events: list[Done]) -> StopEvent:
                batch = sorted(e.n for e in events)
                batches.append(batch)
                if len(batches) == 1:
                    raise RuntimeError("transient join failure")
                return StopEvent(result=batch)

        return RetryWF

    if kind == "catch_error":

        class CatchWF(_TopLevelBase):
            @step
            async def join(self, events: list[Done]) -> StopEvent:
                batches.append(sorted(e.n for e in events))
                raise RuntimeError("terminal join failure")

            @catch_error(for_steps=["join"])
            async def recover(self, ev: StepFailedEvent) -> StopEvent:
                release = ev.input_event
                assert isinstance(release, CollectionReleaseEvent)
                batch = sorted(e.n for e in release.events)
                batches.append(batch)
                return StopEvent(result=batch)

        return CatchWF

    class WaiterWF(_TopLevelBase):
        @step
        async def join(self, ctx: Context, events: list[Done]) -> StopEvent:
            batch = sorted(e.n for e in events)
            batches.append(batch)
            await ctx.wait_for_event(
                Approval,
                waiter_event=ApprovalRequired(token="top-join"),
                waiter_id="top-join",
                timeout=3,
            )
            return StopEvent(result=batch)

    return WaiterWF


@pytest.mark.parametrize("kind", _KINDS)
async def test_top_level_join_redelivery_completes_with_full_batch(
    kind: str,
) -> None:
    batches: list[list[int]] = []
    result = await _run(_make_top_level_join_wf(kind, batches)(timeout=8))
    assert result == [0, 1, 2, 3]
    # Both deliveries of the collect invocation saw the identical batch.
    assert batches == [[0, 1, 2, 3], [0, 1, 2, 3]], batches


class _NestedBase(Workflow):
    """Outer fan-out of 3, inner fan-out of 2; per_inner defined per kind."""

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
    async def per_outer(self, events: list[InnerSummary]) -> StopEvent:
        return StopEvent(result=sorted((s.outer, s.total) for s in events))


def _make_nested_join_wf(kind: str, batches: list[list[int]]) -> type[Workflow]:
    """The re-delivered step is the inner collect step, for outer==1 only.

    `batches` records the inner indices seen by each delivery of that one
    invocation; the other outers run undisturbed.
    """
    if kind == "retry":

        class RetryWF(_NestedBase):
            @step(retry_policy=_FAST_RETRY)
            async def per_inner(self, events: list[InnerDone]) -> InnerSummary:
                outer = events[0].outer
                if outer == 1:
                    batches.append(sorted(e.inner for e in events))
                    if len(batches) == 1:
                        raise RuntimeError("transient inner join failure")
                return InnerSummary(outer=outer, total=len(events))

        return RetryWF

    if kind == "catch_error":

        class CatchWF(_NestedBase):
            @step
            async def per_inner(self, events: list[InnerDone]) -> InnerSummary:
                outer = events[0].outer
                if outer == 1:
                    batches.append(sorted(e.inner for e in events))
                    raise RuntimeError("terminal inner join failure")
                return InnerSummary(outer=outer, total=len(events))

            @catch_error(for_steps=["per_inner"])
            async def recover(self, ev: StepFailedEvent) -> InnerSummary:
                release = ev.input_event
                assert isinstance(release, CollectionReleaseEvent)
                members = release.events
                batches.append(sorted(e.inner for e in members))
                # The recovered summary must rejoin the OUTER stream so the
                # outer collect still sees all three summaries.
                return InnerSummary(outer=members[0].outer, total=len(members))

        return CatchWF

    class WaiterWF(_NestedBase):
        @step
        async def per_inner(
            self, ctx: Context, events: list[InnerDone]
        ) -> InnerSummary:
            outer = events[0].outer
            if outer == 1:
                batches.append(sorted(e.inner for e in events))
                await ctx.wait_for_event(
                    Approval,
                    waiter_event=ApprovalRequired(token="inner-1"),
                    waiter_id="inner-1",
                    timeout=3,
                )
            return InnerSummary(outer=outer, total=len(events))

    return WaiterWF


@pytest.mark.parametrize("kind", _KINDS)
async def test_nested_join_redelivery_completes_with_full_batch(kind: str) -> None:
    batches: list[list[int]] = []
    result = await _run(_make_nested_join_wf(kind, batches)(timeout=8))
    assert result == [(0, 2), (1, 2), (2, 2)]
    assert batches == [[0, 1], [0, 1]], batches


class _BranchBase(Workflow):
    """Fan-out of 3 with a plain join; the worker is defined per kind."""

    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(3)]

    @step
    async def join(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=sorted(e.n for e in events))


def _make_branch_member_wf(kind: str, deliveries: list[int]) -> type[Workflow]:
    """The re-delivered step is the 1:1 worker for stream member n==1.

    Its output must keep its scope so it still joins the stream.
    """
    if kind == "retry":

        class RetryWF(_BranchBase):
            @step(retry_policy=_FAST_RETRY)
            async def work(self, ev: Task) -> Done:
                if ev.n == 1:
                    deliveries.append(ev.n)
                    if len(deliveries) == 1:
                        raise RuntimeError("transient member failure")
                return Done(n=ev.n)

        return RetryWF

    if kind == "catch_error":

        class CatchWF(_BranchBase):
            @step
            async def work(self, ev: Task) -> Done:
                if ev.n == 1:
                    deliveries.append(ev.n)
                    raise RuntimeError("terminal member failure")
                return Done(n=ev.n)

            @catch_error(for_steps=["work"])
            async def recover(self, ev: StepFailedEvent) -> Done:
                failed = ev.input_event
                assert isinstance(failed, Task)
                deliveries.append(failed.n)
                return Done(n=failed.n)

        return CatchWF

    class WaiterWF(_BranchBase):
        @step
        async def work(self, ctx: Context, ev: Task) -> Done:
            if ev.n == 1:
                deliveries.append(ev.n)
                await ctx.wait_for_event(
                    Approval,
                    waiter_event=ApprovalRequired(token="member-1"),
                    waiter_id="member-1",
                    timeout=3,
                )
            return Done(n=ev.n)

    return WaiterWF


@pytest.mark.parametrize("kind", _KINDS)
async def test_branch_member_redelivery_output_still_joins(kind: str) -> None:
    deliveries: list[int] = []
    result = await _run(_make_branch_member_wf(kind, deliveries)(timeout=8))
    assert result == [0, 1, 2]
    # Member 1 was delivered twice (original + re-delivery), same event.
    assert deliveries == [1, 1], deliveries


# ---------------------------------------------------------------------------
# Serialize/resume round-trips mid-flight. Module-level gates/state survive the
# first run's cancellation so the resumed run (same module event loop) can be
# released and compared against the original deliveries.
# ---------------------------------------------------------------------------


def _waiter_branch_workflow() -> type[Workflow]:
    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step(num_workers=8)
        async def work(self, ctx: Context, ev: Task) -> Done:
            if ev.n == 2:
                await ctx.wait_for_event(
                    Approval,
                    waiter_event=ApprovalRequired(token="branch-2"),
                    waiter_id="branch-2",
                    timeout=8,
                )
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    return WF


async def test_resume_with_unresolved_waiter_in_branch() -> None:
    """Snapshot while one branch is parked on wait_for_event, resume, approve."""
    wf = _waiter_branch_workflow()(timeout=20)
    handler = wf.run()
    async for ev in handler.stream_events():
        if isinstance(ev, ApprovalRequired):
            break
    await asyncio.sleep(0.2)  # let the other members settle into the join buffer
    snapshot = await _snapshot_cancel(handler)

    wf2 = _waiter_branch_workflow()(timeout=20)
    restored = Context.from_dict(wf2, snapshot)
    handler2 = wf2.run(ctx=restored)
    drain = asyncio.create_task(_drain(handler2))
    result_task: asyncio.Future[object] = asyncio.ensure_future(handler2)
    # The waiter was restored from the snapshot; nudge it until it resolves.
    for _ in range(50):
        if result_task.done():
            break
        handler2.ctx.send_event(Approval(token="branch-2"))
        await asyncio.sleep(0.1)
    result = await asyncio.wait_for(result_task, timeout=5)
    await asyncio.wait_for(drain, timeout=5)
    assert result == [0, 1, 2, 3], result


_MID_COLLECT_GATE = asyncio.Event()
_MID_COLLECT_BATCHES: list[list[int]] = []


def _gated_collect_workflow() -> type[Workflow]:
    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            _MID_COLLECT_BATCHES.append(sorted(e.n for e in events))
            await _MID_COLLECT_GATE.wait()  # hold the invocation open
            return StopEvent(result=sorted(e.n for e in events))

    return WF


async def test_resume_mid_collect_invocation_preserves_payload() -> None:
    """Snapshot while the collect invocation is in progress; resume re-fires it
    with the identical batch (the release payload travels in the snapshot)."""
    _MID_COLLECT_GATE.clear()
    _MID_COLLECT_BATCHES.clear()

    wf = _gated_collect_workflow()(timeout=20)
    handler = wf.run()
    for _ in range(300):
        if _MID_COLLECT_BATCHES:
            break
        await asyncio.sleep(0.02)
    assert _MID_COLLECT_BATCHES, "collect invocation never started"
    snapshot = await _snapshot_cancel(handler)

    _MID_COLLECT_GATE.set()
    wf2 = _gated_collect_workflow()(timeout=20)
    restored = Context.from_dict(wf2, snapshot)
    result = await asyncio.wait_for(wf2.run(ctx=restored), timeout=8)
    assert result == [0, 1, 2, 3], result
    assert _MID_COLLECT_BATCHES == [[0, 1, 2, 3], [0, 1, 2, 3]], _MID_COLLECT_BATCHES


_RETRY_COLLECT_GATE = asyncio.Event()
_RETRY_COLLECT_BATCHES: list[list[int]] = []


def _retrying_collect_workflow() -> type[Workflow]:
    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step(retry_policy=_FAST_RETRY)
        async def join(self, events: list[Done]) -> StopEvent:
            _RETRY_COLLECT_BATCHES.append(sorted(e.n for e in events))
            if len(_RETRY_COLLECT_BATCHES) == 1:
                raise RuntimeError("transient join failure")
            await _RETRY_COLLECT_GATE.wait()  # hold the retry attempt open
            return StopEvent(result=sorted(e.n for e in events))

    return WF


async def test_resume_mid_collect_retry_preserves_payload_and_lineage() -> None:
    """Snapshot while a collect invocation's RETRY attempt is in flight.

    The retry re-runs the same work item; snapshotting mid-retry and resuming
    must re-deliver the identical batch.

    Note: a snapshot taken inside the retry *delay window* (between the failed
    attempt and the rescheduled one) is NOT capturable today — the delayed
    re-delivery lives only in the runner's in-memory scheduled-wakeups heap, so
    the work item is absent from the serialized state and the resumed run
    hangs. That gap is general to retry delays (not collect-specific), so this
    test pins the in-flight-retry variant instead.
    """
    _RETRY_COLLECT_GATE.clear()
    _RETRY_COLLECT_BATCHES.clear()

    wf = _retrying_collect_workflow()(timeout=20)
    handler = wf.run()
    # Wait until the retry attempt (second delivery) has entered the step body.
    for _ in range(300):
        if len(_RETRY_COLLECT_BATCHES) >= 2:
            break
        await asyncio.sleep(0.02)
    assert len(_RETRY_COLLECT_BATCHES) >= 2, "retry attempt never started"
    snapshot = await _snapshot_cancel(handler)

    _RETRY_COLLECT_GATE.set()
    wf2 = _retrying_collect_workflow()(timeout=20)
    restored = Context.from_dict(wf2, snapshot)
    result = await asyncio.wait_for(wf2.run(ctx=restored), timeout=8)
    assert result == [0, 1, 2, 3], result
    # Failed attempt, gated retry attempt, resumed attempt: identical batches.
    assert _RETRY_COLLECT_BATCHES == [[0, 1, 2, 3]] * 3, _RETRY_COLLECT_BATCHES


# ---------------------------------------------------------------------------
# Interaction cells.
# ---------------------------------------------------------------------------


async def test_take_two_collect_retry_redelivers_same_two_members() -> None:
    """Take(2) releases the first two members; a retry re-fires with the SAME
    two, not a re-evaluated window over later arrivals."""
    batches: list[list[int]] = []

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step(num_workers=1)  # serialize members so "first two" is [0, 1]
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step(retry_policy=_FAST_RETRY)
        async def first_two(
            self, events: Annotated[list[Done], Collect(Take(2))]
        ) -> StopEvent:
            batch = sorted(e.n for e in events)
            batches.append(batch)
            if len(batches) == 1:
                raise RuntimeError("transient take-two failure")
            return StopEvent(result=batch)

    result = await _run(WF(timeout=8))
    assert result == [0, 1]
    assert batches == [[0, 1], [0, 1]], batches


class Seed(Event):
    gid: int


class Leaf(Event):
    gid: int
    k: int


class Collected(Event):
    gid: int
    n: int


async def test_overlapping_releases_num_workers_2_with_retry() -> None:
    """Two independent streams release into a num_workers=2 collect step whose
    invocations overlap; one invocation fails once. Both batches arrive intact
    and the retried invocation re-fires with its own original batch."""
    entered = 0
    proceed = asyncio.Event()
    failed: dict[int, bool] = {}
    batches: list[tuple[int, list[int]]] = []
    finished: list[tuple[int, int]] = []

    class WF(Workflow):
        @step
        async def seed(self, ev: StartEvent) -> list[Seed]:
            return [Seed(gid=0), Seed(gid=1)]

        @step(num_workers=2)
        async def fan_inner(self, ev: Seed) -> list[Leaf]:
            return [Leaf(gid=ev.gid, k=k) for k in range(3)]

        @step(num_workers=2, retry_policy=_FAST_RETRY)
        async def collect(self, stream: list[Leaf]) -> Collected:
            nonlocal entered
            gid = stream[0].gid
            batches.append((gid, sorted(b.k for b in stream)))
            entered += 1
            if entered >= 2:
                proceed.set()
            # One-shot rendezvous: the first two invocations overlap in time.
            await asyncio.wait_for(proceed.wait(), timeout=5)
            if gid == 0 and not failed.get(0):
                failed[0] = True
                raise RuntimeError("transient on gid 0")
            return Collected(gid=gid, n=len(stream))

        @step
        async def finish(self, ev: Collected) -> StopEvent | None:
            finished.append((ev.gid, ev.n))
            if len(finished) < 2:
                return None
            return StopEvent(result=sorted(finished))

    result = await _run(WF(timeout=10))
    assert result == [(0, 3), (1, 3)]
    # gid 0 delivered twice (original + retry) with the same batch; gid 1 once.
    assert sorted(batches) == [
        (0, [0, 1, 2]),
        (0, [0, 1, 2]),
        (1, [0, 1, 2]),
    ], batches
