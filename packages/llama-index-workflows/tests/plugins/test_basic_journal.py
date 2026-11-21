# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.plugins.basic import BasicRuntime, JournalRecord


class GatherEvent(Event):
    n: int


class MidEvent(Event):
    total: int


class JournalWorkflow(Workflow):
    """Collect two events and pause before finishing."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.gate = asyncio.Event()
        self.slow_started = asyncio.Event()

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> GatherEvent:
        await ctx.store.set("started", True)
        ctx.send_event(GatherEvent(n=1))
        return GatherEvent(n=2)

    @step
    async def gather(self, ctx: Context, ev: GatherEvent) -> MidEvent | None:
        events = ctx.collect_events(ev, [GatherEvent, GatherEvent])
        if events is None:
            return None
        return MidEvent(total=sum(e.n for e in events))  # type: ignore[attr-defined]

    @step
    async def slow(self, ev: MidEvent) -> StopEvent:
        self.slow_started.set()
        await self.gate.wait()
        return StopEvent(result=ev.total)


@pytest.fixture
def runtime() -> BasicRuntime:
    return BasicRuntime()


async def _collect(runtime: BasicRuntime, run_id: str) -> list[JournalRecord]:
    return [record async for record in runtime.journal(run_id)]


async def test_journal_started_after_run_sees_every_record(
    runtime: BasicRuntime,
) -> None:
    wf = JournalWorkflow(runtime=runtime)
    wf.gate.set()
    handler = wf.run()
    await asyncio.sleep(0)
    reader = asyncio.create_task(_collect(runtime, handler.run_id))
    assert await handler == 3
    records = await asyncio.wait_for(reader, timeout=5)

    assert [r.seq for r in records] == list(range(len(records)))
    assert handler.ctx is not None
    assert handler.ctx.to_dict()["journal_seq"] == len(records)


async def test_journal_after_completion_ends(runtime: BasicRuntime) -> None:
    wf = JournalWorkflow(runtime=runtime)
    wf.gate.set()
    handler = wf.run()
    await handler
    records = await asyncio.wait_for(_collect(runtime, handler.run_id), timeout=5)
    assert records[0].seq == 0
    assert len(records) > 0


async def _record_resumed_run(
    runtime: BasicRuntime,
) -> tuple[JournalWorkflow, list[JournalRecord]]:
    """Snapshot during the slow step and record the resumed run."""
    wf = JournalWorkflow(runtime=runtime)
    handler = wf.run()
    reader = asyncio.create_task(_collect(runtime, handler.run_id))
    await asyncio.wait_for(wf.slow_started.wait(), timeout=5)
    assert handler.ctx is not None
    snapshot = handler.ctx.to_dict()
    assert any(w["in_progress"] for w in snapshot["workers"].values())
    await handler.cancel_run()
    first = await asyncio.wait_for(reader, timeout=5)

    wf.gate.set()
    resumed = wf.run(ctx=Context.from_dict(wf, snapshot))
    reader = asyncio.create_task(_collect(runtime, resumed.run_id))
    assert await resumed == 3
    second = await asyncio.wait_for(reader, timeout=5)

    # Resume from the snapshot position. Discard later ticks from the old session.
    assert second[0].seq == snapshot["journal_seq"]
    return wf, [r for r in first if r.seq < snapshot["journal_seq"]] + second


async def test_restore_from_any_cut_point_matches_full_fold(
    runtime: BasicRuntime,
) -> None:
    wf, records = await _record_resumed_run(runtime)
    assert [r.seq for r in records] == list(range(len(records)))

    folds = [
        runtime.restore(wf, None, records[:m]).to_dict()
        for m in range(len(records) + 1)
    ]
    assert [f["journal_seq"] for f in folds] == list(range(len(records) + 1))
    # Compare every prefix because completion clears evidence of active work.
    for k, head in enumerate(folds):
        for m in range(k, len(records) + 1):
            resumed = runtime.restore(wf, head, records[k:m]).to_dict()
            assert resumed == folds[m], (k, m)


async def test_restore_skips_covered_records_and_rejects_gaps(
    runtime: BasicRuntime,
) -> None:
    wf, records = await _record_resumed_run(runtime)
    head = runtime.restore(wf, None, records[:4]).to_dict()

    overlapping = runtime.restore(wf, head, records).to_dict()
    assert overlapping == runtime.restore(wf, None, records).to_dict()

    with pytest.raises(ValueError, match="Journal gap"):
        runtime.restore(wf, head, records[5:])


async def test_restored_context_runs_to_completion(runtime: BasicRuntime) -> None:
    wf, records = await _record_resumed_run(runtime)
    mid = runtime.restore(wf, None, records[: len(records) // 2])
    assert await wf.run(ctx=mid) == 3


def _settled(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Select finished state that can be compared across resumes.

    Exclude store state because the journal does not record store writes.
    Exclude buffer counters because reruns can append and clear again.
    """
    return {
        "is_running": snapshot["is_running"],
        "workers": {
            name: {k: v for k, v in worker.items() if k != "collect_generations"}
            for name, worker in snapshot["workers"].items()
        },
        "deliveries": snapshot["deliveries"],
    }


def _record_type(records: list[JournalRecord]) -> str:
    if not records:
        return "<empty>"
    return json.loads(records[-1].data)["value"]["type"]


async def test_every_journal_cut_runs_to_the_uninterrupted_result(
    runtime: BasicRuntime,
) -> None:
    wf, records = await _record_resumed_run(runtime)
    # The open gate lets the slow step finish on each rerun.
    baseline = wf.run()
    expected_result = await asyncio.wait_for(baseline, timeout=5)
    assert baseline.ctx is not None
    expected = _settled(baseline.ctx.to_dict())

    failures: list[str] = []
    for k in range(len(records) + 1):
        where = f"k={k} after {_record_type(records[:k])}"
        handler = wf.run(ctx=runtime.restore(wf, None, records[:k]))
        try:
            result = await asyncio.wait_for(handler, timeout=2)
        except asyncio.TimeoutError:
            failures.append(f"{where}: stalled")
            continue
        assert handler.ctx is not None
        if result != expected_result:
            failures.append(f"{where}: result {result!r}")
        elif _settled(handler.ctx.to_dict()) != expected:
            failures.append(f"{where}: final context differs")
    assert failures == [], f"{len(records) + 1} cuts restored, failures: {failures}"


async def test_to_dict_without_state_leaves_state_empty(
    runtime: BasicRuntime,
) -> None:
    wf = JournalWorkflow(runtime=runtime)
    handler = wf.run()
    await asyncio.wait_for(wf.slow_started.wait(), timeout=5)
    assert handler.ctx is not None
    snapshot = handler.ctx.to_dict(include_state=False)
    assert snapshot["state"] == {}
    assert handler.ctx.to_dict()["state"] != {}
    await handler.cancel_run()

    wf.gate.set()
    ctx = Context.from_dict(wf, snapshot)
    assert await wf.run(ctx=ctx) == 3
    assert await ctx.store.get("started", default=None) is None


class AskA(Event):
    i: int


class AskB(Event):
    i: int


class Answer(Event):
    waiter: str


class BranchDone(Event):
    branch: str


class WaiterRequirementsWorkflow(Workflow):
    """Two parallel branches that wait on keyed Answer events twice."""

    def __init__(self, *, pre_wait_yield: bool = False, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pre_wait_yield = pre_wait_yield

    @step
    async def begin(self, ctx: Context, ev: StartEvent) -> AskA | AskB | None:
        ctx.send_event(AskA(i=0))
        ctx.send_event(AskB(i=0))
        return None

    @step
    async def ask_a(self, ctx: Context, ev: AskA) -> AskA | BranchDone:
        if self.pre_wait_yield:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        wid = f"a{ev.i}"
        await ctx.wait_for_event(
            Answer,
            waiter_id=wid,
            waiter_event=ev,
            requirements={"waiter": wid},
        )
        if ev.i == 0:
            return AskA(i=1)
        return BranchDone(branch="a")

    @step
    async def ask_b(self, ctx: Context, ev: AskB) -> AskB | BranchDone:
        if self.pre_wait_yield:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        wid = f"b{ev.i}"
        await ctx.wait_for_event(
            Answer,
            waiter_id=wid,
            waiter_event=ev,
            requirements={"waiter": wid},
        )
        if ev.i == 0:
            return AskB(i=1)
        return BranchDone(branch="b")

    @step
    async def finish(self, ctx: Context, ev: BranchDone) -> StopEvent | None:
        done = ctx.collect_events(ev, [BranchDone, BranchDone])
        if done is None:
            return None
        return StopEvent(result="done")


def _added_waiters(record: JournalRecord) -> list[str]:
    payload = json.loads(record.data)["value"]
    if payload.get("type") != "step_result":
        return []
    return [
        item["waiter_id"]
        for item in payload.get("result", [])
        if item.get("type") == "add_waiter"
    ]


async def test_journal_reads_through_a_verbose_wrapper(runtime: BasicRuntime) -> None:
    """verbose=True wraps wf.runtime, but the held BasicRuntime still owns the run."""
    wf = JournalWorkflow(runtime=runtime, verbose=True)
    wf.gate.set()
    handler = wf.run()
    assert await handler == 3
    records = await asyncio.wait_for(_collect(runtime, handler.run_id), timeout=5)
    assert records[0].seq == 0
    assert runtime.restore(wf, None, records).to_dict()["journal_seq"] == len(records)


async def test_multi_restore_with_requirement_waiters(runtime: BasicRuntime) -> None:
    wf = WaiterRequirementsWorkflow(runtime=runtime)
    all_records: list[JournalRecord] = []

    # Round 0: start run, wait for both initial waiters, then cancel and restore.
    h0 = wf.run()
    seen0: set[str] = set()
    async for rec in runtime.journal(h0.run_id):
        seen0.update(_added_waiters(rec))
        if {"a0", "b0"} <= seen0:
            break
    await h0.cancel_run()
    r0 = await asyncio.wait_for(_collect(runtime, h0.run_id), timeout=5)
    all_records.extend(r0)

    # Round 1: restore from round-0 journal, resolve a0 and b0 in sequence,
    # wait for follow-up waiters a1 and b1, then cancel and restore again.
    h1 = wf.run(ctx=runtime.restore(wf, None, all_records))
    assert h1.ctx is not None
    seen1: set[str] = set()
    sent_a0 = False
    sent_b0 = False
    async for rec in runtime.journal(h1.run_id):
        seen1.update(_added_waiters(rec))
        if not sent_a0 and {"a0", "b0"} <= seen1:
            sent_a0 = True
            h1.ctx.send_event(Answer(waiter="a0"))
        elif sent_a0 and not sent_b0 and "a1" in seen1:
            sent_b0 = True
            h1.ctx.send_event(Answer(waiter="b0"))
        elif sent_b0 and "b1" in seen1:
            break
    await h1.cancel_run()
    r1 = await asyncio.wait_for(_collect(runtime, h1.run_id), timeout=5)
    all_records.extend(r1)

    # Round 2: restore from combined multi-session journal (failed prior to fix)
    # and resolve a1 and b1 to completion.
    h2 = wf.run(ctx=runtime.restore(wf, None, all_records))
    assert h2.ctx is not None
    seen2: set[str] = set()
    sent_final = False
    async for rec in runtime.journal(h2.run_id):
        seen2.update(_added_waiters(rec))
        if not sent_final and {"a1", "b1"} <= seen2:
            sent_final = True
            h2.ctx.send_event(Answer(waiter="a1"))
            h2.ctx.send_event(Answer(waiter="b1"))
    assert await asyncio.wait_for(h2, timeout=5) == "done"
    r2 = await asyncio.wait_for(_collect(runtime, h2.run_id), timeout=5)
    all_records.extend(r2)

    assert [r.seq for r in all_records] == list(range(len(all_records)))
    folds = [
        runtime.restore(wf, None, all_records[:m]).to_dict()
        for m in range(len(all_records) + 1)
    ]
    for k, head in enumerate(folds):
        assert runtime.restore(wf, head, all_records[k:]).to_dict() == folds[-1], k
    for boundary in (len(r0), len(r0) + len(r1)):
        for m in range(boundary, len(all_records) + 1):
            resumed = runtime.restore(
                wf, folds[boundary], all_records[boundary:m]
            ).to_dict()
            assert resumed == folds[m], (boundary, m)


async def test_single_run_requirement_waiters_restore_across_mismatched_events(
    runtime: BasicRuntime,
) -> None:
    wf = WaiterRequirementsWorkflow(runtime=runtime)
    handler = wf.run()
    assert handler.ctx is not None

    seen: set[str] = set()
    step_stage = 0
    async for rec in runtime.journal(handler.run_id):
        seen.update(_added_waiters(rec))
        if step_stage == 0 and {"a0", "b0"} <= seen:
            step_stage = 1
            handler.ctx.send_event(Answer(waiter="nope"))
            handler.ctx.send_event(Answer(waiter="a0"))
        elif step_stage == 1 and "a1" in seen:
            step_stage = 2
            # b0 resolves while a1 is waiting with its requirements stripped in journal.
            handler.ctx.send_event(Answer(waiter="b0"))
        elif step_stage == 2 and "b1" in seen:
            step_stage = 3
            handler.ctx.send_event(Answer(waiter="a1"))
            handler.ctx.send_event(Answer(waiter="b1"))

    assert await asyncio.wait_for(handler, timeout=5) == "done"
    records = await asyncio.wait_for(_collect(runtime, handler.run_id), timeout=5)

    for k in range(len(records) + 1):
        restored = runtime.restore(wf, None, records[:k])
        assert restored.to_dict()["journal_seq"] == k


async def test_snapshot_resume_defers_external_events_until_waiter_rehydration(
    runtime: BasicRuntime,
) -> None:
    wf = WaiterRequirementsWorkflow(runtime=runtime, pre_wait_yield=True)
    h0 = wf.run()
    seen0: set[str] = set()
    async for rec in runtime.journal(h0.run_id):
        seen0.update(_added_waiters(rec))
        if {"a0", "b0"} <= seen0:
            break
    assert h0.ctx is not None
    snapshot = h0.ctx.to_dict()
    await h0.cancel_run()

    ctx = Context.from_dict(wf, snapshot)
    h1 = wf.run(ctx=ctx)
    # Send Answer(a0) immediately before ask_a/ask_b finish re-running to their waits.
    ctx.send_event(Answer(waiter="a0"))

    seen1: set[str] = set()
    sent_b0 = False
    sent_final = False
    async for rec in runtime.journal(h1.run_id):
        seen1.update(_added_waiters(rec))
        # Only a1 should advance from Answer(a0); b0 must wait for Answer(b0).
        assert "b1" not in seen1 or sent_b0
        if not sent_b0 and "a1" in seen1:
            sent_b0 = True
            ctx.send_event(Answer(waiter="b0"))
        elif sent_b0 and not sent_final and "b1" in seen1:
            sent_final = True
            ctx.send_event(Answer(waiter="a1"))
            ctx.send_event(Answer(waiter="b1"))

    assert await asyncio.wait_for(h1, timeout=5) == "done"
