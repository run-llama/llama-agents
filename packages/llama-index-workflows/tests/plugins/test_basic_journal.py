# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.plugins.basic import (
    BasicRuntime,
    JournalRecord,
    assert_is_basic,
)
from workflows.runtime.verbose import VerboseDecorator


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


def test_assert_is_basic_accepts_basic_runtime(runtime: BasicRuntime) -> None:
    assert assert_is_basic(runtime) is runtime


def test_assert_is_basic_rejects_decorated_runtime(runtime: BasicRuntime) -> None:
    with pytest.raises(TypeError):
        assert_is_basic(VerboseDecorator(runtime))
