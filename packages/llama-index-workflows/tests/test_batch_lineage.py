# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Batch-lineage fan-out / fan-in (Phase L2).

Covers the terse ``join(events: list[Done])`` form for both static ``list[E]``
and ``AsyncIterator[E]`` producers, multi-level fan-out, replay equality of
batch ids and grouping, empty batches, branch death, and BatchAborted.
"""

from __future__ import annotations

from typing import AsyncIterator

import pytest
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.retry_policy import ConstantDelayRetryPolicy
from workflows.runtime import control_loop as _cl
from workflows.runtime.control_loop import (
    rebuild_state_from_ticks_stream,
    replay_ticks_stream,
)
from workflows.runtime.types.commands import (
    CommandCloseBatch,
    CommandQueueEvent,
    CommandRunWorker,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.results import RetryAttempt
from workflows.runtime.types.step_function import as_step_worker_functions
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickBatchClosed,
    TickStepResult,
    WorkflowTick,
)

# Each test gets its own event loop. The session default is a module-scoped
# loop; with it, a finished run's lingering pull task can survive into the next
# test and intermittently hang it under xdist.
pytestmark = pytest.mark.asyncio(loop_scope="function")


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


async def _stream(ticks: list[WorkflowTick]) -> AsyncIterator[WorkflowTick]:
    for t in ticks:
        yield t


async def test_static_list_producer_join_fires_once_with_all() -> None:
    """`join(events: list[Done])` fires once with the full batch, no ctx.store."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(5)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await FanOut(timeout=10).run()
    assert result == [0, 1, 2, 3, 4]


async def test_async_generator_producer_join_fires_once() -> None:
    """An `AsyncIterator[E]` (async generator) producer drives the same join."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> AsyncIterator[Task]:
            for i in range(4):
                yield Task(n=i)

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n * 10)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await FanOut(timeout=10).run()
    assert result == [0, 10, 20, 30]


async def test_join_fires_exactly_once() -> None:
    """The join body executes exactly once per batch."""

    calls: list[int] = []

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            calls.append(len(events))
            return StopEvent(result=len(events))

    result = await FanOut(timeout=10).run()
    assert result == 3
    assert calls == [3]


async def test_empty_batch_fires_join_once_with_empty_list() -> None:
    """`return []` still closes the batch; the join fires once with `[]`."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return []

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=["empty", len(events)])

    result = await FanOut(timeout=10).run()
    assert result == ["empty", 0]


async def test_branch_death_join_sees_surviving_subset() -> None:
    """A 1:1 worker returning None drops its branch; the join fires with the rest."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(6)]

        @step
        async def work(self, ev: Task) -> Done | None:
            # Drop even branches.
            if ev.n % 2 == 0:
                return None
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await FanOut(timeout=10).run()
    assert result == [1, 3, 5]


async def test_multi_level_fan_out_joins_at_innermost_level() -> None:
    """Nested fan-out: inner joins fire per outer task, then an outer join."""

    class FanOut(Workflow):
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
        async def per_inner(self, events: list[InnerDone]) -> InnerSummary:
            outer = events[0].outer
            return InnerSummary(outer=outer, total=len(events))

        @step
        async def per_outer(self, events: list[InnerSummary]) -> StopEvent:
            return StopEvent(result=sorted((s.outer, s.total) for s in events))

    result = await FanOut(timeout=10).run()
    # Three outer tasks, each producing a 2-member inner batch.
    assert result == [(0, 2), (1, 2), (2, 2)]


class _ReplayFanOut(Workflow):
    """Single-level fan-out used by the replay-determinism test."""

    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(4)]

    @step
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)

    @step
    async def join(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=sorted(e.n for e in events))


async def _drive_reducer(
    wf: Workflow, run_id: str
) -> tuple[BrokerState, list[WorkflowTick]]:
    """Drive the pure reducer over a fan-out run, recording every tick.

    Exercises the exact reduction chokepoint (``_reduce_tick``) and the
    command -> tick conversion the live runner uses, with NO global patching.
    Returns the final broker state and the ordered tick stream; feeding that
    stream back through ``replay_ticks_stream`` must reproduce identical lineage.
    """
    step_fns = as_step_worker_functions(wf)
    state = BrokerState.from_workflow(wf)
    state.is_running = True
    pending: list[WorkflowTick] = [TickAddEvent(event=StartEvent())]
    recorded: list[WorkflowTick] = []
    now = 1000.0  # fixed; batch ids never depend on wall-clock
    for _ in range(1000):  # iteration cap: a logic bug fails fast, not hangs
        if not pending:
            break
        tick = pending.pop(0)
        recorded.append(tick)
        state, commands = _cl._reduce_tick(tick, state, now, run_id)
        for cmd in commands:
            if isinstance(cmd, CommandQueueEvent):
                pending.append(
                    TickAddEvent(
                        event=cmd.event,
                        step_name=cmd.step_name,
                        attempts=cmd.attempts,
                        first_attempt_at=cmd.first_attempt_at,
                        recovery_counts=dict(cmd.recovery_counts),
                        batch_stack=cmd.batch_stack,
                    )
                )
            elif isinstance(cmd, CommandCloseBatch):
                pending.append(
                    TickBatchClosed(
                        batch_id=cmd.batch_id,
                        step_name=cmd.step_name,
                        batch_stack=cmd.batch_stack,
                    )
                )
            elif isinstance(cmd, CommandRunWorker):
                worker = next(
                    w
                    for w in state.workers[cmd.step_name].in_progress
                    if w.worker_id == cmd.id
                )
                result = await step_fns[cmd.step_name](
                    state=worker.shared_state,
                    step_name=cmd.step_name,
                    event=cmd.event,
                    workflow=wf,
                    retry=RetryAttempt(),
                )
                pending.append(
                    TickStepResult(
                        step_name=cmd.step_name,
                        worker_id=cmd.id,
                        event=cmd.event,
                        result=result,
                    )
                )
    return state, recorded


def _done_batch_ids(ticks: list[WorkflowTick]) -> list[str]:
    return [
        t.batch_stack[-1]
        for t in ticks
        if isinstance(t, TickAddEvent) and isinstance(t.event, Done) and t.batch_stack
    ]


async def test_replay_reproduces_identical_batch_ids_and_grouping() -> None:
    """Drive a fan-out run through the pure reducer, then replay the recorded
    tick stream and assert identical batch ids and grouping. The determinism
    lives in the reducer itself — no global patching, no live runner."""

    wf = _ReplayFanOut()
    state, ticks = await _drive_reducer(wf, run_id="run-A")
    assert ticks, "expected a recorded tick stream"

    # The Done events all carry one batch id (single fan-out level).
    done_ids = _done_batch_ids(ticks)
    assert len(done_ids) == 4, done_ids
    assert len(set(done_ids)) == 1, done_ids
    the_batch = done_ids[0]

    # Exactly one close tick for that batch.
    closes = [
        t for t in ticks if isinstance(t, TickBatchClosed) and t.batch_id == the_batch
    ]
    assert len(closes) == 1, closes

    # Replay the exact stream twice; both reproduce the same final state.
    replay1 = await replay_ticks_stream(BrokerState.from_workflow(wf), _stream(ticks))
    replay2 = await replay_ticks_stream(BrokerState.from_workflow(wf), _stream(ticks))
    assert replay1.state.batch_seq == replay2.state.batch_seq == state.batch_seq
    assert replay1.state.batch_seq >= 1  # at least one batch was minted

    rebuilt = await rebuild_state_from_ticks_stream(
        BrokerState.from_workflow(wf), _stream(ticks)
    )
    assert rebuilt.batch_seq == replay1.state.batch_seq

    # The SAME run id reproduces IDENTICAL batch ids — ids are a pure function of
    # run_id + producing step + per-run sequence.
    _, ticks_again = await _drive_reducer(_ReplayFanOut(), run_id="run-A")
    assert _done_batch_ids(ticks_again) == done_ids

    # A DIFFERENT run id yields different ids but identical grouping cardinality.
    _, ticks_b = await _drive_reducer(_ReplayFanOut(), run_id="run-B")
    done_ids_b = _done_batch_ids(ticks_b)
    assert len(set(done_ids_b)) == 1, done_ids_b
    assert done_ids_b[0] != the_batch


async def test_batch_aborted_fail_default_fails_workflow() -> None:
    """A fan-out exhausting retries mid-stream fails the run under on_partial=fail."""

    class FanOut(Workflow):
        @step(retry_policy=ConstantDelayRetryPolicy(maximum_attempts=1, delay=0))
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            raise RuntimeError("boom mid-stream")

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=len(events))

    with pytest.raises((RuntimeError, WorkflowRuntimeError)):
        await FanOut(timeout=10).run()


async def test_batch_aborted_fire_fires_join_with_partial() -> None:
    """With on_partial="fire", a mid-stream-aborted batch still fires the join.

    on_partial is not user-settable until L3; set it on the config directly to
    exercise the TickBatchAborted -> fire path end-to-end.
    """

    class FanOut(Workflow):
        @step(retry_policy=ConstantDelayRetryPolicy(maximum_attempts=1, delay=0))
        async def fan_out(self, ev: StartEvent) -> AsyncIterator[Task]:
            yield Task(n=0)
            yield Task(n=1)
            raise RuntimeError("mid-stream boom")

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=["partial", sorted(e.n for e in events)])

    wf = FanOut(timeout=10)
    wf.join._step_config.on_partial = "fire"
    result = await wf.run()
    # The two pre-failure tasks reached work and the join fired with them.
    assert result[0] == "partial"
    assert result[1] == [0, 1]


async def test_async_generator_with_context_param() -> None:
    """An async-generator fan-out may also take a Context parameter."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ctx: Context, ev: StartEvent) -> AsyncIterator[Task]:
            await ctx.store.set("emitted", 3)
            for i in range(3):
                yield Task(n=i)

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=len(events))

    result = await FanOut(timeout=10).run()
    assert result == 3


async def test_no_ctx_store_threading_needed() -> None:
    """Sanity: the terse form needs neither ctx.store nor collect_events."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ctx: Context, ev: StartEvent) -> list[Task]:
            # Deliberately do NOT set any cardinality in ctx.store.
            return [Task(n=i) for i in range(7)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=len(events))

    result = await FanOut(timeout=10).run()
    assert result == 7
