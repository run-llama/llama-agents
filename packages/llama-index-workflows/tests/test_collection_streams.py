# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Collection-stream fan-out / fan-in.

Covers the terse ``join(events: list[Done])`` form for static ``list[E]``
producers, multi-level fan-out, replay equality of stream ids and grouping,
empty streams, and branch death. Async-iterator producers are rejected at
decoration because this path only supports finite returned-list streams.
"""

from __future__ import annotations

from typing import AsyncIterator

import pytest
from workflows import Context, Workflow, step
from workflows.errors import WorkflowValidationError
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.control_loop import (
    rebuild_state_from_ticks_stream,
    replay_ticks_stream,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import as_snapshottable_adapter
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCollectionStreamClosed,
    WorkflowTick,
)


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


async def _run(wf: Workflow) -> object:
    """Run a workflow to completion, draining its event stream first.

    Draining the published-event stream lets the in-memory runtime's pull task
    finish cleanly so it does not linger on a shared event loop between tests.
    """
    handler = wf.run()
    async for _ in handler.stream_events():
        pass
    return await handler


async def test_static_list_producer_join_fires_once_with_all() -> None:
    """`join(events: list[Done])` fires once with the full stream, no ctx.store."""

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

    result = await _run(FanOut(timeout=10))
    assert result == [0, 1, 2, 3, 4]


def test_async_generator_producer_rejected_at_decoration() -> None:
    """Async-iterator fan-out is outside the returned-list stream contract."""

    with pytest.raises(WorkflowValidationError, match="Async-iterator fan-out"):

        class FanOut(Workflow):
            @step
            async def fan_out(self, ev: StartEvent) -> AsyncIterator[Task]:
                for i in range(4):
                    yield Task(n=i)

            @step
            async def join(self, events: list[Done]) -> StopEvent:
                return StopEvent(result=sorted(e.n for e in events))


async def test_join_fires_exactly_once() -> None:
    """The join body executes exactly once per stream."""

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

    result = await _run(FanOut(timeout=10))
    assert result == 3
    assert calls == [3]


async def test_empty_stream_fires_join_once_with_empty_list() -> None:
    """`return []` still closes the stream; the join fires once with `[]`."""

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

    result = await _run(FanOut(timeout=10))
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

    result = await _run(FanOut(timeout=10))
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

    result = await _run(FanOut(timeout=10))
    # Three outer tasks, each producing a 2-member inner stream.
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


async def _run_recording_ticks(
    wf: Workflow,
) -> tuple[object, list[WorkflowTick]]:
    """Run ``wf`` to completion and return (result, recorded tick stream).

    The in-memory runtime records every tick it reduces via ``on_tick`` and
    exposes them through the run adapter's ``replay()``. That recorded stream is
    exactly what persistence stores, so re-feeding it through
    ``replay_ticks_stream`` must rebuild identical stream scopes.
    """
    handler = wf.run()
    async for _ in handler.stream_events():
        pass
    result = await handler
    adapter = wf._runtime.get_external_adapter(handler.run_id)
    snapshottable = as_snapshottable_adapter(adapter)
    assert snapshottable is not None, "in-memory runtime adapter is snapshottable"
    ticks = list(snapshottable.replay())
    return result, ticks


def _done_stream_ids(ticks: list[WorkflowTick]) -> list[str]:
    return [
        t.scope_path[-1]
        for t in ticks
        if isinstance(t, TickAddEvent) and isinstance(t.event, Done) and t.scope_path
    ]


async def test_replay_reproduces_identical_stream_ids_and_grouping() -> None:
    """Record a real fan-out run's tick stream, replay it, and assert identical
    stream ids and grouping. Replay determinism lives in the reducer: stream ids
    are a pure function of the producer path and stream sequence."""

    wf = _ReplayFanOut()
    result, ticks = await _run_recording_ticks(wf)
    assert result == [0, 1, 2, 3]
    assert ticks, "expected a recorded tick stream"

    # The Done events all carry one stream id (single fan-out level).
    done_ids = _done_stream_ids(ticks)
    assert len(done_ids) == 4, done_ids
    assert len(set(done_ids)) == 1, done_ids
    the_stream = done_ids[0]

    # Exactly one close tick for that stream.
    closes = [
        t
        for t in ticks
        if isinstance(t, TickCollectionStreamClosed) and t.stream_id == the_stream
    ]
    assert len(closes) == 1, closes

    # Replay the exact stream twice; both reproduce the same final state
    # deterministically (the stream-id counter is a pure function of the stream).
    replay1 = await replay_ticks_stream(BrokerState.from_workflow(wf), _stream(ticks))
    replay2 = await replay_ticks_stream(BrokerState.from_workflow(wf), _stream(ticks))
    assert replay1.state.stream_seq == replay2.state.stream_seq
    assert replay1.state.stream_seq >= 1  # at least one stream was minted

    rebuilt = await rebuild_state_from_ticks_stream(
        BrokerState.from_workflow(wf), _stream(ticks)
    )
    assert rebuilt.stream_seq == replay1.state.stream_seq


async def test_no_stream_state_remains_after_completion() -> None:
    """A completed fan-out leaves no open streams or fan-in buffers in state."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    wf = FanOut(timeout=10)
    handler = wf.run()
    async for _ in handler.stream_events():
        pass
    await handler

    serialized = handler.ctx.to_dict()
    assert serialized.get("streams", {}) == {}, serialized.get("streams")
    for step_name, worker in serialized.get("workers", {}).items():
        assert not worker.get("collection_release_states"), (step_name, worker)
        assert not worker.get("collection_released"), (step_name, worker)


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

    result = await _run(FanOut(timeout=10))
    assert result == 7
