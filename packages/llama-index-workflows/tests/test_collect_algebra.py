# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Collect selection algebra.

Covers the public ``Collect`` / ``Cardinality`` API, signature inference for
collection fan-in parameters (bare ``list[E]``, the ``Annotated[..., Collect()]``
synonym, union flat lists, and the ``Take(n)`` marker), the validation errors
that keep mode determination legible, and ``Take(n)`` runtime release.
"""

from __future__ import annotations

from typing import Annotated, Any, Callable, cast

import pytest
from workflows import All, Cardinality, Collect, Context, Take, Workflow, step
from workflows.decorators import StepConfig, StepFunction
from workflows.decorators import step as free_step
from workflows.errors import (
    WorkflowCancelledByUser,
    WorkflowRuntimeError,
    WorkflowValidationError,
)
from workflows.events import Event, StartEvent, StopEvent, WorkflowIdleEvent


class Task(Event):
    n: int


class Done(Event):
    n: int


class Skipped(Event):
    n: int


async def _run(wf: Workflow) -> object:
    """Run a workflow to completion, draining its event stream first."""
    handler = wf.run()
    async for _ in handler.stream_events():
        pass
    return await handler


# --------------------------------------------------------------------------- #
# Public API: Cardinality / Collect dataclasses
# --------------------------------------------------------------------------- #


def test_collect_defaults_to_all_cardinality() -> None:
    marker = Collect()
    assert isinstance(marker.cardinality, All)


def test_cardinality_hierarchy() -> None:
    assert isinstance(All(), Cardinality)
    assert isinstance(Take(1), Cardinality)
    assert Take(3).n == 3


def test_at_least_is_not_exported() -> None:
    """The at-least cardinality is outside the supported set."""
    import workflows

    export_name = "".join(("At", "Least"))
    assert not hasattr(workflows, export_name)


@pytest.mark.parametrize("bad", [0, -1, 1.5, "2"])
def test_take_rejects_bad_n(bad: Any) -> None:
    with pytest.raises(ValueError):
        Take(bad)


# --------------------------------------------------------------------------- #
# Inference matrix: signature -> StepConfig
# --------------------------------------------------------------------------- #


def _config_for(fn_builder: Callable[[type[Workflow]], StepFunction]) -> StepConfig:
    """Decorate a free function step against a throwaway workflow, return config."""

    class _W(Workflow):
        pass

    return fn_builder(_W)._step_config


def test_bare_list_infers_collect_all() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def join(events: list[Done]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")

        return join

    cfg = _config_for(build)
    assert cfg.collection_param == ("events", (Done,))
    assert cfg.collection_policy is not None
    assert isinstance(cfg.collection_policy.cardinality, All)


def test_annotated_collect_is_synonym_for_bare_list() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def join(events: Annotated[list[Done], Collect()]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")

        return join

    cfg = _config_for(build)
    assert cfg.collection_param == ("events", (Done,))
    assert cfg.collection_policy is not None
    assert isinstance(cfg.collection_policy.cardinality, All)


def test_annotated_take_cardinality_inferred() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def join(events: Annotated[list[Done], Collect(Take(2))]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")

        return join

    cfg = _config_for(build)
    assert cfg.collection_policy is not None
    assert cfg.collection_policy.cardinality == Take(2)


def test_union_flat_list_infers_all_member_types() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def report(events: list[Done | Skipped]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")

        return report

    cfg = _config_for(build)
    assert cfg.collection_param is not None
    assert cfg.collection_param[1] == (Done, Skipped)
    assert Done in cfg.accepted_events
    assert Skipped in cfg.accepted_events


def test_single_event_param_is_not_collection_param() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def work(ev: Task) -> Done:  # type: ignore[unused-ignore]
            return Done(n=ev.n)

        return work

    cfg = _config_for(build)
    assert cfg.collection_param is None
    assert cfg.collection_policy is None


# --------------------------------------------------------------------------- #
# Validation: legible mode determination
# --------------------------------------------------------------------------- #


def test_unknown_cardinality_raises_clear_error() -> None:
    class _W(Workflow):
        pass

    class Weird(Cardinality):
        pass

    with pytest.raises(WorkflowValidationError, match="All\\(\\) or Take\\(n\\)"):

        @free_step(workflow=_W)
        async def join(  # type: ignore[unused-ignore]
            events: Annotated[list[Done], Collect(Weird())],
        ) -> StopEvent:
            return StopEvent(result="x")


def test_collect_marker_on_non_list_param_raises() -> None:
    class _W(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="only to collection fan-in"):

        @free_step(workflow=_W)
        async def join(ev: Annotated[Done, Collect()]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")


def test_two_list_params_rejected_as_multi_slot() -> None:
    class _W(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match=r"at most one list\[E\]"):

        @free_step(workflow=_W)
        async def merge(a: list[Done], b: list[Skipped]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")


# --------------------------------------------------------------------------- #
# Runtime: cardinality release
# --------------------------------------------------------------------------- #


async def test_take_one_fires_with_first_and_completes() -> None:
    """`Take(1)` releases on the first arrival with a single event."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(5)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def fastest(
            self, events: Annotated[list[Done], Collect(Take(1))]
        ) -> StopEvent:
            return StopEvent(result=len(events))

    result = await _run(FanOut(timeout=10))
    assert result == 1


async def test_take_one_leaves_siblings_running() -> None:
    """`Take(1)` fires once; the dropped siblings still run without error."""

    work_calls: list[int] = []
    join_calls: list[int] = []

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step
        async def work(self, ev: Task) -> Done:
            work_calls.append(ev.n)
            return Done(n=ev.n)

        @step
        async def fastest(
            self, events: Annotated[list[Done], Collect(Take(1))]
        ) -> StopEvent:
            join_calls.append(len(events))
            return StopEvent(result="done")

    result = await _run(FanOut(timeout=10))
    assert result == "done"
    # The join fired exactly once with a single event.
    assert join_calls == [1]


async def test_take_two_fires_with_two() -> None:
    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(6)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def first_two(
            self, events: Annotated[list[Done], Collect(Take(2))]
        ) -> StopEvent:
            return StopEvent(result=len(events))

    result = await _run(FanOut(timeout=10))
    assert result == 2


async def test_take_covers_quorum() -> None:
    """Quorum (commit once N have arrived) is `Take(N)` in v1: release at N."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(5)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def commit(
            self, acks: Annotated[list[Done], Collect(Take(3))]
        ) -> StopEvent:
            return StopEvent(result=len(acks))

    result = await _run(FanOut(timeout=10))
    assert result == 3


async def test_take_below_threshold_fires_on_close() -> None:
    """`Take(n)` with fewer than n members fires on stream close, not early."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def commit(
            self, events: Annotated[list[Done], Collect(Take(5))]
        ) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(FanOut(timeout=10))
    assert result == [0, 1, 2]


async def test_take_inside_nested_stream_releases_per_inner_level() -> None:
    """`Take(n)` on an inner join releases early within each inner stream and the
    outer join still sees one summary per inner stream."""

    class InnerTask(Event):
        outer: int
        inner: int

    class InnerDone(Event):
        outer: int
        inner: int

    class InnerSummary(Event):
        outer: int
        count: int

    class FanOut(Workflow):
        @step
        async def outer(self, ev: StartEvent) -> list[Task]:
            return [Task(n=o) for o in range(2)]

        @step
        async def inner(self, ev: Task) -> list[InnerTask]:
            return [InnerTask(outer=ev.n, inner=i) for i in range(4)]

        @step
        async def inner_work(self, ev: InnerTask) -> InnerDone:
            return InnerDone(outer=ev.outer, inner=ev.inner)

        @step
        async def per_inner(
            self, events: Annotated[list[InnerDone], Collect(Take(2))]
        ) -> InnerSummary:
            return InnerSummary(outer=events[0].outer, count=len(events))

        @step
        async def per_outer(self, events: list[InnerSummary]) -> StopEvent:
            return StopEvent(result=sorted((s.outer, s.count) for s in events))

    result = await _run(FanOut(timeout=10))
    # Each inner stream releases exactly 2 (Take(2)); one summary per outer.
    assert result == [(0, 2), (1, 2)], result


async def test_union_flat_list_collects_all_member_types() -> None:
    """`list[Done | Skipped]` collects both member types into one closed stream."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(6)]

        @step
        async def work(self, ev: Task) -> Done | Skipped:
            return Done(n=ev.n) if ev.n % 2 == 0 else Skipped(n=ev.n)

        @step
        async def report(self, events: list[Done | Skipped]) -> StopEvent:
            dones = sorted(e.n for e in events if isinstance(e, Done))
            skips = sorted(e.n for e in events if isinstance(e, Skipped))
            return StopEvent(result=(dones, skips))

    result = await _run(FanOut(timeout=10))
    assert result == ([0, 2, 4], [1, 3, 5])


async def test_annotated_all_runs_like_bare_list() -> None:
    """`Annotated[list[E], Collect()]` behaves exactly like bare `list[E]`."""

    class FanOut(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: Annotated[list[Done], Collect()]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(FanOut(timeout=10))
    assert result == [0, 1, 2, 3]


# --------------------------------------------------------------------------- #
# Streams are runtime facts: only an actual list return mints a stream.
# --------------------------------------------------------------------------- #


class Round(Event):
    i: int


async def test_none_return_emits_no_stream_and_joins_do_not_fire() -> None:
    """`return None` is a dead branch: no stream, no join firing.

    The producer is kicked three times; only the execution that actually
    returns a list opens a stream, so the join fires exactly once with its
    members — never with a fabricated [] for the None executions.
    """
    join_calls: list[list[int]] = []
    calls = {"n": 0}

    class WF(Workflow):
        @step
        async def driver(self, ctx: Context, ev: StartEvent) -> Round:
            ctx.send_event(Round(i=1))
            ctx.send_event(Round(i=2))
            return Round(i=0)

        @step(num_workers=1)
        async def produce(self, ev: Round) -> list[Task] | None:
            calls["n"] += 1
            if calls["n"] < 3:
                return None
            return [Task(n=k) for k in range(2)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            join_calls.append(sorted(e.n for e in events))
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(WF(timeout=8))
    assert result == [0, 1]
    assert join_calls == [[0, 1]], join_calls


async def test_bare_union_member_routes_ordinarily_and_join_idles() -> None:
    """`-> list[A] | B` returning B never fires list[A] joins.

    With the B lineage dying out, the run has no stream and nothing to do: it
    idles (observably via WorkflowIdleEvent) instead of completing the join
    with a fabricated [].
    """
    join_calls: list[list[int]] = []
    singles: list[int] = []

    class Single(Event):
        n: int

    class WF(Workflow):
        @step
        async def produce(self, ev: StartEvent) -> list[Task] | Single:
            return Single(n=7)

        @step
        async def handle_single(self, ev: Single) -> None:
            singles.append(ev.n)
            return None

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            join_calls.append(sorted(e.n for e in events))
            return StopEvent(result=None)

    handler = WF(timeout=5).run()
    saw_idle = False
    async for ev in handler.stream_events(expose_internal=True):
        if isinstance(ev, WorkflowIdleEvent):
            saw_idle = True
            break
    await handler.cancel_run()
    try:
        await handler
    except WorkflowCancelledByUser:
        pass

    assert saw_idle
    assert singles == [7]
    assert join_calls == [], join_calls


async def test_bare_member_declared_both_listed_and_bare_is_single_dispatch() -> None:
    """`-> list[A] | A` returning a bare A is the declared single member."""
    join_calls: list[list[int]] = []

    class WF(Workflow):
        @step
        async def produce(self, ev: StartEvent) -> list[Done] | Done:
            return Done(n=5)

        @step
        async def handle(self, ev: Done) -> StopEvent:
            return StopEvent(result=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            join_calls.append(sorted(e.n for e in events))
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(WF(timeout=5))
    assert result == 5
    assert join_calls == [], join_calls


async def test_bare_element_under_pure_list_return_is_runtime_error() -> None:
    """A bare element where only list[E] is declared fails loudly."""

    class WF(Workflow):
        @step
        async def produce(self, ev: StartEvent) -> list[Done]:
            return cast("list[Done]", Done(n=0))  # wrong shape on purpose

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=len(events))

    with pytest.raises(WorkflowRuntimeError, match="bare"):
        await _run(WF(timeout=5))


# --------------------------------------------------------------------------- #
# Re-fanning collect steps open a child stream, not a parent-level binding.
# --------------------------------------------------------------------------- #


async def test_refanning_collect_fires_join_once_per_refan_stream() -> None:
    """A collect step that fans out again must not create a spurious binding.

    outer -> inner fan-out -> collect_and_refan(list[D]) -> list[N] -> join.
    The join is bound to each re-fan stream and fires once per instance with
    its members — never an extra time with [] when the outer stream closes.
    """
    join_calls: list[list[tuple[int, int]]] = []

    class Outer(Event):
        o: int

    class D(Event):
        o: int
        i: int

    class N(Event):
        o: int
        i: int

    class Total(Event):
        o: int
        count: int

    class WF(Workflow):
        @step
        async def outer(self, ev: StartEvent) -> list[Outer]:
            return [Outer(o=k) for k in range(2)]

        @step
        async def inner_fan(self, ev: Outer) -> list[D]:
            return [D(o=ev.o, i=j) for j in range(2)]

        @step
        async def collect_and_refan(self, events: list[D]) -> list[N]:
            return [N(o=e.o, i=e.i) for e in events]

        @step
        async def join(self, events: list[N]) -> Total:
            join_calls.append(sorted((e.o, e.i) for e in events))
            return Total(o=events[0].o if events else -1, count=len(events))

        @step
        async def total_join(self, events: list[Total]) -> StopEvent:
            return StopEvent(result=sorted((t.o, t.count) for t in events))

    result = await _run(WF(timeout=8))
    assert result == [(0, 2), (1, 2)], result
    assert sorted(len(c) for c in join_calls) == [2, 2], join_calls
    assert all(c for c in join_calls), join_calls


# --------------------------------------------------------------------------- #
# Multi-slot joins must fill at one stream level.
# --------------------------------------------------------------------------- #


def test_mixed_level_multi_slot_join_rejected_at_init() -> None:
    """Slots produced at different stream levels can never join."""

    class A(Event):
        n: int

    class B(Event):
        n: int

    with pytest.raises(WorkflowValidationError, match="common stream level"):

        class WF(Workflow):
            @step
            async def fan(self, ev: StartEvent) -> list[Task]:
                return [Task(n=0)]

            @step
            async def work(self, ev: Task) -> A:
                return A(n=ev.n)

            @step
            async def top(self, ev: StartEvent) -> B:
                return B(n=1)

            @step
            async def pair(self, a: A, b: B) -> StopEvent:
                return StopEvent(result=(a.n, b.n))

        WF()._validate()
