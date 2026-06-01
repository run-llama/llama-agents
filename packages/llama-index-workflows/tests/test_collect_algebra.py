# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Collect selection algebra.

Covers the public ``Collect`` / ``Cardinality`` API, signature inference for
batch fan-in parameters (bare ``list[E]``, the ``Annotated[..., Collect()]``
synonym, union flat lists, and the ``Take(n)`` marker), the validation errors
that keep mode determination legible, and ``Take(n)`` runtime release. Other
cardinalities and the cross-level/provenance/predicate knobs are reserved and
raise validation errors when declared.
"""

from __future__ import annotations

from typing import Annotated, Any, Callable

import pytest
from workflows import All, Cardinality, Collect, Take, Workflow, step
from workflows.decorators import StepConfig, StepFunction
from workflows.decorators import step as free_step
from workflows.errors import WorkflowValidationError
from workflows.events import Event, StartEvent, StopEvent


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
    assert marker.at is None
    assert marker.from_ is None
    assert marker.where is None


def test_cardinality_hierarchy() -> None:
    assert isinstance(All(), Cardinality)
    assert isinstance(Take(1), Cardinality)
    assert Take(3).n == 3


def test_atleast_is_not_exported() -> None:
    """AtLeast is outside the supported cardinality set."""
    import workflows

    assert not hasattr(workflows, "AtLeast")


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
    assert cfg.batch_collect_param == ("events", (Done,))
    assert cfg.batch_collect is not None
    assert isinstance(cfg.batch_collect.cardinality, All)


def test_annotated_collect_is_synonym_for_bare_list() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def join(events: Annotated[list[Done], Collect()]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")

        return join

    cfg = _config_for(build)
    assert cfg.batch_collect_param == ("events", (Done,))
    assert cfg.batch_collect is not None
    assert isinstance(cfg.batch_collect.cardinality, All)


def test_annotated_take_cardinality_inferred() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def join(events: Annotated[list[Done], Collect(Take(2))]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")

        return join

    cfg = _config_for(build)
    assert cfg.batch_collect is not None
    assert cfg.batch_collect.cardinality == Take(2)


def test_union_flat_list_infers_all_member_types() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def report(events: list[Done | Skipped]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")

        return report

    cfg = _config_for(build)
    assert cfg.batch_collect_param is not None
    assert cfg.batch_collect_param[1] == (Done, Skipped)
    assert Done in cfg.accepted_events
    assert Skipped in cfg.accepted_events


def test_single_event_param_is_not_batch_collect() -> None:
    def build(w: type[Workflow]) -> StepFunction:
        @free_step(workflow=w)
        async def work(ev: Task) -> Done:  # type: ignore[unused-ignore]
            return Done(n=ev.n)

        return work

    cfg = _config_for(build)
    assert cfg.batch_collect_param is None
    assert cfg.batch_collect is None


# --------------------------------------------------------------------------- #
# Validation: legible mode determination, reserved knobs
# --------------------------------------------------------------------------- #


def test_collect_at_raises_clear_error() -> None:
    class _W(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="at=.*not implemented"):

        @free_step(workflow=_W)
        async def join(events: Annotated[list[Done], Collect(at="other")]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")


def test_collect_from_raises_clear_error() -> None:
    class _W(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="from_=.*not implemented"):

        @free_step(workflow=_W)
        async def join(
            events: Annotated[list[Done], Collect(from_="src")],
        ) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")


def test_collect_where_raises_clear_error() -> None:
    class _W(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="where=.*not supported"):

        @free_step(workflow=_W)
        async def join(  # type: ignore[unused-ignore]
            events: Annotated[list[Done], Collect(where=lambda e: True)],
        ) -> StopEvent:
            return StopEvent(result="x")


def test_collect_marker_on_non_list_param_raises() -> None:
    class _W(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="only to batch fan-in"):

        @free_step(workflow=_W)
        async def join(ev: Annotated[Done, Collect()]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")


def test_two_list_params_rejected_as_multi_slot() -> None:
    class _W(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="at most one batch"):

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
    """`Take(n)` with fewer than n members fires on batch close, not early."""

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


async def test_take_inside_nested_batch_releases_per_inner_level() -> None:
    """`Take(n)` on an inner join releases early within each inner batch and the
    outer join still sees one summary per inner batch."""

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
    # Each inner batch releases exactly 2 (Take(2)); one summary per outer.
    assert result == [(0, 2), (1, 2)], result


async def test_union_flat_list_collects_all_member_types() -> None:
    """`list[Done | Skipped]` collects both member types into one closed batch."""

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
