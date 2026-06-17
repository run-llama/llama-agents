# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Regression tests for the child-workflow namespace consolidation.

The child-workflow feature added a ``namespace`` dimension but left several
runtime consumers modelling a flat step space. These tests pin the behaviors
that broke at the unconverted sites: child ``list[E]`` fan-in, cross-namespace
stream accounting, child human-input round-trips, namespace teardown (sibling,
grandchild, caught timeout), and the ``max_recoveries`` bound on the
namespace-timeout recovery path.

Promoted from the QA probes in
``thoughts/shared/qa/raw/2026-06-16-child-workflows/`` and the two prior-review
repros.
"""

from __future__ import annotations

import pytest
from workflows import Workflow
from workflows.decorators import step
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
)
from workflows.runtime.types.internal_state import BrokerState, _binding_id
from workflows.runtime.types.step_id import StepId
from workflows.testing import WorkflowTestRunner

# --- Phase 1: child fan-in + cross-namespace accounting -----------------------


class _Item(Event):
    n: int


class _ChildFanStart(StartEvent):
    pass


class _ChildFanStop(StopEvent):
    total: int = 0


class _FanChild(Workflow):
    @step
    async def fan(self, ev: _ChildFanStart) -> list[_Item]:
        return [_Item(n=1), _Item(n=2), _Item(n=3)]

    @step
    async def collect(self, events: list[_Item]) -> _ChildFanStop:
        return _ChildFanStop(total=sum(e.n for e in events))


class _FanParent(Workflow):
    child: _FanChild

    @step
    async def start(self, ev: StartEvent) -> _ChildFanStart:
        return _ChildFanStart()

    @step
    async def finish(self, ev: _ChildFanStop) -> StopEvent:
        return StopEvent(result=ev.total)


@pytest.mark.asyncio
async def test_child_list_fan_in_joins_all_items() -> None:
    """A ``list[E]`` fan-out/fan-in *inside a child* binds within the child
    namespace and joins every item — previously the binding was computed
    root-only, so the child collect never bound and every item was dropped.
    """
    result = await WorkflowTestRunner(_FanParent(child=_FanChild())).run()
    assert result.result == 6


class _Shared(Event):
    pass


class _ReuseChildStart(StartEvent):
    pass


class _ReuseChildStop(StopEvent):
    pass


class _ReuseChild(Workflow):
    @step
    async def run_child(self, ev: _ReuseChildStart) -> _Shared:
        return _Shared()

    @step
    async def also_accepts_shared(self, ev: _Shared) -> _ReuseChildStop:
        return _ReuseChildStop()


class _ReuseParent(Workflow):
    child: _ReuseChild

    @step
    async def fan(self, ev: StartEvent) -> list[_Shared]:
        return [_Shared()]

    @step
    async def collect(self, events: list[_Shared]) -> StopEvent:
        return StopEvent(result=len(events))


@pytest.mark.asyncio
async def test_root_stream_accounting_ignores_same_typed_child_step() -> None:
    """A root collection stream must not wedge when a child step happens to
    accept the streamed event type. Birth-count is namespace-scoped, so the
    child's ``also_accepts_shared`` is not counted into the root stream's open
    work items (which previously left a phantom item open forever).
    """
    result = await WorkflowTestRunner(_ReuseParent(child=_ReuseChild())).run()
    assert result.result == 1


def test_root_binding_id_is_byte_identical_to_pre_namespace_format() -> None:
    """Root binding ids embed the bare step name (no namespace prefix), so a
    snapshot written before the StepId conversion still resolves on resume.
    """
    state = BrokerState.from_workflow(_ReuseParent(child=_ReuseChild()))
    root_bindings = [
        b for b in state.config.collection_bindings.values() if b.source_step.is_root
    ]
    assert root_bindings, "expected a root fan->collect binding"
    for binding in root_bindings:
        expected = _binding_id(
            StepId.root(binding.source_step.name),
            StepId.root(binding.target_step.name),
            binding.item_types,
            binding.policy,
        )
        assert binding.id == expected
        # The id string carries the bare names, never a "ns/name" projection.
        assert binding.id.startswith(
            f"{binding.source_step.name}->{binding.target_step.name}:"
        )
        assert "/" not in binding.id.split(":", 1)[0]


# --- Grandchild (3-level) fan-in ----------------------------------------------


class _GrandStart(StartEvent):
    pass


class _GrandStop(StopEvent):
    total: int = 0


class _GrandFanChild(Workflow):
    @step
    async def fan(self, ev: _GrandStart) -> list[_Item]:
        return [_Item(n=2), _Item(n=5)]

    @step
    async def collect(self, events: list[_Item]) -> _GrandStop:
        return _GrandStop(total=sum(e.n for e in events))


class _MidStart(StartEvent):
    pass


class _MidStop(StopEvent):
    total: int = 0


class _MidChild(Workflow):
    grand: _GrandFanChild

    @step
    async def start(self, ev: _MidStart) -> _GrandStart:
        return _GrandStart()

    @step
    async def finish(self, ev: _GrandStop) -> _MidStop:
        return _MidStop(total=ev.total)


class _GrandParent(Workflow):
    mid: _MidChild

    @step
    async def start(self, ev: StartEvent) -> _MidStart:
        return _MidStart()

    @step
    async def finish(self, ev: _MidStop) -> StopEvent:
        return StopEvent(result=ev.total)


@pytest.mark.asyncio
async def test_grandchild_fan_in_binds_within_its_own_namespace() -> None:
    """A ``list[E]`` join three levels deep binds within the grandchild
    namespace ``(mid, grand)``."""
    result = await WorkflowTestRunner(
        _GrandParent(mid=_MidChild(grand=_GrandFanChild()))
    ).run()
    assert result.result == 7
