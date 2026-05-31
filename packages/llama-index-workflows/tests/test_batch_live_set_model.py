# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Model test for the fan-out/fan-in live-set closure rule.

Drives a synthetic live set ``L(B)`` through the transitions the reducer applies
— seed at open, same-level emission, collect delivery, fan-out placeholder, and
close-on-empty — directly against the pure helpers, so the core accounting is
verifiable without spinning a full workflow. The behavioral end-to-end coverage
lives in ``test_batch_lineage_regressions.py``; this pins the invariant itself:
**a batch closes exactly when its live set empties, never before.**
"""

from __future__ import annotations

from workflows import Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.control_loop import (
    _apply_live_delta,
    _close_batch,
    _count_accepting_steps,
)
from workflows.runtime.types.commands import CommandCloseBatch, WorkflowCommand
from workflows.runtime.types.internal_state import Batch, BrokerState


class Task(Event):
    n: int


class Done(Event):
    n: int


class _WF(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(3)]

    @step
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)

    @step
    async def collect_a(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=len(events))

    @step
    async def collect_b(self, events: list[Done]) -> None:
        return None


def _state() -> BrokerState:
    return BrokerState.from_workflow(_WF())


def _open_batch(state: BrokerState, live: int) -> Batch:
    batch = Batch(
        batch_id="batch-test",
        producer="fan_out",
        origin_stack=(),
        bound_collects=("collect_a", "collect_b"),
        live=live,
    )
    state.batches[batch.batch_id] = batch
    return batch


def test_count_accepting_steps_is_work_item_fan_out_factor() -> None:
    state = _state()
    # Task is accepted by exactly one step (work); Done by two collects.
    assert _count_accepting_steps(state, Task) == 1
    assert _count_accepting_steps(state, Done) == 2


def test_close_fires_exactly_when_live_empties() -> None:
    state = _state()
    _open_batch(state, live=1)
    # A positive/neutral delta never closes.
    assert _apply_live_delta(state, "batch-test", +2) == []
    assert state.batches["batch-test"].live == 3
    assert _apply_live_delta(state, "batch-test", -2) == []
    assert state.batches["batch-test"].live == 1
    # Reaching zero closes exactly once and removes the record.
    commands = _apply_live_delta(state, "batch-test", -1)
    assert len(commands) == 1
    assert isinstance(commands[0], CommandCloseBatch)
    assert commands[0].batch_id == "batch-test"
    assert commands[0].step_name == "fan_out"  # producer, not a collect
    assert "batch-test" not in state.batches


def test_full_two_collect_batch_drains_to_close() -> None:
    """Walk #1's accounting: 3 Tasks, each work emits Done to two collects.

    Seed L(B) = sum of accepting-step counts per member. Each work resolves
    (-1 + 2 successors); each of the 6 collect deliveries resolves (-1). The
    batch closes precisely on the last delivery, never earlier.
    """
    state = _state()
    members = [Task(n=i) for i in range(3)]
    seed = sum(_count_accepting_steps(state, type(m)) for m in members)
    _open_batch(state, live=seed)
    assert seed == 3

    # Three 1:1 work resolutions: -1 (death) + 2 (Done accepted by two collects).
    for _ in range(3):
        assert (
            _apply_live_delta(
                state, "batch-test", _count_accepting_steps(state, Done) - 1
            )
            == []
        )
    assert state.batches["batch-test"].live == 6

    # Six collect deliveries (3 Done x 2 collects), each -1. Only the last closes.
    closed: list[WorkflowCommand] = []
    for _ in range(6):
        closed.extend(_apply_live_delta(state, "batch-test", -1))
    assert len(closed) == 1
    assert isinstance(closed[0], CommandCloseBatch)
    assert "batch-test" not in state.batches


def test_apply_delta_is_noop_for_missing_or_none_batch() -> None:
    state = _state()
    assert _apply_live_delta(state, None, -1) == []
    assert _apply_live_delta(state, "nonexistent", -1) == []
    assert _close_batch(state, "nonexistent") == []
