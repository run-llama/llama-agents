# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Model test for the fan-out/fan-in stream liveness rule.

Drives a synthetic stream through the transitions the reducer applies: seed at
open, same-scope emission, collect delivery, nested fan-out, and close-on-empty.
End-to-end coverage lives in
``test_collection_stream_regressions.py``; this pins the invariant itself:
**a stream closes exactly when its open work set empties, never before.**
"""

from __future__ import annotations

from workflows import Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.control_loop import (
    _apply_stream_work_delta,
    _close_collection_stream,
    _count_accepting_steps,
)
from workflows.runtime.types.commands import (
    CommandRunWorker,
    WorkflowCommand,
)
from workflows.runtime.types.internal_state import BrokerState, CollectionStreamInstance


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


def _open_stream(state: BrokerState, open_work_items: int) -> CollectionStreamInstance:
    stream = CollectionStreamInstance(
        stream_id="stream-test",
        source_step="fan_out",
        source_execution_id="fan_out:0:0",
        parent_stream_id=None,
        scope_path=(),
        accepting_binding_ids=tuple(
            binding.id for binding in state.config.bindings_for_source("fan_out")
        ),
        open_work_items=open_work_items,
    )
    state.streams[stream.stream_id] = stream
    return stream


def test_count_accepting_steps_is_work_item_fan_out_factor() -> None:
    state = _state()
    # Task is accepted by exactly one step (work); Done by two collects.
    assert _count_accepting_steps(state, Task) == 1
    assert _count_accepting_steps(state, Done) == 2


def _release_targets(commands: list[WorkflowCommand]) -> list[str]:
    """Step names invoked by inline release-firing commands."""
    return sorted(c.step_name for c in commands if isinstance(c, CommandRunWorker))


def test_close_fires_exactly_when_live_empties() -> None:
    state = _state()
    _open_stream(state, open_work_items=1)
    # A positive/neutral delta never closes.
    assert _apply_stream_work_delta(state, "stream-test", +2, 0.0) == []
    assert state.streams["stream-test"].open_work_items == 3
    assert _apply_stream_work_delta(state, "stream-test", -2, 0.0) == []
    assert state.streams["stream-test"].open_work_items == 1
    # Reaching zero closes exactly once: the record is removed and both
    # bindings' releases fire inline as collect invocations.
    commands = _apply_stream_work_delta(state, "stream-test", -1, 0.0)
    assert _release_targets(commands) == ["collect_a", "collect_b"]
    assert "stream-test" not in state.streams
    assert state.collection_release_states == {}


def test_full_two_collect_stream_drains_to_close() -> None:
    """Walk #1's accounting: 3 Tasks, each work emits Done to two collects.

    Seed open work = sum of accepting-step counts per member. Each work resolves
    (-1 + 2 successors); each of the 6 collect deliveries resolves (-1). The
    stream closes precisely on the last delivery, never earlier.
    """
    state = _state()
    members = [Task(n=i) for i in range(3)]
    seed = sum(_count_accepting_steps(state, type(m)) for m in members)
    _open_stream(state, open_work_items=seed)
    assert seed == 3

    # Three 1:1 work resolutions: -1 (death) + 2 (Done accepted by two collects).
    for _ in range(3):
        assert (
            _apply_stream_work_delta(
                state, "stream-test", _count_accepting_steps(state, Done) - 1, 0.0
            )
            == []
        )
    assert state.streams["stream-test"].open_work_items == 6

    # Six collect deliveries (3 Done x 2 collects), each -1. Only the last
    # closes, firing both bindings' releases inline.
    closed: list[WorkflowCommand] = []
    for _ in range(6):
        closed.extend(_apply_stream_work_delta(state, "stream-test", -1, 0.0))
    assert _release_targets(closed) == ["collect_a", "collect_b"]
    assert "stream-test" not in state.streams


def test_apply_delta_is_noop_for_missing_or_none_stream() -> None:
    state = _state()
    assert _apply_stream_work_delta(state, None, -1, 0.0) == []
    assert _apply_stream_work_delta(state, "nonexistent", -1, 0.0) == []
    assert _close_collection_stream(state, "nonexistent", 0.0) == []
