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

import pytest
from workflows import Workflow, step
from workflows.context.serializers import JsonSerializer
from workflows.errors import WorkflowRuntimeError
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.control_loop.streams import (
    _adjust_open_work_items,
    _classify_work_item,
    _close_collection_stream,
    _count_accepting_steps,
)
from workflows.runtime.types.commands import (
    CommandRunWorker,
    WorkflowCommand,
)
from workflows.runtime.types.internal_state import (
    BrokerState,
    CollectionReleaseState,
    CollectionStreamInstance,
)
from workflows.runtime.types.results import StepWorkerFailed
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickStepResult


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
    return sorted(c.step_id.name for c in commands if isinstance(c, CommandRunWorker))


def test_close_fires_exactly_when_live_empties() -> None:
    state = _state()
    _open_stream(state, open_work_items=1)
    # A positive/neutral delta never closes.
    assert _adjust_open_work_items(state, "stream-test", +2, 0.0) == []
    assert state.streams["stream-test"].open_work_items == 3
    assert _adjust_open_work_items(state, "stream-test", -2, 0.0) == []
    assert state.streams["stream-test"].open_work_items == 1
    # Reaching zero closes exactly once: the record is removed and both
    # bindings' releases fire inline as collect invocations.
    commands = _adjust_open_work_items(state, "stream-test", -1, 0.0)
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
            _adjust_open_work_items(
                state, "stream-test", _count_accepting_steps(state, Done) - 1, 0.0
            )
            == []
        )
    assert state.streams["stream-test"].open_work_items == 6

    # Six collect deliveries (3 Done x 2 collects), each -1. Only the last
    # closes, firing both bindings' releases inline.
    closed: list[WorkflowCommand] = []
    for _ in range(6):
        closed.extend(_adjust_open_work_items(state, "stream-test", -1, 0.0))
    assert _release_targets(closed) == ["collect_a", "collect_b"]
    assert "stream-test" not in state.streams


def test_apply_delta_is_noop_for_missing_or_none_stream() -> None:
    state = _state()
    assert _adjust_open_work_items(state, None, -1, 0.0) == []
    assert _adjust_open_work_items(state, "nonexistent", -1, 0.0) == []
    assert _close_collection_stream(state, "nonexistent", 0.0) == []


def test_failed_work_without_redelivery_is_not_classified_live() -> None:
    tick = TickStepResult(
        step_id=StepId.root("work"),
        worker_id=0,
        event=Task(n=0),
        result=[StepWorkerFailed(exception=RuntimeError("boom"), failed_at=1.0)],
    )

    with pytest.raises(WorkflowRuntimeError, match="Cannot classify"):
        _classify_work_item(
            tick,
            [],
            rerun_scheduled=False,
            redelivery_scheduled=False,
            fanned_out=False,
        )


def test_unreleased_release_buffer_serializes_and_fires_on_close() -> None:
    state = _state()
    stream = _open_stream(state, open_work_items=1)
    binding = next(
        b
        for b in state.config.bindings_for_source("fan_out")
        if b.target_step == "collect_a"
    )
    key = f"{stream.stream_id}:{binding.id}"
    state.collection_release_states[key] = CollectionReleaseState(
        binding_id=binding.id,
        stream_id=stream.stream_id,
        buffer=[Done(n=7)],
    )

    serializer = JsonSerializer()
    restored = BrokerState.from_serialized(
        state.to_serialized(serializer), _WF(), serializer
    )
    restored_buffer = restored.collection_release_states[key].buffer
    assert [event.n for event in restored_buffer] == [7]

    commands = _close_collection_stream(restored, stream.stream_id, 0.0)
    assert _release_targets(commands) == ["collect_a", "collect_b"]
    payload = (
        restored.workers["collect_a"]
        .in_progress[0]
        .shared_state.collection_release_payload
    )
    assert payload is not None
    assert [event.n for event in payload.events] == [7]
