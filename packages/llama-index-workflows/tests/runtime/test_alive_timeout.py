# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from workflows import Workflow, step
from workflows.context.serializers import JsonSerializer
from workflows.events import StartEvent, StopEvent
from workflows.runtime.control_loop.reduce import _reduce_tick
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.ticks import TickAddEvent, TickSessionStart


class _AliveBudgetWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


def test_stamped_ticks_accrue_alive_budget_and_round_trip() -> None:
    workflow = _AliveBudgetWorkflow()
    state = BrokerState.from_workflow(workflow)

    state, _ = _reduce_tick(TickSessionStart(stamped_at=10.0), state, 999.0)
    state, _ = _reduce_tick(
        TickAddEvent(event=StartEvent(), stamped_at=12.5), state, 999.0
    )

    assert state.elapsed_alive == 2.5
    assert state.last_alive_stamp == 12.5

    serializer = JsonSerializer()
    serialized = state.to_serialized(serializer)
    restored = BrokerState.from_serialized(serialized, workflow, serializer)

    assert serialized.elapsed_alive == 2.5
    assert serialized.last_alive_stamp == 12.5
    assert restored.elapsed_alive == 2.5
    assert restored.last_alive_stamp == 12.5


def test_session_start_forgives_downtime_without_accruing() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state.elapsed_alive = 1.0
    state.last_alive_stamp = 10.0

    state, _ = _reduce_tick(TickSessionStart(stamped_at=100.0), state, 100.0)

    assert state.elapsed_alive == 1.0
    assert state.last_alive_stamp == 100.0

    state, _ = _reduce_tick(
        TickAddEvent(event=StartEvent(), stamped_at=101.0), state, 101.0
    )

    assert state.elapsed_alive == 2.0
    assert state.last_alive_stamp == 101.0


def test_legacy_unstamped_ticks_do_not_accrue_alive_budget() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())

    state, _ = _reduce_tick(TickAddEvent(event=StartEvent()), state, 50.0)

    assert state.elapsed_alive == 0.0
    assert state.last_alive_stamp is None
