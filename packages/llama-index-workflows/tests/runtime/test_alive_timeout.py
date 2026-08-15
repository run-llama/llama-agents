# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from typing import cast

import pytest
from workflows import Workflow, step
from workflows.context import Context
from workflows.context.serializers import JsonSerializer
from workflows.errors import WorkflowTimeoutError
from workflows.events import StartEvent, StopEvent
from workflows.runtime.control_loop.reduce import _reduce_tick
from workflows.runtime.control_loop.runner import _ControlLoopRunner
from workflows.runtime.types.commands import CommandHalt, CommandScheduleTimeout
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.results import StepWorkerWaiter
from workflows.runtime.types.step_function import as_step_worker_functions
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickSessionStart,
    TickTimeout,
    TickWaiterTimeout,
    TickWakeup,
)

from .conftest import MockRunAdapter


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


def test_timeout_tick_accrues_at_fire_and_halts_when_budget_is_spent() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state, _ = _reduce_tick(TickSessionStart(stamped_at=10.0), state, 999.0)

    state, commands = _reduce_tick(
        TickTimeout(timeout=5.0, stamped_at=15.0), state, 999.0
    )

    assert state.elapsed_alive == 5.0
    assert state.last_alive_stamp == 15.0
    assert any(isinstance(command, CommandHalt) for command in commands)


def test_timeout_tick_rearms_when_budget_is_not_spent() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state, _ = _reduce_tick(TickSessionStart(stamped_at=10.0), state, 999.0)

    state, commands = _reduce_tick(
        TickTimeout(timeout=5.0, stamped_at=13.0), state, 999.0
    )

    assert state.elapsed_alive == 3.0
    assert not any(isinstance(command, CommandHalt) for command in commands)
    schedules = [
        command for command in commands if isinstance(command, CommandScheduleTimeout)
    ]
    assert schedules == [CommandScheduleTimeout(timeout=5.0, at_time=15.0)]


def test_stale_timeout_tick_rearms_from_monotonic_anchor() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state.elapsed_alive = 4.0
    state.last_alive_stamp = 100.0

    _, commands = _reduce_tick(
        TickTimeout(timeout=10.0, stamped_at=90.0), state, 999.0
    )

    schedules = [
        command for command in commands if isinstance(command, CommandScheduleTimeout)
    ]
    assert schedules == [CommandScheduleTimeout(timeout=10.0, at_time=106.0)]


def test_legacy_unstamped_timeout_tick_halts_unconditionally() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())

    _, commands = _reduce_tick(TickTimeout(timeout=5.0), state, 1.0)

    assert any(isinstance(command, CommandHalt) for command in commands)


@pytest.mark.asyncio
async def test_resume_with_exhausted_budget_halts_before_running_step() -> None:
    calls: list[str] = []

    class CountingWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            calls.append("start")
            return StopEvent(result="ok")

    workflow = CountingWorkflow(timeout=5.0)
    state = BrokerState.from_workflow(workflow)
    state.elapsed_alive = 5.0
    state.workers[StepId.root("start")].collected_waiters.append(
        StepWorkerWaiter(
            waiter_id="rehydrated-waiter",
            event=StartEvent(),
            waiting_for_event=StopEvent,
            requirements={},
            has_requirements=True,
            resolved_event=None,
        )
    )
    assert any(isinstance(tick, TickAddEvent) for tick in state.rehydrate_with_ticks())
    adapter = MockRunAdapter(run_id="test")
    runner = _ControlLoopRunner(
        workflow,
        adapter,
        cast(Context, object()),
        as_step_worker_functions(workflow),
        state,
    )

    with pytest.raises(WorkflowTimeoutError):
        await runner.run()

    assert calls == []
    assert not any(isinstance(tick, TickAddEvent) for tick in adapter.replay())


def test_clock_regression_does_not_move_accrual_anchor_backwards() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state, _ = _reduce_tick(TickSessionStart(stamped_at=100.0), state, 999.0)
    state, _ = _reduce_tick(
        TickAddEvent(event=StartEvent(), stamped_at=90.0), state, 999.0
    )
    state, _ = _reduce_tick(
        TickAddEvent(event=StartEvent(), stamped_at=101.0), state, 999.0
    )

    assert state.elapsed_alive == 1.0
    assert state.last_alive_stamp == 101.0


def test_namespaced_tick_accrues_root_to_leaf_descent_chain() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state.children["nested"] = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state, _ = _reduce_tick(TickSessionStart(stamped_at=10.0), state, 999.0)

    state, _ = _reduce_tick(
        TickAddEvent(
            event=StartEvent(),
            origin_namespace=("nested",),
            stamped_at=12.0,
        ),
        state,
        999.0,
    )

    assert state.elapsed_alive == 2.0
    assert state.children["nested"].elapsed_alive == 2.0


def test_wakeup_and_waiter_timeout_ticks_accrue_alive_budget() -> None:
    state = BrokerState.from_workflow(_AliveBudgetWorkflow())
    state, _ = _reduce_tick(TickSessionStart(stamped_at=10.0), state, 999.0)
    state, _ = _reduce_tick(TickWakeup(due=11.0, stamped_at=11.0), state, 999.0)
    state, _ = _reduce_tick(
        TickWaiterTimeout(
            step_id=StepId.root("start"),
            waiter_id="missing",
            stamped_at=12.0,
        ),
        state,
        999.0,
    )

    assert state.elapsed_alive == 2.0
    assert state.last_alive_stamp == 12.0
