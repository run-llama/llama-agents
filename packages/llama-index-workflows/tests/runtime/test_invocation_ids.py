# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Every step dispatch gets a fresh invocation id minted by the reducer."""

from __future__ import annotations

import pytest
from workflows import Workflow, step
from workflows.context.serializers import JsonSerializer
from workflows.events import Event, StartEvent, StopEvent
from workflows.retry_policy import retry_policy, stop_after_attempt, wait_fixed
from workflows.runtime.control_loop.reduce import _reduce_tick
from workflows.runtime.types.commands import CommandRunWorker, WorkflowCommand
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.results import (
    AddCollectedEvent,
    DeleteCollectedEvent,
    StepFunctionResult,
    StepWorkerFailed,
    StepWorkerResult,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickSessionStart,
    TickStepResult,
    WorkflowTick,
)

WORK = StepId.root("work")


class Job(Event):
    n: int


class _Jobs(Workflow):
    @step
    async def begin(self, ev: StartEvent) -> Job:
        return Job(n=0)

    @step(num_workers=2)
    async def work(self, ev: Job) -> StopEvent:
        return StopEvent()


@pytest.fixture
def state() -> BrokerState:
    broker = BrokerState.from_workflow(_Jobs())
    broker.is_running = True
    return broker


def _reduce(
    state: BrokerState, tick: WorkflowTick
) -> tuple[BrokerState, list[CommandRunWorker]]:
    new_state, commands = _reduce_tick(tick, state, now_seconds=100.0)
    return new_state, _runs(commands)


def _runs(commands: list[WorkflowCommand]) -> list[CommandRunWorker]:
    return [c for c in commands if isinstance(c, CommandRunWorker)]


def _result(
    run: CommandRunWorker, *results: StepFunctionResult, worker_id: int | None = None
) -> TickStepResult:
    return TickStepResult(
        step_id=run.step_id,
        worker_id=run.id if worker_id is None else worker_id,
        event=run.event,
        result=list(results),
        invocation_id=run.invocation_id,
    )


def test_each_dispatch_mints_a_fresh_invocation_id(state: BrokerState) -> None:
    state, [first] = _reduce(state, TickAddEvent(event=Job(n=1)))
    state, [second] = _reduce(state, TickAddEvent(event=Job(n=2)))

    assert [first.invocation_id, second.invocation_id] == [
        "invocation_1",
        "invocation_2",
    ]
    in_progress = state.workers[WORK].in_progress
    assert [ip.invocation_id for ip in in_progress] == ["invocation_1", "invocation_2"]
    assert [ip.shared_state.invocation_id for ip in in_progress] == [
        "invocation_1",
        "invocation_2",
    ]
    assert state.invocation_seq == 2


def test_step_result_is_matched_by_invocation_id(state: BrokerState) -> None:
    state, [first] = _reduce(state, TickAddEvent(event=Job(n=1)))
    state, [second] = _reduce(state, TickAddEvent(event=Job(n=2)))

    # The worker slot is deliberately wrong: the invocation id decides.
    state, _ = _reduce(
        state, _result(second, StepWorkerResult(result=None), worker_id=first.id)
    )

    assert [ip.invocation_id for ip in state.workers[WORK].in_progress] == [
        first.invocation_id
    ]


def test_retry_dispatches_a_new_invocation(state: BrokerState) -> None:
    state.workers[WORK].config.retry_policy = retry_policy(
        wait=wait_fixed(0.0), stop=stop_after_attempt(3)
    )
    state, [run] = _reduce(state, TickAddEvent(event=Job(n=1)))

    state, [retry] = _reduce(
        state,
        _result(run, StepWorkerFailed(exception=ValueError("x"), failed_at=100.0)),
    )

    assert retry.invocation_id == "invocation_2"
    assert [ip.attempts for ip in state.workers[WORK].in_progress] == [1]


def test_session_start_reruns_in_progress_work_as_new_invocations(
    state: BrokerState,
) -> None:
    state, _ = _reduce(state, TickAddEvent(event=Job(n=1)))

    state, [rerun] = _reduce(state, TickSessionStart(stamped_at=100.0))

    assert rerun.invocation_id == "invocation_2"


def test_stale_collect_reruns_mint_new_invocations(state: BrokerState) -> None:
    state, [first] = _reduce(state, TickAddEvent(event=Job(n=1)))
    state, [second] = _reduce(state, TickAddEvent(event=Job(n=2)))
    state, _ = _reduce(
        state,
        _result(first, AddCollectedEvent(event_id="buf", event=Job(n=1))),
    )

    # second was dispatched before the buffer changed, so its firing is stale.
    state, [rerun] = _reduce(
        state, _result(second, DeleteCollectedEvent(event_id="buf"))
    )

    assert rerun.invocation_id == "invocation_3"
    [execution] = state.workers[WORK].in_progress
    assert execution.invocation_id == "invocation_3"
    assert execution.shared_state.invocation_id == "invocation_3"


def test_add_collected_rerun_mints_a_new_invocation(state: BrokerState) -> None:
    state, [first] = _reduce(state, TickAddEvent(event=Job(n=1)))
    state, [second] = _reduce(state, TickAddEvent(event=Job(n=2)))
    state, _ = _reduce(
        state,
        _result(first, AddCollectedEvent(event_id="buf", event=Job(n=1))),
    )

    state, [rerun] = _reduce(
        state, _result(second, AddCollectedEvent(event_id="buf", event=Job(n=2)))
    )

    assert rerun.invocation_id == "invocation_3"
    [execution] = state.workers[WORK].in_progress
    assert execution.invocation_id == "invocation_3"


def test_invocation_seq_survives_serialization(state: BrokerState) -> None:
    state, _ = _reduce(state, TickAddEvent(event=Job(n=1)))
    serializer = JsonSerializer()

    restored = BrokerState.from_serialized(
        state.to_serialized(serializer), _Jobs(), serializer
    )

    assert restored.invocation_seq == 1
    assert [ip.invocation_id for ip in restored.workers[WORK].in_progress] == [
        "invocation_1"
    ]


def test_start_event_dispatch_is_keyed_too(state: BrokerState) -> None:
    _, [run] = _reduce(state, TickAddEvent(event=StartEvent()))

    assert run.invocation_id == "invocation_1"
