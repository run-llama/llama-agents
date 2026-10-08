# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Reducer rules for delivery obligations.

Each test drives the reducer tick by tick, the way the runner does, and turns
every ``CommandQueueEvent`` into the ``TickAddEvent`` the runner would buffer.
"""

from __future__ import annotations

import pytest
from workflows.events import StepFailedEvent, StopEvent
from workflows.runtime.control_loop.reduce import _mark_terminal
from workflows.runtime.types.commands import CommandCompleteRun
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.results import (
    AddCollectedEvent,
    DeleteCollectedEvent,
    EmissionKey,
    StepWorkerFailed,
    StepWorkerResult,
)
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickTimeout,
    WorkflowTick,
)

from tests.runtime.delivery_fixtures import (
    SERIALIZER,
    SESSION_START,
    Boom,
    Both,
    Flow,
    Job,
    Out,
    Ping,
    Recovering,
    derived,
    dispatch,
    flow_state,
    fold,
    keys,
    label,
    ok,
    owing,
    reduce,
    restore,
    result,
    resume,
    routed,
    runs,
    send,
)


@pytest.fixture
def state() -> BrokerState:
    return flow_state()


def test_suffix_after_active_invocation_delivers_send_once(
    state: BrokerState,
) -> None:
    state, run = dispatch(state, Job(label="a"))

    restored, commands = fold(
        restore(state),
        [
            send(run, 0, Ping(label="ping-a")),
            result(run, ok(Out(label="a")), sends=[Ping(label="ping-a")]),
        ],
    )

    assert routed(restored, "sink") == ["ping-a"]
    assert routed(restored, "producer") == []
    # The send was routed while A was active; only the returned event is owed.
    assert keys(restored) == [(run.invocation_id, 1)]
    restored, _ = fold(restored, derived(commands))
    assert routed(restored, "consumer") == ["a"]
    assert restored.deliveries == {}


@pytest.mark.parametrize("order", [("x", "y"), ("y", "x")])
def test_suffix_with_two_active_inputs_applies_each_result_to_its_invocation(
    state: BrokerState, order: tuple[str, str]
) -> None:
    state, run_x = dispatch(state, Job(label="x"))
    state, run_y = dispatch(state, Job(label="y"))
    active = {"x": run_x, "y": run_y}

    ticks: list[WorkflowTick] = [
        send(active[name], 0, Ping(label=f"ping-{name}")) for name in ("x", "y")
    ]
    ticks += [
        result(active[name], ok(Out(label=name)), sends=[Ping(label=f"ping-{name}")])
        for name in order
    ]
    restored, commands = fold(restore(state), ticks)

    assert routed(restored, "sink") == ["ping-x", "ping-y"]
    assert routed(restored, "producer") == []
    restored, _ = fold(restored, derived(commands))
    assert routed(restored, "consumer") == ["x", "y"]
    assert restored.deliveries == {}


def test_shared_producer_with_delayed_send_tick_delivers_each_event_once(
    state: BrokerState,
) -> None:
    state, commands = reduce(state, TickAddEvent(event=Both(label="s")))
    by_step = {r.step_id.name: r for r in runs(commands)}
    sender, returner = by_step["sender"], by_step["returner"]

    # The sender's mailbox tick is delayed past both results.
    state, result_commands = fold(
        state,
        [
            result(sender, ok(None), sends=[Ping(label="x")]),
            result(returner, ok(Out(label="r"))),
        ],
    )
    restored = resume(state)
    # The journaled originals arrive late and are duplicates.
    late = [send(sender, 0, Ping(label="x")), *derived(result_commands)]
    restored, _ = fold(restored, late)

    assert routed(restored, "sink") == ["x"]
    assert routed(restored, "consumer") == ["r"]
    assert restored.deliveries == {}


def test_rerun_consumes_once_per_invocation_and_drops_the_stale_tick(
    state: BrokerState,
) -> None:
    state, first = dispatch(state, Job(label="a"))
    state, _ = reduce(state, send(first, 0, Ping(label="ping-1")))
    assert routed(state, "sink") == ["ping-1"]

    state, commands = reduce(state, SESSION_START)
    [second] = [r for r in runs(commands) if r.step_id.name == "producer"]
    assert second.invocation_id != first.invocation_id

    # The first invocation's tick arrives again after the rewind: dropped.
    state, _ = fold(
        state,
        [
            send(first, 0, Ping(label="ping-1")),
            send(second, 0, Ping(label="ping-2")),
            result(second, ok(Out(label="a")), sends=[Ping(label="ping-2")]),
        ],
    )

    assert routed(state, "sink") == ["ping-1", "ping-2"]
    assert keys(state) == [(second.invocation_id, 1)]


def test_send_from_a_stale_collect_rerun_survives_snapshot_before_its_tick(
    state: BrokerState,
) -> None:
    state, first = dispatch(state, Job(label="a"))
    state, second = dispatch(state, Job(label="b"))
    state, _ = reduce(
        state, result(first, AddCollectedEvent(event_id="buf", event=Job(label="a")))
    )
    # second fires against a stale buffer and reruns; its send X is still in
    # the mailbox when the snapshot is taken.
    state, commands = reduce(
        state,
        result(
            second,
            DeleteCollectedEvent(event_id="buf"),
            ok(Out(label="b")),
            sends=[Ping(label="x")],
        ),
    )
    assert [r.invocation_id for r in runs(commands)] != [second.invocation_id]

    restored = resume(state)
    restored, _ = reduce(restored, send(second, 0, Ping(label="x")))

    assert routed(restored, "sink") == ["x"]
    assert restored.deliveries == {}


@pytest.mark.parametrize("exit_by", ["exhausted_failure", "root_timeout"])
def test_terminal_exit_clears_deliveries(state: BrokerState, exit_by: str) -> None:
    state, failure = owing(state)

    terminal = failure if exit_by == "exhausted_failure" else TickTimeout(timeout=1.0)
    state, _ = reduce(state, terminal)

    assert state.is_running is False
    assert state.deliveries == {}
    assert state.to_serialized(SERIALIZER).deliveries == []


def test_cancelled_run_keeps_deliveries_and_resume_reemits_them(
    state: BrokerState,
) -> None:
    state, _ = owing(state)

    state, _ = reduce(state, TickCancelRun())
    restored, commands = reduce(restore(state), SESSION_START)

    assert [label(getattr(t, "event")) for t in derived(commands)] == ["owed"]
    restored, _ = fold(restored, derived(commands))
    assert routed(restored, "sink") == ["owed"]
    assert restored.deliveries == {}


@pytest.mark.parametrize("journaled_first", [True, False])
def test_journaled_derived_tick_and_reemitted_copy_deliver_once(
    state: BrokerState, journaled_first: bool
) -> None:
    state, run = dispatch(state, Job(label="a"))
    state, commands = reduce(state, result(run, ok(Out(label="a"))))
    [journaled] = derived(commands)
    restored = restore(state)

    if journaled_first:
        restored, _ = reduce(restored, journaled)
        restored, reemitted = reduce(restored, SESSION_START)
        assert derived(reemitted) == []
    else:
        restored, reemitted = reduce(restored, SESSION_START)
        restored, _ = fold(restored, [*derived(reemitted), journaled])

    assert routed(restored, "consumer") == ["a"]
    assert restored.deliveries == {}


def test_fan_out_with_stop_event_records_nothing_and_stops(
    state: BrokerState,
) -> None:
    state, _ = owing(state)
    state, run = dispatch(state, Job(label="a"))

    state, commands = reduce(
        state,
        result(
            run,
            StepWorkerResult(result=StopEvent(), fanned_out=True),
            StepWorkerResult(result=Out(label="a"), fanned_out=True),
        ),
    )

    assert state.is_running is False
    assert state.deliveries == {}
    assert derived(commands) == []
    assert any(isinstance(c, CommandCompleteRun) for c in commands)


def test_unkeyed_add_event_always_routes(state: BrokerState) -> None:
    state, _ = fold(state, [TickAddEvent(event=Ping(label="p"))] * 2)

    assert routed(state, "sink") == ["p", "p"]


def test_emissions_number_sends_then_returns_then_catch_error() -> None:
    # Validation builds the catch_error routing table.
    workflow = Recovering()
    workflow.validate()
    state = BrokerState.from_workflow(workflow)
    state.is_running = True
    state, run = dispatch(state, Boom(label="boom"))

    state, commands = reduce(
        state,
        result(
            run,
            StepWorkerFailed(exception=ValueError("x"), failed_at=100.0),
            sends=[Ping(label="first"), Ping(label="second")],
        ),
    )

    assert keys(state) == [(run.invocation_id, i) for i in range(3)]
    [catch] = derived(commands)
    assert isinstance(catch, TickAddEvent)
    assert catch.emission == EmissionKey(invocation_id=str(run.invocation_id), index=2)
    assert isinstance(catch.event, StepFailedEvent)


def test_terminal_exit_in_a_child_broker_stops_the_whole_run(
    state: BrokerState,
) -> None:
    child = BrokerState.from_workflow(Flow(disable_validation=True))
    child.is_running = True
    state.children["nested"] = child

    _mark_terminal(state, child)

    assert child.is_running is False
    assert state.is_running is False


def test_result_without_invocation_id_emits_unkeyed_and_records_nothing(
    state: BrokerState,
) -> None:
    state, run = dispatch(state, Job(label="a"))
    current_release = result(run, ok(Out(label="a"))).model_copy(
        update={"invocation_id": None}
    )

    state, commands = reduce(state, current_release)

    assert routed(state, "producer") == []
    assert state.deliveries == {}
    [unkeyed] = derived(commands)
    assert isinstance(unkeyed, TickAddEvent)
    assert unkeyed.emission is None
    state, _ = reduce(state, unkeyed)
    assert routed(state, "consumer") == ["a"]
