# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
import time

import pytest
from pydantic import TypeAdapter
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
    _deserialize_event,
    _deserialize_event_type,
    _deserialize_exception,
    _serialize_event,
    _serialize_event_type,
    _serialize_exception,
)
from workflows.runtime.types.results import (
    AddCollectedEvent,
    AddWaiter,
    DeleteCollectedEvent,
    DeleteWaiter,
    StepWorkerFailed,
    StepWorkerResult,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickPublishEvent,
    TickStepResult,
    TickTimeout,
    WorkflowTick,
    WorkflowTickAdapter,
)


class MyEvent(Event):
    value: str = "hello"


# -- Serialization helper roundtrip tests --


def test_event_roundtrip() -> None:
    event = MyEvent(value="world")
    serialized = _serialize_event(event)
    result = _deserialize_event(serialized)
    assert isinstance(result, MyEvent)
    assert result.value == "world"


def test_exception_roundtrip() -> None:
    exc = ValueError("something went wrong")
    serialized = _serialize_exception(exc)
    result = _deserialize_exception(serialized)
    assert isinstance(result, ValueError)
    assert str(result) == "something went wrong"


def test_exception_roundtrip_unimportable() -> None:
    CustomError = type("CustomError", (Exception,), {})
    exc = CustomError("oops")
    serialized = _serialize_exception(exc)
    result = _deserialize_exception(serialized)
    assert type(result) is Exception
    assert str(result) == "oops"


def test_event_type_roundtrip() -> None:
    serialized = _serialize_event_type(MyEvent)
    result = _deserialize_event_type(serialized)
    assert result is MyEvent


# -- Tick roundtrip tests --


@pytest.mark.parametrize(
    "tick",
    [
        pytest.param(
            TickAddEvent(
                event=StartEvent(),
                step_id=StepId.root("my_step"),
                attempts=3,
                first_attempt_at=1234567890.0,
            ),
            id="add_event",
        ),
        pytest.param(
            TickPublishEvent(event=MyEvent(value="world")),
            id="publish_event",
        ),
        pytest.param(
            TickCancelRun(),
            id="cancel_run",
        ),
        pytest.param(
            TickTimeout(timeout=30.5),
            id="timeout",
        ),
        pytest.param(
            TickStepResult(
                step_id=StepId.root("process"),
                worker_id=42,
                event=MyEvent(value="trigger"),
                result=[StepWorkerResult(result=StopEvent(result="done"))],
            ),
            id="step_result_with_event",
        ),
        pytest.param(
            TickStepResult(
                step_id=StepId.root("process"),
                worker_id=1,
                event=StartEvent(),
                result=[StepWorkerResult(result=None)],
            ),
            id="step_result_with_none",
        ),
        pytest.param(
            TickStepResult(
                step_id=StepId.root("collector"),
                worker_id=2,
                event=StartEvent(),
                result=[
                    AddCollectedEvent(
                        event_id="evt-1", event=MyEvent(value="collected")
                    )
                ],
            ),
            id="step_result_add_collected_event",
        ),
        pytest.param(
            TickStepResult(
                step_id=StepId.root("collector"),
                worker_id=3,
                event=StartEvent(),
                result=[DeleteCollectedEvent(event_id="evt-2")],
            ),
            id="step_result_delete_collected_event",
        ),
        pytest.param(
            TickStepResult(
                step_id=StepId.root("cleanup"),
                worker_id=5,
                event=StartEvent(),
                result=[DeleteWaiter(waiter_id="w-2")],
            ),
            id="step_result_delete_waiter",
        ),
    ],
)
def test_tick_roundtrip(tick: WorkflowTick) -> None:
    serialized = tick.model_dump(mode="json")
    roundtripped = json.loads(json.dumps(serialized))
    result = type(tick).model_validate(roundtripped)
    assert result == tick


# -- Tick roundtrip tests with lossy serialization --


def test_tick_step_result_with_failed_value_error() -> None:
    failed_at = time.time()
    tick = TickStepResult(
        step_id=StepId.root("broken_step"),
        worker_id=7,
        event=StartEvent(),
        result=[
            StepWorkerFailed(
                exception=ValueError("something went wrong"), failed_at=failed_at
            )
        ],
    )
    serialized = tick.model_dump(mode="json")
    roundtripped = json.loads(json.dumps(serialized))
    result = TickStepResult.model_validate(roundtripped)

    assert isinstance(result, TickStepResult)
    r = result.result[0]
    assert isinstance(r, StepWorkerFailed)
    assert isinstance(r.exception, ValueError)
    assert str(r.exception) == "something went wrong"
    assert r.failed_at == failed_at


def test_tick_step_result_with_failed_unimportable_exception() -> None:
    CustomError = type("CustomError", (Exception,), {})
    failed_at = time.time()
    tick = TickStepResult(
        step_id=StepId.root("broken_step"),
        worker_id=8,
        event=StartEvent(),
        result=[StepWorkerFailed(exception=CustomError("oops"), failed_at=failed_at)],
    )
    serialized = tick.model_dump(mode="json")
    roundtripped = json.loads(json.dumps(serialized))
    result = TickStepResult.model_validate(roundtripped)

    assert isinstance(result, TickStepResult)
    r = result.result[0]
    assert isinstance(r, StepWorkerFailed)
    assert type(r.exception) is Exception
    assert str(r.exception) == "oops"
    assert r.failed_at == failed_at


def test_tick_step_result_with_add_waiter() -> None:
    tick = TickStepResult(
        step_id=StepId.root("waiter_step"),
        worker_id=4,
        event=StartEvent(),
        result=[
            AddWaiter(
                waiter_id="w-1",
                waiter_event=MyEvent(value="waiting"),
                requirements={"key": "value"},
                timeout=60.0,
                event_type=MyEvent,
            )
        ],
    )
    serialized = tick.model_dump(mode="json")

    # Verify the serialized form captures has_requirements correctly
    waiter_data = serialized["result"][0]
    assert waiter_data["has_requirements"] is True
    assert waiter_data["requirements"] == {}

    roundtripped = json.loads(json.dumps(serialized))
    result = TickStepResult.model_validate(roundtripped)

    assert isinstance(result, TickStepResult)
    r = result.result[0]
    assert isinstance(r, AddWaiter)
    assert r.waiter_id == "w-1"
    assert isinstance(r.waiter_event, MyEvent)
    assert r.waiter_event.value == "waiting"
    # Requirements are always {} after deserialization
    assert r.requirements == {}
    assert r.timeout == 60.0
    assert r.event_type is MyEvent


# -- WorkflowTick discriminated union tests --


def test_workflow_tick_discriminated_union_roundtrip() -> None:
    """Verify that WorkflowTick TypeAdapter can roundtrip all tick types."""
    adapter = TypeAdapter(WorkflowTick)

    ticks = [
        TickAddEvent(event=StartEvent(), step_id=StepId.root("s")),
        TickPublishEvent(event=MyEvent(value="x")),
        TickCancelRun(),
        TickTimeout(timeout=10.0),
        TickStepResult(
            step_id=StepId.root("s"),
            worker_id=0,
            event=StartEvent(),
            result=[StepWorkerResult(result=None)],
        ),
    ]
    for tick in ticks:
        dumped = adapter.dump_python(tick, mode="json")
        roundtripped = json.loads(json.dumps(dumped))
        restored = adapter.validate_python(roundtripped)
        assert type(restored) is type(tick)


def test_legacy_step_name_key_deserializes_to_step_id() -> None:
    """Pre-StepId journals used a ``step_name`` key; it must still decode.

    The wire format now serializes a root step id as the bare name under the
    ``step_id`` key. The ``step_name`` alias keeps old journals readable: a
    legacy dict deserializes to a tick whose ``step_id`` is the root StepId.
    """
    legacy = {
        "type": "step_result",
        "step_name": "foo",
        "worker_id": 0,
        "event": _serialize_event(StartEvent()),
        "result": [],
    }
    restored = WorkflowTickAdapter.validate_python(legacy)
    assert isinstance(restored, TickStepResult)
    assert restored.step_id == StepId.root("foo")
