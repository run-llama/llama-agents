# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
from http.client import HTTPException
from typing import Any, cast

import pytest
from pydantic import PrivateAttr
from workflows.context import JsonSerializer, PickleSerializer
from workflows.context.utils import import_module_from_qualified_name
from workflows.events import (
    Event,
    StopEvent,
    UnreconstructedException,
    WorkflowCancelledEvent,
    WorkflowFailedEvent,
    WorkflowTimedOutEvent,
)


class _TestEvent(Event):
    param: str
    _private_param_1: str = PrivateAttr()
    _private_param_2: str = PrivateAttr(default_factory=str)


class _TestEvent2(Event):
    """
    Custom Test Event.

    Private Attrs:
        _private_param: doesn't get modified during construction
        _modified_private_param: gets processed before being set
    """

    _private_param: int = PrivateAttr()
    _modified_private_param: int = PrivateAttr()

    def __init__(self, _modified_private_param: int, **params: Any):
        super().__init__(**params)
        self._modified_private_param = _modified_private_param * 2


def test_event_init_basic() -> None:
    evt = Event(a=1, b=2, c="c")

    assert evt.a == 1
    assert evt.b == 2
    assert evt.c == "c"
    assert evt["a"] == evt.a
    assert evt["b"] == evt.b
    assert evt["c"] == evt.c
    assert evt.keys() == {"a": 1, "b": 2, "c": "c"}.keys()


def test_custom_event_with_fields_and_private_params() -> None:
    evt = _TestEvent(a=1, param="test_param", _private_param_1="test_private_param_1")  # type: ignore

    assert evt.a == 1
    assert evt["a"] == evt.a
    assert evt.param == "test_param"
    assert evt._data == {"a": 1}
    assert evt._private_param_1 == "test_private_param_1"
    assert evt._private_param_2 == ""


def test_custom_event_override_init() -> None:
    evt = _TestEvent2(a=1, b=2, _private_param=2, _modified_private_param=2)

    assert evt.a == 1
    assert evt.b == 2
    assert evt._data == {"a": 1, "b": 2}
    assert evt._private_param == 2
    assert evt._modified_private_param == 4


def test_event_missing_key() -> None:
    ev = _TestEvent(param="bar")
    with pytest.raises(AttributeError):
        ev.wrong_key


def test_event_not_a_field() -> None:
    ev = _TestEvent(param="foo", not_a_field="bar")  # type: ignore
    assert ev._data["not_a_field"] == "bar"
    ev.not_a_field = "baz"
    assert ev._data["not_a_field"] == "baz"
    ev["not_a_field"] = "barbaz"
    assert ev._data["not_a_field"] == "barbaz"
    assert ev.get("not_a_field") == "barbaz"


def test_event_dict_api() -> None:
    ev = _TestEvent(param="foo")
    assert len(ev) == 0
    ev["a_new_key"] = "bar"
    assert len(ev) == 1
    assert list(ev.values()) == ["bar"]
    k, v = next(iter(ev.items()))
    assert k == "a_new_key"
    assert v == "bar"
    assert next(iter(ev)) == "a_new_key"
    assert ev.to_dict() == {"a_new_key": "bar"}


def test_event_serialization() -> None:
    ev = _TestEvent(param="foo", not_a_field="bar")  # type: ignore
    serializer = JsonSerializer()
    serialized_ev = serializer.serialize(ev)
    deseriazlied_ev = serializer.deserialize(serialized_ev)

    assert type(deseriazlied_ev).__name__ == type(ev).__name__
    deseriazlied_ev = cast(
        _TestEvent,
        deseriazlied_ev,
    )
    assert ev.param == deseriazlied_ev.param
    assert ev._data == deseriazlied_ev._data


def test_bool() -> None:
    assert bool(_TestEvent(param="foo")) is True


def test_stop_event_serialization() -> None:
    ev = StopEvent(result="foo")
    data_dict = ev.model_dump()
    assert data_dict == {"result": "foo"}

    serializer = JsonSerializer()
    serialized_ev = serializer.serialize(ev)
    deseriazlied_ev = serializer.deserialize(serialized_ev)

    assert type(deseriazlied_ev).__name__ == type(ev).__name__
    deseriazlied_ev = cast(
        StopEvent,
        deseriazlied_ev,
    )
    assert ev.result == deseriazlied_ev.result


class CustomStopEvent(StopEvent):
    foo: str
    bar: int


def test_custom_stop_event_serialization() -> None:
    ev = CustomStopEvent(foo="foo", bar=42)
    data_dict = ev.model_dump()
    assert data_dict == {"foo": "foo", "bar": 42}

    serializer = JsonSerializer()
    serialized_ev = serializer.serialize(ev)
    deserialized_ev = serializer.deserialize(serialized_ev)

    assert type(deserialized_ev).__name__ == type(ev).__name__
    deserialized_ev = cast(
        CustomStopEvent,
        deserialized_ev,
    )
    assert ev.foo == deserialized_ev.foo
    assert ev.bar == deserialized_ev.bar


@pytest.mark.parametrize("serializer_type", [JsonSerializer, PickleSerializer])
@pytest.mark.parametrize("event_type", [StopEvent, CustomStopEvent])
@pytest.mark.parametrize("result", [None, 0, "done"])
def test_stop_event_dynamic_fields_serialization(
    serializer_type: type[JsonSerializer],
    event_type: type[StopEvent],
    result: Any,
) -> None:
    ev = event_type(
        result=result,
        foo="foo",
        bar=42,
        document_id="doc-1",
        metadata={"items": [1, 2]},
    )
    assert ev.model_dump()["_data"]["document_id"] == "doc-1"
    assert event_type.model_validate_json(ev.model_dump_json()).document_id == "doc-1"
    serializer = serializer_type()
    restored = serializer.deserialize(serializer.serialize(ev))

    assert type(restored) is event_type
    assert restored.result == result
    assert restored.document_id == "doc-1"
    assert restored["metadata"] == {"items": [1, 2]}
    assert restored.foo == "foo"
    assert restored.bar == 42


def test_stop_event_repr() -> None:
    ev = StopEvent(foo="foo", result=42)
    assert repr(ev) == "StopEvent(foo='foo', result=42)"


def test_custom_stop_event_repr_no_result() -> None:
    ev = CustomStopEvent(foo="foo", bar=42)
    rep = repr(ev)
    assert rep == "CustomStopEvent(foo='foo', bar=42)"


# Tests for workflow termination event subclasses


def test_workflow_termination_events_are_stop_events() -> None:
    """Verify workflow termination events are subclasses of StopEvent."""
    assert issubclass(WorkflowTimedOutEvent, StopEvent)
    assert issubclass(WorkflowCancelledEvent, StopEvent)
    assert issubclass(WorkflowFailedEvent, StopEvent)


def test_workflow_timed_out_event() -> None:
    """Test WorkflowTimedOutEvent creation and attributes."""
    ev = WorkflowTimedOutEvent(timeout=30.0, active_steps=["step1", "step2"])
    assert ev.timeout == 30.0
    assert ev.active_steps == ["step1", "step2"]
    assert isinstance(ev, StopEvent)


def test_workflow_timed_out_event_empty_active_steps() -> None:
    """Test WorkflowTimedOutEvent with no active steps."""
    ev = WorkflowTimedOutEvent(timeout=5.0, active_steps=[])
    assert ev.timeout == 5.0
    assert ev.active_steps == []


def test_workflow_timed_out_event_serialization() -> None:
    """Test WorkflowTimedOutEvent serialization and deserialization."""
    ev = WorkflowTimedOutEvent(timeout=30.0, active_steps=["step1", "step2"])
    data_dict = ev.model_dump()
    assert data_dict == {"timeout": 30.0, "active_steps": ["step1", "step2"]}

    serializer = JsonSerializer()
    serialized_ev = serializer.serialize(ev)
    deserialized_ev = serializer.deserialize(serialized_ev)

    assert type(deserialized_ev).__name__ == type(ev).__name__
    deserialized_ev = cast(WorkflowTimedOutEvent, deserialized_ev)
    assert ev.timeout == deserialized_ev.timeout
    assert ev.active_steps == deserialized_ev.active_steps


def test_workflow_timed_out_event_repr() -> None:
    """Test WorkflowTimedOutEvent string representation."""
    ev = WorkflowTimedOutEvent(timeout=10.0, active_steps=["my_step"])
    rep = repr(ev)
    assert "WorkflowTimedOutEvent" in rep
    assert "timeout=10.0" in rep
    assert "active_steps=['my_step']" in rep


def test_workflow_cancelled_event() -> None:
    """Test WorkflowCancelledEvent creation."""
    ev = WorkflowCancelledEvent()
    assert isinstance(ev, StopEvent)


def test_workflow_cancelled_event_serialization() -> None:
    """Test WorkflowCancelledEvent serialization and deserialization."""
    ev = WorkflowCancelledEvent()
    data_dict = ev.model_dump()
    assert data_dict == {}

    serializer = JsonSerializer()
    serialized_ev = serializer.serialize(ev)
    deserialized_ev = serializer.deserialize(serialized_ev)

    assert type(deserialized_ev).__name__ == type(ev).__name__


def test_workflow_cancelled_event_repr() -> None:
    """Test WorkflowCancelledEvent string representation."""
    ev = WorkflowCancelledEvent()
    rep = repr(ev)
    assert rep == "WorkflowCancelledEvent()"


def test_workflow_failed_event() -> None:
    """Test WorkflowFailedEvent creation and attributes."""
    ev = WorkflowFailedEvent(
        step_name="my_step",
        exception=ValueError("Something went wrong"),
        attempts=3,
        elapsed_seconds=1.5,
    )
    assert ev.step_name == "my_step"
    assert isinstance(ev.exception, ValueError)
    assert str(ev.exception) == "Something went wrong"
    assert ev.attempts == 3
    assert ev.elapsed_seconds == 1.5
    assert isinstance(ev, StopEvent)


def test_workflow_failed_event_serialization() -> None:
    """Test WorkflowFailedEvent serialization and deserialization."""
    ev = WorkflowFailedEvent(
        step_name="failing_step",
        exception=RuntimeError("Test failure"),
        attempts=2,
        elapsed_seconds=0.5,
    )
    data_dict = ev.model_dump()
    assert data_dict == {
        "step_name": "failing_step",
        "exception": {
            "exception_type": "builtins.RuntimeError",
            "exception_message": "Test failure",
        },
        "attempts": 2,
        "elapsed_seconds": 0.5,
    }

    serializer = JsonSerializer()
    serialized_ev = serializer.serialize(ev)
    deserialized_ev = serializer.deserialize(serialized_ev)

    assert type(deserialized_ev).__name__ == type(ev).__name__
    deserialized_ev = cast(WorkflowFailedEvent, deserialized_ev)
    assert ev.step_name == deserialized_ev.step_name
    assert type(ev.exception) is type(deserialized_ev.exception)
    assert str(ev.exception) == str(deserialized_ev.exception)
    assert ev.attempts == deserialized_ev.attempts
    assert ev.elapsed_seconds == deserialized_ev.elapsed_seconds


def test_workflow_failed_event_repr() -> None:
    """Test WorkflowFailedEvent string representation."""
    ev = WorkflowFailedEvent(
        step_name="my_step",
        exception=ValueError("error msg"),
        attempts=1,
        elapsed_seconds=0.1,
    )
    rep = repr(ev)
    assert "WorkflowFailedEvent" in rep
    assert "step_name='my_step'" in rep
    assert "error msg" in rep


def test_workflow_failed_event_with_nested_exception_type() -> None:
    """Test WorkflowFailedEvent with a qualified exception type name."""
    ev = WorkflowFailedEvent(
        step_name="api_step",
        exception=HTTPException("Connection refused"),
        attempts=5,
        elapsed_seconds=10.0,
    )
    assert isinstance(ev.exception, HTTPException)
    assert ev.attempts == 5
    assert ev.elapsed_seconds == 10.0


def _failed_event(exception: Exception) -> WorkflowFailedEvent:
    return WorkflowFailedEvent(
        step_name="step",
        exception=exception,
        attempts=1,
        elapsed_seconds=0.1,
    )


def _roundtrip(
    ev: WorkflowFailedEvent, serializer: JsonSerializer | None = None
) -> WorkflowFailedEvent:
    serializer = serializer or JsonSerializer()
    return cast(WorkflowFailedEvent, serializer.deserialize(serializer.serialize(ev)))


def test_message_only_exception_roundtrips_by_type() -> None:
    """Importable single-arg exceptions still reconstruct as their real type."""
    restored = _roundtrip(_failed_event(HTTPException("Connection refused")))
    assert type(restored.exception) is HTTPException
    assert str(restored.exception) == "Connection refused"


def test_multi_arg_ctor_exception_degrades_to_breadcrumb() -> None:
    """An importable exception whose ctor rejects ``cls(message)`` degrades
    instead of crashing the reload (the old uncaught-TypeError path)."""
    exc = json.JSONDecodeError("Expecting value", "doc", 0)
    restored = _roundtrip(_failed_event(exc))
    assert isinstance(restored.exception, UnreconstructedException)
    assert restored.exception.original_type == "json.decoder.JSONDecodeError"
    assert str(restored.exception) == str(exc)


def test_unimportable_exception_degrades_to_breadcrumb() -> None:
    """A type that cannot be re-imported (``<locals>`` qualname) degrades to the
    breadcrumb type rather than a bare ``Exception``."""

    def make_local_exception() -> type[Exception]:
        class LocalError(Exception):
            pass

        return LocalError

    local_exc = make_local_exception()("boom")
    restored = _roundtrip(_failed_event(local_exc))
    assert isinstance(restored.exception, UnreconstructedException)
    assert restored.exception.original_type is not None
    assert "LocalError" in restored.exception.original_type
    assert str(restored.exception) == "boom"


def test_unreconstructed_exception_reserializes_without_double_wrapping() -> None:
    """Re-serializing a degraded exception keeps the message intact; the
    breadcrumb type itself reconstructs cleanly with ``original_type`` reset."""
    serializer = JsonSerializer()
    once = _roundtrip(_failed_event(json.JSONDecodeError("Expecting value", "doc", 0)))
    assert isinstance(once.exception, UnreconstructedException)

    twice = _roundtrip(once, serializer)
    assert isinstance(twice.exception, UnreconstructedException)
    assert str(twice.exception) == str(once.exception)
    # serialize keeps only type + message, so original_type is dropped on re-roundtrip
    assert twice.exception.original_type is None


def test_unreconstructed_exception_reserializes_under_active_allowlist() -> None:
    """The breadcrumb type itself is always permitted, so a degraded exception
    round-trips stably even under an allowlist that does not list it (rather than
    degrading into a self-referential breadcrumb)."""
    serializer = JsonSerializer(allowed_types=[WorkflowFailedEvent])
    once = _roundtrip(
        _failed_event(json.JSONDecodeError("Expecting value", "doc", 0)), serializer
    )
    assert isinstance(once.exception, UnreconstructedException)

    twice = _roundtrip(once, serializer)
    assert isinstance(twice.exception, UnreconstructedException)
    assert str(twice.exception) == str(once.exception)
    assert twice.exception.original_type is None


def test_allowlist_set_still_reconstructs_builtin_exceptions() -> None:
    """builtins are exempt from the allowlist and reconstruct as their real type."""
    serializer = JsonSerializer(allowed_types=[WorkflowFailedEvent])
    restored = _roundtrip(_failed_event(ValueError("bad value")), serializer)
    assert type(restored.exception) is ValueError
    assert str(restored.exception) == "bad value"


def test_no_allowlist_reconstructs_non_builtin_exception() -> None:
    """With no allowlist, non-builtin exception reconstruction stays permissive."""
    restored = _roundtrip(_failed_event(HTTPException("Connection refused")))
    assert type(restored.exception) is HTTPException


def test_disallowed_non_builtin_exception_degrades_without_importing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-builtin exception outside the allowlist degrades and is never
    imported — the allowlist is a real boundary for exception types."""
    serializer = JsonSerializer(allowed_types=[WorkflowFailedEvent])
    blob = serializer.serialize(_failed_event(HTTPException("nope")))

    imported: list[str] = []

    def spy(name: str) -> Any:
        imported.append(name)
        return import_module_from_qualified_name(name)

    monkeypatch.setattr("workflows.events.import_module_from_qualified_name", spy)

    restored = cast(WorkflowFailedEvent, serializer.deserialize(blob))
    assert isinstance(restored.exception, UnreconstructedException)
    assert restored.exception.original_type == "http.client.HTTPException"
    assert str(restored.exception) == "nope"
    assert "http.client.HTTPException" not in imported


def test_non_exception_type_is_never_called() -> None:
    """A serialized blob naming a non-exception builtin (e.g. ``builtins.eval``)
    must not be invoked — reconstruction only ever constructs real exceptions."""
    blob = json.dumps(
        {
            "__is_pydantic": True,
            "qualified_name": "workflows.events.WorkflowFailedEvent",
            "value": {
                "step_name": "x",
                "exception": {
                    "exception_type": "builtins.eval",
                    "exception_message": "1 + 1",
                },
                "attempts": 1,
                "elapsed_seconds": 0.1,
            },
        }
    )
    restored = cast(WorkflowFailedEvent, JsonSerializer().deserialize(blob))
    assert isinstance(restored.exception, UnreconstructedException)
    assert restored.exception.original_type == "builtins.eval"
    assert str(restored.exception) == "1 + 1"
