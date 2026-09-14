# ty: ignore[invalid-argument-type]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.


import json

import pytest
from llama_agents.client.protocol.serializable_events import (
    EventEnvelope,
    EventEnvelopeWithMetadata,
    EventValidationError,
)
from workflows.events import (
    Event,
    StepState,
    StepStateChanged,
    StopEvent,
)


def test_envelope_user_defined_event() -> None:
    class MyEvent(Event):
        x: int

    ev = MyEvent(x=1)
    env = EventEnvelopeWithMetadata.from_event(ev).model_dump()

    assert isinstance(env.get("value", {}), dict)
    types = env.get("types")
    assert types is None
    # User-defined event
    assert env.get("type", "") == "MyEvent"


def test_envelope_builtin_stop_event() -> None:
    ev = StopEvent()
    env = EventEnvelopeWithMetadata.from_event(ev).model_dump()

    assert isinstance(env.get("value", {}), dict)
    types = env.get("types")
    assert types is None
    assert env.get("type", "") == "StopEvent"


def test_envelope_stop_event_subclass() -> None:
    class MyStop(StopEvent):
        pass

    ev = MyStop()
    env = EventEnvelopeWithMetadata.from_event(ev).model_dump()

    assert isinstance(env.get("value", {}), dict)
    # Subclass is user-defined
    assert env.get("type", "") == "MyStop"
    # Must include base StopEvent in MRO
    types = env.get("types")
    assert types is not None
    assert "StopEvent" in types


def test_envelope_internal_event() -> None:
    ev = StepStateChanged(
        name="s",
        step_state=StepState.PREPARING,
        worker_id="w1",
        input_event_name="X",
    )
    env = EventEnvelopeWithMetadata.from_event(ev).model_dump()

    assert isinstance(env.get("value", {}), dict)
    assert env.get("type", "") == "StepStateChanged"
    # Internal event types contains specific class and base Event
    types = env.get("types")
    assert types is not None
    assert "InternalDispatchEvent" in types


# Module-scope events for qualified_name import tests
class ModuleScopeEvent(Event):
    x: int


class ModuleScopeOtherEvent(Event):
    y: int


def test_parse_with_registry_type_success() -> None:
    class MyEvent(Event):
        x: int

    payload = {"type": "MyEvent", "value": {"x": 1}}
    ev = EventEnvelope.parse(client_data=payload, registry={"MyEvent": MyEvent})
    assert isinstance(ev, MyEvent)
    assert ev.x == 1


def test_parse_with_qualified_name_fallback_success() -> None:
    qn = f"{ModuleScopeEvent.__module__}.{ModuleScopeEvent.__name__}"
    payload = {"qualified_name": qn, "value": {"x": 7}}
    ev = EventEnvelope.parse(client_data=payload)
    assert isinstance(ev, ModuleScopeEvent)
    assert ev.x == 7


def test_parse_with_type_unknown_but_qualified_name_valid() -> None:
    qn = f"{ModuleScopeOtherEvent.__module__}.{ModuleScopeOtherEvent.__name__}"
    payload = {"type": "NotInRegistry", "qualified_name": qn, "value": {"y": 3}}
    ev = EventEnvelope.parse(client_data=payload, registry={})
    assert isinstance(ev, ModuleScopeOtherEvent)
    assert ev.y == 3


def test_parse_alias_data_to_value() -> None:
    class MyEvent(Event):
        x: int

    payload = {"type": "MyEvent", "data": {"x": 9}}
    ev = EventEnvelope.parse(client_data=payload, registry={"MyEvent": MyEvent})
    assert isinstance(ev, MyEvent)
    assert ev.x == 9


def test_parse_from_json_string() -> None:
    class MyEvent(Event):
        x: int

    obj = {"type": "MyEvent", "value": {"x": 11}}
    ev = EventEnvelope.parse(client_data=json.dumps(obj), registry={"MyEvent": MyEvent})
    assert isinstance(ev, MyEvent)
    assert ev.x == 11


def test_parse_value_only_with_explicit_event() -> None:
    class MyStart(Event):
        foo: str

    payload = {"foo": "bar"}
    ev = EventEnvelope.parse(client_data=payload, explicit_event=MyStart)
    assert isinstance(ev, MyStart)
    assert ev.foo == "bar"


def test_parse_invalid_inputs_raise() -> None:
    with pytest.raises(EventValidationError) as e:
        EventEnvelope.parse(client_data=123)  # type: ignore[arg-type]
    assert "Failed to deserialize event" in str(e)


def test_from_event_roundtrip_with_registry() -> None:
    class MyEv(Event):
        a: int

    original = MyEv(a=5)
    env = EventEnvelope.from_event(original).model_dump()
    parsed = EventEnvelope.parse(client_data=env, registry={"MyEv": MyEv})
    assert isinstance(parsed, MyEv)
    assert parsed.a == 5


def test_metadata_envelope_load_event_with_registry() -> None:
    class MyMeta(Event):
        z: int

    ev = MyMeta(z=42)
    env = EventEnvelopeWithMetadata.from_event(ev)
    loaded = env.load_event([MyMeta])
    assert isinstance(loaded, MyMeta)
    assert loaded.z == 42


def test_metadata_envelope_qualified_name_toggle() -> None:
    class MyMetaQ(Event):
        q: int

    ev = MyMetaQ(q=1)
    with_qn = EventEnvelopeWithMetadata.from_event(ev, include_qualified_name=True)
    assert with_qn.qualified_name is not None

    without_qn = EventEnvelopeWithMetadata.from_event(ev, include_qualified_name=False)
    assert without_qn.qualified_name is None


def test_json_serializer_back_compat_with_pydantic_flag() -> None:
    qn = f"{ModuleScopeEvent.__module__}.{ModuleScopeEvent.__name__}"
    payload = {
        "__is_pydantic": True,  # ignored if present
        "qualified_name": qn,
        "value": {"x": 123},
    }
    ev = EventEnvelope.parse(client_data=payload)
    assert isinstance(ev, ModuleScopeEvent)
    assert ev.x == 123


def test_missing_type_and_qualified_name_raises() -> None:
    with pytest.raises(EventValidationError) as e:
        EventEnvelope.parse(client_data={"x": 1})
    assert "Failed to deserialize event" in str(e)
