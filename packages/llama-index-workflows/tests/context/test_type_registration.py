# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
from typing import Annotated, Any

import pytest
from pydantic import BaseModel, TypeAdapter
from workflows.context.serializers import JsonSerializer, PickleSerializer
from workflows.events import (
    Event,
    HumanResponseEvent,
    SerializableEvent,
    SerializableEventType,
    SerializableOptionalEvent,
    UnreconstructedException,
    WorkflowFailedEvent,
)


class Payload(BaseModel):
    value: int


class NestedEvent(Event):
    event: SerializableEvent
    optional_event: SerializableOptionalEvent = None
    event_type: SerializableEventType


class LocalError(Exception):
    pass


class Component:
    def class_name(self) -> str:
        return "Component"

    def to_dict(self) -> dict[str, int]:
        return {"value": 1}

    @classmethod
    def from_dict(cls, data: dict[str, int]) -> Component:
        assert data == {"value": 1}
        return cls()


def qualified(cls: type[Any]) -> str:
    return f"{cls.__module__}.{cls.__name__}"


@pytest.fixture
def forbid_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(name: str) -> Any:
        pytest.fail(f"Unexpected dynamic import: {name}")

    monkeypatch.setattr("workflows.context.utils.import_module", fail)


def registered(*types: type[Any]) -> JsonSerializer:
    return JsonSerializer(allowed_types=list(types))


def test_framework_event_resolves_without_registration() -> None:
    serializer = JsonSerializer(allowed_types=[])
    assert (
        serializer.resolve_class("workflows.events.HumanResponseEvent")
        is HumanResponseEvent
    )


def test_registered_classes_roundtrip_without_imports(forbid_imports: None) -> None:
    class State(BaseModel):
        payloads: list[Payload | None]

    serializer = registered(State, Payload)
    value = State(payloads=[Payload(value=4), None])
    assert serializer.deserialize(serializer.serialize(value)) == value
    assert serializer.resolve_class(qualified(State)) is State


def test_declared_model_fields_need_no_registration(forbid_imports: None) -> None:
    class State(BaseModel):
        payload: Payload

    serializer = registered(State)
    value = State(payload=Payload(value=7))
    assert serializer.deserialize(serializer.serialize(value)) == value


def test_qualname_and_legacy_names_both_resolve() -> None:
    class Nested(BaseModel):
        pass

    serializer = registered(Nested)
    assert "<locals>" in Nested.__qualname__
    assert serializer.resolve_class(f"{Nested.__module__}.{Nested.__qualname__}") is (
        Nested
    )
    assert serializer.resolve_class(qualified(Nested)) is Nested


def test_colliding_wire_names_raise() -> None:
    def make_state() -> type[BaseModel]:
        class Duplicate(BaseModel):
            pass

        return Duplicate

    first, second = make_state(), make_state()
    with pytest.raises(ValueError, match="claim the serialized name"):
        JsonSerializer(allowed_types=[first, second])
    # The same class listed twice stays fine.
    assert registered(first, first).resolve_class(qualified(first)) is first


@pytest.mark.parametrize("kind", ["__is_pydantic", "__is_component"])
def test_unregistered_names_do_not_resolve(kind: str, forbid_imports: None) -> None:
    serializer = registered()
    with pytest.raises(ValueError, match="not in the serializer's allowed types"):
        serializer.deserialize(
            json.dumps({kind: True, "qualified_name": "unknown.Type", "value": {}})
        )


def test_serialization_does_not_register_types(forbid_imports: None) -> None:
    serializer = registered()
    payload = serializer.serialize(Payload(value=1))
    with pytest.raises(ValueError, match="not in the serializer's allowed types"):
        serializer.deserialize(payload)


def test_string_entries_only_restrict_names() -> None:
    permissive = JsonSerializer(allowed_types=[qualified(Payload)])
    payload = Payload(value=2)
    assert permissive.deserialize(permissive.serialize(payload)) == payload

    with pytest.raises(ValueError, match="not in the serializer's allowed types"):
        permissive.deserialize(JsonSerializer().serialize(Event()))


def test_released_defaults_keep_dynamic_lookup() -> None:
    for serializer in (
        JsonSerializer(),
        JsonSerializer(allowed_types=[Payload]),
        JsonSerializer(allowed_types=[qualified(Payload)]),
    ):
        payload = Payload(value=3)
        assert serializer.deserialize(serializer.serialize(payload)) == payload


def test_component_roundtrip_without_imports(forbid_imports: None) -> None:
    serializer = registered(Component)
    assert isinstance(
        serializer.deserialize(serializer.serialize(Component())), Component
    )


@pytest.mark.parametrize("field", ["event", "optional_event", "event_type"])
def test_nested_event_fields_require_event_types(
    field: str, forbid_imports: None
) -> None:
    class Allowed(Event):
        pass

    serializer = registered(NestedEvent, Allowed, Payload)
    event = NestedEvent(event=Allowed(), optional_event=Allowed(), event_type=Allowed)
    payload = serializer.serialize(event)
    restored = serializer.deserialize(payload)
    assert type(restored.event) is Allowed
    assert type(restored.optional_event) is Allowed
    assert restored.event_type is Allowed

    data = json.loads(payload)
    data["value"][field] = (
        qualified(Payload)
        if field == "event_type"
        else JsonSerializer().serialize_value(Payload(value=9))
    )
    with pytest.raises(ValueError, match="must resolve to an Event"):
        serializer.deserialize(json.dumps(data))


def test_component_from_dict_uses_the_registry(forbid_imports: None) -> None:
    class ComponentWithEvent:
        def class_name(self) -> str:
            return "ComponentWithEvent"

        def to_dict(self) -> dict[str, Any]:
            return {"event": JsonSerializer().serialize_value(Event())}

        @classmethod
        def from_dict(cls, data: dict[str, Any]) -> ComponentWithEvent:
            TypeAdapter(SerializableEvent).validate_python(data["event"])
            return cls()

    serializer = registered(ComponentWithEvent, Event)
    payload = serializer.serialize(ComponentWithEvent())
    assert isinstance(serializer.deserialize(payload), ComponentWithEvent)

    restricted = registered(ComponentWithEvent)
    with pytest.raises(ValueError, match="not in the serializer's allowed types"):
        restricted.deserialize(payload)


def test_registered_exceptions_reconstruct_and_others_degrade(
    forbid_imports: None,
) -> None:
    serializer = registered(WorkflowFailedEvent, LocalError)
    event = WorkflowFailedEvent(step_name="step", exception=LocalError("failed"))
    payload = serializer.serialize(event)
    assert type(serializer.deserialize(payload).exception) is LocalError

    restricted = registered(WorkflowFailedEvent)
    restored = restricted.deserialize(payload)
    assert isinstance(restored.exception, UnreconstructedException)
    assert restored.exception.original_type == qualified(LocalError)


@pytest.mark.parametrize(
    "exception", [ValueError("bad"), UnreconstructedException("bad")]
)
def test_builtin_and_placeholder_exceptions_need_no_imports(
    exception: Exception, forbid_imports: None
) -> None:
    serializer = registered(WorkflowFailedEvent)
    event = WorkflowFailedEvent(step_name="step", exception=exception)
    assert type(serializer.deserialize(serializer.serialize(event)).exception) is type(
        exception
    )


def test_active_serializer_is_restored_after_error() -> None:
    serializer = registered(NestedEvent)
    event = NestedEvent(event=Event(), event_type=WorkflowFailedEvent)
    with pytest.raises(ValueError, match="not in the serializer's allowed types"):
        serializer.deserialize(serializer.serialize(event))
    restored = NestedEvent.model_validate(
        JsonSerializer().serialize_value(event)["value"]
    )
    assert restored.event_type is WorkflowFailedEvent


def test_pickle_serializer_roundtrips() -> None:
    serializer = PickleSerializer()
    value = {1, 2}
    assert serializer.deserialize(serializer.serialize(value)) == value


@pytest.mark.parametrize(
    "annotation", [Any, list[Payload], Annotated[Payload, "metadata"]]
)
def test_registration_raises_on_annotations(annotation: Any) -> None:
    with pytest.raises(TypeError, match="concrete classes"):
        JsonSerializer(allowed_types=[annotation])
