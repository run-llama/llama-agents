from __future__ import annotations

import pytest
from llama_agents.client.protocol.serializable_events import (
    EventEnvelope,
    EventValidationError,
)
from workflows.context.serializers import JsonSerializer
from workflows.events import Event, SerializableEvent


class NestedEvent(Event):
    nested: SerializableEvent


class ChildEvent(Event):
    pass


def test_registered_types_decode_nested_events() -> None:
    event = NestedEvent(nested=ChildEvent())
    envelope = EventEnvelope.from_event(event).model_dump()
    restored = EventEnvelope.parse(
        envelope,
        registry={"NestedEvent": NestedEvent},
        json_serializer=JsonSerializer(allowed_types=[NestedEvent, ChildEvent]),
    )
    assert isinstance(restored, NestedEvent)
    assert type(restored.nested) is ChildEvent


def test_unregistered_nested_events_are_rejected() -> None:
    envelope = EventEnvelope.from_event(NestedEvent(nested=ChildEvent())).model_dump()
    with pytest.raises(EventValidationError, match="Refusing to import"):
        EventEnvelope.parse(
            envelope,
            registry={"NestedEvent": NestedEvent},
            json_serializer=JsonSerializer(allowed_types=[NestedEvent]),
        )


def test_qualified_name_lookup_uses_registered_types() -> None:
    envelope = {
        "qualified_name": f"{ChildEvent.__module__}.{ChildEvent.__name__}",
        "value": {},
    }
    serializer = JsonSerializer(allowed_types=[ChildEvent])
    assert type(EventEnvelope.parse(envelope, json_serializer=serializer)) is ChildEvent
    with pytest.raises(EventValidationError, match="Refusing to import"):
        EventEnvelope.parse(
            envelope,
            json_serializer=JsonSerializer(allowed_types=[]),
        )
