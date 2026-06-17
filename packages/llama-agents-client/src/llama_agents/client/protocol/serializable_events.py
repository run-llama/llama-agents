# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import builtins
import json
from typing import Any

from pydantic import BaseModel, ValidationError, model_serializer, model_validator
from workflows.context.utils import import_module_from_qualified_name
from workflows.events import (
    Event,
    _set_event_origin_namespace,
    get_event_origin_namespace,
)


class EventEnvelopeWithMetadata(BaseModel):
    """
    Client readable representation of an Event. Includes class metadata in order to support
    matching event types semantically in an extendable manner (e.g. "StartEvent", "StopEvent", etc.).
    """

    value: dict[str, Any]

    # deprecated, use type instead
    qualified_name: str | None

    # New metadata
    type: str
    types: list[str] | None
    origin_namespace: tuple[str, ...] = ()

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        if not self.origin_namespace:
            data.pop("origin_namespace", None)
        return data

    def load_event(self, registry: list[type[Event]] = []) -> Event:
        """
        Attempts to load the event data as a python class based on the envelope metadata.
        Looks up the event from the registry, if provided. Falls back to the qualified_name, attempting to load from the module path.
        """
        registry_lookup = {e.__name__: e for e in registry}
        as_event_envelope = EventEnvelope(
            value=self.value, type=self.type, qualified_name=self.qualified_name
        ).model_dump()
        event = EventEnvelope.parse(
            client_data=as_event_envelope, registry=registry_lookup
        )
        if self.origin_namespace:
            _set_event_origin_namespace(event, self.origin_namespace)
        return event

    @classmethod
    def from_event(
        cls, event: Event, include_qualified_name: bool = True
    ) -> EventEnvelopeWithMetadata:
        """
        Build a backward-compatible envelope for an Event, preserving existing
        fields (e.g., qualified_name, value) while adding metadata useful for
        type-safe clients.

        """
        # Start with the existing JSON-serializable structure
        value = event.model_dump(mode="json")

        envelope = EventEnvelopeWithMetadata(
            value=value,
            qualified_name=_get_qualified_name(type(event))
            if include_qualified_name
            else None,
            types=_get_event_subtypes(type(event)),
            type=type(event).__name__,
            origin_namespace=get_event_origin_namespace(event),
        )
        return envelope


class EventEnvelope(BaseModel):
    """
    Client write representation of an Event. Simpler than the server provided EventEnvelopeWithMetadata, as the metadata can be inferred based on looking up the runtime type
    """

    value: Any | None
    type: str | None = None
    qualified_name: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _format_compatibility(cls, data: Any) -> Any:
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                pass
        if isinstance(data, dict):
            if "value" not in data and "data" in data:
                # Preserve other keys while defaulting "value" from legacy "data"
                data = {**data, "value": data["data"]}
        return data

    @classmethod
    def from_event(cls, event: Event) -> EventEnvelope:
        return cls(
            value=event.model_dump(mode="json"),
            type=type(event).__name__,
        )

    @classmethod
    def parse(
        cls,
        client_data: dict[str, Any] | str,
        registry: dict[str, builtins.type[Event]] | None = None,
        explicit_event: builtins.type[Event] | None = None,
    ) -> Event:
        """
        Parse client data into an Event. Raises an EventValidationError if the client data is invalid.

        Args:
            client_data: The client data to parse. Can be a dictionary, a string, or an explicit Event class.
            registry: The registry of event type names to Event classes.
            explicit_event: An explicit Event class to treat the dict as

        Returns:
            The parsed Event.
        """
        registry = registry or {}
        errors: list[str] = []
        try:
            as_dict = (
                json.loads(client_data) if isinstance(client_data, str) else client_data
            )
        except json.JSONDecodeError:
            as_dict = client_data
        if not isinstance(as_dict, dict):
            raise EventValidationError(
                "Failed to deserialize event. Must be a json object, or stringified json object"
            )
        missing_qualifiers = (
            "qualified_name" not in as_dict or "type" not in as_dict
        ) and "value" not in as_dict
        if missing_qualifiers and explicit_event:
            if explicit_event.__name__ not in registry:
                registry = {**registry, explicit_event.__name__: explicit_event}
            as_dict = {
                "type": explicit_event.__name__,
                "value": as_dict,
            }
        try:
            event = EventEnvelope.model_validate(as_dict)

            if event.type:
                if event.type not in registry:
                    errors.append(
                        f"Invalid event type: {event.type}. Expected one of {', '.join(registry.keys())}"
                    )
                else:
                    return registry[event.type].model_validate(event.value)
            if event.qualified_name:
                module_class = import_module_from_qualified_name(event.qualified_name)
                if not issubclass(module_class, Event):
                    errors.append(
                        f"Invalid client data. Qualified name {event.qualified_name} does not correspond to an Event subclass"
                    )
                else:
                    return module_class.model_validate(event.value)
        except ValidationError as e:
            errors.append(f"Failed to deserialize event: {str(e)}")
        errors = (
            errors
            if errors
            else [
                "Invalid client data. Must have a type or a qualified name, got {event}"
            ]
        )
        raise EventValidationError(" ".join(errors))


def _get_event_subtypes(cls: type[Event]) -> list[str] | None:
    """
    Traverses the MRO (Module Resolution Order) of a class and returns the list of only Event subclasses.
    """
    names: list[str] = []
    # Skip the class itself by starting from the second MRO entry
    for c in cls.mro()[1:]:
        if c is Event:
            break
        if issubclass(c, Event):
            names.append(c.__name__)
    if not names:
        return None
    return names


def _get_qualified_name(event: type[Event]) -> str:
    return f"{event.__module__}.{event.__name__}"


class EventValidationError(Exception):
    """Raised when the client data is invalid."""
