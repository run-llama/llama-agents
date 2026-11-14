# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from typing import Any

import pytest
from pydantic import (
    AliasChoices,
    AliasPath,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
)
from pydantic.alias_generators import to_camel
from workflows import Workflow, step
from workflows.context import JsonSerializer
from workflows.events import Event, StartEvent, StopEvent


class AliasedEvent(Event):
    count: int = Field(default=0, ge=0, alias="itemCount")
    _trace: str = PrivateAttr(default="default")


class AliasedStartEvent(StartEvent):
    document_id: str = Field(default="DEFAULT", alias="documentId")


class EchoAliasedInput(Workflow):
    @step
    async def echo(self, ev: AliasedStartEvent) -> StopEvent:
        return StopEvent(result=ev.document_id)


def test_alias_preserves_fields_private_attributes_and_dynamic_data() -> None:
    params: dict[str, Any] = {"itemCount": 3, "extra": "kept", "_trace": "provided"}
    event = AliasedEvent(**params)

    assert event.count == 3
    assert event._trace == "provided"
    assert event.to_dict() == {"extra": "kept"}
    assert event["extra"] == "kept"


def test_required_field_accepts_alias() -> None:
    class RequiredEvent(Event):
        document_id: str = Field(alias="documentId")

    assert RequiredEvent(documentId="doc-123").document_id == "doc-123"


def test_aliased_field_is_validated() -> None:
    params: dict[str, Any] = {"itemCount": -1}
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        AliasedEvent(**params)


@pytest.mark.parametrize(
    ("validation_alias", "params"),
    [
        ("documentId", {"documentId": "doc-123"}),
        ("", {"": "doc-123"}),
        (AliasPath("payload", "id"), {"payload": {"id": "doc-123"}}),
        (AliasPath("payload", 0), {"payload": ["doc-123"]}),
        (AliasChoices("documentId", "doc_id"), {"documentId": "doc-123"}),
        (AliasChoices("documentId", "doc_id"), {"doc_id": "doc-123"}),
        (
            AliasChoices("documentId", AliasPath("payload", "id")),
            {"payload": {"id": "doc-123"}},
        ),
    ],
)
def test_validation_alias_forms(
    validation_alias: str | AliasChoices | AliasPath, params: dict[str, Any]
) -> None:
    class RequestEvent(Event):
        document_id: str = Field(default="DEFAULT", validation_alias=validation_alias)

    event_params = {**params, "extra": "kept"}
    event = RequestEvent(**event_params)
    assert event.document_id == "doc-123"
    assert event.to_dict() == {"extra": "kept"}
    restored = RequestEvent.model_validate_json(event.model_dump_json())
    assert restored.document_id == "doc-123"
    assert restored.to_dict() == {"extra": "kept"}


def test_validation_alias_takes_precedence_over_alias() -> None:
    class RequestEvent(Event):
        document_id: str = Field(
            default="DEFAULT", alias="oldName", validation_alias="documentId"
        )

    params: dict[str, Any] = {"documentId": "doc-123", "oldName": "dynamic"}
    event = RequestEvent(**params)
    assert event.document_id == "doc-123"
    assert event.to_dict() == {"oldName": "dynamic"}


def test_serialization_alias_is_not_an_input_alias() -> None:
    class RequestEvent(Event):
        document_id: str = Field(default="DEFAULT", serialization_alias="outputId")

    params: dict[str, Any] = {"outputId": "dynamic"}
    event = RequestEvent(**params)
    assert event.document_id == "DEFAULT"
    assert event.to_dict() == {"outputId": "dynamic"}


def test_alias_generator() -> None:
    class RequestEvent(Event):
        model_config = ConfigDict(alias_generator=to_camel)
        document_id: str = "DEFAULT"

    params: dict[str, Any] = {"documentId": "doc-123"}
    assert RequestEvent(**params).document_id == "doc-123"


def test_canonical_name_and_alias_precedence() -> None:
    params: dict[str, Any] = {"count": 3}
    assert AliasedEvent(**params).count == 3
    params["itemCount"] = 7
    assert AliasedEvent(**params).count == 7


def test_alias_validation_can_be_disabled() -> None:
    class NameOnlyEvent(AliasedEvent):
        model_config = ConfigDict(validate_by_alias=False)

    params: dict[str, Any] = {"count": 3, "itemCount": 7}
    event = NameOnlyEvent(**params)
    assert event.count == 3
    assert event.to_dict() == {"itemCount": 7}


def test_name_validation_can_be_disabled() -> None:
    class AliasOnlyEvent(AliasedEvent):
        model_config = ConfigDict(validate_by_name=False)

    params: dict[str, Any] = {"itemCount": 3}
    assert AliasOnlyEvent(**params).count == 3
    params = {"count": 3}
    assert AliasOnlyEvent(**params).count == 0


def test_aliased_event_json_round_trip() -> None:
    serializer = JsonSerializer()
    params: dict[str, Any] = {"itemCount": 3, "extra": "kept"}
    event = AliasedEvent(**params)

    restored = serializer.deserialize(serializer.serialize(event))

    assert isinstance(restored, AliasedEvent)
    assert restored.count == 3
    assert restored.to_dict() == {"extra": "kept"}


async def test_workflow_run_uses_aliased_start_event_input() -> None:
    assert await EchoAliasedInput(timeout=2).run(documentId="doc-123") == "doc-123"
