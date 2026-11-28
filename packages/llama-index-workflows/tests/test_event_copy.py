# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from pydantic import PrivateAttr
from workflows.context.serializers import JsonSerializer
from workflows.events import Event


class TypedCopyEvent(Event):
    label: str
    _handle: object = PrivateAttr(default_factory=object)


@pytest.mark.parametrize("deep", [False, True])
def test_model_copy_dynamic_updates_survive_serialization(deep: bool) -> None:
    original = Event(value=1, nested={"items": ["original"]})

    copied = original.model_copy(update={"value": 2, "added": 3}, deep=deep)

    assert copied.value == copied["value"] == copied.get("value") == 2
    assert copied.added == copied["added"] == 3
    assert original.value == original["value"] == 1
    assert "added" not in original

    serializer = JsonSerializer()
    restored = serializer.deserialize(serializer.serialize(copied))
    assert isinstance(restored, Event)
    assert restored.value == restored["value"] == 2
    assert restored.added == restored["added"] == 3

    copied.nested["items"].append("copied")
    assert original.nested["items"] == (
        ["original"] if deep else ["original", "copied"]
    )


@pytest.mark.parametrize("deep", [False, True])
def test_model_copy_keeps_typed_fields_and_dynamic_updates(deep: bool) -> None:
    original = TypedCopyEvent(label="original")
    original["value"] = 1

    copied = original.model_copy(update={"label": "copied", "value": 2}, deep=deep)

    assert isinstance(copied, TypedCopyEvent)
    assert copied.label == "copied"
    assert copied.value == copied["value"] == 2
    assert original.label == "original"
    assert original.value == original["value"] == 1
    if not deep:
        assert copied._handle is original._handle

    serializer = JsonSerializer(allowed_types=[TypedCopyEvent])
    restored = serializer.deserialize(serializer.serialize(copied))
    assert isinstance(restored, TypedCopyEvent)
    assert restored.label == "copied"
    assert restored.value == restored["value"] == 2


def test_model_copy_retains_unvalidated_field_updates() -> None:
    original = TypedCopyEvent(label="original")
    original["value"] = 1

    copied = original.model_copy(update={"label": 42, "value": 2})

    assert copied.label == 42
    assert copied.value == copied["value"] == 2
    assert original.label == "original"
