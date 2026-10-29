# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
from datetime import date
from uuid import UUID

import pytest
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationError,
    ValidationInfo,
    field_validator,
)
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import DictState, InMemoryStateStore


class StrictState(BaseModel):
    model_config = ConfigDict(strict=True)

    day: date
    identifier: UUID
    coordinates: tuple[int, int]
    count: int = 1


class StrictFieldState(BaseModel):
    day: date = Field(strict=True)


class StrictDateRoot(RootModel[date]):
    model_config = ConfigDict(strict=True)


class JsonValidatedState(BaseModel):
    value: int

    @field_validator("value", mode="before")
    @classmethod
    def validate_json_value(cls, value: object, info: ValidationInfo) -> object:
        if info.mode == "json":
            assert isinstance(value, int), "JSON value must be an integer"
        return value


@pytest.fixture
def strict_state() -> StrictState:
    return StrictState(
        day=date(2026, 1, 2),
        identifier=UUID("12345678-1234-5678-1234-567812345678"),
        coordinates=(1, 2),
    )


@pytest.mark.parametrize("tree", [False, True])
def test_strict_models_roundtrip_json(strict_state: StrictState, tree: bool) -> None:
    serializer = JsonSerializer(allowed_types=[StrictState, StrictFieldState])
    value = {"states": [strict_state, StrictFieldState(day=strict_state.day)]}
    payload = serializer.serialize(value)
    restored = (
        serializer.deserialize_value(json.loads(payload))
        if tree
        else serializer.deserialize(payload)
    )
    assert restored == value
    assert type(restored["states"][0]) is StrictState
    assert type(restored["states"][0].coordinates) is tuple


@pytest.mark.parametrize("typed", [False, True])
async def test_strict_state_store_snapshot_roundtrip(
    strict_state: StrictState, typed: bool
) -> None:
    serializer = JsonSerializer(allowed_types=[StrictState])
    state = strict_state if typed else DictState(value=strict_state)
    store = InMemoryStateStore(state)
    payload = json.loads(json.dumps(store.to_dict(serializer)))
    restored = InMemoryStateStore.from_dict(payload, serializer)
    assert await restored.get_state() == state


def test_json_restoration_keeps_strict_primitive_validation(
    strict_state: StrictState,
) -> None:
    serializer = JsonSerializer(allowed_types=[StrictState])
    payload = json.loads(serializer.serialize(strict_state))
    payload["value"]["count"] = "1"
    with pytest.raises(ValidationError, match="count"):
        serializer.deserialize(json.dumps(payload))


def test_strict_root_model_roundtrip_json() -> None:
    serializer = JsonSerializer(allowed_types=[StrictDateRoot])
    value = StrictDateRoot(date(2026, 1, 2))
    assert serializer.deserialize(serializer.serialize(value)) == value


def test_deserialization_uses_json_validators() -> None:
    serializer = JsonSerializer(allowed_types=[JsonValidatedState])
    payload = json.loads(serializer.serialize(JsonValidatedState(value=1)))
    payload["value"]["value"] = "1"
    with pytest.raises(ValidationError, match="JSON value must be an integer"):
        serializer.deserialize(json.dumps(payload))
