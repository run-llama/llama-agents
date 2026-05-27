# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel
from workflows.context import Context
from workflows.context.serializers import JsonSerializer
from workflows.errors import ContextSerdeError
from workflows.workflow import Workflow


def test_serialization_roundtrip(ctx: Context, workflow: Workflow) -> None:
    assert Context.from_dict(workflow, ctx.to_dict())


def test_deserialization_invalid(ctx: Context, workflow: Workflow) -> None:
    old_payload = {
        "globals": {},
        "streaming_queue": "[]",
        "queues": {"test_id": "[]"},
        "events_buffer": {},
        "in_progress": "This should be a dict",
        "accepted_events": [],
        "broker_log": [],
        "waiter_id": "test_id",
        "is_running": False,
    }
    with pytest.raises(ContextSerdeError):
        Context.from_dict(workflow, old_payload)


class _MyModel(BaseModel):
    x: int


def test_json_serializer_allow_unknown_types_default_roundtrip() -> None:
    serializer = JsonSerializer()
    payload = serializer.serialize(_MyModel(x=1))
    restored = serializer.deserialize(payload)
    assert isinstance(restored, _MyModel)
    assert restored.x == 1


def test_json_serializer_disallows_unknown_types_with_empty_allowlist() -> None:
    serializer = JsonSerializer(allowed_types=())
    payload = json.dumps(
        {
            "__is_pydantic": True,
            "qualified_name": f"{_MyModel.__module__}.{_MyModel.__qualname__}",
            "value": {"x": 1},
        }
    )
    with pytest.raises(
        ValueError, match="Refusing to import disallowed workflow state type"
    ):
        serializer.deserialize(payload)


def test_json_serializer_allows_explicit_type_allowlist() -> None:
    serializer = JsonSerializer(allowed_types=(_MyModel,))
    payload = serializer.serialize(_MyModel(x=2))
    restored = serializer.deserialize(payload)
    assert isinstance(restored, _MyModel)
    assert restored.x == 2
