# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows.context import Context
from workflows.context.serializers import PickleSerializer
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


class NotJsonSerializable:
    def __init__(self, value: int) -> None:
        self.value = value


def test_pickle_serializer_json_value_is_not_misdecoded_as_pickle() -> None:
    # "SwEu" is valid base64 that happens to decode to a pickled int, so a
    # JSON-encoded payload of this string must not be fed to pickle.loads.
    serializer = PickleSerializer()
    payload = serializer.serialize("SwEu")
    assert serializer.deserialize(payload) == "SwEu"


def test_pickle_serializer_still_falls_back_to_pickle_for_non_json_values() -> None:
    serializer = PickleSerializer()
    payload = serializer.serialize(NotJsonSerializable(7))
    restored = serializer.deserialize(payload)
    assert isinstance(restored, NotJsonSerializable)
    assert restored.value == 7
