# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows.context import Context
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


def test_pickle_serializer_json_string_roundtrip() -> None:
    from workflows.context.serializers import PickleSerializer

    serializer = PickleSerializer()
    # A JSON string like "SwEu" is serialized with surrounding quotes '"SwEu"'.
    # Without base64 validation, b64decode drops the quotes and decodes b'K\x01.',
    # which incorrectly unpickles to integer 1 instead of the string "SwEu".
    serialized = serializer.serialize("SwEu")
    assert serializer.deserialize(serialized) == "SwEu"
