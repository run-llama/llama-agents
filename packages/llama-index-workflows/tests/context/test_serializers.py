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


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"foo": "bar"},
        {"version": 0},
    ],
)
def test_deserialization_rejects_payload_without_context_fields(
    workflow: Workflow, payload: dict[str, object]
) -> None:
    with pytest.raises(ContextSerdeError, match="no recognized fields"):
        Context.from_dict(workflow, payload)


def test_deserialization_rejects_nested_snapshot(
    ctx: Context, workflow: Workflow
) -> None:
    with pytest.raises(ContextSerdeError, match="no recognized fields"):
        Context.from_dict(workflow, {"context": ctx.to_dict()})


@pytest.mark.parametrize("payload", [{"state": {}}, {"version": 0, "state": {}}])
def test_deserialization_accepts_legacy_payload_with_context_field(
    workflow: Workflow, payload: dict[str, object]
) -> None:
    assert Context.from_dict(workflow, payload)
