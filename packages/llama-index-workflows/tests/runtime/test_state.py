from json import JSONDecodeError

import pytest
from pydantic import ValidationError
from workflows.context.context_types import (
    CURRENT_SERIALIZED_VERSION,
    SerializedContext,
    SerializedContextV0,
)
from workflows.context.serializers import JsonSerializer
from workflows.events import StartEvent
from workflows.workflow import Workflow


def _v1_payload(in_progress: list[str], queue: list[str]) -> dict:
    """A version-1 serialized context with one worker.

    v1 stored ``in_progress`` as bare event strings (queue entries were already
    structured attempts) and had none of the batch-lineage fields.
    """
    return {
        "version": 1,
        "state": {},
        "is_running": True,
        "workers": {
            "middle_step": {
                "queue": [
                    {"event": ev, "attempts": 0, "first_attempt_at": None}
                    for ev in queue
                ],
                "in_progress": in_progress,
                "collected_events": {},
                "collected_waiters": [],
            }
        },
    }


def test_v1_in_progress_strings_fail_strict_validation() -> None:
    """A v1 payload no longer validates against the current schema directly.

    in_progress changed from list[str] to list[SerializedEventAttempt]; this is
    why from_dict_auto must route v1 through a migration rather than straight
    model_validate.
    """
    event = JsonSerializer().serialize(StartEvent())
    with pytest.raises(ValidationError):
        SerializedContext.model_validate(_v1_payload(in_progress=[event], queue=[]))


def test_from_dict_auto_migrates_v1_in_progress_strings() -> None:
    """from_dict_auto upgrades a v1 payload, lifting in_progress strings into attempts."""
    event = JsonSerializer().serialize(StartEvent())
    result = SerializedContext.from_dict_auto(
        _v1_payload(in_progress=[event], queue=[])
    )

    assert result.version == CURRENT_SERIALIZED_VERSION
    worker = result.workers["middle_step"]
    assert len(worker.in_progress) == 1
    attempt = worker.in_progress[0]
    assert attempt.event == event
    assert attempt.attempts == 0
    # New lineage fields fall back to empty defaults.
    assert attempt.batch_stack == []
    assert worker.batch_buffers == {}
    assert worker.batch_fired == []
    assert result.batch_seq == 0
    assert result.batches == {}


def test_from_dict_auto_migrates_v1_with_empty_in_progress() -> None:
    """v1 payloads with no in-progress work still upgrade cleanly."""
    queued = JsonSerializer().serialize(StartEvent())
    result = SerializedContext.from_dict_auto(
        _v1_payload(in_progress=[], queue=[queued])
    )

    assert result.version == CURRENT_SERIALIZED_VERSION
    worker = result.workers["middle_step"]
    assert worker.in_progress == []
    assert [a.event for a in worker.queue] == [queued]


def test_from_dict_auto_passes_current_version_through() -> None:
    """A current-version payload is validated as-is, not migrated."""
    event = JsonSerializer().serialize(StartEvent())
    payload = {
        "version": CURRENT_SERIALIZED_VERSION,
        "state": {},
        "is_running": False,
        "workers": {
            "middle_step": {
                "queue": [],
                "in_progress": [
                    {
                        "event": event,
                        "attempts": 1,
                        "first_attempt_at": None,
                        "batch_stack": ["b0"],
                    }
                ],
                "collected_events": {},
                "collected_waiters": [],
                "batch_buffers": {},
                "batch_fired": [],
            }
        },
        "batch_seq": 3,
        "batches": {},
    }
    result = SerializedContext.from_dict_auto(payload)

    assert result.version == CURRENT_SERIALIZED_VERSION
    assert result.batch_seq == 3
    attempt = result.workers["middle_step"].in_progress[0]
    assert attempt.batch_stack == ["b0"]
    assert attempt.attempts == 1


def test_deserialize_broken_state_raises_validation_error(workflow: Workflow) -> None:
    """Test that broken V0 state raises an error when deserializing."""
    broken_state = {
        "state": {},
        "streaming_queue": "[]",
        "queues": {"middle_step": "not-deserializable-as-a-queue"},
        "event_buffers": {},
        "in_progress": {},
        "accepted_events": [],
        "broker_log": [],
        "is_running": True,
        "waiting_ids": [],
    }

    # This is V0 format (no version field)
    serialized_v0 = SerializedContextV0.model_validate(broken_state)

    # The broken queue string should cause an error during V0->V1 conversion
    # because the queue value is not valid JSON
    with pytest.raises(JSONDecodeError):
        SerializedContext.from_v0(serialized_v0)
