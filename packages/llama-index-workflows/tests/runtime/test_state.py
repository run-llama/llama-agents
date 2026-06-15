from json import JSONDecodeError

import pytest
from pydantic import ValidationError
from workflows import step
from workflows.context.context_types import (
    CURRENT_SERIALIZED_VERSION,
    SerializedContext,
    SerializedContextV0,
)
from workflows.context.serializers import JsonSerializer
from workflows.events import StartEvent, StopEvent
from workflows.runtime.types.internal_state import (
    BrokerState,
    EventAttempt,
    InProgressState,
)
from workflows.runtime.types.results import StepWorkerState
from workflows.workflow import Workflow


def _v1_payload(in_progress: list[str], queue: list[str]) -> dict:
    """A version-1 serialized context with one worker.

    v1 stored ``in_progress`` as bare event strings (queue entries were already
    structured attempts).
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


class _RetryStateWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


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
    assert attempt.not_before is None


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
                    }
                ],
                "collected_events": {},
                "collected_waiters": [],
            }
        },
    }
    result = SerializedContext.from_dict_auto(payload)

    assert result.version == CURRENT_SERIALIZED_VERSION
    attempt = result.workers["middle_step"].in_progress[0]
    assert attempt.attempts == 1


def test_from_dict_auto_rejects_future_version() -> None:
    """A payload from a newer library version fails loudly, not as near-empty V0."""
    payload = {"version": CURRENT_SERIALIZED_VERSION + 1, "state": {}, "workers": {}}
    with pytest.raises(ValueError, match="newer version"):
        SerializedContext.from_dict_auto(payload)


def test_from_dict_auto_rejects_non_int_version() -> None:
    """A stringified version marker is unrecognized and fails loudly."""
    payload = {"version": str(CURRENT_SERIALIZED_VERSION), "state": {}, "workers": {}}
    with pytest.raises(ValueError, match="version"):
        SerializedContext.from_dict_auto(payload)


def test_from_dict_auto_missing_version_routes_to_v0() -> None:
    """No version marker still parses as the legacy V0 format."""
    result = SerializedContext.from_dict_auto({"state": {}, "is_running": False})
    assert result.version == CURRENT_SERIALIZED_VERSION


def test_in_progress_retry_state_survives_serialization() -> None:
    """In-flight work is requeued with its retry state on resume."""
    workflow = _RetryStateWorkflow()
    state = BrokerState.from_workflow(workflow)
    state.workers["start"].in_progress.append(
        InProgressState(
            event=StartEvent(),
            worker_id=0,
            shared_state=StepWorkerState(
                step_name="start",
                collected_events={},
                collected_waiters=[],
            ),
            attempts=2,
            first_attempt_at=100.0,
            last_exception=ValueError("transient"),
            last_failed_at=125.0,
            recovery_counts={"handler": 1},
        )
    )

    serializer = JsonSerializer()
    serialized = state.to_serialized(serializer)
    restored = BrokerState.from_serialized(serialized, workflow, serializer)
    attempt = restored.workers["start"].queue[0]

    assert serialized.version == CURRENT_SERIALIZED_VERSION
    assert attempt.attempts == 2
    assert attempt.first_attempt_at == 100.0
    assert str(attempt.last_exception) == "transient"
    assert attempt.last_failed_at == 125.0
    assert attempt.recovery_counts == {"handler": 1}


def test_queued_not_before_survives_serialization() -> None:
    """The v2 change preserves the delayed-retry field added to queued attempts."""
    workflow = _RetryStateWorkflow()
    state = BrokerState.from_workflow(workflow)
    state.workers["start"].queue.append(
        EventAttempt(
            event=StartEvent(),
            attempts=1,
            first_attempt_at=100.0,
            not_before=150.0,
        )
    )

    serializer = JsonSerializer()
    serialized = state.to_serialized(serializer)
    restored = BrokerState.from_serialized(serialized, workflow, serializer)

    assert serialized.workers["start"].queue[0].not_before == 150.0
    assert restored.workers["start"].queue[0].not_before == 150.0


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
