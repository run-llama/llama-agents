# ty: ignore[invalid-argument-type, unknown-argument]
from __future__ import annotations

from typing import Any, cast

from llama_agents.server import PersistentHandler
from workflows.events import StopEvent


def _base_handler_kwargs() -> dict[str, Any]:
    return {
        "handler_id": "h1",
        "workflow_name": "wf",
        "status": cast("str", "completed"),
    }


def test_stop_event_round_trip() -> None:
    handler = PersistentHandler(**_base_handler_kwargs(), result=StopEvent(result=1))

    dumped = handler.model_dump(mode="python")
    assert "result_unreadable" not in dumped
    restored = PersistentHandler(**dumped)
    assert isinstance(restored.result, StopEvent)
    assert restored.result.result == 1


def test_legacy_result_dict_is_coerced_to_stop_event() -> None:
    legacy_payload = {"foo": 2}
    handler = PersistentHandler(
        **_base_handler_kwargs(), result=cast(Any, legacy_payload)
    )

    assert isinstance(handler.result, StopEvent)
    assert handler.result.result == legacy_payload

    dumped = handler.model_dump(mode="python")

    restored = PersistentHandler(**dumped)
    assert isinstance(restored.result, StopEvent)
    assert restored.result.result == legacy_payload


class MyStop(StopEvent):
    pass


def test_stop_event_subclass_round_trip() -> None:
    payload = {"y": 3}
    handler = PersistentHandler(
        **_base_handler_kwargs(),
        result=MyStop(result=payload),  # type: ignore[call-arg]
    )

    dumped = handler.model_dump(mode="python")

    restored = PersistentHandler(**dumped)
    assert isinstance(restored.result, MyStop)
    assert restored.result.result == payload


def test_converts_to_stop_event() -> None:
    handler = PersistentHandler(**_base_handler_kwargs(), result=123)  # type: ignore[arg-type]
    assert isinstance(handler.result, StopEvent)
    assert handler.result.result == 123
