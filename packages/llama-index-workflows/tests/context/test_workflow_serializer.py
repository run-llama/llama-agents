from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from workflows import Context, Workflow, step
from workflows.context.context_types import SerializedContext
from workflows.context.pre_context import PreContext
from workflows.context.serializers import (
    BaseSerializer,
    JsonSerializer,
    PickleSerializer,
)
from workflows.events import StartEvent, StopEvent, UnreconstructedException


class ExampleWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


async def test_workflow_serializer_is_read_only_and_used_for_context() -> None:
    serializer = PickleSerializer()
    workflow = ExampleWorkflow(serializer=serializer)
    context = Context(workflow)
    assert isinstance(context._face, PreContext)
    assert context._face.serializer is serializer
    assert workflow.runtime.get_serializer(workflow) is serializer
    with pytest.raises(AttributeError):
        setattr(workflow, "serializer", JsonSerializer())
    assert await workflow.run(ctx=context) == "ok"


def test_standalone_default_remains_json() -> None:
    workflow = ExampleWorkflow()
    assert workflow.serializer is None
    first = workflow.runtime.get_serializer(workflow)
    assert type(first) is JsonSerializer
    assert workflow.runtime.get_serializer(workflow) is first


def test_snapshot_retry_exception_uses_selected_serializer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbid_import(name: str) -> Any:
        pytest.fail(f"Unexpected exception import: {name}")

    monkeypatch.setattr(
        "workflows.events.import_module_from_qualified_name", forbid_import
    )
    serializer = JsonSerializer(allowed_types=[StartEvent])
    workflow = ExampleWorkflow(serializer=serializer)
    data = {
        "version": 2,
        "workers": {
            "start": {
                "queue": [
                    {
                        "event": serializer.serialize(StartEvent()),
                        "last_exception": {
                            "exception_type": "untrusted_module.HiddenError",
                            "exception_message": "failed",
                        },
                    }
                ]
            }
        },
    }
    context = Context.from_dict(workflow, data)
    assert isinstance(context._face, PreContext)
    exception = context._face.init_snapshot.workers["start"].queue[0].last_exception
    assert isinstance(exception, UnreconstructedException)


def test_snapshot_validation_uses_custom_scope_without_decoding_metadata() -> None:
    calls: list[str] = []

    class ScopedSerializer(BaseSerializer):
        @contextmanager
        def validation_context(self) -> Iterator[None]:
            calls.append("scope")
            yield

        def serialize(self, value: Any) -> str:
            pytest.fail("Metadata must not be encoded with the application serializer")

        def deserialize(self, value: str) -> Any:
            pytest.fail("Metadata must not be decoded with the application serializer")

    parsed = SerializedContext.from_dict_auto({"version": 2}, ScopedSerializer())
    assert parsed.version == 2
    assert calls == ["scope"]
