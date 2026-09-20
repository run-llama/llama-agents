# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from pydantic import BaseModel
from workflows import Context, Workflow, step
from workflows.context.context_types import SerializedContext
from workflows.context.pre_context import PreContext
from workflows.context.serializers import (
    BaseSerializer,
    JsonSerializer,
    PickleSerializer,
)
from workflows.events import StartEvent, StopEvent, UnreconstructedException
from workflows.runtime.types.ticks import TickAddEvent


class WorkflowState(BaseModel):
    count: int = 0


class UserValue(BaseModel):
    value: str


class UndeclaredValue(BaseModel):
    value: str


class ExampleWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


class TypedWorkflow(Workflow):
    @step
    async def start(self, ctx: Context[WorkflowState], ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


class UnhashableWorkflow(Workflow):
    __hash__: Any = None

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


@pytest.mark.asyncio
async def test_unhashable_workflow_runs() -> None:
    workflow = UnhashableWorkflow()

    assert await workflow.run() == "ok"


def test_standalone_default_resolves_import_paths() -> None:
    workflow = ExampleWorkflow()
    assert workflow.serializer is None
    first = workflow.runtime.get_serializer(workflow)
    assert type(first) is JsonSerializer
    assert workflow.runtime.get_serializer(workflow) is first

    value = UndeclaredValue(value="restored")
    assert first.deserialize(first.serialize(value)) == value


@pytest.mark.asyncio
async def test_default_context_restore_resolves_undeclared_store_value() -> None:
    workflow = ExampleWorkflow()
    context = Context(workflow)
    await workflow.run(ctx=context)
    value = UndeclaredValue(value="restored")
    await context.store.set("value", value)

    restored = Context.from_dict(workflow, context.to_dict())

    assert await restored.store.get("value") == value


@pytest.mark.asyncio
async def test_default_context_restore_resolves_typed_state_model() -> None:
    workflow = TypedWorkflow()
    context = Context(workflow)
    await workflow.run(ctx=context)
    async with context.store.edit_state() as state:
        state.count = 3

    restored = Context.from_dict(workflow, context.to_dict())

    assert await restored.store.get_state() == WorkflowState(count=3)


@pytest.mark.asyncio
async def test_explicit_open_serializer_restores_undeclared_store_value() -> None:
    workflow = ExampleWorkflow(serializer=JsonSerializer())
    context = Context(workflow)
    value = UndeclaredValue(value="allowed")
    await workflow.run(ctx=context)
    await context.store.set("value", value)

    restored = Context.from_dict(workflow, context.to_dict())

    assert await restored.store.get("value") == value


def test_runtime_json_serializer_adds_workflow_declared_types() -> None:
    configured = JsonSerializer(allowed_types=[UserValue])
    workflow = TypedWorkflow(serializer=configured)

    selected = workflow.runtime.get_serializer(workflow)

    assert selected is workflow.runtime.get_serializer(workflow)
    assert selected is not configured
    for value in (
        TickAddEvent(event=StartEvent()),
        WorkflowState(count=1),
        UserValue(value="configured"),
    ):
        assert selected.deserialize(selected.serialize(value)) == value
    value = UndeclaredValue(value="missing")
    with pytest.raises(ValueError, match="not in the serializer's allowed types"):
        selected.deserialize(selected.serialize(value))


def test_runtime_recomposes_serializer_when_additional_types_change() -> None:
    workflow = ExampleWorkflow()
    serializer = JsonSerializer(allowed_types=[])

    first = workflow.runtime._compose_serializer(workflow, serializer, UserValue)
    second = workflow.runtime._compose_serializer(workflow, serializer, UndeclaredValue)

    assert second is not first
    assert isinstance(second, JsonSerializer)
    qualified_name = f"{UndeclaredValue.__module__}.{UndeclaredValue.__qualname__}"
    assert second.resolve_class(qualified_name) is UndeclaredValue


def test_empty_allowlist_resolves_only_workflow_declared_types() -> None:
    workflow = TypedWorkflow(serializer=JsonSerializer(allowed_types=[]))
    selected = workflow.runtime.get_serializer(workflow)

    tick = TickAddEvent(event=StartEvent())
    assert selected.deserialize(selected.serialize(tick)) == tick
    assert selected.deserialize(selected.serialize(WorkflowState())) == WorkflowState()

    value = UndeclaredValue(value="missing")
    with pytest.raises(ValueError, match="not in the serializer's allowed types"):
        selected.deserialize(selected.serialize(value))


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
                            "exception_type": "other_module.MissingError",
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


def test_snapshot_validation_does_not_decode_metadata_with_the_serializer() -> None:
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
