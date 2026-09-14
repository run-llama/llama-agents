# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import base64
import json
import pickle
from pathlib import Path
from typing import Any, cast

import pytest
from httpx import ASGITransport, AsyncClient
from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from llama_agents.server._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    Status,
    _handler_result_decoding_failed,
    query_handlers,
    stream_workflow_ticks,
)
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from llama_agents.server.runtime import _DurableWorkflowRuntime
from workflows import Context, Workflow, step
from workflows.context.serializers import (
    BaseSerializer,
    JsonSerializer,
    PickleSerializer,
)
from workflows.events import Event, StartEvent, StopEvent


class PythonEvent(Event):
    pass


class AdditionalEvent(Event):
    pass


class PythonWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> PythonEvent:
        await ctx.store.set("python_value", complex(1, 2))
        return PythonEvent.model_validate({"value": complex(3, 4)})

    @step
    async def finish(self, ctx: Context, ev: PythonEvent) -> StopEvent:
        assert await ctx.store.get("python_value") == complex(1, 2)
        assert ev.value == complex(3, 4)
        return StopEvent(result="ok")


class CustomSerializer(BaseSerializer):
    def __init__(self) -> None:
        self.encodes = 0
        self.decodes = 0

    def serialize(self, value: Any) -> str:
        self.encodes += 1
        return "custom:" + base64.b64encode(pickle.dumps(value)).decode()

    def deserialize(self, value: str) -> Any:
        self.decodes += 1
        assert value.startswith("custom:")
        return pickle.loads(base64.b64decode(value.removeprefix("custom:")))


def test_server_serializer_adds_additional_events() -> None:
    configured = JsonSerializer(allowed_types=[])
    workflow = PythonWorkflow()
    server = WorkflowServer(serializer=configured)
    server.add_workflow("python", workflow, additional_events=[AdditionalEvent])

    selected = workflow.runtime.get_serializer(workflow)

    assert selected is workflow.runtime.get_serializer(workflow)
    assert selected is not configured
    event = AdditionalEvent()
    assert selected.deserialize(selected.serialize(event)) == event


@pytest.mark.parametrize("use_workflow_override", [True, False])
@pytest.mark.parametrize("serializer_type", [PickleSerializer, CustomSerializer])
async def test_bound_serializer_handles_internal_python_values(
    tmp_path: Path,
    use_workflow_override: bool,
    serializer_type: type[BaseSerializer],
) -> None:
    selected = serializer_type()
    workflow = PythonWorkflow(serializer=selected if use_workflow_override else None)
    store = SqliteWorkflowStore(db_path=str(tmp_path / "workflows.db"))
    server = WorkflowServer(
        workflow_store=store,
        serializer=JsonSerializer() if use_workflow_override else selected,
    )
    server.add_workflow("python", workflow)
    assert workflow.runtime.get_serializer(workflow) is selected
    async with server._runtime_core.contextmanager():
        result = await server._runtime_core.run("python")
        completed = await server._service.await_workflow(result)
        assert completed.result is not None
        assert completed.result.value["result"] == "ok"
        assert result.run_id is not None
        ticks = [
            tick
            async for tick in stream_workflow_ticks(
                store, result.run_id, serializer=selected
            )
        ]
        assert ticks
        assert server._runtime_core._persistence is not None
        replayed = await server._runtime_core._persistence.context_from_ticks(
            workflow, result.run_id
        )
        assert replayed is not None
    if isinstance(selected, CustomSerializer):
        assert selected.encodes > 0
        assert selected.decodes > 0


class FirstStop(StopEvent):
    pass


class SecondStop(StopEvent):
    pass


async def test_store_selects_decoder_by_row_workflow_before_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    store = SqliteWorkflowStore(db_path=str(tmp_path / "results.db"))
    await store.start()
    for name, event_type in [("first", FirstStop), ("second", SecondStop)]:
        await store.update(
            PersistentHandler(
                handler_id=name,
                workflow_name=name,
                status="completed",
                result=event_type.model_validate({"result": name}),
            )
        )
    decoders = {
        "first": JsonSerializer(allowed_types=[FirstStop]),
        "second": JsonSerializer(allowed_types=[SecondStop]),
    }
    handlers = await store.query(HandlerQuery(), result_decoder=decoders.__getitem__)
    assert {type(handler.result) for handler in handlers} == {FirstStop, SecondStop}

    def fail(name: str) -> Any:
        pytest.fail(f"Unexpected metadata import: {name}")

    monkeypatch.setattr("workflows.context.utils.import_module", fail)
    with store._connect() as connection:
        connection.execute(
            "UPDATE handlers SET result = ? WHERE handler_id = ?",
            (
                json.dumps(
                    {
                        "__is_pydantic": True,
                        "qualified_name": "unregistered_payload.Result",
                        "value": {"secret": "do-not-log"},
                    }
                ),
                "first",
            ),
        )
        connection.commit()
    handlers = await store.query(HandlerQuery(), result_decoder=decoders.__getitem__)
    restored = {handler.handler_id: handler for handler in handlers}
    assert restored["first"].result is None
    assert _handler_result_decoding_failed(restored["first"])
    assert isinstance(restored["second"].result, SecondStop)
    assert "handler_id='first' workflow_name='first' error=" in caplog.text
    assert "unregistered_payload" not in caplog.text
    assert "do-not-log" not in caplog.text


async def test_unreadable_persisted_result_is_reported_over_http(
    tmp_path: Path,
) -> None:
    store = SqliteWorkflowStore(db_path=str(tmp_path / "results.db"))
    await store.update(
        PersistentHandler(
            handler_id="unreadable",
            workflow_name="missing",
            status="completed",
            result=StopEvent(result="stored"),
        )
    )
    with store._connect() as connection:
        connection.execute(
            """UPDATE handlers
               SET result = '{"__is_pydantic": true,
                              "qualified_name": "missing.result.StoredResult",
                              "value": {}}'
               WHERE handler_id = 'unreadable'"""
        )
        connection.commit()

    server = WorkflowServer(workflow_store=store)
    async with server.contextmanager():
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            handler = await client.get("/handlers/unreadable")
            result = await client.get("/results/unreadable")
            listing = await client.get("/handlers")

    assert handler.status_code == 422
    assert result.status_code == 422
    assert listing.status_code == 200
    assert listing.json()["handlers"][0]["result"] is None


async def test_legacy_custom_query_signature_is_used_without_decoder(
    tmp_path: Path,
) -> None:
    class LegacyStore:
        async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
            return []

    store = cast(AbstractWorkflowStore, LegacyStore())
    assert await query_handlers(store, HandlerQuery()) == []
    assert (
        await query_handlers(
            store, HandlerQuery(), result_decoder=lambda name: JsonSerializer()
        )
        == []
    )


async def test_legacy_status_override_receives_no_new_keyword(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MemoryWorkflowStore()
    calls: list[str] = []

    async def update(
        run_id: str,
        *,
        status: Status | None = None,
        result: StopEvent | None = None,
        error: str | None = None,
    ) -> None:
        calls.append(run_id)
        await MemoryWorkflowStore.update_handler_status(
            store, run_id, status=status, result=result, error=error
        )

    monkeypatch.setattr(store, "update_handler_status", update)
    runtime = _DurableWorkflowRuntime(
        workflow_store=store, serializer=PickleSerializer()
    )
    runtime.add_workflow("python", PythonWorkflow())
    async with runtime.contextmanager():
        result = await runtime.run("python")
        completed = await runtime._service.await_workflow(result)
        assert completed.status == "completed"
    assert calls
