# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, ClassVar, Literal

import pytest
from httpx import ASGITransport, AsyncClient
from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from llama_agents.server._store.abstract_workflow_store import (
    HandlerQuery,
    PersistentHandler,
    decode_persistent_handler,
)
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from pydantic import BaseModel, model_validator
from workflows import Context, Workflow, step
from workflows.context.serializers import (
    BaseSerializer,
    JsonSerializer,
    PickleSerializer,
)
from workflows.events import (
    Event,
    SerializableEvent,
    StartEvent,
    StopEvent,
)


class State(BaseModel):
    count: int = 0


class ExtraEvent(Event):
    pass


class InputEvent(StartEvent):
    nested: SerializableEvent


class OutputEvent(StopEvent):
    pass


class ExcludedOutputEvent(StopEvent):
    pass


class ValidatedOutputEvent(StopEvent):
    validation_calls: ClassVar[int] = 0

    @model_validator(mode="before")
    @classmethod
    def count_validation(cls, value: Any) -> Any:
        cls.validation_calls += 1
        return value


class DeclaredWorkflow(Workflow):
    @step
    async def start(self, ctx: Context[State], ev: InputEvent) -> OutputEvent:
        async with ctx.store.edit_state() as state:
            state.count += 1
            count = state.count
        return OutputEvent.model_validate({"result": count})


class DictStateWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")


class ValidatedOutputWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> ValidatedOutputEvent:
        return ValidatedOutputEvent.model_validate({"result": "done"})


class LegacyMemoryWorkflowStore(MemoryWorkflowStore):
    # This was a valid override before result decoders were added to built-in stores.
    async def query(  # ty: ignore[invalid-method-override]  # pyright: ignore[reportIncompatibleMethodOverride]
        self, query: HandlerQuery
    ) -> list[PersistentHandler]:
        return await super().query(query)


def make_server(path: Path, serializer: BaseSerializer | None = None) -> WorkflowServer:
    server = WorkflowServer(
        workflow_store=SqliteWorkflowStore(db_path=str(path)),
        accept_context_api=True,
        serializer=serializer,
    )
    server.add_workflow("declared", DeclaredWorkflow(), additional_events=[ExtraEvent])
    return server


@pytest.fixture
async def client(
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> AsyncIterator[tuple[WorkflowServer, AsyncClient, Path]]:
    path = tmp_path / "server.db"
    server = make_server(path, getattr(request, "param", None))
    async with (
        server.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=server.app, raise_app_exceptions=False),
            base_url="http://test",
        ) as client,
    ):
        yield server, client, path


@pytest.fixture
def forbid_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(name: str) -> Any:
        pytest.fail(f"Unexpected metadata import: {name}")

    monkeypatch.setattr("workflows.context.utils.import_module", fail)


def input_payload(qualified_name: str | None = None) -> dict[str, Any]:
    nested = JsonSerializer().serialize_value(ExtraEvent())
    if qualified_name is not None:
        nested["qualified_name"] = qualified_name
    return {"type": "InputEvent", "value": {"nested": nested}}


def workflow_decoder(server: WorkflowServer, name: str) -> JsonSerializer:
    return server.get_json_decoder(name)


async def test_declared_and_additional_events_work(
    client: tuple[WorkflowServer, AsyncClient, Path],
) -> None:
    server, http, _ = client
    response = await http.post(
        "/workflows/declared/run", json={"start_event": input_payload()}
    )
    assert response.status_code == 200, response.text
    assert response.json()["result"]["value"]["result"] == 1
    assert server.serializer is None
    workflow = server.get_workflows()["declared"]
    selected = workflow.runtime.get_serializer(workflow)
    state = State(count=4)
    assert selected.deserialize(selected.serialize(state)) == state


@pytest.mark.parametrize(
    "client", [None, JsonSerializer(), PickleSerializer()], indirect=True
)
@pytest.mark.parametrize("nested", [False, True])
async def test_unknown_api_metadata_does_not_resolve(
    client: tuple[WorkflowServer, AsyncClient, Path],
    forbid_imports: None,
    nested: bool,
) -> None:
    _, http, _ = client
    payload = (
        input_payload("unregistered_payload.Model")
        if nested
        else {"qualified_name": "unregistered_payload.Event", "value": {}}
    )
    response = await http.post("/workflows/declared/run", json={"start_event": payload})
    assert response.status_code == 400
    assert "Refusing to import" in response.text


@pytest.mark.parametrize(
    "client", [None, JsonSerializer(), PickleSerializer()], indirect=True
)
async def test_unknown_persisted_result_does_not_resolve(
    client: tuple[WorkflowServer, AsyncClient, Path],
    forbid_imports: None,
) -> None:
    _, http, path = client
    response = await http.post(
        "/workflows/declared/run",
        json={"handler_id": "stored", "start_event": input_payload()},
    )
    assert response.status_code == 200, response.text
    response = await http.post(
        "/workflows/declared/run",
        json={"handler_id": "healthy", "start_event": input_payload()},
    )
    assert response.status_code == 200, response.text
    bad = {
        "__is_pydantic": True,
        "qualified_name": "unregistered_payload.Result",
        "value": {},
    }
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE handlers SET result = ? WHERE handler_id = ?",
            (json.dumps(bad), "stored"),
        )
    response = await http.get("/handlers/stored")
    assert response.status_code == 422
    assert response.json() == {"detail": "Stored handler result cannot be decoded"}
    response = await http.get("/results/stored")
    assert response.status_code == 422
    response = await http.get("/handlers")
    assert response.status_code == 200, response.text
    handlers = {item["handler_id"]: item for item in response.json()["handlers"]}
    stored = handlers["stored"]
    assert stored["handler_id"] == "stored"
    assert stored["workflow_name"] == "declared"
    assert stored["status"] == "completed"
    assert stored["result"] is None
    assert handlers["healthy"]["result"]["qualified_name"] == (
        f"{OutputEvent.__module__}.{OutputEvent.__qualname__}"
    )
    response = await http.post("/handlers/stored/cancel")
    assert response.status_code == 404
    response = await http.post("/handlers/stored/cancel?purge=true")
    assert response.status_code == 200
    assert response.json() == {"status": "deleted"}


async def test_legacy_custom_store_query_signature_works_through_handler_apis(
    forbid_imports: None,
) -> None:
    store = LegacyMemoryWorkflowStore()
    server = WorkflowServer(workflow_store=store)
    server.add_workflow("declared", DeclaredWorkflow())
    await store.update(
        PersistentHandler(
            handler_id="legacy",
            workflow_name="declared",
            status="completed",
            result=OutputEvent.model_validate({"result": "done"}),
        )
    )
    async with (
        server.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=server.app), base_url="http://test"
        ) as client,
    ):
        listing = await client.get("/handlers")
        assert listing.status_code == 200
        assert listing.json()["handlers"][0]["handler_id"] == "legacy"
        assert (await client.get("/handlers/legacy")).status_code == 200
        assert (await client.get("/results/legacy")).status_code == 200
        assert (await client.post("/handlers/legacy/cancel")).status_code == 404
        purged = await client.post("/handlers/legacy/cancel?purge=true")
        assert purged.status_code == 200
        assert purged.json() == {"status": "deleted"}


async def test_purge_does_not_validate_persisted_result(tmp_path: Path) -> None:
    store = SqliteWorkflowStore(db_path=str(tmp_path / "purge.db"))
    server = WorkflowServer(workflow_store=store)
    server.add_workflow("validated", ValidatedOutputWorkflow())
    async with (
        server.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=server.app), base_url="http://test"
        ) as client,
    ):
        await store.update(
            PersistentHandler(
                handler_id="purge",
                workflow_name="validated",
                status="completed",
                result=ValidatedOutputEvent.model_validate({"result": "done"}),
            )
        )
        ValidatedOutputEvent.validation_calls = 0
        response = await client.post("/handlers/purge/cancel?purge=true")
    assert response.status_code == 200
    assert response.json() == {"status": "deleted"}
    assert ValidatedOutputEvent.validation_calls == 0


async def test_restart_loads_declared_result_and_continues_typed_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "restart.db"
    for count in (1, 2):
        server = make_server(path)
        async with (
            server.contextmanager(),
            AsyncClient(
                transport=ASGITransport(app=server.app), base_url="http://test"
            ) as http,
        ):
            if count == 2:
                stored = await http.get("/handlers/restarted")
                assert stored.status_code == 200, stored.text
                assert stored.json()["result"]["value"]["result"] == 1
            response = await http.post(
                "/workflows/declared/run",
                json={"handler_id": "restarted", "start_event": input_payload()},
            )
            assert response.status_code == 200, response.text
            assert response.json()["result"]["value"]["result"] == count


def test_explicit_json_serializer_only_controls_internal_encoding() -> None:
    dynamic = JsonSerializer()
    server = WorkflowServer(serializer=dynamic)
    server.add_workflow("declared", DeclaredWorkflow())
    workflow = server.get_workflows()["declared"]
    selected = workflow.runtime.get_serializer(workflow)
    value = State(count=3)
    assert selected.deserialize(selected.serialize(value)) == value
    assert selected is dynamic
    assert workflow_decoder(server, "declared") is not dynamic


def test_additional_events_extend_the_workflow_decoder() -> None:
    server = WorkflowServer()
    server.add_workflow("declared", DeclaredWorkflow(), additional_events=[ExtraEvent])
    selected = workflow_decoder(server, "declared")
    value = ExtraEvent()
    assert selected.deserialize(selected.serialize(value)) == value


def test_state_types_are_not_registered_as_public_events() -> None:
    server = WorkflowServer()
    server.add_workflow("typed", DeclaredWorkflow())
    server.add_workflow("dict", DictStateWorkflow())
    with pytest.raises(ValueError, match="Refusing to import"):
        workflow_decoder(server, "typed").resolve_class(
            f"{State.__module__}.{State.__qualname__}"
        )
    with pytest.raises(ValueError, match="Refusing to import"):
        workflow_decoder(server, "dict").resolve_class(
            f"{State.__module__}.{State.__qualname__}"
        )


def test_registering_another_workflow_leaves_the_existing_decoder_alone(
    forbid_imports: None,
) -> None:
    server = WorkflowServer()
    workflow = DeclaredWorkflow()
    server.add_workflow("first", workflow)
    before = workflow_decoder(server, "first")
    server.add_workflow("second", DeclaredWorkflow(), additional_events=[ExtraEvent])
    assert workflow_decoder(server, "first") is before
    value = ExtraEvent()
    with pytest.raises(ValueError, match="Refusing to import"):
        before.deserialize(before.serialize(value))


class LaterInput(StartEvent):
    value: int


class LaterOutput(StopEvent):
    pass


class LaterWorkflow(Workflow):
    @step
    async def start(self, ctx: Context[State], ev: LaterInput) -> LaterOutput:
        async with ctx.store.edit_state() as state:
            state.count = ev.value
        return LaterOutput.model_validate({"result": ev.value})


async def test_workflow_registered_later_uses_own_declarations() -> None:
    server = WorkflowServer()
    server.add_workflow("first", DeclaredWorkflow())
    later = LaterWorkflow()
    server.add_workflow("later", later)
    event = LaterInput(value=8)
    async with server.contextmanager():
        handler = await server._runtime_core.run("later", start_event=event)
        completed = await server._service.await_workflow(handler)
        assert completed.result is not None
        assert completed.result.value["result"] == 8


async def test_run_api_does_not_resolve_another_workflows_event() -> None:
    server = WorkflowServer()
    server.add_workflow("first", DeclaredWorkflow())
    server.add_workflow("later", LaterWorkflow())
    async with (
        server.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=server.app), base_url="http://test"
        ) as client,
    ):
        response = await client.post(
            "/workflows/first/run",
            json={
                "start_event": {
                    "qualified_name": f"{LaterInput.__module__}.{LaterInput.__qualname__}",
                    "value": {"value": 1},
                }
            },
        )
    assert response.status_code == 400
    assert "Refusing to import" in response.text


def test_qualified_name_collisions_are_scoped_to_each_workflow() -> None:
    def make_event() -> type[Event]:
        class Collision(Event):
            pass

        return Collision

    first, second = make_event(), make_event()
    server = WorkflowServer()
    server.add_workflow("first", DeclaredWorkflow(), additional_events=[first])
    server.add_workflow("second", DeclaredWorkflow(), additional_events=[second])
    qualified_name = f"{first.__module__}.{first.__qualname__}"
    assert workflow_decoder(server, "first").resolve_class(qualified_name) is first
    assert workflow_decoder(server, "second").resolve_class(qualified_name) is second


def test_stored_result_decoder_uses_handler_workflow_name() -> None:
    server = WorkflowServer()
    server.add_workflow("first", DeclaredWorkflow())
    server.add_workflow("later", LaterWorkflow())
    data = {
        "handler_id": "handler",
        "workflow_name": "first",
        "status": "completed",
        "result": JsonSerializer().serialize_value(
            LaterOutput.model_validate({"result": 1})
        ),
    }

    def result_decoder(name: str) -> JsonSerializer:
        return workflow_decoder(server, name)

    other_workflow = decode_persistent_handler(data, result_decoder)
    assert other_workflow.result is None
    data["workflow_name"] = "later"
    restored = decode_persistent_handler(data, result_decoder)
    assert isinstance(restored.result, LaterOutput)


async def test_persisted_result_outside_workflow_types_does_not_decode(
    tmp_path: Path,
) -> None:
    store = SqliteWorkflowStore(db_path=str(tmp_path / "excluded-result.db"))
    server = WorkflowServer(workflow_store=store)
    server.add_workflow("declared", DeclaredWorkflow())
    await store.update(
        PersistentHandler(
            handler_id="excluded",
            workflow_name="declared",
            status="completed",
            result=ExcludedOutputEvent.model_validate({"result": "hidden"}),
        )
    )

    restored = await server._service.load_persistent_handler("excluded")

    assert restored is not None
    assert restored.result is None
    assert restored.result_unreadable


def test_unexpected_result_decoder_error_propagates() -> None:
    data = {
        "handler_id": "handler",
        "workflow_name": "workflow",
        "status": "completed",
        "result": {"result": 1},
    }

    def fail(workflow_name: str) -> JsonSerializer:
        raise RuntimeError(workflow_name)

    with pytest.raises(RuntimeError, match="workflow"):
        decode_persistent_handler(data, fail)


@pytest.mark.parametrize("status", ["completed", "running"])
async def test_stale_workflow_custom_stop_result_does_not_block_purge(
    tmp_path: Path,
    forbid_imports: None,
    status: Literal["completed", "running"],
) -> None:
    server = WorkflowServer(
        workflow_store=SqliteWorkflowStore(db_path=str(tmp_path / "stale.db"))
    )
    server.add_workflow("renamed", DeclaredWorkflow())
    async with (
        server.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=server.app), base_url="http://test"
        ) as client,
    ):
        await server._workflow_store.update(
            PersistentHandler(
                handler_id="stale",
                workflow_name="removed",
                status=status,
                run_id="removed-run",
                result=OutputEvent.model_validate({"result": "done"}),
            )
        )
        listing = await client.get("/handlers")
        assert listing.status_code == 200, listing.text
        [stale] = listing.json()["handlers"]
        assert stale["handler_id"] == "stale"
        assert stale["workflow_name"] == "removed"
        assert stale["result"] is None
        response = await client.get("/handlers/stale")
        assert response.status_code == 422
        response = await client.get("/results/stale")
        assert response.status_code == 422
        if status == "completed":
            response = await client.post("/handlers/stale/cancel")
            assert response.status_code == 404
        response = await client.post("/handlers/stale/cancel?purge=true")
        assert response.status_code == 200
        assert response.json() == {"status": "deleted"}
