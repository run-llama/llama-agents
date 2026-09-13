from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from llama_agents.server import WorkflowServer
from llama_agents.server._store.abstract_workflow_store import decode_persistent_handler
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from pydantic import BaseModel
from workflows import Context, Workflow, step
from workflows.context.pre_context import PreContext
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
    UnreconstructedException,
)


class State(BaseModel):
    count: int = 0


class ExtraEvent(Event):
    pass


class InputEvent(StartEvent):
    nested: SerializableEvent


class OutputEvent(StopEvent):
    pass


class DeclaredWorkflow(Workflow):
    @step
    async def start(self, ctx: Context[State], ev: InputEvent) -> OutputEvent:
        async with ctx.store.edit_state() as state:
            state.count += 1
            count = state.count
        return OutputEvent.model_validate({"result": count})


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
    workflow = server.get_workflows()[name]
    return workflow.runtime._get_json_decoder(workflow)


async def test_declared_and_additional_events_and_typed_state_work(
    client: tuple[WorkflowServer, AsyncClient, Path],
    forbid_imports: None,
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
async def test_unknown_api_metadata_never_imports(
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


async def test_unknown_context_event_never_imports(
    client: tuple[WorkflowServer, AsyncClient, Path],
    forbid_imports: None,
) -> None:
    _, http, _ = client
    event = JsonSerializer().serialize_value(ExtraEvent())
    event["qualified_name"] = "unregistered_payload.Event"
    context = {
        "version": 2,
        "workers": {"start": {"queue": [{"event": json.dumps(event)}]}},
    }
    response = await http.post(
        "/workflows/declared/run",
        json={"start_event": input_payload(), "context": context},
    )
    assert response.status_code == 400, response.text
    assert "Refusing to import" in response.text


@pytest.mark.parametrize(
    "client", [None, JsonSerializer(), PickleSerializer()], indirect=True
)
async def test_unknown_persisted_result_never_imports(
    client: tuple[WorkflowServer, AsyncClient, Path],
    forbid_imports: None,
) -> None:
    _, http, path = client
    response = await http.post(
        "/workflows/declared/run",
        json={"handler_id": "stored", "start_event": input_payload()},
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
    assert response.status_code == 500


async def test_restart_loads_declared_result_and_continues_typed_state(
    tmp_path: Path,
    forbid_imports: None,
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


def test_extra_types_register_independently_stored_models() -> None:
    class Stored(BaseModel):
        value: int = 0

    server = WorkflowServer(extra_types=[Stored])
    server.add_workflow("declared", DeclaredWorkflow())
    selected = workflow_decoder(server, "declared")
    value = Stored(value=5)
    assert selected.deserialize(selected.serialize(value)) == value


@pytest.mark.parametrize(
    ("qualified_name", "expected_type"),
    [
        ("builtins.ValueError", ValueError),
        ("workflows.errors.WorkflowRuntimeError", UnreconstructedException),
        ("unknown_metadata.Error", UnreconstructedException),
    ],
)
def test_server_context_retry_exception_uses_selected_serializer(
    tmp_path: Path,
    forbid_imports: None,
    qualified_name: str,
    expected_type: type[Exception],
) -> None:
    server = make_server(tmp_path / "exceptions.db")
    workflow = server.get_workflows()["declared"]
    decoder = workflow_decoder(server, "declared")
    event = InputEvent(nested=ExtraEvent())
    context = Context.from_dict(
        workflow,
        {
            "version": 2,
            "workers": {
                "start": {
                    "queue": [
                        {
                            "event": decoder.serialize(event),
                            "last_exception": {
                                "exception_type": qualified_name,
                                "exception_message": "retry failed",
                            },
                        }
                    ]
                }
            },
        },
    )
    assert isinstance(context._face, PreContext)
    error = context._face.init_snapshot.workers["start"].queue[0].last_exception
    assert isinstance(error, expected_type)


def test_registering_another_workflow_keeps_existing_decoder_restricted(
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


async def test_run_api_rejects_another_workflows_event() -> None:
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

    with pytest.raises(ValueError, match="Refusing to import"):
        decode_persistent_handler(data, result_decoder)
    data["workflow_name"] = "later"
    restored = decode_persistent_handler(data, result_decoder)
    assert isinstance(restored.result, LaterOutput)


async def test_registered_model_is_not_an_outer_event(forbid_imports: None) -> None:
    server = WorkflowServer(extra_types=[State])
    server.add_workflow("declared", DeclaredWorkflow())
    async with (
        server.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=server.app),
            base_url="http://test",
        ) as client,
    ):
        response = await client.post(
            "/workflows/declared/run",
            json={
                "start_event": {
                    "qualified_name": f"{State.__module__}.{State.__qualname__}",
                    "value": {},
                }
            },
        )
    assert response.status_code == 400
    assert "Event subclass" in response.text
