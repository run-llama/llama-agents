from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from llama_agents.server import WorkflowServer
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from pydantic import BaseModel
from workflows import Context, Workflow, step
from workflows.context.pre_context import PreContext
from workflows.context.serializers import JsonSerializer
from workflows.errors import WorkflowRuntimeError
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


def make_server(path: Path) -> WorkflowServer:
    server = WorkflowServer(
        workflow_store=SqliteWorkflowStore(db_path=str(path)),
        accept_context_api=True,
    )
    server.add_workflow("declared", DeclaredWorkflow(), additional_events=[ExtraEvent])
    return server


@pytest.fixture
async def client(
    tmp_path: Path,
) -> AsyncIterator[tuple[WorkflowServer, AsyncClient, Path]]:
    path = tmp_path / "server.db"
    server = make_server(path)
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


def test_explicit_dynamic_lookup_restores_legacy_json_default() -> None:
    dynamic = JsonSerializer()
    server = WorkflowServer(json_serializer=dynamic)
    server.add_workflow("declared", DeclaredWorkflow())
    workflow = server.get_workflows()["declared"]
    selected = workflow.runtime.get_serializer(workflow)
    value = State(count=3)
    assert selected.deserialize(selected.serialize(value)) == value
    assert server.json_serializer is dynamic


def test_extra_types_register_independently_stored_models() -> None:
    class Stored(BaseModel):
        value: int = 0

    server = WorkflowServer(extra_types=[Stored])
    server.add_workflow("declared", DeclaredWorkflow())
    selected = server.json_serializer
    value = Stored(value=5)
    assert selected.deserialize(selected.serialize(value)) == value


def test_declarations_follow_workflow_registration() -> None:
    server = WorkflowServer()
    name = f"{ExtraEvent.__module__}.{ExtraEvent.__name__}"
    with pytest.raises(ValueError, match="Refusing to import"):
        server.json_serializer.resolve_class(name)
    server.add_workflow("declared", DeclaredWorkflow(), additional_events=[ExtraEvent])
    assert server.json_serializer.resolve_class(name) is ExtraEvent
    # A captured registration snapshot survives transfer to another server.
    snapshot = server.json_serializer
    WorkflowServer().add_workflow("declared", server.get_workflows()["declared"])
    assert server.json_serializer is snapshot
    assert snapshot.resolve_class(name) is ExtraEvent


@pytest.mark.parametrize(
    ("qualified_name", "expected_type"),
    [
        ("builtins.ValueError", ValueError),
        ("workflows.errors.WorkflowRuntimeError", WorkflowRuntimeError),
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
    event = InputEvent(nested=ExtraEvent())
    context = Context.from_dict(
        workflow,
        {
            "version": 2,
            "workers": {
                "start": {
                    "queue": [
                        {
                            "event": server.json_serializer.serialize(event),
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


def test_registration_replaces_snapshot_without_changing_existing_decoders(
    forbid_imports: None,
) -> None:
    server = WorkflowServer()
    workflow = DeclaredWorkflow()
    server.add_workflow("first", workflow)
    old_context = Context(workflow)
    before = workflow.runtime.get_serializer(workflow)
    assert before is server.json_serializer
    server.add_workflow("second", DeclaredWorkflow(), additional_events=[ExtraEvent])
    after = workflow.runtime.get_serializer(workflow)
    assert after is server.json_serializer
    assert after is not before
    new_context = Context(workflow)
    assert isinstance(old_context._face, PreContext)
    assert isinstance(new_context._face, PreContext)
    assert old_context._face._serializer is before
    assert new_context._face._serializer is after
    assert workflow.runtime.get_json_serializer(workflow) is after
    value = ExtraEvent()
    with pytest.raises(ValueError, match="Refusing to import"):
        before.deserialize(before.serialize(value))
    assert after.deserialize(after.serialize(value)) == value


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


async def test_run_after_registration_uses_new_declarations(
    forbid_imports: None,
) -> None:
    server = WorkflowServer()
    server.add_workflow("first", DeclaredWorkflow())
    captured = server.json_serializer
    later = LaterWorkflow()
    server.add_workflow("later", later)
    event = LaterInput(value=8)
    with pytest.raises(ValueError, match="Refusing to import"):
        captured.deserialize(captured.serialize(event))
    async with server.contextmanager():
        handler = await server._runtime_core.run("later", start_event=event)
        completed = await server._service.await_workflow(handler)
        assert completed.result is not None
        assert completed.result.value["result"] == 8
