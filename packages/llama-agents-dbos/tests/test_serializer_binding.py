from __future__ import annotations

from typing import Any

import pytest
from llama_agents.dbos.idle_release import DBOSIdleReleaseDecorator
from llama_agents.dbos.runtime import DBOSRuntime, InternalDBOSAdapter
from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from llama_agents.server._store.abstract_workflow_store import (
    PersistentHandler,
    decode_persistent_handler,
)
from llama_agents.server._store.sqlite.sqlite_state_store import SqliteStateStore
from sqlalchemy.engine import Engine
from workflows import Workflow, step
from workflows.context.serializers import JsonSerializer, PickleSerializer
from workflows.events import StartEvent, StopEvent


async def test_dbos_state_facades_share_selected_serializer(
    journal_db_path: str, sqlite_engine: Engine
) -> None:
    serializer = PickleSerializer()
    adapter = InternalDBOSAdapter(
        run_id="serializer-run",
        engine=sqlite_engine,
        db_path=journal_db_path,
        serializer=serializer,
    )
    for namespace in [(), ("child",)]:
        store = adapter.get_state_store(namespace)
        assert isinstance(store, SqliteStateStore)
        assert store._serializer is serializer
        await store.set("value", complex(1, 2))
        assert await store.get("value") == complex(1, 2)
        assert adapter.get_state_store(namespace) is store


class BoundWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="ok")


class HiddenStop(StopEvent):
    pass


@pytest.mark.parametrize("explicit_decoder", [False, True])
def test_prebuilt_dbos_chain_selects_public_decoder_from_outer_binding(
    monkeypatch: pytest.MonkeyPatch, explicit_decoder: bool
) -> None:
    selected = JsonSerializer(allowed_types=[HiddenStop], dynamic_import=False)
    chain = DBOSRuntime().build_server_runtime(
        result_decoder=(lambda name: selected) if explicit_decoder else None
    )
    assert isinstance(chain, DBOSIdleReleaseDecorator)
    internal = PickleSerializer()
    server = WorkflowServer(
        runtime=chain,
        workflow_store=MemoryWorkflowStore(),
        serializer=internal,
        json_serializer=JsonSerializer(
            allowed_types=[StartEvent, StopEvent], dynamic_import=False
        ),
    )
    workflow = BoundWorkflow()
    server.add_workflow("bound", workflow)
    assert workflow.runtime.get_serializer(workflow) is internal
    assert chain.get_json_serializer(workflow) is not internal
    assert chain._get_result_decoder("bound") is (
        selected if explicit_decoder else server.json_serializer
    )

    def forbid_import(name: str) -> Any:
        pytest.fail(f"Unexpected metadata import: {name}")

    monkeypatch.setattr("workflows.context.utils.import_module", forbid_import)
    row = PersistentHandler(
        handler_id="stored",
        workflow_name="bound",
        status="completed",
        result=HiddenStop.model_validate({"result": "hidden"}),
    ).model_dump(mode="json")
    if explicit_decoder:
        handler = decode_persistent_handler(row, chain._get_result_decoder)
        assert isinstance(handler.result, HiddenStop)
    else:
        with pytest.raises(ValueError, match="Refusing to import"):
            decode_persistent_handler(row, chain._get_result_decoder)
        row["workflow_name"] = "missing"
        with pytest.raises(ValueError, match="Workflow missing not found"):
            decode_persistent_handler(row, chain._get_result_decoder)
