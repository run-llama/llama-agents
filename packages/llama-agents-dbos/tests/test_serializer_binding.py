# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import pytest
from dbos import DBOS, DBOSConfig
from httpx import ASGITransport, AsyncClient
from llama_agents.dbos.runtime import (
    DBOSRuntime,
    DBOSWorkflowStore,
    InternalDBOSAdapter,
)
from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from llama_agents.server._store.sqlite.sqlite_state_store import SqliteStateStore
from pydantic import BaseModel
from sqlalchemy.engine import Engine
from workflows import Context, Workflow, step
from workflows.context.serializers import JsonSerializer, PickleSerializer
from workflows.events import Event, StartEvent, StopEvent


class StoredValue(BaseModel):
    value: str


class ValueStored(Event):
    pass


class ExampleWorkflow(Workflow):
    @step
    async def store_value(self, ctx: Context, ev: StartEvent) -> ValueStored:
        await ctx.store.set("value", StoredValue(value="restored"))
        return ValueStored()

    @step
    async def restore_value(self, ctx: Context, ev: ValueStored) -> StopEvent:
        value = await ctx.store.get("value")
        return StopEvent(result=value.value)


async def test_server_open_serializer_restores_undeclared_value_through_dbos(
    journal_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config: DBOSConfig = {
        "name": "serializer-binding",
        "system_database_url": f"sqlite+pysqlite:///{journal_db_path}",
        "run_admin_server": False,
        "notification_listener_polling_interval_sec": 0.01,
    }
    DBOS(config=config)
    runtime = DBOSRuntime(polling_interval_sec=0.01)

    def fail_inner_lookup(_workflow: Workflow) -> JsonSerializer:
        raise AssertionError("DBOSRuntime.get_serializer should not be called")

    monkeypatch.setattr(runtime, "get_serializer", fail_inner_lookup)
    server = WorkflowServer(runtime=runtime, serializer=JsonSerializer())
    server.add_workflow("restore", ExampleWorkflow())

    async with (
        server.contextmanager(),
        AsyncClient(
            transport=ASGITransport(app=server.app), base_url="http://test"
        ) as client,
    ):
        response = await client.post("/workflows/restore/run", json={})

    assert response.status_code == 200, response.text
    assert response.json()["result"]["value"]["result"] == "restored"


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


def test_dbos_store_propagates_result_decoder_before_resolution() -> None:
    def decoder(workflow_name: str) -> JsonSerializer:
        return JsonSerializer()

    before_inner = MemoryWorkflowStore()
    before = DBOSWorkflowStore(lambda: before_inner)
    before.result_decoder = decoder
    assert before._resolve() is before_inner
    assert before_inner.result_decoder is decoder
