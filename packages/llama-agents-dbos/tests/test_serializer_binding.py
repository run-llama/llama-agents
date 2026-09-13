from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from llama_agents.dbos.runtime import DBOSWorkflowStore, InternalDBOSAdapter
from llama_agents.server._store.abstract_workflow_store import (
    HandlerQuery,
    PersistentHandler,
    _handler_result_decoding_failed,
    query_handlers,
)
from llama_agents.server._store.sqlite.sqlite_state_store import SqliteStateStore
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from sqlalchemy.engine import Engine
from workflows.context.serializers import JsonSerializer, PickleSerializer
from workflows.events import StopEvent


class DBOSStopEvent(StopEvent):
    pass


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


async def test_dbos_store_forwards_restricted_result_decoder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "handlers.db"
    inner = SqliteWorkflowStore(db_path=str(path))
    store = DBOSWorkflowStore(lambda: inner)
    await store.start()
    for handler_id in ("healthy", "rejected"):
        await store.update(
            PersistentHandler(
                handler_id=handler_id,
                workflow_name="dbos",
                status="completed",
                result=DBOSStopEvent.model_validate({"result": handler_id}),
            )
        )

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE handlers SET result = ? WHERE handler_id = ?",
            (
                json.dumps(
                    {
                        "__is_pydantic": True,
                        "qualified_name": "unregistered_payload.Result",
                        "value": {},
                    }
                ),
                "rejected",
            ),
        )

    def fail(name: str) -> Any:
        pytest.fail(f"Unexpected metadata import: {name}")

    monkeypatch.setattr("workflows.context.utils.import_module", fail)
    decoder_calls: list[str] = []

    def result_decoder(workflow_name: str) -> JsonSerializer:
        decoder_calls.append(workflow_name)
        return JsonSerializer(allowed_types=[DBOSStopEvent])

    handlers = {
        handler.handler_id: handler
        for handler in await query_handlers(
            store, HandlerQuery(), result_decoder=result_decoder
        )
    }
    assert decoder_calls == ["dbos", "dbos"]
    assert isinstance(handlers["healthy"].result, DBOSStopEvent)
    assert handlers["healthy"].result.result == "healthy"
    assert handlers["rejected"].result is None
    assert _handler_result_decoding_failed(handlers["rejected"])
