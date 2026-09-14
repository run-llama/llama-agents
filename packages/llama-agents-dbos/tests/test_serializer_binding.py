# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from llama_agents.dbos.runtime import DBOSWorkflowStore, InternalDBOSAdapter
from llama_agents.server import MemoryWorkflowStore
from llama_agents.server._store.abstract_workflow_store import (
    HandlerQuery,
    HandlerResultDecoder,
    PersistentHandler,
)
from llama_agents.server._store.sqlite.sqlite_state_store import SqliteStateStore
from sqlalchemy.engine import Engine
from workflows.context.serializers import JsonSerializer, PickleSerializer


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


async def test_dbos_store_forwards_result_decoder() -> None:
    class StubStore(MemoryWorkflowStore):
        def __init__(self) -> None:
            super().__init__()
            self.decoder: HandlerResultDecoder | None = None

        async def query(
            self,
            query: HandlerQuery,
            *,
            result_decoder: HandlerResultDecoder | None = None,
        ) -> list[PersistentHandler]:
            self.decoder = result_decoder
            return []

    inner = StubStore()
    store = DBOSWorkflowStore(lambda: inner)

    def decoder(workflow_name: str) -> JsonSerializer:
        return JsonSerializer()

    assert await store.query(HandlerQuery(), result_decoder=decoder) == []
    assert inner.decoder is decoder
