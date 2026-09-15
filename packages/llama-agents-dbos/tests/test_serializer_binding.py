# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from llama_agents.dbos.runtime import DBOSWorkflowStore, InternalDBOSAdapter
from llama_agents.server import MemoryWorkflowStore
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


def test_dbos_store_propagates_result_decoder_before_and_after_resolution() -> None:
    def decoder(workflow_name: str) -> JsonSerializer:
        return JsonSerializer()

    before_inner = MemoryWorkflowStore()
    before = DBOSWorkflowStore(lambda: before_inner)
    before.result_decoder = decoder
    assert before._resolve() is before_inner
    assert before_inner.result_decoder is decoder

    after_inner = MemoryWorkflowStore()
    after = DBOSWorkflowStore(lambda: after_inner)
    assert after._resolve() is after_inner
    after.result_decoder = decoder
    assert after_inner.result_decoder is decoder
