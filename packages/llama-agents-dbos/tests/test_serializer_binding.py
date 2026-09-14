# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from llama_agents.dbos.runtime import InternalDBOSAdapter
from llama_agents.server._store.sqlite.sqlite_state_store import SqliteStateStore
from sqlalchemy.engine import Engine
from workflows.context.serializers import PickleSerializer


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
