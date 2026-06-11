# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""AgentDataStateStore — StateStore backed by the LlamaCloud Agent Data API."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Generic, Literal, cast

from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import DictState, _StateStorage
from workflows.context.state_store_integration import (
    StateRecord,
    StateStoreFacade,
    restored_run_id,
)

from .agent_data_client import AgentDataClient

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]

_FIELD_RUN_ID = "run_id"


class _AgentDataStateRecord(BaseModel):
    """Validates the shape persisted in the Agent Data API."""

    run_id: str
    data: str
    state_type: str | None = None
    state_module: str | None = None


class AgentDataSerializedState(BaseModel):
    """Serialized state referencing an agent data store."""

    store_type: Literal["agent_data"] = "agent_data"
    run_id: str
    collection: str = "workflow_state"


class _AgentDataStateStorage:
    """Raw state storage backed by the LlamaCloud Agent Data API.

    Uses a single item in a ``workflow_state`` collection, keyed by ``run_id``.
    Caches the item id (the run→item-id mapping is immutable) to avoid a
    search round-trip per operation.
    """

    def __init__(
        self,
        *,
        client: AgentDataClient,
        run_id: str,
        collection: str = "workflow_state",
    ) -> None:
        self._client = client
        self._run_id = run_id
        self._collection = collection
        self._item_id: str | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_AgentDataStateStorage]:
        # HTTP-backed: no per-call connections, the storage scopes itself.
        yield self

    async def _load_record(self) -> _AgentDataStateRecord | None:
        items = await self._client.search(
            self._collection,
            {_FIELD_RUN_ID: {"eq": self._run_id}},
            page_size=1,
        )
        if not items:
            return None
        self._item_id = items[0]["id"]
        return _AgentDataStateRecord.model_validate(items[0]["data"])

    async def load(self) -> StateRecord | None:
        record = await self._load_record()
        if record is None:
            return None
        return StateRecord(data=record.data)

    async def save(self, record: StateRecord) -> None:
        stored = _AgentDataStateRecord(
            run_id=self._run_id,
            data=record.data,
            state_type=record.state_type,
            state_module=record.state_module,
        )
        payload = stored.model_dump()
        if self._item_id is not None:
            await self._client.update_item(self._item_id, payload)
        else:
            items = await self._client.search(
                self._collection,
                {_FIELD_RUN_ID: {"eq": self._run_id}},
                page_size=1,
            )
            if items:
                item_id = items[0]["id"]
                self._item_id = item_id
                await self._client.update_item(item_id, payload)
            else:
                result = await self._client.create(self._collection, payload)
                self._item_id = result["id"]

    def to_handle(self) -> dict[str, Any]:
        payload = AgentDataSerializedState(
            run_id=self._run_id, collection=self._collection
        )
        return payload.model_dump()

    def parse_own_handle(
        self, payload: dict[str, Any]
    ) -> AgentDataSerializedState | None:
        if payload.get("store_type") != "agent_data":
            return None
        return AgentDataSerializedState.model_validate(payload)

    async def copy_from_handle(self, handle: AgentDataSerializedState) -> None:
        """Copy the source target's record into this one (no-op if absent).

        Goes through ``save`` so ``_item_id`` stays consistent with the
        copied row.
        """
        source = _AgentDataStateStorage(
            client=self._client,
            run_id=handle.run_id,
            collection=handle.collection,
        )
        record = await source.load()
        if record is None:
            return
        await self.save(record)


class AgentDataStateStore(StateStoreFacade[MODEL_T], Generic[MODEL_T]):
    """StateStore facade backed by Agent Data storage.

    Caches the decoded state model: the backend is a database over HTTP and
    reads are single-process, so reads after the first skip the round-trip
    and the re-decode. The cached instance is private — loads and saves
    exchange deep copies, never the cached object itself.
    """

    def __init__(
        self,
        *,
        client: AgentDataClient,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        collection: str = "workflow_state",
        serializer: BaseSerializer | None = None,
    ) -> None:
        super().__init__(
            _AgentDataStateStorage(client=client, run_id=run_id, collection=collection),
            state_type,
            serializer,
        )
        self._cached_state: MODEL_T | None = None

    async def ensure_seeded(self) -> None:
        if self._pending_seed is None:
            return
        await super().ensure_seeded()
        # Seed materialization can bypass _write_state (copy_from_handle),
        # so any materialized seed drops the cache.
        self._cached_state = None

    async def _load_state_or_none(
        self, storage: _StateStorage | None = None
    ) -> MODEL_T | None:
        # The cache check must come after seeding: a late re-staged seed
        # invalidates the cache in ensure_seeded.
        await self.ensure_seeded()
        if self._cached_state is not None:
            return self._cached_state.model_copy(deep=True)
        state = await super()._load_state_or_none(storage)
        if state is not None:
            self._cached_state = state.model_copy(deep=True)
        return state

    async def _write_state(
        self, state: BaseModel, storage: _StateStorage | None = None
    ) -> None:
        await super()._write_state(state, storage)
        # The caller still holds a reference to `state`; cache a private copy.
        self._cached_state = cast(MODEL_T, state.model_copy(deep=True))

    @classmethod
    def from_dict(
        cls,
        serialized_state: dict[str, Any],
        serializer: BaseSerializer,
        *,
        client: AgentDataClient,
        state_type: type[BaseModel] | None = None,
        run_id: str | None = None,
        collection: str | None = None,
    ) -> AgentDataStateStore[Any]:
        """Restore a state store from a serialized payload.

        Construct + seed: ``add_seed`` validates the payload eagerly
        (foreign durable handles raise) and materializes it lazily.
        """
        if not serialized_state:
            raise ValueError("Cannot restore AgentDataStateStore from empty dict")

        effective_collection = (
            collection or serialized_state.get("collection") or "workflow_state"
        )
        store: AgentDataStateStore[Any] = cls(
            client=client,
            run_id=restored_run_id(run_id, serialized_state),
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            collection=effective_collection,
            serializer=serializer,
        )
        store.add_seed(serialized_state, serializer)
        return store
