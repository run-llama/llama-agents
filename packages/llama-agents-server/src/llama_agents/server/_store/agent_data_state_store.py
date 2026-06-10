# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""AgentDataStateStore — StateStore backed by the LlamaCloud Agent Data API."""

from __future__ import annotations

import uuid
from typing import Any, Generic, Literal

from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import DictState
from workflows.context.state_store_integration import (
    StateRecord,
    StateStoreFacade,
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
    Caches the item id and last-seen record — genuine backend I/O policy to
    avoid a search round-trip per operation.
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
        self._cached_record: StateRecord | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

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
        if self._cached_record is not None:
            return self._cached_record.model_copy(deep=True)
        record = await self._load_record()
        if record is None:
            return None
        self._cached_record = StateRecord(data=record.data)
        return self._cached_record.model_copy(deep=True)

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
        self._cached_record = StateRecord(data=stored.data)

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

        Goes through ``save`` so ``_cached_record``/``_item_id`` stay
        consistent with the copied row.
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
    """StateStore facade backed by Agent Data storage."""

    def __init__(
        self,
        *,
        client: AgentDataClient,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        collection: str = "workflow_state",
        serializer: BaseSerializer | None = None,
    ) -> None:
        self._agent_data_storage = _AgentDataStateStorage(
            client=client,
            run_id=run_id,
            collection=collection,
        )
        super().__init__(
            self._agent_data_storage,
            state_type or DictState,  # type: ignore[arg-type]
            serializer or JsonSerializer(),
        )

    @property
    def run_id(self) -> str:
        return self._agent_data_storage.run_id

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

        effective_run_id = run_id or serialized_state.get("run_id") or str(uuid.uuid4())
        effective_collection = (
            collection or serialized_state.get("collection") or "workflow_state"
        )
        store: AgentDataStateStore[Any] = cls(
            client=client,
            run_id=effective_run_id,
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            collection=effective_collection,
            serializer=serializer,
        )
        store.add_seed(serialized_state, serializer)
        return store
