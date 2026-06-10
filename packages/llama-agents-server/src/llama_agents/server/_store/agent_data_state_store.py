# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""AgentDataStateStore — StateStore backed by the LlamaCloud Agent Data API."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Generic, Literal

from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import DictState
from workflows.context.state_store_integration import (
    StateRecord,
    StateStoreFacade,
    decode_seed_state,
    string_record_from_state,
)

from .agent_data_client import AgentDataClient

logger = logging.getLogger(__name__)

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]

_FIELD_RUN_ID = "run_id"
_FIELD_DATA = "data"


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
    """

    def __init__(
        self,
        *,
        client: AgentDataClient,
        run_id: str,
        collection: str = "workflow_state",
        pending_seed: tuple[dict[str, Any], BaseSerializer] | None = None,
    ) -> None:
        self._client = client
        self._run_id = run_id
        self._collection = collection
        self._pending_seed = pending_seed
        self._seed_lock = asyncio.Lock()
        self._item_id: str | None = None
        self._cached_record: StateRecord | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    # ------------------------------------------------------------------
    # Load / save through API
    # ------------------------------------------------------------------

    async def ensure_seeded(self) -> None:
        if self._pending_seed is None:
            return
        async with self._seed_lock:
            if self._pending_seed is None:
                return
            serialized_state, serializer = self._pending_seed
            self._pending_seed = None
            store_type = serialized_state.get("store_type")

            if store_type == "agent_data":
                parsed = AgentDataSerializedState.model_validate(serialized_state)
                if (
                    parsed.collection == self._collection
                    and parsed.run_id == self._run_id
                ):
                    return
                source = _AgentDataStateStorage(
                    client=self._client,
                    run_id=parsed.run_id,
                    collection=parsed.collection,
                )
                record = await source._load_without_seed()
                if record is not None:
                    await self._save_without_seed(record)
                return

            state = decode_seed_state(serialized_state, serializer)
            await self._save_without_seed(string_record_from_state(state, serializer))

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
        await self.ensure_seeded()
        return await self._load_without_seed()

    async def _load_without_seed(self) -> StateRecord | None:
        if self._cached_record is not None:
            return self._cached_record.model_copy(deep=True)
        record = await self._load_record()
        if record is None:
            return None
        self._cached_record = StateRecord(
            data=record.data,
            state_type=record.state_type,
            state_module=record.state_module,
        )
        return self._cached_record.model_copy(deep=True)

    async def save(self, record: StateRecord) -> None:
        await self.ensure_seeded()
        await self._save_without_seed(record)

    async def _save_without_seed(self, record: StateRecord) -> None:
        # json.dumps raises TypeError for non-JSON data rather than
        # silently writing a Python repr into the stored record.
        data = record.data if isinstance(record.data, str) else json.dumps(record.data)
        stored = _AgentDataStateRecord(
            run_id=self._run_id,
            data=data,
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
        self._cached_record = StateRecord(
            data=stored.data,
            state_type=stored.state_type,
            state_module=stored.state_module,
        )

    def to_handle(self) -> dict[str, Any]:
        payload = AgentDataSerializedState(
            run_id=self._run_id, collection=self._collection
        )
        return payload.model_dump()


class AgentDataStateStore(StateStoreFacade[MODEL_T], Generic[MODEL_T]):
    """Compatibility StateStore facade backed by Agent Data storage."""

    def __init__(
        self,
        *,
        client: AgentDataClient,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        collection: str = "workflow_state",
        serializer: BaseSerializer | None = None,
        pending_seed: tuple[dict[str, Any], BaseSerializer] | None = None,
    ) -> None:
        self._agent_data_storage = _AgentDataStateStorage(
            client=client,
            run_id=run_id,
            collection=collection,
            pending_seed=pending_seed,
        )
        super().__init__(
            self._agent_data_storage,
            state_type or DictState,  # type: ignore[arg-type]
            serializer or JsonSerializer(),
            to_dict_mode="handle",
        )

    @property
    def run_id(self) -> str:
        return self._agent_data_storage.run_id

    async def _save_state(self, state: BaseModel) -> None:
        await self.ensure_seeded()
        await self._agent_data_storage.save(
            string_record_from_state(state, self._serializer)
        )

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
        if not serialized_state:
            raise ValueError("Cannot restore AgentDataStateStore from empty dict")
        store_type = serialized_state.get("store_type")

        if store_type == "agent_data":
            parsed = AgentDataSerializedState.model_validate(serialized_state)
            effective_run_id = run_id or parsed.run_id
            effective_collection = collection or parsed.collection
            return cls(
                client=client,
                run_id=effective_run_id,
                state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
                collection=effective_collection,
                serializer=serializer,
                pending_seed=(
                    (serialized_state, serializer)
                    if (
                        parsed.run_id != effective_run_id
                        or parsed.collection != effective_collection
                    )
                    else None
                ),
            )

        if store_type not in (None, "in_memory"):
            raise ValueError(
                f"Cannot restore store_type '{store_type}' with AgentDataStateStore.from_dict()"
            )

        effective_run_id = run_id or str(uuid.uuid4())
        return cls(
            client=client,
            run_id=effective_run_id,
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            collection=collection or "workflow_state",
            serializer=serializer,
            pending_seed=(serialized_state, serializer),
        )
