# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""AgentDataStateStore — StateStore backed by the LlamaCloud Agent Data API."""

from __future__ import annotations

import asyncio
import functools
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Generic, Literal, cast

from pydantic import BaseModel
from typing_extensions import TypeVar
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    DictState,
    create_cleared_state,
    decode_seed_state,
    decode_state,
    encode_state_to_str,
    get_by_path,
    merge_state,
    set_by_path,
)

from .agent_data_client import AgentDataClient

logger = logging.getLogger(__name__)

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]

_FIELD_RUN_ID = "run_id"
_FIELD_DATA = "data"


class _StoredStateRecord(BaseModel):
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


class AgentDataStateStore(Generic[MODEL_T]):
    """StateStore backed by the LlamaCloud Agent Data API.

    Uses a single item in a ``workflow_state`` collection, keyed by ``run_id``.
    """

    state_type: type[MODEL_T]

    def __init__(
        self,
        *,
        client: AgentDataClient,
        run_id: str,
        state_type: type[MODEL_T] | None = None,
        collection: str = "workflow_state",
        serializer: BaseSerializer | None = None,
    ) -> None:
        self._client = client
        self._run_id = run_id
        self.state_type = state_type or DictState  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        self._collection = collection
        self._serializer = serializer or JsonSerializer()
        # Cache the agent data item ID once found
        self._item_id: str | None = None
        # Write-through state cache — avoids HTTP searches when state is
        # already known from a previous load or save.
        self._cached_state: MODEL_T | None = None
        self._pending_seed: tuple[dict[str, Any], BaseSerializer] | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    @functools.cached_property
    def _lock(self) -> asyncio.Lock:
        return asyncio.Lock()

    # ------------------------------------------------------------------
    # State serialization
    # ------------------------------------------------------------------

    def _create_default_state(self) -> MODEL_T:
        return self.state_type()

    # ------------------------------------------------------------------
    # Load / save through API
    # ------------------------------------------------------------------

    async def _load_record(self) -> _StoredStateRecord | None:
        items = await self._client.search(
            self._collection,
            {_FIELD_RUN_ID: {"eq": self._run_id}},
            page_size=1,
        )
        if not items:
            return None
        self._item_id = items[0]["id"]
        return _StoredStateRecord.model_validate(items[0]["data"])

    async def _flush_pending_seed(self) -> None:
        if self._pending_seed is None:
            return

        serialized_state, serializer = self._pending_seed
        self._pending_seed = None
        self._serializer = serializer
        store_type = serialized_state.get("store_type")

        if store_type == "agent_data":
            parsed = AgentDataSerializedState.model_validate(serialized_state)
            if parsed.collection == self._collection and parsed.run_id == self._run_id:
                return
            source = AgentDataStateStore(
                client=self._client,
                run_id=parsed.run_id,
                state_type=self.state_type,
                collection=parsed.collection,
                serializer=serializer,
            )
            state = await source._load_state()
            await self._save_state(state)
            return

        state = decode_seed_state(serialized_state, serializer)
        await self._save_state(state)

    async def _load_existing_state(self) -> MODEL_T | None:
        if self._cached_state is not None:
            return self._cached_state.model_copy()
        record = await self._load_record()
        if record is None:
            return None
        state = cast(
            MODEL_T,
            decode_state(
                record.data, record.state_type, record.state_module, self._serializer
            ),
        )
        self._cached_state = state
        return state.model_copy()

    async def _load_state(self) -> MODEL_T:
        await self._flush_pending_seed()
        state = await self._load_existing_state()
        if state is not None:
            return state
        default_state = self._create_default_state()
        await self._save_state(default_state)
        return default_state.model_copy()

    async def _load_state_or_none(self) -> MODEL_T | None:
        await self._flush_pending_seed()
        return await self._load_existing_state()

    async def _save_state(self, state: BaseModel) -> None:
        state_json, state_type_name, state_module = encode_state_to_str(
            state, self._serializer
        )
        record = _StoredStateRecord(
            run_id=self._run_id,
            data=state_json,
            state_type=state_type_name,
            state_module=state_module,
        )
        payload = record.model_dump()
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
        self._cached_state = state.model_copy()  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

    # ------------------------------------------------------------------
    # StateStore protocol
    # ------------------------------------------------------------------

    async def get_state(self) -> MODEL_T:
        return await self._load_state()

    async def set_state(self, state: MODEL_T) -> None:
        current = await self._load_state_or_none()
        if current is None:
            await self._save_state(state)
            return
        merged = merge_state(current, state)
        await self._save_state(merged)

    async def get(self, path: str, default: Any = ...) -> Any:
        state = await self._load_state()
        return get_by_path(state, path, default)

    async def set(self, path: str, value: Any) -> None:
        async with self.edit_state() as state:
            set_by_path(state, path, value)

    async def clear(self) -> None:
        cleared = create_cleared_state(self.state_type)
        await self._save_state(cleared)

    @asynccontextmanager
    async def edit_state(self) -> AsyncGenerator[MODEL_T, None]:
        async with self._lock:
            state = await self._load_state()
            yield state
            await self._save_state(state)

    def to_dict(self, serializer: BaseSerializer) -> dict[str, Any]:
        payload = AgentDataSerializedState(
            run_id=self._run_id, collection=self._collection
        )
        return payload.model_dump()

    @classmethod
    def from_dict(
        cls,
        serialized_state: dict[str, Any],
        serializer: BaseSerializer,
        *,
        client: AgentDataClient,
        state_type: type[BaseModel] | None = None,
        run_id: str | None = None,
    ) -> AgentDataStateStore[Any]:
        if not serialized_state:
            raise ValueError("Cannot restore AgentDataStateStore from empty dict")
        parsed = AgentDataSerializedState.model_validate(serialized_state)
        effective_run_id = run_id or parsed.run_id
        return cls(
            client=client,
            run_id=effective_run_id,
            state_type=state_type,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            collection=parsed.collection,
            serializer=serializer,
        )
