# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from typing import Any, Generic, Protocol, runtime_checkable

from pydantic import BaseModel
from typing_extensions import TypeVar

from . import state_store as _state_store
from .serializers import BaseSerializer
from .state_store import DictState, StateStore

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]


StateRecord = _state_store._StateRecord
"""Raw state record loaded and saved by a storage backend."""


@runtime_checkable
class StateStorage(_state_store._StateStorage, Protocol):
    """Integration protocol for raw workflow state persistence."""


class StateStoreFacade(_state_store._TypedStateStore[MODEL_T], Generic[MODEL_T]):
    """Typed state-store facade over raw persistence."""


def decode_seed_state(
    serialized_state: dict[str, Any], serializer: BaseSerializer
) -> BaseModel:
    """Decode a portable in-memory state seed."""
    return _state_store._decode_seed_state(serialized_state, serializer)


def string_record_from_state(
    state: BaseModel, serializer: BaseSerializer
) -> StateRecord:
    """Encode state into the string-backed storage record format."""
    record = _state_store._string_record_from_state(state, serializer)
    return StateRecord.model_validate(record.model_dump())


@runtime_checkable
class StateStoreSnapshotter(Protocol):
    """Integration protocol for stores that can emit portable state snapshots."""

    async def snapshot(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize portable state data."""
        ...


@runtime_checkable
class StateStoreHandleProvider(Protocol):
    """Integration protocol for durable stores that can emit reconnect handles."""

    def handle(self) -> dict[str, Any]:
        """Serialize reconnect metadata for durable storage."""
        ...


@runtime_checkable
class StateStorePreparer(Protocol):
    """Integration protocol for stores with async lazy materialization."""

    async def ensure_seeded(self) -> None:
        """Prepare storage before exposing a durable handle."""
        ...


async def state_store_handoff(
    store: StateStore[Any],
    serializer: BaseSerializer,
) -> dict[str, Any]:
    """Serialize a store for runtime handoff.

    Durable stores can return a reconnect handle. Snapshot-capable stores can
    return a portable state payload. Legacy stores fall back to ``to_dict``.
    """
    if isinstance(store, StateStorePreparer):
        await store.ensure_seeded()
    if isinstance(store, StateStoreHandleProvider):
        return store.handle()
    if isinstance(store, StateStoreSnapshotter):
        return await store.snapshot(serializer)
    return store.to_dict(serializer)


__all__ = [
    "StateRecord",
    "StateStorage",
    "StateStoreFacade",
    "StateStoreHandleProvider",
    "StateStorePreparer",
    "StateStoreSnapshotter",
    "decode_seed_state",
    "state_store_handoff",
    "string_record_from_state",
]
