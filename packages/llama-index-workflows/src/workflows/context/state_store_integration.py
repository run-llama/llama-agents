# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from . import state_store as _state_store
from .serializers import BaseSerializer

StateRecord = _state_store._StateRecord
StateStorage = _state_store._StateStorage
StateStoreFacade = _state_store._TypedStateStore
decode_seed_state = _state_store._decode_seed_state
string_record_from_state = _state_store._string_record_from_state


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


__all__ = [
    "StateRecord",
    "StateStorage",
    "StateStoreFacade",
    "StateStoreHandleProvider",
    "StateStoreSnapshotter",
    "decode_seed_state",
    "string_record_from_state",
]
