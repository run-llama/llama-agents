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
class StateStorage(_state_store._DurableStateStorage, Protocol):
    """Integration protocol for raw durable workflow state persistence."""


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


async def state_store_handoff(
    store: StateStore[Any],
    serializer: BaseSerializer,
) -> dict[str, Any]:
    """Serialize a store for runtime handoff.

    Facade-based stores self-describe through ``serialize_for_handoff``
    (durable reconnect handle or portable snapshot, the store decides).
    Legacy third-party stores fall back to ``to_dict``.
    """
    serialize = getattr(store, "serialize_for_handoff", None)
    if callable(serialize):
        return await serialize(serializer)
    return store.to_dict(serializer)


__all__ = [
    "StateRecord",
    "StateStorage",
    "StateStoreFacade",
    "decode_seed_state",
    "state_store_handoff",
    "string_record_from_state",
]
