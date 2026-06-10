# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from typing import Any

from .serializers import BaseSerializer
from .state_store import (
    StateRecord,
    StateStorage,
    StateStore,
    StateStoreFacade,
    decode_seed_state,
    restored_run_id,
    string_record_from_state,
)


async def state_store_handoff(
    store: StateStore[Any],
    serializer: BaseSerializer,
) -> dict[str, Any]:
    """Serialize a store for runtime handoff.

    Facade-based stores self-describe through ``serialize_for_handoff``
    (durable reconnect handle or portable snapshot, the store decides).
    Legacy third-party stores fall back to ``to_dict``.
    """
    if isinstance(store, StateStoreFacade):
        return await store.serialize_for_handoff(serializer)
    return store.to_dict(serializer)


__all__ = [
    "StateRecord",
    "StateStorage",
    "StateStoreFacade",
    "decode_seed_state",
    "restored_run_id",
    "state_store_handoff",
    "string_record_from_state",
]
