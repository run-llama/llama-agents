# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
import logging
import weakref
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, MutableMapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, Protocol, runtime_checkable

from llama_agents.client.protocol.serializable_events import (
    EventEnvelopeWithMetadata,
)
from pydantic import (
    BaseModel,
    field_serializer,
    field_validator,
)
from workflows.context import JsonSerializer
from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import DictState, StateStore, StateStoreFacade
from workflows.events import StopEvent
from workflows.runtime.types.ticks import WorkflowTick, WorkflowTickAdapter

logger = logging.getLogger(__name__)

Status = Literal["running", "completed", "failed", "cancelled"]

TERMINAL_STATUSES: frozenset[Status] = frozenset(("completed", "failed", "cancelled"))


def is_terminal_status(status: Status) -> bool:
    return status in TERMINAL_STATUSES


class _Unset(Enum):
    UNSET = "UNSET"


_UNSET = _Unset.UNSET


@dataclass()
class HandlerQuery:
    # Matches if any of the handler_ids match
    handler_id_in: list[str] | None = None
    # Matches if any of the run_ids match
    run_id_in: list[str] | None = None
    # Matches if any of the workflow_names match
    workflow_name_in: list[str] | None = None
    # Matches if the status flag matches
    status_in: list[Status] | None = None
    # True = only idle handlers, False = only non-idle handlers, None = all
    is_idle: bool | None = None


class PersistentHandler(BaseModel):
    handler_id: str
    workflow_name: str
    status: Status
    run_id: str | None = None
    error: str | None = None
    result: StopEvent | None = None
    started_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None
    idle_since: datetime | None = None

    @field_validator("result", mode="before")
    @classmethod
    def _parse_stop_event(cls, data: Any) -> StopEvent | None:
        if isinstance(data, StopEvent):
            return data
        elif isinstance(data, dict):
            deserialized = JsonSerializer().deserialize_value(data)
            if isinstance(deserialized, StopEvent):
                return deserialized
            else:
                return StopEvent(result=data)
        elif data is None:
            return None
        else:
            return StopEvent(result=data)

    @field_serializer("result", mode="plain")
    def _serialize_stop_event(self, data: StopEvent | None) -> Any:
        if data is None:
            return None
        result = JsonSerializer().serialize_value(data)
        return result


class StoredTick(BaseModel):
    run_id: str
    sequence: int
    timestamp: datetime
    tick_data: dict[str, Any]


class StoredEvent(BaseModel):
    run_id: str
    sequence: int
    timestamp: datetime
    event: EventEnvelopeWithMetadata


class AbstractWorkflowStore(ABC):
    poll_interval: float = 0.1

    def __init__(self) -> None:
        # Per-run facade cache: the single memoization site for state stores.
        # Weak-valued by default so facades die with their last consumer.
        # Backends needing a different lifecycle (strong refs + explicit
        # eviction) assign a plain dict in their __init__.
        self._state_store_cache: MutableMapping[
            tuple[str, tuple[str, ...]], StateStoreFacade[Any]
        ] = weakref.WeakValueDictionary()

    async def start(self) -> None:
        """Initialize backend resources. Default is a no-op."""

    def create_state_store(
        self,
        run_id: str,
        state_type: type[Any] | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
        namespace: tuple[str, ...] = (),
    ) -> StateStore[Any]:
        """Return the per-(run, namespace) state store, caching on first use.

        Namespace is a key dimension alongside ``run_id``: the default ``()``
        is the root namespace and reproduces the single-record behavior. One
        facade per (run, namespace) per process, so its write lock is a real
        guarantee. If *serialized_state* is provided, it is staged as a seed on
        the (possibly already handed-out) facade: validation is eager, the I/O
        to materialize it stays lazy (first async state access or handoff).
        """
        cache_key = (run_id, namespace)
        store = self._state_store_cache.get(cache_key)
        if store is None:
            store = self._build_state_store(run_id, namespace, state_type, serializer)
            self._state_store_cache[cache_key] = store
        elif state_type is not None and store.state_type is DictState:
            # An earlier type-less caller (e.g. handler continuation) must
            # not shadow the workflow's concrete state type.
            store.state_type = state_type
        if serialized_state is not None:
            store.add_seed(serialized_state, serializer or JsonSerializer())
        return store

    def _evict_run_state_stores(self, run_id: str) -> None:
        """Drop every cached namespace facade for *run_id* (all namespaces)."""
        for key in [k for k in self._state_store_cache if k[0] == run_id]:
            self._state_store_cache.pop(key, None)

    @abstractmethod
    def _build_state_store(
        self,
        run_id: str,
        namespace: tuple[str, ...],
        state_type: type[Any] | None,
        serializer: BaseSerializer | None,
    ) -> StateStoreFacade[Any]:
        """Construct the backend facade for a (run, namespace). No caching."""

    @abstractmethod
    async def query(self, query: HandlerQuery) -> list[PersistentHandler]: ...

    @abstractmethod
    async def update(self, handler: PersistentHandler) -> None: ...

    @abstractmethod
    async def delete(self, query: HandlerQuery) -> int: ...

    @abstractmethod
    async def append_event(
        self, run_id: str, event: EventEnvelopeWithMetadata
    ) -> None: ...

    @abstractmethod
    async def query_events(
        self, run_id: str, after_sequence: int | None = None, limit: int | None = None
    ) -> list[StoredEvent]: ...

    @abstractmethod
    async def append_tick(self, run_id: str, tick_data: dict[str, Any]) -> None: ...

    @abstractmethod
    async def get_ticks(self, run_id: str) -> list[StoredTick]: ...

    async def stream_ticks(self, run_id: str) -> AsyncIterator[StoredTick]:
        """Async-iterate stored ticks in sequence order (ascending).

        Default loads all ticks via :meth:`get_ticks`. Override for true
        streaming (e.g. cursor-based pagination).
        """
        for tick in await self.get_ticks(run_id):
            yield tick

    async def after_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        """Called after a tick's commands have been processed.

        Stores can override to gather in-flight writes, update caches, etc.
        Default is no-op.
        """
        pass

    async def update_handler_status(
        self,
        run_id: str,
        *,
        status: Status | None = None,
        result: StopEvent | None = None,
        error: str | None = None,
        idle_since: datetime | None | _Unset = _UNSET,
    ) -> None:
        """Update status and related fields for an existing handler.

        Loads the handler by run_id, updates status/timestamps/provided fields,
        and writes back. If the handler is not found, logs a warning and returns.
        """
        found = await self.query(HandlerQuery(run_id_in=[run_id]))
        if not found:
            logger.warning("update_handler_status: run %s not found, skipping", run_id)
            return
        handler = found[0]
        now = datetime.now(timezone.utc)
        if status is not None:
            handler.status = status
        handler.updated_at = now
        if status in ("completed", "failed", "cancelled"):
            handler.completed_at = now
        if result is not None:
            handler.result = result
        if error is not None:
            handler.error = error
        if not isinstance(idle_since, _Unset):
            handler.idle_since = idle_since
        await self.update(handler)

    @staticmethod
    def _is_terminal_event(event: StoredEvent) -> bool:
        """Check if a stored event is terminal (StopEvent or subclass, etc.)."""

        if event.event.origin_namespace:
            return False
        types = (event.event.types or []) + [event.event.type]
        return StopEvent.__name__ in types

    async def subscribe_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        """Stream events starting after *after_sequence*, yielding in real time.

        The default implementation polls via :meth:`query_events`.
        :class:`MemoryWorkflowStore` overrides this with condition-based
        notification so there is no polling.

        The iterator terminates once a terminal event
        (``StopEvent``, ``WorkflowFailedEvent``, ``WorkflowCancelledEvent``)
        is yielded.
        """
        cursor = after_sequence
        while True:
            events = await self.query_events(run_id, after_sequence=cursor)
            for event in events:
                yield event
                cursor = event.sequence
                if self._is_terminal_event(event):
                    return
            if not events:
                await asyncio.sleep(self.poll_interval)


@runtime_checkable
class LegacyContextStore(Protocol):
    """Opt-in protocol for stores that can provide old serialized context data from the ctx column."""

    def get_legacy_ctx(self, run_id: str) -> dict[str, Any] | None:
        """Return the old serialized context dict for a run, or None if not available."""
        ...


def as_legacy_context_store(store: AbstractWorkflowStore) -> LegacyContextStore | None:
    """Return the store as a LegacyContextStore if it supports it, else None."""
    if isinstance(store, LegacyContextStore):
        return store
    return None


async def stream_workflow_ticks(
    store: AbstractWorkflowStore,
    run_id: str,
) -> AsyncIterator[WorkflowTick]:
    """Stream validated WorkflowTick objects for *run_id* from *store*."""
    async for stored in store.stream_ticks(run_id):
        yield WorkflowTickAdapter.validate_python(stored.tick_data)
