from __future__ import annotations

import asyncio
import weakref
from collections import deque
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import (
    DictState,
    InMemoryStateStore,
    StateStoreFacade,
)

from .abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    StoredEvent,
    StoredTick,
    is_terminal_status,
)


def _matches_query(handler: PersistentHandler, query: HandlerQuery) -> bool:
    # Empty lists should match nothing (short-circuit)
    if query.handler_id_in is not None:
        if len(query.handler_id_in) == 0:
            return False
        if handler.handler_id not in query.handler_id_in:
            return False

    if query.run_id_in is not None:
        if len(query.run_id_in) == 0:
            return False
        if handler.run_id not in query.run_id_in:
            return False

    if query.workflow_name_in is not None:
        if len(query.workflow_name_in) == 0:
            return False
        if handler.workflow_name not in query.workflow_name_in:
            return False

    if query.status_in is not None:
        if len(query.status_in) == 0:
            return False
        if handler.status not in query.status_in:
            return False

    if query.is_idle is not None:
        handler_is_idle = handler.idle_since is not None
        if query.is_idle != handler_is_idle:
            return False

    return True


class MemoryWorkflowStore(AbstractWorkflowStore):
    def __init__(self, max_completed: int | None = 1000) -> None:
        super().__init__()
        if max_completed is not None and max_completed < 0:
            raise ValueError("max_completed must be >= 0 or None")

        self.handlers: dict[str, PersistentHandler] = {}
        self.events: dict[str, list[StoredEvent]] = {}
        self.ticks: dict[str, list[StoredTick]] = {}
        # Strong refs: facades live until eviction. Public alias kept for
        # tests/plugins that inject stores; the ABC template reads the cache.
        self.state_stores: dict[tuple[str, tuple[str, ...]], StateStoreFacade[Any]] = {}
        self._state_store_cache = self.state_stores
        self._conditions: weakref.WeakValueDictionary[str, asyncio.Condition] = (
            weakref.WeakValueDictionary()
        )
        self.max_completed = max_completed
        self._terminal_queue: deque[str] = deque()

    def _build_state_store(
        self,
        run_id: str,
        namespace: tuple[str, ...],
        state_type: type[Any] | None,
        serializer: BaseSerializer | None,
    ) -> InMemoryStateStore[Any]:
        return InMemoryStateStore(state_type() if state_type else DictState())

    async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
        return [
            handler
            for handler in self.handlers.values()
            if _matches_query(handler, query)
        ]

    async def update(self, handler: PersistentHandler) -> None:
        self.handlers[handler.handler_id] = handler
        if is_terminal_status(handler.status):
            self._terminal_queue.append(handler.handler_id)
            self._evict_oldest_completed()

    async def delete(self, query: HandlerQuery) -> int:
        to_delete = [
            handler_id
            for handler_id, handler in list(self.handlers.items())
            if _matches_query(handler, query)
        ]
        for handler_id in to_delete:
            del self.handlers[handler_id]
        return len(to_delete)

    def _evict_oldest_completed(self) -> None:
        """Remove the oldest completed handlers when the cap is exceeded.

        Uses _terminal_queue (insertion-ordered deque) for O(1) eviction
        instead of scanning and sorting all handlers.
        """
        if self.max_completed is None:
            return

        while len(self._terminal_queue) > self.max_completed:
            handler_id = self._terminal_queue.popleft()
            handler = self.handlers.get(handler_id)
            if handler is None:
                # Already removed (e.g. via delete()), skip.
                continue
            if not is_terminal_status(handler.status):
                # Stale terminal-queue entry for a handler_id that was upserted
                # into a newer non-terminal row.
                continue

            self.handlers.pop(handler_id, None)
            run_id = handler.run_id
            if run_id is not None:
                self.events.pop(run_id, None)
                self.ticks.pop(run_id, None)
                self._evict_run_state_stores(run_id)

    def _get_or_create_condition(self, run_id: str) -> asyncio.Condition:
        cond = self._conditions.get(run_id)
        if cond is None:
            cond = asyncio.Condition()
            self._conditions[run_id] = cond
        return cond

    async def append_event(self, run_id: str, event: EventEnvelopeWithMetadata) -> None:
        if run_id not in self.events:
            self.events[run_id] = []
        existing = self.events[run_id]
        next_seq = (existing[-1].sequence + 1) if existing else 0
        stored = StoredEvent(
            run_id=run_id,
            sequence=next_seq,
            timestamp=datetime.now(timezone.utc),
            event=event,
        )
        existing.append(stored)
        condition = self._conditions.get(run_id)
        if condition is not None:
            async with condition:
                condition.notify_all()

    async def query_events(
        self,
        run_id: str,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        events = self.events.get(run_id, [])
        if after_sequence is not None:
            events = [e for e in events if e.sequence > after_sequence]
        if limit is not None:
            events = events[:limit]
        return events

    async def append_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        if run_id not in self.ticks:
            self.ticks[run_id] = []
        existing = self.ticks[run_id]
        next_seq = (existing[-1].sequence + 1) if existing else 0
        stored = StoredTick(
            run_id=run_id,
            sequence=next_seq,
            timestamp=datetime.now(timezone.utc),
            tick_data=tick_data,
        )
        existing.append(stored)

    async def get_ticks(self, run_id: str) -> list[StoredTick]:
        return list(self.ticks.get(run_id, []))

    async def subscribe_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        """Condition-based subscription — no polling.

        Uses list-index cursoring rather than sequence-field cursoring to
        handle duplicate sequence numbers (which occur when multiple internal
        adapters share the same run_id).
        """
        # Determine starting index: skip events with sequence <= after_sequence
        all_events = self.events.get(run_id, [])
        if after_sequence >= 0:
            cursor = 0
            for i, e in enumerate(all_events):
                if e.sequence <= after_sequence:
                    cursor = i + 1
        else:
            cursor = 0

        condition = self._get_or_create_condition(run_id)

        while True:
            async with condition:
                all_events = self.events.get(run_id, [])
                batch = all_events[cursor:]
                if not batch:
                    await condition.wait()
                    continue

            for event in batch:
                yield event
                cursor += 1
                if self._is_terminal_event(event):
                    return
