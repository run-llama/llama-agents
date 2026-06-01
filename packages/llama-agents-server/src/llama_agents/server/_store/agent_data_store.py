# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""AgentDataStore — AbstractWorkflowStore backed by the LlamaCloud Agent Data API."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import StateStore

from .._keyed_lock import KeyedLock
from .._lru_cache import LRUCache
from .abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    StoredEvent,
    StoredTick,
)
from .agent_data_client import AgentDataClient
from .agent_data_state_store import AgentDataStateStore

logger = logging.getLogger(__name__)

_TICK_PAGE_SIZE = 100


class AgentDataStore(AbstractWorkflowStore):
    """Workflow store backed by the LlamaCloud Agent Data API.

    Optimized for streaming performance:
    - Same-process subscribers receive events via in-memory queues (no HTTP).
    - Tick and event writes are fire-and-forget, gathered at step boundaries.
    - Terminal events gather all pending writes before cleanup.
    - HTTP connections are reused across operations.

    State stores are in-memory (``InMemoryStateStore``) — workflow state
    is reconstructed from ticks on reload, so cloud persistence of the
    mutable state object is unnecessary.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        project_id: str,
        deployment_name: str,
        collection: str = "workflow_contexts",
        poll_interval: float = 30.0,
    ) -> None:
        self._client = AgentDataClient(
            base_url=base_url,
            api_key=api_key,
            project_id=project_id,
            deployment_name=deployment_name,
        )
        self._collection = collection
        self.poll_interval = poll_interval

        self._events_collection = f"{collection}_events"
        self._ticks_collection = f"{collection}_ticks"

        self._id_cache: LRUCache[str, str] = LRUCache(maxsize=256)
        self._locks = KeyedLock()

        self._event_sequences: dict[str, int] = {}
        self._tick_sequences: dict[str, int] = {}
        self._event_seq_lock: asyncio.Lock | None = None
        self._tick_seq_lock: asyncio.Lock | None = None

        self._subscriber_queues: dict[str, list[asyncio.Queue[StoredEvent | None]]] = {}
        self._state_stores: dict[str, AgentDataStateStore[Any]] = {}
        self._pending_ticks: dict[str, list[asyncio.Task[Any]]] = {}
        self._pending_events: dict[str, list[asyncio.Task[Any]]] = {}

    def _get_event_seq_lock(self) -> asyncio.Lock:
        if self._event_seq_lock is None:
            self._event_seq_lock = asyncio.Lock()
        return self._event_seq_lock

    def _get_tick_seq_lock(self) -> asyncio.Lock:
        if self._tick_seq_lock is None:
            self._tick_seq_lock = asyncio.Lock()
        return self._tick_seq_lock

    # ------------------------------------------------------------------
    # In-memory subscriber helpers
    # ------------------------------------------------------------------

    def _add_subscriber_queue(self, run_id: str) -> asyncio.Queue[StoredEvent | None]:
        """Create and register a new subscriber queue for a run."""
        queue: asyncio.Queue[StoredEvent | None] = asyncio.Queue()
        self._subscriber_queues.setdefault(run_id, []).append(queue)
        return queue

    def _remove_subscriber_queue(
        self, run_id: str, queue: asyncio.Queue[StoredEvent | None]
    ) -> None:
        """Unregister a subscriber queue. Cleans up the list if empty."""
        queues = self._subscriber_queues.get(run_id)
        if queues is not None:
            try:
                queues.remove(queue)
            except ValueError:
                pass
            if not queues:
                del self._subscriber_queues[run_id]

    def _broadcast_to_subscribers(self, run_id: str, event: StoredEvent) -> None:
        """Deliver an event to all in-memory subscriber queues for a run."""
        for queue in self._subscriber_queues.get(run_id, ()):
            queue.put_nowait(event)

    def _track_pending(
        self,
        pending: dict[str, list[asyncio.Task[Any]]],
        run_id: str,
        collection: str,
        data: dict[str, Any],
    ) -> None:
        """Create a fire-and-forget task and track it in the pending dict."""
        task = asyncio.create_task(self._client.create(collection, data))
        tasks = pending.setdefault(run_id, [])
        tasks.append(task)
        if len(tasks) > 50:
            pending[run_id] = [t for t in tasks if not t.done()]

    @staticmethod
    async def _regroup(
        pending: dict[str, list[asyncio.Task[Any]]], run_id: str
    ) -> None:
        """Await all in-flight tasks for a run. Raises the first error."""
        tasks = pending.pop(run_id, [])
        if not tasks:
            return
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            raise errors[0]

    async def _regroup_ticks(self, run_id: str) -> None:
        await self._regroup(self._pending_ticks, run_id)

    async def _regroup_events(self, run_id: str) -> None:
        await self._regroup(self._pending_events, run_id)

    async def after_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        """Gather all in-flight tick and event writes for a run."""
        await self._regroup_ticks(run_id)
        await self._regroup_events(run_id)

    async def _cleanup_run(self, run_id: str) -> None:
        """Clean up pending writes and subscriber queues for a completed run."""
        await self._regroup_ticks(run_id)
        await self._regroup_events(run_id)
        # Signal subscribers that the run is done, then remove the key
        for queue in self._subscriber_queues.get(run_id, []):
            queue.put_nowait(None)
        self._subscriber_queues.pop(run_id, None)
        # Clean up sequence counters and cached state store
        self._event_sequences.pop(run_id, None)
        self._tick_sequences.pop(run_id, None)
        self._state_stores.pop(run_id, None)

    # ------------------------------------------------------------------
    # Sequence helpers
    # ------------------------------------------------------------------

    async def _max_sequence(self, collection: str, run_id: str) -> int:
        """Query the API for the max sequence in a collection for a run_id.

        Returns -1 if no items exist.
        """
        items = await self._client.search(
            collection,
            {"run_id": {"eq": run_id}},
            page_size=1,
            order_by="sequence desc",
        )
        if items:
            return items[0]["data"].get("sequence", -1)
        return -1

    # ------------------------------------------------------------------
    # Handler CRUD
    # ------------------------------------------------------------------

    @staticmethod
    def _in_filter(field: str, values: list[Any] | None) -> tuple[bool, dict[str, Any]]:
        """Build an ``includes`` filter for *field*.

        Returns ``(ok, filter_fragment)`` where *ok* is ``False`` when the
        caller should short-circuit with "match nothing" (empty list).
        """
        if values is None:
            return True, {}
        if len(values) == 0:
            return False, {}
        return True, {field: {"includes": values}}

    @staticmethod
    def _build_handler_filters(query: HandlerQuery) -> dict[str, Any] | None:
        """Convert a HandlerQuery to Agent Data API filter format.

        Returns ``{}`` for "match everything" or ``None`` for "match nothing".
        """
        filters: dict[str, Any] = {}

        for field, values in [
            ("handler_id", query.handler_id_in),
            ("run_id", query.run_id_in),
            ("workflow_name", query.workflow_name_in),
            ("status", query.status_in),
        ]:
            ok, fragment = AgentDataStore._in_filter(field, values)
            if not ok:
                return None
            filters.update(fragment)

        if query.is_idle is not None:
            if query.is_idle:
                filters["idle_since"] = {"ne": None}
            else:
                filters["idle_since"] = {"eq": None}

        return filters

    @staticmethod
    def _item_to_handler(item: dict[str, Any]) -> PersistentHandler:
        """Convert an Agent Data API item to a PersistentHandler."""
        data = item["data"]
        return PersistentHandler.model_validate(data)

    async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
        filters = self._build_handler_filters(query)
        if filters is None:
            return []

        items = await self._client.search(self._collection, filters or None)
        handlers = [self._item_to_handler(item) for item in items]
        return handlers

    async def update(self, handler: PersistentHandler) -> None:
        data = handler.model_dump(mode="json")
        handler_id = handler.handler_id

        async with self._locks(handler_id):
            # Check cache for existing agent_data_id
            cached_id = self._id_cache.get(handler_id)
            if cached_id is not None:
                await self._client.update_item(cached_id, data)
                return

            # Search for existing item. Sort newest-first so that items[-1]
            # is deterministically the oldest row — relied on by the
            # dedupe-survivor choice below.
            items = await self._client.search(
                self._collection,
                {"handler_id": {"eq": handler_id}},
                order_by="created_at desc",
            )
            if not items:
                result = await self._client.create(self._collection, data)
                self._id_cache.put(handler_id, result["id"])
                return

            if len(items) == 1:
                item_id = items[0]["id"]
                self._id_cache.put(handler_id, item_id)
                await self._client.update_item(item_id, data)
                return

            # Duplicate handler rows — converge to one survivor. The
            # invariant is one row per handler_id; run_id is a mutable field
            # on the row, so mismatched run_ids just mean an earlier run was
            # superseded. Collapse regardless. Oldest survivor preserves the
            # row's original created_at.
            survivor_id = items[-1]["id"]
            victim_ids = [item["id"] for item in items[:-1]]
            logger.warning(
                "Collapsing %d duplicate rows for handler %s; survivor=%s",
                len(items),
                handler_id,
                survivor_id,
            )
            # Tolerate partial delete failures. The next update() will see
            # whatever duplicates remain and retry; letting one failed delete
            # abort the whole operation would also skip the survivor write,
            # leaving the row stale.
            results = await asyncio.gather(
                *(self._client.delete_item(vid) for vid in victim_ids),
                return_exceptions=True,
            )
            for vid, outcome in zip(victim_ids, results):
                if isinstance(outcome, BaseException):
                    logger.warning(
                        "Failed to delete duplicate handler row %s for %s: %s",
                        vid,
                        handler_id,
                        outcome,
                    )
            self._id_cache.put(handler_id, survivor_id)
            await self._client.update_item(survivor_id, data)

    async def delete(self, query: HandlerQuery) -> int:
        filters = self._build_handler_filters(query)
        if filters is None:
            return 0

        # Invalidate cached IDs for matching handlers before bulk delete
        items = await self._client.search(self._collection, filters or None)
        for item in items:
            handler_id = item["data"].get("handler_id")
            if handler_id:
                self._id_cache.delete(handler_id)

        if not items:
            return 0

        return await self._client.delete_many(self._collection, filters or {})

    # ------------------------------------------------------------------
    # Event journal
    # ------------------------------------------------------------------

    async def _next_event_sequence(self, run_id: str) -> int:
        async with self._get_event_seq_lock():
            if run_id not in self._event_sequences:
                self._event_sequences[run_id] = await self._max_sequence(
                    self._events_collection, run_id
                )
            seq = self._event_sequences[run_id] + 1
            self._event_sequences[run_id] = seq
            return seq

    async def append_event(self, run_id: str, event: EventEnvelopeWithMetadata) -> None:
        seq = await self._next_event_sequence(run_id)
        now = datetime.now(timezone.utc)
        stored = StoredEvent(
            run_id=run_id,
            sequence=seq,
            timestamp=now,
            event=event,
        )

        # Instant in-memory delivery
        self._broadcast_to_subscribers(run_id, stored)

        # Fire-and-forget HTTP persistence
        self._track_pending(
            self._pending_events,
            run_id,
            self._events_collection,
            stored.model_dump(mode="json"),
        )

        if self._is_terminal_event(stored):
            await self._cleanup_run(run_id)

    async def query_events(
        self,
        run_id: str,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        await self._regroup_events(run_id)

        filters: dict[str, Any] = {"run_id": {"eq": run_id}}
        if after_sequence is not None:
            filters["sequence"] = {"gte": after_sequence + 1}

        items = await self._client.search(
            self._events_collection,
            filters,
            page_size=limit or 1000,
            order_by="sequence",
        )

        return [StoredEvent.model_validate(item["data"]) for item in items]

    # ------------------------------------------------------------------
    # Event subscription
    # ------------------------------------------------------------------

    async def subscribe_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[StoredEvent]:
        """In-memory queue-based subscription.

        Subscribes to the queue *before* running backfill to avoid losing
        events in the race window. Deduplicates by sequence number.
        """
        # Register queue before backfill to avoid race condition
        queue = self._add_subscriber_queue(run_id)
        try:
            cursor = after_sequence

            # Backfill: yield historical events already persisted
            backfill = await self.query_events(run_id, after_sequence=cursor)
            for event in backfill:
                yield event
                cursor = event.sequence
                if self._is_terminal_event(event):
                    return

            # Stream from in-memory queue
            while True:
                event = await queue.get()
                if event is None:
                    # Run completed, queue was signaled
                    return
                # Deduplicate: skip events already yielded in backfill
                if event.sequence <= cursor:
                    continue
                yield event
                cursor = event.sequence
                if self._is_terminal_event(event):
                    return
        finally:
            self._remove_subscriber_queue(run_id, queue)

    # ------------------------------------------------------------------
    # Tick journal
    # ------------------------------------------------------------------

    async def _next_tick_sequence(self, run_id: str) -> int:
        async with self._get_tick_seq_lock():
            if run_id not in self._tick_sequences:
                self._tick_sequences[run_id] = await self._max_sequence(
                    self._ticks_collection, run_id
                )
            seq = self._tick_sequences[run_id] + 1
            self._tick_sequences[run_id] = seq
            return seq

    async def append_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        seq = await self._next_tick_sequence(run_id)
        now = datetime.now(timezone.utc)
        stored = StoredTick(
            run_id=run_id,
            sequence=seq,
            timestamp=now,
            tick_data=tick_data,
        )

        # Fire-and-forget: tick creates run in the background so they don't
        # block the control loop.  Failures surface at _regroup_ticks time.
        self._track_pending(
            self._pending_ticks,
            run_id,
            self._ticks_collection,
            stored.model_dump(mode="json"),
        )

    async def get_ticks(self, run_id: str) -> list[StoredTick]:
        return [t async for t in self.stream_ticks(run_id)]

    async def stream_ticks(self, run_id: str) -> AsyncIterator[StoredTick]:
        await self._regroup_ticks(run_id)
        cursor: int | None = None
        while True:
            filters: dict[str, Any] = {"run_id": {"eq": run_id}}
            if cursor is not None:
                filters["sequence"] = {"gt": cursor}
            page = await self._client.search(
                self._ticks_collection,
                filters,
                page_size=_TICK_PAGE_SIZE,
                order_by="sequence",
            )
            for item in page:
                tick = StoredTick.model_validate(item["data"])
                yield tick
                cursor = tick.sequence
            if len(page) < _TICK_PAGE_SIZE:
                return

    # ------------------------------------------------------------------
    # State store
    # ------------------------------------------------------------------

    def _create_agent_data_state_store(
        self,
        run_id: str,
        state_type: type[Any] | None,
        serializer: BaseSerializer | None,
    ) -> AgentDataStateStore[Any]:
        return AgentDataStateStore(
            client=self._client,
            run_id=run_id,
            state_type=state_type,
            collection=f"{self._collection}_state",
            serializer=serializer,
        )

    def create_state_store(
        self,
        run_id: str,
        state_type: type[Any] | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> StateStore[Any]:
        cached = self._state_stores.get(run_id)
        if cached is not None:
            return cached
        reconnecting = (
            serialized_state is not None
            and serializer is not None
            and serialized_state.get("store_type") == "agent_data"
            and serialized_state.get("run_id") == run_id
        )
        if reconnecting:
            store = AgentDataStateStore.from_dict(
                serialized_state,
                serializer,
                client=self._client,
                state_type=state_type,
                run_id=run_id,
            )
        else:
            store = self._create_agent_data_state_store(run_id, state_type, serializer)
            if serialized_state is not None and serializer is not None:
                store._pending_seed = (serialized_state, serializer)
        self._state_stores[run_id] = store
        return store
