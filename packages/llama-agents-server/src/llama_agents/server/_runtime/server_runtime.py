# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
Server runtime decorator: the main required runtime decorator for workflows
served by the WorkflowServer. Handles event recording, handler persistence,
and status updates.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from llama_agents.client.protocol.serializable_events import (
    EventEnvelopeWithMetadata,
)
from typing_extensions import override
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    DictState,
    StateStore,
    StateStoreFacade,
    namespaced_seed_payloads,
    namespaced_state_types,
)
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
    WorkflowCancelledEvent,
    WorkflowFailedEvent,
    WorkflowTimedOutEvent,
)
from workflows.runtime.runtime_decorators import (
    BaseInternalRunAdapterDecorator,
    BaseRuntimeDecorator,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.invocation import slot_namespace
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    Runtime,
)
from workflows.runtime.types.ticks import WorkflowTick
from workflows.workflow import Workflow

from .._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    PersistentHandler,
    Status,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# _ServerInternalRunAdapter
# ---------------------------------------------------------------------------


class _ServerInternalRunAdapter(BaseInternalRunAdapterDecorator):
    """Internal adapter that records every emitted event to the workflow store.

    Handles event recording and terminal-event status updates.
    """

    def __init__(
        self,
        decorated: InternalRunAdapter,
        runtime: ServerRuntimeDecorator,
        *,
        state_types: dict[tuple[str, ...], type[Any]] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> None:
        super().__init__(decorated)
        self._runtime = runtime
        self._store = runtime._store
        # State types use static slot paths; runtime invocation paths project
        # onto them with slot_namespace.
        self._state_types = state_types or {}
        self._serializer = serializer
        self._state_stores: dict[tuple[str, ...], StateStore[Any]] = {}
        self._write_lock: asyncio.Lock | None = None
        # Seed resolution: split the run's snapshot into per-namespace seeds once.
        self._seeds_resolved = False
        self._namespace_seeds: dict[tuple[str, ...], dict[str, Any]] = {}
        self._seed_serializer: BaseSerializer | None = None
        # A durable handle can't be split per-namespace; it seeds the root only,
        # and the backend's copy_from_handle fans out to every namespace row.
        self._root_handle: tuple[dict[str, Any], BaseSerializer] | None = None
        self._root_handle_materialized = False

    def _resolve_seeds(self) -> None:
        if self._seeds_resolved:
            return
        self._seeds_resolved = True
        initial = self._runtime._initial_state.pop(self.run_id, None)
        if initial is None:
            return
        serialized_state, serializer = initial
        seeds = namespaced_seed_payloads(serialized_state, self._state_types)
        if seeds is None:
            # Durable handle (or empty): seed the root; copy_from_handle fans out.
            self._root_handle = (serialized_state, serializer)
        else:
            self._namespace_seeds = seeds
            self._seed_serializer = serializer

    @override
    def get_state_store(self, namespace: tuple[str, ...] = ()) -> StateStore[Any]:
        cached = self._state_stores.get(namespace)
        if cached is not None:
            return cached
        self._resolve_seeds()
        state_type = self._state_types.get(slot_namespace(namespace), DictState)
        serialized_state: dict[str, Any] | None = None
        serializer: BaseSerializer | None = self._serializer
        seed = self._namespace_seeds.get(namespace)
        if seed is not None:
            serialized_state, serializer = seed, self._seed_serializer
        elif namespace == () and self._root_handle is not None:
            serialized_state, serializer = self._root_handle
        store = self._store.create_state_store(
            self.run_id,
            state_type,
            serialized_state,
            serializer,
            namespace=namespace,
        )
        self._state_stores[namespace] = store
        return store

    @override
    async def on_tick(self, tick: WorkflowTick) -> None:
        # Fan a durable state handle out to every namespace row before any step
        # runs. Child stores are created lazily on first ``ctx.store`` access;
        # if that first access happens inside a child (e.g. a fork whose parent
        # start step never touches the root store), the root store might never
        # materialize and the backend's copy_from_handle never fires, leaving the
        # child reading empty state. Eagerly materializing the root at the first
        # tick — before ``process_command`` starts any step task — closes the
        # gap. Mirrors the DBOS runtime's eager seed in ``run_workflow``.
        await self._ensure_root_handle_materialized()
        await super().on_tick(tick)

    async def _ensure_root_handle_materialized(self) -> None:
        if self._root_handle_materialized:
            return
        self._root_handle_materialized = True
        self._resolve_seeds()
        if self._root_handle is None:
            return
        store = self.get_state_store(())
        if isinstance(store, StateStoreFacade):
            await store.ensure_seeded()

    @override
    async def write_to_event_stream(self, event: Event) -> None:
        """Record events to the workflow store, skipping duplicates on replay.

        Uses a lock to serialize writes, ensuring events are stored in the
        order they were emitted even when called from concurrent tasks.
        """
        if self._write_lock is None:
            self._write_lock = asyncio.Lock()
        async with self._write_lock:
            replaying = self.is_replaying()

            if not replaying:
                if isinstance(event, WorkflowFailedEvent):
                    exc_type = type(event.exception)
                    logger.error(
                        "Workflow step %s failed (run=%s): [%s.%s] %s",
                        event.step_name,
                        self.run_id,
                        exc_type.__module__,
                        exc_type.__qualname__,
                        event.exception,
                    )
                    await self._runtime._handle_status_update(
                        run_id=self.run_id,
                        status="failed",
                        error=str(event.exception),
                    )
                elif isinstance(event, WorkflowTimedOutEvent):
                    logger.error(
                        "Workflow timed out after %ss (run=%s)",
                        event.timeout,
                        self.run_id,
                    )
                    await self._runtime._handle_status_update(
                        run_id=self.run_id,
                        status="failed",
                        error=f"Workflow timed out after {event.timeout}s",
                    )
                elif isinstance(event, WorkflowCancelledEvent):
                    await self._runtime._handle_status_update(
                        run_id=self.run_id, status="cancelled"
                    )
                elif isinstance(event, StopEvent):
                    await self._runtime._handle_status_update(
                        run_id=self.run_id,
                        status="completed",
                        result=event,
                    )

                envelope = EventEnvelopeWithMetadata.from_event(event)
                await self._store.append_event(self.run_id, envelope)

            # Always forward to inner adapter (e.g. idle detection, DBOS stream)
            await super().write_to_event_stream(event)


# ---------------------------------------------------------------------------
# ServerRuntimeDecorator -- adapter wrapping, handler persistence,
# status updates, and workflow registry
# ---------------------------------------------------------------------------


class ServerRuntimeDecorator(BaseRuntimeDecorator):
    """
    Runtime decorator that wraps the main runtime to also record events to a configured
    workflow store, for integration with the WorkflowService for querying
    workflow run state.
    """

    def __init__(
        self,
        decorated: Runtime,
        store: AbstractWorkflowStore,
        *,
        persistence_backoff: list[float] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> None:
        super().__init__(
            decorated,
            default_serializer=(
                serializer
                if serializer is not None
                else JsonSerializer(allowed_types=[])
            ),
        )
        self._store: AbstractWorkflowStore = store
        self._registered_workflows: dict[str, Workflow] = {}
        self._additional_events: dict[str, tuple[type[Event], ...]] = {}
        self._initial_state: dict[str, Any] = {}
        self._persistence_backoff = (
            list(persistence_backoff) if persistence_backoff is not None else [0.5, 3]
        )

    async def _retry_store_write(self, coro_fn: Callable[[], Awaitable[None]]) -> None:
        """Wrap a store write with retry/backoff."""
        backoffs = list(self._persistence_backoff)
        while True:
            try:
                await coro_fn()
                return
            except Exception as e:
                backoff = backoffs.pop(0) if backoffs else None
                if backoff is None:
                    logger.error(
                        "Store write failed after final attempt",
                        exc_info=True,
                    )
                    raise
                logger.error(f"Store write failed, retrying in {backoff}s: {e}")
                await asyncio.sleep(backoff)

    # ------------------------------------------------------------------
    # Workflow registration
    # ------------------------------------------------------------------

    @override
    def track_workflow(self, workflow: Workflow) -> None:
        # Keep a strong reference — the base WorkflowSet uses weak refs,
        # so without this the workflow can be GC'd before launch().
        self._registered_workflows[workflow.workflow_name] = workflow
        super().track_workflow(workflow)

    @override
    def untrack_workflow(self, workflow: Workflow) -> None:
        self._registered_workflows.pop(workflow.workflow_name, None)
        super().untrack_workflow(workflow)

    def get_serializer(self, workflow: Workflow) -> BaseSerializer:
        serializer = workflow.serializer or self._default_serializer

        return self._compose_serializer(
            workflow,
            serializer,
            *self._additional_events.get(workflow.workflow_name, ()),
        )

    def register_additional_events(
        self, workflow_name: str, events: list[type[Event]]
    ) -> None:
        self._additional_events[workflow_name] = tuple(events)
        workflow = self.get_workflow(workflow_name)
        if workflow is not None:
            self._clear_serializer_cache(workflow)

    def get_workflow(self, name: str) -> Workflow | None:
        return self._registered_workflows.get(name)

    def get_workflow_names(self) -> list[str]:
        return list(self._registered_workflows.keys())

    # ------------------------------------------------------------------
    # Adapter wiring
    # ------------------------------------------------------------------

    async def _handle_status_update(
        self,
        run_id: str,
        status: Status,
        result: StopEvent | None = None,
        error: str | None = None,
    ) -> None:
        """Callback for adapter terminal-event status updates."""
        await self._retry_store_write(
            lambda: self._store.update_handler_status(
                run_id,
                status=status,
                result=result,
                error=error,
            )
        )

    @override
    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> ExternalRunAdapter:
        # Intercept serialized state: we handle seeding ourselves in get_state_store
        # so non-InMemory formats don't leak to the base runtime.
        passthrough_state = serialized_state
        if serialized_state and serializer:
            self._initial_state[run_id] = (serialized_state, serializer)
            store_type = serialized_state.get("store_type")
            if store_type is not None and store_type != "in_memory":
                passthrough_state = None
        return super().run_workflow(
            run_id,
            workflow,
            init_state,
            start_event=start_event,
            serialized_state=passthrough_state,
            serializer=serializer,
        )

    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        """Wraps the inner runtime's adapter in _ServerInternalRunAdapter."""
        inner_adapter = self._decorated.get_internal_adapter(workflow)
        state_types = namespaced_state_types(workflow)
        return _ServerInternalRunAdapter(
            inner_adapter,
            self,
            state_types=state_types,
            serializer=self.get_serializer(workflow),
        )

    # ------------------------------------------------------------------
    # Handler persistence
    # ------------------------------------------------------------------

    async def run_workflow_handler(
        self,
        handler_id: str,
        workflow_name: str,
        run_id: str,
    ) -> None:
        """Persist initial handler record to store.

        Must be called before the workflow is started so that
        ``update_handler_status`` can find the handler row when
        the workflow completes.
        """
        started_at = datetime.now(timezone.utc)

        await self._retry_store_write(
            lambda: self._store.update(
                PersistentHandler(
                    handler_id=handler_id,
                    workflow_name=workflow_name,
                    status="running",
                    run_id=run_id,
                    started_at=started_at,
                    updated_at=started_at,
                )
            )
        )
