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
from pydantic import BaseModel
from typing_extensions import override
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    DictState,
    StateStore,
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
    get_event_origin_namespace,
)
from workflows.runtime.runtime_decorators import (
    BaseExternalRunAdapterDecorator,
    BaseInternalRunAdapterDecorator,
    BaseRuntimeDecorator,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    Runtime,
)
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
        state_types: dict[tuple[str, ...], type[BaseModel]] | None = None,
    ) -> None:
        super().__init__(decorated)
        self._runtime = runtime
        self._store = runtime._store
        default_types: dict[tuple[str, ...], type[BaseModel]] = {(): DictState}
        self._state_types = state_types or default_types
        self._seed_resolved = False
        self._seeds: dict[tuple[str, ...], dict[str, Any]] | None = None
        self._root_handle: dict[str, Any] | None = None
        self._serializer: BaseSerializer = JsonSerializer()
        self._write_lock: asyncio.Lock | None = None

    def _resolve_seeds(self) -> None:
        """Split the resume snapshot into per-namespace seeds, once.

        A portable in-memory tree splits into one in-memory seed per namespace.
        A durable handle can't split — its ``copy_from_handle`` copies every
        namespace row under the run in one shot — so it seeds the root only.
        Restart-resume of the same run carries no snapshot: rows already exist.
        """
        if self._seed_resolved:
            return
        initial = self._runtime._initial_state.pop(self.run_id, None)
        serialized_state, serializer = initial if initial is not None else (None, None)
        self._serializer = serializer or JsonSerializer()
        self._seeds = namespaced_seed_payloads(serialized_state, self._state_types)
        if self._seeds is not None:
            for namespace, seed in list(self._seeds.items()):
                self._store.create_state_store(
                    self.run_id,
                    self._state_types.get(namespace, DictState),
                    seed,
                    self._serializer,
                    namespace=namespace,
                )
        if self._seeds is None and serialized_state:
            self._root_handle = serialized_state
        self._seed_resolved = True

    @override
    def get_state_store(self, namespace: tuple[str, ...] = ()) -> StateStore[Any]:
        """Vend the per-namespace durable store, seeding it on first build.

        Each namespace owns its own record. The seed (if any) is applied once,
        on the first build of that namespace; ``create_state_store`` caches the
        facade thereafter, so later calls reconnect without re-seeding.
        """
        self._resolve_seeds()
        seed: dict[str, Any] | None = None
        if self._seeds is not None:
            seed = self._seeds.pop(namespace, None)
        elif namespace == () and self._root_handle is not None:
            seed = self._root_handle
            self._root_handle = None
        return self._store.create_state_store(
            self.run_id,
            self._state_types.get(namespace, DictState),
            seed,
            self._serializer if seed else None,
            namespace=namespace,
        )

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
                elif isinstance(event, StopEvent) and not get_event_origin_namespace(
                    event
                ):
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
# _ServerExternalRunAdapter
# ---------------------------------------------------------------------------


class _ServerExternalRunAdapter(BaseExternalRunAdapterDecorator):
    """External adapter that reconnects a run's per-namespace durable rows.

    The base (basic) external adapter only knows the in-memory queues; under the
    server, state lives in durable rows keyed by ``(run_id, namespace)``.
    Reconnecting them here lets ``Context.to_dict`` serialize the whole tree
    without a shared cache.
    """

    def __init__(
        self,
        decorated: ExternalRunAdapter,
        store: AbstractWorkflowStore,
        run_id: str,
        underlying_state_type: type[Any],
    ) -> None:
        super().__init__(decorated)
        self._store = store
        self._run_id = run_id
        self._underlying_state_type = underlying_state_type

    @override
    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        # Reconnect the namespace's existing durable row (no seed). DictState
        # blob: a durable ``to_dict`` returns the root reconnect handle, which
        # already spans every namespace row, so callers never read child rows
        # here; resume reconnects typed rows via the internal adapter.
        return self._store.create_state_store(
            self._run_id, self._underlying_state_type, namespace=namespace
        )


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
    ) -> None:
        super().__init__(decorated)
        self._store: AbstractWorkflowStore = store
        self._registered_workflows: dict[str, Workflow] = {}
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
                run_id, status=status, result=result, error=error
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
        # Intercept serialized state: get_state_store seeds the durable
        # store itself, so non-InMemory formats don't leak to the base runtime.
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
        return _ServerInternalRunAdapter(
            inner_adapter, self, state_types=namespaced_state_types(workflow)
        )

    @override
    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        inner = self._decorated.get_external_adapter(run_id)
        # Reconnect the durable row as a DictState blob: this serves the
        # child-ful tree directly and yields a type-independent reference for
        # ``to_dict`` (resume reconnects with the correct typed store via the
        # internal adapter, which knows the workflow).
        return _ServerExternalRunAdapter(inner, self._store, run_id, DictState)

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
