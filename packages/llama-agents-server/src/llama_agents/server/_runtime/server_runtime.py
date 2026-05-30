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
    CHILD_STATES_KEY,
    ROOT_STATE_KEY,
    DictState,
    InMemoryStateStore,
    NamespacedStateStores,
    StateStore,
    infer_state_type,
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
from workflows.runtime.types.results import StepWorkerStateContextVar
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
        state_type: type[Any] | None = None,
    ) -> None:
        super().__init__(decorated)
        self._runtime = runtime
        self._store = runtime._store
        self._state_type = state_type
        self._state_store: StateStore[Any] | None = None
        self._write_lock: asyncio.Lock | None = None

    @override
    def get_state_store(self) -> StateStore[Any]:
        # Child-ful runs partition state by namespace into a single durable blob.
        # Select this step's namespace from the contextvar (root steps and any
        # caller outside a step get ``()``), exactly like the basic adapter.
        namespaced = self._runtime._namespaced_state.get(self.run_id)
        if namespaced is not None:
            namespace: tuple[str, ...] = ()
            try:
                namespace = StepWorkerStateContextVar.get().namespace
            except LookupError:
                pass
            return namespaced.view(namespace)

        # Single-namespace run: existing single-store path, byte-identical.
        if self._state_store is not None:
            return self._state_store
        initial = self._runtime._initial_state.pop(self.run_id, None)
        if initial is not None:
            serialized_state, serializer = initial
            store = self._store.create_state_store(
                self.run_id, self._state_type, serialized_state, serializer
            )
        else:
            store = self._store.create_state_store(self.run_id, self._state_type)
        self._state_store = store
        return store

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
# _ServerExternalRunAdapter
# ---------------------------------------------------------------------------


class _ServerExternalRunAdapter(BaseExternalRunAdapterDecorator):
    """External adapter exposing per-namespace durable stores for child-ful runs.

    The base (basic) external adapter only knows the in-memory queues; under the
    server, child state lives in the durable namespaced blob. Surfacing the
    namespaced views here lets ``Context.to_dict`` serialize the whole tree.
    """

    def __init__(
        self,
        decorated: ExternalRunAdapter,
        namespaced: NamespacedStateStores,
    ) -> None:
        super().__init__(decorated)
        self._namespaced = namespaced

    @override
    def get_state_store(self) -> StateStore[Any] | None:
        return self._namespaced.view(())

    def get_all_state_stores(self) -> dict[tuple[str, ...], StateStore[Any]]:
        return self._namespaced.all_views()


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
        # Per-run namespaced state for child-ful workflows: the whole child tree
        # persists in one durable blob, partitioned per namespace. Keyed by
        # run_id; absent for single-namespace runs (which use the legacy path).
        self._namespaced_state: dict[str, NamespacedStateStores] = {}
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
        # Intercept serialized state: we handle seeding ourselves in get_state_store
        # so non-InMemory formats don't leak to the base runtime.
        passthrough_state = serialized_state
        if serialized_state and serializer:
            self._initial_state[run_id] = (serialized_state, serializer)
            store_type = serialized_state.get("store_type")
            if store_type is not None and store_type != "in_memory":
                passthrough_state = None

        # Build the per-run namespaced state once, from the ROOT workflow, for
        # child-ful runs only. Single-namespace runs keep the legacy path.
        self._maybe_build_namespaced_state(
            run_id, workflow, serialized_state, serializer
        )

        return super().run_workflow(
            run_id,
            workflow,
            init_state,
            start_event=start_event,
            serialized_state=passthrough_state,
            serializer=serializer,
        )

    def _maybe_build_namespaced_state(
        self,
        run_id: str,
        workflow: Workflow,
        serialized_state: dict[str, Any] | None,
        serializer: BaseSerializer | None,
    ) -> None:
        """Construct the run's :class:`NamespacedStateStores` if it has children.

        The underlying durable store is a single ``DictState`` blob row keyed by
        ``run_id``; per-namespace views read/write their slot of it. Idempotent
        across fresh-start and idle reload (both call ``run_workflow`` with the
        root). When the incoming ``serialized_state`` is an in-memory nested blob
        (a portable ``ctx.to_dict`` payload), seed the row from it; sqlite/postgres
        references reconnect to the existing row with no seeding.
        """
        instances = workflow._namespace_instances()
        if len(instances) <= 1:
            return
        if run_id in self._namespaced_state:
            return

        active_serializer = serializer or JsonSerializer()
        state_types: dict[tuple[str, ...], type[Any]] = {
            namespace: infer_state_type(instance)
            for namespace, instance in instances.items()
        }

        seed = self._namespaced_seed(serialized_state, active_serializer)
        underlying = self._store.create_state_store(
            run_id, DictState, seed, active_serializer if seed else None
        )
        self._namespaced_state[run_id] = NamespacedStateStores(
            underlying=underlying,
            serializer=active_serializer,
            state_types=state_types,
        )

    @staticmethod
    def _namespaced_seed(
        serialized_state: dict[str, Any] | None,
        serializer: BaseSerializer,
    ) -> dict[str, Any] | None:
        """Turn a portable nested ``ctx.to_dict`` blob into a DictState row seed.

        Only in-memory nested blobs (those carrying ``CHILD_STATES_KEY``) need
        explicit seeding; a persisted reference already has its row. Returns the
        in-memory payload for the underlying ``DictState`` row, or ``None``.
        """
        if not serialized_state:
            return None
        if serialized_state.get("store_type") not in (None, "in_memory"):
            return None
        if CHILD_STATES_KEY not in serialized_state:
            return None
        root_payload = dict(serialized_state)
        children = root_payload.pop(CHILD_STATES_KEY) or {}
        blob = DictState()
        blob[ROOT_STATE_KEY] = root_payload
        blob[CHILD_STATES_KEY] = children
        return InMemoryStateStore(blob).to_dict(serializer)

    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        """Wraps the inner runtime's adapter in _ServerInternalRunAdapter."""
        inner_adapter = self._decorated.get_internal_adapter(workflow)
        state_type = infer_state_type(workflow)
        return _ServerInternalRunAdapter(inner_adapter, self, state_type=state_type)

    @override
    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        inner = self._decorated.get_external_adapter(run_id)
        namespaced = self._namespaced_state.get(run_id)
        if namespaced is None:
            return inner
        return _ServerExternalRunAdapter(inner, namespaced)

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
