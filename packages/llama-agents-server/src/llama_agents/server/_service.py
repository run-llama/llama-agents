# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
Application-level orchestration layer for workflow handler lifecycle.

_WorkflowService is a plain class (not a Runtime subclass) that provides
the public interface consumed by _api.py. It delegates to the decorated
runtime for persistence and adapter wiring, and to the store for queries.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Literal

from llama_agents.client.protocol import HandlerData
from llama_agents.client.protocol.serializable_events import (
    EventEnvelopeWithMetadata,
)
from llama_agents.server._runtime.server_runtime import ServerRuntimeDecorator
from llama_index_instrumentation.dispatcher import instrument_tags
from workflows import Context
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store_integration import StateStoreHandleProvider
from workflows.events import Event, StartEvent
from workflows.handler import WorkflowHandler
from workflows.utils import _nanoid as nanoid
from workflows.workflow import Workflow

from ._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    is_terminal_status,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class HandlerNotFoundError(Exception):
    pass


class HandlerCompletedError(Exception):
    pass


class EventSendError(Exception):
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def handler_data_from_persistent(persistent: PersistentHandler) -> HandlerData:
    return HandlerData(
        handler_id=persistent.handler_id,
        workflow_name=persistent.workflow_name,
        run_id=persistent.run_id,
        status=persistent.status,
        started_at=persistent.started_at.isoformat()
        if persistent.started_at is not None
        else datetime.now(timezone.utc).isoformat(),
        updated_at=persistent.updated_at.isoformat()
        if persistent.updated_at is not None
        else None,
        completed_at=persistent.completed_at.isoformat()
        if persistent.completed_at is not None
        else None,
        error=persistent.error,
        result=EventEnvelopeWithMetadata.from_event(persistent.result)
        if persistent.result is not None
        else None,
    )


# ---------------------------------------------------------------------------
# _WorkflowService
# ---------------------------------------------------------------------------


class _WorkflowService:
    """Application-level service facade for workflow handler lifecycle.

    This is NOT a Runtime. It holds references to the decorated runtime
    (for running workflows and getting adapters) and the store (for queries).
    """

    def __init__(
        self,
        runtime: ServerRuntimeDecorator,
        store: AbstractWorkflowStore,
    ) -> None:
        self._runtime: ServerRuntimeDecorator = runtime
        self._store = store

    # ------------------------------------------------------------------
    # Workflow registration
    # ------------------------------------------------------------------

    def get_workflow(self, name: str) -> Workflow | None:
        return self._runtime.get_workflow(name)

    def get_workflow_names(self) -> list[str]:
        return self._runtime.get_workflow_names()

    # ------------------------------------------------------------------
    # Store access
    # ------------------------------------------------------------------

    @property
    def store(self) -> AbstractWorkflowStore:
        return self._store

    async def query_handlers(self, query: HandlerQuery) -> list[PersistentHandler]:
        return await self._store.query(query)

    # ------------------------------------------------------------------
    # Handler lifecycle
    # ------------------------------------------------------------------

    async def load_handler(self, handler_id: str) -> HandlerData | None:
        found = await self._store.query(HandlerQuery(handler_id_in=[handler_id]))
        if not found:
            return None
        return handler_data_from_persistent(found[0])

    async def resolve_handler(self, handler_id: str) -> HandlerData:
        handler_data = await self.load_handler(handler_id)
        if handler_data is None:
            raise HandlerNotFoundError()
        if is_terminal_status(handler_data.status):
            raise HandlerCompletedError()
        return handler_data

    async def send_event(
        self,
        handler_id: str,
        event: Event,
        step: str | None = None,
    ) -> None:
        """Send a parsed event to a running handler."""
        handler_data = await self.resolve_handler(handler_id)

        workflow = self._runtime.get_workflow(handler_data.workflow_name)
        if workflow is None:
            raise EventSendError(
                f"Workflow {handler_data.workflow_name} not registered"
            )
        if handler_data.run_id is None:
            raise EventSendError(f"Handler {handler_id} has no run ID")

        try:
            handler = WorkflowHandler(
                workflow, self._runtime.get_external_adapter(handler_data.run_id)
            )
            await handler.send_event(event, step=step)
        except Exception as e:
            raise EventSendError(f"Failed to send event: {e}") from e

    async def cancel_handler(
        self, handler_id: str, purge: bool = False
    ) -> Literal["cancelled", "deleted"] | None:
        found = await self._store.query(HandlerQuery(handler_id_in=[handler_id]))
        if not found:
            return None
        persisted = handler_data_from_persistent(found[0])
        if not purge and (
            persisted.run_id is None or is_terminal_status(persisted.status)
        ):
            return None

        is_terminal = is_terminal_status(persisted.status)
        if not is_terminal and persisted.run_id is not None:
            handler = self._workflow_run_handler(
                persisted.workflow_name, persisted.run_id
            )
            await self._cancel_run(handler)

        if purge:
            n_deleted = await self._store.delete(
                HandlerQuery(handler_id_in=[handler_id])
            )
            if n_deleted == 0:
                return None

        return "deleted" if purge else "cancelled"

    async def start_workflow(
        self,
        workflow: Workflow,
        handler_id: str,
        start_event: StartEvent | None = None,
        context: Context | None = None,
    ) -> HandlerData:
        with instrument_tags({"llamaindex.handler_id": handler_id}):
            if context is None:
                context = await self._context_from_handler_id(workflow, handler_id)
            # Pre-generate run_id and persist the handler record BEFORE starting
            # the workflow. This prevents a race where a fast workflow completes
            # and tries to update_handler_status before the handler row exists,
            # causing the status update to be silently skipped.
            run_id = nanoid()
            await self._runtime.run_workflow_handler(
                handler_id, workflow.workflow_name, run_id
            )
            _ = workflow.run(
                ctx=context,
                start_event=start_event,
                run_id=run_id,
            )
            handler_data = await self.load_handler(handler_id)
            if handler_data is None:
                raise RuntimeError(f"Handler {handler_id} not found after creation")
            return handler_data

    async def await_workflow(self, handler: HandlerData) -> HandlerData:
        if handler.run_id is None:
            raise HandlerNotFoundError("Handler exists, but has no run ID")
        run = self._workflow_run_handler(handler.workflow_name, handler.run_id)

        try:
            await run
        except Exception:
            logger.error(
                "Workflow %s (handler=%s, run=%s) raised an exception",
                handler.workflow_name,
                handler.handler_id,
                handler.run_id,
                exc_info=True,
            )
        handler_data = await self.load_handler(handler.handler_id)
        if handler_data is None:
            raise HandlerNotFoundError()
        return handler_data

    # ------------------------------------------------------------------
    # Start / stop
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Launch runtimes and register tracked workflows."""
        await self._runtime.launch()

    async def stop(self) -> None:
        """Stop active runs and destroy the runtime."""
        await self._runtime.destroy()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _context_from_handler_id(
        self, workflow: Workflow, handler_id: str
    ) -> Context | None:
        """Look up a completed handler's final state and build a Context from it.

        Returns the lightweight serialized state reference so that SQL-backed
        stores can do an optimized copy rather than round-tripping through memory.

        Returns None if the handler doesn't exist, isn't completed, or has no state.
        """
        found = await self._store.query(HandlerQuery(handler_id_in=[handler_id]))
        if not found:
            return None
        handler = found[0]
        if not is_terminal_status(handler.status) or handler.run_id is None:
            return None

        try:
            old_state_store = self._store.create_state_store(handler.run_id)
            if not isinstance(old_state_store, StateStoreHandleProvider):
                return None
            state_dict = old_state_store.handle()
            if not state_dict:
                return None
            return Context.from_dict(
                workflow=workflow,
                data={"version": 1, "state": state_dict},
                serializer=JsonSerializer(),
            )
        except Exception:
            logger.warning(
                "Failed to read state from previous handler %s",
                handler_id,
                exc_info=True,
            )
            return None

    def _workflow_run_handler(self, workflow_name: str, run_id: str) -> WorkflowHandler:
        workflow = self._runtime.get_workflow(workflow_name)
        if workflow is None:
            raise HandlerNotFoundError(f"Workflow {workflow_name} not registered")
        return WorkflowHandler(
            workflow=workflow,
            external_adapter=workflow._runtime.get_external_adapter(run_id),
        )

    async def _cancel_run(self, run: WorkflowHandler) -> None:
        """Gracefully cancel the workflow run, then kill tasks."""
        if not run.done():
            try:
                await run.cancel_run()
            except Exception:
                pass
            try:
                await run
            except (asyncio.CancelledError, Exception):
                pass
        await self._kill_run(run)

    async def _kill_run(self, run: WorkflowHandler) -> None:
        """Force-kill the handler without graceful cancellation."""
        if not run.done():
            try:
                run.cancel()
            except Exception:
                pass
