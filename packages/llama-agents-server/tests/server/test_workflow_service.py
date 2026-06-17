# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, cast

import pytest
from llama_agents.server import (
    HandlerQuery,
    MemoryWorkflowStore,
    PersistentHandler,
    WorkflowServer,
)
from llama_agents.server._service import EventSendError, HandlerCompletedError
from pydantic import BaseModel
from server_test_fixtures import (  # type: ignore[import]
    ErrorWorkflow,
    ExternalEvent,
    wait_for_passing,
    wait_for_requested_external_event,
)
from workflows import Context, Workflow
from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import DictState, InMemoryStateStore
from workflows.decorators import step
from workflows.events import StartEvent, StopEvent


class ToDictOnlyStateStore:
    state_type = DictState

    def __init__(self) -> None:
        self._inner = InMemoryStateStore(DictState(count=7))

    async def get_state(self) -> DictState:
        return await self._inner.get_state()

    async def set_state(self, state: DictState) -> None:
        await self._inner.set_state(state)

    async def get(self, path: str, default: Any = ...) -> Any:
        return await self._inner.get(path, default)

    async def set(self, path: str, value: Any) -> None:
        await self._inner.set(path, value)

    async def clear(self) -> None:
        await self._inner.clear()

    @asynccontextmanager
    async def edit_state(self) -> AsyncGenerator[DictState, None]:
        async with self._inner.edit_state() as state:
            yield state

    def to_dict(self, serializer: BaseSerializer) -> dict[str, Any]:
        return self._inner.to_dict(serializer)


@pytest.mark.asyncio
async def test_cancel_running_handler(
    memory_store: MemoryWorkflowStore, interactive_workflow: Workflow
) -> None:
    """Start an interactive workflow, cancel it, and verify status becomes cancelled."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow(
        "interactive", interactive_workflow, additional_events=[ExternalEvent]
    )

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(
            interactive_workflow, "cancel-test-1"
        )
        assert handler_data.run_id is not None

        result = await server._service.cancel_handler("cancel-test-1")
        assert result == "cancelled"

        async def status_is_cancelled() -> None:
            persisted = await memory_store.query(
                HandlerQuery(handler_id_in=["cancel-test-1"])
            )
            assert len(persisted) == 1
            assert persisted[0].status == "cancelled"

        await wait_for_passing(status_is_cancelled, max_duration=2.0, interval=0.01)


@pytest.mark.asyncio
async def test_cancel_handler_with_purge(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """Start and complete a workflow, then purge it from the store."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("simple", simple_test_workflow)

    async with server.contextmanager():
        await server._service.start_workflow(simple_test_workflow, "purge-test-1")

        # Wait for completion
        async def handler_completed() -> None:
            persisted = await memory_store.query(
                HandlerQuery(handler_id_in=["purge-test-1"])
            )
            assert len(persisted) == 1
            assert persisted[0].status == "completed"

        await wait_for_passing(handler_completed, max_duration=2.0, interval=0.01)

        result = await server._service.cancel_handler("purge-test-1", purge=True)
        assert result == "deleted"

        # Handler should be gone from store
        persisted = await memory_store.query(
            HandlerQuery(handler_id_in=["purge-test-1"])
        )
        assert len(persisted) == 0


@pytest.mark.asyncio
async def test_cancel_handler_not_found(memory_store: MemoryWorkflowStore) -> None:
    """Cancelling a nonexistent handler returns None."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)

    async with server.contextmanager():
        result = await server._service.cancel_handler("nonexistent")
        assert result is None


@pytest.mark.asyncio
async def test_send_event_workflow_not_registered(
    memory_store: MemoryWorkflowStore,
) -> None:
    """Sending an event to a handler whose workflow is not registered raises EventSendError."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)

    # Seed store with a handler for an unregistered workflow
    await memory_store.update(
        PersistentHandler(
            handler_id="orphan-handler",
            workflow_name="unregistered",
            status="running",
            run_id="some-run-id",
            started_at=datetime.now(timezone.utc),
        )
    )

    async with server.contextmanager():
        with pytest.raises(EventSendError, match="not registered"):
            await server._service.send_event(
                "orphan-handler", ExternalEvent(response="hello")
            )


@pytest.mark.asyncio
async def test_send_event_no_run_id(
    memory_store: MemoryWorkflowStore, interactive_workflow: Workflow
) -> None:
    """Sending an event to a handler with no run_id raises EventSendError."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow(
        "interactive", interactive_workflow, additional_events=[ExternalEvent]
    )

    # Seed store with a handler that has no run_id
    await memory_store.update(
        PersistentHandler(
            handler_id="no-run-handler",
            workflow_name="interactive",
            status="running",
            run_id=None,
            started_at=datetime.now(timezone.utc),
        )
    )

    async with server.contextmanager():
        with pytest.raises(EventSendError, match="no run ID"):
            await server._service.send_event(
                "no-run-handler", ExternalEvent(response="hello")
            )


@pytest.mark.asyncio
async def test_start_workflow_happy_path(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """start_workflow returns HandlerData with correct initial fields."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("simple", simple_test_workflow)

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(
            simple_test_workflow, "start-hp-1"
        )
        assert handler_data.handler_id == "start-hp-1"
        assert handler_data.workflow_name == "simple"
        assert handler_data.run_id is not None
        assert handler_data.status == "running"


@pytest.mark.asyncio
async def test_await_workflow_happy_path(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """await_workflow returns completed HandlerData."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("simple", simple_test_workflow)

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(
            simple_test_workflow, "await-hp-1"
        )
        result = await server._service.await_workflow(handler_data)
        assert result.status == "completed"
        assert result.handler_id == "await-hp-1"


@pytest.mark.asyncio
async def test_await_workflow_error_returns_failed(
    memory_store: MemoryWorkflowStore,
) -> None:
    """await_workflow on an ErrorWorkflow returns failed status, not an exception."""
    error_wf = ErrorWorkflow()
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("error", error_wf)

    async with server.contextmanager():
        handler_data = await server._service.start_workflow(error_wf, "await-err-1")
        result = await server._service.await_workflow(handler_data)
        assert result.status == "failed"


@pytest.mark.asyncio
async def test_resolve_handler_raises_on_completed(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """resolve_handler raises HandlerCompletedError for a terminal handler."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("simple", simple_test_workflow)

    async with server.contextmanager():
        await server._service.start_workflow(simple_test_workflow, "resolve-done-1")

        async def handler_completed() -> None:
            persisted = await memory_store.query(
                HandlerQuery(handler_id_in=["resolve-done-1"])
            )
            assert len(persisted) == 1
            assert persisted[0].status == "completed"

        await wait_for_passing(handler_completed, max_duration=2.0, interval=0.01)

        with pytest.raises(HandlerCompletedError):
            await server._service.resolve_handler("resolve-done-1")


@pytest.mark.asyncio
async def test_send_event_happy_path(
    memory_store: MemoryWorkflowStore, interactive_workflow: Workflow
) -> None:
    """send_event delivers an event and the workflow completes."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow(
        "interactive", interactive_workflow, additional_events=[ExternalEvent]
    )

    async with server.contextmanager():
        await server._service.start_workflow(interactive_workflow, "send-hp-1")

        await wait_for_requested_external_event(memory_store, "send-hp-1")

        await server._service.send_event("send-hp-1", ExternalEvent(response="pong"))

        async def handler_completed() -> None:
            persisted = await memory_store.query(
                HandlerQuery(handler_id_in=["send-hp-1"])
            )
            assert len(persisted) == 1
            assert persisted[0].status == "completed"

        await wait_for_passing(handler_completed, max_duration=2.0, interval=0.01)


@pytest.mark.asyncio
async def test_cancel_terminal_handler_without_purge(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    """cancel_handler on an already-completed handler without purge returns None."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("simple", simple_test_workflow)

    async with server.contextmanager():
        await server._service.start_workflow(simple_test_workflow, "cancel-term-1")

        async def handler_completed() -> None:
            persisted = await memory_store.query(
                HandlerQuery(handler_id_in=["cancel-term-1"])
            )
            assert len(persisted) == 1
            assert persisted[0].status == "completed"

        await wait_for_passing(handler_completed, max_duration=2.0, interval=0.01)

        result = await server._service.cancel_handler("cancel-term-1", purge=False)
        assert result is None

        # Handler should still exist unchanged
        persisted = await memory_store.query(
            HandlerQuery(handler_id_in=["cancel-term-1"])
        )
        assert len(persisted) == 1
        assert persisted[0].status == "completed"


@pytest.mark.asyncio
async def test_context_from_handler_id_falls_back_to_legacy_state_snapshot(
    memory_store: MemoryWorkflowStore, simple_test_workflow: Workflow
) -> None:
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("simple", simple_test_workflow)
    await memory_store.update(
        PersistentHandler(
            handler_id="completed-with-plugin-state",
            workflow_name="simple",
            status="completed",
            run_id="plugin-run",
            started_at=datetime.now(timezone.utc),
        )
    )
    state_stores = cast(dict[Any, Any], memory_store.state_stores)
    state_stores[("plugin-run", ())] = ToDictOnlyStateStore()

    async with server.contextmanager():
        ctx = await server._service._context_from_handler_id(
            simple_test_workflow, "completed-with-plugin-state"
        )

    assert ctx is not None
    assert await ctx.store.get("count") == 7


class _InnerValue(BaseModel):
    x: int = 1


class _TypedHandoffState(BaseModel):
    counter: int = 0


class _TypedHandoffWorkflow(Workflow):
    @step
    async def go(self, ev: StartEvent, ctx: Context[_TypedHandoffState]) -> StopEvent:
        async with ctx.store.edit_state() as st:
            st.counter += 1
            return StopEvent(result=st.counter)


class _DictHandoffWorkflow(Workflow):
    @step
    async def go(self, ev: StartEvent, ctx: Context) -> StopEvent:
        obj = await ctx.store.get("obj", default=None)
        return StopEvent(result=type(obj).__name__)


@pytest.mark.asyncio
async def test_typed_state_continuation_via_memory_store(
    memory_store: MemoryWorkflowStore,
) -> None:
    """Handler continuation must round-trip typed state through the handoff payload."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    wf = _TypedHandoffWorkflow()
    server.add_workflow("typed", wf)
    await memory_store.update(
        PersistentHandler(
            handler_id="h-typed-continuation",
            workflow_name="typed",
            status="completed",
            run_id="run-typed-continuation",
            started_at=datetime.now(timezone.utc),
        )
    )
    # Previous run left typed state in the memory store.
    prev = memory_store.create_state_store(
        "run-typed-continuation", state_type=_TypedHandoffState
    )
    await prev.set("counter", 41)

    async with server.contextmanager():
        ctx = await server._service._context_from_handler_id(wf, "h-typed-continuation")
        assert ctx is not None
        handler = wf.run(ctx=ctx)
        # The continued run must see the previous run's typed state (41) and
        # be able to use it as the typed model (crash/degradation regression).
        result = await handler
        assert result == 42


@pytest.mark.asyncio
async def test_dict_state_pydantic_value_continuation_via_memory_store(
    memory_store: MemoryWorkflowStore,
) -> None:
    """Pydantic values in DictState must survive handler continuation undegraded."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    wf = _DictHandoffWorkflow()
    server.add_workflow("dictwf", wf)
    await memory_store.update(
        PersistentHandler(
            handler_id="h-dict-continuation",
            workflow_name="dictwf",
            status="completed",
            run_id="run-dict-continuation",
            started_at=datetime.now(timezone.utc),
        )
    )
    prev = memory_store.create_state_store("run-dict-continuation")
    await prev.set("obj", _InnerValue(x=42))

    async with server.contextmanager():
        ctx = await server._service._context_from_handler_id(wf, "h-dict-continuation")
        assert ctx is not None
        handler = wf.run(ctx=ctx)
        result = await handler
        assert result == "_InnerValue", f"pydantic value degraded to {result}"
