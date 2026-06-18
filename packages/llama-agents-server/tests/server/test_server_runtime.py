# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ServerRuntimeDecorator and _ServerInternalRunAdapter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, AsyncGenerator
from unittest.mock import MagicMock

import pytest
from llama_agents.server import (
    HandlerQuery,
    MemoryWorkflowStore,
    PersistentHandler,
    WorkflowServer,
)
from llama_agents.server._runtime.idle_release_runtime import IdleReleaseDecorator
from llama_agents.server._runtime.server_runtime import (
    ServerRuntimeDecorator,
    _ServerInternalRunAdapter,
)
from workflows import Workflow, step
from workflows.context.state_store import StateStore
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
    WorkflowCancelledEvent,
    WorkflowFailedEvent,
    WorkflowTimedOutEvent,
)
from workflows.runtime.runtime_decorators import BaseRuntimeDecorator
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    WaitResult,
    WaitResultTimeout,
)
from workflows.runtime.types.ticks import WorkflowTick

# -- Stubs -----------------------------------------------------------------


class StubInternalAdapter(InternalRunAdapter):
    def __init__(self) -> None:
        self.closed = False

    @property
    def run_id(self) -> str:
        return "r1"

    async def write_to_event_stream(self, event: Event) -> None:
        pass

    async def get_now(self) -> float:
        return 1.0

    async def send_event(self, tick: WorkflowTick) -> None:
        pass

    async def wait_receive(self, timeout_seconds: float | None = None) -> WaitResult:
        return WaitResultTimeout()

    async def close(self) -> None:
        self.closed = True

    def get_state_store(self) -> StateStore[Any] | None:
        return None


class StubExternalAdapter(ExternalRunAdapter):
    def __init__(self) -> None:
        self.closed = False

    @property
    def run_id(self) -> str:
        return "r1"

    async def send_event(self, tick: WorkflowTick) -> None:
        pass

    async def stream_published_events(self) -> AsyncGenerator[Event, None]:
        yield StopEvent(result="done")

    async def close(self) -> None:
        self.closed = True

    async def get_result(self) -> StopEvent:
        return StopEvent(result="done")

    def get_state_store(self) -> StateStore[Any] | None:
        return None


class StubRuntime(Runtime):
    def __init__(self) -> None:
        super().__init__()
        self.launched = False

    def register(self, workflow: Any) -> RegisteredWorkflow:
        return RegisteredWorkflow(
            workflow=workflow, workflow_run_fn=MagicMock(), steps={}
        )

    def run_workflow(
        self,
        run_id: str,
        workflow: Any,
        init_state: Any,
        start_event: Any = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: Any = None,
    ) -> ExternalRunAdapter:
        return StubExternalAdapter()

    def get_internal_adapter(self, workflow: Any) -> InternalRunAdapter:
        return StubInternalAdapter()

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        return StubExternalAdapter()

    async def launch(self) -> None:
        self.launched = True

    async def destroy(self) -> None:
        pass


class RecordingStore(MemoryWorkflowStore):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    async def start(self) -> None:
        self._events.append("store")
        await super().start()


class RecordingRuntime(StubRuntime):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    async def launch(self) -> None:
        self._events.append("runtime")
        await super().launch()


class SimpleWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")


# -- Tests -----------------------------------------------------------------


def test_add_workflow_sets_workflow_name() -> None:
    server = WorkflowServer()
    wf = SimpleWorkflow(runtime=StubRuntime())
    server.add_workflow("greeting", wf)
    assert wf.workflow_name == "greeting"


def test_add_workflow_wraps_runtime_with_decorator() -> None:
    server = WorkflowServer()
    wf = SimpleWorkflow(runtime=StubRuntime())
    server.add_workflow("greeting", wf)
    assert isinstance(wf.runtime, BaseRuntimeDecorator)


def test_add_workflow_no_double_wrap() -> None:
    server = WorkflowServer()
    wf = SimpleWorkflow(runtime=StubRuntime())
    server.add_workflow("greeting", wf)
    server.add_workflow("greeting", wf)
    assert isinstance(wf.runtime, ServerRuntimeDecorator)
    # Inner should be IdleReleaseDecorator, not another ServerRuntimeDecorator
    assert isinstance(wf.runtime._decorated, IdleReleaseDecorator)
    assert not isinstance(wf.runtime._decorated, ServerRuntimeDecorator)


async def test_custom_server_runtime_launches_before_store_start() -> None:
    events: list[str] = []
    server = WorkflowServer(
        workflow_store=RecordingStore(events),
        runtime=RecordingRuntime(events),
    )

    await server.start()
    assert events == ["runtime", "store"]
    await server.stop()


def test_server_runtime_decorator_wraps_internal_adapter() -> None:
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)
    wf = SimpleWorkflow(runtime=decorator)
    adapter = decorator.get_internal_adapter(wf)
    assert isinstance(adapter, _ServerInternalRunAdapter)


async def test_server_internal_adapter_records_events_to_store() -> None:
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)
    wf = SimpleWorkflow(runtime=decorator)
    adapter = decorator.get_internal_adapter(wf)

    await adapter.write_to_event_stream(StopEvent(result="hello"))
    await adapter.write_to_event_stream(StopEvent(result="world"))

    events = await store.query_events(adapter.run_id)
    assert len(events) == 2
    assert events[0].sequence == 0
    assert events[1].sequence == 1
    assert events[0].event.type == "StopEvent"
    assert events[1].event.type == "StopEvent"


async def test_server_internal_adapter_forwards_to_inner() -> None:
    """The adapter should forward write_to_event_stream to the inner adapter.

    Events are recorded to the store AND forwarded so that inner decorators
    (e.g. _DurableInternalRunAdapter) can process them for idle detection.
    """

    class RecordingAdapter(StubInternalAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.recorded_events: list[Event] = []

        async def write_to_event_stream(self, event: Event) -> None:
            self.recorded_events.append(event)

    inner = RecordingAdapter()
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)
    adapter = _ServerInternalRunAdapter(inner, decorator)

    stop = StopEvent(result="forwarded")
    await adapter.write_to_event_stream(stop)

    # Event is forwarded to inner adapter for decorator chain processing
    assert len(inner.recorded_events) == 1
    # And also recorded in the store
    events = await store.query_events(adapter.run_id)
    assert len(events) == 1


def test_add_workflow_uses_server_runtime_decorator() -> None:
    server = WorkflowServer()
    wf = SimpleWorkflow(runtime=StubRuntime())
    server.add_workflow("test", wf)
    assert isinstance(wf.runtime, ServerRuntimeDecorator)


async def test_concurrent_runs_get_independent_sequences() -> None:
    """Two adapters from the same decorator should have independent sequences."""
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)

    adapter_a = _ServerInternalRunAdapter(StubInternalAdapterWithId("run-a"), decorator)
    adapter_b = _ServerInternalRunAdapter(StubInternalAdapterWithId("run-b"), decorator)

    # Interleave writes from both adapters
    await adapter_a.write_to_event_stream(StopEvent(result="a1"))
    await adapter_b.write_to_event_stream(StopEvent(result="b1"))
    await adapter_a.write_to_event_stream(StopEvent(result="a2"))
    await adapter_b.write_to_event_stream(StopEvent(result="b2"))
    await adapter_b.write_to_event_stream(StopEvent(result="b3"))

    events_a = await store.query_events("run-a")
    events_b = await store.query_events("run-b")

    assert len(events_a) == 2
    assert [e.sequence for e in events_a] == [0, 1]

    assert len(events_b) == 3
    assert [e.sequence for e in events_b] == [0, 1, 2]


class StubInternalAdapterWithId(StubInternalAdapter):
    def __init__(self, run_id: str) -> None:
        super().__init__()
        self._run_id = run_id

    @property
    def run_id(self) -> str:
        return self._run_id


@pytest.mark.parametrize(
    "event, expected_status, expected_error, expected_has_result",
    [
        pytest.param(
            StopEvent(result="done"),
            "completed",
            None,
            True,
            id="stop-event",
        ),
        pytest.param(
            WorkflowFailedEvent(
                step_name="s",
                exception=RuntimeError("boom"),
                attempts=1,
                elapsed_seconds=0.0,
            ),
            "failed",
            "boom",
            False,
            id="failed-event",
        ),
        pytest.param(
            WorkflowTimedOutEvent(
                timeout=10.0,
                active_steps=["s"],
            ),
            "failed",
            "Workflow timed out after 10.0s",
            False,
            id="timed-out-event",
        ),
        pytest.param(
            WorkflowCancelledEvent(),
            "cancelled",
            None,
            False,
            id="cancelled-event",
        ),
    ],
)
async def test_terminal_event_status_transitions(
    event: Event,
    expected_status: str,
    expected_error: str | None,
    expected_has_result: bool,
) -> None:
    """Writing a terminal event updates handler status in the store."""
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)
    decorator._persistence_backoff = [0, 0]

    run_id = "run-terminal"
    # Seed a handler record so update_handler_status can find it
    await store.update(
        PersistentHandler(
            handler_id="h1",
            workflow_name="test",
            status="running",
            run_id=run_id,
            started_at=datetime.now(timezone.utc),
        )
    )

    inner = StubInternalAdapterWithId(run_id)
    adapter = _ServerInternalRunAdapter(inner, decorator)

    await adapter.write_to_event_stream(event)

    found = await store.query(HandlerQuery(run_id_in=[run_id]))
    assert len(found) == 1
    handler = found[0]
    assert handler.status == expected_status
    assert handler.error == expected_error
    if expected_has_result:
        assert handler.result is not None
    else:
        assert handler.result is None


async def test_run_workflow_handler_persists_initial_record() -> None:
    """run_workflow_handler creates a running handler record in the store."""
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)
    decorator._persistence_backoff = [0, 0]

    await decorator.run_workflow_handler("h-init", "my_workflow", "test-run")

    found = await store.query(HandlerQuery(handler_id_in=["h-init"]))
    assert len(found) == 1
    handler = found[0]
    assert handler.handler_id == "h-init"
    assert handler.workflow_name == "my_workflow"
    assert handler.status == "running"
    assert handler.run_id == "test-run"
    assert handler.started_at is not None


async def test_retry_store_write_succeeds_after_failures() -> None:
    """_retry_store_write retries and eventually succeeds."""
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)
    decorator._persistence_backoff = [0, 0]

    call_count = 0

    async def flaky() -> None:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("transient")

    await decorator._retry_store_write(flaky)
    assert call_count == 3


async def test_retry_store_write_raises_after_exhaustion() -> None:
    """_retry_store_write raises when all retries are exhausted."""
    store = MemoryWorkflowStore()
    decorator = ServerRuntimeDecorator(StubRuntime(), store=store)
    decorator._persistence_backoff = [0, 0]

    async def always_fail() -> None:
        raise RuntimeError("permanent")

    with pytest.raises(RuntimeError, match="permanent"):
        await decorator._retry_store_write(always_fail)
