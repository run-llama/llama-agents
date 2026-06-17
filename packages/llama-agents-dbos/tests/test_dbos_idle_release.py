# ty: ignore[invalid-assignment]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Unit tests for DBOSIdleReleaseDecorator."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from llama_agents.dbos.idle_release import (
    CRASH_TIMEOUT_SECONDS,
    DBOSIdleReleaseDecorator,
    DBOSIdleReleaseExternalRunAdapter,
    _DBOSIdleReleaseInternalRunAdapter,
)
from llama_agents.dbos.journal.crud import JournalCrud
from llama_agents.dbos.journal.lifecycle import RunLifecycleLock, RunLifecycleState
from llama_agents.server._store.abstract_workflow_store import (
    PersistentHandler,
)
from llama_agents.server._store.memory_workflow_store import MemoryWorkflowStore
from pydantic import BaseModel
from workflows.context import Context
from workflows.context.state_store import InMemoryStateStore, StateStore
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent, WorkflowIdleEvent
from workflows.runtime.runtime_decorators import BaseRuntimeDecorator
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    WaitResult,
    WaitResultTick,
    WaitResultTimeout,
)
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickIdleCheck,
    TickIdleRelease,
    WorkflowTick,
    WorkflowTickAdapter,
)
from workflows.workflow import Workflow

# -- Stubs -----------------------------------------------------------------


class StubInternalAdapter(InternalRunAdapter):
    def __init__(self, run_id: str = "run-1") -> None:
        self._run_id = run_id
        self.written_events: list[Event] = []
        self.closed = False

    @property
    def run_id(self) -> str:
        return self._run_id

    async def write_to_event_stream(self, event: Event) -> None:
        self.written_events.append(event)

    async def get_now(self) -> float:
        return 1.0

    async def send_event(self, tick: WorkflowTick) -> None:
        pass

    async def wait_receive(self, timeout_seconds: float | None = None) -> WaitResult:
        return WaitResultTimeout()

    async def close(self) -> None:
        self.closed = True

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        return None


class StubExternalAdapter(ExternalRunAdapter):
    def __init__(self, run_id: str = "run-1", result: StopEvent | None = None) -> None:
        self._run_id = run_id
        self._result = result or StopEvent(result="done")
        self.sent_events: list[WorkflowTick] = []
        self.closed = False

    @property
    def run_id(self) -> str:
        return self._run_id

    async def send_event(self, tick: WorkflowTick) -> None:
        self.sent_events.append(tick)

    async def stream_published_events(self) -> AsyncGenerator[Event, None]:
        yield self._result

    async def close(self) -> None:
        self.closed = True

    async def get_result(self) -> StopEvent:
        return self._result

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        return None


class StubRuntime(Runtime):
    def __init__(self) -> None:
        super().__init__()
        self.run_workflow_calls: list[dict[str, Any]] = []
        self._external_adapters: dict[str, StubExternalAdapter] = {}

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
        self.run_workflow_calls.append(
            {
                "run_id": run_id,
                "workflow": workflow,
                "init_state": init_state,
                "start_event": start_event,
                "serialized_state": serialized_state,
                "serializer": serializer,
            }
        )
        adapter = StubExternalAdapter(run_id=run_id)
        self._external_adapters[run_id] = adapter
        return adapter

    def get_internal_adapter(self, workflow: Any) -> InternalRunAdapter:
        return StubInternalAdapter()

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        if run_id in self._external_adapters:
            return self._external_adapters[run_id]
        adapter = StubExternalAdapter(run_id=run_id)
        self._external_adapters[run_id] = adapter
        return adapter

    async def launch(self) -> None:
        pass

    async def destroy(self) -> None:
        pass


class SimpleWorkflow(Workflow):
    @step
    async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")


class MyState(BaseModel):
    counter: int = 0


class StatefulWorkflow(Workflow):
    @step
    async def process(self, ctx: Context[MyState], ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")


class FakeLifecycleLock(RunLifecycleLock):
    """In-memory lifecycle lock that implements the real state machine."""

    def __init__(self) -> None:
        self._states: dict[str, tuple[RunLifecycleState, datetime]] = {}

    async def create(self, run_id: str) -> None:
        self._states[run_id] = (RunLifecycleState.active, datetime.now(timezone.utc))

    async def begin_release(self, run_id: str) -> bool:
        entry = self._states.get(run_id)
        if entry is None or entry[0] != RunLifecycleState.active:
            return False
        self._states[run_id] = (RunLifecycleState.releasing, datetime.now(timezone.utc))
        return True

    async def complete_release(self, run_id: str) -> None:
        entry = self._states.get(run_id)
        if entry is not None and entry[0] == RunLifecycleState.releasing:
            self._states[run_id] = (
                RunLifecycleState.released,
                datetime.now(timezone.utc),
            )

    async def try_begin_resume(
        self, run_id: str, crash_timeout_seconds: float | None = None
    ) -> RunLifecycleState | None:
        entry = self._states.get(run_id)
        if entry is None:
            return None
        state, updated_at = entry
        if state == RunLifecycleState.active:
            return None
        if state == RunLifecycleState.released or (
            state == RunLifecycleState.releasing
            and crash_timeout_seconds is not None
            and (datetime.now(timezone.utc) - updated_at).total_seconds()
            > crash_timeout_seconds
        ):
            self._states[run_id] = (
                RunLifecycleState.active,
                datetime.now(timezone.utc),
            )
            return RunLifecycleState.released
        return RunLifecycleState.releasing

    def get_state(self, run_id: str) -> tuple[RunLifecycleState, datetime] | None:
        """Test-only helper for assertions."""
        return self._states.get(run_id)

    def set_updated_at(self, run_id: str, updated_at: datetime) -> None:
        """Test hook: override the updated_at timestamp for crash timeout testing."""
        entry = self._states.get(run_id)
        if entry is not None:
            self._states[run_id] = (entry[0], updated_at)


# -- Fixtures --------------------------------------------------------------


@pytest.fixture()
def store() -> MemoryWorkflowStore:
    return MemoryWorkflowStore()


@pytest.fixture()
def stub_runtime() -> StubRuntime:
    return StubRuntime()


@pytest.fixture()
def mock_journal_crud() -> AsyncMock:
    return AsyncMock(spec=JournalCrud)


@pytest.fixture()
def mock_dbos():
    with patch("llama_agents.dbos.idle_release.DBOS") as dbos:
        dbos.delete_workflow_async = AsyncMock()
        dbos.retrieve_workflow_async = AsyncMock()
        dbos.retrieve_workflow_async.return_value = AsyncMock()
        yield dbos


@pytest.fixture()
def lifecycle() -> FakeLifecycleLock:
    return FakeLifecycleLock()


def _make_decorator(
    stub_runtime: StubRuntime,
    store: MemoryWorkflowStore,
    mock_journal_crud: AsyncMock,
    lifecycle_lock: RunLifecycleLock,
    idle_timeout: float = 0.1,
) -> DBOSIdleReleaseDecorator:
    return DBOSIdleReleaseDecorator(
        BaseRuntimeDecorator(stub_runtime),
        store,
        idle_timeout=idle_timeout,
        journal_crud=lambda: mock_journal_crud,
        lifecycle_lock=lambda: lifecycle_lock,
    )


@pytest.fixture()
def decorator(
    stub_runtime: StubRuntime,
    store: MemoryWorkflowStore,
    mock_journal_crud: AsyncMock,
    lifecycle: FakeLifecycleLock,
) -> DBOSIdleReleaseDecorator:
    return _make_decorator(stub_runtime, store, mock_journal_crud, lifecycle)


# -- Helpers ---------------------------------------------------------------


def _seed_handler(
    store: MemoryWorkflowStore,
    handler_id: str = "handler-1",
    workflow_name: str = "test_wf",
    run_id: str = "run-1",
    idle_since: datetime | None = None,
) -> PersistentHandler:
    handler = PersistentHandler(
        handler_id=handler_id,
        workflow_name=workflow_name,
        status="running",
        run_id=run_id,
        idle_since=idle_since,
        started_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    store.handlers[handler_id] = handler
    return handler


# -- Tests -----------------------------------------------------------------


@pytest.mark.asyncio()
async def test_idle_event_schedules_release_without_stamping_idle_since(
    decorator: DBOSIdleReleaseDecorator, store: MemoryWorkflowStore
) -> None:
    """WorkflowIdleEvent should schedule deferred release but NOT stamp idle_since."""
    inner_adapter = StubInternalAdapter(run_id="run-1")
    _seed_handler(store, run_id="run-1")

    adapter = _DBOSIdleReleaseInternalRunAdapter(inner_adapter, decorator, store)
    event = WorkflowIdleEvent()

    await adapter.write_to_event_stream(event)

    # Should NOT have stamped idle_since
    handler = store.handlers["handler-1"]
    assert handler.idle_since is None

    # Should have forwarded the event
    assert inner_adapter.written_events == [event]

    # Should have a deferred release task tracked by run_id
    assert "run-1" in decorator._deferred_release_tasks


@pytest.mark.asyncio()
async def test_release_uses_lifecycle_lock(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    lifecycle: FakeLifecycleLock,
) -> None:
    """Release should use begin_release and send TickIdleRelease."""
    await lifecycle.create("run-1")

    # Pre-populate an external adapter so get_external_adapter returns it
    stub_runtime._external_adapters["run-1"] = StubExternalAdapter(run_id="run-1")

    await decorator._release_idle_handler("run-1")

    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.releasing
    ext = stub_runtime._external_adapters["run-1"]
    assert len(ext.sent_events) == 1
    assert isinstance(ext.sent_events[0], TickIdleRelease)


@pytest.mark.asyncio()
async def test_release_skips_if_begin_release_fails(
    decorator: DBOSIdleReleaseDecorator,
    stub_runtime: StubRuntime,
    lifecycle: FakeLifecycleLock,
) -> None:
    """Release should skip if begin_release returns False."""
    # No row exists, so begin_release returns False already

    await decorator._release_idle_handler("run-1")

    # No external adapter should have been accessed / no events sent
    assert "run-1" not in stub_runtime._external_adapters


@pytest.mark.asyncio()
async def test_await_and_mark_released_sets_idle_since(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    lifecycle: FakeLifecycleLock,
) -> None:
    """After workflow completes, idle_since should be set and state marked released."""
    _seed_handler(store, run_id="run-1")

    await lifecycle.create("run-1")
    await lifecycle.begin_release("run-1")

    external = StubExternalAdapter(run_id="run-1")

    await decorator._await_and_mark_released("run-1", external)

    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.released
    handler = store.handlers["handler-1"]
    assert handler.idle_since is not None


@pytest.mark.asyncio()
async def test_send_event_resumes_when_released(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    lifecycle: FakeLifecycleLock,
    mock_dbos: AsyncMock,
) -> None:
    """send_event should resume workflow if lifecycle state is released."""
    lifecycle._states["run-1"] = (
        RunLifecycleState.released,
        datetime.now(timezone.utc),
    )

    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)

    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    adapter = DBOSIdleReleaseExternalRunAdapter(decorator, "run-1")

    tick = TickIdleCheck()
    await adapter.send_event(tick)

    # Should have started a new workflow run with the same run_id
    assert len(stub_runtime.run_workflow_calls) == 1
    call = stub_runtime.run_workflow_calls[0]
    assert call["run_id"] == "run-1"

    # Handler should have been updated in store
    handler = store.handlers["handler-1"]
    assert handler.idle_since is None
    assert handler.status == "running"
    assert handler.run_id == "run-1"

    # DBOS workflow record must be deleted before re-creating with same run_id
    mock_dbos.delete_workflow_async.assert_called_once_with("run-1")

    # Journal should have been purged
    mock_journal_crud.delete.assert_called_once_with("run-1")


@pytest.mark.asyncio()
async def test_send_event_sends_normally_when_active(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    lifecycle: FakeLifecycleLock,
) -> None:
    """send_event should send normally if lifecycle returns None (active)."""
    await lifecycle.create("run-1")

    # Pre-populate an external adapter
    stub_runtime._external_adapters["run-1"] = StubExternalAdapter(run_id="run-1")

    adapter = DBOSIdleReleaseExternalRunAdapter(decorator, "run-1")
    await adapter.send_event(TickIdleCheck())

    assert len(stub_runtime.run_workflow_calls) == 0
    ext = stub_runtime._external_adapters["run-1"]
    assert len(ext.sent_events) == 1


@pytest.mark.asyncio()
async def test_send_event_waits_on_releasing_then_resumes(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    lifecycle: FakeLifecycleLock,
    mock_dbos: AsyncMock,
) -> None:
    """send_event should poll when releasing, then resume when released."""
    lifecycle._states["run-1"] = (
        RunLifecycleState.releasing,
        datetime.now(timezone.utc),
    )

    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)

    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    adapter = DBOSIdleReleaseExternalRunAdapter(decorator, "run-1")

    async def _complete_release_during_sleep(delay: float) -> None:
        await lifecycle.complete_release("run-1")

    with patch("asyncio.sleep", side_effect=_complete_release_during_sleep):
        await adapter.send_event(TickIdleCheck())

    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.active
    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_send_event_force_resumes_on_crash_timeout(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    lifecycle: FakeLifecycleLock,
    mock_dbos: AsyncMock,
) -> None:
    """send_event should force resume if releasing state is stale."""
    lifecycle._states["run-1"] = (
        RunLifecycleState.releasing,
        datetime.now(timezone.utc),
    )
    stale_time = datetime.now(timezone.utc) - timedelta(
        seconds=CRASH_TIMEOUT_SECONDS + 10
    )
    lifecycle.set_updated_at("run-1", stale_time)

    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)

    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    adapter = DBOSIdleReleaseExternalRunAdapter(decorator, "run-1")

    await adapter.send_event(TickIdleCheck())

    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.active
    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_wait_receive_cancels_pending_release_timer(
    decorator: DBOSIdleReleaseDecorator, store: MemoryWorkflowStore
) -> None:
    """wait_receive returning a tick should cancel the pending release timer."""
    decorator._schedule_deferred_release("run-1")
    assert "run-1" in decorator._deferred_release_tasks
    task = decorator._deferred_release_tasks["run-1"]

    inner_adapter = StubInternalAdapter(run_id="run-1")

    # Override wait_receive to return a tick
    tick_result = WaitResultTick(tick=TickIdleCheck())

    async def _return_tick(timeout_seconds: float | None = None) -> WaitResult:
        return tick_result

    inner_adapter.wait_receive = _return_tick  # type: ignore[assignment]

    adapter = _DBOSIdleReleaseInternalRunAdapter(inner_adapter, decorator, store)

    result = await adapter.wait_receive(timeout_seconds=5.0)

    assert result is tick_result
    await asyncio.sleep(0)
    assert task.cancelled()
    assert "run-1" not in decorator._deferred_release_tasks


@pytest.mark.asyncio()
async def test_no_destroy_or_shutdown_cancellation(
    decorator: DBOSIdleReleaseDecorator,
) -> None:
    """Decorator should not have destroy/shutdown methods that cancel workflows."""
    assert not hasattr(decorator, "stop_task")
    assert not hasattr(decorator, "_on_server_stop")
    assert not hasattr(decorator, "_close_internal_adapter")
    assert not hasattr(decorator, "_active_run_ids")
    assert not hasattr(decorator, "_internal_adapters")


@pytest.mark.asyncio()
async def test_do_resume_carries_over_serialized_state(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
) -> None:
    """_do_resume should pass serialized_state from the old state store."""
    workflow = StatefulWorkflow()
    decorator.track_workflow(workflow)

    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    # Seed the state store with actual state data
    state_store = InMemoryStateStore(MyState(counter=42))
    store.state_stores[("run-1", ())] = state_store

    await decorator._do_resume("run-1")

    assert len(stub_runtime.run_workflow_calls) == 1
    call = stub_runtime.run_workflow_calls[0]
    serialized_state = call["serialized_state"]
    assert serialized_state is not None
    assert "counter" in serialized_state["state_data"]


@pytest.mark.asyncio()
async def test_send_event_polls_when_releasing_then_completes(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    lifecycle: FakeLifecycleLock,
    mock_dbos: AsyncMock,
) -> None:
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    # Start in releasing state
    lifecycle._states["run-1"] = (
        RunLifecycleState.releasing,
        datetime.now(timezone.utc),
    )

    sleep_count = 0

    async def _on_sleep(delay: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        # After first sleep, complete the release so next try_begin_resume returns released
        await lifecycle.complete_release("run-1")

    adapter = DBOSIdleReleaseExternalRunAdapter(decorator, "run-1")
    with patch("asyncio.sleep", side_effect=_on_sleep):
        await adapter.send_event(TickIdleCheck())

    assert sleep_count >= 1
    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_send_event_crash_timeout_boundary_does_not_force_resume(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    lifecycle: FakeLifecycleLock,
    mock_dbos: AsyncMock,
) -> None:
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    # Set exactly at boundary (not greater)
    lifecycle._states["run-1"] = (
        RunLifecycleState.releasing,
        datetime.now(timezone.utc),
    )
    boundary_time = datetime.now(timezone.utc) - timedelta(
        seconds=CRASH_TIMEOUT_SECONDS
    )
    lifecycle.set_updated_at("run-1", boundary_time)

    async def _complete_release_during_sleep(delay: float) -> None:
        await lifecycle.complete_release("run-1")

    adapter = DBOSIdleReleaseExternalRunAdapter(decorator, "run-1")
    with patch("asyncio.sleep", side_effect=_complete_release_during_sleep):
        await adapter.send_event(TickIdleCheck())

    # Should have resumed normally (not force), state should be active
    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.active
    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_do_resume_continues_when_old_dbos_workflow_gone(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
) -> None:
    mock_dbos.retrieve_workflow_async.side_effect = Exception("workflow not found")
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    await decorator._do_resume("run-1")

    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_do_resume_continues_when_state_carryover_fails(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
) -> None:
    workflow = StatefulWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    with patch.object(store, "create_state_store", side_effect=Exception("boom")):
        await decorator._do_resume("run-1")

    assert len(stub_runtime.run_workflow_calls) == 1
    call = stub_runtime.run_workflow_calls[0]
    assert call["serialized_state"] is None


@pytest.mark.asyncio()
async def test_do_resume_continues_when_journal_delete_fails(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
) -> None:
    mock_journal_crud.delete.side_effect = Exception("db error")
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    await decorator._do_resume("run-1")

    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_do_resume_continues_when_dbos_delete_fails(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
) -> None:
    mock_dbos.delete_workflow_async.side_effect = Exception("delete failed")
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    await decorator._do_resume("run-1")

    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_do_resume_raises_when_handler_not_found(
    decorator: DBOSIdleReleaseDecorator,
    mock_dbos: AsyncMock,
) -> None:
    with pytest.raises(ValueError, match="Expected 1 handler"):
        await decorator._do_resume("run-1")


@pytest.mark.asyncio()
async def test_do_resume_raises_when_workflow_not_tracked(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    mock_dbos: AsyncMock,
) -> None:
    _seed_handler(store, workflow_name="unknown_workflow", run_id="run-1")
    with pytest.raises(ValueError, match="not found"):
        await decorator._do_resume("run-1")


@pytest.mark.asyncio()
async def test_do_resume_includes_pending_tick_in_rebuilt_state(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
) -> None:
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    # Use TickAddEvent with StartEvent — this sets is_running=True in BrokerState,
    # giving us a concrete observable difference vs. no pending tick.
    tick = TickAddEvent(event=StartEvent())
    await decorator._do_resume("run-1", pending_tick=tick)

    assert len(stub_runtime.run_workflow_calls) == 1
    call = stub_runtime.run_workflow_calls[0]
    init_state = call["init_state"]
    assert init_state is not None
    # Without the pending tick injection, is_running would remain False.
    # StartEvent via TickAddEvent sets is_running=True in the rebuilt state.
    assert init_state.is_running is True


@pytest.mark.asyncio()
async def test_do_resume_replays_persisted_ticks(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
) -> None:
    """Persisted ticks must be replayed to rebuild BrokerState on resume (Fault 8)."""
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    # Seed a persisted TickAddEvent(StartEvent) — simulates a tick recorded
    # before the workflow was released.
    tick = TickAddEvent(event=StartEvent())
    tick_data = WorkflowTickAdapter.dump_python(tick)
    await store.append_tick("run-1", tick_data)

    await decorator._do_resume("run-1")

    assert len(stub_runtime.run_workflow_calls) == 1
    call = stub_runtime.run_workflow_calls[0]
    init_state = call["init_state"]
    assert init_state is not None
    # Without tick replay, is_running would remain False (bare BrokerState).
    # The persisted TickAddEvent(StartEvent) sets is_running=True.
    assert init_state.is_running is True


@pytest.mark.asyncio()
async def test_await_and_mark_released_handles_get_result_failure(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    lifecycle: FakeLifecycleLock,
) -> None:
    _seed_handler(store, run_id="run-1")

    external = StubExternalAdapter(run_id="run-1")
    external.get_result = AsyncMock(side_effect=Exception("get_result failed"))  # type: ignore[assignment]

    await decorator._await_and_mark_released("run-1", external)

    # complete_release should not have been called since get_result failed
    assert "run-1" not in lifecycle._states


@pytest.mark.asyncio()
async def test_deferred_release_fires_after_timeout(
    stub_runtime: StubRuntime,
    store: MemoryWorkflowStore,
    mock_journal_crud: AsyncMock,
    lifecycle: FakeLifecycleLock,
) -> None:
    dec = _make_decorator(
        stub_runtime, store, mock_journal_crud, lifecycle, idle_timeout=0.01
    )
    await lifecycle.create("run-1")
    stub_runtime._external_adapters["run-1"] = StubExternalAdapter(run_id="run-1")

    dec._schedule_deferred_release("run-1")
    await asyncio.sleep(0.05)

    state = lifecycle.get_state("run-1")
    assert state is not None
    # After deferred release fires, the handler goes through releasing -> released
    assert state[0] in (RunLifecycleState.releasing, RunLifecycleState.released)
