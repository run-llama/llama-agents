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
from llama_agents.dbos.journal.lifecycle import (
    ResumeClaim,
    RunLifecycleLock,
    RunLifecycleState,
)
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
from workflows.runtime.types.internal_state import BrokerState
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

    def get_state_store(self) -> StateStore[Any] | None:
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

    def get_state_store(self) -> StateStore[Any] | None:
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
        self._states: dict[str, tuple[RunLifecycleState, datetime, str | None]] = {}

    async def create(self, run_id: str) -> None:
        self._states[run_id] = (
            RunLifecycleState.active,
            datetime.now(timezone.utc),
            None,
        )

    async def begin_release(self, run_id: str) -> bool:
        entry = self._states.get(run_id)
        if entry is None or entry[0] != RunLifecycleState.active:
            return False
        self._states[run_id] = (
            RunLifecycleState.releasing,
            datetime.now(timezone.utc),
            None,
        )
        return True

    async def complete_release(self, run_id: str) -> bool:
        entry = self._states.get(run_id)
        if entry is not None and entry[0] == RunLifecycleState.releasing:
            self._states[run_id] = (
                RunLifecycleState.released,
                datetime.now(timezone.utc),
                None,
            )
            return True
        return False

    async def try_begin_resume(
        self, run_id: str, crash_timeout_seconds: float | None = None
    ) -> ResumeClaim | RunLifecycleState | None:
        entry = self._states.get(run_id)
        if entry is None:
            return None
        state, updated_at, _token = entry
        if state == RunLifecycleState.active:
            return None
        if state == RunLifecycleState.released or (
            state in (RunLifecycleState.releasing, RunLifecycleState.resuming)
            and crash_timeout_seconds is not None
            and (datetime.now(timezone.utc) - updated_at).total_seconds()
            > crash_timeout_seconds
        ):
            token = datetime.now(timezone.utc)
            self._states[run_id] = (
                RunLifecycleState.resuming,
                token,
                None,
            )
            return ResumeClaim(token=token, previous_state=state)
        return state

    async def is_resume_owner(self, run_id: str, token: datetime | str) -> bool:
        entry = self._states.get(run_id)
        return (
            entry is not None
            and entry[0] == RunLifecycleState.resuming
            and entry[1] == token
        )

    async def refresh_resume_owner(
        self, run_id: str, token: datetime | str
    ) -> ResumeClaim | None:
        if not await self.is_resume_owner(run_id, token):
            return None
        new_token = datetime.now(timezone.utc)
        self._states[run_id] = (
            RunLifecycleState.resuming,
            new_token,
            None,
        )
        return ResumeClaim(
            token=new_token,
            previous_state=RunLifecycleState.resuming,
        )

    async def complete_resume(self, run_id: str, token: datetime | str) -> bool:
        if not await self.is_resume_owner(run_id, token):
            return False
        self._states[run_id] = (
            RunLifecycleState.active,
            datetime.now(timezone.utc),
            None,
        )
        return True

    def get_state(
        self, run_id: str
    ) -> tuple[RunLifecycleState, datetime, str | None] | None:
        """Test-only helper for assertions."""
        return self._states.get(run_id)

    def set_updated_at(self, run_id: str, updated_at: datetime) -> None:
        """Test hook: override the updated_at timestamp for crash timeout testing."""
        entry = self._states.get(run_id)
        if entry is not None:
            self._states[run_id] = (entry[0], updated_at, entry[2])


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


async def _claim_resume(
    lifecycle: FakeLifecycleLock,
    run_id: str = "run-1",
    previous_state: RunLifecycleState = RunLifecycleState.released,
) -> ResumeClaim:
    lifecycle._states[run_id] = (
        previous_state,
        datetime.now(timezone.utc),
        None,
    )
    claim = await lifecycle.try_begin_resume(run_id)
    assert isinstance(claim, ResumeClaim)
    return claim


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
async def test_run_workflow_creates_lifecycle_row(
    decorator: DBOSIdleReleaseDecorator,
    lifecycle: FakeLifecycleLock,
) -> None:
    workflow = SimpleWorkflow()

    decorator.run_workflow(
        "run-1",
        workflow,
        init_state=BrokerState.from_workflow(workflow),
        start_event=StartEvent(),
    )
    await asyncio.sleep(0)

    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.active


@pytest.mark.asyncio()
async def test_release_waits_for_lifecycle_create(
    stub_runtime: StubRuntime,
    store: MemoryWorkflowStore,
    mock_journal_crud: AsyncMock,
) -> None:
    class SlowCreateLifecycle(FakeLifecycleLock):
        def __init__(self) -> None:
            super().__init__()
            self.allow_create = asyncio.Event()

        async def create(self, run_id: str) -> None:
            await self.allow_create.wait()
            await super().create(run_id)

    lifecycle = SlowCreateLifecycle()
    decorator = _make_decorator(stub_runtime, store, mock_journal_crud, lifecycle)
    workflow = SimpleWorkflow()
    stub_runtime._external_adapters["run-1"] = StubExternalAdapter(run_id="run-1")

    decorator.run_workflow(
        "run-1",
        workflow,
        init_state=BrokerState.from_workflow(workflow),
        start_event=StartEvent(),
    )
    release_task = asyncio.create_task(decorator._release_idle_handler("run-1"))
    await asyncio.sleep(0)

    ext = stub_runtime._external_adapters["run-1"]
    assert ext.sent_events == []

    lifecycle.allow_create.set()
    await release_task

    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] in (RunLifecycleState.releasing, RunLifecycleState.released)
    assert len(ext.sent_events) == 1
    assert isinstance(ext.sent_events[0], TickIdleRelease)


@pytest.mark.asyncio()
async def test_deferred_release_can_be_cancelled_while_lifecycle_create_pending(
    stub_runtime: StubRuntime,
    store: MemoryWorkflowStore,
    mock_journal_crud: AsyncMock,
) -> None:
    class SlowCreateLifecycle(FakeLifecycleLock):
        def __init__(self) -> None:
            super().__init__()
            self.allow_create = asyncio.Event()

        async def create(self, run_id: str) -> None:
            await self.allow_create.wait()
            await super().create(run_id)

    lifecycle = SlowCreateLifecycle()
    decorator = _make_decorator(
        stub_runtime, store, mock_journal_crud, lifecycle, idle_timeout=0.01
    )
    workflow = SimpleWorkflow()
    stub_runtime._external_adapters["run-1"] = StubExternalAdapter(run_id="run-1")

    decorator.run_workflow(
        "run-1",
        workflow,
        init_state=BrokerState.from_workflow(workflow),
        start_event=StartEvent(),
    )
    decorator._schedule_deferred_release("run-1")
    await asyncio.sleep(0.05)

    assert "run-1" in decorator._deferred_release_tasks
    decorator._cancel_deferred_release("run-1")
    lifecycle.allow_create.set()
    await asyncio.sleep(0)

    ext = stub_runtime._external_adapters["run-1"]
    assert ext.sent_events == []
    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.active


@pytest.mark.asyncio()
async def test_cancelled_deferred_release_does_not_cancel_lifecycle_create(
    stub_runtime: StubRuntime,
    store: MemoryWorkflowStore,
    mock_journal_crud: AsyncMock,
) -> None:
    class SlowCreateLifecycle(FakeLifecycleLock):
        def __init__(self) -> None:
            super().__init__()
            self.allow_create = asyncio.Event()

        async def create(self, run_id: str) -> None:
            await self.allow_create.wait()
            await super().create(run_id)

    lifecycle = SlowCreateLifecycle()
    decorator = _make_decorator(
        stub_runtime, store, mock_journal_crud, lifecycle, idle_timeout=0.01
    )
    workflow = SimpleWorkflow()

    decorator.run_workflow(
        "run-1",
        workflow,
        init_state=BrokerState.from_workflow(workflow),
        start_event=StartEvent(),
    )
    decorator._schedule_deferred_release("run-1")
    await asyncio.sleep(0.05)

    decorator._cancel_deferred_release("run-1")
    lifecycle.allow_create.set()
    await asyncio.sleep(0)

    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.active


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
async def test_await_and_mark_released_skips_idle_since_if_release_not_owned(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    lifecycle: FakeLifecycleLock,
) -> None:
    _seed_handler(store, run_id="run-1")
    lifecycle._states["run-1"] = (
        RunLifecycleState.resuming,
        datetime.now(timezone.utc),
        "token",
    )

    external = StubExternalAdapter(run_id="run-1")

    await decorator._await_and_mark_released("run-1", external)

    handler = store.handlers["handler-1"]
    assert handler.idle_since is None


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
        None,
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
        None,
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
async def test_second_send_waits_while_resume_is_in_progress(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    lifecycle: FakeLifecycleLock,
) -> None:
    lifecycle._states["run-1"] = (
        RunLifecycleState.released,
        datetime.now(timezone.utc),
        None,
    )
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )

    resume_started = asyncio.Event()
    resume_continue = asyncio.Event()

    async def _slow_resume(
        run_id: str,
        resume_claim: ResumeClaim,
        pending_tick: WorkflowTick | None = None,
    ) -> tuple[str, ExternalRunAdapter] | None:
        resume_started.set()
        await resume_continue.wait()
        await lifecycle.complete_resume(run_id, resume_claim.token)
        return run_id, stub_runtime.get_external_adapter(run_id)

    decorator._do_resume = _slow_resume  # type: ignore[method-assign]
    original_sleep = asyncio.sleep

    async def _fast_sleep(delay: float) -> None:
        await original_sleep(0)

    first = asyncio.create_task(
        DBOSIdleReleaseExternalRunAdapter(decorator, "run-1").send_event(
            TickIdleCheck()
        )
    )
    await resume_started.wait()
    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.resuming

    second = asyncio.create_task(
        DBOSIdleReleaseExternalRunAdapter(decorator, "run-1").send_event(
            TickIdleCheck()
        )
    )
    with patch("asyncio.sleep", side_effect=_fast_sleep):
        await original_sleep(0)
        ext = stub_runtime.get_external_adapter("run-1")
        assert isinstance(ext, StubExternalAdapter)
        assert ext.sent_events == []

        resume_continue.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=1)

    ext = stub_runtime.get_external_adapter("run-1")
    assert isinstance(ext, StubExternalAdapter)
    assert len(ext.sent_events) == 1


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
        None,
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
    lifecycle: FakeLifecycleLock,
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
    store.state_stores["run-1"] = state_store

    claim = await _claim_resume(lifecycle)
    await decorator._do_resume("run-1", resume_claim=claim)

    assert len(stub_runtime.run_workflow_calls) == 1
    call = stub_runtime.run_workflow_calls[0]
    serialized_state = call["serialized_state"]
    assert serialized_state is not None
    assert "counter" in serialized_state["state_data"]


@pytest.mark.asyncio()
async def test_do_resume_stops_if_resume_ownership_was_lost(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
) -> None:
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    claim = await _claim_resume(lifecycle)
    lifecycle._states["run-1"] = (
        RunLifecycleState.resuming,
        datetime.now(timezone.utc),
        "new-owner",
    )

    result = await decorator._do_resume("run-1", resume_claim=claim)

    assert result is None
    assert len(stub_runtime.run_workflow_calls) == 0
    mock_dbos.delete_workflow_async.assert_not_called()
    mock_journal_crud.delete.assert_not_called()


@pytest.mark.asyncio()
async def test_do_resume_refreshes_owner_before_starting_replacement(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
) -> None:
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    claim = await _claim_resume(lifecycle)

    async def _delete_and_steal(run_id: str) -> None:
        lifecycle._states[run_id] = (
            RunLifecycleState.resuming,
            datetime.now(timezone.utc),
            "new-owner",
        )

    mock_journal_crud.delete.side_effect = _delete_and_steal

    result = await decorator._do_resume("run-1", resume_claim=claim)

    assert result is None
    assert len(stub_runtime.run_workflow_calls) == 0


@pytest.mark.asyncio()
async def test_do_resume_stale_resuming_takeover_does_not_wait_for_dbos_result(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
) -> None:
    workflow = SimpleWorkflow()
    decorator.track_workflow(workflow)
    _seed_handler(
        store,
        workflow_name=workflow.workflow_name,
        run_id="run-1",
        idle_since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    lifecycle._states["run-1"] = (
        RunLifecycleState.resuming,
        datetime.now(timezone.utc) - timedelta(seconds=CRASH_TIMEOUT_SECONDS + 10),
        "old-owner",
    )
    claim = await lifecycle.try_begin_resume(
        "run-1", crash_timeout_seconds=CRASH_TIMEOUT_SECONDS
    )
    assert isinstance(claim, ResumeClaim)
    assert claim.previous_state == RunLifecycleState.resuming

    await decorator._do_resume("run-1", resume_claim=claim)

    mock_dbos.retrieve_workflow_async.assert_not_called()
    assert len(stub_runtime.run_workflow_calls) == 1
    state = lifecycle.get_state("run-1")
    assert state is not None
    assert state[0] == RunLifecycleState.active


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
        None,
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
        None,
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
    lifecycle: FakeLifecycleLock,
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

    claim = await _claim_resume(lifecycle)
    await decorator._do_resume("run-1", resume_claim=claim)

    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_do_resume_continues_when_state_carryover_fails(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
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
        claim = await _claim_resume(lifecycle)
        await decorator._do_resume("run-1", resume_claim=claim)

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
    lifecycle: FakeLifecycleLock,
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

    claim = await _claim_resume(lifecycle)
    await decorator._do_resume("run-1", resume_claim=claim)

    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_do_resume_continues_when_dbos_delete_fails(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
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

    claim = await _claim_resume(lifecycle)
    await decorator._do_resume("run-1", resume_claim=claim)

    assert len(stub_runtime.run_workflow_calls) == 1


@pytest.mark.asyncio()
async def test_do_resume_raises_when_handler_not_found(
    decorator: DBOSIdleReleaseDecorator,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
) -> None:
    claim = await _claim_resume(lifecycle)
    with pytest.raises(ValueError, match="Expected 1 handler"):
        await decorator._do_resume("run-1", resume_claim=claim)


@pytest.mark.asyncio()
async def test_do_resume_raises_when_workflow_not_tracked(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
) -> None:
    _seed_handler(store, workflow_name="unknown_workflow", run_id="run-1")
    claim = await _claim_resume(lifecycle)
    with pytest.raises(ValueError, match="not found"):
        await decorator._do_resume("run-1", resume_claim=claim)


@pytest.mark.asyncio()
async def test_do_resume_includes_pending_tick_in_rebuilt_state(
    decorator: DBOSIdleReleaseDecorator,
    store: MemoryWorkflowStore,
    stub_runtime: StubRuntime,
    mock_journal_crud: AsyncMock,
    mock_dbos: AsyncMock,
    lifecycle: FakeLifecycleLock,
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
    claim = await _claim_resume(lifecycle)
    await decorator._do_resume("run-1", resume_claim=claim, pending_tick=tick)

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
    lifecycle: FakeLifecycleLock,
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

    claim = await _claim_resume(lifecycle)
    await decorator._do_resume("run-1", resume_claim=claim)

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
