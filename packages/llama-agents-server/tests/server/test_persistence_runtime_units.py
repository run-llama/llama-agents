# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
# ty: ignore[invalid-assignment]
"""Unit tests for persistence_runtime branches not exercised by integration tests.

Complements test_durable_runtime.py. Focuses on individual helpers and
swallowed-exception paths: handler_status_from_exit_command branches, store
write failures, track/untrack mechanics, destroy() cleanup edge cases, and
legacy ctx error handling.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from llama_agents.server import (
    HandlerQuery,
    MemoryWorkflowStore,
    PersistentHandler,
    WorkflowServer,
)
from llama_agents.server._runtime.persistence_runtime import (
    PersistenceDecorator,
    handler_status_from_exit_command,
)
from server_test_fixtures import (  # type: ignore[import]
    SimpleTestWorkflow,
    wait_for_passing,
)
from workflows import Workflow, step
from workflows.context.context_types import SerializedContext
from workflows.errors import WorkflowCancelledByUser
from workflows.events import Event, IdleReleasedEvent, StartEvent, StopEvent
from workflows.plugins.basic import basic_runtime
from workflows.runtime.types.commands import (
    CommandCompleteRun,
    CommandFailWorkflow,
    CommandHalt,
)

# Suppress test_durable_runtime imports being needed; we use the persistence helper directly.
from test_durable_runtime import _get_persistence  # type: ignore[import]


# --- Helpers / fake stores --------------------------------------------------


class FlakyAppendStore(MemoryWorkflowStore):
    """MemoryWorkflowStore whose append_tick always raises."""

    def __init__(self) -> None:
        super().__init__()
        self.append_calls = 0

    async def append_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        self.append_calls += 1
        raise RuntimeError("append_tick boom")


class FlakyAfterTickStore(MemoryWorkflowStore):
    """MemoryWorkflowStore whose after_tick always raises, but append_tick succeeds."""

    def __init__(self) -> None:
        super().__init__()
        self.after_calls = 0

    async def after_tick(self, run_id: str, tick_data: dict[str, Any]) -> None:
        self.after_calls += 1
        raise RuntimeError("after_tick boom")


class LegacyCtxRaisingStore(MemoryWorkflowStore):
    """Store that satisfies the LegacyContextStore protocol but always raises."""

    def get_legacy_ctx(self, run_id: str) -> dict[str, Any] | None:
        raise RuntimeError("legacy boom")


class LegacyCtxReturningStore(MemoryWorkflowStore):
    """Store that satisfies the LegacyContextStore protocol and returns a fixed dict."""

    def __init__(self, legacy_ctx: dict[str, Any] | None) -> None:
        super().__init__()
        self._legacy_ctx = legacy_ctx

    def get_legacy_ctx(self, run_id: str) -> dict[str, Any] | None:
        return self._legacy_ctx


def _stop_event_with_result(result: object) -> StopEvent:
    return StopEvent(result=result)


class _Intermediate(Event):
    """Bridge event between steps in TwoStepWorkflow."""


class TwoStepWorkflow(Workflow):
    """Two-step workflow whose first step emits a non-terminal event.

    Needed for the after_tick failure test: the control loop only invokes
    adapter.after_tick when the tick's commands produce no result. A single-
    step workflow that emits StopEvent skips the after_tick call on its only
    TickStepResult. Two steps guarantee an intermediate TickStepResult that
    DOES invoke after_tick.
    """

    @step
    async def first(self, ev: StartEvent) -> _Intermediate:
        return _Intermediate()

    @step
    async def second(self, ev: _Intermediate) -> StopEvent:
        return StopEvent(result="done")


# --- handler_status_from_exit_command (5 branches) --------------------------


def test_handler_status_idle_release_returns_none() -> None:
    cmd = CommandCompleteRun(result=IdleReleasedEvent())
    assert handler_status_from_exit_command(cmd) is None


def test_handler_status_complete_run_returns_completed_with_result() -> None:
    stop = _stop_event_with_result("ok")
    cmd = CommandCompleteRun(result=stop)
    result = handler_status_from_exit_command(cmd)
    assert result == ("completed", stop, None)


def test_handler_status_fail_workflow_returns_failed_with_error_string() -> None:
    cmd = CommandFailWorkflow(step_name="x", exception=RuntimeError("bad"))
    result = handler_status_from_exit_command(cmd)
    assert result == ("failed", None, "bad")


def test_handler_status_halt_cancelled_returns_cancelled() -> None:
    cmd = CommandHalt(exception=WorkflowCancelledByUser())
    result = handler_status_from_exit_command(cmd)
    assert result == ("cancelled", None, None)


def test_handler_status_halt_other_returns_failed() -> None:
    cmd = CommandHalt(exception=TimeoutError("slow"))
    result = handler_status_from_exit_command(cmd)
    assert result == ("failed", None, "slow")


# --- _PersistenceInternalRunAdapter store-failure swallow paths -------------


@pytest.mark.asyncio
async def test_on_tick_store_failure_is_logged_and_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failing append_tick must be logged and the workflow must still complete."""
    store = FlakyAppendStore()
    wf = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow("test", wf)

    with caplog.at_level(
        logging.ERROR, logger="llama_agents.server._runtime.persistence_runtime"
    ):
        async with server.contextmanager():
            await server._service.start_workflow(wf, "ot-1")

            async def store_called() -> None:
                assert store.append_calls > 0

            await wait_for_passing(store_called, max_duration=3.0, interval=0.05)

    # Both prove the swallow path: store.append_tick was called (and raised), and
    # the persistence_runtime logger captured the exception message.
    assert any(
        "Failed to persist tick for run" in r.message for r in caplog.records
    )


@pytest.mark.asyncio
async def test_after_tick_store_failure_is_logged_and_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failing after_tick must be logged and the workflow must still complete."""
    store = FlakyAfterTickStore()
    wf = TwoStepWorkflow()
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow("test", wf)

    with caplog.at_level(
        logging.ERROR, logger="llama_agents.server._runtime.persistence_runtime"
    ):
        async with server.contextmanager():
            await server._service.start_workflow(wf, "at-1")

            async def store_called() -> None:
                assert store.after_calls > 0

            await wait_for_passing(store_called, max_duration=3.0, interval=0.05)

    assert any(
        "Failed to gather pending writes for run" in r.message for r in caplog.records
    )


# --- track_workflow / untrack_workflow --------------------------------------


@pytest.mark.asyncio
async def test_track_workflow_registers_by_name(
    memory_store: MemoryWorkflowStore,
) -> None:
    wf = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("tracked", wf)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        assert persistence.get_tracked_workflow("tracked") is wf


@pytest.mark.asyncio
async def test_untrack_workflow_removes_by_name(
    memory_store: MemoryWorkflowStore,
) -> None:
    wf = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("to-remove", wf)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        assert persistence.get_tracked_workflow("to-remove") is wf

        persistence.untrack_workflow(wf)
        assert persistence.get_tracked_workflow("to-remove") is None


@pytest.mark.asyncio
async def test_untrack_workflow_is_idempotent_on_missing(
    memory_store: MemoryWorkflowStore,
) -> None:
    """untrack_workflow() on a workflow that was never tracked is a no-op."""
    wf = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        # Never tracked, should not raise.
        persistence.untrack_workflow(wf)
        assert persistence.get_tracked_workflow(wf.workflow_name) is None


# --- context_from_ticks no-data ---------------------------------------------


@pytest.mark.asyncio
async def test_context_from_ticks_no_ticks_no_legacy_returns_none(
    memory_store: MemoryWorkflowStore,
) -> None:
    """No ticks AND no legacy ctx → context_from_ticks returns None."""
    wf = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("nope", wf)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        # MemoryWorkflowStore does not implement LegacyContextStore, so the
        # legacy_ctx branch is None; combined with no ticks for this run_id,
        # the function should return None.
        result = await persistence.context_from_ticks(wf, "run-id-unknown")
        assert result is None
        # And no state-store should have been created as a side effect.
        assert "run-id-unknown" not in memory_store.state_stores


# --- destroy() cleanup branches ---------------------------------------------


@pytest.mark.asyncio
async def test_destroy_with_no_resume_task_is_noop() -> None:
    """destroy() must not raise when resume_task is None (launch never called)."""
    decorator = PersistenceDecorator(basic_runtime, store=MemoryWorkflowStore())
    assert decorator.resume_task is None
    # Skip launch(). destroy() should be a no-op.
    await decorator.destroy()


class _RaisingCancelTask:
    """Stand-in for an asyncio.Task whose .cancel() raises."""

    def cancel(self) -> bool:
        raise RuntimeError("cancel boom")


@pytest.mark.asyncio
async def test_destroy_swallows_cancel_exception(
    memory_store: MemoryWorkflowStore,
) -> None:
    """If resume_task.cancel() raises, destroy() must swallow it."""
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", SimpleTestWorkflow())

    async with server.contextmanager():
        persistence = _get_persistence(server)
        # Make sure the real resume task is done before we swap it out.
        if persistence.resume_task is not None:
            try:
                await persistence.resume_task
            except BaseException:
                pass
        persistence.resume_task = _RaisingCancelTask()  # pyright: ignore[reportAttributeAccessIssue]
        # destroy() is called when the contextmanager exits; explicitly invoke
        # here so any exception surfaces in this test rather than the teardown.
        await persistence.destroy()


# --- _get_legacy_ctx exception path -----------------------------------------


@pytest.mark.asyncio
async def test_get_legacy_ctx_swallow_and_warn_on_store_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If the store raises in get_legacy_ctx, _get_legacy_ctx returns None and warns."""
    store = LegacyCtxRaisingStore()
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow("x", SimpleTestWorkflow())

    async with server.contextmanager():
        persistence = _get_persistence(server)
        with caplog.at_level(logging.WARNING):
            result = persistence._get_legacy_ctx("run-x")
        assert result is None
        assert any("Failed to read legacy ctx" in r.message for r in caplog.records)


# --- _seed_legacy_state early-return branches -------------------------------


@pytest.mark.asyncio
async def test_seed_legacy_state_parse_exception_warns_and_returns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed legacy ctx dict should log a warning and return cleanly."""
    store = MemoryWorkflowStore()
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow("x", SimpleTestWorkflow())

    async with server.contextmanager():
        persistence = _get_persistence(server)
        with caplog.at_level(
            logging.WARNING,
            logger="llama_agents.server._runtime.persistence_runtime",
        ):
            # broker_log expects list — feeding a string triggers a ValidationError.
            persistence._seed_legacy_state("rid", {"broker_log": "not-a-list"})
        assert any(
            "Failed to parse legacy ctx for state migration" in r.message
            for r in caplog.records
        )


@pytest.mark.asyncio
async def test_seed_legacy_state_empty_state_data_returns_early(
    memory_store: MemoryWorkflowStore,
) -> None:
    """If parsed.state is falsy, _seed_legacy_state returns without creating state."""
    wf = SimpleTestWorkflow()
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("x", wf)

    # Build a SerializedContext dict with an empty state.
    from workflows.context.serializers import JsonSerializer
    from workflows.runtime.types.internal_state import BrokerState

    serializer = JsonSerializer()
    init_state = BrokerState.from_workflow(wf)
    serialized = init_state.to_serialized(serializer)
    legacy = serialized.model_dump()
    legacy["state"] = {}

    # Sanity-check that this dict still parses.
    SerializedContext.from_dict_auto(legacy)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        persistence._seed_legacy_state("rid-empty", legacy)
        # No state store should have been created.
        assert "rid-empty" not in memory_store.state_stores


@pytest.mark.asyncio
async def test_seed_legacy_state_non_sqlite_store_returns_early() -> None:
    """If the state store isn't SqliteStateStore, _seed_legacy_state returns early."""
    from workflows.context.serializers import JsonSerializer
    from workflows.context.state_store import DictState, serialize_dict_state_data
    from workflows.runtime.types.internal_state import BrokerState

    wf = SimpleTestWorkflow()
    serializer = JsonSerializer()

    dict_state = DictState()
    dict_state["k"] = "v"
    state_payload = {
        "store_type": "in_memory",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": serialize_dict_state_data(dict_state, serializer, ()),
    }
    init_state = BrokerState.from_workflow(wf)
    serialized = init_state.to_serialized(serializer)
    legacy = serialized.model_dump()
    legacy["state"] = state_payload

    store = LegacyCtxReturningStore(legacy_ctx=legacy)
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow("x", wf)

    async with server.contextmanager():
        persistence = _get_persistence(server)
        # Calling _seed_legacy_state should hit the
        # `not isinstance(state_store, SqliteStateStore)` early return.
        persistence._seed_legacy_state("rid-non-sqlite", legacy)
        # MemoryWorkflowStore.create_state_store will have created an
        # InMemoryStateStore as a side effect, but the legacy payload should
        # not have been written into it — verify the key is absent.
        state_store = store.state_stores["rid-non-sqlite"]
        assert await state_store.get("k", None) is None


# --- _on_server_start with run_id=None --------------------------------------


@pytest.mark.asyncio
async def test_on_server_start_handles_run_id_none(
    memory_store: MemoryWorkflowStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A handler row with run_id=None must be skipped with an error log."""
    await memory_store.update(
        PersistentHandler(
            handler_id="orphan-1",
            workflow_name="test",
            status="running",
            run_id=None,
        )
    )

    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow("test", SimpleTestWorkflow())

    with caplog.at_level(logging.ERROR):
        async with server.contextmanager():
            persistence = _get_persistence(server)
            # Wait for the resume task to finish so we can assert on its effects.
            if persistence.resume_task is not None:
                try:
                    await persistence.resume_task
                except BaseException:
                    pass

    assert any(
        "Run ID is required for handler orphan-1" in r.message for r in caplog.records
    )
    # The handler row must remain untouched (still running, not failed).
    found = await memory_store.query(HandlerQuery(handler_id_in=["orphan-1"]))
    assert len(found) == 1
    assert found[0].status == "running"


# Silence unused-import noise (kept for type clarity).
_ = Workflow
