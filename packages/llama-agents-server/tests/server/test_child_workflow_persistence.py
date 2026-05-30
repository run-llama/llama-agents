# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Child-workflow composition through the server's durable persistence path.

Phase-4 of the child-workflows feature only exercised the in-memory basic
runtime. These tests drive a parent-with-child workflow through the server's
sqlite-backed tick journal + state store, across a full server restart, to
confirm:

- the namespaced ``StepId`` journal round-trips (child steps deserialize and
  are skipped on resume rather than re-run), and
- how per-namespace child state behaves under the single-``run_id`` server
  state store (which, unlike the basic runtime, does not partition by
  namespace).
"""

from __future__ import annotations

import pytest
from llama_agents.server import (
    HandlerQuery,
    PersistentHandler,
    SqliteWorkflowStore,
    WorkflowServer,
)
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent

# Counts child step executions across the (in-process) server restart so we can
# prove resume does not re-run an already-completed child step. A module global
# survives the WorkflowServer teardown the way an instance attribute would not.
CHILD_RUN_COUNTS: dict[str, int] = {}


class ChildStart(StartEvent):
    payload: str = ""


class ChildStop(StopEvent):
    out: str = ""


class HumanGo(Event):
    answer: str


class StateChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: ChildStart) -> ChildStop:
        CHILD_RUN_COUNTS["child"] = CHILD_RUN_COUNTS.get("child", 0) + 1
        # Written only by the child; used as an isolation probe against the
        # parent's root state store.
        await ctx.store.set("child_marker", "from-child")
        return ChildStop(out=ev.payload.upper())


class HitlParentWithChild(Workflow):
    child: StateChild

    @step
    async def begin(self, ev: StartEvent) -> ChildStart:
        return ChildStart(payload="hello")

    @step
    async def gather(self, ctx: Context, ev: ChildStop) -> HumanGo:
        # Child has completed; stash its output, then idle for a human input.
        await ctx.store.set("from_child", ev.out)
        human = await ctx.wait_for_event(HumanGo)
        return human

    @step
    async def complete(self, ctx: Context, ev: HumanGo) -> StopEvent:
        from_child = await ctx.store.get("from_child")
        return StopEvent(result=f"{from_child}:{ev.answer}")


EXTRA_EVENTS: list[type[Event]] = [HumanGo]


async def _wait_handler_status(
    store: SqliteWorkflowStore,
    handler_id: str,
    status: str,
    max_duration: float = 5.0,
) -> PersistentHandler:
    async def check() -> PersistentHandler:
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].status == status
        return found[0]

    return await wait_for_passing(check, max_duration=max_duration, interval=0.05)


async def _wait_handler_idle(
    store: SqliteWorkflowStore,
    handler_id: str,
    max_duration: float = 5.0,
) -> PersistentHandler:
    async def check() -> PersistentHandler:
        found = await store.query(HandlerQuery(handler_id_in=[handler_id]))
        assert len(found) == 1
        assert found[0].idle_since is not None
        return found[0]

    return await wait_for_passing(check, max_duration=max_duration, interval=0.05)


def _make_server(store: SqliteWorkflowStore) -> WorkflowServer:
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow(
        "test", HitlParentWithChild(child=StateChild()), additional_events=EXTRA_EVENTS
    )
    return server


@pytest.mark.asyncio
async def test_child_workflow_journal_round_trips_across_server_restart(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """A parent-with-child runs, idles at a HITL point after the child completed,
    survives a full server restart, and finishes -- without re-running the child
    step (the namespaced StepId journal round-trips through sqlite)."""
    CHILD_RUN_COUNTS.pop("child", None)
    handler_id = "child-restart-1"

    # Server 1: start, let the child complete and the parent idle at the human wait.
    server1 = _make_server(sqlite_store)
    async with server1.contextmanager():
        wf1 = server1._service._runtime.get_workflow("test")
        assert wf1 is not None
        await server1._service.start_workflow(wf1, handler_id)
        await _wait_handler_idle(sqlite_store, handler_id)

    # Child ran exactly once before the checkpoint.
    assert CHILD_RUN_COUNTS["child"] == 1

    # Server 2: restart (forces a reload from the persisted tick journal), feed
    # the human input, expect completion with the child's output threaded through.
    server2 = _make_server(sqlite_store)
    async with server2.contextmanager():
        await server2._service.send_event(handler_id, HumanGo(answer="42"))
        handler = await _wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "HELLO:42"

    # The already-completed child step was NOT re-run on resume; the namespaced
    # StepId deserialized and was recognized as done.
    assert CHILD_RUN_COUNTS["child"] == 1


@pytest.mark.asyncio
async def test_child_state_writes_are_not_persisted_under_server_runtime(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """KNOWN LIMITATION: a child step's ``ctx.store`` writes are NOT durable on
    the server.

    ``add_workflow`` switches the *parent* onto the server runtime, but an
    already-attached child keeps the basic in-memory runtime it adopted at
    construction. So a child step resolves an in-memory, per-namespace state
    store (never serialized to the ``workflow_state`` table), while only the
    parent's root store is persisted. The child's tick journal *is* persisted
    via the parent's root adapter, so the child step is replayed (not re-run)
    and its StopEvent output survives -- but anything it wrote to ``ctx.store``
    is gone after a reload.

    This test pins that behavior: the child-only key is absent from the
    persisted root store, and it did not leak into the parent's state either.
    A future fix that makes child state durable (per-namespace server stores)
    should deliberately flip these assertions.
    """
    CHILD_RUN_COUNTS.pop("child", None)
    handler_id = "child-state-1"
    sentinel = object()

    server = _make_server(sqlite_store)
    async with server.contextmanager():
        wf = server._service._runtime.get_workflow("test")
        assert wf is not None
        await server._service.start_workflow(wf, handler_id)
        await _wait_handler_idle(sqlite_store, handler_id)

    # The child actually ran (proving it isn't simply being skipped).
    assert CHILD_RUN_COUNTS["child"] == 1

    run_id = (await sqlite_store.query(HandlerQuery(handler_id_in=[handler_id])))[
        0
    ].run_id
    assert run_id is not None

    persisted = sqlite_store.create_state_store(run_id)
    # The child-only key was never persisted (in-memory child store, lost).
    assert await persisted.get("child_marker", sentinel) is sentinel
    # The parent's own write to the root store IS persisted.
    assert await persisted.get("from_child") == "HELLO"
