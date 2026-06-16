# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Child-workflow composition through the server's durable persistence path.

These tests drive a parent-with-child workflow through the server's
sqlite-backed tick journal + state store, across a full server restart, to
confirm:

- the namespaced ``StepId`` journal round-trips (child steps deserialize and
  are skipped on resume rather than re-run), and
- a child step's ``ctx.store`` writes are durable: they persist into the
  single ``workflow_state`` row under the nested ``children`` slot,
  survive a full server restart, and stay isolated from the parent's root
  state (and, for grandchildren, from intermediate namespaces).
"""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from llama_agents.server import (
    HandlerQuery,
    PersistentHandler,
    SqliteWorkflowStore,
    WorkflowServer,
)
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows import Context, Workflow, step
from workflows.context.state_store import CHILD_STATES_KEY
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
async def test_child_state_writes_are_persisted_under_server_runtime(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """A child step's ``ctx.store`` writes are durable on the server.

    The runtime switch now propagates into the child, and the server adapter
    is namespace-aware: the whole child tree's state persists in the single
    ``workflow_state`` row, with the child's compact payload nested under the
    reserved ``children`` slot. So a child's write survives a full server
    restart while staying isolated from the parent's root state.

    The child-only key is NOT at the decoded root store's top level.
    Durability is verified by inspecting the raw ``children`` slice directly,
    and by restarting the server and confirming the parent finishes with the
    child's threaded-through output.
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

    # A child-ful run appends every child payload to the root row. Decoded reads
    # expose the root namespace; raw storage exposes the compact child payloads.
    persisted = sqlite_store.create_state_store(run_id)
    assert await persisted.get("child_marker", sentinel) is sentinel
    assert await persisted.get("from_child") == "HELLO"

    record = await cast(Any, persisted)._storage.load()
    assert record is not None
    blob = json.loads(record.data)
    root_data = blob["_data"]
    child_states = blob.get(CHILD_STATES_KEY)
    assert child_states is not None
    assert "child" in child_states
    child_payload = child_states["child"]["_data"]

    # The parent's root write is durable under the root slot.
    assert root_data["from_child"] == '"HELLO"'
    # The child's ``ctx.store`` write is durable in its own nested slot, isolated
    # from the parent's root namespace.
    assert child_payload["child_marker"] == '"from-child"'
    assert "child_marker" not in root_data
    assert "from_child" not in child_payload

    # Restart: resume from the persisted journal + state, finish the run. The
    # child step is replayed (not re-run) and its output threads through.
    server2 = _make_server(sqlite_store)
    async with server2.contextmanager():
        await server2._service.send_event(handler_id, HumanGo(answer="ok"))
        handler = await _wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "HELLO:ok"
    assert CHILD_RUN_COUNTS["child"] == 1


# ---------------------------------------------------------------------------
# Three-level (grandchild) durability
# ---------------------------------------------------------------------------


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    pass


class MidStart(StartEvent):
    pass


class MidStop(StopEvent):
    pass


class StateGrandchild(Workflow):
    @step
    async def run_grand(self, ctx: Context, ev: GrandStart) -> GrandStop:
        CHILD_RUN_COUNTS["grand"] = CHILD_RUN_COUNTS.get("grand", 0) + 1
        await ctx.store.set("grand_marker", "from-grand")
        return GrandStop()


class StateMid(Workflow):
    grand: StateGrandchild

    @step
    async def begin(self, ctx: Context, ev: MidStart) -> GrandStart:
        await ctx.store.set("mid_marker", "from-mid")
        return GrandStart()

    @step
    async def finish(self, ev: GrandStop) -> MidStop:
        return MidStop()


class HitlTopWithGrandchild(Workflow):
    mid: StateMid

    @step
    async def begin(self, ev: StartEvent) -> MidStart:
        return MidStart()

    @step
    async def gather(self, ctx: Context, ev: MidStop) -> HumanGo:
        await ctx.store.set("top_marker", "from-top")
        return await ctx.wait_for_event(HumanGo)

    @step
    async def complete(self, ctx: Context, ev: HumanGo) -> StopEvent:
        top = await ctx.store.get("top_marker")
        return StopEvent(result=f"{top}:{ev.answer}")


def _make_grandchild_server(store: SqliteWorkflowStore) -> WorkflowServer:
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow(
        "test",
        HitlTopWithGrandchild(mid=StateMid(grand=StateGrandchild())),
        additional_events=EXTRA_EVENTS,
    )
    return server


@pytest.mark.asyncio
async def test_grandchild_state_writes_are_durable_and_isolated(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """A grandchild's ``ctx.store`` write survives a restart and stays isolated.

    A top -> mid -> grandchild tree, each namespace writing its own marker,
    idles at a HITL point after the grandchild completed. The persisted row
    holds each namespace's payload in its own ``__child_states__`` slot keyed by
    the "/"-joined path (``mid``, ``mid/grand``); markers never cross
    namespaces. A full server restart resumes and completes the run.
    """
    CHILD_RUN_COUNTS.pop("grand", None)
    handler_id = "grand-state-1"
    sentinel = object()

    server = _make_grandchild_server(sqlite_store)
    async with server.contextmanager():
        wf = server._service._runtime.get_workflow("test")
        assert wf is not None
        await server._service.start_workflow(wf, handler_id)
        await _wait_handler_idle(sqlite_store, handler_id)

    assert CHILD_RUN_COUNTS["grand"] == 1

    run_id = (await sqlite_store.query(HandlerQuery(handler_id_in=[handler_id])))[
        0
    ].run_id
    assert run_id is not None

    persisted = sqlite_store.create_state_store(run_id)
    record = await cast(Any, persisted)._storage.load()
    assert record is not None
    blob = json.loads(record.data)

    def _data(payload: dict) -> dict:
        return payload["_data"]

    root_data = _data(blob)
    child_states = blob.get(CHILD_STATES_KEY)
    assert child_states is not None
    mid_data = _data(child_states["mid"])
    grand_data = _data(child_states["mid/grand"])

    # Each namespace holds only its own marker.
    assert root_data["top_marker"] == '"from-top"'
    assert mid_data["mid_marker"] == '"from-mid"'
    assert grand_data["grand_marker"] == '"from-grand"'
    # No cross-namespace leakage.
    assert "grand_marker" not in root_data and "grand_marker" not in mid_data
    assert "mid_marker" not in root_data and "mid_marker" not in grand_data
    assert "top_marker" not in mid_data and "top_marker" not in grand_data
    # Flat read of the row never surfaces a nested namespace key.
    assert await persisted.get("grand_marker", sentinel) is sentinel

    # Restart and finish.
    server2 = _make_grandchild_server(sqlite_store)
    async with server2.contextmanager():
        await server2._service.send_event(handler_id, HumanGo(answer="done"))
        handler = await _wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "from-top:done"
    assert CHILD_RUN_COUNTS["grand"] == 1
