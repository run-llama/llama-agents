# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Child-workflow composition through the server's durable persistence path.

These tests drive a parent-with-child workflow through the server's
sqlite-backed tick journal + state store, across a full server restart, to
confirm:

- the namespaced ``StepId`` journal round-trips (child steps deserialize and
  are skipped on resume rather than re-run), and
- a child step's ``ctx.store`` writes are durable: each namespace persists to
  its own ``workflow_state`` row, survives a full server restart, and stays
  isolated from the parent's root row (and, for grandchildren, from intermediate
  namespaces).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from llama_agents.server import (
    HandlerQuery,
    MemoryWorkflowStore,
    PersistentHandler,
    SqliteWorkflowStore,
    WorkflowServer,
)
from llama_agents.server._runtime.server_runtime import (
    ServerRuntimeDecorator,
    _ServerInternalRunAdapter,
)
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows import Context, Workflow, step
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import (
    DictState,
    InMemoryStateStore,
    build_namespaced_state,
)
from workflows.events import Event, StartEvent, StopEvent, get_event_origin_namespace
from workflows.runtime.types.plugin import InternalRunAdapter, WaitResultTimeout
from workflows.runtime.types.ticks import WorkflowTick

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


class StreamChildStart(StartEvent):
    pass


class StreamChildStop(StopEvent):
    pass


class StreamChildPing(Event):
    msg: str = ""


class StreamParentPing(Event):
    msg: str = ""


class StreamingChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: StreamChildStart) -> StreamChildStop:
        ctx.write_event_to_stream(StreamChildPing(msg="child"))
        return StreamChildStop()


class StreamingParent(Workflow):
    child: StreamingChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> StreamChildStart:
        ctx.write_event_to_stream(StreamParentPing(msg="parent"))
        return StreamChildStart()

    @step
    async def finish(self, ev: StreamChildStop) -> StopEvent:
        return StopEvent(result="done")


class _FakeInternalAdapter(InternalRunAdapter):
    @property
    def run_id(self) -> str:
        return "seed-run"

    async def write_to_event_stream(self, event: Event) -> None:
        pass

    async def get_now(self) -> float:
        return 0.0

    async def send_event(self, tick: WorkflowTick) -> None:
        pass

    async def wait_receive(
        self, timeout_seconds: float | None = None
    ) -> WaitResultTimeout:
        return WaitResultTimeout()


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


@pytest.mark.asyncio
async def test_persisted_child_event_origin_survives_server_store_boundary(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """Child-origin metadata survives SQLite event persistence and stream filtering.

    The in-process handler tags child stream events before the server persists
    them. The persisted envelope must keep that tag so HTTP/default consumers
    can hide child events, while opt-in consumers can still see and attribute
    them.
    """
    handler_id = "child-origin-stream-1"
    server = WorkflowServer(workflow_store=sqlite_store)
    server.add_workflow("stream", StreamingParent(child=StreamingChild()))

    async with server.contextmanager():
        wf = server._service._runtime.get_workflow("stream")
        assert wf is not None
        await server._service.start_workflow(wf, handler_id)
        handler = await _wait_handler_status(sqlite_store, handler_id, "completed")

        assert handler.run_id is not None
        stored = await sqlite_store.query_events(handler.run_id)
        child_envelope = next(
            event.event
            for event in stored
            if event.event.type == StreamChildPing.__name__
        )
        assert child_envelope.origin_namespace == ("child",)
        loaded_child = child_envelope.load_event([StreamChildPing])
        assert get_event_origin_namespace(loaded_child) == ("child",)

        default_gen = await server._api._resolve_event_stream(
            handler_id,
            after_sequence=-1,
            include_internal=False,
            include_children=False,
            include_qualified_name=True,
        )
        assert default_gen is not None
        default_events = [event async for _, event in default_gen]
        assert [event.type for event in default_events] == [
            StreamParentPing.__name__,
            StopEvent.__name__,
        ]

        child_gen = await server._api._resolve_event_stream(
            handler_id,
            after_sequence=-1,
            include_internal=False,
            include_children=True,
            include_qualified_name=True,
        )
        assert child_gen is not None
        child_events = [event async for _, event in child_gen]
        assert [event.type for event in child_events] == [
            StreamParentPing.__name__,
            StreamChildPing.__name__,
            StopEvent.__name__,
        ]
        assert child_events[1].origin_namespace == ("child",)


def test_in_memory_child_seeds_are_staged_even_if_child_store_is_never_requested() -> (
    None
):
    serializer = JsonSerializer()
    workflow = StreamingParent(child=StreamingChild())

    def make_store(namespace: tuple[str, ...]) -> InMemoryStateStore[DictState]:
        label = "/".join(namespace) if namespace else "root"
        return InMemoryStateStore(DictState(_data={"marker": label}))

    serialized = build_namespaced_state(
        workflow, make_store, serializer
    ).serialize_tree(serializer)
    store = MemoryWorkflowStore()
    runtime = SimpleNamespace(
        _store=store,
        _initial_state={"seed-run": (serialized, serializer)},
    )
    adapter = _ServerInternalRunAdapter(
        _FakeInternalAdapter(),
        cast(ServerRuntimeDecorator, runtime),
    )

    adapter.get_state_store(())

    child_store = store.state_stores[("seed-run", ("child",))]
    assert child_store.to_dict(serializer)["state_data"]["_data"]["marker"] == '"child"'


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
    is namespace-aware: each namespace owns its own ``workflow_state`` row. The
    child's write lands in the ``child`` namespace row, isolated from the
    parent's root row, and survives a full server restart.

    Durability is verified by reconnecting each namespace's row directly, and by
    restarting the server and confirming the parent finishes with the child's
    threaded-through output.
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

    # Each namespace persists to its own row; reconnect each and read it back.
    root = sqlite_store.create_state_store(run_id)
    child = sqlite_store.create_state_store(run_id, namespace=("child",))

    # The parent's root write is durable in the root row, the child's in the
    # child row, with no cross-namespace leakage.
    assert await root.get("from_child") == "HELLO"
    assert await root.get("child_marker", sentinel) is sentinel
    assert await child.get("child_marker") == "from-child"
    assert await child.get("from_child", sentinel) is sentinel

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
    idles at a HITL point after the grandchild completed. Each namespace owns
    its own row, keyed by the "/"-joined path (``mid``, ``mid/grand``); markers
    never cross namespaces. A full server restart resumes and completes the run.
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

    # Reconnect each namespace's own row.
    root = sqlite_store.create_state_store(run_id)
    mid = sqlite_store.create_state_store(run_id, namespace=("mid",))
    grand = sqlite_store.create_state_store(run_id, namespace=("mid", "grand"))

    # Each namespace holds only its own marker, with no cross-namespace leakage.
    assert await root.get("top_marker") == "from-top"
    assert await mid.get("mid_marker") == "from-mid"
    assert await grand.get("grand_marker") == "from-grand"
    assert await root.get("grand_marker", sentinel) is sentinel
    assert await root.get("mid_marker", sentinel) is sentinel
    assert await mid.get("grand_marker", sentinel) is sentinel
    assert await mid.get("top_marker", sentinel) is sentinel
    assert await grand.get("mid_marker", sentinel) is sentinel
    assert await grand.get("top_marker", sentinel) is sentinel

    # Restart and finish.
    server2 = _make_grandchild_server(sqlite_store)
    async with server2.contextmanager():
        await server2._service.send_event(handler_id, HumanGo(answer="done"))
        handler = await _wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "from-top:done"
    assert CHILD_RUN_COUNTS["grand"] == 1
