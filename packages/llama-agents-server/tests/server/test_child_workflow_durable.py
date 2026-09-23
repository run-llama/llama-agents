# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Child workflows on the durable server runtime.

Covers per-namespace state isolation, resume with a suspended child waiter
across a full server restart, and origin-namespace threading over the event
stream.
"""

from __future__ import annotations

import pytest
from llama_agents.server import (
    HandlerQuery,
    MemoryWorkflowStore,
    SqliteWorkflowStore,
    WorkflowServer,
)
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from test_durable_runtime import (  # type: ignore[import]
    wait_handler_idle,
    wait_handler_status,
)
from workflows import Context, Workflow
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store_integration import state_store_handoff
from workflows.decorators import step
from workflows.events import (
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    answer: str = ""


class ChildAsk(InputRequiredEvent):
    prefix: str = "child?"


class ChildAnswer(HumanResponseEvent):
    response: str = ""


class DurableChild(Workflow):
    @step
    async def ask(self, ctx: Context, ev: ChildStart) -> ChildAsk:
        await ctx.store.set("child_key", "child-value")
        return ChildAsk()

    @step
    async def answer(self, ctx: Context, ev: ChildAnswer) -> ChildStop:
        await ctx.store.set("answered", ev.response)
        return ChildStop(answer=ev.response)


class DurableParent(Workflow):
    child: DurableChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        await ctx.store.set("parent_key", "parent-value")
        return ChildStart()

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> StopEvent:
        return StopEvent(result=ev.answer)


def _make_parent_server(store: SqliteWorkflowStore) -> WorkflowServer:
    server = WorkflowServer(workflow_store=store, idle_timeout=0.01)
    server.add_workflow(
        "parent",
        DurableParent(child=DurableChild()),
        additional_events=[HumanResponseEvent],
    )
    return server


@pytest.mark.asyncio
async def test_child_state_isolated_per_namespace_row(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """Parent and child each persist to their own namespaced state row."""
    handler_id = "child-iso-1"
    server = _make_parent_server(sqlite_store)

    async with server.contextmanager():
        wf = server._service._runtime.get_workflow("parent")
        assert wf is not None
        handler_data = await server._service.start_workflow(wf, handler_id)
        run_id = handler_data.run_id
        assert run_id is not None

        # The run goes idle once the child is waiting for the human response.
        await wait_handler_idle(sqlite_store, handler_id)

        root_store = sqlite_store.create_state_store(run_id)
        child_store = sqlite_store.create_state_store(run_id, namespace=("child#0",))

        # Each namespace sees only its own writes.
        assert await root_store.get("parent_key") == "parent-value"
        assert await root_store.get("child_key", None) is None
        assert await child_store.get("child_key") == "child-value"
        assert await child_store.get("parent_key", None) is None


@pytest.mark.asyncio
async def test_resume_with_suspended_child_waiter_across_restart(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """A child suspended on a human-input waiter resumes after a full restart.

    The child broker (with its pending waiter) round-trips through the tick
    journal; answering the child's concrete invocation path completes the run.
    """
    handler_id = "child-resume-1"

    # Server 1: start, let the child reach its wait point, then stop.
    server1 = _make_parent_server(sqlite_store)
    async with server1.contextmanager():
        wf1 = server1._service._runtime.get_workflow("parent")
        assert wf1 is not None
        await server1._service.start_workflow(wf1, handler_id)
        await wait_handler_idle(sqlite_store, handler_id)

    run_id = (await sqlite_store.query(HandlerQuery(handler_id_in=[handler_id])))[
        0
    ].run_id
    assert run_id is not None

    # Server 2: restart, answer the child's concrete path, expect completion.
    server2 = _make_parent_server(sqlite_store)
    async with server2.contextmanager():
        await server2._service.send_event(
            handler_id,
            ChildAnswer(response="resolved"),
            step="child#0/answer",
        )
        handler = await wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "resolved"

    # The child's own row retained its writes across the restart.
    child_store = sqlite_store.create_state_store(run_id, namespace=("child#0",))
    assert await child_store.get("answered") == "resolved"


@pytest.mark.asyncio
async def test_child_events_tagged_with_origin_namespace(
    memory_store: MemoryWorkflowStore,
) -> None:
    """Events published from a child are persisted with their origin namespace.

    The stored envelope carries the child's invocation path, which is what the
    server's ``include_children`` filter keys off of.
    """
    handler_id = "child-stream-1"
    server = WorkflowServer(workflow_store=memory_store, idle_timeout=0.01)
    server.add_workflow(
        "parent",
        DurableParent(child=DurableChild()),
        additional_events=[HumanResponseEvent],
    )

    async with server.contextmanager():
        wf = server._service._runtime.get_workflow("parent")
        assert wf is not None
        handler_data = await server._service.start_workflow(wf, handler_id)
        run_id = handler_data.run_id
        assert run_id is not None

        await wait_handler_idle(memory_store, handler_id)

        # The InputRequiredEvent published from the child is persisted with a
        # non-empty origin namespace; a root StartEvent stays empty.
        async def child_event_recorded() -> None:
            events = await memory_store.query_events(run_id)
            child_input = [
                e
                for e in events
                if "InputRequiredEvent" in ((e.event.types or []) + [e.event.type])
            ]
            assert child_input, "expected the child's InputRequiredEvent to persist"
            assert child_input[0].event.origin_namespace == ("child#0",)

        await wait_for_passing(child_event_recorded, max_duration=5.0, interval=0.05)


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    val: str = ""


class DurableGrandchild(Workflow):
    @step
    async def run_grand(self, ctx: Context, ev: GrandStart) -> GrandStop:
        await ctx.store.set("grand_key", "grand-value")
        return GrandStop(val="ok")


class DurableMid(Workflow):
    grand: DurableGrandchild

    @step
    async def run_mid(self, ctx: Context, ev: ChildStart) -> GrandStart:
        await ctx.store.set("mid_key", "mid-value")
        return GrandStart()

    @step
    async def finish_mid(self, ctx: Context, ev: GrandStop) -> ChildStop:
        return ChildStop(answer=await ctx.store.get("mid_key"))


class DurableGrandparent(Workflow):
    child: DurableMid

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        await ctx.store.set("root_key", "root-value")
        return ChildStart()

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> StopEvent:
        return StopEvent(result=f"{await ctx.store.get('root_key')}|{ev.answer}")


@pytest.mark.asyncio
async def test_grandchild_state_isolated_per_namespace_row(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """Three nesting levels each persist to their own namespaced durable row."""
    handler_id = "grand-iso-1"
    server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    server.add_workflow(
        "grandparent",
        DurableGrandparent(child=DurableMid(grand=DurableGrandchild())),
    )

    async with server.contextmanager():
        wf = server._service._runtime.get_workflow("grandparent")
        assert wf is not None
        handler_data = await server._service.start_workflow(wf, handler_id)
        run_id = handler_data.run_id
        assert run_id is not None

        handler = await wait_handler_status(sqlite_store, handler_id, "completed")
        assert handler.result is not None
        assert handler.result.result == "root-value|mid-value"

    root_store = sqlite_store.create_state_store(run_id)
    mid_store = sqlite_store.create_state_store(run_id, namespace=("child#0",))
    grand_store = sqlite_store.create_state_store(
        run_id, namespace=("child#0", "grand#0")
    )
    assert await root_store.get("root_key") == "root-value"
    assert await root_store.get("mid_key", None) is None
    assert await mid_store.get("mid_key") == "mid-value"
    assert await mid_store.get("grand_key", None) is None
    assert await grand_store.get("grand_key") == "grand-value"


class CarryChild(Workflow):
    @step
    async def read(self, ctx: Context, ev: ChildStart) -> ChildStop:
        # First store access of the whole run happens here, inside the child.
        return ChildStop(answer=await ctx.store.get("carried", "MISSING"))


class CarryParent(Workflow):
    child: CarryChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        # Deliberately does not touch ctx.store: on a durable-handle fork the
        # root store must still fan out before the child reads.
        return ChildStart()

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> StopEvent:
        return StopEvent(result=ev.answer)


@pytest.mark.asyncio
async def test_fork_restores_child_state_when_first_access_is_in_child(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    """A fork seeded from a durable handle restores child state even when the
    parent never touches the root store.

    The durable handle seeds only the root; the backend's copy_from_handle
    fans out to every namespace row. If the first ``ctx.store`` access of the
    run happens inside a child (parent start touches nothing), the fan-out must
    still have run — otherwise the child reads an empty row.
    """
    # Source run: only the child namespace holds state.
    src_child = sqlite_store.create_state_store("run-src", namespace=("child#0",))
    await src_child.set("carried", "restored-value")

    server = WorkflowServer(workflow_store=sqlite_store, idle_timeout=0.01)
    wf = CarryParent(child=CarryChild())
    server.add_workflow("carry", wf)

    async with server.contextmanager():
        # Build the fork context from the source run's root durable handle,
        # exactly as the service does when restarting a completed handler.
        old_root = sqlite_store.create_state_store("run-src")
        state_dict = await state_store_handoff(old_root, JsonSerializer())
        context = Context.from_dict(
            workflow=wf,
            data={"version": 1, "state": state_dict},
            serializer=JsonSerializer(),
        )

        handler_data = await server._service.start_workflow(
            wf, "carry-fork-1", context=context
        )
        run_id = handler_data.run_id
        assert run_id is not None

        handler = await wait_handler_status(sqlite_store, "carry-fork-1", "completed")
        assert handler.result is not None
        assert handler.result.result == "restored-value"

    # The forked run's own child row carries the restored value.
    forked_child = sqlite_store.create_state_store(run_id, namespace=("child#0",))
    assert await forked_child.get("carried") == "restored-value"


def test_event_envelope_origin_namespace_round_trips() -> None:
    """Non-empty origin namespaces survive the wire; empty ones are omitted."""
    from llama_agents.client.protocol.serializable_events import (
        EventEnvelopeWithMetadata,
    )
    from workflows.events import (
        _set_event_origin_namespace,
        get_event_origin_namespace,
    )

    child_event = ChildAsk()
    _set_event_origin_namespace(child_event, ("child#0",))
    envelope = EventEnvelopeWithMetadata.from_event(child_event)
    assert envelope.origin_namespace == ("child#0",)
    dumped = envelope.model_dump()
    assert dumped["origin_namespace"] == ("child#0",)
    reloaded = EventEnvelopeWithMetadata.model_validate_json(envelope.model_dump_json())
    assert get_event_origin_namespace(reloaded.load_event()) == ("child#0",)

    root_event = ChildAsk()
    root_envelope = EventEnvelopeWithMetadata.from_event(root_event)
    assert root_envelope.origin_namespace == ()
    # Root events keep the byte-identical pre-child wire shape.
    assert "origin_namespace" not in root_envelope.model_dump()
