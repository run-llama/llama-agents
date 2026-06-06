# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import gc
import json
import weakref
from typing import cast

import pytest
from pydantic import BaseModel
from workflows.context import Context
from workflows.context.context import (
    _warn_cancel_before_start,
    _warn_cancel_in_step,
    _warn_get_result,
    _warn_is_running_in_step,
)
from workflows.context.external_context import ExternalContext
from workflows.context.internal_context import InternalContext
from workflows.context.pre_context import PreContext
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import (
    DictState,
    InMemoryStateStore,
    deserialize_state_from_dict,
)
from workflows.decorators import step
from workflows.errors import ContextStateError, WorkflowRuntimeError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)
from workflows.plugins.basic import (
    AsyncioAdapterQueues,
    BasicRuntime,
    setting_run_id,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.ticks import TickAddEvent
from workflows.testing import WorkflowTestRunner
from workflows.workflow import Workflow

from ..conftest import (  # type: ignore[import]
    AnotherTestEvent,
    LastEvent,
    OneTestEvent,
)


@pytest.fixture()
def internal_ctx(workflow: Workflow) -> Context:
    """Create a context directly in internal face for testing store operations."""
    # Set up a runtime with state store for this workflow
    runtime = BasicRuntime()
    run_id = "test-run"
    init_state = BrokerState.from_workflow(workflow)
    # Create queues with state store so get_internal_adapter() returns adapter with store
    queues = AsyncioAdapterQueues(
        run_id=run_id,
        init_state=init_state,
        state_store=InMemoryStateStore(DictState()),
    )
    runtime._queues[run_id] = queues
    workflow._runtime = runtime
    with setting_run_id(run_id):
        return Context._create_internal(workflow=workflow)


@pytest.mark.asyncio
async def test_collect_events() -> None:
    ev1 = OneTestEvent()
    ev2 = AnotherTestEvent()

    class TestWorkflow(Workflow):
        @step
        async def step1(self, _: StartEvent) -> OneTestEvent:
            return ev1

        @step
        async def step2(self, _: StartEvent) -> AnotherTestEvent:
            return ev2

        @step
        async def step3(
            self, ctx: Context, ev: OneTestEvent | AnotherTestEvent
        ) -> StopEvent | None:
            events = ctx.collect_events(ev, [OneTestEvent, AnotherTestEvent])
            if events is None:
                return None
            return StopEvent(result=events)

    r = await WorkflowTestRunner(TestWorkflow()).run()
    assert r.result == [ev1, ev2]


@pytest.mark.asyncio
async def test_collect_events_empty_expected_list() -> None:
    """
    Test that collect_events returns an empty list (not None) when the
    expected list is empty. This edge case should immediately return []
    since there are no events to collect.
    """

    class TestWorkflow(Workflow):
        @step
        async def start_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
            # Pass an empty list of expected events
            events = ctx.collect_events(ev, [])
            # Should return an empty list, not None
            return StopEvent(result=events)

    r = await WorkflowTestRunner(TestWorkflow()).run()
    assert r.result == []


@pytest.mark.asyncio
async def test_collect_events_with_extra_event_type() -> None:
    """
    Test that collect_events properly handles when an event of a different type
    arrives first, before the expected events.

    This validates that when collect_events is called with an event that's NOT
    in the expected types list, it returns None and waits for matching events.
    """

    class TestWorkflow(Workflow):
        @step
        async def start_step(
            self, ctx: Context, ev: StartEvent
        ) -> OneTestEvent | AnotherTestEvent | LastEvent:
            await ctx.store.set("num_to_collect", 2)
            await ctx.store.set("calls", 0)
            # Send a LastEvent first (not in the expected collection types)
            ctx.send_event(LastEvent())
            # Then send the events we want to collect
            ctx.send_event(OneTestEvent(test_param="first"))
            ctx.send_event(AnotherTestEvent(another_test_param="second"))
            return None  # type: ignore

        @step
        async def collector(
            self, ctx: Context, ev: OneTestEvent | AnotherTestEvent | LastEvent
        ) -> StopEvent | None:
            # Track how many times this step is called
            calls = await ctx.store.get("calls")
            await ctx.store.set("calls", calls + 1)

            # Try to collect OneTestEvent and AnotherTestEvent
            # LastEvent is NOT in this list
            events = ctx.collect_events(ev, [OneTestEvent, AnotherTestEvent])
            if events is None:
                # This happens when we receive LastEvent or haven't received all events yet
                return None

            # Verify we got the right events
            assert len(events) == 2
            assert isinstance(events[0], OneTestEvent)
            assert isinstance(events[1], AnotherTestEvent)
            assert events[0].test_param == "first"
            assert events[1].another_test_param == "second"

            return StopEvent(result="collected")

    r = await WorkflowTestRunner(TestWorkflow()).run()
    assert r.result == "collected"

    # Verify the collector was called multiple times (once for each event)
    ctx = r.ctx
    assert ctx is not None
    ctx_dict = ctx.to_dict()
    # State is serialized as JSON strings under state_data._data
    calls = json.loads(ctx_dict["state"]["state_data"]["_data"]["calls"])
    # Should be called at least 3 times: once for LastEvent (returns None),
    # once for OneTestEvent (returns None), once for AnotherTestEvent (returns result)
    assert calls >= 3


@pytest.mark.asyncio
async def test_get_default(internal_ctx: Context) -> None:
    assert await internal_ctx.store.get("test_key", default=42) == 42


@pytest.mark.asyncio
async def test_get(internal_ctx: Context) -> None:
    await internal_ctx.store.set("foo", 42)
    assert await internal_ctx.store.get("foo") == 42


@pytest.mark.asyncio
async def test_get_not_found(internal_ctx: Context) -> None:
    with pytest.raises(ValueError):
        await internal_ctx.store.get("foo")


@pytest.mark.asyncio
async def test_send_event_step_is_none() -> None:
    """Test that external events create TickAddEvent with step_name=None.

    Uses a workflow that waits for the external event so we can verify
    the tick is logged before the workflow completes.
    """
    ev = Event(foo="bar")

    class WaitingWorkflow(Workflow):
        @step
        async def wait_for_external(self, ctx: Context, ev: StartEvent) -> StopEvent:
            # Wait for an external Event to arrive
            result = await ctx.wait_for_event(Event, requirements={"foo": "bar"})
            return StopEvent(result=result.foo)

    wf = WaitingWorkflow()
    handler = wf.run()
    try:
        # Send the external event
        handler.ctx.send_event(ev)
        external_face = handler.ctx._face
        assert isinstance(external_face, ExternalContext)

        # Wait for event to appear in tick log (up to 1 second)
        expected_tick = TickAddEvent(event=ev, step_name=None)
        for _ in range(100):
            if expected_tick in external_face._tick_log:
                break
            await asyncio.sleep(0.01)

        assert expected_tick in external_face._tick_log

        # Let workflow complete
        result = await handler
        assert result == "bar"
    finally:
        external_face = handler.ctx._face
        assert isinstance(external_face, ExternalContext)
        await external_face.shutdown()


@pytest.mark.asyncio
async def test_send_event_to_non_existent_step(ctx: Context) -> None:
    with pytest.raises(
        WorkflowRuntimeError, match="Step does_not_exist does not exist"
    ):
        ctx.send_event(Event(), "does_not_exist")


@pytest.mark.asyncio
async def test_send_event_to_wrong_step(ctx: Context) -> None:
    with pytest.raises(
        WorkflowRuntimeError,
        match="Step middle_step does not accept event of type <class 'workflows.events.Event'>",
    ):
        ctx.send_event(Event(), "middle_step")


@pytest.mark.asyncio
async def test_empty_inprogress_when_workflow_done(workflow: Workflow) -> None:
    result = await WorkflowTestRunner(workflow).run()
    ctx = result.ctx

    # there shouldn't be any in progress events
    assert ctx is not None
    assert isinstance(ctx._face, ExternalContext)
    # After workflow completion, in_progress should be empty for all steps
    state = ctx._face._state
    for step_name, worker_state in state.workers.items():
        assert len(worker_state.in_progress) == 0, (
            f"Step {step_name} has {len(worker_state.in_progress)} in-progress events"
        )


@pytest.mark.asyncio
async def test_wait_for_event_in_workflow() -> None:
    class TestWorkflow(Workflow):
        @step
        async def step1(self, ctx: Context, ev: StartEvent) -> StopEvent:
            result = await ctx.wait_for_event(
                Event,
                waiter_event=Event(msg="foo"),
                waiter_id="test_id",
            )
            return StopEvent(result=result.msg)

    workflow = TestWorkflow()
    handler = workflow.run()
    assert handler.ctx
    async for ev in handler.stream_events():
        if isinstance(ev, Event) and ev.msg == "foo":
            handler.ctx.send_event(Event(msg="bar"))
            break

    result = await handler
    assert result == "bar"


class CustomState(BaseModel):
    pass


@pytest.mark.asyncio
async def test_wait_for_event_in_workflow_serialization() -> None:
    """Ensure hitl works with serialization and custom state."""

    class TestWorkflow(Workflow):
        @step
        async def step1(self, ctx: Context[CustomState], ev: StartEvent) -> StopEvent:
            result = await ctx.wait_for_event(
                Event,
                waiter_event=Event(msg="foo"),
                waiter_id="test_id",
            )
            return StopEvent(result=result.msg)

    workflow = TestWorkflow()
    handler = workflow.run()
    ctx_dict = None

    assert handler.ctx
    async for ev in handler.stream_events():
        if isinstance(ev, Event) and ev.msg == "foo":
            ctx_dict = handler.ctx.to_dict()
            # Check that at least one worker has waiters
            assert ctx_dict["version"] == 1
            total_waiters = sum(
                len(worker_data["collected_waiters"])
                for worker_data in ctx_dict["workers"].values()
            )
            assert total_waiters == 1
            await handler.cancel_run()
            break

    # Roundtrip the context
    assert ctx_dict is not None
    # verify creating a new context has the correct state
    new_ctx = Context.from_dict(workflow, ctx_dict)
    new_handler = workflow.run(ctx=new_ctx)
    assert isinstance(new_handler.ctx._face, ExternalContext)
    # Check that the waiters are properly restored
    state = new_handler.ctx._face._state
    total_waiters = sum(
        len(worker.collected_waiters) for worker in state.workers.values()
    )
    assert total_waiters == 1

    # Continue the workflow
    assert new_handler.ctx
    new_handler.ctx.send_event(Event(msg="bar"))
    result = await new_handler
    assert result == "bar"
    assert isinstance(new_handler.ctx._face, ExternalContext)
    # After workflow completion, there should be no more waiters
    state = new_handler.ctx._face._state
    total_waiters = sum(
        len(worker.collected_waiters) for worker in state.workers.values()
    )
    assert total_waiters == 0


class ApprovalRequiredEvent(InputRequiredEvent):
    prefix: str


class ApprovalResponseEvent(HumanResponseEvent):
    response: str
    waiter_id: str | None = None


@pytest.mark.asyncio
async def test_waiter_requirements_rehydrate_from_legacy_snapshot() -> None:
    class ApprovalWorkflow(Workflow):
        @step
        async def review(self, ctx: Context, ev: StartEvent) -> StopEvent:
            result = await ctx.wait_for_event(
                ApprovalResponseEvent,
                waiter_event=ApprovalRequiredEvent(prefix="approval"),
                requirements={"waiter_id": "waiter_one"},
            )
            return StopEvent(result=result.response)

    workflow = ApprovalWorkflow()
    handler = workflow.run()
    ctx_dict = None

    async for ev in handler.stream_events():
        if isinstance(ev, InputRequiredEvent):
            ctx_dict = handler.ctx.to_dict()
            await handler.cancel_run()
            break

    assert ctx_dict is not None
    waiters = [
        waiter
        for worker in ctx_dict["workers"].values()
        for waiter in worker["collected_waiters"]
    ]
    assert len(waiters) == 1
    assert waiters[0]["has_requirements"] is True

    # Simulate a legacy context snapshot written before waiter requirement
    # metadata was persisted. Since that format does not provide a reliable
    # per-waiter requirement signal, restore conservatively replays unresolved
    # legacy waiters to reconstruct any requirements they owned.
    waiters[0].pop("has_requirements")

    legacy_ctx = Context.from_dict(workflow, ctx_dict)
    assert isinstance(legacy_ctx._face, PreContext)
    legacy_state = BrokerState.from_serialized(
        legacy_ctx._face.init_snapshot, workflow, JsonSerializer()
    )
    assert len(legacy_state.rehydrate_with_ticks()) == 1

    new_handler = workflow.run(ctx=legacy_ctx)
    new_handler.ctx.send_event(ApprovalResponseEvent(response="unbound"))

    task = asyncio.ensure_future(new_handler)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(task), timeout=0.2)

    new_handler.ctx.send_event(
        ApprovalResponseEvent(response="bound", waiter_id="waiter_one")
    )
    assert await task == "bound"


@pytest.mark.asyncio
async def test_waiter_without_requirements_does_not_rehydrate() -> None:
    class ApprovalWorkflow(Workflow):
        @step
        async def review(self, ctx: Context, ev: StartEvent) -> StopEvent:
            result = await ctx.wait_for_event(
                ApprovalResponseEvent,
                waiter_event=ApprovalRequiredEvent(prefix="approval"),
            )
            return StopEvent(result=result.response)

    workflow = ApprovalWorkflow()
    handler = workflow.run()
    ctx_dict = None

    async for ev in handler.stream_events():
        if isinstance(ev, InputRequiredEvent):
            ctx_dict = handler.ctx.to_dict()
            await handler.cancel_run()
            break

    assert ctx_dict is not None
    waiters = [
        waiter
        for worker in ctx_dict["workers"].values()
        for waiter in worker["collected_waiters"]
    ]
    assert len(waiters) == 1
    assert waiters[0]["has_requirements"] is False

    new_ctx = Context.from_dict(workflow, ctx_dict)
    assert isinstance(new_ctx._face, PreContext)
    current_state = BrokerState.from_serialized(
        new_ctx._face.init_snapshot, workflow, JsonSerializer()
    )
    assert current_state.rehydrate_with_ticks() == []


class Waiter1(Event):
    msg: str


class Waiter2(Event):
    msg: str


class ResultEvent(Event):
    result: str


class WaitingWorkflow(Workflow):
    @step
    async def spawn_waiters(self, ctx: Context, ev: StartEvent) -> Waiter1 | Waiter2:
        ctx.send_event(Waiter1(msg="foo"))
        ctx.send_event(Waiter2(msg="bar"))
        return None  # type: ignore

    @step
    async def waiter_one(self, ctx: Context, ev: Waiter1) -> ResultEvent:
        ctx.write_event_to_stream(InputRequiredEvent(prefix="waiter_one"))  # type: ignore

        new_ev: HumanResponseEvent = await ctx.wait_for_event(
            HumanResponseEvent,
            requirements={"waiter_id": "waiter_one"},
        )
        return ResultEvent(result=new_ev.response)

    @step
    async def waiter_two(self, ctx: Context, ev: Waiter2) -> ResultEvent:
        ctx.write_event_to_stream(InputRequiredEvent(prefix="waiter_two"))  # type: ignore

        new_ev: HumanResponseEvent = await ctx.wait_for_event(
            HumanResponseEvent,
            requirements={"waiter_id": "waiter_two"},
        )
        return ResultEvent(result=new_ev.response)

    @step
    async def collect_waiters(self, ctx: Context, ev: ResultEvent) -> StopEvent:
        events: list[ResultEvent] | None = ctx.collect_events(  # type: ignore
            ev, [ResultEvent, ResultEvent]
        )
        if events is None:
            return None  # type: ignore

        return StopEvent(result=[e.result for e in events])


@pytest.mark.asyncio
async def test_wait_for_multiple_events_in_workflow() -> None:
    workflow = WaitingWorkflow()
    handler = workflow.run()
    assert handler.ctx

    async for ev in handler.stream_events():
        if isinstance(ev, InputRequiredEvent) and ev.prefix == "waiter_one":
            handler.ctx.send_event(
                HumanResponseEvent(response="foo", waiter_id="waiter_one")  # type: ignore
            )
        elif isinstance(ev, InputRequiredEvent) and ev.prefix == "waiter_two":
            handler.ctx.send_event(
                HumanResponseEvent(response="bar", waiter_id="waiter_two")  # type: ignore
            )

    result = await handler
    # Order is non-deterministic since waiters run concurrently
    assert sorted(result) == ["bar", "foo"]
    assert not handler.ctx.is_running

    # serialize and resume
    ctx_dict = handler.ctx.to_dict()
    ctx = Context.from_dict(workflow, ctx_dict)
    handler = workflow.run(ctx=ctx)
    assert handler.ctx

    async for ev in handler.stream_events():
        if isinstance(ev, InputRequiredEvent) and ev.prefix == "waiter_one":
            handler.ctx.send_event(
                HumanResponseEvent(response="fizz", waiter_id="waiter_one")  # type: ignore
            )
        elif isinstance(ev, InputRequiredEvent) and ev.prefix == "waiter_two":
            handler.ctx.send_event(
                HumanResponseEvent(response="buzz", waiter_id="waiter_two")  # type: ignore
            )

    result = await handler
    # Order is non-deterministic since waiters run concurrently
    assert sorted(result) == ["buzz", "fizz"]


@pytest.mark.asyncio
async def test_clear(internal_ctx: Context) -> None:
    await internal_ctx.store.set("test_key", 42)
    await internal_ctx.store.clear()
    res = await internal_ctx.store.get("test_key", default=None)
    assert res is None


@pytest.mark.asyncio
async def test_running_steps_before_run_raises(workflow: Workflow) -> None:
    """Calling running_steps() before workflow.run() should raise ContextStateError."""
    ctx = Context(workflow)
    with pytest.raises(ContextStateError, match="requires a running workflow"):
        await ctx.running_steps()


@pytest.mark.asyncio
async def test_store_access_outside_step_works() -> None:
    """Accessing ctx.store from handler code (outside step) should work."""

    class SimpleWorkflow(Workflow):
        @step
        async def only(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="done")

    wf = SimpleWorkflow()
    handler = wf.run()

    # Access store from external context (handler code) should work
    store = handler.ctx.store
    assert isinstance(store, InMemoryStateStore)

    # Verify reads and writes work
    await store.set("key", "value")
    assert await store.get("key") == "value"

    await handler


@pytest.mark.asyncio
async def test_store_access_before_run_works() -> None:
    """Accessing ctx.store before workflow.run() should return a staging store."""

    class SimpleWorkflow(Workflow):
        @step
        async def only(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="done")

    wf = SimpleWorkflow()
    ctx = Context(wf)

    store = ctx.store
    assert isinstance(store, InMemoryStateStore)
    await store.set("key", "value")
    assert await store.get("key") == "value"


@pytest.mark.asyncio
async def test_store_seed_before_run_visible_in_step() -> None:
    """State seeded via ctx.store before run should be visible inside steps."""

    class SeededWorkflow(Workflow):
        @step
        async def read_seed(self, ctx: Context, ev: StartEvent) -> StopEvent:
            val = await ctx.store.get("seeded_key")
            return StopEvent(result=val)

    wf = SeededWorkflow()
    ctx = Context(wf)
    await ctx.store.set("seeded_key", "hello")

    result = await wf.run(ctx=ctx)
    assert result == "hello"


@pytest.mark.asyncio
async def test_store_seed_on_deserialized_context() -> None:
    """Seeding state on a deserialized context should merge with existing state."""

    class StatefulWorkflow(Workflow):
        @step
        async def first_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
            # Just pass through; state is set externally
            return StopEvent(result="done")

    class ReadWorkflow(Workflow):
        @step
        async def check(self, ctx: Context, ev: StartEvent) -> StopEvent:
            old = await ctx.store.get("existing")
            new = await ctx.store.get("added")
            return StopEvent(result=f"{old}-{new}")

    wf = StatefulWorkflow()

    # First run to create some state
    ctx = Context(wf)
    await ctx.store.set("existing", "orig")
    handler = wf.run(ctx=ctx)
    await handler

    # Serialize and restore (use ReadWorkflow for second run)
    ctx_dict = ctx.to_dict()
    read_wf = ReadWorkflow()
    restored_ctx = Context.from_dict(read_wf, ctx_dict)

    # Seed additional state before next run
    await restored_ctx.store.set("added", "new")

    result = await read_wf.run(ctx=restored_ctx)
    assert result == "orig-new"


@pytest.mark.asyncio
async def test_store_continuation_with_pre_run_seeding() -> None:
    """Continuation runs with pre-run seeding should carry state through."""

    class CountWorkflow(Workflow):
        @step
        async def increment(self, ctx: Context, ev: StartEvent) -> StopEvent:
            count = await ctx.store.get("count", default=0)
            count += 1
            await ctx.store.set("count", count)
            return StopEvent(result=count)

    wf = CountWorkflow()
    ctx = Context(wf)

    # Seed initial count
    await ctx.store.set("count", 10)

    # First run: 10 + 1 = 11
    result = await wf.run(ctx=ctx)
    assert result == 11

    # Second run (continuation): 11 + 1 = 12
    result = await wf.run(ctx=ctx)
    assert result == 12


@pytest.mark.asyncio
async def test_to_dict_before_run_raises(workflow: Workflow) -> None:
    """Calling to_dict() before workflow.run() should raise ContextStateError."""
    ctx = Context(workflow)
    with pytest.raises(ContextStateError, match="requires a running workflow"):
        ctx.to_dict()


@pytest.mark.asyncio
async def test_stream_events_before_run_raises(workflow: Workflow) -> None:
    """Calling stream_events() before workflow.run() should raise ContextStateError."""
    ctx = Context(workflow)
    with pytest.raises(ContextStateError, match="requires a running workflow"):
        ctx.stream_events()


# ============================================================================
# Warning Tests
# ============================================================================


@pytest.mark.asyncio
async def test_cancel_before_start_warns(workflow: Workflow) -> None:
    """Calling cancel before run() should emit warning."""
    # Clear the lru_cache to ensure warning fires
    _warn_cancel_before_start.cache_clear()

    ctx = Context(workflow)
    with pytest.warns(UserWarning, match="cancel.*called before workflow started"):
        ctx._workflow_cancel_run()


@pytest.mark.asyncio
async def test_send_event_before_start_raises(workflow: Workflow) -> None:
    """Sending event before run() should raise ContextStateError."""
    ctx = Context(workflow)
    with pytest.raises(
        ContextStateError, match="send_event.*called before workflow started"
    ):
        ctx.send_event(Event())


@pytest.mark.asyncio
async def test_is_running_in_step_warns() -> None:
    """Calling is_running from within a step should emit deprecation warning."""
    # Clear the lru_cache to ensure warning fires
    _warn_is_running_in_step.cache_clear()

    is_running_value = None

    class TestWorkflow(Workflow):
        @step
        async def check_running(self, ctx: Context, ev: StartEvent) -> StopEvent:
            nonlocal is_running_value
            is_running_value = ctx.is_running
            return StopEvent(result="done")

    wf = TestWorkflow()
    with pytest.warns(DeprecationWarning, match="is_running called from within a step"):
        await wf.run()

    # Should still return True despite the warning
    assert is_running_value is True


@pytest.mark.asyncio
async def test_cancel_in_step_warns() -> None:
    """Calling cancel from within a step should emit warning."""
    # Clear the lru_cache to ensure warning fires
    _warn_cancel_in_step.cache_clear()

    class TestWorkflow(Workflow):
        @step
        async def cancel_self(self, ctx: Context, ev: StartEvent) -> StopEvent:
            ctx._workflow_cancel_run()
            return StopEvent(result="done")

    wf = TestWorkflow()
    with pytest.warns(UserWarning, match="cancel.*called from within a step"):
        await wf.run()


@pytest.mark.asyncio
async def test_get_result_before_complete_raises() -> None:
    """Calling get_result() while workflow still running should raise WorkflowRuntimeError."""
    # Clear the lru_cache to ensure deprecation warning fires
    _warn_get_result.cache_clear()

    step_started = asyncio.Event()
    step_continue = asyncio.Event()

    class SlowWorkflow(Workflow):
        @step
        async def slow(self, ev: StartEvent) -> StopEvent:
            step_started.set()
            await step_continue.wait()
            return StopEvent(result="done")

    wf = SlowWorkflow()
    handler = wf.run()

    # Wait for step to start
    await step_started.wait()

    # Try to get result before workflow completes - should raise
    with pytest.warns(DeprecationWarning):  # get_result is deprecated
        with pytest.raises(WorkflowRuntimeError, match="is not complete"):
            handler.ctx.get_result()

    # Let workflow complete
    step_continue.set()
    await handler


@pytest.mark.asyncio
async def test_get_result_pre_context_raises(workflow: Workflow) -> None:
    """Calling get_result() before run() should raise ContextStateError."""
    # Clear the lru_cache to ensure deprecation warning fires
    _warn_get_result.cache_clear()

    ctx = Context(workflow)

    with pytest.warns(DeprecationWarning):  # get_result is deprecated
        with pytest.raises(ContextStateError, match="requires a running workflow"):
            ctx.get_result()


# ============================================================================
# deserialize_state_from_dict Tests
# ============================================================================


class TypedTestState(BaseModel):
    """Typed state for deserialize_state_from_dict testing."""

    counter: int = 0
    name: str = "default"


@pytest.mark.asyncio
async def test_deserialize_state_from_dict_with_dict_state() -> None:
    """Test deserializing DictState from to_dict() format."""
    serializer = JsonSerializer()

    # Create state and serialize it
    store = InMemoryStateStore(DictState())
    await store.set("counter", 42)
    await store.set("name", "test-value")
    serialized = store.to_dict(serializer)

    # Deserialize
    result = deserialize_state_from_dict(serialized, serializer)

    assert isinstance(result, DictState)
    assert result["counter"] == 42
    assert result["name"] == "test-value"


def test_deserialize_state_from_dict_with_typed_state() -> None:
    """Test deserializing typed Pydantic model from to_dict() format."""
    serializer = JsonSerializer()

    # Create typed state and serialize it
    initial = TypedTestState(counter=100, name="typed-test")
    store = InMemoryStateStore(initial)
    serialized = store.to_dict(serializer)

    # Deserialize
    result = deserialize_state_from_dict(serialized, serializer)

    assert isinstance(result, TypedTestState)
    assert result.counter == 100
    assert result.name == "typed-test"


def test_deserialize_state_from_dict_empty_dict_state() -> None:
    """Test deserializing empty DictState."""
    serializer = JsonSerializer()

    serialized = {
        "state_data": {"_data": {}},
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
    }

    result = deserialize_state_from_dict(serialized, serializer)

    assert isinstance(result, DictState)
    assert len(list(result.items())) == 0


# ============================================================================
# Context.get_step_context() Tests
# ============================================================================


@pytest.mark.asyncio
async def test_get_step_context_outside_step_raises() -> None:
    """Calling Context.get_step_context() outside a step should raise WorkflowRuntimeError."""
    with pytest.raises(WorkflowRuntimeError, match="may only be called from within"):
        Context.get_step_context()


@pytest.mark.asyncio
async def test_get_step_context_inside_step() -> None:
    """Context.get_step_context() should return the step's context inside a step."""
    captured_ctx = None

    class TestWorkflow(Workflow):
        @step
        async def my_step(self, ev: StartEvent) -> StopEvent:
            nonlocal captured_ctx
            captured_ctx = Context.get_step_context()
            return StopEvent(result="done")

    wf = TestWorkflow()
    result = await wf.run()
    assert result == "done"
    assert captured_ctx is not None
    # The returned context should be in internal face state
    assert isinstance(captured_ctx._face, InternalContext)


@pytest.mark.asyncio
async def test_get_step_context_matches_ctx_parameter() -> None:
    """Context.get_step_context() should return the same Context as the ctx parameter."""
    ctx_from_param = None
    ctx_from_get_step_context = None

    class TestWorkflow(Workflow):
        @step
        async def my_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
            nonlocal ctx_from_param, ctx_from_get_step_context
            ctx_from_param = ctx
            ctx_from_get_step_context = Context.get_step_context()
            return StopEvent(result="done")

    wf = TestWorkflow()
    await wf.run()
    assert ctx_from_param is ctx_from_get_step_context


@pytest.mark.asyncio
async def test_get_step_context_supports_wait_for_event() -> None:
    """Context.get_step_context() should return a context that supports wait_for_event."""

    class ResumeEvent(Event):
        value: str = "resumed"

    class TestWorkflow(Workflow):
        @step
        async def waiting_step(self, ev: StartEvent) -> StopEvent:
            ctx = Context.get_step_context()
            result = await ctx.wait_for_event(
                ResumeEvent,
                waiter_event=InputRequiredEvent(),
            )
            return StopEvent(result=result.value)

    wf = TestWorkflow()
    handler = wf.run()

    async for ev in handler.stream_events():
        if isinstance(ev, InputRequiredEvent):
            handler.ctx.send_event(ResumeEvent(value="hello"))

    result = await handler
    assert result == "hello"


@pytest.mark.asyncio
async def test_get_step_context_does_not_pin_workflow() -> None:
    """InternalContextVar should not pin the Workflow via timer handle context snapshots."""
    handles: list[asyncio.TimerHandle] = []

    class TinyWorkflow(Workflow):
        @step
        async def only(self, ev: StartEvent) -> StopEvent:
            ctx = Context.get_step_context()
            assert ctx is not None
            # Schedule a long-lived timer that snapshots the current ContextVars
            handles.append(asyncio.get_running_loop().call_later(3600, lambda: None))
            return StopEvent(result="done")

    refs: list[weakref.ReferenceType[Workflow]] = []
    try:
        for _ in range(5):
            wf = TinyWorkflow()
            refs.append(cast(weakref.ReferenceType[Workflow], weakref.ref(wf)))
            await WorkflowTestRunner(wf).run()
            del wf

        for _ in range(3):
            gc.collect()

        assert all(r() is None for r in refs), (
            f"{sum(r() is not None for r in refs)} workflows pinned by "
            "InternalContextVar in TimerHandle context"
        )
    finally:
        for h in handles:
            h.cancel()


def test_deserialize_state_from_dict_defaults_to_dict_state() -> None:
    """Test that missing state_type defaults to DictState."""
    serializer = JsonSerializer()

    serialized = {"state_data": {"_data": {}}}

    result = deserialize_state_from_dict(serialized, serializer)

    assert isinstance(result, DictState)


# ============================================================================
# Serialized State Format Tests (parse_in_memory_state)
# ============================================================================


def test_parse_in_memory_state_old_format_no_store_type() -> None:
    """Test that old format (no store_type) parses as InMemorySerializedState."""
    from workflows.context.state_store import (
        InMemorySerializedState,
        parse_in_memory_state,
    )

    # Old format without store_type field
    old_format = {
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": {"_data": {"counter": 42}},
    }

    result = parse_in_memory_state(old_format)

    assert isinstance(result, InMemorySerializedState)
    assert result.store_type == "in_memory"
    assert result.state_type == "DictState"
    assert result.state_module == "workflows.context.state_store"
    assert result.state_data == {"_data": {"counter": 42}}


def test_parse_in_memory_state_explicit_in_memory() -> None:
    """Test that explicit store_type='in_memory' parses as InMemorySerializedState."""
    from workflows.context.state_store import (
        InMemorySerializedState,
        parse_in_memory_state,
    )

    serialized = {
        "store_type": "in_memory",
        "state_type": "CustomState",
        "state_module": "myapp.models",
        "state_data": {"name": "test", "value": 123},
    }

    result = parse_in_memory_state(serialized)

    assert isinstance(result, InMemorySerializedState)
    assert result.store_type == "in_memory"
    assert result.state_type == "CustomState"
    assert result.state_module == "myapp.models"
    assert result.state_data == {"name": "test", "value": 123}


def test_parse_in_memory_state_rejects_sql_store_type() -> None:
    """Test that store_type='sql' raises ValueError."""
    from workflows.context.state_store import parse_in_memory_state

    serialized = {
        "store_type": "sql",
        "run_id": "run-12345",
        "state_type": "WorkflowState",
        "state_module": "myapp.states",
        "schema": "public",
    }

    with pytest.raises(ValueError, match="Cannot parse store_type 'sql'"):
        parse_in_memory_state(serialized)


def test_parse_in_memory_state_unknown_store_type_raises() -> None:
    """Test that unknown store_type raises ValueError."""
    from workflows.context.state_store import parse_in_memory_state

    serialized = {
        "store_type": "redis",  # Unknown store type
        "state_type": "SomeState",
        "state_module": "some.module",
    }

    with pytest.raises(ValueError, match="Cannot parse store_type 'redis'"):
        parse_in_memory_state(serialized)


# ============================================================================
# InMemoryStateStore Serialization Tests
# ============================================================================


def test_in_memory_state_store_to_dict_includes_store_type() -> None:
    """Test that to_dict() includes store_type='in_memory'."""
    store = InMemoryStateStore(DictState())
    serializer = JsonSerializer()

    result = store.to_dict(serializer)

    assert result["store_type"] == "in_memory"
    assert "state_type" in result
    assert "state_module" in result
    assert "state_data" in result


def test_in_memory_state_store_from_dict_rejects_sql_format() -> None:
    """Test that from_dict() rejects SQL format with clear error."""
    sql_format = {
        "store_type": "sql",
        "run_id": "run-12345",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
    }
    serializer = JsonSerializer()

    with pytest.raises(ValueError, match="Cannot parse store_type 'sql'"):
        InMemoryStateStore.from_dict(sql_format, serializer)
