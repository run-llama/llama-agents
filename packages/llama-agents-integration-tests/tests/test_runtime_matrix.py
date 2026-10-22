# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Runtime matrix tests - testing workflows against BasicRuntime and DBOSRuntime.

All workflow classes are defined at module level so they can be registered with
DBOS once at module initialization time, avoiding repeated init/destroy cycles.

Note: The dbos-postgres variant requires Docker to be available and is marked
with the 'docker' pytest marker. Run with `pytest -m docker` to include it.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, AsyncGenerator, Generator

import pytest
from dbos import DBOS, DBOSConfig
from llama_agents.dbos import DBOSRuntime
from pydantic import BaseModel, Field
from testcontainers.postgres import PostgresContainer
from workflows.context import Context
from workflows.decorators import step
from workflows.errors import WorkflowTimeoutError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)
from workflows.plugins.basic import BasicRuntime
from workflows.runtime.types.plugin import Runtime
from workflows.testing import WorkflowTestRunner
from workflows.workflow import Workflow

# -- Fixtures --


def _get_runtime_params() -> list[Any]:
    """Get runtime parameters for the test matrix.

    Includes:
    - basic: BasicRuntime (fast, no dependencies)
    - dbos: DBOSRuntime with SQLite backend (fast, no Docker)
    - dbos-postgres: DBOSRuntime with PostgreSQL backend (requires Docker)

    Note: The dbos-postgres variant is marked with the 'docker' marker and
    requires Docker to be running. It only runs when explicitly requested
    via `pytest -m docker`.
    """
    return [
        pytest.param("basic", id="basic"),
        pytest.param("dbos", id="dbos"),
        pytest.param("dbos-postgres", marks=pytest.mark.docker, id="dbos-postgres"),
    ]


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Module-scoped PostgreSQL container for DBOS tests.

    This fixture is only used when dbos-postgres runtime is requested.
    Requires Docker to be running.
    """
    with PostgresContainer("postgres:16", driver=None) as postgres:
        yield postgres


@pytest.fixture
def dbos_runtime_sqlite(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[DBOSRuntime, None, None]:
    """Function-scoped DBOS runtime with SQLite backend (fresh DB per test).

    DBOS.destroy() shuts down its executor which it also installs as the event
    loop's default executor. Since the event loop is module-scoped, we must
    restore a fresh default executor after destroy to avoid poisoning subsequent
    tests that use run_in_executor(None, ...).
    """
    db_file: Path = tmp_path_factory.mktemp("dbos") / "dbos_test.sqlite3"
    system_db_url: str = f"sqlite+pysqlite:///{db_file}?check_same_thread=false"
    config: DBOSConfig = {
        "name": "workflows-py-dbostest",
        "system_database_url": system_db_url,
        "run_admin_server": False,
        "notification_listener_polling_interval_sec": 0.01,
    }
    DBOS(config=config)
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    # Capture the event loop before destroy_sync() can close it (asyncio.run()
    # creates and closes a temporary loop, which removes the thread-local loop).
    loop = asyncio.get_event_loop()
    try:
        yield runtime
    finally:
        runtime.destroy_sync()
        # DBOS sets itself as the default executor and destroy() shuts it down.
        # Restore a fresh executor so the module-scoped event loop remains usable.
        # Re-set the event loop since asyncio.run() cleared it.
        asyncio.set_event_loop(loop)
        loop.set_default_executor(ThreadPoolExecutor())


@pytest.fixture(scope="module")
def dbos_runtime_postgres(
    postgres_container: PostgresContainer,
) -> Generator[DBOSRuntime, None, None]:
    """Module-scoped DBOS runtime with PostgreSQL backend."""
    connection_url = postgres_container.get_connection_url()
    config: DBOSConfig = {
        "name": "wf-dbos-pg-test",  # Must be <= 30 chars
        "system_database_url": connection_url,
        "run_admin_server": False,
        "notification_listener_polling_interval_sec": 0.01,
    }
    DBOS(config=config)
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    try:
        yield runtime
    finally:
        runtime.destroy_sync()


@pytest.fixture(params=_get_runtime_params())
async def runtime(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Runtime, None]:
    """Yield an unlaunched runtime.

    For DBOS variants, returns the module-scoped runtime (already created, not yet
    launched). Each test must call runtime.launch() after creating workflows.

    Note: Only one DBOS variant can be used per test run since DBOS is a singleton.
    Use TEST_DBOS_POSTGRES=1 to run with PostgreSQL instead of the default SQLite.
    """
    if request.param == "basic":
        rt = BasicRuntime()
        try:
            yield rt
        finally:
            await rt.destroy()
    elif request.param == "dbos":
        dbos_rt: DBOSRuntime = request.getfixturevalue("dbos_runtime_sqlite")
        yield dbos_rt
    elif request.param == "dbos-postgres":
        dbos_rt = request.getfixturevalue("dbos_runtime_postgres")
        yield dbos_rt


# -- Shared event types --


class OneTestEvent(Event):
    test_param: str = Field(default="test")


class AnotherTestEvent(Event):
    another_test_param: str = Field(default="another_test")


class LastEvent(Event):
    pass


class MyStart(StartEvent):
    query: str


class MyStop(StopEvent):
    outcome: str


# -- Workflow definitions (module level for DBOS registration) --


class SimpleWorkflow(Workflow):
    @step
    async def start_step(self, ev: StartEvent) -> OneTestEvent:
        return OneTestEvent()

    @step
    async def middle_step(self, ev: OneTestEvent) -> LastEvent:
        return LastEvent()

    @step
    async def end_step(self, ev: LastEvent) -> StopEvent:
        return StopEvent(result="Workflow completed")


class SlowWorkflow(Workflow):
    @step
    async def slow_step(self, ev: StartEvent) -> StopEvent:
        await asyncio.sleep(2.0)
        return StopEvent(result="Done")


class EventTrackingWorkflow(Workflow):
    """Workflow that tracks events in an external list."""

    tracked_events: list[str] = []

    @step
    async def step1(self, ev: StartEvent) -> OneTestEvent:
        self.tracked_events.append("step1")
        return OneTestEvent()

    @step
    async def step2(self, ev: OneTestEvent) -> StopEvent:
        self.tracked_events.append("step2")
        return StopEvent(result="Done")


class SyncAsyncWorkflow(Workflow):
    @step
    async def async_step(self, ev: StartEvent) -> OneTestEvent:
        return OneTestEvent()

    @step
    def sync_step(self, ev: OneTestEvent) -> StopEvent:
        return StopEvent(result="Done")


class SyncWorkflow(Workflow):
    @step
    def step_one(self, ctx: Context, ev: StartEvent) -> OneTestEvent:
        ctx.collect_events(ev, [StartEvent])
        return OneTestEvent()

    @step
    def step_two(self, ctx: Context, ev: OneTestEvent) -> StopEvent:
        return StopEvent()


class MultiRunWorkflow(Workflow):
    @step
    async def step(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result=ev.number * 2)


class ErrorWorkflow(Workflow):
    @step
    async def step(self, ev: StartEvent) -> StopEvent:
        raise ValueError("The step raised an error!")


class CounterWorkflow(Workflow):
    @step
    async def step(self, ctx: Context, ev: StartEvent) -> StopEvent:
        cur_number = await ctx.store.get("number", default=0)
        new_number = cur_number + 1
        await ctx.store.set("number", new_number)
        return StopEvent(result=new_number)


class StepSendEventWorkflow(Workflow):
    @step
    async def step1(self, ctx: Context, ev: StartEvent) -> OneTestEvent:
        ctx.send_event(OneTestEvent(), step="step2")
        return None  # type: ignore

    @step
    async def step2(self, ev: OneTestEvent) -> StopEvent:
        return StopEvent(result="step2")

    @step
    async def step3(self, ev: OneTestEvent) -> StopEvent:
        return StopEvent(result="step3")


class NumWorkersWorkflow(Workflow):
    @step
    async def original_step(
        self, ctx: Context, ev: StartEvent
    ) -> OneTestEvent | LastEvent:
        await ctx.store.set("num_to_collect", 3)
        ctx.send_event(OneTestEvent(test_param="test1"))
        ctx.send_event(OneTestEvent(test_param="test2"))
        ctx.send_event(OneTestEvent(test_param="test3"))
        ctx.send_event(AnotherTestEvent(another_test_param="test4"))
        return LastEvent()

    @step(num_workers=3)
    async def test_step(self, ev: OneTestEvent) -> AnotherTestEvent:
        # Note: await_count logic needs to be injected per-test
        return AnotherTestEvent(another_test_param=ev.test_param)

    @step
    async def final_step(
        self, ctx: Context, ev: AnotherTestEvent | LastEvent
    ) -> StopEvent:
        n = await ctx.store.get("num_to_collect")
        events = ctx.collect_events(ev, [AnotherTestEvent] * n)
        if events is None:
            return None  # type: ignore
        return StopEvent(result=[ev.another_test_param for ev in events])


class CustomEventsWorkflow(Workflow):
    @step
    async def start_step(self, ev: MyStart) -> OneTestEvent:
        # Small delay to avoid DBOS read_stream_async race condition where
        # the workflow completes before the stream reader starts polling.
        # See thoughts/shared/bugs/dbos-read-stream-race.md
        await asyncio.sleep(0.05)
        return OneTestEvent()

    @step
    async def middle_step(self, ev: OneTestEvent) -> LastEvent:
        return LastEvent()

    @step
    async def end_step(self, ev: LastEvent) -> MyStop:
        return MyStop(outcome="Workflow completed")


class HITLWorkflow(Workflow):
    @step
    async def step1(self, ctx: Context, ev: StartEvent) -> InputRequiredEvent:
        cur_runs = await ctx.store.get("step1_runs", default=0)
        await ctx.store.set("step1_runs", cur_runs + 1)
        return InputRequiredEvent(prefix="Enter a number: ")  # type:ignore

    @step
    async def step2(self, ctx: Context, ev: HumanResponseEvent) -> StopEvent:
        cur_runs = await ctx.store.get("step2_runs", default=0)
        await ctx.store.set("step2_runs", cur_runs + 1)
        return StopEvent(result=ev.response)


class StreamWorkflow(Workflow):
    @step
    async def chat(self, ctx: Context, ev: StartEvent) -> StopEvent:
        async def stream_messages() -> AsyncGenerator[str, None]:
            resp = "Paul Graham is a British-American computer scientist, entrepreneur, vc, and writer."
            for word in resp.split():
                yield word

        async for w in stream_messages():
            ctx.write_event_to_stream(Event(msg=w))

        return StopEvent(result=None)


class ErrorStreamingWorkflow(Workflow):
    @step
    async def step(self, ctx: Context, ev: StartEvent) -> StopEvent:
        ctx.write_event_to_stream(OneTestEvent(test_param="foo"))
        raise ValueError("The step raised an error!")


class TimeoutStreamingWorkflow(Workflow):
    @step
    async def step(self, ctx: Context, ev: StartEvent) -> StopEvent:
        ctx.write_event_to_stream(OneTestEvent(test_param="foo"))
        await asyncio.sleep(2)
        return StopEvent()


# -- Tests --


@pytest.mark.asyncio
async def test_workflow_run(runtime: Runtime) -> None:
    workflow = SimpleWorkflow(runtime=runtime)
    await runtime.launch()
    r = await WorkflowTestRunner(workflow).run()
    assert r.result == "Workflow completed"


@pytest.mark.asyncio
async def test_workflow_timeout(runtime: Runtime) -> None:
    wf = SlowWorkflow(timeout=0.1, runtime=runtime)
    await runtime.launch()
    with pytest.raises(WorkflowTimeoutError):
        await WorkflowTestRunner(wf).run()


@pytest.mark.asyncio
async def test_workflow_event_propagation(runtime: Runtime) -> None:
    events: list[str] = []

    class LocalEventTrackingWorkflow(Workflow):
        @step
        async def step1(self, ev: StartEvent) -> OneTestEvent:
            events.append("step1")
            return OneTestEvent()

        @step
        async def step2(self, ev: OneTestEvent) -> StopEvent:
            events.append("step2")
            return StopEvent(result="Done")

    wf = LocalEventTrackingWorkflow(runtime=runtime)
    await runtime.launch()
    await WorkflowTestRunner(wf).run()
    assert events == ["step1", "step2"]


@pytest.mark.asyncio
async def test_workflow_sync_async_steps(runtime: Runtime) -> None:
    wf = SyncAsyncWorkflow(runtime=runtime)
    await runtime.launch()
    await WorkflowTestRunner(wf).run()


@pytest.mark.asyncio
async def test_workflow_sync_steps_only(runtime: Runtime) -> None:
    wf = SyncWorkflow(runtime=runtime)
    await runtime.launch()
    await WorkflowTestRunner(wf).run()


@pytest.mark.asyncio
async def test_workflow_multiple_runs(runtime: Runtime) -> None:
    wf = MultiRunWorkflow(runtime=runtime)
    await runtime.launch()
    runner = WorkflowTestRunner(wf)
    results = await asyncio.gather(
        runner.run(StartEvent(number=3)),  # type: ignore
        runner.run(StartEvent(number=42)),  # type: ignore
        runner.run(StartEvent(number=-99)),  # type: ignore
    )
    assert set([r.result for r in results]) == {6, 84, -198}


@pytest.mark.asyncio
async def test_workflow_task_raises(runtime: Runtime) -> None:
    wf = ErrorWorkflow(runtime=runtime)
    await runtime.launch()
    with pytest.raises(ValueError, match="The step raised an error!"):
        await WorkflowTestRunner(wf).run()


@pytest.mark.asyncio
async def test_workflow_step_send_event(runtime: Runtime) -> None:
    workflow = StepSendEventWorkflow(runtime=runtime)
    await runtime.launch()
    r = await WorkflowTestRunner(workflow).run()
    assert r.result == "step2"


@pytest.mark.asyncio
async def test_workflow_num_workers(runtime: Runtime) -> None:
    """Test that num_workers limits concurrent step executions.

    This test verifies that:
    1. A step with num_workers=5 can process up to 5 events concurrently
    2. All 5 workers can run simultaneously (they synchronize to prove concurrency)
    3. The workflow completes successfully with all events processed
    """
    num_workers = 5
    num_events = 10
    # Track max concurrent executions
    lock = asyncio.Lock()
    current_workers = 0
    max_concurrent = 0
    # Barrier to ensure all workers reach this point before any proceed
    barrier_count = 0
    barrier_event = asyncio.Event()

    class NumWorkersWorkflow(Workflow):
        @step
        async def fan_out(self, ctx: Context, ev: StartEvent) -> OneTestEvent:
            # Send more events than num_workers to test queuing
            for i in range(num_events):
                ctx.send_event(OneTestEvent(test_param=str(i)))
            return None  # type: ignore

        @step(num_workers=num_workers)
        async def worker_step(self, ev: OneTestEvent) -> AnotherTestEvent:
            nonlocal current_workers, max_concurrent, barrier_count

            async with lock:
                current_workers += 1
                max_concurrent = max(max_concurrent, current_workers)
                barrier_count += 1
                if barrier_count == num_workers:
                    # All workers have arrived, release them
                    barrier_event.set()

            # Wait for all workers to arrive (proves concurrency)
            await barrier_event.wait()

            async with lock:
                current_workers -= 1

            return AnotherTestEvent(another_test_param=ev.test_param)

        @step
        async def collect_step(
            self, ctx: Context, ev: AnotherTestEvent
        ) -> StopEvent | None:
            events = ctx.collect_events(ev, [AnotherTestEvent] * num_events)
            if events is None:
                return None
            return StopEvent(result=[e.another_test_param for e in events])

    # Concurrency is asserted by the barrier, not by database throughput.
    # The pytest timeout still bounds a stalled workflow.
    workflow = NumWorkersWorkflow(timeout=None, runtime=runtime)
    await runtime.launch()
    r = await WorkflowTestRunner(workflow).run()

    # Verify all events were processed
    assert len(r.result) == num_events
    assert set(r.result) == {str(i) for i in range(num_events)}
    # Verify we achieved the expected concurrency (all 5 workers ran together)
    assert max_concurrent == num_workers


@pytest.mark.asyncio
async def test_custom_stop_event(runtime: Runtime) -> None:
    wf = CustomEventsWorkflow(runtime=runtime)
    await runtime.launch()

    assert wf._start_event_class == MyStart
    assert wf.start_event_class == wf._start_event_class
    assert wf._stop_event_class == MyStop
    assert wf.stop_event_class == wf._stop_event_class
    result: MyStop = await wf.run(query="foo")
    assert result.outcome == "Workflow completed"

    # Run again with the same workflow instance
    assert wf._start_event_class == MyStart
    assert wf._stop_event_class == MyStop
    result = await wf.run(query="foo")
    assert result.outcome == "Workflow completed"

    # ensure that streaming exits
    r = await WorkflowTestRunner(wf).run(MyStart(query="foo"))
    assert len(r.collected) > 0


@pytest.mark.asyncio
async def test_human_in_the_loop(runtime: Runtime) -> None:
    # Create both workflow instances before launch
    timeout_wf = HITLWorkflow(timeout=0.01, runtime=runtime)
    workflow = HITLWorkflow(runtime=runtime)
    await runtime.launch()

    # workflow should raise a timeout error because hitl only works with streaming
    with pytest.raises(WorkflowTimeoutError):
        await WorkflowTestRunner(timeout_wf).run()

    # workflow should work with streaming
    handler = workflow.run()
    assert handler.ctx
    async for event in handler.stream_events():
        if isinstance(event, InputRequiredEvent):
            assert event.prefix == "Enter a number: "
            handler.ctx.send_event(HumanResponseEvent(response="42"))  # type:ignore

    final_result = await handler
    assert final_result == "42"


@pytest.mark.asyncio
async def test_workflow_stream_events_exits(runtime: Runtime) -> None:
    wf = CustomEventsWorkflow(runtime=runtime)
    await runtime.launch()
    handler = wf.run(query="foo")

    async def _stream_events() -> MyStop:
        async for event in handler.stream_events():
            continue
        return await handler

    stream_task = asyncio.create_task(_stream_events())
    result = await asyncio.wait_for(stream_task, timeout=10)
    assert result.outcome == "Workflow completed"


# -- Streaming tests --


@pytest.mark.asyncio
async def test_streaming_e2e(runtime: Runtime) -> None:
    wf = StreamWorkflow(runtime=runtime)
    await runtime.launch()
    test_runner = WorkflowTestRunner(wf)
    r = await test_runner.run(expose_internal=False, exclude_events=[StopEvent])
    assert all("msg" in ev for ev in r.collected)


@pytest.mark.asyncio
async def test_streaming_task_raised(runtime: Runtime) -> None:
    wf = ErrorStreamingWorkflow(runtime=runtime)
    await runtime.launch()
    r = wf.run()

    async for ev in r.stream_events():
        if not isinstance(ev, StopEvent):
            assert ev.test_param == "foo"

    with pytest.raises(ValueError, match="The step raised an error!"):
        await r


@pytest.mark.asyncio
async def test_streaming_task_timeout(runtime: Runtime) -> None:
    wf = TimeoutStreamingWorkflow(timeout=0.1, runtime=runtime)
    await runtime.launch()
    r = wf.run()

    async for ev in r.stream_events():
        if not isinstance(ev, StopEvent):
            assert ev.test_param == "foo"

    with pytest.raises(WorkflowTimeoutError, match="Operation timed out"):
        await r


# -- Workflow State Tests --


class StatefulWorkflow(Workflow):
    """Workflow that accumulates state across steps."""

    @step
    async def step1(self, ctx: Context, ev: StartEvent) -> OneTestEvent:
        await ctx.store.set("step1_ran", True)
        await ctx.store.set("counter", 1)
        return OneTestEvent()

    @step
    async def step2(self, ctx: Context, ev: OneTestEvent) -> StopEvent:
        await ctx.store.set("step2_ran", True)
        counter = await ctx.store.get("counter")
        await ctx.store.set("counter", counter + 1)
        final_counter = await ctx.store.get("counter")
        return StopEvent(result={"counter": final_counter})


class NestedStateWorkflow(Workflow):
    """Workflow that uses nested state paths."""

    @step
    async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
        await ctx.store.set("user", {"name": "Alice", "profile": {"level": 1}})
        await ctx.store.set("user.profile.level", 2)
        level = await ctx.store.get("user.profile.level")
        name = await ctx.store.get("user.name")
        return StopEvent(result={"name": name, "level": level})


@pytest.mark.asyncio
async def test_workflow_state_basic(runtime: Runtime) -> None:
    """Test basic state operations within a workflow."""
    wf = CounterWorkflow(runtime=runtime)
    await runtime.launch()
    result = await WorkflowTestRunner(wf).run()
    assert result.result == 1


@pytest.mark.asyncio
async def test_workflow_state_across_steps(runtime: Runtime) -> None:
    """Test state persistence across multiple workflow steps."""
    wf = StatefulWorkflow(runtime=runtime)
    await runtime.launch()
    result = await WorkflowTestRunner(wf).run()
    assert result.result == {"counter": 2}


@pytest.mark.asyncio
async def test_workflow_nested_state(runtime: Runtime) -> None:
    """Test nested state path access within workflows."""
    wf = NestedStateWorkflow(runtime=runtime)
    await runtime.launch()
    result = await WorkflowTestRunner(wf).run()
    assert result.result == {"name": "Alice", "level": 2}


@pytest.mark.asyncio
async def test_workflow_state_multiple_runs(runtime: Runtime) -> None:
    """Test that each workflow run has isolated state."""
    wf = CounterWorkflow(runtime=runtime)
    await runtime.launch()
    runner = WorkflowTestRunner(wf)

    # Run multiple times - each should start fresh
    results = await asyncio.gather(
        runner.run(),
        runner.run(),
        runner.run(),
    )

    # Each run should have counter=1 (not accumulating)
    for r in results:
        assert r.result == 1


# -- Typed State Tests --


class TypedState(BaseModel):
    """Custom typed state for workflow testing."""

    counter: int = 0
    name: str = "default"
    items: list[str] = Field(default_factory=list)


class TypeStateStopEvent(StopEvent):
    state_type: str
    initial_counter: int
    final_counter: int
    final_name: str


class TypedStateWorkflow(Workflow):
    """Workflow that uses typed state via Context[TypedState]."""

    @step
    async def process(self, ctx: Context[TypedState], ev: StartEvent) -> StopEvent:
        # Access typed state
        state = await ctx.store.get_state()

        # Verify we got the right type
        state_type_name = type(state).__name__

        # Modify state using typed fields
        await ctx.store.set("counter", state.counter + 1)
        await ctx.store.set("name", "modified")

        final_state = await ctx.store.get_state()
        return TypeStateStopEvent(
            state_type=state_type_name,
            initial_counter=state.counter,
            final_counter=final_state.counter,
            final_name=final_state.name,
        )


@pytest.mark.asyncio
async def test_typed_state_workflow(runtime: Runtime) -> None:
    """Test workflow with typed state Context[TypedState].

    This verifies that:
    1. The state type is correctly inferred from Context[T] annotation
    2. The state is created with the correct type
    3. Typed field access works correctly
    """
    wf = TypedStateWorkflow(runtime=runtime)
    await runtime.launch()

    result = await WorkflowTestRunner(wf).run()

    # The state should be TypedState, not DictState
    assert result.result.state_type == "TypedState", (
        f"Expected TypedState but got {result.result.state_type}. "
        "State type inference may not be working."
    )
    assert result.result.initial_counter == 0
    assert result.result.final_counter == 1
    assert result.result.final_name == "modified"


class TypedStateWithDefaultsWorkflow(Workflow):
    """Workflow that verifies typed state has correct defaults."""

    @step
    async def check_defaults(
        self, ctx: Context[TypedState], ev: StartEvent
    ) -> StopEvent:
        state = await ctx.store.get_state()
        return StopEvent(
            result={
                "counter": state.counter,
                "name": state.name,
                "items": state.items,
            }
        )


@pytest.mark.asyncio
async def test_typed_state_defaults(runtime: Runtime) -> None:
    """Test that typed state is initialized with correct defaults."""
    wf = TypedStateWithDefaultsWorkflow(runtime=runtime)
    await runtime.launch()

    result = await WorkflowTestRunner(wf).run()

    assert result.result["counter"] == 0
    assert result.result["name"] == "default"
    assert result.result["items"] == []


@pytest.mark.asyncio
async def test_typed_state_with_initial_values(runtime: Runtime) -> None:
    """Test that initial state values are passed through to the workflow.

    This verifies that:
    1. State can be set before running the workflow
    2. The initial values are correctly used (not replaced with defaults)
    3. Modifications build on the initial values
    """
    wf = TypedStateWorkflow(runtime=runtime)
    await runtime.launch()

    # Create a context and set initial state with counter=1 (default is 0)
    ctx = Context(wf)
    await ctx.store.set("counter", 1)

    result = await WorkflowTestRunner(wf).run(ctx=ctx)

    # If initial state wasn't passed through, initial_counter would be 0
    # and final_counter would be 1 (from default 0 + 1)
    assert result.result.initial_counter == 1, (
        f"Expected initial_counter=1 but got {result.result.initial_counter}. "
        "Initial state was not passed through to the workflow."
    )
    assert result.result.final_counter == 2, (
        f"Expected final_counter=2 but got {result.result.final_counter}. "
        "State modification did not build on initial value."
    )
