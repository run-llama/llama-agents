# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""
Tests for the control_loop function in the runtime module.

The control loop is the core event processing engine that:
- Processes workflow ticks (events, step results, timeouts, cancellations)
- Manages step worker state and execution
- Coordinates event routing between steps
- Handles retries, timeouts, and failures
"""

import asyncio
import time
import uuid
from typing import Coroutine

import pytest
import time_machine
from workflows.context import Context
from workflows.context.state_store import DictState, InMemoryStateStore
from workflows.decorators import step
from workflows.errors import WorkflowCancelledByUser, WorkflowTimeoutError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StepStateChanged,
    StopEvent,
    WorkflowCancelledEvent,
    WorkflowFailedEvent,
    WorkflowIdleEvent,
    WorkflowTimedOutEvent,
)
from workflows.plugins.basic import setting_run_id
from workflows.retry_policy import (
    ConstantDelayRetryPolicy,
    RetryPolicy,
    retry_policy,
    stop_before_delay,
    wait_fixed,
)
from workflows.runtime.control_loop.runner import _ControlLoopRunner, control_loop
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import RunContext, run_context
from workflows.runtime.types.step_function import as_step_worker_function
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickIdleCheck,
    TickWakeup,
    WorkflowTick,
)
from workflows.workflow import Workflow

from .conftest import MockRunAdapter, MockRuntime

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


class IntermediateEvent(Event):
    """Test event passed between workflow steps."""

    value: int


class FinalEvent(Event):
    """Test event indicating workflow completion."""

    final_value: str


class SimpleWorkflow(Workflow):
    """
    A simple three-step workflow for testing the happy path.

    Flow:
        StartEvent -> IntermediateEvent -> FinalEvent -> StopEvent
    """

    @step
    async def start_step(self, ev: StartEvent) -> IntermediateEvent:
        """First step: receives start event and produces intermediate event."""
        return IntermediateEvent(value=42)

    @step
    async def middle_step(self, ev: IntermediateEvent) -> FinalEvent:
        """Second step: processes intermediate event and produces final event."""
        return FinalEvent(final_value=f"processed_{ev.value}")

    @step
    async def end_step(self, ev: FinalEvent) -> StopEvent:
        """Final step: receives final event and returns stop event."""
        return StopEvent(result=ev.final_value)


class CollectEv(Event):
    i: int


class CollectEv2(Event):
    j: int


class CollectMultipleEventTypesWorkflow(Workflow):
    @step
    async def accept_start(self, ev: StartEvent, context: Context) -> CollectEv | None:
        for i in range(2):
            context.send_event(CollectEv(i=i + 1))
        return None

    @step
    async def accept_collect1(self, ev: CollectEv, context: Context) -> CollectEv2:
        return CollectEv2(j=ev.i * 10)

    @step
    async def collector(
        self, ev: CollectEv | CollectEv2, context: Context
    ) -> StopEvent | None:
        events = context.collect_events(ev, [CollectEv, CollectEv2] * 2)
        if events is None:
            return None
        assert [type(x) for x in events] == [
            CollectEv,
            CollectEv2,
        ] * 2  # same order as expected
        events = sum(
            [
                e.i
                if isinstance(e, CollectEv)
                else e.j
                if isinstance(e, CollectEv2)
                else 0
                for e in events
            ]
        )
        return StopEvent(result=f"sum_{events}")


class CollectWorkflow(Workflow):
    @step
    async def accept_start(self, ev: StartEvent, context: Context) -> CollectEv | None:
        for i in range(4):
            context.send_event(CollectEv(i=i + 1))
        return None

    @step
    async def collector(self, ev: CollectEv, context: Context) -> StopEvent | None:
        events = context.collect_events(ev, [CollectEv] * 4)
        if events is None:
            return None
        events = sum([e.i for e in events])
        return StopEvent(result=f"sum_{events}")


def run_control_loop(
    workflow: Workflow,
    start_event: StartEvent | None,
    test_runtime: MockRunAdapter,
) -> Coroutine[None, None, StopEvent]:
    step_workers = {}
    for name, step_func in workflow._get_steps().items():
        unbound = getattr(step_func, "__func__", step_func)
        step_workers[StepId.root(name)] = as_step_worker_function(unbound)
    run_id = str(uuid.uuid4())
    # Set up mock runtime with the test adapter
    mock_runtime = MockRuntime()
    test_runtime.set_state_store(InMemoryStateStore(DictState()))
    mock_runtime.set_adapter(run_id, test_runtime)
    # Override workflow's runtime to use mock
    workflow._runtime = mock_runtime
    with setting_run_id(run_id):
        ctx = Context._create_internal(workflow=workflow)

    async def _run() -> StopEvent:
        with setting_run_id(run_id):
            run_ctx = RunContext(
                workflow=workflow,
                run_adapter=test_runtime,
                context=ctx,
                steps=step_workers,
            )
            with run_context(run_ctx):
                return await control_loop(
                    start_event=start_event,
                    init_state=BrokerState.from_workflow(workflow),
                    run_id=run_id,
                )

    return _run()


async def wait_for_stop_event(
    plugin: MockRunAdapter, timeout: float = 1.0
) -> StopEvent | None:
    """
    Helper to wait for a StopEvent in the event stream.

    Args:
        plugin: The MockRunAdapter to read events from
        timeout: Maximum time to wait for StopEvent (default: 1.0 seconds)

    Returns:
        The StopEvent if found, None if timeout occurs
    """
    try:
        while True:
            try:
                ev = await asyncio.wait_for(
                    plugin.get_stream_event(timeout=timeout), timeout=timeout
                )
                if isinstance(ev, StopEvent):
                    return ev
            except asyncio.TimeoutError:
                return None
    except Exception:
        return None


@pytest.mark.asyncio
async def test_control_loop_happy_path(test_plugin: MockRunAdapter) -> None:
    """
    Test the happy path through the control loop.

    This test validates that:
    1. The control loop properly initializes with workflow state
    2. Events flow through the workflow steps in order
    3. Each step executes and produces the correct output event
    4. The workflow completes with the expected StopEvent result
    5. Step state changes are published to the event stream
    """

    result = await run_control_loop(
        workflow=SimpleWorkflow(timeout=1.0),
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )

    # Verify the workflow completed with expected result
    assert isinstance(result, StopEvent)
    assert result.result == "processed_42"


@pytest.mark.asyncio
async def test_control_loop_with_external_event(
    test_plugin: MockRunAdapter,
) -> None:
    """
    Test that external events can be sent to a running workflow.

    This validates that the control loop can receive events from outside
    during execution, useful for human-in-the-loop or webhook scenarios.

    The workflow starts with no initial event, and we inject a StartEvent
    externally using the plugin's send_event method.
    """

    class ExternalTriggerWorkflow(Workflow):
        """Workflow that waits for an external event."""

        @step
        async def start_step(self, ev: StartEvent) -> StopEvent:
            """Step that processes the externally sent start event."""
            return StopEvent(result="received_external_event")

    # Setup
    workflow = ExternalTriggerWorkflow(timeout=1.0)

    result_task = asyncio.create_task(
        run_control_loop(
            workflow=workflow,
            start_event=None,
            test_runtime=test_plugin,
        )
    )

    # Now send an external event to trigger the workflow
    await test_plugin.send_event(TickAddEvent(event=StartEvent()))

    # Wait for completion
    result = await asyncio.wait_for(result_task, timeout=5.0)

    # Verify
    assert isinstance(result, StopEvent)
    assert result.result == "received_external_event"


@pytest.mark.asyncio
async def test_control_loop_timeout(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
) -> None:
    """
    Test that workflow timeout raises WorkflowTimeoutError and publishes WorkflowTimedOutEvent.

    When a workflow times out, a WorkflowTimedOutEvent should be published to the stream
    to inform consumers about the timeout before the exception is raised.
    """
    test_plugin, _ = test_plugin_with_time_machine

    class SlowWorkflow(Workflow):
        @step
        async def slow(self, ev: StartEvent) -> StopEvent:
            await asyncio.sleep(0.5)
            return StopEvent(result="never")

    wf = SlowWorkflow(timeout=0.01)

    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Wait for the StopEvent to be published
    stop_event = await wait_for_stop_event(test_plugin)

    # Verify that the timeout exception is raised
    with pytest.raises(WorkflowTimeoutError):
        await asyncio.wait_for(task, timeout=1.0)

    # Verify a WorkflowTimedOutEvent was published to the stream
    assert stop_event is not None, (
        "Timeout should publish WorkflowTimedOutEvent to stream before raising exception"
    )
    assert isinstance(stop_event, WorkflowTimedOutEvent), (
        f"Expected WorkflowTimedOutEvent, got {type(stop_event).__name__}"
    )
    assert stop_event.timeout == 0.01, "Timeout event should contain the timeout value"
    assert stop_event.active_steps == ["slow"], "Timeout event should list active steps"


@pytest.mark.asyncio
async def test_wait_for_event_timeout(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
) -> None:
    """wait_for_event raises asyncio.TimeoutError when the timeout elapses."""
    test_plugin, _ = test_plugin_with_time_machine

    class AwaitedEvent(Event):
        pass

    class WaiterWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent, ctx: Context) -> StopEvent:
            await ctx.wait_for_event(AwaitedEvent, timeout=0.01)
            return StopEvent(result="should not reach")

    wf = WaiterWorkflow()
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # No event is sent — the waiter should time out
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_control_loop_retry_policy(test_plugin: MockRunAdapter) -> None:
    """
    Test that retry policy works correctly when a step fails initially but succeeds on retry.
    """

    class RetryWorkflow(Workflow):
        attempts = 0

        @step(retry_policy=ConstantDelayRetryPolicy(maximum_attempts=2, delay=0))
        async def flaky(self, ev: StartEvent) -> StopEvent:
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("fail once")
            return StopEvent(result=f"ok_{self.attempts}")

    wf = RetryWorkflow(timeout=1.0)

    result = await run_control_loop(
        workflow=wf,
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )

    assert isinstance(result, StopEvent)
    assert result.result == "ok_2"


@pytest.mark.asyncio
async def test_control_loop_step_failure_publishes_stop_event(
    test_plugin: MockRunAdapter,
) -> None:
    """
    Test that when a step fails permanently (retries exhausted),
    a WorkflowFailedEvent is published to the stream before raising the exception.

    This allows external consumers to know why the workflow stream has ended.
    """

    class FailingWorkflow(Workflow):
        @step(retry_policy=ConstantDelayRetryPolicy(maximum_attempts=1, delay=0))
        async def always_fails(self, ev: StartEvent) -> StopEvent:
            raise ValueError("intentional failure")

    wf = FailingWorkflow(timeout=1.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Wait for the StopEvent to be published
    stop_event = await wait_for_stop_event(test_plugin)

    # Now verify the workflow raised an exception
    with pytest.raises(ValueError, match="intentional failure"):
        await asyncio.wait_for(task, timeout=1.0)

    # Verify that a WorkflowFailedEvent was published before the exception
    assert stop_event is not None, (
        "WorkflowFailedEvent should be published to stream when step fails permanently"
    )
    assert isinstance(stop_event, WorkflowFailedEvent), (
        f"Expected WorkflowFailedEvent, got {type(stop_event).__name__}"
    )
    assert stop_event.step_name == "always_fails", (
        "Failed event should contain the step name"
    )
    assert isinstance(stop_event.exception, ValueError), (
        "Failed event should carry the live exception"
    )
    assert str(stop_event.exception) == "intentional failure", (
        "Failed event exception should carry the message"
    )
    assert stop_event.attempts == 1, "Failed event should contain the attempt count"
    assert stop_event.elapsed_seconds is not None and stop_event.elapsed_seconds >= 0, (
        "Failed event should contain elapsed time"
    )


@pytest.mark.asyncio
async def test_control_loop_waiter_resolution(test_plugin: MockRunAdapter) -> None:
    class Awaited(Event):
        tag: str

    class WaiterWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent, ctx: Context) -> StopEvent:
            print("waiting for event")
            awaited = await ctx.wait_for_event(
                Awaited,
                waiter_event=InputRequiredEvent(),
                requirements={"tag": "go"},
            )
            return StopEvent(result=f"got_{awaited.tag}")

    wf = WaiterWorkflow(timeout=1.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Let first run add the waiter
    async def wait_input_required() -> InputRequiredEvent:
        async for event in test_plugin.stream_published_events():
            if isinstance(event, InputRequiredEvent):
                return event
        raise TimeoutError("InputRequiredEvent not found")

    await asyncio.wait_for(wait_input_required(), timeout=1.0)

    # Send the awaited event that satisfies requirements
    await test_plugin.send_event(TickAddEvent(event=Awaited(tag="go")))

    result = await asyncio.wait_for(task, timeout=2.0)
    assert isinstance(result, StopEvent)
    assert result.result == "got_go"


@pytest.mark.asyncio
async def test_control_loop_input_required_published_to_stream(
    test_plugin: MockRunAdapter,
) -> None:
    """
    Test that InputRequiredEvent is automatically published to the outward stream.

    When a workflow step calls wait_for_event, an InputRequiredEvent should be
    automatically published to the event stream so that external consumers
    (like UIs or monitoring systems) can be notified that the workflow is
    waiting for input.
    """

    class AwaitedEvent(Event):
        value: str

    class WaitingWorkflow(Workflow):
        @step
        async def waiter(self, ev: StartEvent, ctx: Context) -> StopEvent:
            # This should cause an InputRequiredEvent to be published
            awaited = await ctx.wait_for_event(
                AwaitedEvent,
                waiter_event=InputRequiredEvent(),
            )
            return StopEvent(result=f"received_{awaited.value}")

    wf = WaitingWorkflow(timeout=2.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Wait for the InputRequiredEvent to appear in the stream
    input_required_found = False
    while True:
        ev = await test_plugin.get_stream_event(timeout=1.0)
        if isinstance(ev, InputRequiredEvent):
            input_required_found = True
            break
        # Skip StepStateChanged events
        if isinstance(ev, StopEvent):
            break

    assert input_required_found, "InputRequiredEvent should be published to stream"

    # Now send the awaited event to complete the workflow
    await test_plugin.send_event(TickAddEvent(event=AwaitedEvent(value="test_data")))

    # Verify workflow completes successfully
    result = await asyncio.wait_for(task, timeout=1.0)
    assert isinstance(result, StopEvent)
    assert result.result == "received_test_data"


@pytest.mark.asyncio
async def test_control_loop_collect_events_same_type(
    test_plugin: MockRunAdapter,
) -> None:
    wf = CollectWorkflow(timeout=1.0)
    result = await asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    assert isinstance(result, StopEvent)
    assert result.result == "sum_10"


@pytest.mark.asyncio
async def test_control_loop_reruns_stale_collect_events_firing(
    test_plugin: MockRunAdapter,
) -> None:
    class Item(Event):
        n: int

    class Pair(Event):
        nums: list[int]

    seeded = asyncio.Event()
    racing_workers: set[int] = set()
    release_race = asyncio.Event()

    class StaleCollectWorkflow(Workflow):
        @step
        async def start(self, ctx: Context, ev: StartEvent) -> None:
            ctx.send_event(Item(n=1))

        @step(num_workers=2)
        async def collect(self, ctx: Context, ev: Item) -> Pair | None:
            if ev.n in {2, 3}:
                racing_workers.add(ev.n)
                if racing_workers == {2, 3}:
                    release_race.set()
                await release_race.wait()

            events = ctx.collect_events(ev, [Item, Item])
            if events is None:
                if ev.n == 1:
                    seeded.set()
                return None
            return Pair(nums=sorted(e.n for e in events))

        @step(num_workers=1)
        async def finish(self, ctx: Context, ev: Pair) -> StopEvent | None:
            pairs = ctx.collect_events(ev, [Pair, Pair])
            if pairs is None:
                return None
            return StopEvent(result=sorted(pair.nums for pair in pairs))

    task = asyncio.create_task(
        run_control_loop(
            workflow=StaleCollectWorkflow(timeout=5.0),
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    await asyncio.wait_for(seeded.wait(), timeout=1.0)
    await test_plugin.send_event(TickAddEvent(event=Item(n=2)))
    await test_plugin.send_event(TickAddEvent(event=Item(n=3)))
    await test_plugin.send_event(TickAddEvent(event=Item(n=4)))

    result = await asyncio.wait_for(task, timeout=2.0)

    assert result.result == [[1, 2], [3, 4]]


@pytest.mark.asyncio
async def test_control_loop_defers_idle_behind_buffered_tick(
    test_plugin: MockRunAdapter,
) -> None:
    class ContinueEvent(Event):
        value: str

    class BufferedTickWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> None:
            return None

        @step
        async def finish(self, ev: ContinueEvent) -> StopEvent:
            return StopEvent(result=ev.value)

    workflow = BufferedTickWorkflow(timeout=2.0)
    step_workers = {}
    for name, step_func in workflow._get_steps().items():
        unbound = getattr(step_func, "__func__", step_func)
        step_workers[StepId.root(name)] = as_step_worker_function(unbound)

    run_id = str(uuid.uuid4())
    mock_runtime = MockRuntime()
    test_plugin.set_state_store(InMemoryStateStore(DictState()))
    mock_runtime.set_adapter(run_id, test_plugin)
    workflow._runtime = mock_runtime
    with setting_run_id(run_id):
        ctx = Context._create_internal(workflow=workflow)

    state = BrokerState.from_workflow(workflow)
    state.is_running = True
    runner = _ControlLoopRunner(workflow, test_plugin, ctx, step_workers, state)
    runner.tick_buffer = [
        TickIdleCheck(),
        TickAddEvent(event=ContinueEvent(value="done")),
    ]

    with setting_run_id(run_id):
        run_ctx = RunContext(
            workflow=workflow,
            run_adapter=test_plugin,
            context=ctx,
            steps=step_workers,
        )
        with run_context(run_ctx):
            result = await runner.run(start_event=None)

    assert result.result == "done"
    seen: list[Event] = []
    while True:
        ev = await test_plugin.get_stream_event(timeout=1.0)
        seen.append(ev)
        if isinstance(ev, StopEvent):
            break
    assert not any(isinstance(ev, WorkflowIdleEvent) for ev in seen)


@pytest.mark.asyncio
async def test_control_loop_collect_events_multiple_types(
    test_plugin: MockRunAdapter,
) -> None:
    wf = CollectMultipleEventTypesWorkflow(timeout=1.0)
    result = await run_control_loop(
        workflow=wf,
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )
    assert isinstance(result, StopEvent)
    assert result.result == "sum_33"


@pytest.mark.asyncio
async def test_control_loop_stream_events(test_plugin: MockRunAdapter) -> None:
    wf = SimpleWorkflow(timeout=5.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Expect at least one StepStateChanged event while running
    stream_events: list[Event] = []
    while True:
        ev = await test_plugin.get_stream_event(timeout=1.0)
        stream_events.append(ev)
        if isinstance(ev, StopEvent):
            break

    result = await asyncio.wait_for(task, timeout=2.0)
    assert isinstance(result, StopEvent)
    # Ensure at least one StepStateChanged observed
    assert len(stream_events) == 7
    assert [type(x) for x in stream_events] == [StepStateChanged] * 6 + [StopEvent]
    change_events = [x for x in stream_events if isinstance(x, StepStateChanged)]
    assert [x.step_state.name + " - " + x.name for x in change_events] == [
        "RUNNING - start_step",
        "NOT_RUNNING - start_step",
        "RUNNING - middle_step",
        "NOT_RUNNING - middle_step",
        "RUNNING - end_step",
        "NOT_RUNNING - end_step",
    ]


class SomeEvent(HumanResponseEvent):
    pass


@pytest.mark.asyncio
async def test_control_loop_per_step_routing(test_plugin: MockRunAdapter) -> None:
    class RouteWorkflow(Workflow):
        @step
        async def starter(self, ev: StartEvent) -> StopEvent | None:
            return None

        @step
        async def first(self, ev: SomeEvent) -> StopEvent:
            return StopEvent(result="first")

        @step
        async def second(self, ev: SomeEvent) -> StopEvent:
            return StopEvent(result="second")

    wf = RouteWorkflow(timeout=1.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Route explicitly to the 'second' step with an accepted event type
    await test_plugin.send_event(
        TickAddEvent(event=SomeEvent(), step_id=StepId.root("second"))
    )

    result = await asyncio.wait_for(task, timeout=2.0)
    assert isinstance(result, StopEvent)
    assert result.result == "second"


@pytest.mark.asyncio
async def test_control_loop_concurrency_queueing(
    test_plugin: MockRunAdapter,
) -> None:
    class LimitedWorkflow(Workflow):
        @step(num_workers=1)
        async def only_one(self, ev: StartEvent) -> StopEvent:
            # Hold to simulate long work
            await asyncio.sleep(0.01)
            return StopEvent(result="done")

    wf = LimitedWorkflow(timeout=5.0)

    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            test_runtime=test_plugin,
            start_event=None,
        )
    )

    await asyncio.sleep(0)
    # Send two events quickly; with num_workers=1, second should queue (PREPARING)
    await asyncio.gather(
        *[test_plugin.send_event(TickAddEvent(event=StartEvent())) for _ in range(10)]
    )

    # Observe stream for PREPARING signal
    saw_preparing = False
    for _ in range(5):
        ev = await test_plugin.get_stream_event(timeout=1.0)
        if isinstance(ev, StepStateChanged) and ev.step_state.name == "PREPARING":
            saw_preparing = True
            break

    # Drain to completion
    result = await asyncio.wait_for(task, timeout=2.0)
    assert isinstance(result, StopEvent)
    assert saw_preparing


@pytest.mark.asyncio
async def test_control_loop_user_cancellation(test_plugin: MockRunAdapter) -> None:
    """
    Test that user cancellation raises WorkflowCancelledByUser and publishes WorkflowCancelledEvent.

    When a workflow is cancelled, a WorkflowCancelledEvent should be published to the stream
    to inform consumers about the cancellation before the exception is raised.
    """

    class CancelWorkflow(Workflow):
        @step
        async def slow(self, ev: StartEvent) -> StopEvent:
            await asyncio.sleep(1.0)
            return StopEvent(result="never")

    wf = CancelWorkflow(timeout=5.0)

    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            test_runtime=test_plugin,
            start_event=StartEvent(),
        )
    )

    # Cancel the run externally
    await asyncio.sleep(0)
    await test_plugin.send_event(TickCancelRun())

    # Wait for the StopEvent to be published
    stop_event = await wait_for_stop_event(test_plugin)

    # Verify that the cancellation exception is raised
    with pytest.raises(WorkflowCancelledByUser):
        await asyncio.wait_for(task, timeout=1.0)

    # Verify a WorkflowCancelledEvent was published to the stream
    assert stop_event is not None, (
        "Cancellation should publish WorkflowCancelledEvent to stream before raising exception"
    )
    assert isinstance(stop_event, WorkflowCancelledEvent), (
        f"Expected WorkflowCancelledEvent, got {type(stop_event).__name__}"
    )


@pytest.mark.asyncio
async def test_control_loop_retry_with_delay(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
) -> None:
    """Test that retry delay is enforced between attempts."""
    test_plugin, _ = test_plugin_with_time_machine
    retry_delay = 0.02

    class DelayedRetryWorkflow(Workflow):
        attempt_times: list[float] = []

        @step(
            retry_policy=ConstantDelayRetryPolicy(maximum_attempts=3, delay=retry_delay)
        )
        async def flaky(self, ev: StartEvent) -> StopEvent:
            self.attempt_times.append(time.time())
            if len(self.attempt_times) < 3:
                raise RuntimeError(f"fail attempt {len(self.attempt_times)}")
            return StopEvent(result=f"ok_after_{len(self.attempt_times)}_attempts")

    wf = DelayedRetryWorkflow(timeout=5.0)

    result = await run_control_loop(
        workflow=wf,
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )

    assert isinstance(result, StopEvent)
    assert result.result == "ok_after_3_attempts"

    assert len(wf.attempt_times) == 3
    for i in range(1, len(wf.attempt_times)):
        elapsed = wf.attempt_times[i] - wf.attempt_times[i - 1]
        assert elapsed >= retry_delay * 0.8, (
            f"expected >= {retry_delay * 0.8:.3f}s, got {elapsed:.3f}s"
        )
    # Verify time-machine is active (epoch starts at 1000.0)
    assert wf.attempt_times[0] >= 1000.0


@pytest.mark.asyncio
async def test_wakeup_heap_never_holds_work_items(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The wakeup heap holds only contentless, re-derivable alarms.

    A delayed retry must be represented in BrokerState (queued with
    not_before) plus a contentless TickWakeup poke — never as a
    payload-carrying TickAddEvent in the heap, which a snapshot cannot see.
    """
    test_plugin, _ = test_plugin_with_time_machine
    scheduled: list[WorkflowTick] = []
    original_schedule_tick = _ControlLoopRunner.schedule_tick

    def recording_schedule_tick(
        self: _ControlLoopRunner, tick: WorkflowTick, at_time: float
    ) -> None:
        scheduled.append(tick)
        original_schedule_tick(self, tick, at_time)

    monkeypatch.setattr(_ControlLoopRunner, "schedule_tick", recording_schedule_tick)

    class RetryingWorkflow(Workflow):
        attempts = 0

        @step(retry_policy=ConstantDelayRetryPolicy(maximum_attempts=3, delay=0.02))
        async def flaky(self, ev: StartEvent) -> StopEvent:
            RetryingWorkflow.attempts += 1
            if RetryingWorkflow.attempts < 2:
                raise RuntimeError("fail once")
            return StopEvent(result="ok")

    result = await run_control_loop(
        workflow=RetryingWorkflow(timeout=5.0),
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )

    assert isinstance(result, StopEvent)
    assert any(isinstance(t, TickWakeup) for t in scheduled)
    assert not any(isinstance(t, TickAddEvent) for t in scheduled)


@pytest.mark.asyncio
async def test_control_loop_retry_gives_up_after_max_attempts(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
) -> None:
    """Test that workflow fails after exhausting maximum_attempts."""
    test_plugin, _ = test_plugin_with_time_machine
    max_attempts = 3

    class AlwaysFailsWorkflow(Workflow):
        attempt_count = 0

        @step(
            retry_policy=ConstantDelayRetryPolicy(
                maximum_attempts=max_attempts, delay=0.01
            )
        )
        async def always_fails(self, ev: StartEvent) -> StopEvent:
            self.attempt_count += 1
            raise ValueError(f"fail #{self.attempt_count}")

    wf = AlwaysFailsWorkflow(timeout=5.0)

    with pytest.raises(ValueError, match="fail #3"):
        await run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )

    assert wf.attempt_count == max_attempts


@pytest.mark.asyncio
async def test_control_loop_retry_exhaustion_respects_total_time(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
) -> None:
    """Test that retry policy receives correct elapsed_time across retries."""
    test_plugin, _ = test_plugin_with_time_machine
    retry_delay = 0.01

    class ElapsedTimeTrackingPolicy(RetryPolicy):
        def __init__(self, retry_delay: float) -> None:
            self.retry_delay = retry_delay
            self.observed_elapsed_times: list[float] = []
            self.observed_attempts: list[int] = []

        def next(
            self,
            elapsed_time: float,
            attempts: int,
            error: BaseException,
            *,
            seed: int | None = None,
        ) -> float | None:
            self.observed_elapsed_times.append(elapsed_time)
            self.observed_attempts.append(attempts)
            return self.retry_delay

    policy: ElapsedTimeTrackingPolicy = ElapsedTimeTrackingPolicy(
        retry_delay=retry_delay
    )

    class TrackedRetryWorkflow(Workflow):
        total_calls = 0

        @step(retry_policy=policy)
        async def always_fail(self, ev: StartEvent) -> StopEvent:
            self.total_calls += 1
            if self.total_calls < 5:
                raise RuntimeError(f"fail {self.total_calls}")
            return StopEvent(result="eventually_ok")

    wf = TrackedRetryWorkflow(timeout=5.0)

    result = await run_control_loop(
        workflow=wf,
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )

    assert isinstance(result, StopEvent)
    assert result.result == "eventually_ok"

    # Verify we had the expected number of failures (4 failures, 5th succeeds)
    assert len(policy.observed_elapsed_times) == 4, (
        f"Expected 4 retry attempts (failures), got {len(policy.observed_elapsed_times)}"
    )

    # Elapsed times should be strictly increasing (not reset to 0)
    # If first_attempt_at was being reset on each retry, all elapsed times would be ~0
    for i in range(1, len(policy.observed_elapsed_times)):
        assert (
            policy.observed_elapsed_times[i] > policy.observed_elapsed_times[i - 1]
        ), f"Elapsed time should increase: {policy.observed_elapsed_times}"

    # Elapsed times should grow: ~0, ~retry_delay, ~2*retry_delay, ...
    for i, elapsed in enumerate(policy.observed_elapsed_times):
        min_expected = retry_delay * i * 0.8
        assert elapsed >= min_expected, (
            f"elapsed[{i}]: expected >= {min_expected:.4f}, got {elapsed:.4f}"
        )

    # Attempts should increment: 1, 2, 3, 4
    expected_attempts = list(range(1, len(policy.observed_attempts) + 1))
    assert policy.observed_attempts == expected_attempts


@pytest.mark.asyncio
async def test_control_loop_stop_before_delay_uses_upcoming_sleep(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
) -> None:
    """Test that stop_before_delay stops before the next sleep crosses the limit."""
    test_plugin, _ = test_plugin_with_time_machine
    retry_delay = 0.2

    class AlwaysFailsWorkflow(Workflow):
        attempt_count = 0

        @step(
            retry_policy=retry_policy(
                wait=wait_fixed(retry_delay),
                stop=stop_before_delay(0.6),
            )
        )
        async def always_fails(self, ev: StartEvent) -> StopEvent:
            self.attempt_count += 1
            raise ValueError(f"fail #{self.attempt_count}")

    wf = AlwaysFailsWorkflow(timeout=5.0)

    with pytest.raises(ValueError, match="fail #3"):
        await run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )

    assert wf.attempt_count == 3


@pytest.mark.asyncio
async def test_control_loop_emits_idle_event_when_waiting(
    test_plugin: MockRunAdapter,
) -> None:
    """
    Test that WorkflowIdleEvent is emitted when workflow becomes idle.

    A workflow is idle when all steps have empty queues and no in-progress
    workers. This uses a two-step pattern: the first step completes (leaving
    state idle), and the second step accepts an external event to finish.
    """

    class ExternalEvent(HumanResponseEvent):
        value: str

    class IdleTrackingWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> None:
            pass

        @step
        async def finish(self, ev: ExternalEvent) -> StopEvent:
            return StopEvent(result=f"received_{ev.value}")

    wf = IdleTrackingWorkflow(timeout=2.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Collect events until we see the WorkflowIdleEvent
    idle_event_found = False

    while True:
        ev = await test_plugin.get_stream_event(timeout=1.0)
        if isinstance(ev, WorkflowIdleEvent):
            idle_event_found = True
            break
        if isinstance(ev, StopEvent):
            break

    assert idle_event_found, (
        "WorkflowIdleEvent should be emitted when workflow has no pending work"
    )

    # Now send the external event to complete the workflow
    await test_plugin.send_event(TickAddEvent(event=ExternalEvent(value="test")))

    result = await asyncio.wait_for(task, timeout=1.0)
    assert isinstance(result, StopEvent)
    assert result.result == "received_test"


@pytest.mark.asyncio
async def test_control_loop_emits_idle_event_with_wait_for_event(
    test_plugin: MockRunAdapter,
) -> None:
    """WorkflowIdleEvent fires when a step uses ctx.wait_for_event().

    wait_for_event raises an internal exception that registers a waiter and
    releases the worker. After that, the state has no queued events and no
    in-progress workers, so the workflow is idle.
    """

    class AwaitedEvent(Event):
        value: str

    class WaitForEventWorkflow(Workflow):
        @step
        async def waiter(self, ev: StartEvent, ctx: Context) -> StopEvent:
            awaited = await ctx.wait_for_event(
                AwaitedEvent,
                waiter_event=InputRequiredEvent(),
            )
            return StopEvent(result=f"received_{awaited.value}")

    wf = WaitForEventWorkflow(timeout=2.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    idle_event_found = False
    input_required_found = False

    while True:
        ev = await test_plugin.get_stream_event(timeout=1.0)
        if isinstance(ev, WorkflowIdleEvent):
            idle_event_found = True
            break
        if isinstance(ev, InputRequiredEvent):
            input_required_found = True
        if isinstance(ev, StopEvent):
            break

    assert idle_event_found, (
        "WorkflowIdleEvent should be emitted when workflow is waiting for external event"
    )
    assert input_required_found, "InputRequiredEvent should be emitted before idle"

    # Send the awaited event to complete the workflow
    await test_plugin.send_event(TickAddEvent(event=AwaitedEvent(value="test")))

    result = await asyncio.wait_for(task, timeout=1.0)
    assert isinstance(result, StopEvent)
    assert result.result == "received_test"


@pytest.mark.asyncio
async def test_control_loop_idle_event_not_emitted_on_completion(
    test_plugin: MockRunAdapter,
) -> None:
    """
    Test that WorkflowIdleEvent is NOT emitted when workflow completes normally.

    Even if a workflow has waiters, if it completes (StopEvent), it should not
    emit an idle event because the workflow is no longer running.
    """

    result = await run_control_loop(
        workflow=SimpleWorkflow(timeout=1.0),
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )

    # Verify the workflow completed
    assert isinstance(result, StopEvent)
    assert result.result == "processed_42"

    # Drain and verify no WorkflowIdleEvent was emitted
    all_events: list[Event] = []
    while test_plugin.has_stream_events():
        ev = await test_plugin.get_stream_event(timeout=0.1)
        all_events.append(ev)

    idle_events = [e for e in all_events if isinstance(e, WorkflowIdleEvent)]
    assert len(idle_events) == 0, (
        "WorkflowIdleEvent should not be emitted when workflow completes normally"
    )


@pytest.mark.asyncio
async def test_simultaneous_retries_with_same_delay(
    test_plugin_with_time_machine: tuple[MockRunAdapter, time_machine.Coordinates],
) -> None:
    """
    Test that the control loop handles multiple retries scheduled at the same timestamp.

    When two steps both fail and have the same retry delay, they get scheduled
    at exactly the same timestamp. Without a sequence counter tiebreaker in the
    heap, Python's heapq would compare WorkflowTick objects directly, causing
    TypeError since they don't implement __lt__.

    This test uses a CoarseTimeAdapter that rounds timestamps to 1-second precision,
    ensuring that retries scheduled within the same second will collide.
    """
    base_plugin, traveller = test_plugin_with_time_machine

    class CoarseTimeAdapter(MockRunAdapter):
        """Adapter that rounds get_now() to 1-second precision to force collisions."""

        async def get_now(self) -> float:
            # Round to nearest second to force timestamp collisions
            return float(int(time.time()))

    test_plugin = CoarseTimeAdapter(run_id="test", traveller=traveller)
    test_plugin.set_state_store(InMemoryStateStore(DictState()))

    # Use a delay that's less than 1 second so both retries land on same rounded second
    retry_delay = 0.01

    class ResultA(Event):
        pass

    class ResultB(Event):
        pass

    class TwoStepsFailOnceWorkflow(Workflow):
        step_a_attempts = 0
        step_b_attempts = 0

        @step(
            retry_policy=ConstantDelayRetryPolicy(maximum_attempts=2, delay=retry_delay)
        )
        async def step_a(self, ev: StartEvent) -> ResultA:
            self.step_a_attempts += 1
            if self.step_a_attempts == 1:
                raise RuntimeError("step_a fails once")
            return ResultA()

        @step(
            retry_policy=ConstantDelayRetryPolicy(maximum_attempts=2, delay=retry_delay)
        )
        async def step_b(self, ev: StartEvent) -> ResultB:
            self.step_b_attempts += 1
            if self.step_b_attempts == 1:
                raise RuntimeError("step_b fails once")
            return ResultB()

        @step
        async def collector(
            self, ev: ResultA | ResultB, ctx: Context
        ) -> StopEvent | None:
            events = ctx.collect_events(ev, [ResultA, ResultB])
            if events is None:
                return None
            return StopEvent(result="both_succeeded")

    wf = TwoStepsFailOnceWorkflow(timeout=5.0)

    result = await run_control_loop(
        workflow=wf,
        start_event=StartEvent(),
        test_runtime=test_plugin,
    )

    assert isinstance(result, StopEvent)
    assert result.result == "both_succeeded"
    assert wf.step_a_attempts == 2
    assert wf.step_b_attempts == 2


@pytest.mark.asyncio
async def test_external_event_not_double_routed_when_waiter_exists(
    test_plugin: MockRunAdapter,
) -> None:
    """Regression test: an external event that resolves a wait_for_event waiter
    should NOT also be routed to another step that accepts the same event type.

    Before the fix, the accepting step would run twice — once from normal
    routing and once from the waiter waking up and re-emitting the event.
    """

    class ExternalInput(Event):
        value: str

    step_run_count = 0

    class DoubleRouteWorkflow(Workflow):
        @step
        async def kickoff(self, ev: StartEvent) -> ExternalInput:
            return ExternalInput(value="init")

        @step
        async def handle_input(self, ev: ExternalInput, ctx: Context) -> StopEvent:
            # This step accepts ExternalInput AND waits for ExternalInput.
            # wait_for_event works by raising an exception on first call,
            # then the control loop re-runs the step after the waiter resolves.
            # So this step runs twice normally: once to register the waiter,
            # once after resolution. The bug caused a THIRD run via normal
            # event routing of the external event to this step.
            nonlocal step_run_count
            step_run_count += 1
            result = await ctx.wait_for_event(
                ExternalInput,
                waiter_event=InputRequiredEvent(),
            )
            return StopEvent(result=f"got_{result.value}")

    wf = DoubleRouteWorkflow(timeout=2.0)
    task = asyncio.create_task(
        run_control_loop(
            workflow=wf,
            start_event=StartEvent(),
            test_runtime=test_plugin,
        )
    )

    # Wait for the waiter to be registered
    async for event in test_plugin.stream_published_events():
        if isinstance(event, InputRequiredEvent):
            break

    # Send the external event — this resolves the waiter on handle_input.
    # Without the fix, handle_input would ALSO get ExternalInput via normal
    # accepted_events routing, causing a second execution.
    await test_plugin.send_event(TickAddEvent(event=ExternalInput(value="hello")))

    result = await asyncio.wait_for(task, timeout=2.0)
    assert isinstance(result, StopEvent)
    assert result.result == "got_hello", (
        f"Expected waiter resolution result, got '{result.result}'"
    )
    # Step runs twice: once to register the waiter (raises WaitingForEvent),
    # once after waiter resolution (returns the result). Without the fix,
    # it would run a third time from the external event being routed directly.
    assert step_run_count == 2, (
        f"handle_input should run exactly twice, but ran {step_run_count} times"
    )
