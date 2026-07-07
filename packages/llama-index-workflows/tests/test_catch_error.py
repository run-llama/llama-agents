# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pytest
from workflows import (
    Context,
    Workflow,
    catch_error,
    step,
)
from workflows.context.serializers import JsonSerializer
from workflows.errors import (
    ContextStateError,
    WorkflowRuntimeError,
    WorkflowTimeoutError,
    WorkflowValidationError,
)
from workflows.events import (
    Event,
    StartEvent,
    StepFailedEvent,
    StopEvent,
    WorkflowFailedEvent,
)
from workflows.retry_policy import (
    RetryInfo,
    retry_always,
    retry_policy,
    stop_after_attempt,
    wait_fixed,
)
from workflows.runtime.types.internal_state import BrokerState, EventAttempt
from workflows.runtime.types.step_id import StepId


def _retry(attempts: int) -> Any:
    return retry_policy(
        retry=retry_always(),
        wait=wait_fixed(0),
        stop=stop_after_attempt(attempts),
    )


class _InputStart(StartEvent):
    query: str = "hello"


# ---------------------------------------------------------------------------
# retry_info()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_info_defaults_on_first_attempt() -> None:
    captured: dict[str, RetryInfo] = {}

    class Flow(Workflow):
        @step
        async def first(self, ctx: Context, ev: StartEvent) -> StopEvent:
            captured["info"] = ctx.retry_info()
            return StopEvent(result="ok")

    await Flow(timeout=5).run()
    info = captured["info"]
    assert info.retry_number == 0
    assert info.elapsed_seconds == 0.0
    assert info.last_exception is None
    assert info.last_failed_at is None


@pytest.mark.asyncio
async def test_retry_info_after_failure_populated() -> None:
    observed: list[RetryInfo] = []

    class Flow(Workflow):
        @step(retry_policy=_retry(3))
        async def flaky(self, ctx: Context, ev: StartEvent) -> StopEvent:
            info = ctx.retry_info()
            observed.append(info)
            if info.retry_number < 1:
                raise ValueError("boom")
            return StopEvent(result="ok")

    result = await Flow(timeout=5).run()
    assert result == "ok"
    assert observed[0].retry_number == 0
    assert observed[0].last_exception is None
    assert observed[0].last_failed_at is None
    assert observed[1].retry_number == 1
    assert observed[1].elapsed_seconds >= 0.0
    assert isinstance(observed[1].last_exception, ValueError)
    assert str(observed[1].last_exception) == "boom"
    assert isinstance(observed[1].last_failed_at, datetime)
    assert observed[1].last_failed_at.tzinfo is not None


def test_retry_info_outside_step_raises() -> None:
    class Flow(Workflow):
        @step
        async def a(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="ok")

    ctx: Context = Context(Flow())
    with pytest.raises((WorkflowRuntimeError, ContextStateError)):
        ctx.retry_info()


def test_last_exception_serialization_roundtrip() -> None:
    class Flow(Workflow):
        @step
        async def a(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="ok")

    wf = Flow()
    state = BrokerState.from_workflow(wf)
    state.workers[StepId.root("a")].queue.append(
        EventAttempt(
            event=StartEvent(),
            attempts=1,
            first_attempt_at=100.0,
            last_exception=ValueError("boom"),
            last_failed_at=123.456,
        )
    )
    serialized = state.to_serialized(JsonSerializer())
    restored = BrokerState.from_serialized(serialized, wf, JsonSerializer())
    restored_attempt = restored.workers[StepId.root("a")].queue[0]
    assert isinstance(restored_attempt.last_exception, ValueError)
    assert str(restored_attempt.last_exception) == "boom"
    assert restored_attempt.last_failed_at == 123.456


# ---------------------------------------------------------------------------
# Handler-decorator validation
# ---------------------------------------------------------------------------


def test_catch_error_wrong_event_type_invalid() -> None:
    with pytest.raises(WorkflowValidationError, match="must accept StepFailedEvent"):

        @catch_error
        async def bad_handler(self: Any, ctx: Context, ev: StartEvent) -> StopEvent:
            return StopEvent()


# ---------------------------------------------------------------------------
# Runtime routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catch_error_returning_stop_completes_workflow() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def flaky(self, ev: StartEvent) -> StopEvent:
            raise RuntimeError("transient")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result={"recovered_from": ev.step_name})

    handler = Flow(timeout=5).run()
    events: list[Event] = []
    async for ev in handler.stream_events():
        events.append(ev)
    result = await handler
    assert result == {"recovered_from": "flaky"}
    assert not any(isinstance(ev, WorkflowFailedEvent) for ev in events)


@pytest.mark.asyncio
async def test_catch_error_raising_fails_workflow() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def flaky(self, ev: StartEvent) -> StopEvent:
            raise RuntimeError("primary")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            raise ValueError("handler-failed")

    handler_run = Flow(timeout=5).run()
    events: list[Event] = []
    async for ev in handler_run.stream_events():
        events.append(ev)
    with pytest.raises(ValueError, match="handler-failed"):
        await handler_run
    failed_events = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert len(failed_events) == 1
    assert failed_events[0].step_name == "handler"
    assert str(failed_events[0].exception) == "handler-failed"


@pytest.mark.asyncio
async def test_catch_error_not_invoked_on_recoverable_retry() -> None:
    handler_invoked: list[bool] = []

    class Flow(Workflow):
        attempts = 0

        @step(retry_policy=_retry(3))
        async def flaky(self, ev: StartEvent) -> StopEvent:
            self.attempts += 1
            if self.attempts < 2:
                raise ValueError("transient")
            return StopEvent(result="recovered")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            handler_invoked.append(True)
            return StopEvent(result="caught")

    result = await Flow(timeout=5).run()
    assert result == "recovered"
    assert handler_invoked == []


@pytest.mark.asyncio
async def test_step_failed_event_fields() -> None:
    captured: dict[str, StepFailedEvent] = {}

    class Flow(Workflow):
        @step(retry_policy=_retry(2))
        async def flaky(self, ev: _InputStart) -> StopEvent:
            raise RuntimeError(f"bad:{ev.query}")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            captured["ev"] = ev
            return StopEvent(result="caught")

    await Flow(timeout=5).run(start_event=_InputStart(query="hello"))
    ev = captured["ev"]
    assert ev.step_name == "flaky"
    assert isinstance(ev.input_event, _InputStart)
    assert ev.input_event.query == "hello"
    assert isinstance(ev.exception, RuntimeError)
    assert str(ev.exception) == "bad:hello"
    assert ev.attempts == 2
    assert ev.elapsed_seconds >= 0.0
    assert isinstance(ev.failed_at, datetime)
    assert ev.failed_at.tzinfo is not None


@pytest.mark.asyncio
async def test_catch_error_not_invoked_on_timeout() -> None:
    handler_invoked: list[bool] = []

    class Flow(Workflow):
        @step
        async def slow(self, ev: StartEvent) -> StopEvent:
            await asyncio.sleep(5)
            return StopEvent(result="unreachable")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            handler_invoked.append(True)
            return StopEvent(result="caught")

    with pytest.raises(WorkflowTimeoutError):
        await Flow(timeout=0.1).run()
    assert handler_invoked == []


@pytest.mark.asyncio
async def test_baseline_without_catch_error_still_fails() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def flaky(self, ev: StartEvent) -> StopEvent:
            raise ValueError("boom")

    handler = Flow(timeout=5).run()
    events: list[Event] = []
    async for ev in handler.stream_events():
        events.append(ev)
    with pytest.raises(ValueError, match="boom"):
        await handler
    assert any(isinstance(ev, WorkflowFailedEvent) for ev in events)


@pytest.mark.asyncio
async def test_catch_error_can_read_context_state() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def flaky(self, ctx: Context, ev: StartEvent) -> StopEvent:
            await ctx.store.set("progress", "halfway")
            raise RuntimeError("boom")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            progress = await ctx.store.get("progress", default="unset")
            return StopEvent(result={"progress": progress, "step": ev.step_name})

    result = await Flow(timeout=5).run()
    assert result == {"progress": "halfway", "step": "flaky"}


# ---------------------------------------------------------------------------
# Scoping: for_steps + wildcard interaction
# ---------------------------------------------------------------------------


class _BEvent(Event):
    pass


@pytest.mark.asyncio
async def test_scoped_handler_catches_listed_step() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def a(self, ev: StartEvent) -> _BEvent:
            raise RuntimeError("a failed")

        @step
        async def b(self, ev: _BEvent) -> StopEvent:
            return StopEvent(result="b-ran")

        @catch_error(for_steps=["a"])
        async def handle_a(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result={"caught": ev.step_name})

    result = await Flow(timeout=5).run()
    assert result == {"caught": "a"}


@pytest.mark.asyncio
async def test_scoped_handler_does_not_catch_other_step() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def a(self, ev: StartEvent) -> _BEvent:
            return _BEvent()

        @step(retry_policy=_retry(1))
        async def b(self, ev: _BEvent) -> StopEvent:
            raise ValueError("b failed")

        @catch_error(for_steps=["a"])
        async def handle_a(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result="caught")

    handler_run = Flow(timeout=5).run()
    with pytest.raises(ValueError, match="b failed"):
        await handler_run


class _AFailedMarker(Event):
    pass


class _BFailedMarker(Event):
    pass


@pytest.mark.asyncio
async def test_multiple_scoped_handlers_each_own_step() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def a(self, ev: StartEvent) -> _BEvent:
            raise RuntimeError("a")

        @step(retry_policy=_retry(1))
        async def b(self, ev: _BEvent) -> StopEvent:
            raise RuntimeError("b")

        @catch_error(for_steps=["a"])
        async def handle_a(self, ctx: Context, ev: StepFailedEvent) -> _AFailedMarker:
            return _AFailedMarker()

        @catch_error(for_steps=["b"])
        async def handle_b(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result={"recovered": ev.step_name})

        @step
        async def finish_a(self, ev: _AFailedMarker) -> _BEvent:
            return _BEvent()

    result = await Flow(timeout=5).run()
    assert result == {"recovered": "b"}


@pytest.mark.asyncio
async def test_scoped_and_wildcard_mix() -> None:
    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def a(self, ev: StartEvent) -> _BEvent:
            raise RuntimeError("a-fail")

        @step(retry_policy=_retry(1))
        async def b(self, ev: _BEvent) -> StopEvent:
            raise RuntimeError("b-fail")

        @catch_error(for_steps=["a"])
        async def handle_a(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result={"via": "scoped", "step": ev.step_name})

        @catch_error
        async def wildcard(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result={"via": "wildcard", "step": ev.step_name})

    result = await Flow(timeout=5).run()
    assert result == {"via": "scoped", "step": "a"}


# ---------------------------------------------------------------------------
# max_recoveries budget
# ---------------------------------------------------------------------------


class _RetryEvent(Event):
    pass


@pytest.mark.asyncio
async def test_max_recoveries_default_fails_on_second_entry() -> None:
    calls: list[str] = []

    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def a(self, ev: StartEvent) -> StopEvent:
            calls.append("a")
            raise RuntimeError("a-fail")

        @catch_error(for_steps=["a"])
        async def handle_a(self, ctx: Context, ev: StepFailedEvent) -> _RetryEvent:
            calls.append("handle_a")
            return _RetryEvent()

        @step(retry_policy=_retry(1))
        async def retry_a(self, ev: _RetryEvent) -> StopEvent:
            calls.append("retry_a")
            raise RuntimeError("retry_a-fail")

    with pytest.raises(RuntimeError):
        await Flow(timeout=5).run()
    # a runs once, handle_a runs once (count=1, <= 1 allowed), retry_a runs
    # once and fails. On its failure, handle_a would be entered a 2nd time
    # (count would be 2, > 1) → workflow fails instead.
    assert calls.count("handle_a") == 1


@pytest.mark.asyncio
async def test_max_recoveries_two_allows_second_entry() -> None:
    calls: list[str] = []

    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def a(self, ev: StartEvent) -> StopEvent:
            calls.append("a")
            raise RuntimeError("a-fail")

        @catch_error(for_steps=["a", "retry_a"], max_recoveries=2)
        async def handle_a(self, ctx: Context, ev: StepFailedEvent) -> _RetryEvent:
            calls.append("handle_a")
            if len([c for c in calls if c == "handle_a"]) >= 2:
                # stop re-trying on the 2nd invocation
                raise RuntimeError("giving up")
            return _RetryEvent()

        @step(retry_policy=_retry(1))
        async def retry_a(self, ev: _RetryEvent) -> StopEvent:
            calls.append("retry_a")
            raise RuntimeError("retry_a-fail")

    with pytest.raises(RuntimeError):
        await Flow(timeout=5).run()
    assert calls.count("handle_a") == 2


# ---------------------------------------------------------------------------
# Handler's own failure falls through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_own_failure_falls_through() -> None:
    events: list[Event] = []

    class Flow(Workflow):
        @step(retry_policy=_retry(1))
        async def a(self, ev: StartEvent) -> StopEvent:
            raise RuntimeError("a-fail")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            raise ValueError("handler-fail")

    handler_run = Flow(timeout=5).run()
    async for ev in handler_run.stream_events():
        events.append(ev)
    with pytest.raises(ValueError, match="handler-fail"):
        await handler_run
    failed = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert len(failed) == 1
    assert failed[0].step_name == "handler"


# ---------------------------------------------------------------------------
# recovery_counts serialization round-trip
# ---------------------------------------------------------------------------


def test_recovery_counts_serialization_roundtrip() -> None:
    class Flow(Workflow):
        @step
        async def a(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="ok")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result="caught")

    wf = Flow()
    wf._validate()
    state = BrokerState.from_workflow(wf)
    state.workers[StepId.root("a")].queue.append(
        EventAttempt(
            event=StartEvent(),
            attempts=1,
            first_attempt_at=100.0,
            recovery_counts={"handler": 1},
        )
    )
    serialized = state.to_serialized(JsonSerializer())
    restored = BrokerState.from_serialized(serialized, wf, JsonSerializer())
    restored_attempt = restored.workers[StepId.root("a")].queue[0]
    assert restored_attempt.recovery_counts == {"handler": 1}
