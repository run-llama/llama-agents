# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Re-delivery of collect-step invocations.

A collect-step invocation is an internal ``CollectionReleaseEvent`` carrying
the released batch, never a member event. These tests pin the consequences:
retried collect steps re-fire with the same batch (top-level and nested),
``ctx.wait_for_event`` works inside fan-out branches and inside collect steps
(the release payload survives waiter suspension), waiters for a member type
are not spuriously resolved by a release carrying that type, and
``@catch_error`` handlers see the real batch on ``StepFailedEvent.input_event``.
"""

from __future__ import annotations

import asyncio

from workflows import Context, Workflow, catch_error, step
from workflows.events import (
    CollectionReleaseEvent,
    Event,
    StartEvent,
    StepFailedEvent,
    StopEvent,
)
from workflows.retry_policy import retry_policy, stop_after_attempt, wait_fixed

_RETRY = retry_policy(wait=wait_fixed(0.05), stop=stop_after_attempt(3))


class Task(Event):
    n: int


class Done(Event):
    n: int


class InnerTask(Event):
    outer: int
    inner: int


class InnerDone(Event):
    outer: int
    inner: int


class InnerSummary(Event):
    outer: int
    total: int


class Approval(Event):
    pass


class Waiting(Event):
    pass


async def _run(wf: Workflow, timeout: float = 8.0) -> object:
    """Run a workflow to completion, draining its event stream first."""
    handler = wf.run()
    async for _ in handler.stream_events():
        pass
    return await asyncio.wait_for(handler, timeout=timeout)


async def _run_sending_on_waiting(
    wf: Workflow, to_send: Event, timeout: float = 8.0
) -> object:
    """Run ``wf``; when a ``Waiting`` marker hits the stream, send ``to_send``."""
    handler = wf.run()
    sent = False
    async for ev in handler.stream_events():
        if isinstance(ev, Waiting) and not sent:
            sent = True
            handler.ctx.send_event(to_send)
    assert sent, "workflow never emitted the Waiting marker"
    return await asyncio.wait_for(handler, timeout=timeout)


# ---------------------------------------------------------------------------
# Retry policies on collect steps: the retried invocation re-fires with the
# same released batch.
# ---------------------------------------------------------------------------


async def test_collect_step_retry_refires_with_same_batch() -> None:
    calls: list[list[int]] = []

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(4)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step(retry_policy=_RETRY)
        async def join(self, events: list[Done]) -> StopEvent:
            calls.append(sorted(e.n for e in events))
            if len(calls) == 1:
                raise RuntimeError("transient join failure")
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run(WF(timeout=8))
    assert result == [0, 1, 2, 3]
    assert calls == [[0, 1, 2, 3], [0, 1, 2, 3]]


async def test_nested_collect_step_retry_refires_with_same_batch() -> None:
    inner_calls: list[list[tuple[int, int]]] = []

    class Nested(Workflow):
        @step
        async def outer(self, ev: StartEvent) -> list[Task]:
            return [Task(n=o) for o in range(3)]

        @step
        async def inner(self, ev: Task) -> list[InnerTask]:
            return [InnerTask(outer=ev.n, inner=i) for i in range(2)]

        @step
        async def inner_work(self, ev: InnerTask) -> InnerDone:
            return InnerDone(outer=ev.outer, inner=ev.inner)

        @step(retry_policy=_RETRY)
        async def inner_join(self, events: list[InnerDone]) -> InnerSummary:
            inner_calls.append(sorted((e.outer, e.inner) for e in events))
            if len(inner_calls) == 1:
                raise RuntimeError("transient inner join failure")
            return InnerSummary(outer=events[0].outer, total=len(events))

        @step
        async def outer_join(self, events: list[InnerSummary]) -> StopEvent:
            return StopEvent(result=sorted((s.outer, s.total) for s in events))

    result = await _run(Nested(timeout=10), timeout=10)
    assert result == [(0, 2), (1, 2), (2, 2)]
    # Three inner streams plus exactly one retry; the retried invocation saw
    # the same batch as the failed first attempt.
    assert len(inner_calls) == 4, inner_calls
    assert inner_calls.count(inner_calls[0]) == 2, inner_calls


# ---------------------------------------------------------------------------
# ctx.wait_for_event inside fan-out scope: the suspended work item resumes
# whole, keeping its stream scope and (for collect steps) the release payload.
# ---------------------------------------------------------------------------


async def test_wait_for_event_in_fan_out_branch_resumes_member() -> None:
    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(2)]

        @step
        async def work(self, ctx: Context, ev: Task) -> Done:
            if ev.n == 0:
                await ctx.wait_for_event(
                    Approval, waiter_event=Waiting(), waiter_id="branch", timeout=3
                )
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run_sending_on_waiting(WF(timeout=8), Approval())
    assert result == [0, 1]


async def test_wait_for_event_inside_collect_step_keeps_batch() -> None:
    batches: list[list[int]] = []

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, ctx: Context, events: list[Done]) -> StopEvent:
            await ctx.wait_for_event(
                Approval, waiter_event=Waiting(), waiter_id="join-gate", timeout=3
            )
            batches.append(sorted(e.n for e in events))
            return StopEvent(result=sorted(e.n for e in events))

    result = await _run_sending_on_waiting(WF(timeout=8), Approval())
    assert result == [0, 1, 2]
    # The step replays once after suspension, with the original full batch.
    assert batches == [[0, 1, 2]]


async def test_waiter_for_member_type_not_resolved_by_collect_redelivery() -> None:
    """A pending waiter for type T must not resolve when a collect invocation
    carrying T members is re-delivered (retry). Only a genuine external T does.

    The watcher starts waiting only after the stream's real Done members have
    already been collected, so the only Done-shaped traffic during the wait is
    the retried release — which must stay a CollectionReleaseEvent, never a
    member event.
    """

    class StartWatch(Event):
        pass

    batches: list[list[int]] = []
    resolved: list[int] = []

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(2)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step(retry_policy=_RETRY)
        async def join(self, ctx: Context, events: list[Done]) -> StartWatch | None:
            batches.append(sorted(e.n for e in events))
            if len(batches) == 1:
                ctx.send_event(StartWatch())
                raise RuntimeError("transient join failure")
            return None

        @step
        async def watcher(self, ctx: Context, ev: StartWatch) -> StopEvent:
            done = await ctx.wait_for_event(Done, timeout=5)
            resolved.append(done.n)
            return StopEvent(result=done.n)

    handler = WF(timeout=10).run()
    # Wait for the retried join invocation to have run with the same batch.
    for _ in range(300):
        if len(batches) == 2:
            break
        await asyncio.sleep(0.02)
    assert batches == [[0, 1], [0, 1]], batches
    # Give any spurious waiter resolution time to surface, then confirm the
    # re-delivered release did not resolve the Done waiter.
    await asyncio.sleep(0.2)
    assert resolved == []

    # A genuine externally-sent Done resolves it.
    handler.ctx.send_event(Done(n=777))
    async for _ in handler.stream_events():
        pass
    result = await asyncio.wait_for(handler, timeout=8)
    assert result == 777
    assert resolved == [777]


# ---------------------------------------------------------------------------
# catch_error on a collect step sees the real batch via CollectionReleaseEvent.
# ---------------------------------------------------------------------------


async def test_catch_error_on_collect_step_receives_release_event() -> None:
    captured: dict[str, StepFailedEvent] = {}

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            raise RuntimeError("join exploded")

        @catch_error(for_steps=["join"])
        async def recover(self, ev: StepFailedEvent) -> StopEvent:
            captured["ev"] = ev
            return StopEvent(result="recovered")

    result = await _run(WF(timeout=8))
    assert result == "recovered"
    failed = captured["ev"]
    assert failed.step_name == "join"
    assert isinstance(failed.input_event, CollectionReleaseEvent)
    assert all(isinstance(e, Done) for e in failed.input_event.events)
    assert sorted(e.n for e in failed.input_event.events) == [0, 1, 2]


async def test_catch_error_on_empty_release_sees_empty_batch() -> None:
    captured: dict[str, StepFailedEvent] = {}

    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return []

        @step
        async def work(self, ev: Task) -> Done:
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            raise RuntimeError("join exploded on empty batch")

        @catch_error(for_steps=["join"])
        async def recover(self, ev: StepFailedEvent) -> StopEvent:
            captured["ev"] = ev
            return StopEvent(result="recovered-empty")

    result = await _run(WF(timeout=8))
    assert result == "recovered-empty"
    failed = captured["ev"]
    assert isinstance(failed.input_event, CollectionReleaseEvent)
    assert failed.input_event.events == []
