# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Re-delivery of collect-step invocations.

A collect-step invocation is an internal ``CollectionReleaseEvent`` carrying
the released batch, never a member event. The re-delivery kind x scope grid
lives in test_collection_stream_redelivery_matrix.py; this file keeps the two
cases outside that grid: a waiter for a member type is not spuriously resolved
by a re-delivered release carrying that type, and ``@catch_error`` on an empty
release sees the real (empty) batch on ``StepFailedEvent.input_event``.
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


async def _run(wf: Workflow, timeout: float = 8.0) -> object:
    """Run a workflow to completion, draining its event stream first."""
    handler = wf.run()
    async for _ in handler.stream_events():
        pass
    return await asyncio.wait_for(handler, timeout=timeout)


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
# catch_error on a collect step whose stream released EMPTY: the handler sees
# the real empty batch (the matrix covers the non-empty release cell).
# ---------------------------------------------------------------------------


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
