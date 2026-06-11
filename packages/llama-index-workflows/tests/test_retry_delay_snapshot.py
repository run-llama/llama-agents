# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Snapshot/resume during a retry-delay window.

A workflow snapshotted between a failed attempt and its delayed retry must
round-trip: the delayed attempt lives in BrokerState (queued with an absolute
``not_before``), so the resumed run re-arms the delay and completes. Without
that, the retry existed only in the runner's in-memory wakeup heap and the
resumed run would hang.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from workflows.context import Context
from workflows.decorators import step
from workflows.errors import WorkflowCancelledByUser
from workflows.events import Event, StartEvent, StopEvent
from workflows.handler import WorkflowHandler
from workflows.retry_policy import (
    ConstantDelayRetryPolicy,
    retry_policy,
    stop_after_attempt,
    wait_exponential_jitter,
)
from workflows.workflow import Workflow

RETRY_DELAY = 0.3


class FlakyWorkflow(Workflow):
    """First attempt fails; the retry (after RETRY_DELAY) succeeds."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.attempt_times: list[float] = []

    @step(retry_policy=ConstantDelayRetryPolicy(maximum_attempts=3, delay=RETRY_DELAY))
    async def flaky(self, ev: StartEvent) -> StopEvent:
        self.attempt_times.append(time.time())
        if len(self.attempt_times) == 1:
            raise RuntimeError("first attempt fails")
        return StopEvent(result=f"ok_after_{len(self.attempt_times)}")


async def _snapshot_in_delay_window(handler: WorkflowHandler) -> dict[str, Any]:
    """Poll until the snapshot shows the queued delayed retry, then return it.

    Polling on the serialized state (rather than on attempt counts) makes the
    capture deterministic: the snapshot is taken strictly after the failure
    was processed and strictly before the retry is redelivered.
    """
    assert handler.ctx is not None
    deadline = time.monotonic() + 5.0
    while True:
        ctx_dict = handler.ctx.to_dict()
        queue = ctx_dict["workers"]["flaky"]["queue"]
        if queue and queue[0]["not_before"] is not None:
            return ctx_dict
        assert time.monotonic() < deadline, (
            "never observed the delayed retry in serialized state"
        )
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_snapshot_during_retry_delay_window_round_trips() -> None:
    wf = FlakyWorkflow(timeout=10.0)
    handler = wf.run()

    ctx_dict = await _snapshot_in_delay_window(handler)
    await handler.cancel_run()
    with pytest.raises(WorkflowCancelledByUser):
        await handler

    # The delayed retry is part of the snapshot, with retry info intact
    queue = ctx_dict["workers"]["flaky"]["queue"]
    assert len(queue) == 1
    assert queue[0]["attempts"] == 1
    assert queue[0]["not_before"] == pytest.approx(
        wf.attempt_times[0] + RETRY_DELAY, abs=0.2
    )

    resumed = wf.run(ctx=Context.from_dict(wf, ctx_dict))
    result = await resumed

    assert result == "ok_after_2"
    assert len(wf.attempt_times) == 2
    # The remaining delay was honored across the resume
    assert wf.attempt_times[1] - wf.attempt_times[0] >= RETRY_DELAY * 0.8


@pytest.mark.asyncio
async def test_resume_after_eligibility_delivers_immediately_once() -> None:
    wf = FlakyWorkflow(timeout=10.0)
    handler = wf.run()

    ctx_dict = await _snapshot_in_delay_window(handler)
    await handler.cancel_run()
    with pytest.raises(WorkflowCancelledByUser):
        await handler

    # Resume well past the eligibility time: delivers immediately, exactly once
    await asyncio.sleep(RETRY_DELAY + 0.1)
    resumed = wf.run(ctx=Context.from_dict(wf, ctx_dict))
    result = await resumed

    assert result == "ok_after_2"
    assert len(wf.attempt_times) == 2


@pytest.mark.asyncio
async def test_snapshot_after_delayed_retry_resolved() -> None:
    """Snapshots stay available after a delayed retry has fired.

    Regression: snapshots are rebuilt by replaying journaled ticks. When
    eligibility was a wall-clock comparison, replaying the failure tick
    recomputed not_before from replay time, so the journaled TickWakeup
    didn't dispatch and the retry's step-result tick crashed with
    "Worker not found in in_progress".
    """
    wf = FlakyWorkflow(timeout=10.0)
    handler = wf.run()
    result = await handler

    assert result == "ok_after_2"
    assert handler.ctx is not None
    ctx_dict = handler.ctx.to_dict()
    assert ctx_dict["workers"]["flaky"]["queue"] == []


@pytest.mark.asyncio
async def test_snapshot_not_before_is_stable_across_snapshots() -> None:
    """Two snapshots taken at different times agree on not_before.

    Regression: not_before was recomputed from the snapshot-time clock, so
    every snapshot restarted the full delay (a late snapshot + resume waited
    the whole delay again instead of the remainder). It must derive from the
    journaled failure timestamp.
    """
    wf = FlakyWorkflow(timeout=10.0)
    handler = wf.run()

    first = await _snapshot_in_delay_window(handler)
    await asyncio.sleep(RETRY_DELAY / 3)
    assert handler.ctx is not None
    second = handler.ctx.to_dict()

    first_nb = first["workers"]["flaky"]["queue"][0]["not_before"]
    second_nb = second["workers"]["flaky"]["queue"][0]["not_before"]
    assert first_nb == second_nb

    await handler.cancel_run()
    with pytest.raises(WorkflowCancelledByUser):
        await handler


class JitteryFlakyWorkflow(Workflow):
    """First attempt fails; retry delay comes from a jittered (seeded) wait."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.attempts = 0

    @step(
        retry_policy=retry_policy(
            wait=wait_exponential_jitter(
                initial=0.01, exp_base=1.0, max=0.05, jitter=0.04
            ),
            stop=stop_after_attempt(3),
        )
    )
    async def flaky(self, ev: StartEvent) -> StopEvent:
        self.attempts += 1
        if self.attempts == 1:
            raise RuntimeError("first attempt fails")
        return StopEvent(result="ok")


@pytest.mark.asyncio
async def test_snapshot_rebuild_with_jittered_retry_policy() -> None:
    """Snapshots of runs with jittered retry policies must rebuild.

    Regression: snapshot rebuild replays journaled ticks, but the replay
    entry points dropped the run id that seeds retry jitter. Replay then
    sampled an unseeded delay, computed a different not_before than the
    journaled TickWakeup.due, never flipped the attempt eligible, and the
    retry's step-result tick crashed with "Worker not found in in_progress"
    (~50% of runs). Repeated to make a reintroduction loud.
    """
    for _ in range(3):
        wf = JitteryFlakyWorkflow(timeout=10.0)
        handler = wf.run()
        result = await handler

        assert result == "ok"
        assert wf.attempts == 2
        assert handler.ctx is not None
        ctx_dict = handler.ctx.to_dict()
        assert ctx_dict["workers"]["flaky"]["queue"] == []


class SideEvent(Event):
    pass


class InterleavedFlakyWorkflow(Workflow):
    """Retried step also receives an unrelated event during the delay window."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.attempt_times: list[float] = []
        self.side_events = 0

    @step(retry_policy=ConstantDelayRetryPolicy(maximum_attempts=3, delay=RETRY_DELAY))
    async def flaky(self, ev: StartEvent | SideEvent) -> StopEvent | SideEvent | None:
        if isinstance(ev, SideEvent):
            self.side_events += 1
            return None
        self.attempt_times.append(time.time())
        if len(self.attempt_times) == 1:
            raise RuntimeError("first attempt fails")
        return StopEvent(result=f"ok_after_{len(self.attempt_times)}")


@pytest.mark.asyncio
async def test_snapshot_after_interleaved_event_in_delay_window() -> None:
    """An event dispatched to the step during the delay window must not break
    snapshot replay.

    Regression: with clock-based eligibility, replay dispatched the retry at
    the failure tick (past due by replay time) while the live run dispatched
    the interleaved event first, cross-wiring worker records and crashing on
    the retry's step-result tick.
    """
    wf = InterleavedFlakyWorkflow(timeout=10.0)
    handler = wf.run()

    await _snapshot_in_delay_window(handler)
    assert handler.ctx is not None
    handler.ctx.send_event(SideEvent())

    result = await handler
    assert result == "ok_after_2"
    assert wf.side_events == 1

    ctx_dict = handler.ctx.to_dict()
    assert ctx_dict["workers"]["flaky"]["queue"] == []
