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
from workflows.events import StartEvent, StopEvent
from workflows.handler import WorkflowHandler
from workflows.retry_policy import ConstantDelayRetryPolicy
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
