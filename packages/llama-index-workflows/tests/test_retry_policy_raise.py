# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import (
    Context,
    Workflow,
    catch_error,
    step,
)
from workflows.events import (
    Event,
    StartEvent,
    StepFailedEvent,
    StopEvent,
    WorkflowFailedEvent,
)
from workflows.retry_policy import (
    RetryPolicy,
    retry_if_exception,
    retry_policy,
    stop_after_attempt,
    wait_fixed,
)


def _status_code_policy() -> RetryPolicy:
    """A policy whose predicate reads an attribute only some errors carry.

    `retry_if_exception` is the documented way to gate on a transport error's
    response. Applied to a `ConnectionError` the predicate itself raises, so
    the policy never returns a verdict.
    """

    def _is_server_error(error: BaseException) -> bool:
        return getattr(error, "response").status_code >= 500

    return retry_policy(
        retry=retry_if_exception(_is_server_error),
        wait=wait_fixed(0),
        stop=stop_after_attempt(3),
    )


@pytest.mark.asyncio
async def test_raising_policy_fails_run_with_the_policy_error() -> None:
    class Flow(Workflow):
        @step(retry_policy=_status_code_policy())
        async def flaky(self, ev: StartEvent) -> StopEvent:
            raise ConnectionError("transient")

    handler = Flow(timeout=5).run()
    events: list[Event] = []
    async for ev in handler.stream_events():
        events.append(ev)
    with pytest.raises(AttributeError, match="response") as excinfo:
        await handler
    assert isinstance(excinfo.value.__cause__, ConnectionError)
    failed = [ev for ev in events if isinstance(ev, WorkflowFailedEvent)]
    assert len(failed) == 1
    assert failed[0].step_name == "flaky"
    assert str(failed[0].exception) == (
        "'ConnectionError' object has no attribute 'response'"
    )


@pytest.mark.asyncio
async def test_raising_policy_reaches_catch_error() -> None:
    class Flow(Workflow):
        @step(retry_policy=_status_code_policy())
        async def flaky(self, ev: StartEvent) -> StopEvent:
            raise ConnectionError("transient")

        @catch_error
        async def handler(self, ctx: Context, ev: StepFailedEvent) -> StopEvent:
            return StopEvent(result={"recovered": ev.step_name})

    handler = Flow(timeout=5).run()
    events: list[Event] = []
    async for ev in handler.stream_events():
        events.append(ev)
    result = await handler
    assert result == {"recovered": "flaky"}
    assert not any(isinstance(ev, WorkflowFailedEvent) for ev in events)
