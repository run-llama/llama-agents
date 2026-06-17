# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Loudness around collection streams: drop warnings and the idle hang detector.

An untargeted ``ctx.send_event`` of a collected type that can never join a
batch warns; a *targeted* send at a collect step is an explicit instruction
the runtime cannot honor and fails the run. A quiescent run with open streams
and no pending waiter inside them is provably stuck and must fail with a
diagnostic naming the leaked streams; an unresolved waiter scoped inside the
stream (HITL) suppresses the detector.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from workflows import Context, Workflow, step
from workflows.errors import WorkflowRuntimeError
from workflows.events import Event, StartEvent, StopEvent, WorkflowIdleEvent
from workflows.runtime.control_loop.reduce import _reduce_tick
from workflows.runtime.types.commands import (
    CommandFailWorkflow,
    CommandPublishEvent,
)
from workflows.runtime.types.internal_state import (
    BrokerState,
    CollectionReleaseState,
    CollectionStreamInstance,
)
from workflows.runtime.types.results import StepWorkerWaiter
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import TickIdleCheck

_CONTROL_LOOP_LOGGER = "workflows.runtime.control_loop"


class Task(Event):
    n: int


class Done(Event):
    n: int


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


def _warnings(caplog: pytest.LogCaptureFixture, needle: str) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == _CONTROL_LOOP_LOGGER
        and r.levelno == logging.WARNING
        and needle in r.getMessage()
    ]


# ---------------------------------------------------------------------------
# Drop warnings
# ---------------------------------------------------------------------------


async def test_untargeted_send_of_collected_type_warns_and_is_ignored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ctx: Context, ev: Task) -> Done:
            if ev.n == 2:
                ctx.send_event(Done(n=99))
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    with caplog.at_level(logging.WARNING, logger=_CONTROL_LOOP_LOGGER):
        result = await _run(WF(timeout=6))

    # The extra Done never joined the batch, and the drop was loud.
    assert result == [0, 1, 2]
    messages = _warnings(caplog, "sent outside any collection stream")
    assert messages, caplog.text
    assert "Done" in messages[0]
    assert "'join'" in messages[0]


async def test_targeted_send_at_collect_step_fails_run() -> None:
    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(3)]

        @step
        async def work(self, ctx: Context, ev: Task) -> Done:
            if ev.n == 0:
                ctx.send_event(Done(n=99), step="join")
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    with pytest.raises(WorkflowRuntimeError, match="targeted events"):
        await _run(WF(timeout=6))


# ---------------------------------------------------------------------------
# Idle hang detector (reducer level)
# ---------------------------------------------------------------------------


class _FanOutWF(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(2)]

    @step
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)

    @step
    async def join(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=len(events))


def _leaked_stream_state() -> BrokerState:
    """Quiescent running state with one open stream and nothing in flight."""
    wf = _FanOutWF()
    wf._validate()
    state = BrokerState.from_workflow(wf)
    state.is_running = True
    state.streams["stream-x"] = CollectionStreamInstance(
        stream_id="stream-x",
        source_step=StepId.root("fan_out"),
        scope_path=("stream-x",),
        open_work_items=1,
    )
    return state


def test_idle_check_fails_run_on_leaked_open_stream() -> None:
    state = _leaked_stream_state()
    _, commands = _reduce_tick(TickIdleCheck(), state, 0.0)

    failures = [c for c in commands if isinstance(c, CommandFailWorkflow)]
    assert len(failures) == 1, commands
    message = str(failures[0].exception)
    assert "fan_out" in message
    assert "stream-x" in message
    assert failures[0].step_id == StepId.root("fan_out")


def test_idle_check_fails_run_on_orphaned_unreleased_release_state() -> None:
    """An unreleased release whose stream is gone fails loudly, never hangs.

    Stream closes fire releases inline, so this state is unreachable in a
    healthy run — it indicates corrupted or version-skewed persisted state.
    The detector must flag it even with no open streams (the blind spot that
    used to hang resumes silently).
    """
    wf = _FanOutWF()
    wf._validate()
    state = BrokerState.from_workflow(wf)
    state.is_running = True
    binding_id = next(iter(state.config.collection_bindings))
    state.collection_release_states[f"stream-gone:{binding_id}"] = (
        CollectionReleaseState(binding_id=binding_id, stream_id="stream-gone")
    )

    _, commands = _reduce_tick(TickIdleCheck(), state, 0.0)

    failures = [c for c in commands if isinstance(c, CommandFailWorkflow)]
    assert len(failures) == 1, commands
    assert failures[0].step_id == StepId.root("join")
    message = str(failures[0].exception)
    assert "stream-gone" in message
    assert "never fire" in message


def _waiter(scope_path: tuple[str, ...]) -> StepWorkerWaiter:
    return StepWorkerWaiter(
        waiter_id="w1",
        event=Task(n=0),
        waiting_for_event=Approval,
        requirements={},
        has_requirements=False,
        resolved_event=None,
        timed_out=False,
        scope_path=scope_path,
    )


def test_idle_check_with_in_stream_waiter_publishes_idle_not_failure() -> None:
    state = _leaked_stream_state()
    state.workers[StepId.root("work")].collected_waiters.append(_waiter(("stream-x",)))
    _, commands = _reduce_tick(TickIdleCheck(), state, 0.0)

    assert not any(isinstance(c, CommandFailWorkflow) for c in commands), commands
    idle_publishes = [
        c
        for c in commands
        if isinstance(c, CommandPublishEvent) and isinstance(c.event, WorkflowIdleEvent)
    ]
    assert len(idle_publishes) == 1, commands


def test_idle_check_with_out_of_stream_waiter_still_fails_run() -> None:
    """A HITL waiter elsewhere in the workflow must not mask a wedged stream.

    Only a waiter whose scope places it inside an open stream (or a
    descendant) can still feed that stream; one outside (empty scope here)
    cannot, so detection proceeds. The failure event carries no fabricated
    attempt counters — this is not an exhausted step attempt.
    """
    state = _leaked_stream_state()
    state.workers[StepId.root("work")].collected_waiters.append(_waiter(()))
    _, commands = _reduce_tick(TickIdleCheck(), state, 0.0)

    failures = [c for c in commands if isinstance(c, CommandFailWorkflow)]
    assert len(failures) == 1, commands
    published = [
        c.event
        for c in commands
        if isinstance(c, CommandPublishEvent) and hasattr(c.event, "attempts")
    ]
    assert published and published[0].attempts is None
    assert published[0].elapsed_seconds is None


# ---------------------------------------------------------------------------
# Idle hang detector (end-to-end): a pending HITL waiter inside an open stream
# must not trip the detector while the run waits.
# ---------------------------------------------------------------------------


async def test_open_stream_with_pending_waiter_does_not_fail_run() -> None:
    class WF(Workflow):
        @step
        async def fan_out(self, ev: StartEvent) -> list[Task]:
            return [Task(n=i) for i in range(2)]

        @step
        async def work(self, ctx: Context, ev: Task) -> Done:
            if ev.n == 0:
                await ctx.wait_for_event(
                    Approval, waiter_event=Waiting(), waiter_id="gate", timeout=5
                )
            return Done(n=ev.n)

        @step
        async def join(self, events: list[Done]) -> StopEvent:
            return StopEvent(result=sorted(e.n for e in events))

    handler = WF(timeout=10).run()
    sent = False
    async for ev in handler.stream_events():
        if isinstance(ev, Waiting) and not sent:
            sent = True
            # Linger in the quiescent waiting state: idle checks run here and
            # must publish idle, not fail the run.
            await asyncio.sleep(0.5)
            assert not handler.is_done()
            handler.ctx.send_event(Approval())
    assert sent, "workflow never emitted the Waiting marker"
    result = await asyncio.wait_for(handler, timeout=8)
    assert result == [0, 1]
