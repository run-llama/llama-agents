# ty: ignore[unknown-argument]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio

import pytest
from workflows import Context, Workflow
from workflows.context.state_store import CHILD_STATES_KEY
from workflows.decorators import catch_error, step
from workflows.errors import WorkflowRuntimeError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StepFailedEvent,
    StopEvent,
    get_event_origin_namespace,
)
from workflows.testing import WorkflowTestRunner


def _assert_child_invocation_namespace(namespace: tuple[str, ...]) -> None:
    assert len(namespace) == 1
    assert namespace[0].startswith("child#")
    assert namespace[0] != "child"
    assert namespace[0].removeprefix("child#")


class _StreamStart(StartEvent):
    pass


class _StreamStop(StopEvent):
    pass


class _ChildProgress(Event):
    pass


class _StreamingChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: _StreamStart) -> _StreamStop:
        ctx.write_event_to_stream(_ChildProgress())
        return _StreamStop()


class _StreamingParent(Workflow):
    child: _StreamingChild

    @step
    async def start(self, ev: StartEvent) -> _StreamStart:
        return _StreamStart()

    @step
    async def finish(self, ev: _StreamStop) -> StopEvent:
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_stream_origin_exposes_opaque_child_invocation_namespace() -> None:
    handler = _StreamingParent(child=_StreamingChild()).run()
    collected: list[Event] = []
    async for ev in handler.stream_events(include_children=True):
        collected.append(ev)
    await handler

    child_progress = next(ev for ev in collected if isinstance(ev, _ChildProgress))
    _assert_child_invocation_namespace(get_event_origin_namespace(child_progress))


class _CountingStart(StartEvent):
    round: int


class _CountingStop(StopEvent):
    round: int
    count: int


class _CountingChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: _CountingStart) -> _CountingStop:
        count = await ctx.store.get("count", default=0) + 1
        await ctx.store.set("count", count)
        return _CountingStop(round=ev.round, count=count)


class _SequentialParent(Workflow):
    child: _CountingChild

    @step
    async def start(self, ev: StartEvent) -> _CountingStart:
        return _CountingStart(round=1)

    @step
    async def continue_or_finish(
        self, ctx: Context, ev: _CountingStop
    ) -> _CountingStart | StopEvent:
        counts = await ctx.store.get("counts", default=[])
        counts = [*counts, ev.count]
        await ctx.store.set("counts", counts)

        if ev.round == 1:
            return _CountingStart(round=2)
        return StopEvent(result=counts)


@pytest.mark.asyncio
async def test_sequential_same_slot_child_invocations_get_fresh_store() -> None:
    result = await WorkflowTestRunner(_SequentialParent(child=_CountingChild())).run()
    assert result.result == [1, 1]


class _ParallelStart(StartEvent):
    label: str
    delay: float


class _ParallelStop(StopEvent):
    label: str


class _ParallelChild(Workflow):
    @step
    async def run_child(self, ev: _ParallelStart) -> _ParallelStop:
        await asyncio.sleep(ev.delay)
        return _ParallelStop(label=ev.label)


class _RecoveringParallelChild(Workflow):
    @step
    async def run_child(self, ev: _ParallelStart) -> _ParallelStop:
        await asyncio.sleep(ev.delay)
        return _ParallelStop(label=ev.label)

    @catch_error
    async def recover(self, ev: StepFailedEvent) -> _ParallelStop:
        assert isinstance(ev.input_event, _ParallelStart)
        return _ParallelStop(label=f"{ev.input_event.label}-timeout")


class _KeepAlive(Event):
    pass


class _ParallelParent(Workflow):
    child: Workflow

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> _ParallelStart | _KeepAlive:
        ctx.send_event(_ParallelStart(label="slow", delay=0.2))
        ctx.send_event(_KeepAlive())
        return _ParallelStart(label="fast", delay=0.01)

    @step
    async def collect(self, ctx: Context, ev: _ParallelStop) -> None:
        labels = await ctx.store.get("labels", default=[])
        await ctx.store.set("labels", [*labels, ev.label])
        return None

    @step
    async def finish(self, ctx: Context, ev: _KeepAlive) -> StopEvent:
        await asyncio.sleep(0.4)
        labels = await ctx.store.get("labels", default=[])
        return StopEvent(result=sorted(labels))


@pytest.mark.asyncio
async def test_overlapping_same_slot_child_invocations_do_not_cancel_each_other() -> (
    None
):
    result = await asyncio.wait_for(
        _ParallelParent(child=_ParallelChild(), timeout=30).run(),
        timeout=10,
    )
    assert result == ["fast", "slow"]


@pytest.mark.asyncio
async def test_overlapping_same_slot_timeout_is_invocation_scoped() -> None:
    result = await asyncio.wait_for(
        _ParallelParent(
            child=_RecoveringParallelChild(timeout=0.05),
            timeout=30,
        ).run(),
        timeout=10,
    )
    assert result == ["fast", "slow-timeout"]


class _HitlStart(StartEvent):
    pass


class _HitlStop(StopEvent):
    answer: str = ""


class _HitlChild(Workflow):
    @step
    async def ask(self, ctx: Context, ev: _HitlStart) -> InputRequiredEvent:
        await ctx.store.set("asked", True)
        return InputRequiredEvent(prefix="child?")  # type: ignore[reportCallIssue]

    @step
    async def answer(self, ev: HumanResponseEvent) -> _HitlStop:
        return _HitlStop(answer=ev.response)


class _HitlParent(Workflow):
    child: _HitlChild

    @step
    async def start(self, ev: StartEvent) -> _HitlStart:
        return _HitlStart()

    @step
    async def finish(self, ev: _HitlStop) -> StopEvent:
        return StopEvent(result=ev.answer)


@pytest.mark.asyncio
async def test_static_child_target_without_invocation_fails_loudly() -> None:
    handler = _HitlParent(child=_HitlChild()).run()
    async for ev in handler.stream_events(include_children=True):
        if isinstance(ev, InputRequiredEvent):
            handler.ctx.send_event(
                HumanResponseEvent(response="ok"),  # type: ignore[reportCallIssue]
                step="child/answer",
            )
            break

    with pytest.raises(WorkflowRuntimeError, match="concrete child invocation"):
        await handler


@pytest.mark.asyncio
async def test_live_idle_child_invocation_state_is_snapshotted() -> None:
    handler = _HitlParent(child=_HitlChild()).run()
    child_origin: tuple[str, ...] | None = None
    async for ev in handler.stream_events(include_children=True):
        if isinstance(ev, InputRequiredEvent):
            child_origin = get_event_origin_namespace(ev)
            break

    assert child_origin is not None
    blob = handler.ctx.to_dict()
    child_states = blob["state"][CHILD_STATES_KEY]
    assert "/".join(child_origin) in child_states

    handler.ctx.send_event(
        HumanResponseEvent(response="ok"),  # type: ignore[reportCallIssue]
        step=f"{child_origin[0]}/answer",
    )
    assert await handler == "ok"


class _ContinuationParent(Workflow):
    child: _CountingChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> _CountingStart:
        await ctx.store.set("root_ran", True)
        return _CountingStart(round=1)

    @step
    async def finish(self, ev: _CountingStop) -> StopEvent:
        return StopEvent(result=ev.count)


@pytest.mark.asyncio
async def test_completed_context_continuation_drops_child_invocation_state() -> None:
    workflow = _ContinuationParent(child=_CountingChild())

    first = await WorkflowTestRunner(workflow).run()
    assert first.result == 1
    first_blob = first.ctx.to_dict()
    child_states = first_blob["state"].get(CHILD_STATES_KEY, {})
    assert not any(key.startswith("child") for key in child_states)

    restored = Context.from_dict(workflow, first_blob)
    second = await WorkflowTestRunner(workflow).run(ctx=restored)
    assert second.result == 1
