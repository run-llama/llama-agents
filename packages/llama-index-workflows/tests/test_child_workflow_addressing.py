# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import pytest
from workflows import Context, Workflow
from workflows.decorators import step
from workflows.errors import WorkflowRuntimeError
from workflows.events import (
    Event,
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
    get_event_origin_namespace,
)


class _AddressStart(StartEvent):
    label: str = "child"


class _AddressAnswer(HumanResponseEvent):
    response: str


class _AddressStop(StopEvent):
    response: str


class _AddressAsk(InputRequiredEvent):
    label: str


class _AddressChild(Workflow):
    @step
    async def ask(self, ev: _AddressStart) -> _AddressAsk:
        return _AddressAsk(label=ev.label)

    @step
    async def answer(self, ev: _AddressAnswer) -> _AddressStop:
        return _AddressStop(response=ev.response)


class _AddressParent(Workflow):
    child: _AddressChild

    @step
    async def start(self, ev: StartEvent) -> _AddressStart:
        return _AddressStart(label="child")

    @step
    async def finish(self, ev: _AddressStop) -> StopEvent:
        return StopEvent(result=ev.response)


@pytest.mark.asyncio
async def test_external_static_child_path_raises_synchronously_and_run_continues() -> (
    None
):
    handler = _AddressParent(child=_AddressChild(), timeout=10).run()
    raised = False

    async for ev in handler.stream_events(include_children=True):
        if isinstance(ev, _AddressAsk):
            with pytest.raises(
                WorkflowRuntimeError,
                match=(
                    "Send addressed to static child path 'child' is ambiguous: "
                    ".*child#0/step"
                ),
            ):
                await handler.send_event(
                    _AddressAnswer(response="static"), step="child/answer"
                )
            raised = True
            child_path = "/".join(get_event_origin_namespace(ev))
            await handler.send_event(
                _AddressAnswer(response="concrete"), step=f"{child_path}/answer"
            )

    assert raised
    assert await handler == "concrete"


@pytest.mark.asyncio
async def test_external_unknown_concrete_slot_still_raises_synchronously() -> None:
    handler = _AddressParent(child=_AddressChild(), timeout=10).run()
    raised = False

    async for ev in handler.stream_events(include_children=True):
        if isinstance(ev, _AddressAsk):
            with pytest.raises(
                WorkflowRuntimeError,
                match="Step nope#0/answer does not exist",
            ):
                await handler.send_event(
                    _AddressAnswer(response="bad"), step="nope#0/answer"
                )
            raised = True
            child_path = "/".join(get_event_origin_namespace(ev))
            await handler.send_event(
                _AddressAnswer(response="ok"), step=f"{child_path}/answer"
            )

    assert raised
    assert await handler == "ok"


@pytest.mark.asyncio
async def test_external_concrete_child_path_still_completes() -> None:
    handler = _AddressParent(child=_AddressChild(), timeout=10).run()
    sent = False

    async for ev in handler.stream_events(include_children=True):
        if isinstance(ev, _AddressAsk):
            child_path = "/".join(get_event_origin_namespace(ev))
            await handler.send_event(
                _AddressAnswer(response="ok"), step=f"{child_path}/answer"
            )
            sent = True

    assert sent
    assert await handler == "ok"


class _RelayStart(StartEvent):
    value: str = "relay"


class _RelayStop(StopEvent):
    value: str


class _RelayChild(Workflow):
    @step
    async def run_child(self, ev: _RelayStart) -> _RelayStop:
        return _RelayStop(value=ev.value)


class _RelayParent(Workflow):
    child: _RelayChild

    @step
    async def start(self, ev: StartEvent) -> _RelayStart:
        return _RelayStart(value="child-local")

    @step
    async def finish(self, ev: _RelayStop) -> StopEvent:
        return StopEvent(result=ev.value)


class _LocalSendEvent(Event):
    value: str


class _LocalSendWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> _LocalSendEvent | None:
        ctx.send_event(_LocalSendEvent(value="root-local"), step="finish")
        return None

    @step
    async def finish(self, ev: _LocalSendEvent) -> StopEvent:
        return StopEvent(result=ev.value)


@pytest.mark.asyncio
async def test_internal_relative_send_and_start_event_trigger_still_work() -> None:
    assert await _LocalSendWorkflow(timeout=10).run() == "root-local"
    assert await _RelayParent(child=_RelayChild(), timeout=10).run() == "child-local"
