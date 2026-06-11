# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for VerboseDecorator and _VerboseInternalRunAdapter."""

from __future__ import annotations

import logging

import pytest
from workflows import Workflow, step
from workflows.events import Event, StartEvent, StepState, StepStateChanged, StopEvent
from workflows.runtime.types.plugin import InternalRunAdapter, WaitResult
from workflows.runtime.types.results import StepWorkerResult
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickIdleRelease,
    TickPublishEvent,
    TickStepResult,
    TickTimeout,
    TickWaiterTimeout,
    WorkflowTick,
)
from workflows.runtime.verbose import _VerboseInternalRunAdapter
from workflows.testing import WorkflowTestRunner


class FakeInternalRunAdapter(InternalRunAdapter):
    """Minimal fake adapter that records events written to the stream."""

    def __init__(self) -> None:
        self.written_events: list[Event] = []

    @property
    def run_id(self) -> str:
        return "fake-run-id"

    async def write_to_event_stream(self, event: Event) -> None:
        self.written_events.append(event)

    async def get_now(self) -> float:
        raise NotImplementedError

    async def send_event(self, tick: WorkflowTick) -> None:
        raise NotImplementedError

    async def wait_receive(
        self,
        timeout_seconds: float | None = None,
    ) -> WaitResult:
        raise NotImplementedError

    async def sleep(self, seconds: float) -> None:
        raise NotImplementedError


def _make_step_state_changed(
    name: str = "my_step",
    step_state: StepState = StepState.RUNNING,
    worker_id: str = "0",
    input_event_name: str = "StartEvent",
    output_event_name: str | None = None,
) -> StepStateChanged:
    return StepStateChanged(
        name=name,
        step_state=step_state,
        worker_id=worker_id,
        input_event_name=input_event_name,
        output_event_name=output_event_name,
    )


@pytest.fixture
def verbose_adapter() -> tuple[FakeInternalRunAdapter, _VerboseInternalRunAdapter]:
    fake = FakeInternalRunAdapter()
    adapter = _VerboseInternalRunAdapter(fake, output=print)
    return fake, adapter


# -- write_to_event_stream tests (step state changes) --


@pytest.mark.parametrize(
    "event,expected",
    [
        pytest.param(
            _make_step_state_changed(
                name="my_step", step_state=StepState.RUNNING, worker_id="0"
            ),
            "[my_step:0] started from StartEvent",
            id="running",
        ),
        pytest.param(
            _make_step_state_changed(
                name="my_step",
                step_state=StepState.NOT_RUNNING,
                output_event_name="MyEvent",
                worker_id="2",
            ),
            "[my_step:2] complete with MyEvent",
            id="complete-with-event",
        ),
        pytest.param(
            _make_step_state_changed(
                name="my_step",
                step_state=StepState.NOT_RUNNING,
                output_event_name=None,
                worker_id="1",
            ),
            "[my_step:1] complete with no result",
            id="complete-no-result",
        ),
        pytest.param(
            _make_step_state_changed(
                name="my_step",
                step_state=StepState.PREPARING,
                worker_id="<enqueued>",
            ),
            "[my_step] enqueued (waiting for capacity)",
            id="preparing",
        ),
    ],
)
async def test_verbose_step_state(
    verbose_adapter: tuple[FakeInternalRunAdapter, _VerboseInternalRunAdapter],
    capsys: pytest.CaptureFixture[str],
    event: StepStateChanged,
    expected: str,
) -> None:
    _, adapter = verbose_adapter
    await adapter.write_to_event_stream(event)
    assert expected in capsys.readouterr().out


async def test_verbose_auto_detects_logger_when_info_enabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("workflows.verbose")
    old_level = logger.level
    try:
        logger.setLevel(logging.INFO)
        from workflows.runtime.verbose import _resolve_output

        output = _resolve_output()
        fake = FakeInternalRunAdapter()
        adapter = _VerboseInternalRunAdapter(fake, output=output)

        event = _make_step_state_changed(name="my_step", step_state=StepState.RUNNING)
        with caplog.at_level(logging.INFO, logger="workflows.verbose"):
            await adapter.write_to_event_stream(event)

        assert "[my_step:0] started from StartEvent" in caplog.text
    finally:
        logger.setLevel(old_level)


async def test_verbose_falls_back_to_print_by_default() -> None:
    logger = logging.getLogger("workflows.verbose")
    old_level = logger.level
    try:
        logger.setLevel(logging.NOTSET)
        from workflows.runtime.verbose import _resolve_output

        output = _resolve_output()
        assert output is print
    finally:
        logger.setLevel(old_level)


async def test_verbose_forwards_events(
    verbose_adapter: tuple[FakeInternalRunAdapter, _VerboseInternalRunAdapter],
) -> None:
    fake, adapter = verbose_adapter
    event = _make_step_state_changed(name="my_step", step_state=StepState.RUNNING)
    await adapter.write_to_event_stream(event)

    assert len(fake.written_events) == 1
    assert fake.written_events[0] is event


# -- on_tick tests (tick-level logging) --


@pytest.mark.parametrize(
    "tick,expected",
    [
        pytest.param(
            TickAddEvent(event=StartEvent()),
            "[tick] add: StartEvent()",
            id="add-event",
        ),
        pytest.param(
            TickAddEvent(event=StartEvent(), step_name="retrieve"),
            "[tick] add: StartEvent() -> retrieve",
            id="add-event-targeted",
        ),
        pytest.param(
            TickPublishEvent(event=StopEvent(result="done")),
            "[tick] publish: StopEvent(result='done')",
            id="publish-event",
        ),
        pytest.param(
            TickTimeout(timeout=30.0),
            "[tick] timeout: 30.0s",
            id="timeout",
        ),
        pytest.param(
            TickWaiterTimeout(step_name="my_step", waiter_id="w-123"),
            "[tick] waiter timeout: step my_step waiter w-123",
            id="waiter-timeout",
        ),
        pytest.param(
            TickCancelRun(),
            "[tick] cancelled",
            id="cancel",
        ),
        pytest.param(
            TickIdleRelease(),
            "[tick] idle release",
            id="idle-release",
        ),
    ],
)
async def test_verbose_on_tick(
    verbose_adapter: tuple[FakeInternalRunAdapter, _VerboseInternalRunAdapter],
    capsys: pytest.CaptureFixture[str],
    tick: WorkflowTick,
    expected: str,
) -> None:
    _, adapter = verbose_adapter
    await adapter.on_tick(tick)
    assert expected in capsys.readouterr().out


async def test_verbose_tick_step_result_logs_stop_event(
    verbose_adapter: tuple[FakeInternalRunAdapter, _VerboseInternalRunAdapter],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """TickStepResult with a StopEvent logs a [result] line."""
    _, adapter = verbose_adapter
    tick = TickStepResult(
        step_name="my_step",
        worker_id=0,
        event=StartEvent(),
        result=[StepWorkerResult(result=StopEvent(result="done"))],
    )
    await adapter.on_tick(tick)

    captured = capsys.readouterr()
    assert "[result] StopEvent(result='done')" in captured.out


async def test_verbose_tick_step_result_silent_for_non_stop(
    verbose_adapter: tuple[FakeInternalRunAdapter, _VerboseInternalRunAdapter],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """TickStepResult without a StopEvent produces no on_tick output."""
    _, adapter = verbose_adapter
    tick = TickStepResult(
        step_name="my_step",
        worker_id=0,
        event=StartEvent(),
        result=[StepWorkerResult(result=StartEvent())],
    )
    await adapter.on_tick(tick)

    assert capsys.readouterr().out == ""


# -- Integration test --


class TwoStepWorkflow(Workflow):
    @step
    async def first(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")


async def test_workflow_verbose_integration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    wf = TwoStepWorkflow(verbose=True)
    result = await WorkflowTestRunner(wf).run()
    assert result.result == "done"

    captured = capsys.readouterr()
    assert "[first:0] started from StartEvent" in captured.out
    assert "[first:0] complete with StopEvent" in captured.out
    assert "[result] StopEvent(result='done')" in captured.out
