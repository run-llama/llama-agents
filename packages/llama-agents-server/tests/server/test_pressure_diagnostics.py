# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any

import pytest
from llama_agents.server import (
    MemoryWorkflowStore,
    PressureDiagnosticsConfig,
    WorkflowServer,
)
from llama_agents.server._diagnostics.pressure import PressureDiagnosticsRecorder
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent


def _get_field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value[name]
    return getattr(value, name)


def _get_optional_field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _event_type(event: Any) -> str:
    return str(
        _get_optional_field(
            event,
            "event_type",
            _get_optional_field(event, "type", _get_optional_field(event, "kind")),
        )
    )


def _payload(event: Any) -> dict[str, Any]:
    payload = _get_optional_field(event, "payload", event)
    if isinstance(payload, dict):
        return payload
    return payload.model_dump(mode="python")


def _make_recorder(
    *,
    config: PressureDiagnosticsConfig | None = None,
    rss_values_mb: Sequence[float] = (),
) -> tuple[PressureDiagnosticsRecorder, Callable[[float], None]]:
    now = 0.0
    rss_iter = iter(rss_values_mb)
    last_rss_mb = 0.0

    def monotonic_clock() -> float:
        return now

    def wall_clock() -> datetime:
        return datetime.fromtimestamp(now, tz=timezone.utc)

    def rss_sampler() -> int:
        nonlocal last_rss_mb
        last_rss_mb = next(rss_iter, last_rss_mb)
        return int(last_rss_mb * 1024 * 1024)

    def advance(seconds: float) -> None:
        nonlocal now
        now += seconds

    recorder = PressureDiagnosticsRecorder(
        config=config or PressureDiagnosticsConfig(enabled=True),
        monotonic_clock=monotonic_clock,
        wall_clock=wall_clock,
        rss_sampler=rss_sampler,
    )
    return recorder, advance


def test_pressure_diagnostics_config_export_and_default_disabled() -> None:
    config = PressureDiagnosticsConfig()

    assert config.enabled is False

    disabled_server = WorkflowServer()
    assert disabled_server.diagnostics == config
    assert disabled_server.pressure_diagnostics_recorder is None

    enabled_server = WorkflowServer(
        diagnostics=PressureDiagnosticsConfig(enabled=True, sample_interval=0.01)
    )
    assert enabled_server.diagnostics.enabled is True
    assert enabled_server.pressure_diagnostics_recorder is not None


def test_pressure_recorder_ring_buffer_snapshot_and_interval_overlap(
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder, advance = _make_recorder(
        config=PressureDiagnosticsConfig(enabled=True, max_events=3)
    )

    advance(1.0)
    recorder.record_step_start(
        run_id="run-a",
        workflow_name="workflow-a",
        step_name="first",
        worker_id="0",
        input_event_name="StartEvent",
    )
    advance(2.0)
    recorder.record_step_start(
        run_id="run-b",
        workflow_name="workflow-b",
        step_name="second",
        worker_id="1",
        input_event_name="StartEvent",
    )
    advance(1.0)
    recorder.record_step_end(
        run_id="run-a",
        step_name="first",
        worker_id="0",
        output_event_name="StopEvent",
    )

    for sequence in range(5):
        recorder.record_pressure_event(
            event_type="synthetic_pressure_event",
            payload={"sequence": sequence},
        )

    assert any(
        getattr(record, "pressure_event_type", None) == "synthetic_pressure_event"
        and getattr(record, "pressure_event", {})["sequence"] == 4
        for record in caplog.records
    )

    recent_events = recorder.recent_events()
    assert [_payload(event)["sequence"] for event in recent_events] == [2, 3, 4]

    snapshot = recorder.snapshot()
    active_intervals = _get_field(snapshot, "active_intervals")
    assert len(active_intervals) == 1
    assert _get_field(active_intervals[0], "run_id") == "run-b"
    assert _get_field(active_intervals[0], "step_name") == "second"

    overlapping_intervals = recorder.overlapping_intervals(
        start_monotonic=2.5,
        end_monotonic=4.5,
    )
    assert {_get_field(interval, "run_id") for interval in overlapping_intervals} == {
        "run-a",
        "run-b",
    }


def test_pressure_recorder_tracks_concurrent_workers_separately() -> None:
    recorder, _advance = _make_recorder()

    for worker_id in ("0", "1"):
        recorder.record_step_start(
            run_id="run-a",
            workflow_name="workflow-a",
            step_name="parallel",
            worker_id=worker_id,
            input_event_name="StartEvent",
        )

    active_intervals = recorder.snapshot()["active_intervals"]
    assert {
        (_get_field(interval, "step_name"), _get_field(interval, "worker_id"))
        for interval in active_intervals
    } == {("parallel", "0"), ("parallel", "1")}

    recorder.record_run_end("run-a", reason="cancelled")

    assert recorder.snapshot()["active_intervals"] == []
    finished = [
        event
        for event in recorder.recent_events()
        if _event_type(event) == "workflow_step_interval_finished"
    ]
    assert {_payload(event)["finish_reason"] for event in finished} == {"cancelled"}


def test_pressure_recorder_memory_threshold_and_growth_with_injected_sampler() -> None:
    recorder, advance = _make_recorder(
        config=PressureDiagnosticsConfig(
            enabled=True,
            memory_rss_threshold_mb=140,
            memory_growth_threshold_mb=40,
            memory_growth_window_seconds=10,
        ),
        rss_values_mb=[100, 150, 171],
    )
    recorder.record_step_start(
        run_id="memory-run",
        workflow_name="memory-workflow",
        step_name="allocate",
        worker_id="0",
        input_event_name="StartEvent",
    )

    recorder.sample_memory_once()
    advance(5.0)
    recorder.sample_memory_once()
    advance(5.0)
    recorder.sample_memory_once()

    events = recorder.recent_events()
    event_types = {_event_type(event) for event in events}
    assert "memory_rss_threshold_exceeded" in event_types
    assert "memory_growth_threshold_exceeded" in event_types

    growth_event = next(
        event
        for event in events
        if _event_type(event) == "memory_growth_threshold_exceeded"
    )
    growth_payload = _payload(growth_event)
    assert growth_payload["growth_mb"] >= 40
    assert growth_payload["window_seconds"] == 10
    assert growth_payload["overlapping_interval_count"] == 1


def test_pressure_recorder_lag_threshold_uses_active_intervals() -> None:
    recorder, _advance = _make_recorder(
        config=PressureDiagnosticsConfig(
            enabled=True,
            event_loop_lag_threshold_ms=25,
            capture_lag_stacks=True,
            lag_stack_max_frames=2,
        )
    )
    recorder.record_step_start(
        run_id="lag-run",
        workflow_name="lag-workflow",
        step_name="block",
        worker_id="0",
        input_event_name="StartEvent",
    )

    recorder.record_event_loop_lag(
        lag_ms=30,
        stack_frames=["workflow.py:10 in block", "runtime.py:20 in tick", "extra"],
    )

    lag_event = next(
        event
        for event in recorder.recent_events()
        if _event_type(event) == "event_loop_lag_threshold_exceeded"
    )
    lag_payload = _payload(lag_event)
    assert lag_payload["lag_ms"] == 30
    assert lag_payload["active_interval_count"] == 1
    assert lag_payload["active_intervals"][0]["run_id"] == "lag-run"
    assert lag_payload["stack_frames"] == [
        "workflow.py:10 in block",
        "runtime.py:20 in tick",
    ]


class DiagnosticWorkflow(Workflow):
    @step
    async def start(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")


async def test_workflow_server_pressure_diagnostics_record_step_intervals() -> None:
    server = WorkflowServer(
        workflow_store=MemoryWorkflowStore(),
        diagnostics=PressureDiagnosticsConfig(enabled=True, sample_interval=0.01),
    )
    workflow = DiagnosticWorkflow()
    server.add_workflow("diagnostic", workflow)

    async with server.contextmanager():
        handler = await server._service.start_workflow(workflow, "diagnostic-handler")
        completed = await server._service.await_workflow(handler)

        assert completed.status == "completed"

        async def interval_events_were_recorded() -> list[Any]:
            recorder = server.pressure_diagnostics_recorder
            assert recorder is not None
            events = recorder.recent_events()
            event_types = {_event_type(event) for event in events}
            assert "workflow_step_interval_started" in event_types
            assert "workflow_step_interval_finished" in event_types
            return events

        events = await wait_for_passing(interval_events_were_recorded)

    interval_events = [
        event
        for event in events
        if _event_type(event)
        in {"workflow_step_interval_started", "workflow_step_interval_finished"}
    ]
    assert {_payload(event)["workflow_name"] for event in interval_events} == {
        "diagnostic"
    }
    assert {_payload(event)["step_name"] for event in interval_events} == {"start"}
