# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""End-to-end DBOS + WorkflowServer + PostgresWorkflowStore integration tests.

These tests verify:
1. Event interceptor prevents published events from reaching dbos.streams
2. Events are stored as clean JSON in our wf_events table
3. subscribe_events works across the full server chain
4. Interrupt/resume preserves the original run and its stored events

All tests require Docker (testcontainers) and use subprocess isolation for
DBOS global state safety.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SERVER_RUNNER_PATH = str(Path(__file__).parent / "fixtures" / "server_runner.py")

pytestmark = [pytest.mark.docker]


def run_server_scenario(
    workflow: str,
    db_url: str,
    run_id: str,
    check_streams: bool = False,
    check_events: bool = False,
    interrupt_after: str | None = None,
    send_event: str | None = None,
    timeout: float = 60.0,
) -> subprocess.CompletedProcess[str]:
    """Run a workflow scenario through the WorkflowServer + DBOS chain."""
    cmd = [
        sys.executable,
        SERVER_RUNNER_PATH,
        "--workflow",
        workflow,
        "--db-url",
        db_url,
        "--run-id",
        run_id,
    ]
    if check_streams:
        cmd.append("--check-streams")
    if check_events:
        cmd.append("--check-events")
    if interrupt_after:
        cmd.extend(["--interrupt-after", interrupt_after])
    if send_event is not None:
        cmd.extend(["--send-event", send_event])
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def assert_no_errors(result: subprocess.CompletedProcess[str]) -> None:
    """Check subprocess result for crashes and errors."""
    if result.returncode != 0:
        msg = f"Subprocess exited with code {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        pytest.fail(msg)
    # Only fail on tracebacks in stdout — stderr tracebacks during DBOS
    # shutdown are noisy but harmless when exit code is 0.
    if "Traceback (most recent call last)" in result.stdout:
        pytest.fail(f"Exception!\nstdout: {result.stdout}\nstderr: {result.stderr}")


def extract_line(output: str, prefix: str) -> str | None:
    """Extract a line starting with prefix from output."""
    for line in output.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :]
    return None


def extract_all_lines(output: str, prefix: str) -> list[str]:
    """Extract all lines starting with prefix from output."""
    return [
        line[len(prefix) :] for line in output.splitlines() if line.startswith(prefix)
    ]


def test_event_interceptor_no_dbos_streams(postgres_dsn: str) -> None:
    """Run a workflow via the server chain and verify no events in dbos.streams."""
    result = run_server_scenario(
        workflow="tests.fixtures.sample_workflows.chained:ChainedWorkflow",
        db_url=postgres_dsn,
        run_id="test-interceptor-001",
        check_streams=True,
        check_events=True,
    )
    assert_no_errors(result)
    assert "SUCCESS" in result.stdout

    streams_count = extract_line(result.stdout, "STREAMS_COUNT:")
    assert streams_count is not None, (
        f"No STREAMS_COUNT found.\nstdout: {result.stdout}"
    )
    assert int(streams_count) == 0, (
        f"Expected 0 events in dbos.streams, got {streams_count}"
    )


def test_events_stored_as_json(postgres_dsn: str) -> None:
    """Verify events are stored in wf_events with valid JSON."""
    result = run_server_scenario(
        workflow="tests.fixtures.sample_workflows.chained:ChainedWorkflow",
        db_url=postgres_dsn,
        run_id="test-events-json-001",
        check_events=True,
    )
    assert_no_errors(result)
    assert "SUCCESS" in result.stdout

    events_count = extract_line(result.stdout, "EVENTS_COUNT:")
    assert events_count is not None, f"No EVENTS_COUNT.\nstdout: {result.stdout}"
    count = int(events_count)
    assert count >= 3, f"Expected at least 3 events, got {count}"

    event_jsons = extract_all_lines(result.stdout, "EVENT_JSON:")
    for i, event_json in enumerate(event_jsons):
        try:
            parsed = json.loads(event_json)
            assert "type" in parsed, f"Event {i} missing 'type' field"
        except json.JSONDecodeError:
            pytest.fail(f"Event {i} is not valid JSON: {event_json}")


def test_subscribe_events_receives_all_events(postgres_dsn: str) -> None:
    """Verify subscribe_events receives all events in order during workflow execution."""
    result = run_server_scenario(
        workflow="tests.fixtures.sample_workflows.chained:ChainedWorkflow",
        db_url=postgres_dsn,
        run_id="test-subscribe-001",
    )
    assert_no_errors(result)
    assert "SUCCESS" in result.stdout

    event_names = extract_all_lines(result.stdout, "EVENT:")
    assert len(event_names) >= 3, f"Expected at least 3 events, got {event_names}"

    # StopEvent should be the last event
    assert event_names[-1] == "StopEvent", (
        f"Last event should be StopEvent, got {event_names[-1]}"
    )


def test_replay_resumes_original_run(postgres_dsn: str) -> None:
    """Resume the interrupted run and retain its events in the server store."""
    run_id = "test-replay-resume-001"

    # The workflow cannot finish before the crash because it needs user input.
    result1 = run_server_scenario(
        workflow="tests.fixtures.sample_workflows.hitl:TestWorkflow",
        db_url=postgres_dsn,
        run_id=run_id,
        interrupt_after="AskInputEvent",
    )
    assert_no_errors(result1)
    assert "INTERRUPTING" in result1.stdout, (
        f"Should have interrupted.\nstdout: {result1.stdout}\nstderr: {result1.stderr}"
    )

    # Run 2: resume to completion with same run_id, check events
    result2 = run_server_scenario(
        workflow="tests.fixtures.sample_workflows.hitl:TestWorkflow",
        db_url=postgres_dsn,
        run_id=run_id,
        send_event=json.dumps({"type": "UserInput", "fields": {"response": "world"}}),
        check_events=True,
        check_streams=True,
    )
    assert_no_errors(result2)
    assert "SUCCESS" in result2.stdout, (
        f"Resume should succeed.\nstdout: {result2.stdout}\nstderr: {result2.stderr}"
    )

    original_run_id = extract_line(result1.stdout, "RUN_ID:")
    assert original_run_id is not None
    assert extract_line(result2.stdout, "RUN_ID:") == original_run_id

    event_names = extract_all_lines(result2.stdout, "EVENT:")
    assert "AskInputEvent" in event_names, event_names
    assert event_names.count("StopEvent") == 1, event_names

    stored_events = [
        json.loads(value) for value in extract_all_lines(result2.stdout, "EVENT_JSON:")
    ]
    stop_events = [event for event in stored_events if event["type"] == "StopEvent"]
    assert len(stop_events) == 1, stored_events
    assert stop_events[0]["value"]["result"] == {"response": "world"}

    # Verify no events in dbos.streams
    streams_count = extract_line(result2.stdout, "STREAMS_COUNT:")
    assert streams_count is not None
    assert int(streams_count) == 0, (
        f"Expected 0 events in dbos.streams after replay, got {streams_count}"
    )
