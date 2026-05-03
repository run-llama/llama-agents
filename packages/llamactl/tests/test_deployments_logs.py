# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``deployments logs``."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, AsyncIterator
from unittest.mock import MagicMock

import llama_agents.cli.config.env_service as env_service
import pytest
from click.testing import CliRunner
from conftest import patch_project_client
from llama_agents.cli.app import app
from llama_agents.core.schema import LogEvent

DEFAULT_BASE_URL = "https://api.cloud.llamaindex.ai"


def _make_log_events(n: int = 3) -> list[LogEvent]:
    base_ts = datetime(2026, 4, 26, 12, 0, 0, tzinfo=timezone.utc)
    return [
        LogEvent(
            pod="pod-x",
            container="app",
            text=(
                f'{{"event": "msg-{i}", "level": "info", '
                f'"timestamp": "2026-04-26T12:00:0{i}.000Z"}}'
            ),
            timestamp=base_ts.replace(second=i),
        )
        for i in range(n)
    ]


def _make_logs_client(events: list[LogEvent]) -> MagicMock:
    """Project-client mock with stream_deployment_logs + aclose."""

    async def _stream(*args: Any, **kwargs: Any) -> AsyncIterator[LogEvent]:
        for ev in events:
            yield ev

    async def _aclose() -> None:
        return None

    client = MagicMock()
    # SAM: side_effect on a non-async returning an async iterator works because
    # the command does `async for ev in client.stream_deployment_logs(...)`.
    # The mock returns the async generator object directly when called.
    client.stream_deployment_logs = MagicMock(side_effect=_stream)
    client.get_deployment = MagicMock()
    client.aclose = MagicMock(side_effect=_aclose)
    client.project_id = "proj_default"
    client.base_url = "http://test:8011"
    return client


def _patch_no_profile_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_auth_svc = MagicMock()
    mock_auth_svc.get_current_profile.return_value = None
    mock_auth_svc.list_profiles.return_value = []
    mock_auth_svc.env = SimpleNamespace(requires_auth=True)
    mock_auth_svc.auth_middleware.return_value = None

    mock_service = MagicMock()
    mock_service.current_auth_service.return_value = mock_auth_svc
    mock_service.get_current_environment.return_value = SimpleNamespace(
        api_url=DEFAULT_BASE_URL,
        requires_auth=True,
    )
    monkeypatch.setattr(env_service, "service", mock_service)


def test_logs_default_prints_recent_and_exits(patched_auth: Any) -> None:
    runner = CliRunner()
    events = _make_log_events(3)
    client = _make_logs_client(events)
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "logs", "my-app", "--no-interactive"]
        )
    assert result.exit_code == 0, result.output
    # Three log lines, one per event.
    lines = [ln for ln in result.output.splitlines() if ln.strip()]
    assert len(lines) == 3
    assert "msg-0" in result.output
    assert "msg-2" in result.output
    # Verify follow=False was passed.
    kwargs = client.stream_deployment_logs.call_args.kwargs
    assert kwargs["follow"] is False


def test_logs_uses_complete_env_auth_without_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "env-api-key")
    monkeypatch.setenv("LLAMA_DEPLOY_PROJECT_ID", "env-project")
    _patch_no_profile_auth(monkeypatch)

    runner = CliRunner()
    client = _make_logs_client(_make_log_events(1))
    client.project_id = "env-project"
    client.base_url = DEFAULT_BASE_URL
    client.api_key = "env-api-key"
    with patch_project_client(client) as ctor:
        result = runner.invoke(
            app, ["deployments", "logs", "my-app", "--no-interactive"]
        )

    assert result.exit_code == 0, result.output
    assert "msg-0" in result.output
    args, _ = ctor.call_args
    assert args == (DEFAULT_BASE_URL, "env-project", "env-api-key", None)


def test_logs_structured_text_uses_body_timestamp_once(patched_auth: Any) -> None:
    runner = CliRunner()
    events = _make_log_events(1)
    client = _make_logs_client(events)
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "logs", "my-app", "--no-interactive"]
        )

    assert result.exit_code == 0, result.output
    line = result.output.strip()
    assert line.startswith("pod-x/app 12:00:00.000")
    assert line.count("12:00:00") == 1


def test_logs_unstructured_text_uses_envelope_timestamp(patched_auth: Any) -> None:
    runner = CliRunner()
    event = LogEvent(
        pod="pod-x",
        container="app",
        text="plain stdout line",
        timestamp=datetime(2026, 4, 26, 12, 0, 0, tzinfo=timezone.utc),
    )
    client = _make_logs_client([event])
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "logs", "my-app", "--no-interactive"]
        )

    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        "2026-04-26T12:00:00+00:00 pod-x/app plain stdout line"
    )


def test_logs_follow_passes_follow_true(patched_auth: Any) -> None:
    runner = CliRunner()
    events = _make_log_events(2)
    client = _make_logs_client(events)
    with patch_project_client(client):
        result = runner.invoke(
            app,
            ["deployments", "logs", "my-app", "--no-interactive", "--follow"],
        )
    assert result.exit_code == 0, result.output
    kwargs = client.stream_deployment_logs.call_args.kwargs
    assert kwargs["follow"] is True


def test_logs_json_outputs_jsonl(patched_auth: Any) -> None:
    runner = CliRunner()
    events = _make_log_events(2)
    client = _make_logs_client(events)
    with patch_project_client(client):
        result = runner.invoke(
            app,
            ["deployments", "logs", "my-app", "--no-interactive", "--json"],
        )
    assert result.exit_code == 0, result.output
    lines = [ln for ln in result.output.splitlines() if ln.strip()]
    assert len(lines) == 2
    parsed = [json.loads(ln) for ln in lines]
    # Each line is a LogEvent envelope.
    for obj in parsed:
        assert obj["pod"] == "pod-x"
        assert obj["container"] == "app"
        assert "text" in obj
        assert "timestamp" in obj


def test_logs_no_events_emits_stderr_note(patched_auth: Any) -> None:
    runner = CliRunner()
    client = _make_logs_client([])
    with patch_project_client(client):
        # mix_stderr=False so we can inspect stderr separately.
        result = runner.invoke(
            app,
            ["deployments", "logs", "my-app", "--no-interactive"],
        )
    assert result.exit_code == 0, result.output
    # stderr message present in combined output (CliRunner default).
    assert "no logs available yet" in result.output


def test_logs_rejects_zero_tail(patched_auth: Any) -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["deployments", "logs", "my-app", "--no-interactive", "--tail", "0"],
    )

    assert result.exit_code != 0
    assert "Invalid value for '--tail'" in result.output


def test_logs_rejects_negative_since_seconds(patched_auth: Any) -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "deployments",
            "logs",
            "my-app",
            "--no-interactive",
            "--since-seconds",
            "-1",
        ],
    )

    assert result.exit_code != 0
    assert "Invalid value for '--since-seconds'" in result.output


def test_deployments_status_command_removed() -> None:
    """``deployments status`` was removed in Slice A.5; ``get`` covers the use case."""
    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "--help"])
    assert result.exit_code == 0
    assert "  status " not in result.output

    result = runner.invoke(app, ["deployments", "status", "--no-interactive"])
    assert result.exit_code != 0
