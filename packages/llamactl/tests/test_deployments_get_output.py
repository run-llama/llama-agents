# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``deployments get`` output modes, ``--project`` override, and the
``deployments list`` hidden alias."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import yaml
from click.testing import CliRunner
from conftest import make_deployment, patch_project_client
from llama_agents.cli.app import app
from llama_agents.core.schema.deployments import (
    DeploymentHistoryResponse,
    DeploymentResponse,
    ReleaseHistoryItem,
)


def _make_client_mock(deployments: list[DeploymentResponse]) -> MagicMock:
    """A mock ProjectClient stand-in with the methods the commands hit."""

    async def _list() -> list[DeploymentResponse]:
        return list(deployments)

    async def _get(
        deployment_id: str, include_events: bool = False
    ) -> DeploymentResponse:
        for d in deployments:
            if d.id == deployment_id:
                return d
        raise RuntimeError(f"deployment not found: {deployment_id}")

    client = MagicMock()
    client.list_deployments.side_effect = _list
    client.get_deployment.side_effect = _get
    client.project_id = "proj_default"
    client.base_url = "http://test:8011"
    return client


def test_deployments_get_text_no_args_lists(patched_auth: Any) -> None:
    runner = CliRunner()
    deployments = [make_deployment("app-a"), make_deployment("app-b")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(app, ["deployments", "get", "--no-interactive"])
    assert result.exit_code == 0, result.output
    assert "app-a" in result.output
    assert "app-b" in result.output
    # Plain-table headers, no Rich markup, no truncation ellipsis.
    assert "NAME" in result.output
    assert "PHASE" in result.output
    assert "\x1b[" not in result.output  # no ANSI escapes
    assert "…" not in result.output


def test_deployments_get_json_array(patched_auth: Any) -> None:
    runner = CliRunner()
    deployments = [make_deployment("app-a"), make_deployment("app-b")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "--no-interactive", "-o", "json"]
        )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert {d["name"] for d in data} == {"app-a", "app-b"}
    # All deployments returned should expose ``status``.
    assert all("status" in d for d in data)
    assert all("phase" in d["status"] for d in data)
    # Deprecated aliases / leaked flags must not appear.
    for d in data:
        assert "id" not in d
        assert "llama_deploy_version" not in d
        assert "has_personal_access_token" not in d
        assert "secret_names" not in d


def test_deployments_get_yaml_list(patched_auth: Any) -> None:
    runner = CliRunner()
    deployments = [make_deployment("only-one")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "--no-interactive", "-o", "yaml"]
        )
    assert result.exit_code == 0, result.output
    parsed = yaml.safe_load(result.output)
    assert isinstance(parsed, list)
    assert parsed[0]["name"] == "only-one"


def test_deployments_get_single_text_no_tui(patched_auth: Any) -> None:
    """``deployments get <name>`` should never launch the Textual monitor."""
    runner = CliRunner()
    deployments = [make_deployment("my-app")]
    client_mock = _make_client_mock(deployments)
    with (
        patch_project_client(client_mock),
        patch(
            "llama_agents.cli.textual.deployment_monitor.monitor_deployment_screen"
        ) as mock_monitor,
    ):
        # Even if interactive=True, get must print a table — TUI is gone.
        result = runner.invoke(app, ["deployments", "get", "my-app", "--interactive"])
    assert result.exit_code == 0, result.output
    assert mock_monitor.call_count == 0
    assert "my-app" in result.output
    # Single-row uses the same column layout as the list view.
    assert "NAME" in result.output
    assert "PHASE" in result.output


def test_deployments_get_single_json(patched_auth: Any) -> None:
    runner = CliRunner()
    deployments = [make_deployment("my-app")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "my-app", "--no-interactive", "-o", "json"]
        )
    assert result.exit_code == 0, result.output
    obj = json.loads(result.output)
    assert isinstance(obj, dict)
    assert obj["name"] == "my-app"
    assert "spec" in obj
    assert obj["status"]["phase"] == "Running"
    assert obj["status"]["project_id"] == "proj_default"
    # warning is always-explicit-null
    assert obj["status"]["warning"] is None
    # No deprecated aliases / leaks
    assert "id" not in obj
    assert "llama_deploy_version" not in obj
    assert "has_personal_access_token" not in obj
    assert "secret_names" not in obj
    # Empty secrets / no PAT means the keys are omitted entirely from spec.
    assert "secrets" not in obj["spec"]
    assert "personal_access_token" not in obj["spec"]


def test_deployments_get_single_yaml(patched_auth: Any) -> None:
    runner = CliRunner()
    deployments = [make_deployment("my-app")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "my-app", "--no-interactive", "-o", "yaml"]
        )
    assert result.exit_code == 0, result.output
    obj = yaml.safe_load(result.output)
    assert isinstance(obj, dict)
    assert obj["name"] == "my-app"
    assert obj["status"]["phase"] == "Running"


def test_deployments_get_strips_secret_mask_sentinels(patched_auth: Any) -> None:
    """``********`` mask sentinels never reach structured output; the keys
    drop entirely so a ``get | edit | apply`` round-trip can't push the mask
    back as the value."""
    runner = CliRunner()
    deployments = [
        make_deployment(
            "secret-app",
            secret_names=["LLAMA_CLOUD_API_KEY", "OPENAI_API_KEY"],
            has_personal_access_token=True,
        )
    ]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "secret-app", "--no-interactive", "-o", "json"]
        )
    assert result.exit_code == 0, result.output
    obj = json.loads(result.output)
    assert "secrets" not in obj["spec"]
    assert "personal_access_token" not in obj["spec"]
    assert "********" not in result.output


def test_deployments_get_empty_json_is_array(patched_auth: Any) -> None:
    runner = CliRunner()
    client_mock = _make_client_mock([])
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "--no-interactive", "-o", "json"]
        )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == []


def test_deployments_list_hidden_alias_works(patched_auth: Any) -> None:
    """The hidden ``deployments list`` alias should still produce JSON output."""
    runner = CliRunner()
    deployments = [make_deployment("app-a")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "list", "--no-interactive", "-o", "json"]
        )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert [d["name"] for d in data] == ["app-a"]


def test_deployments_list_hidden_in_help(patched_auth: Any) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "--help"])
    assert result.exit_code == 0
    # `list` should be hidden (not surfaced in `--help`), but `get` should be.
    assert "  list " not in result.output
    assert "  get " in result.output


def test_deployments_get_project_override_threads_to_client(
    patched_auth: Any,
) -> None:
    """``--project foo`` should construct a ProjectClient with project_id='foo'."""
    runner = CliRunner()
    deployments = [make_deployment("app-a", project_id="proj_other")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock) as ctor:
        result = runner.invoke(
            app,
            [
                "deployments",
                "get",
                "--no-interactive",
                "--project",
                "proj_other",
                "-o",
                "json",
            ],
        )
    assert result.exit_code == 0, result.output
    # ProjectClient was called with project_id='proj_other'
    args, kwargs = ctor.call_args
    # Positional: (api_url, project_id, api_key, auth_middleware)
    assert args[1] == "proj_other"


def _full_sha(prefix: str) -> str:
    """Return a 40-char hex string starting with ``prefix`` for history tests."""
    return (prefix + "0" * 40)[:40]


def _history_client_mock(items: list[ReleaseHistoryItem]) -> MagicMock:
    client_mock = _make_client_mock([make_deployment("my-app")])

    async def _hist(deployment_id: str) -> DeploymentHistoryResponse:
        return DeploymentHistoryResponse(
            deployment_id=deployment_id, history=list(items)
        )

    client_mock.get_deployment_history.side_effect = _hist
    return client_mock


def test_deployments_history_json_output(patched_auth: Any) -> None:
    runner = CliRunner()
    items = [
        ReleaseHistoryItem(
            git_sha=_full_sha("aaaaaaa1111"),
            released_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        ReleaseHistoryItem(
            git_sha=_full_sha("bbbbbbb2222"),
            released_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ),
    ]
    client_mock = _history_client_mock(items)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app,
            [
                "deployments",
                "history",
                "my-app",
                "--no-interactive",
                "-o",
                "json",
            ],
        )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert isinstance(data, list)
    # Newest first
    assert data[0]["git_sha"] == _full_sha("bbbbbbb2222")
    assert data[1]["git_sha"] == _full_sha("aaaaaaa1111")
    # JSON keeps full 40-char shas.
    assert all(len(d["git_sha"]) == 40 for d in data)
    # JSON timestamps are Z-suffixed (Pydantic default).
    assert all(d["released_at"].endswith("Z") for d in data)


def test_deployments_history_text_short_sha_and_z_timestamp(
    patched_auth: Any,
) -> None:
    runner = CliRunner()
    full_sha = _full_sha("640f764")
    items = [
        ReleaseHistoryItem(
            git_sha=full_sha,
            released_at=datetime(2026, 4, 25, 15, 1, 15, tzinfo=timezone.utc),
            image_tag="0.11.1",
        ),
    ]
    client_mock = _history_client_mock(items)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app,
            ["deployments", "history", "my-app", "--no-interactive"],
        )
    assert result.exit_code == 0, result.output
    # Header present.
    assert "RELEASED_AT" in result.output
    assert "GIT_SHA" in result.output
    assert "IMAGE_TAG" in result.output
    # Z-suffixed timestamp; no +00:00.
    assert "2026-04-25T15:01:15Z" in result.output
    assert "+00:00" not in result.output
    # Short sha (7 chars) only — full sha must not appear.
    assert "640f764" in result.output
    assert full_sha not in result.output
    assert "0.11.1" in result.output


def _http_status_error(
    status: int, *, url: str = "http://internal/api"
) -> httpx.HTTPStatusError:
    """Build an HTTPStatusError with a real Response for friendly-error tests."""
    request = httpx.Request("GET", url)
    response = httpx.Response(status, request=request, text='{"detail":"x"}')
    return httpx.HTTPStatusError(
        f"HTTP {status} for url {url} - {response.text}",
        request=request,
        response=response,
    )


def test_deployments_get_404_renders_friendly_message(patched_auth: Any) -> None:
    runner = CliRunner()
    client_mock = _make_client_mock([])

    async def _raise_404(deployment_id: str, include_events: bool = False) -> None:
        raise _http_status_error(404)

    client_mock.get_deployment.side_effect = _raise_404
    with patch_project_client(client_mock):
        result = runner.invoke(
            app,
            ["deployments", "get", "nonexistent-app", "--no-interactive"],
        )
    assert result.exit_code != 0
    assert (
        "deployment 'nonexistent-app' not found in project 'proj_default'"
        in result.output
    )
    # No URL, no JSON body should leak through.
    assert "http://" not in result.output
    assert "detail" not in result.output


def test_deployments_get_404_with_project_includes_project(
    patched_auth: Any,
) -> None:
    runner = CliRunner()
    client_mock = _make_client_mock([])
    client_mock.project_id = "proj_other"

    async def _raise_404(deployment_id: str, include_events: bool = False) -> None:
        raise _http_status_error(404)

    client_mock.get_deployment.side_effect = _raise_404
    with patch_project_client(client_mock):
        result = runner.invoke(
            app,
            [
                "deployments",
                "get",
                "nonexistent-app",
                "--no-interactive",
                "--project",
                "proj_other",
            ],
        )
    assert result.exit_code != 0
    assert (
        "deployment 'nonexistent-app' not found in project 'proj_other'"
        in result.output
    )


def test_deployments_get_500_keeps_verbose_message(patched_auth: Any) -> None:
    runner = CliRunner()
    client_mock = _make_client_mock([])

    async def _raise_500(deployment_id: str, include_events: bool = False) -> None:
        raise _http_status_error(500)

    client_mock.get_deployment.side_effect = _raise_500
    with patch_project_client(client_mock):
        result = runner.invoke(
            app,
            ["deployments", "get", "boom", "--no-interactive"],
        )
    assert result.exit_code != 0
    # Non-404 keeps the verbose default message (URL + body) for debug-visibility.
    assert "HTTP 500" in result.output
    assert "http://" in result.output


def test_deployments_get_text_column_order(patched_auth: Any) -> None:
    """Text mode columns are in declaration order: NAME, REPO, GIT_REF, PHASE."""
    runner = CliRunner()
    deployments = [make_deployment("app-a")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(app, ["deployments", "get", "--no-interactive"])
    assert result.exit_code == 0, result.output
    header = result.output.splitlines()[0]
    name_idx = header.index("NAME")
    repo_idx = header.index("REPO")
    ref_idx = header.index("GIT_REF")
    phase_idx = header.index("PHASE")
    assert name_idx < repo_idx < ref_idx < phase_idx


def test_deployments_get_text_no_wide_columns(patched_auth: Any) -> None:
    """``-o text`` excludes wide-only columns (GIT_SHA, APISERVER_URL, ...)."""
    runner = CliRunner()
    deployments = [make_deployment("app-a")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(app, ["deployments", "get", "--no-interactive"])
    assert result.exit_code == 0, result.output
    assert "GIT_SHA" not in result.output
    assert "APISERVER_URL" not in result.output
    assert "PROJECT" not in result.output
    assert "APPSERVER" not in result.output
    assert "SUSPENDED" not in result.output


def test_deployments_get_wide_includes_extra_columns(patched_auth: Any) -> None:
    runner = CliRunner()
    deployments = [
        make_deployment("app-a", git_sha="abc1234567", appserver_version="0.4.2")
    ]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "--no-interactive", "-o", "wide"]
        )
    assert result.exit_code == 0, result.output
    header = result.output.splitlines()[0]
    # Default columns still present; wide columns now appear too.
    for h in ("NAME", "REPO", "GIT_REF", "PHASE", "APPSERVER", "GIT_SHA"):
        assert h in header
    # Wide columns slot into their natural positions, interleaved.
    # APPSERVER (spec) should appear before PHASE (status).
    assert header.index("APPSERVER") < header.index("PHASE")


def test_deployments_get_template_single_emits_apply_shape(patched_auth: Any) -> None:
    runner = CliRunner()
    deployments = [
        make_deployment(
            "my-app",
            secret_names=["OPENAI_API_KEY"],
            has_personal_access_token=True,
            appserver_version="0.5.0",
        )
    ]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app,
            ["deployments", "get", "my-app", "--no-interactive", "-o", "template"],
        )
    assert result.exit_code == 0, result.output
    out = result.output
    parsed = yaml.safe_load(out)
    assert parsed["name"] == "my-app"
    assert "spec" in parsed
    # Status is omitted from template output unconditionally.
    assert "status" not in parsed
    assert "phase" not in out
    # Doc comments above fields.
    assert "## Stable id for the deployment" in out
    # Masked secrets / PAT are stripped at the emit boundary — they must not
    # round-trip as literal ``********`` values into apply input.
    assert "********" not in out
    assert "secrets" not in (parsed["spec"] or {})
    assert "personal_access_token" not in (parsed["spec"] or {})


def test_deployments_get_template_does_not_scaffold_generate_name(
    patched_auth: Any,
) -> None:
    """``get -o template`` is a faithful projection of an existing deployment;
    it does not emit the ``# generate_name`` scaffolding line that the offline
    ``deployments template`` command does."""
    runner = CliRunner()
    deployments = [make_deployment("my-app", display_name="My App")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app,
            ["deployments", "get", "my-app", "--no-interactive", "-o", "template"],
        )
    assert result.exit_code == 0, result.output
    assert "generate_name" not in result.output
    assert "generateName" not in result.output


def test_deployments_get_yaml_emits_generate_name_at_top_level(
    patched_auth: Any,
) -> None:
    """``-o yaml`` lifts ``display_name`` out of ``spec`` to the top level
    as ``generate_name`` (sibling to ``name``)."""
    runner = CliRunner()
    deployments = [make_deployment("my-app", display_name="My App")]
    client_mock = _make_client_mock(deployments)
    with patch_project_client(client_mock):
        result = runner.invoke(
            app, ["deployments", "get", "my-app", "--no-interactive", "-o", "yaml"]
        )
    assert result.exit_code == 0, result.output
    obj = yaml.safe_load(result.output)
    assert obj["generate_name"] == "My App"
    assert "display_name" not in obj
    assert "display_name" not in obj["spec"]


def test_deployments_get_template_no_name_errors(patched_auth: Any) -> None:
    """``deployments get -o template`` without a deployment name errors clearly."""
    runner = CliRunner()
    # No client interaction expected — fail fast before list_deployments.
    result = runner.invoke(
        app, ["deployments", "get", "--no-interactive", "-o", "template"]
    )
    assert result.exit_code != 0
    assert "template requires a deployment name" in result.output


def test_other_commands_reject_template_mode(patched_auth: Any) -> None:
    """``-o template`` is only meaningful for ``deployments get``."""
    runner = CliRunner()
    result = runner.invoke(app, ["auth", "list", "-o", "template"])
    # The choice is accepted but render_output rejects it with a clear message.
    assert result.exit_code != 0
    assert "only supported for" in result.output or "template" in result.output
