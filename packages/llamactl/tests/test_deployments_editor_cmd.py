# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for editor-backed deployment create/edit commands."""

from __future__ import annotations

import inspect
import subprocess
import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from click.testing import CliRunner
from conftest import make_deployment, patch_project_client
from llama_agents.cli.app import app
from llama_agents.cli.commands import deployment as deployment_cmd
from llama_agents.cli.local_context import LocalContext
from llama_agents.core.schema.deployments import DeploymentResponse
from llama_agents.core.schema.git_validation import RepositoryValidationResponse

_DEPLOY_CMD = "llama_agents.cli.commands.deployment"


def _http_404(deployment_id: str = "unknown") -> httpx.HTTPStatusError:
    request = httpx.Request(
        "GET", f"http://test/api/v1beta1/deployments/{deployment_id}"
    )
    response = httpx.Response(404, request=request, text='{"detail":"not found"}')
    return httpx.HTTPStatusError("HTTP 404", request=request, response=response)


def _editor_client_mock(
    *,
    existing: DeploymentResponse | None = None,
    created: DeploymentResponse | None = None,
) -> MagicMock:
    client = MagicMock()
    client.project_id = "proj_default"
    client.base_url = "http://test:8011"
    client.api_key = "profile-client-key"

    if existing is None:
        client.get_deployment = AsyncMock(side_effect=_http_404())
    else:

        async def _get(deployment_id: str) -> DeploymentResponse:
            if deployment_id == existing.id:
                return existing
            raise _http_404(deployment_id)

        client.get_deployment = AsyncMock(side_effect=_get)

    client.create_deployment = AsyncMock(
        return_value=created or make_deployment("new-app")
    )
    client.update_deployment = AsyncMock(
        return_value=existing or make_deployment("my-app")
    )
    client.validate_repository = AsyncMock(
        return_value=RepositoryValidationResponse(accessible=True, message="ok")
    )
    return client


def _patch_local_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        deployment_cmd,
        "gather_local_context",
        lambda: LocalContext(
            is_git_repo=True,
            repo_url="https://github.com/example/repo",
            git_ref="main",
            generate_name="scaffold-app",
            deployment_file_path="llama_deploy.yaml",
            installed_appserver_version="0.5.0",
        ),
    )


def test_create_opens_editor_with_template_and_applies_saved_yaml(
    patched_auth: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_local_context(monkeypatch)
    runner = CliRunner()
    client = _editor_client_mock(created=make_deployment("editor-app"))
    opened_texts: list[str] = []

    def _edit(filename: str) -> None:
        path = Path(filename)
        opened_texts.append(path.read_text())
        path.write_text(
            textwrap.dedent("""\
                generate_name: Editor App
                spec:
                  repo_url: https://github.com/example/repo
            """)
        )

    with patch_project_client(client), patch(f"{_DEPLOY_CMD}.click.edit", _edit):
        result = runner.invoke(app, ["deployments", "create", "--interactive"])

    assert result.exit_code == 0, result.output
    assert len(opened_texts) == 1
    assert "# generate_name: scaffold-app" in opened_texts[0]
    assert 'repo_url: ""' in opened_texts[0]
    client.create_deployment.assert_called_once()
    assert "created editor-app" in result.output


def test_create_file_applies_without_editor_and_threads_project_no_push(
    patched_auth: Any, tmp_path: Path
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("generate_name: File App\nspec:\n  repo_url: ''\n")
    client = _editor_client_mock(created=make_deployment("file-app"))

    with (
        patch_project_client(client) as ctor,
        patch(f"{_DEPLOY_CMD}.click.edit") as edit,
        patch(f"{_DEPLOY_CMD}.is_git_repo", return_value=True),
        patch(f"{_DEPLOY_CMD}.push_to_remote") as push,
    ):
        result = runner.invoke(
            app,
            [
                "deployments",
                "create",
                "-f",
                str(f),
                "--project",
                "proj_other",
                "--no-push",
            ],
        )

    assert result.exit_code == 0, result.output
    edit.assert_not_called()
    push.assert_not_called()
    args, _ = ctor.call_args
    assert args[1] == "proj_other"
    client.create_deployment.assert_called_once()


def test_edit_opens_current_template_and_updates_saved_yaml(patched_auth: Any) -> None:
    runner = CliRunner()
    existing = make_deployment("my-app", git_ref="main")
    client = _editor_client_mock(existing=existing)
    opened_texts: list[str] = []

    def _edit(filename: str) -> None:
        path = Path(filename)
        opened_texts.append(path.read_text())
        path.write_text(
            textwrap.dedent("""\
                name: my-app
                spec:
                  git_ref: v2
            """)
        )

    with patch_project_client(client), patch(f"{_DEPLOY_CMD}.click.edit", _edit):
        result = runner.invoke(app, ["deployments", "edit", "my-app", "--interactive"])

    assert result.exit_code == 0, result.output
    assert len(opened_texts) == 1
    assert "name: my-app" in opened_texts[0]
    assert "status:" not in opened_texts[0]
    assert client.get_deployment.await_count == 2
    client.update_deployment.assert_called_once()
    assert client.update_deployment.call_args[0][0] == "my-app"
    assert "updated my-app" in result.output


def test_editor_parse_error_annotates_and_reopens(patched_auth: Any) -> None:
    runner = CliRunner()
    client = _editor_client_mock(created=make_deployment("retry-app"))
    opened_texts: list[str] = []

    def _edit(filename: str) -> None:
        path = Path(filename)
        opened_texts.append(path.read_text())
        if len(opened_texts) == 1:
            path.write_text(
                textwrap.dedent("""\
                    generate_name: Retry App
                    spec:
                      bogus: nope
                """)
            )
        else:
            assert (
                "## ERROR: spec.bogus: Extra inputs are not permitted"
                in path.read_text()
            )
            path.write_text(
                textwrap.dedent("""\
                    generate_name: Retry App
                    spec:
                      repo_url: https://github.com/example/repo
                """)
            )

    with patch_project_client(client), patch(f"{_DEPLOY_CMD}.click.edit", _edit):
        result = runner.invoke(app, ["deployments", "create", "--interactive"])

    assert result.exit_code == 0, result.output
    assert len(opened_texts) == 2
    client.create_deployment.assert_called_once()
    assert "created retry-app" in result.output


def test_unchanged_editor_file_aborts_without_api_calls(
    patched_auth: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_local_context(monkeypatch)
    runner = CliRunner()
    client = _editor_client_mock()

    with patch_project_client(client), patch(f"{_DEPLOY_CMD}.click.edit") as edit:
        result = runner.invoke(app, ["deployments", "create", "--interactive"])

    assert result.exit_code == 0, result.output
    edit.assert_called_once()
    client.get_deployment.assert_not_called()
    client.create_deployment.assert_not_called()
    client.update_deployment.assert_not_called()
    client.validate_repository.assert_not_called()


@pytest.mark.parametrize("saved_text", ["", "# only comments\n\n  # still comments\n"])
def test_empty_or_all_comment_editor_file_aborts_without_api_calls(
    patched_auth: Any,
    monkeypatch: pytest.MonkeyPatch,
    saved_text: str,
) -> None:
    _patch_local_context(monkeypatch)
    runner = CliRunner()
    client = _editor_client_mock()

    def _edit(filename: str) -> None:
        Path(filename).write_text(saved_text)

    with patch_project_client(client), patch(f"{_DEPLOY_CMD}.click.edit", _edit):
        result = runner.invoke(app, ["deployments", "create", "--interactive"])

    assert result.exit_code == 0, result.output
    client.get_deployment.assert_not_called()
    client.create_deployment.assert_not_called()
    client.update_deployment.assert_not_called()
    client.validate_repository.assert_not_called()


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["deployments", "create", "--no-interactive"], "create"),
        (["deployments", "edit", "my-app", "--no-interactive"], "edit"),
    ],
)
def test_non_interactive_editor_commands_require_file(
    argv: list[str], message: str
) -> None:
    runner = CliRunner()
    result = runner.invoke(app, argv)

    assert result.exit_code != 0
    assert f"pass -f <file> for non-interactive {message}" in result.output


def test_ci_forces_editor_commands_to_require_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CI", "true")
    runner = CliRunner()

    result = runner.invoke(app, ["deployments", "edit", "my-app", "--interactive"])

    assert result.exit_code != 0
    assert "pass -f <file> for non-interactive edit" in result.output


def test_create_edit_do_not_reference_textual_deployment_form() -> None:
    assert deployment_cmd.create_deployment.callback is not None
    assert deployment_cmd.edit_deployment.callback is not None
    create_source = inspect.getsource(deployment_cmd.create_deployment.callback)
    edit_source = inspect.getsource(deployment_cmd.edit_deployment.callback)

    assert "textual.deployment_form" not in create_source
    assert "textual.deployment_form" not in edit_source
    assert "create_deployment_form" not in create_source
    assert "edit_deployment_form" not in edit_source


def test_editor_commands_do_not_import_textual_deployment_form_on_help() -> None:
    script = textwrap.dedent(
        """
        import sys
        from click.testing import CliRunner
        from llama_agents.cli.app import app

        result = CliRunner().invoke(app, ["deployments", "create", "--help"])
        if result.exit_code != 0:
            raise SystemExit(result.exit_code)
        print("llama_agents.cli.textual.deployment_form" in sys.modules)
        """
    )

    proc = subprocess.run(
        ["python", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "False"
