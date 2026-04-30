# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``deployments apply -f`` and ``delete -f`` CLI commands."""

from __future__ import annotations

import subprocess
import textwrap
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from click.testing import CliRunner
from conftest import make_deployment, patch_project_client
from llama_agents.cli.app import app
from llama_agents.core.schema.deployments import DeploymentResponse
from llama_agents.core.schema.git_validation import RepositoryValidationResponse

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _http_404(deployment_id: str = "unknown") -> httpx.HTTPStatusError:
    request = httpx.Request(
        "GET", f"http://test/api/v1beta1/deployments/{deployment_id}"
    )
    response = httpx.Response(404, request=request, text='{"detail":"not found"}')
    return httpx.HTTPStatusError("HTTP 404", request=request, response=response)


def _http_409() -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://test/api/v1beta1/deployments")
    response = httpx.Response(
        409, request=request, text='{"detail":"conflict: deployment already exists"}'
    )
    return httpx.HTTPStatusError("HTTP 409", request=request, response=response)


def _apply_client_mock(
    *,
    existing: DeploymentResponse | None = None,
    created: DeploymentResponse | None = None,
    validate_accessible: bool = True,
) -> MagicMock:
    """Mock client for apply tests."""
    client = MagicMock()
    client.project_id = "proj_default"
    client.base_url = "http://test:8011"

    if existing:

        async def _get(
            deployment_id: str, include_events: bool = False
        ) -> DeploymentResponse:
            if deployment_id == existing.id:
                return existing
            raise _http_404(deployment_id)

        client.get_deployment = AsyncMock(side_effect=_get)
    else:
        client.get_deployment = AsyncMock(
            side_effect=lambda *a, **kw: (_ for _ in ()).throw(_http_404())
        )

    if created:
        client.create_deployment = AsyncMock(return_value=created)
    else:
        client.create_deployment = AsyncMock(return_value=make_deployment("new-app"))

    client.update_deployment = AsyncMock(
        return_value=existing or make_deployment("my-app")
    )

    async def _validate(
        repo_url: str,
        deployment_id: str | None = None,
        pat: str | None = None,
    ) -> RepositoryValidationResponse:
        return RepositoryValidationResponse(
            accessible=validate_accessible,
            message="ok" if validate_accessible else "repo not found",
        )

    client.validate_repository = AsyncMock(side_effect=_validate)
    client.delete_deployment = AsyncMock()

    return client


MINIMAL_CREATE_YAML = textwrap.dedent("""\
    name: new-app
    generate_name: New App
    spec:
      repo_url: https://github.com/example/repo
""")

MINIMAL_UPDATE_YAML = textwrap.dedent("""\
    name: my-app
    spec:
      git_ref: v2
""")


def test_apply_creates_when_not_found(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock(created=make_deployment("new-app"))
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    client.create_deployment.assert_called_once()
    assert "created" in result.output.lower()
    assert "new-app" in result.output


def test_apply_updates_when_exists(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_UPDATE_YAML)

    existing = make_deployment("my-app")
    client = _apply_client_mock(existing=existing)
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    client.update_deployment.assert_called_once()
    call_args = client.update_deployment.call_args
    assert call_args[0][0] == "my-app"
    assert "updated" in result.output.lower()
    assert "my-app" in result.output


def test_apply_reads_stdin(patched_auth: Any) -> None:
    runner = CliRunner()
    client = _apply_client_mock(created=make_deployment("new-app"))
    with patch_project_client(client):
        result = runner.invoke(
            app,
            ["deployments", "apply", "-f", "-"],
            input=MINIMAL_CREATE_YAML,
        )

    assert result.exit_code == 0, result.output
    client.create_deployment.assert_called_once()
    assert "new-app" in result.output


def test_apply_generate_name_only(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        generateName: My App
        spec:
          repo_url: https://github.com/example/repo
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    server_returned = make_deployment("my-app-xyz", display_name="My App")
    client = _apply_client_mock(created=server_returned)
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    client.create_deployment.assert_called_once()
    create_payload = client.create_deployment.call_args[0][0]
    assert create_payload.id is None
    # The server-assigned id should appear in output.
    assert "my-app-xyz" in result.output


def test_apply_generate_name_aliases_produce_same_create(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    variants = ["generateName", "generate_name", "display_name"]
    for alias in variants:
        yaml_text = textwrap.dedent(f"""\
            {alias}: My App
            spec:
              repo_url: https://github.com/example/repo
        """)
        f = tmp_path / "deploy.yaml"
        f.write_text(yaml_text)

        client = _apply_client_mock(created=make_deployment("my-app"))
        with patch_project_client(client):
            result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])
        assert result.exit_code == 0, f"failed for alias {alias}: {result.output}"
        client.create_deployment.assert_called_once()


def test_apply_409_surfaces_error(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock()
    client.create_deployment = AsyncMock(side_effect=_http_409())
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    # HTTPStatusError is re-raised directly; the 409 info surfaces either
    # in output (if Click wraps it) or in the exception object.
    assert "409" in result.output or (
        result.exception is not None and "409" in str(result.exception)
    )


def test_apply_dry_run_named(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f), "--dry-run"])

    assert result.exit_code == 0, result.output
    client.get_deployment.assert_not_called()
    client.create_deployment.assert_not_called()
    client.update_deployment.assert_not_called()
    assert "would" in result.output.lower()
    assert "upsert" in result.output.lower()


def test_apply_dry_run_generate_name_only(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        generateName: My App
        spec:
          repo_url: https://github.com/example/repo
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f), "--dry-run"])

    assert result.exit_code == 0, result.output
    client.get_deployment.assert_not_called()
    client.create_deployment.assert_not_called()
    assert "would" in result.output.lower()
    assert "create" in result.output.lower()


def test_apply_dry_run_server_errors(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--dry-run=server"]
        )

    assert result.exit_code != 0
    assert "not" in result.output.lower() and "supported" in result.output.lower()


def test_apply_no_name_no_generate_name_errors(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        spec:
          repo_url: https://github.com/example/repo
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    output_lower = result.output.lower()
    assert "name" in output_lower or "generate_name" in output_lower


def test_apply_name_without_generate_name_404_errors(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        name: new-app
        spec:
          repo_url: https://github.com/example/repo
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()  # get_deployment raises 404
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    assert (
        "generate_name" in result.output.lower()
        or "generatename" in result.output.lower()
    )


def test_apply_validate_repository_blocks_create(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock(validate_accessible=False)
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    client.create_deployment.assert_not_called()


def test_apply_push_mode_skips_validate_repository(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: ""
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f), "--no-push"])

    assert result.exit_code == 0, result.output
    client.validate_repository.assert_not_called()


def test_apply_env_var_resolves(
    patched_auth: Any, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MY_REPO_URL", "https://github.com/env-resolved/repo")
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        name: env-app
        generate_name: Env App
        spec:
          repo_url: ${MY_REPO_URL}
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock(
        created=make_deployment(
            "env-app", repo_url="https://github.com/env-resolved/repo"
        )
    )
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    # The resolved URL should have been passed to the client.
    create_payload = client.create_deployment.call_args[0][0]
    assert create_payload.repo_url == "https://github.com/env-resolved/repo"


def test_apply_unresolved_env_var_errors(
    patched_auth: Any, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("NONEXISTENT_VAR_FOR_TEST", raising=False)
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        name: env-app
        generate_name: Env App
        spec:
          repo_url: ${NONEXISTENT_VAR_FOR_TEST}
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    assert "NONEXISTENT_VAR_FOR_TEST" in result.output


def test_delete_from_file(patched_auth: Any, tmp_path: Any) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        name: doomed-app
        spec:
          repo_url: https://github.com/example/repo
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "delete", "-f", str(f), "--no-interactive"]
        )

    assert result.exit_code == 0, result.output
    client.delete_deployment.assert_called_once()
    call_args = client.delete_deployment.call_args
    assert call_args[0][0] == "doomed-app"


def test_delete_file_and_positional_mutually_exclusive(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    yaml_text = "name: my-app\nspec: {}\n"
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app,
            ["deployments", "delete", "my-app", "-f", str(f), "--no-interactive"],
        )

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()


def test_delete_reads_stdin(patched_auth: Any) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        name: stdin-app
        spec:
          repo_url: https://github.com/example/repo
    """)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app,
            ["deployments", "delete", "-f", "-", "--no-interactive"],
            input=yaml_text,
        )

    assert result.exit_code == 0, result.output
    client.delete_deployment.assert_called_once()
    call_args = client.delete_deployment.call_args
    assert call_args[0][0] == "stdin-app"


def _push_mode_client(
    *,
    existing_repo_url: str = "internal://",
    deployment_id: str = "my-app",
) -> MagicMock:
    """Client mock for a push-mode deployment."""
    existing = make_deployment(deployment_id, repo_url=existing_repo_url)
    client = _apply_client_mock(existing=existing)
    client.update_deployment = AsyncMock(return_value=existing)
    return client


_DEPLOY_CMD = "llama_agents.cli.commands.deployment"


@contextmanager
def _patched_git_push(
    *, returncode: int = 0, stderr: bytes = b""
) -> Generator[MagicMock, None, None]:
    """Patch the three git-push helpers so push-mode tests don't hit real git.

    Yields the ``push_to_remote`` mock for assertions.
    """
    with (
        patch(f"{_DEPLOY_CMD}.configure_git_remote", return_value="llamaagents-test"),
        patch(
            f"{_DEPLOY_CMD}.push_to_remote",
            return_value=subprocess.CompletedProcess([], returncode, stderr=stderr),
        ) as mock_push,
        patch(f"{_DEPLOY_CMD}.get_api_key", return_value="test-key"),
    ):
        yield mock_push


def test_apply_push_mode_create_does_save_then_push(
    patched_auth: Any, tmp_path: Any
) -> None:
    """Create with repo_url="" → save first (POST), then push."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("generate_name: My App\nspec:\n  repo_url: ''\n  git_ref: main\n")

    client = _apply_client_mock()
    with patch_project_client(client), _patched_git_push() as mock_push:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    assert "created new-app" in result.output
    client.create_deployment.assert_called_once()
    mock_push.assert_called_once()


def test_apply_push_mode_update_does_push_then_save(
    patched_auth: Any, tmp_path: Any
) -> None:
    """Existing push-mode + desired push-mode → push first, then save."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  git_ref: feature-branch\n")

    client = _push_mode_client()
    with patch_project_client(client), _patched_git_push() as mock_push:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    assert "updated my-app" in result.output
    mock_push.assert_called_once()
    client.update_deployment.assert_called_once()


def test_apply_push_then_save_push_failure_aborts(
    patched_auth: Any, tmp_path: Any
) -> None:
    """Push-then-save: if push fails, update must NOT be called."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  git_ref: main\n")

    client = _push_mode_client()
    with (
        patch_project_client(client),
        _patched_git_push(returncode=1, stderr=b"push rejected"),
    ):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    assert "push failed" in result.output.lower() or "push rejected" in result.output
    client.update_deployment.assert_not_called()


def test_apply_save_then_push_push_failure_shows_recovery(
    patched_auth: Any, tmp_path: Any
) -> None:
    """Save-then-push: if push fails after save, show recovery hint."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("generate_name: My App\nspec:\n  repo_url: ''\n")

    client = _apply_client_mock()
    with (
        patch_project_client(client),
        _patched_git_push(returncode=1, stderr=b"auth failed"),
    ):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    assert "created new-app" in result.output
    assert "re-run" in result.output.lower()


def test_apply_no_push_skips_push_with_warning(
    patched_auth: Any, tmp_path: Any
) -> None:
    """--no-push skips push and prints a warning."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  git_ref: main\n")

    client = _push_mode_client()
    with patch_project_client(client), _patched_git_push() as mock_push:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f), "--no-push"])

    assert result.exit_code == 0, result.output
    assert "updated my-app" in result.output
    mock_push.assert_not_called()


def test_apply_external_to_push_mode_does_save_then_push(
    patched_auth: Any, tmp_path: Any
) -> None:
    """Switching from external repo to push-mode → save then push."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  repo_url: ''\n")

    client = _push_mode_client(existing_repo_url="https://github.com/org/repo")
    with patch_project_client(client), _patched_git_push() as mock_push:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    assert "updated my-app" in result.output
    client.update_deployment.assert_called_once()
    mock_push.assert_called_once()


def test_apply_push_to_external_does_save_only(
    patched_auth: Any, tmp_path: Any
) -> None:
    """Switching from push-mode to external → save only, no push."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  repo_url: https://github.com/org/new-repo\n")

    client = _push_mode_client()
    with patch_project_client(client), _patched_git_push() as mock_push:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    assert "updated my-app" in result.output
    mock_push.assert_not_called()
