# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``deployments apply -f`` and ``delete -f`` CLI commands."""

from __future__ import annotations

import asyncio
import subprocess
import textwrap
from collections.abc import Generator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import llama_agents.cli.config.env_service as env_service
import pytest
from click.testing import CliRunner
from conftest import make_deployment, patch_project_client
from llama_agents.cli.app import app
from llama_agents.core.schema.deployments import DeploymentResponse
from llama_agents.core.schema.git_validation import RepositoryValidationResponse

DEFAULT_BASE_URL = "https://api.cloud.llamaindex.ai"

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


def _http_422_detail(detail: list[dict[str, Any]]) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://test/api/v1beta1/deployments")
    response = httpx.Response(422, request=request, json={"detail": detail})
    return httpx.HTTPStatusError("HTTP 422", request=request, response=response)


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
    client.api_key = "profile-client-key"

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


def test_apply_uses_complete_env_auth_without_profile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "env-api-key")
    monkeypatch.setenv("LLAMA_DEPLOY_PROJECT_ID", "env-project")
    _patch_no_profile_auth(monkeypatch)

    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock(created=make_deployment("new-app"))
    client.project_id = "env-project"
    client.base_url = DEFAULT_BASE_URL
    client.api_key = "env-api-key"
    with patch_project_client(client) as ctor:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    assert "created new-app" in result.output
    args, _ = ctor.call_args
    assert args == (DEFAULT_BASE_URL, "env-project", "env-api-key", None)


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


def test_apply_update_uses_one_event_loop(patched_auth: Any, tmp_path: Any) -> None:
    loop: asyncio.AbstractEventLoop | None = None

    def check_loop() -> None:
        nonlocal loop
        running = asyncio.get_running_loop()
        if loop is None:
            loop = running
        elif running is not loop:
            raise RuntimeError("Event loop is closed")

    async def get_deployment(deployment_id: str) -> DeploymentResponse:
        check_loop()
        return make_deployment(deployment_id)

    async def update_deployment(deployment_id: str, payload: Any) -> DeploymentResponse:
        check_loop()
        return make_deployment(deployment_id)

    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_UPDATE_YAML)

    client = MagicMock()
    client.project_id = "proj_default"
    client.base_url = "http://test:8011"
    client.get_deployment = AsyncMock(side_effect=get_deployment)
    client.update_deployment = AsyncMock(side_effect=update_deployment)
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    client.update_deployment.assert_called_once()
    assert "updated" in result.output.lower()


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
        generate_name: My App
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
        generate_name: My App
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


def test_apply_dry_run_validates_appserver_version(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
          appserver_version: "1!0.11.3"
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f), "--dry-run"])

    assert result.exit_code != 0
    assert "invalid appserver_version" in result.output
    client.get_deployment.assert_not_called()
    client.create_deployment.assert_not_called()
    client.validate_repository.assert_not_called()


def test_apply_dry_run_masks_resolved_secret_values(
    patched_auth: Any, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("API_KEY_FOR_DRY_RUN", "sk-test-secret")
    monkeypatch.setenv("PAT_FOR_DRY_RUN", "ghp-test-secret")
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
          secrets:
            API_KEY: ${API_KEY_FOR_DRY_RUN}
          personal_access_token: ${PAT_FOR_DRY_RUN}
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f), "--dry-run"])

    assert result.exit_code == 0, result.output
    client.create_deployment.assert_not_called()
    assert "sk-test-secret" not in result.output
    assert "ghp-test-secret" not in result.output
    assert "API_KEY: '********'" in result.output
    assert "personal_access_token: '********'" in result.output


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
    assert "generate_name" in result.output.lower()


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


def test_apply_validates_payload_before_repository(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    yaml_text = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
          appserver_version: "1!0.11.3"
    """)
    f = tmp_path / "deploy.yaml"
    f.write_text(yaml_text)

    client = _apply_client_mock(validate_accessible=False)
    with patch_project_client(client):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code != 0
    assert "invalid appserver_version" in result.output
    client.validate_repository.assert_not_called()
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
    with patch_project_client(client), _patched_git_push():
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

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


def test_apply_annotate_parse_error_rewrites_file(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  bogus: nope\n")

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--annotate-on-error"]
        )

    assert result.exit_code == 1
    assert "apply failed; see annotations" in result.output
    assert "## ERROR: spec.bogus: Extra inputs are not permitted" in f.read_text()
    client.create_deployment.assert_not_called()


def test_apply_annotate_unresolved_env_var_rewrites_file(
    patched_auth: Any, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GITHUB_TOKEN_FOR_ANNOTATION", raising=False)
    monkeypatch.delenv("OPENAI_KEY_FOR_ANNOTATION", raising=False)
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(
        textwrap.dedent("""\
            generate_name: Env App
            spec:
              repo_url: https://github.com/example/repo
              personal_access_token: ${GITHUB_TOKEN_FOR_ANNOTATION}
              secrets:
                OPENAI_API_KEY: ${OPENAI_KEY_FOR_ANNOTATION}
        """)
    )

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--annotate-on-error"]
        )

    assert result.exit_code == 1
    annotated = f.read_text()
    assert (
        "  ## ERROR: unresolved environment variables: "
        "GITHUB_TOKEN_FOR_ANNOTATION\n"
        "  personal_access_token:"
    ) in annotated
    assert (
        "    ## ERROR: unresolved environment variables: "
        "OPENAI_KEY_FOR_ANNOTATION\n"
        "    OPENAI_API_KEY:"
    ) in annotated
    client.create_deployment.assert_not_called()


def test_apply_annotate_stdin_writes_yaml_to_stdout(patched_auth: Any) -> None:
    runner = CliRunner()
    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app,
            ["deployments", "apply", "-f", "-", "--annotate-on-error"],
            input="name: new-app\nspec:\n  repo_url: https://github.com/example/repo\n",
        )

    assert result.exit_code == 1
    assert (
        "## ERROR: generate_name: deployment not found and no generate_name provided "
        "for create"
    ) in result.output
    assert "generate_name" in result.output


def test_apply_annotate_dry_run_is_non_mutating(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    original = "spec:\n  repo_url: https://github.com/example/repo\n"
    f.write_text(original)

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app,
            [
                "deployments",
                "apply",
                "-f",
                str(f),
                "--dry-run",
                "--annotate-on-error",
            ],
        )

    assert result.exit_code != 0
    assert f.read_text() == original
    assert "generate_name" in result.output


def test_apply_annotate_repository_failure_targets_repo_url(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock(validate_accessible=False)
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--annotate-on-error"]
        )

    assert result.exit_code == 1
    assert "  ## ERROR: repo not found\n  repo_url:" in f.read_text()
    client.create_deployment.assert_not_called()


def test_apply_annotate_server_validation_remaps_body_loc(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(MINIMAL_CREATE_YAML)

    client = _apply_client_mock()
    client.create_deployment = AsyncMock(
        side_effect=_http_422_detail(
            [
                {
                    "loc": ["body", "repo_url"],
                    "msg": "invalid repository URL",
                    "type": "value_error",
                }
            ]
        )
    )
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--annotate-on-error"]
        )

    assert result.exit_code == 1
    assert "  ## ERROR: invalid repository URL\n  repo_url:" in f.read_text()


def test_apply_annotate_create_secret_null_targets_secret(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(
        textwrap.dedent("""\
            generate_name: My App
            spec:
              repo_url: https://github.com/example/repo
              secrets:
                DELETE_ME: null
        """)
    )

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--annotate-on-error"]
        )

    assert result.exit_code == 1
    assert "    ## ERROR: cannot delete secrets on create" in f.read_text()
    client.create_deployment.assert_not_called()


def test_apply_annotate_invalid_appserver_version_targets_field(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text(
        textwrap.dedent("""\
            generate_name: My App
            spec:
              repo_url: ""
              appserver_version: tilt-dev
        """)
    )

    client = _apply_client_mock()
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--annotate-on-error"]
        )

    assert result.exit_code == 1
    assert "  ## ERROR: invalid appserver_version" in f.read_text()
    assert "  appserver_version: tilt-dev" in f.read_text()
    client.create_deployment.assert_not_called()


def test_apply_annotate_save_then_push_failure_preserves_recovery(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("generate_name: My App\nspec:\n  repo_url: ''\n")

    client = _apply_client_mock()
    with (
        patch_project_client(client),
        _patched_git_push(returncode=1, stderr=b"auth failed"),
    ):
        result = runner.invoke(
            app, ["deployments", "apply", "-f", str(f), "--annotate-on-error"]
        )

    assert result.exit_code == 1
    annotated = f.read_text()
    assert "push failed: auth failed" in annotated
    assert "re-run `llamactl deployments apply -f <file>`" in annotated
    assert "created new-app" not in annotated


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
        patch(f"{_DEPLOY_CMD}.get_api_key", return_value="test-key", create=True),
    ):
        yield mock_push


def test_configure_git_remote_uses_profile_project_client_api_key(
    patched_auth: Any,
) -> None:
    runner = CliRunner()
    client = _apply_client_mock()
    client.api_key = "profile-client-key"

    with (
        patch_project_client(client),
        patch(f"{_DEPLOY_CMD}.is_git_repo", return_value=True),
        patch(
            f"{_DEPLOY_CMD}.configure_git_remote", return_value="llamaagents-my-app"
        ) as mock_configure,
        patch(
            f"{_DEPLOY_CMD}.get_api_key",
            return_value="profile-state-key",
            create=True,
        ) as mock_get_api_key,
    ):
        result = runner.invoke(
            app,
            [
                "deployments",
                "configure-git-remote",
                "my-app",
                "--no-interactive",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_get_api_key.assert_not_called()
    mock_configure.assert_called_once_with(
        "http://test:8011/api/v1beta1/deployments/my-app/git",
        "profile-client-key",
        "proj_default",
        "my-app",
    )


def test_configure_git_remote_uses_env_project_client_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "env-api-key")
    monkeypatch.setenv("LLAMA_DEPLOY_PROJECT_ID", "env-project")
    _patch_no_profile_auth(monkeypatch)

    runner = CliRunner()
    client = _apply_client_mock()
    client.project_id = "env-project"
    client.base_url = DEFAULT_BASE_URL
    client.api_key = "env-api-key"

    with (
        patch_project_client(client),
        patch(f"{_DEPLOY_CMD}.is_git_repo", return_value=True),
        patch(
            f"{_DEPLOY_CMD}.configure_git_remote", return_value="llamaagents-my-app"
        ) as mock_configure,
        patch(
            f"{_DEPLOY_CMD}.get_api_key",
            return_value="profile-state-key",
            create=True,
        ) as mock_get_api_key,
    ):
        result = runner.invoke(
            app,
            [
                "deployments",
                "configure-git-remote",
                "my-app",
                "--no-interactive",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_get_api_key.assert_not_called()
    mock_configure.assert_called_once_with(
        f"{DEFAULT_BASE_URL}/api/v1beta1/deployments/my-app/git",
        "env-api-key",
        "env-project",
        "my-app",
    )


def test_apply_push_mode_uses_selected_project_client_api_key(
    patched_auth: Any, tmp_path: Any
) -> None:
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("generate_name: My App\nspec:\n  repo_url: ''\n")

    client = _apply_client_mock()
    client.api_key = "profile-client-key"
    with (
        patch_project_client(client),
        patch(f"{_DEPLOY_CMD}.is_git_repo", return_value=True),
        patch(
            f"{_DEPLOY_CMD}.configure_git_remote", return_value="llamaagents-new-app"
        ) as mock_configure,
        patch(
            f"{_DEPLOY_CMD}.push_to_remote",
            return_value=subprocess.CompletedProcess([], 0, stderr=b""),
        ),
        patch(
            f"{_DEPLOY_CMD}.get_api_key",
            return_value="profile-state-key",
            create=True,
        ) as mock_get_api_key,
    ):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    mock_get_api_key.assert_not_called()
    mock_configure.assert_called_once_with(
        "http://test:8011/api/v1beta1/deployments/new-app/git",
        "profile-client-key",
        "proj_default",
        "new-app",
    )


def test_apply_push_mode_uses_env_project_client_api_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "env-api-key")
    monkeypatch.setenv("LLAMA_DEPLOY_PROJECT_ID", "env-project")
    _patch_no_profile_auth(monkeypatch)

    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("generate_name: My App\nspec:\n  repo_url: ''\n")

    client = _apply_client_mock()
    client.project_id = "env-project"
    client.base_url = DEFAULT_BASE_URL
    client.api_key = "env-api-key"
    with (
        patch_project_client(client),
        patch(f"{_DEPLOY_CMD}.is_git_repo", return_value=True),
        patch(
            f"{_DEPLOY_CMD}.configure_git_remote", return_value="llamaagents-new-app"
        ) as mock_configure,
        patch(
            f"{_DEPLOY_CMD}.push_to_remote",
            return_value=subprocess.CompletedProcess([], 0, stderr=b""),
        ),
        patch(
            f"{_DEPLOY_CMD}.get_api_key",
            return_value="profile-state-key",
            create=True,
        ) as mock_get_api_key,
    ):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    mock_get_api_key.assert_not_called()
    mock_configure.assert_called_once_with(
        f"{DEFAULT_BASE_URL}/api/v1beta1/deployments/new-app/git",
        "env-api-key",
        "env-project",
        "new-app",
    )


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


def test_apply_internal_scheme_roundtrip_pushes(
    patched_auth: Any, tmp_path: Any
) -> None:
    """``get -o template`` emits ``repo_url: internal://``; re-applying that
    YAML should treat it as push-mode and push-then-save."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  repo_url: 'internal://'\n  git_ref: main\n")

    client = _push_mode_client()
    with patch_project_client(client), _patched_git_push() as mock_push:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    assert "updated my-app" in result.output
    mock_push.assert_called_once()
    client.validate_repository.assert_not_called()


def test_apply_internal_scheme_skips_push_when_not_in_git_repo(
    patched_auth: Any, tmp_path: Any
) -> None:
    """When not in a git repo, push-mode apply should skip the push
    and still succeed (spec-only update)."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  suspended: true\n")

    client = _push_mode_client()
    with (
        patch_project_client(client),
        _patched_git_push() as mock_push,
        patch(f"{_DEPLOY_CMD}.is_git_repo", return_value=False),
    ):
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f)])

    assert result.exit_code == 0, result.output
    assert "updated my-app" in result.output
    # Push should be skipped entirely.
    mock_push.assert_not_called()
    # The update should still go through.
    client.update_deployment.assert_called_once()


def test_apply_no_push_skips_push(patched_auth: Any, tmp_path: Any) -> None:
    """``--no-push`` suppresses the git push even when the deployment is
    push-mode and cwd is a valid git repo."""
    runner = CliRunner()
    f = tmp_path / "deploy.yaml"
    f.write_text("name: my-app\nspec:\n  git_ref: feature-branch\n")

    client = _push_mode_client()
    with patch_project_client(client), _patched_git_push() as mock_push:
        result = runner.invoke(app, ["deployments", "apply", "-f", str(f), "--no-push"])

    assert result.exit_code == 0, result.output
    assert "updated my-app" in result.output
    mock_push.assert_not_called()
    client.update_deployment.assert_called_once()
