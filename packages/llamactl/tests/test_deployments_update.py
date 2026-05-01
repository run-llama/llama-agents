# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``deployments update``.

The headline regression here: ``deployments update <id>`` for an internal-code
deployment used to call ``asyncio.run`` twice with the same ``ProjectClient``.
The shared httpx pool was bound to the first (closed) event loop, and the
second run raised ``RuntimeError: Event loop is closed`` — surfacing in CI as
``Error: Event loop is closed`` after the "Continuing with update using last
pushed code" fallback when the internal git mirror returned a transient 500.
"""

from __future__ import annotations

import subprocess
from typing import Any
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from conftest import make_deployment, patch_project_client
from llama_agents.cli.app import app
from llama_agents.core.schema.deployments import (
    INTERNAL_CODE_REPO_SCHEME,
    DeploymentResponse,
)


def _client_mock(current: DeploymentResponse, updated: DeploymentResponse) -> MagicMock:
    """ProjectClient stand-in for the update flow."""

    async def _get(
        deployment_id: str, include_events: bool = False
    ) -> DeploymentResponse:
        return current

    async def _update(deployment_id: str, update: Any) -> DeploymentResponse:
        return updated

    client = MagicMock()
    client.get_deployment.side_effect = _get
    client.update_deployment.side_effect = _update
    client.project_id = "proj_default"
    client.base_url = "http://test:8011"
    return client


def _completed_process(
    returncode: int = 0, stdout: bytes = b"", stderr: bytes = b""
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr
    )


def test_deployments_update_external_repo(patched_auth: Any) -> None:
    """External-repo update: no push step, single asyncio.run, exit 0."""
    runner = CliRunner()
    current = make_deployment("my-app", git_sha="a" * 40)
    updated = make_deployment("my-app", git_sha="b" * 40)
    client = _client_mock(current, updated)
    with patch_project_client(client):
        result = runner.invoke(
            app, ["deployments", "update", "my-app", "--no-interactive"]
        )
    assert result.exit_code == 0, result.output
    client.get_deployment.assert_called_once()
    client.update_deployment.assert_called_once()


def test_deployments_update_internal_repo_push_failure_does_not_abort(
    patched_auth: Any,
) -> None:
    """Push 500 → fallback path runs, deployment still updates, exit 0.

    Reproduces the GitHub Actions failure mode where a transient 500 from the
    internal git mirror caused ``Event loop is closed`` and a non-zero exit.
    The fix routes both ``get_deployment`` and ``update_deployment`` through a
    single ``asyncio.run`` so the httpx pool isn't reused across loops.

    Patches are at the helper boundary (``configure_git_remote`` /
    ``push_to_remote``) so the test doesn't depend on the exact shape of the
    underlying git plumbing.
    """
    runner = CliRunner()
    current = make_deployment(
        "my-app", repo_url=INTERNAL_CODE_REPO_SCHEME, git_sha="a" * 40
    )
    updated = make_deployment(
        "my-app", repo_url=INTERNAL_CODE_REPO_SCHEME, git_sha="b" * 40
    )
    client = _client_mock(current, updated)

    with (
        patch_project_client(client),
        patch("llama_agents.cli.commands.deployment.is_git_repo", return_value=True),
        patch(
            "llama_agents.cli.commands.deployment.configure_git_remote",
            return_value="llamaagents-my-app",
        ),
        # The push itself fails with a 500 (the originally reported flake).
        patch(
            "llama_agents.cli.commands.deployment.push_to_remote",
            return_value=_completed_process(
                returncode=1,
                stderr=(
                    b"error: RPC failed; HTTP 500\n"
                    b"fatal: the remote end hung up unexpectedly\n"
                ),
            ),
        ),
    ):
        result = runner.invoke(
            app, ["deployments", "update", "my-app", "--no-interactive"]
        )

    assert result.exit_code == 0, result.output
    assert "Event loop is closed" not in result.output
    client.get_deployment.assert_called_once()
    client.update_deployment.assert_called_once()
