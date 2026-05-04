# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Test session configuration for isolating per-worker state.

Each xdist worker gets a unique HOME and LLAMACTL_CONFIG_DIR so migrations and
SQLite DBs do not clash across processes.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from llama_agents.core.schema.deployments import DeploymentResponse
from llama_agents.core.schema.git_validation import RepositoryValidationResponse

# Base temp dir for the whole session
_TEST_HOME = tempfile.mkdtemp(prefix="llamactl_test_home_")

# Worker-specific subdir (e.g. gw0, gw1). Falls back to 'main' without xdist.
_WORKER_ID = os.environ.get("PYTEST_XDIST_WORKER", "main")
_WORKER_HOME = os.path.join(_TEST_HOME, _WORKER_ID)
_WORKER_CONFIG = os.path.join(_WORKER_HOME, ".config", "llamactl")

# Ensure directories exist
os.makedirs(_WORKER_CONFIG, exist_ok=True)

# Isolate HOME and config per worker
os.environ["HOME"] = _WORKER_HOME
os.environ.setdefault("TERM", "xterm")
os.environ["LLAMACTL_CONFIG_DIR"] = _WORKER_CONFIG

LLAMA_CLOUD_ENV_VARS = (
    "LLAMA_CLOUD_API_KEY",
    "LLAMA_CLOUD_BASE_URL",
    "LLAMA_CLOUD_USE_PROFILE",
    "LLAMA_AGENTS_PROJECT_ID",
    "LLAMA_DEPLOY_PROJECT_ID",
    "_LLAMACTL_COMPLETE",
    "llama_cloud_api_key",
    "llama_cloud_base_url",
    "llama_cloud_use_profile",
    "llama_agents_project_id",
    "llama_deploy_project_id",
)


def pytest_sessionfinish(session: pytest.Session, exitstatus: pytest.ExitCode) -> None:
    try:
        shutil.rmtree(_TEST_HOME, ignore_errors=True)
    except Exception:
        pass


def make_deployment(
    deployment_id: str = "my-app", **overrides: Any
) -> DeploymentResponse:
    """Build a DeploymentResponse with sensible defaults for command tests."""
    base: dict[str, Any] = {
        "id": deployment_id,
        "display_name": deployment_id,
        "repo_url": "https://github.com/example/repo",
        "deployment_file_path": "llama_deploy.yaml",
        "git_ref": "main",
        "git_sha": "abc1234567890",
        "project_id": "proj_default",
        "secret_names": [],
        "apiserver_url": None,
        "status": "Running",
    }
    base.update(overrides)
    return DeploymentResponse.model_validate(base)


def make_loop_bound_project_client(
    *,
    existing: DeploymentResponse | None = None,
    created: DeploymentResponse | None = None,
    updated: DeploymentResponse | None = None,
    validate_accessible: bool = True,
) -> MagicMock:
    loop: asyncio.AbstractEventLoop | None = None

    def check_loop() -> None:
        nonlocal loop
        running = asyncio.get_running_loop()
        if loop is None:
            loop = running
        elif running is not loop:
            raise RuntimeError("Event loop is closed")

    async def get_deployment(
        deployment_id: str, include_events: bool = False
    ) -> DeploymentResponse:
        check_loop()
        if existing is not None and deployment_id == existing.id:
            return existing
        request = httpx.Request(
            "GET", f"http://test/api/v1beta1/deployments/{deployment_id}"
        )
        response = httpx.Response(404, request=request, text='{"detail":"not found"}')
        raise httpx.HTTPStatusError("HTTP 404", request=request, response=response)

    async def validate_repository(
        repo_url: str,
        deployment_id: str | None = None,
        pat: str | None = None,
    ) -> RepositoryValidationResponse:
        check_loop()
        return RepositoryValidationResponse(
            accessible=validate_accessible,
            message="ok" if validate_accessible else "repo not found",
        )

    async def create_deployment(payload: Any) -> DeploymentResponse:
        check_loop()
        return created or make_deployment("new-app")

    async def update_deployment(deployment_id: str, payload: Any) -> DeploymentResponse:
        check_loop()
        return updated or existing or make_deployment(deployment_id)

    async def aclose() -> None:
        check_loop()

    client = MagicMock()
    client.project_id = "proj_default"
    client.base_url = "http://test:8011"
    client.api_key = "profile-client-key"
    client.get_deployment = AsyncMock(side_effect=get_deployment)
    client.validate_repository = AsyncMock(side_effect=validate_repository)
    client.create_deployment = AsyncMock(side_effect=create_deployment)
    client.update_deployment = AsyncMock(side_effect=update_deployment)
    client.aclose = AsyncMock(side_effect=aclose)
    return client


def clear_llama_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in LLAMA_CLOUD_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def set_llama_cloud_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    api_key: str | None = None,
    project_id: str | None = None,
    base_url: str | None = None,
    use_profile: bool | None = None,
    completion: str | None = None,
) -> None:
    clear_llama_cloud_env(monkeypatch)
    if api_key is not None:
        monkeypatch.setenv("LLAMA_CLOUD_API_KEY", api_key)
    if project_id is not None:
        monkeypatch.setenv("LLAMA_AGENTS_PROJECT_ID", project_id)
    if base_url is not None:
        monkeypatch.setenv("LLAMA_CLOUD_BASE_URL", base_url)
    if use_profile is not None:
        monkeypatch.setenv("LLAMA_CLOUD_USE_PROFILE", "1" if use_profile else "0")
    if completion is not None:
        monkeypatch.setenv("_LLAMACTL_COMPLETE", completion)


def patch_project_client(client_mock: MagicMock) -> Any:
    """Patch ProjectClient construction inside ``cli.client.get_project_client``."""
    return patch(
        "llama_agents.core.client.manage_client.ProjectClient",
        return_value=client_mock,
    )


@pytest.fixture
def fake_profile() -> SimpleNamespace:
    return SimpleNamespace(
        api_url="http://test:8011",
        project_id="proj_default",
        api_key="key",
        device_oidc=None,
        name="prof",
    )


@pytest.fixture
def patched_auth(fake_profile: SimpleNamespace) -> Any:
    """Patch the env service so commands use a fake authenticated profile."""
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = fake_profile
        mock_auth_svc.list_profiles.return_value = [fake_profile]
        mock_auth_svc.env = SimpleNamespace(requires_auth=True)
        mock_auth_svc.auth_middleware.return_value = None
        mock_service.current_auth_service.return_value = mock_auth_svc
        yield mock_service
