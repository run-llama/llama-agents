# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import llama_agents.cli.client as client_module
import llama_agents.cli.config.env_service as env_service
import pytest
from conftest import clear_llama_cloud_env, set_llama_cloud_env
from llama_agents.cli.client import get_control_plane_client, get_project_client

DEFAULT_BASE_URL = "https://api.cloud.llamaindex.ai"
OLD_MISSING_PROJECT_MESSAGE = (
    "LLAMA_CLOUD_API_KEY is set but LLAMA_DEPLOY_PROJECT_ID is missing. "
    "Set it or pass --project."
)
OVERRIDE_WARNING = (
    "Using LLAMA_CLOUD_API_KEY from environment (overriding profile 'prof'). "
    "Set LLAMA_CLOUD_USE_PROFILE=1 to use the profile instead."
)


@pytest.fixture(autouse=True)
def clean_env_var_auth_state(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_llama_cloud_env(monkeypatch)

    for name in dir(client_module):
        value = getattr(client_module, name)
        lowered = name.lower()
        if isinstance(value, bool) and "warn" in lowered and "env" in lowered:
            monkeypatch.setattr(client_module, name, False)


def _profile(
    *,
    api_url: str = "http://test:8011",
    project_id: str = "default-project",
    api_key: str | None = None,
    name: str = "prof",
) -> SimpleNamespace:
    return SimpleNamespace(
        api_url=api_url,
        project_id=project_id,
        api_key=api_key,
        device_oidc=None,
        name=name,
    )


def _set_current_profile(
    monkeypatch: pytest.MonkeyPatch, profile: SimpleNamespace | None
) -> MagicMock:
    mock_auth_svc = MagicMock()
    mock_auth_svc.get_current_profile.return_value = profile
    mock_auth_svc.list_profiles.return_value = [] if profile is None else [profile]
    mock_auth_svc.env = SimpleNamespace(requires_auth=True)
    mock_auth_svc.auth_middleware.return_value = None

    mock_service = MagicMock()
    mock_service.current_auth_service.return_value = mock_auth_svc
    mock_service.get_current_environment.return_value = SimpleNamespace(
        api_url=DEFAULT_BASE_URL,
        requires_auth=True,
    )
    monkeypatch.setattr(env_service, "service", mock_service)
    return mock_auth_svc


def _close_client(client: Any) -> None:
    asyncio.run(client.aclose())


def test_deployment_project_resolution() -> None:
    """Test that get_project_client uses profile's project by default"""
    profile = _profile()
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = profile
        mock_service.current_auth_service.return_value = mock_auth_svc
        client = get_project_client()
        try:
            assert client.base_url == "http://test:8011"
            assert client.project_id == "default-project"
        finally:
            _close_client(client)


def test_client_requires_profile_with_project() -> None:
    """Test that client works when profile has a project (project_id is required)"""
    profile = _profile(project_id="test-project")
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = profile
        mock_service.current_auth_service.return_value = mock_auth_svc
        client = get_project_client()
        try:
            assert client.project_id == "test-project"
        finally:
            _close_client(client)


def test_client_requires_valid_profile() -> None:
    """Test that client fails when no profile is configured"""
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = None
        mock_service.current_auth_service.return_value = mock_auth_svc
        with pytest.raises(SystemExit):
            get_project_client()


def test_env_var_project_client_uses_default_base_url_and_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key", project_id="env-project")
    _set_current_profile(monkeypatch, None)

    client = get_project_client()
    try:
        assert client.base_url == DEFAULT_BASE_URL
        assert client.api_key == "env-api-key"
        assert client.project_id == "env-project"
    finally:
        _close_client(client)


def test_env_var_control_plane_client_strips_base_url_and_uses_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(
        monkeypatch,
        api_key="env-api-key",
        project_id="env-project",
        base_url="https://api.example.test/",
    )
    _set_current_profile(monkeypatch, None)

    client = get_control_plane_client()
    try:
        assert client.base_url == "https://api.example.test"
        assert client.api_key == "env-api-key"
    finally:
        _close_client(client)


def test_incomplete_env_var_project_client_uses_active_profile_without_warning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key")
    auth_svc = _set_current_profile(
        monkeypatch,
        _profile(
            api_url="https://profile.example.test",
            project_id="profile-project",
            api_key="profile-api-key",
        ),
    )

    client = get_project_client()
    try:
        captured = capsys.readouterr()
        assert client.base_url == "https://profile.example.test"
        assert client.api_key == "profile-api-key"
        assert client.project_id == "profile-project"
        assert "Using LLAMA_CLOUD_API_KEY from environment" not in captured.err
        auth_svc.auth_middleware.assert_called_once_with()
    finally:
        _close_client(client)


def test_incomplete_env_var_control_plane_client_uses_active_profile_without_warning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key")
    auth_svc = _set_current_profile(
        monkeypatch,
        _profile(
            api_url="https://profile.example.test/",
            project_id="profile-project",
            api_key="profile-api-key",
        ),
    )

    client = get_control_plane_client()
    try:
        captured = capsys.readouterr()
        assert client.base_url == "https://profile.example.test"
        assert client.api_key == "profile-api-key"
        assert "Using LLAMA_CLOUD_API_KEY from environment" not in captured.err
        auth_svc.auth_middleware.assert_called_once_with()
    finally:
        _close_client(client)


def test_incomplete_env_var_project_client_without_profile_uses_generic_no_profile_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key")
    _set_current_profile(monkeypatch, None)

    with pytest.raises(SystemExit) as exc_info:
        get_project_client()

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "No profile configured" in captured.out
    assert OLD_MISSING_PROJECT_MESSAGE not in captured.out
    assert OLD_MISSING_PROJECT_MESSAGE not in str(exc_info.value)


def test_env_var_project_override_wins_over_env_project_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key", project_id="env-project")
    _set_current_profile(monkeypatch, None)

    client = get_project_client(project_id_override="flag-project")
    try:
        assert client.project_id == "flag-project"
        assert client.api_key == "env-api-key"
    finally:
        _close_client(client)


def test_env_var_use_profile_falls_through_to_profile_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(
        monkeypatch,
        api_key="env-api-key",
        project_id="env-project",
        base_url="https://env.example.test",
        use_profile=True,
    )
    auth_svc = _set_current_profile(
        monkeypatch,
        _profile(
            api_url="https://profile.example.test",
            project_id="profile-project",
            api_key="profile-api-key",
        ),
    )

    client = get_project_client()
    try:
        assert client.base_url == "https://profile.example.test"
        assert client.api_key == "profile-api-key"
        assert client.project_id == "profile-project"
        auth_svc.auth_middleware.assert_called_once_with()
    finally:
        _close_client(client)


def test_env_var_override_warning_fires_once_with_active_profile(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key", project_id="env-project")
    _set_current_profile(monkeypatch, _profile(api_key="profile-api-key"))

    project_client = get_project_client()
    control_plane_client = get_control_plane_client()
    try:
        captured = capsys.readouterr()
        assert captured.err.count(OVERRIDE_WARNING) == 1
    finally:
        _close_client(project_client)
        _close_client(control_plane_client)


def test_env_var_override_warning_does_not_fire_without_profile(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key", project_id="env-project")
    _set_current_profile(monkeypatch, None)

    client = get_project_client()
    try:
        captured = capsys.readouterr()
        assert "Using LLAMA_CLOUD_API_KEY from environment" not in captured.err
    finally:
        _close_client(client)


def test_env_var_override_warning_does_not_fire_under_completion(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    set_llama_cloud_env(
        monkeypatch,
        api_key="env-api-key",
        project_id="env-project",
        completion="zsh_source",
    )
    _set_current_profile(monkeypatch, _profile(api_key="profile-api-key"))

    client = get_project_client()
    try:
        captured = capsys.readouterr()
        assert client.api_key == "env-api-key"
        assert client.project_id == "env-project"
        assert "Using LLAMA_CLOUD_API_KEY from environment" not in captured.err
    finally:
        _close_client(client)
