"""Tests for llama_agents.cli.utils.git_push utilities."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
from llama_agents.cli.utils.git_push import (
    _git_remote_name,
    _set_extra_headers,
    configure_git_remote,
    get_api_key,
    get_deployment_git_url,
    has_deployment_git_remote,
    push_to_remote,
)

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_git_remote_name() -> None:
    assert _git_remote_name("dep-123") == "llamaagents-dep-123"


def test_get_deployment_git_url() -> None:
    url = get_deployment_git_url("http://localhost:8000", "dep-1")
    assert url == "http://localhost:8000/api/v1beta1/deployments/dep-1/git"


def test_get_deployment_git_url_strips_trailing_slash() -> None:
    url = get_deployment_git_url("http://localhost:8000/", "dep-1")
    assert url == "http://localhost:8000/api/v1beta1/deployments/dep-1/git"


@patch("llama_agents.cli.utils.git_push.subprocess")
def test_has_deployment_git_remote_returns_true_when_remote_exists(
    mock_subprocess: MagicMock,
) -> None:
    mock_subprocess.run.return_value = MagicMock(returncode=0)

    assert has_deployment_git_remote("dep-1") is True
    mock_subprocess.run.assert_called_once_with(
        ["git", "remote", "get-url", "llamaagents-dep-1"],
        capture_output=True,
    )


@patch("llama_agents.cli.utils.git_push.subprocess")
def test_has_deployment_git_remote_returns_false_when_remote_missing(
    mock_subprocess: MagicMock,
) -> None:
    mock_subprocess.run.return_value = MagicMock(returncode=2)

    assert has_deployment_git_remote("dep-1") is False


# ---------------------------------------------------------------------------
# get_api_key
# ---------------------------------------------------------------------------


@patch("llama_agents.cli.config.env_service.service")
def test_get_api_key_returns_key(mock_service: MagicMock) -> None:
    profile = SimpleNamespace(api_key="sk-test-abc")
    auth_svc = MagicMock()
    auth_svc.get_current_profile.return_value = profile
    mock_service.current_auth_service.return_value = auth_svc

    assert get_api_key() == "sk-test-abc"


@patch("llama_agents.cli.config.env_service.service")
def test_get_api_key_returns_none_when_no_auth_required(
    mock_service: MagicMock,
) -> None:
    auth_svc = MagicMock()
    auth_svc.get_current_profile.return_value = None
    auth_svc.env.requires_auth = False
    mock_service.current_auth_service.return_value = auth_svc

    assert get_api_key() is None


@patch("llama_agents.cli.config.env_service.service")
def test_get_api_key_raises_when_auth_required_but_no_profile(
    mock_service: MagicMock,
) -> None:
    auth_svc = MagicMock()
    auth_svc.get_current_profile.return_value = None
    auth_svc.env.requires_auth = True
    mock_service.current_auth_service.return_value = auth_svc

    with pytest.raises(RuntimeError, match="Not authenticated"):
        get_api_key()


# ---------------------------------------------------------------------------
# _set_extra_headers
# ---------------------------------------------------------------------------


@patch("llama_agents.cli.utils.git_push.subprocess")
def test_set_extra_headers_with_api_key(mock_subprocess: MagicMock) -> None:
    _set_extra_headers("http://git-url", "sk-key", "proj-1")

    calls = mock_subprocess.run.call_args_list
    # First call: unset-all existing headers
    assert calls[0] == call(
        ["git", "config", "--local", "--unset-all", "http.http://git-url.extraHeader"],
        capture_output=True,
    )
    # Second call: project-id header
    assert calls[1] == call(
        [
            "git",
            "config",
            "--local",
            "--add",
            "http.http://git-url.extraHeader",
            "project-id: proj-1",
        ],
        check=True,
        capture_output=True,
    )
    # Third call: Authorization header
    assert calls[2] == call(
        [
            "git",
            "config",
            "--local",
            "--add",
            "http.http://git-url.extraHeader",
            "Authorization: Bearer sk-key",
        ],
        check=True,
        capture_output=True,
    )
    assert len(calls) == 3


@patch("llama_agents.cli.utils.git_push.subprocess")
def test_set_extra_headers_without_api_key(mock_subprocess: MagicMock) -> None:
    _set_extra_headers("http://git-url", None, "proj-1")

    calls = mock_subprocess.run.call_args_list
    # unset-all + project-id only, no Authorization header
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# configure_git_remote
# ---------------------------------------------------------------------------


@patch("llama_agents.cli.utils.git_push.subprocess")
@patch("llama_agents.cli.utils.git_push._set_extra_headers")
def test_configure_git_remote_adds_new_remote(
    mock_headers: MagicMock, mock_subprocess: MagicMock
) -> None:
    # Simulate remote not existing (get-url fails)
    mock_subprocess.run.side_effect = [
        MagicMock(returncode=2),  # git remote get-url -> not found
        MagicMock(returncode=0),  # git remote add -> success
    ]

    name = configure_git_remote("http://git-url", "sk-key", "proj-1", "dep-1")
    assert name == "llamaagents-dep-1"

    # Should call remote add (not set-url)
    add_call = mock_subprocess.run.call_args_list[1]
    assert add_call[0][0] == [
        "git",
        "remote",
        "add",
        "llamaagents-dep-1",
        "http://git-url",
    ]


@patch("llama_agents.cli.utils.git_push.subprocess")
@patch("llama_agents.cli.utils.git_push._set_extra_headers")
def test_configure_git_remote_updates_existing_remote(
    mock_headers: MagicMock, mock_subprocess: MagicMock
) -> None:
    # Simulate remote already existing
    mock_subprocess.run.side_effect = [
        MagicMock(returncode=0),  # git remote get-url -> found
        MagicMock(returncode=0),  # git remote set-url -> success
    ]

    name = configure_git_remote("http://git-url", "sk-key", "proj-1", "dep-1")
    assert name == "llamaagents-dep-1"

    # Should call remote set-url (not add)
    set_call = mock_subprocess.run.call_args_list[1]
    assert set_call[0][0] == [
        "git",
        "remote",
        "set-url",
        "llamaagents-dep-1",
        "http://git-url",
    ]


# ---------------------------------------------------------------------------
# push_to_remote
# ---------------------------------------------------------------------------


@patch("llama_agents.cli.utils.git_push.subprocess")
def test_push_to_remote(mock_subprocess: MagicMock) -> None:
    mock_subprocess.run.return_value = MagicMock(returncode=0, stderr=b"")

    result = push_to_remote(
        "my-remote", local_ref="feature", target_ref="refs/heads/main"
    )
    assert result.returncode == 0

    mock_subprocess.run.assert_called_once_with(
        ["git", "push", "my-remote", "feature:refs/heads/main"],
        capture_output=True,
    )


@patch("llama_agents.cli.utils.git_push.subprocess")
def test_push_to_remote_uses_defaults(mock_subprocess: MagicMock) -> None:
    mock_subprocess.run.return_value = MagicMock(returncode=0)

    push_to_remote("my-remote")

    mock_subprocess.run.assert_called_once_with(
        ["git", "push", "my-remote", "HEAD:refs/heads/main"],
        capture_output=True,
    )
