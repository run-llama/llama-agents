from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from click.testing import CliRunner
from llama_agents.cli.app import app
from llama_agents.cli.commands.auth import _create_or_update_agent_api_key
from llama_agents.cli.config.schema import Auth


def test_auth_create_api_key_profile_non_interactive_validation() -> None:
    runner = CliRunner()
    with patch(
        "llama_agents.cli.commands.auth.is_interactive_session", return_value=False
    ):
        result = runner.invoke(app, ["auth", "token"])
    assert result.exit_code != 0
    assert (
        "--api-key and --project-id are required in non-interactive mode"
        in result.output
    )


def test_auth_create_api_key_profile_non_interactive_success() -> None:
    runner = CliRunner()
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch(
            "llama_agents.cli.commands.auth.is_interactive_session", return_value=False
        ),
    ):
        mock_auth_svc = MagicMock()
        mock_auth_svc.create_profile_from_token.return_value = SimpleNamespace(
            name="prof"
        )
        mock_service.current_auth_service.return_value = mock_auth_svc

        result = runner.invoke(
            app,
            [
                "auth",
                "token",
                "--project-id",
                "p",
                "--api-key",
                "key",
            ],
        )
        assert result.exit_code == 0
        mock_auth_svc.create_profile_from_token.assert_called_once_with("p", "key")


def test_auth_list_profiles_no_profiles() -> None:
    runner = CliRunner()
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        mock_auth_svc.list_profiles.return_value = []
        mock_auth_svc.get_current_profile.return_value = None
        mock_service.current_auth_service.return_value = mock_auth_svc
        result = runner.invoke(app, ["auth", "list"])
        assert result.exit_code == 0
        assert "No profiles found" in result.output


def test_auth_switch_profile_success_and_missing() -> None:
    runner = CliRunner()
    with (
        patch("llama_agents.cli.commands.auth._select_profile") as mock_select,
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_auth_svc = MagicMock()
        mock_service.current_auth_service.return_value = mock_auth_svc
        mock_select.return_value = SimpleNamespace(name="p1")
        result = runner.invoke(app, ["auth", "switch", "p1"])
        assert result.exit_code == 0
        mock_auth_svc.set_current_profile.assert_called_once_with("p1")

    with (
        patch("llama_agents.cli.commands.auth._select_profile", return_value=None),
        patch("llama_agents.cli.config.env_service.service") as mock_service2,
    ):
        mock_service2.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["auth", "switch", "doesnt-exist"])
        assert result.exit_code == 0
        assert "No profile selected" in result.output


def test_auth_logout_existing() -> None:
    runner = CliRunner()
    with (
        patch("llama_agents.cli.commands.auth._select_profile") as mock_select,
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_auth_svc = MagicMock()
        mock_service.current_auth_service.return_value = mock_auth_svc
        mock_select.return_value = SimpleNamespace(name="p1")
        mock_auth_svc.delete_profile = AsyncMock(return_value=True)
        result = runner.invoke(app, ["auth", "logout", "p1"])
        assert result.exit_code == 0


def test_auth_logout_missing() -> None:
    runner = CliRunner()
    with (
        patch("llama_agents.cli.commands.auth._select_profile", return_value=None),
        patch("llama_agents.cli.config.env_service.service") as mock_service2,
    ):
        mock_service2.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["auth", "logout", "missing"])
        assert result.exit_code == 0
        assert "No profile selected" in result.output


def test_auth_project_non_interactive_lists_options_and_hints() -> None:
    runner = CliRunner()
    with (
        patch(
            "llama_agents.cli.commands.auth.validate_authenticated_profile",
            return_value=MagicMock(name="p", project_id="x"),
        ),
        patch(
            "llama_agents.cli.commands.auth._discover_organization",
            return_value=None,
        ),
        patch(
            "llama_agents.cli.commands.auth._list_projects",
            return_value=[
                MagicMock(
                    project_id="abc-123", project_name="My Project", deployment_count=2
                ),
            ],
        ),
        patch(
            "llama_agents.cli.commands.auth.is_interactive_session", return_value=False
        ),
    ):
        result = runner.invoke(app, ["auth", "project"])
        assert result.exit_code != 0
        assert "abc-123" in result.output
        assert "Pass <project_id> to choose one" in result.output


def test_auth_project_interactive_sets_selected() -> None:
    runner = CliRunner()
    with (
        patch(
            "llama_agents.cli.commands.auth.validate_authenticated_profile",
            return_value=MagicMock(name="p"),
        ),
        patch(
            "llama_agents.cli.commands.auth._list_projects",
            return_value=[
                MagicMock(project_id="proj", project_name="Proj", deployment_count=1)
            ],
        ),
        patch("llama_agents.cli.commands.auth.select_or_exit") as mock_select,
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch(
            "llama_agents.cli.commands.auth.is_interactive_session", return_value=True
        ),
    ):
        mock_auth_svc = MagicMock()
        mock_service.current_auth_service.return_value = mock_auth_svc
        mock_select.return_value = "proj"
        result = runner.invoke(app, ["auth", "project"])
        assert result.exit_code == 0
        mock_auth_svc.set_project.assert_called_once()


@pytest.mark.asyncio
async def test_create_or_update_agent_api_key_does_not_retry_read_timeout() -> None:
    """Read-phase errors must not trigger retries.

    create_agent_api_key is a non-idempotent POST: a ReadTimeout after the
    request left the client could mean the server already created a key, so
    retrying would duplicate it.
    """
    profile = Auth(
        id="id-1",
        name="test",
        api_url="https://example.com",
        project_id="proj",
        api_key=None,
        api_key_id=None,
        device_oidc=None,
    )

    mock_auth_svc = MagicMock()
    mock_client_cm = AsyncMock()
    mock_client = MagicMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_auth_svc.profile_client.return_value = mock_client_cm

    mock_client.create_agent_api_key = AsyncMock(
        side_effect=httpx.ReadTimeout("server took too long")
    )

    with pytest.raises(Exception) as exc_info:
        await _create_or_update_agent_api_key(mock_auth_svc, profile)

    assert "Network error while provisioning an API token" in str(exc_info.value)
    assert mock_client.create_agent_api_key.await_count == 1


@pytest.mark.asyncio
async def test_create_or_update_agent_api_key_retries_connect_error() -> None:
    """Connect-phase errors (DNS, connection refused, connect timeout) happen
    before the request is sent, so retrying is safe even for a non-idempotent
    POST. The CLI should absorb a brief connectivity blip.
    """
    profile = Auth(
        id="id-1",
        name="test",
        api_url="https://example.com",
        project_id="proj",
        api_key=None,
        api_key_id=None,
        device_oidc=None,
    )

    mock_auth_svc = MagicMock()
    mock_client_cm = AsyncMock()
    mock_client = MagicMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_auth_svc.profile_client.return_value = mock_client_cm

    mock_client.create_agent_api_key = AsyncMock(
        side_effect=httpx.ConnectError("connection refused")
    )

    # Patch tenacity's sleep so the test doesn't pay real back-off.
    with patch("tenacity.nap.time.sleep"), patch("asyncio.sleep", AsyncMock()):
        with pytest.raises(Exception) as exc_info:
            await _create_or_update_agent_api_key(mock_auth_svc, profile)

    assert "Network error while provisioning an API token" in str(exc_info.value)
    # Default max_attempts=3 — every attempt should have run the operation.
    assert mock_client.create_agent_api_key.await_count == 3
