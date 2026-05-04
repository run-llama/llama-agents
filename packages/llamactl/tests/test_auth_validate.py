from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest
from llama_agents.cli.commands.auth import validate_authenticated_profile


class DummyProfile:
    def __init__(self, name: str):
        self.name = name


def test_validate_authenticated_profile_returns_current_when_present() -> None:
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = DummyProfile("cur")
        mock_service.current_auth_service.return_value = mock_auth_svc
        prof = validate_authenticated_profile(interactive=False)
        assert isinstance(prof, DummyProfile)
        assert prof.name == "cur"


def test_validate_authenticated_profile_raises_when_non_interactive_and_missing() -> (
    None
):
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = None
        mock_service.current_auth_service.return_value = mock_auth_svc
        with pytest.raises(click.ClickException):
            validate_authenticated_profile(interactive=False)


def test_validate_authenticated_profile_interactive_multiple_profiles_selects() -> None:
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.auth.select_or_exit") as mock_select,
    ):
        mock_auth_svc = MagicMock()
        profiles = [DummyProfile("a"), DummyProfile("b")]
        mock_auth_svc.get_current_profile.return_value = None
        mock_auth_svc.list_profiles.return_value = profiles
        mock_service.current_auth_service.return_value = mock_auth_svc
        mock_select.return_value = profiles[1]
        prof = validate_authenticated_profile(interactive=True)
        mock_auth_svc.set_current_profile.assert_called_once_with("b")
        assert prof.name == "b"


def test_validate_authenticated_profile_interactive_multiple_profiles_none_selected() -> (
    None
):
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.auth.select_or_exit") as mock_select,
    ):
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = None
        mock_auth_svc.list_profiles.return_value = [
            DummyProfile("a"),
            DummyProfile("b"),
        ]
        mock_service.current_auth_service.return_value = mock_auth_svc
        mock_select.side_effect = click.ClickException("Cancelled")
        with pytest.raises(click.ClickException):
            validate_authenticated_profile(interactive=True)


def test_validate_authenticated_profile_interactive_single_profile_sets_current() -> (
    None
):
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_auth_svc = MagicMock()
        only = DummyProfile("only-one")
        mock_auth_svc.get_current_profile.return_value = None
        mock_auth_svc.list_profiles.return_value = [only]
        mock_service.current_auth_service.return_value = mock_auth_svc
        prof = validate_authenticated_profile(interactive=True)
        mock_auth_svc.set_current_profile.assert_called_once_with("only-one")
        assert prof.name == "only-one"


def test_validate_authenticated_profile_interactive_no_profiles_and_no_auth_cancel() -> (
    None
):
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.auth.click.prompt") as mock_prompt,
    ):
        mock_auth_svc = MagicMock()
        mock_auth_svc.get_current_profile.return_value = None
        mock_auth_svc.list_profiles.return_value = []
        mock_auth_svc.env = SimpleNamespace(requires_auth=False)
        mock_service.current_auth_service.return_value = mock_auth_svc
        mock_prompt.return_value = ""
        with pytest.raises(click.ClickException):
            validate_authenticated_profile(interactive=True)
