# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from click.testing import CliRunner
from llama_agents.cli.app import app
from llama_agents.cli.config._config import Environment

_INTERACTIVE_PATCH = "llama_agents.cli.commands.environments.is_interactive_session"


def test_environments_get_prints_table() -> None:
    runner = CliRunner()
    env1 = Environment(api_url="https://api1", requires_auth=False)
    env2 = Environment(api_url="https://api2", requires_auth=True)
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.list_environments.return_value = [env1, env2]
        mock_service.get_current_environment.return_value = env2
        result = runner.invoke(app, ["environments", "get"])
        assert result.exit_code == 0
        assert "REQUIRES_AUTH  ACTIVE" in result.output
        assert "https://api1  no" in result.output
        assert "https://api2  yes" in result.output
        assert "https://api1" in result.output
        assert "https://api2" in result.output


def test_environments_get_single_environment_json() -> None:
    runner = CliRunner()
    env1 = Environment(api_url="https://api1", requires_auth=False)
    env2 = Environment(api_url="https://api2", requires_auth=True)
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.list_environments.return_value = [env1, env2]
        mock_service.get_current_environment.return_value = None
        result = runner.invoke(
            app, ["environments", "get", "https://api2", "-o", "json"]
        )
        assert result.exit_code == 0, result.output
        assert '"api_url": "https://api2"' in result.output
        assert result.output.strip().startswith("{")


def test_environments_help_describes_commands() -> None:
    result = CliRunner().invoke(app, ["environments", "--help"])
    assert result.exit_code == 0
    assert "get     List environments or show one environment." in result.output
    assert "add     Probe and store an environment." in result.output
    assert "delete  Delete an environment and its profiles." in result.output
    assert "use     Set the active environment." in result.output


def test_environments_get_does_not_offer_wide_output() -> None:
    result = CliRunner().invoke(app, ["environments", "get", "-o", "wide"])
    assert result.exit_code != 0
    assert "'wide' is not one of 'text', 'json', 'yaml'" in result.output


def test_environments_add_probes_and_upserts() -> None:
    runner = CliRunner()
    env = Environment(
        api_url="https://api", requires_auth=True, min_llamactl_version=None
    )
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.probe_environment.return_value = env
        result = runner.invoke(app, ["environments", "add", "https://api/"])
        assert result.exit_code == 0
        mock_service.probe_environment.assert_called_once_with("https://api")
        mock_service.create_or_update_environment.assert_called_once_with(env)


def test_environments_use_argument_and_interactive() -> None:
    runner = CliRunner()
    # Argument path
    env = Environment(api_url="https://api", requires_auth=False)
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.switch_environment.return_value = env
        mock_service.auto_update_env.return_value = env
        result = runner.invoke(app, ["environments", "use", "https://api"])
        assert result.exit_code == 0
        mock_service.switch_environment.assert_called_once_with("https://api")

    # Interactive path (select existing)
    envs = [
        Environment(api_url="https://e1", requires_auth=False),
        Environment(api_url="https://e2", requires_auth=True),
    ]
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.environments.select_or_exit") as mock_select,
        patch(_INTERACTIVE_PATCH, return_value=True),
    ):
        mock_service.list_environments.return_value = envs
        mock_service.get_current_environment.return_value = envs[0]
        mock_service.switch_environment.return_value = envs[1]
        mock_service.auto_update_env.return_value = envs[1]
        mock_select.return_value = SimpleNamespace(api_url="https://e2")
        result = runner.invoke(app, ["environments", "use"])
        assert result.exit_code == 0
        mock_service.switch_environment.assert_called_once_with("https://e2")

    # Missing environment should error
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.switch_environment.side_effect = ValueError(
            "Environment 'https://missing' not found. Add it with 'llamactl environments add <API_URL>'"
        )
        result = runner.invoke(app, ["environments", "use", "https://missing"])
        assert result.exit_code != 0
        assert "not found" in result.output


def test_environments_add_interactive_selection_for_url() -> None:
    runner = CliRunner()
    env = Environment(api_url="https://x", requires_auth=False)
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.environments.click.prompt") as mock_prompt,
        patch(_INTERACTIVE_PATCH, return_value=True),
    ):
        mock_service.get_current_environment.return_value = Environment(
            api_url="https://default", requires_auth=False
        )
        mock_service.probe_environment.return_value = env
        mock_prompt.return_value = "https://x"
        result = runner.invoke(app, ["environments", "add"])
        assert result.exit_code == 0
        mock_service.create_or_update_environment.assert_called_once_with(env)

    # Non-interactive missing URL should error with hint
    with patch(_INTERACTIVE_PATCH, return_value=False):
        result = runner.invoke(app, ["environments", "add"])
    assert result.exit_code != 0
    assert "Pass <api_url>" in result.output


def test_environments_delete_argument_and_prompt() -> None:
    runner = CliRunner()
    # Argument path
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.delete_environment.return_value = True
        result = runner.invoke(app, ["environments", "delete", "https://api"])
        assert result.exit_code == 0
        mock_service.delete_environment.assert_called_once_with("https://api")

    # Interactive prompt path
    envs = [
        Environment(api_url="https://e1", requires_auth=False),
        Environment(api_url="https://e2", requires_auth=True),
    ]
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.environments.select_or_exit") as mock_select,
        patch(_INTERACTIVE_PATCH, return_value=True),
    ):
        mock_service.list_environments.return_value = envs
        mock_service.get_current_environment.return_value = envs[0]
        mock_service.delete_environment.return_value = True
        mock_select.return_value = SimpleNamespace(api_url="https://e2")
        result = runner.invoke(app, ["environments", "delete"])
        assert result.exit_code == 0
        mock_service.delete_environment.assert_called_once_with("https://e2")

    # Non-interactive missing URL should list envs and hint
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch(_INTERACTIVE_PATCH, return_value=False),
    ):
        mock_service.list_environments.return_value = envs
        mock_service.get_current_environment.return_value = envs[0]
        result = runner.invoke(app, ["environments", "delete"])
    assert result.exit_code != 0
    assert "Pass <api_url>" in result.output
