# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from llama_agents.cli.app import app


def test_config_text_shows_current_context() -> None:
    runner = CliRunner()
    profile = SimpleNamespace(name="prof", project_id="project-1")
    project = SimpleNamespace(project_id="project-1", project_name="Production")
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.config._list_projects", return_value=[project]),
    ):
        mock_service.get_current_environment.return_value = SimpleNamespace(
            api_url="https://api.example"
        )
        auth_svc = MagicMock()
        auth_svc.get_current_profile.return_value = profile
        mock_service.current_auth_service.return_value = auth_svc

        result = runner.invoke(app, ["config"])

    assert result.exit_code == 0, result.output
    assert "environment:  https://api.example" in result.output
    assert "profile:      prof" in result.output
    assert "project:      project-1 (Production)" in result.output


def test_config_json_shows_none_values_without_error() -> None:
    runner = CliRunner()
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.get_current_environment.return_value = SimpleNamespace(
            api_url="https://api.example"
        )
        auth_svc = MagicMock()
        auth_svc.get_current_profile.return_value = None
        mock_service.current_auth_service.return_value = auth_svc

        result = runner.invoke(app, ["config", "-o", "json"])

    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data == {
        "environment": "https://api.example",
        "profile": None,
        "project_id": None,
        "project_name": None,
    }


def test_config_hidden_debug_commands() -> None:
    runner = CliRunner()
    with patch("llama_agents.cli.config.env_service.service") as mock_service:
        mock_service.config_manager.return_value.db_path = "/tmp/llamactl.db"
        result = runner.invoke(app, ["config", "show-db"])

    assert result.exit_code == 0, result.output
    assert "/tmp/llamactl.db" in result.output
