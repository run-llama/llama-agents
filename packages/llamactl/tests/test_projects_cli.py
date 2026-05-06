# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from llama_agents.cli.app import app


def test_projects_get_lists_projects_json() -> None:
    runner = CliRunner()
    projects = [
        MagicMock(project_id="proj-a", project_name="Project A", deployment_count=2),
        MagicMock(project_id="proj-b", project_name="Project B", deployment_count=0),
    ]
    with (
        patch(
            "llama_agents.cli.commands.projects.validate_authenticated_profile",
            return_value=MagicMock(project_id="proj-a"),
        ),
        patch(
            "llama_agents.cli.commands.projects._discover_organization",
            return_value=None,
        ),
        patch(
            "llama_agents.cli.commands.projects._list_projects", return_value=projects
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["projects", "get", "-o", "json"])

    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert [item["project_id"] for item in data] == ["proj-a", "proj-b"]
    assert list(data[0]) == [
        "project_id",
        "project_name",
        "deployment_count",
        "active",
    ]
    assert data[0]["active"] is True


def test_projects_get_text_puts_name_first_and_uses_yes_no() -> None:
    runner = CliRunner()
    projects = [
        MagicMock(project_id="proj-a", project_name="Project A", deployment_count=2),
        MagicMock(project_id="proj-b", project_name="Project B", deployment_count=0),
    ]
    with (
        patch(
            "llama_agents.cli.commands.projects.validate_authenticated_profile",
            return_value=MagicMock(project_id="proj-a"),
        ),
        patch(
            "llama_agents.cli.commands.projects._discover_organization",
            return_value=None,
        ),
        patch(
            "llama_agents.cli.commands.projects._list_projects", return_value=projects
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["projects", "get"])

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0].startswith("NAME")
    assert "Project A" in result.output
    assert "yes" in result.output
    assert "no" in result.output


def test_projects_get_single_project() -> None:
    runner = CliRunner()
    projects = [
        MagicMock(project_id="proj-a", project_name="Project A", deployment_count=2),
        MagicMock(project_id="proj-b", project_name="Project B", deployment_count=0),
    ]
    with (
        patch(
            "llama_agents.cli.commands.projects.validate_authenticated_profile",
            return_value=MagicMock(project_id="proj-a"),
        ),
        patch(
            "llama_agents.cli.commands.projects._discover_organization",
            return_value=None,
        ),
        patch(
            "llama_agents.cli.commands.projects._list_projects", return_value=projects
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["projects", "get", "proj-b", "-o", "json"])

    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["project_id"] == "proj-b"
    assert data["active"] is False


def test_auth_project_no_longer_exists() -> None:
    result = CliRunner().invoke(app, ["auth", "project"])
    assert result.exit_code != 0
    assert "Use `llamactl projects` instead." in result.output


def test_projects_get_does_not_offer_wide_output() -> None:
    result = CliRunner().invoke(app, ["projects", "get", "-o", "wide"])
    assert result.exit_code != 0
    assert "'wide' is not one of 'text', 'json', 'yaml'" in result.output
