# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``llamactl organizations get`` output modes."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner
from llama_agents.cli.app import app
from llama_agents.core.schema.projects import OrgSummary


def test_organizations_get_text_lists_orgs() -> None:
    runner = CliRunner()
    orgs = [
        OrgSummary(org_id="org-a", org_name="Acme", is_default=True),
        OrgSummary(org_id="org-b", org_name="Beta"),
    ]
    auth_svc = MagicMock()
    auth_svc.get_current_profile.return_value = SimpleNamespace(project_id="proj-b")
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=[
                SimpleNamespace(project_id="proj-b", org_id="org-b"),
            ],
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "get"])
    assert result.exit_code == 0, result.output
    assert "ORG_ID" in result.output
    assert "NAME" in result.output
    assert "DEFAULT" in result.output
    assert "ACTIVE" in result.output
    assert result.output.splitlines()[0].startswith("NAME")
    assert "org-a" in result.output
    assert "Acme" in result.output
    assert "yes" in result.output
    assert "no" in result.output
    assert "org-b" in result.output
    # ANSI / Rich markup should not leak.
    assert "\x1b[" not in result.output


def test_organizations_get_json() -> None:
    runner = CliRunner()
    orgs = [
        OrgSummary(org_id="org-a", org_name="Acme", is_default=True),
        OrgSummary(org_id="org-b", org_name="Beta"),
    ]
    auth_svc = MagicMock()
    auth_svc.get_current_profile.return_value = SimpleNamespace(project_id="proj-b")
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=[
                SimpleNamespace(project_id="proj-b", org_id="org-b"),
            ],
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "get", "-o", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert list(data[0]) == ["org_id", "org_name", "is_default", "active"]
    assert {d["org_id"] for d in data} == {"org-a", "org-b"}
    default = next(d for d in data if d["org_id"] == "org-a")
    active = next(d for d in data if d["org_id"] == "org-b")
    assert default["is_default"] is True
    assert default["active"] is False
    assert active["active"] is True


def test_organizations_get_yaml() -> None:
    runner = CliRunner()
    orgs = [OrgSummary(org_id="org-a", org_name="Acme", is_default=True)]
    auth_svc = MagicMock()
    auth_svc.get_current_profile.return_value = SimpleNamespace(project_id="proj-a")
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=[
                SimpleNamespace(project_id="proj-a", org_id="org-a"),
            ],
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "get", "-o", "yaml"])
    assert result.exit_code == 0, result.output
    data = yaml.safe_load(result.output)
    assert isinstance(data, list)
    assert data[0]["org_id"] == "org-a"


def test_organizations_get_falls_back_to_default_org_when_project_org_unknown() -> None:
    runner = CliRunner()
    orgs = [
        OrgSummary(org_id="org-a", org_name="Acme", is_default=True),
        OrgSummary(org_id="org-b", org_name="Beta"),
    ]
    auth_svc = MagicMock()
    auth_svc.get_current_profile.return_value = SimpleNamespace(
        project_id="proj-missing"
    )
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=[
                SimpleNamespace(project_id="proj-b", org_id="org-b"),
            ],
        ),
        patch(
            "llama_agents.cli.commands.organizations.validate_authenticated_profile",
        ) as mock_validate,
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "get", "-o", "json"])

    assert result.exit_code == 0, result.output
    mock_validate.assert_not_called()
    data = json.loads(result.output)
    assert next(d for d in data if d["org_id"] == "org-a")["active"] is True
    assert next(d for d in data if d["org_id"] == "org-b")["active"] is False


def test_organizations_get_unsupported_text_warns() -> None:
    runner = CliRunner()
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=False,
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["organizations", "get"])
    assert result.exit_code == 0, result.output
    assert "does not support organizations" in result.output


def test_organizations_get_unsupported_json_emits_empty_list() -> None:
    """Structured outputs should be parseable even on unsupported servers."""
    runner = CliRunner()
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=False,
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["organizations", "get", "-o", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == []


def test_organizations_get_does_not_offer_wide_output() -> None:
    result = CliRunner().invoke(app, ["organizations", "get", "-o", "wide"])
    assert result.exit_code != 0
    assert "'wide' is not one of 'text', 'json', 'yaml'" in result.output


def test_organizations_use_switches_to_first_project_for_org() -> None:
    runner = CliRunner()
    orgs = [
        OrgSummary(org_id="org-a", org_name="Acme", is_default=True),
        OrgSummary(org_id="org-b", org_name="Beta"),
    ]
    projects = [
        MagicMock(
            project_id="proj-b-default",
            project_name="Beta Default",
            deployment_count=0,
        )
    ]
    auth_svc = MagicMock()
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations.validate_authenticated_profile",
            return_value=SimpleNamespace(name="default", project_id="proj-a"),
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=projects,
        ) as mock_list_projects,
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "use", "org-b"])

    assert result.exit_code == 0, result.output
    mock_list_projects.assert_called_once_with(auth_svc, org_id="org-b")
    auth_svc.set_project.assert_called_once_with("default", "proj-b-default")
    assert "switched organization Beta and project Beta Default" in result.output


def test_organizations_use_selects_org_when_argument_missing() -> None:
    runner = CliRunner()
    orgs = [
        OrgSummary(org_id="org-a", org_name="Acme", is_default=True),
        OrgSummary(org_id="org-b", org_name="Beta"),
    ]
    projects = [
        MagicMock(
            project_id="proj-b-default",
            project_name="Beta Default",
            deployment_count=0,
        )
    ]
    auth_svc = MagicMock()
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations.validate_authenticated_profile",
            return_value=SimpleNamespace(name="default", project_id="proj-a"),
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=projects,
        ),
        patch(
            "llama_agents.cli.commands.organizations.select_or_exit",
            return_value=orgs[1],
        ) as mock_select,
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "use"])

    assert result.exit_code == 0, result.output
    mock_select.assert_called_once()
    auth_svc.set_project.assert_called_once_with("default", "proj-b-default")


def test_organizations_use_current_project_noops() -> None:
    runner = CliRunner()
    orgs = [OrgSummary(org_id="org-b", org_name="Beta")]
    projects = [
        MagicMock(
            project_id="proj-b-default",
            project_name="Beta Default",
            deployment_count=0,
        )
    ]
    auth_svc = MagicMock()
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations.validate_authenticated_profile",
            return_value=SimpleNamespace(name="default", project_id="proj-b-default"),
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=projects,
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "use", "org-b"])

    assert result.exit_code == 0, result.output
    assert "already using organization Beta" in result.output
    auth_svc.set_project.assert_not_called()


def test_organizations_use_missing_org_errors() -> None:
    runner = CliRunner()
    orgs = [OrgSummary(org_id="org-a", org_name="Acme", is_default=True)]
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations.validate_authenticated_profile",
            return_value=SimpleNamespace(name="default", project_id="proj-a"),
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["organizations", "use", "org-missing"])

    assert result.exit_code != 0
    assert "Organization org-missing not found" in result.output


def test_organizations_use_errors_when_org_has_no_projects() -> None:
    runner = CliRunner()
    orgs = [OrgSummary(org_id="org-a", org_name="Acme", is_default=True)]
    auth_svc = MagicMock()
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations.validate_authenticated_profile",
            return_value=SimpleNamespace(name="default", project_id="proj-a"),
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_projects",
            return_value=[],
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = auth_svc
        result = runner.invoke(app, ["organizations", "use", "org-a"])

    assert result.exit_code != 0
    assert "Organization org-a has no projects" in result.output
    auth_svc.set_project.assert_not_called()


def test_organizations_use_unsupported_server_errors() -> None:
    runner = CliRunner()
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=False,
        ),
        patch(
            "llama_agents.cli.commands.organizations.validate_authenticated_profile",
            return_value=SimpleNamespace(name="default", project_id="proj-a"),
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["organizations", "use", "org-a"])

    assert result.exit_code != 0
    assert "does not support organizations" in result.output
