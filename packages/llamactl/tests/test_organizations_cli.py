# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``llamactl organizations get`` output modes."""

from __future__ import annotations

import json
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
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["organizations", "get"])
    assert result.exit_code == 0, result.output
    assert "ORG_ID" in result.output
    assert "NAME" in result.output
    assert "DEFAULT" in result.output
    assert "ACTIVE" in result.output
    assert "org-a" in result.output
    assert "Acme" in result.output
    # ANSI / Rich markup should not leak.
    assert "\x1b[" not in result.output


def test_organizations_get_json() -> None:
    runner = CliRunner()
    orgs = [
        OrgSummary(org_id="org-a", org_name="Acme", is_default=True),
        OrgSummary(org_id="org-b", org_name="Beta"),
    ]
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["organizations", "get", "-o", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert {d["org_id"] for d in data} == {"org-a", "org-b"}
    default = next(d for d in data if d["org_id"] == "org-a")
    assert default["is_default"] is True
    assert default["active"] is True


def test_organizations_get_yaml() -> None:
    runner = CliRunner()
    orgs = [OrgSummary(org_id="org-a", org_name="Acme", is_default=True)]
    with (
        patch(
            "llama_agents.cli.commands.organizations.probe_organizations_support",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.commands.organizations._list_organizations",
            return_value=orgs,
        ),
        patch("llama_agents.cli.config.env_service.service") as mock_service,
    ):
        mock_service.current_auth_service.return_value = MagicMock()
        result = runner.invoke(app, ["organizations", "get", "-o", "yaml"])
    assert result.exit_code == 0, result.output
    data = yaml.safe_load(result.output)
    assert isinstance(data, list)
    assert data[0]["org_id"] == "org-a"


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
