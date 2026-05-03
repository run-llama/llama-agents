# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import llama_agents.cli.config.env_service as env_service
import llama_agents.cli.param_types as param_types
import pytest
from click.testing import CliRunner
from llama_agents.cli.app import app
from llama_agents.core.client.manage_client import ProjectClient


def _first_matching_line_index(lines: list[str], predicate: str) -> int:
    for index, line in enumerate(lines):
        if predicate in line and not line.lstrip().startswith("#"):
            return index
    raise AssertionError(f"Could not find live line containing {predicate!r}")


def _invoke_zsh_install(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "home", lambda: home)
    runner = CliRunner()
    result = runner.invoke(app, ["completion", "install", "--shell", "zsh"])
    assert result.exit_code == 0, result.output


def test_completion_generate_zsh() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["completion", "generate", "zsh"])
    assert result.exit_code == 0
    assert "_LLAMACTL_COMPLETE" in result.output or "compdef" in result.output


def test_completion_generate_bash() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["completion", "generate", "bash"])
    assert result.exit_code == 0
    assert "_LLAMACTL_COMPLETE" in result.output or "complete" in result.output


def test_completion_generate_fish() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["completion", "generate", "fish"])
    assert result.exit_code == 0
    assert "llamactl" in result.output


def test_completion_generate_invalid_shell() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["completion", "generate", "powershell"])
    assert result.exit_code != 0


def test_completion_install_dry_run() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app, ["completion", "install", "--shell", "zsh", "--dry-run"]
    )
    assert result.exit_code == 0
    assert "Would write" in result.output


def test_completion_install_dry_run_bash() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app, ["completion", "install", "--shell", "bash", "--dry-run"]
    )
    assert result.exit_code == 0
    assert "Would write" in result.output


def test_completion_install_dry_run_fish() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app, ["completion", "install", "--shell", "fish", "--dry-run"]
    )
    assert result.exit_code == 0
    assert "Would write" in result.output


def test_completion_install_zsh_repairs_ordered_completion_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    zshrc = home / ".zshrc"
    zshrc.write_text(
        "autoload -Uz compinit && compinit\n"
        "fpath=(~/.zfunc $fpath)\n"
        'echo "custom shell setup"\n'
    )

    _invoke_zsh_install(home, monkeypatch)

    lines = zshrc.read_text().splitlines()
    fpath_index = _first_matching_line_index(lines, "~/.zfunc")
    compinit_index = _first_matching_line_index(lines, "compinit")
    assert fpath_index < compinit_index

    first_pass = zshrc.read_text()
    _invoke_zsh_install(home, monkeypatch)
    assert zshrc.read_text() == first_pass


def test_completion_install_zsh_bootstraps_compinit_when_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    zshrc = home / ".zshrc"
    zshrc.write_text('export PATH="$HOME/bin:$PATH"\n')

    _invoke_zsh_install(home, monkeypatch)

    lines = zshrc.read_text().splitlines()
    fpath_index = _first_matching_line_index(lines, "~/.zfunc")
    compinit_index = _first_matching_line_index(lines, "compinit")
    assert fpath_index < compinit_index
    live_compinit_lines = [
        line
        for line in lines
        if "compinit" in line and not line.lstrip().startswith("#")
    ]
    assert len(live_compinit_lines) == 1


def test_completion_group_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["completion", "--help"])
    assert result.exit_code == 0
    assert "generate" in result.output
    assert "install" in result.output


def test_completion_safe_fetch_handles_env_api_key_without_project_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "env-api-key")
    monkeypatch.delenv("LLAMA_DEPLOY_PROJECT_ID", raising=False)
    monkeypatch.setenv("_LLAMACTL_COMPLETE", "zsh_source")

    mock_auth_svc = MagicMock()
    mock_auth_svc.get_current_profile.return_value = None
    mock_auth_svc.env = SimpleNamespace(requires_auth=True)
    mock_service = MagicMock()
    mock_service.current_auth_service.return_value = mock_auth_svc
    mock_service.get_current_environment.return_value = SimpleNamespace(
        api_url="https://api.cloud.llamaindex.ai",
        requires_auth=True,
    )
    monkeypatch.setattr(env_service, "service", mock_service)

    async def _empty_deployments(self: ProjectClient) -> list[Any]:
        return []

    monkeypatch.setattr(ProjectClient, "list_deployments", _empty_deployments)

    assert param_types._safe_fetch(param_types._fetch_deployments, timeout=1.0) == []
