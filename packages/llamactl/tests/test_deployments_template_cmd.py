# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``llamactl deployments template``."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml as pyyaml
from click.testing import CliRunner
from llama_agents.cli.app import app
from llama_agents.cli.local_context import LocalContext


def _patch_git(
    monkeypatch: pytest.MonkeyPatch,
    *,
    is_repo: bool,
    branch: str = "main",
    remotes: list[str] | None = None,
    git_root: Path | None = None,
) -> None:
    monkeypatch.setattr("llama_agents.cli.local_context.is_git_repo", lambda: is_repo)
    monkeypatch.setattr(
        "llama_agents.cli.local_context.list_remotes",
        lambda: remotes if remotes is not None else [],
    )
    monkeypatch.setattr(
        "llama_agents.cli.local_context.get_current_branch", lambda: branch
    )
    if git_root is not None:
        monkeypatch.setattr(
            "llama_agents.cli.local_context.get_git_root", lambda: git_root
        )


def test_template_in_git_repo_emits_expected_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llama_deploy.yaml").write_text(
        """
name: my-app
workflows:
  svc: "module.workflow:flow"
required_env_vars: ["API_KEY", "DB_URL"]
""".strip()
    )
    (tmp_path / ".env").write_text("API_KEY=k\n")

    _patch_git(
        monkeypatch,
        is_repo=True,
        branch="develop",
        remotes=["git@github.com:user/repo.git"],
        git_root=tmp_path,
    )

    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "template"])
    assert result.exit_code == 0, result.output

    out = result.output
    # Identity tier: both keys commented-out; ``generate_name`` carries the
    # config-derived name (``my-app`` from llama_deploy.yaml).
    assert "\n# name: " in out
    assert "# generate_name: my-app" in out
    # Spec block has no legacy display_name / generateName.
    assert "  display_name:" not in out
    assert "generateName" not in out
    # Push-mode signal: empty repo_url, double-quoted.
    assert 'repo_url: ""' in out
    # Detected remote alternative line follows directly under the empty repo_url.
    repo_idx = out.index('repo_url: ""')
    after = out[repo_idx:]
    next_line = after.splitlines()[1]
    assert "# repo_url: https://github.com/user/repo" in next_line
    assert "## auto-detected from your git remotes" in next_line
    # Detected branch.
    assert "git_ref: develop" in out
    # Required secrets rendered as ${VAR}.
    assert "API_KEY: ${API_KEY}" in out
    assert "DB_URL: ${DB_URL}" in out
    # Set-mode secret annotations are trailing comments on the secret lines.
    api_line = next(line for line in out.splitlines() if "API_KEY:" in line)
    assert api_line.startswith("    API_KEY: ${API_KEY}")
    assert api_line.endswith("## from your .env")
    assert "    ## from your .env" not in out, out
    # Missing-from-.env secret carries the explicit "not in your .env" comment.
    db_line = next(line for line in out.splitlines() if "DB_URL:" in line)
    assert db_line.startswith("    DB_URL: ${DB_URL}")
    assert db_line.endswith("## not in your .env — add it before apply")
    assert "Not in your .env" not in out
    assert "before `apply`" not in out
    # No "Optional fields" tail block — single-pass rendering.
    assert "Optional fields" not in out
    # Output parses as YAML when comments are stripped.
    parsed = pyyaml.safe_load(out)
    assert parsed["spec"]["repo_url"] == ""
    assert parsed["spec"]["git_ref"] == "develop"


def test_template_emits_local_context_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "llama_agents.cli.commands.deployment.gather_local_context",
        lambda: LocalContext(
            is_git_repo=True,
            repo_url="https://github.com/user/repo",
            git_ref="main",
            warnings=["Could not parse local deployment config. It may be invalid."],
        ),
    )

    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "template"])
    assert result.exit_code == 0, result.output

    lines = result.output.splitlines()
    assert (
        lines[0]
        == "## WARNING: Could not parse local deployment config. It may be invalid."
    )
    assert lines[1] == "##"
    assert lines[2] == "## Edit, then run: llamactl deployments apply -f <file>"


def test_template_outside_git_repo_emits_compact_head_and_required_tildes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_git(monkeypatch, is_repo=False)
    cwd_name = tmp_path.name

    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "template"])
    assert result.exit_code == 0, result.output

    out = result.output
    lines = out.splitlines()
    assert lines[0] == "## Edit, then run: llamactl deployments apply -f <file>"
    assert lines[1] == "##"
    assert (
        lines[2]
        == "## NOT IN A GIT REPO — set repo_url, or cd into a working tree and re-run."
    )
    assert "═══" not in out
    assert "Set repo_url below before running apply." not in out

    # ``repo_url`` is the only required-tilde field outside a git repo;
    # ``name`` and ``generate_name`` are commented-out (server defaults the id).
    assert "  repo_url: ~" in out
    repo_idx = out.index("  repo_url: ~")
    assert (
        '  ## REQUIRED. "" = push your local working tree on apply.' in out[:repo_idx]
    )
    assert "## Required — set before `apply`." not in out

    # Top-level identity tier: both keys commented-out, cwd-derived defaults.
    assert f"\n# name: {cwd_name}" in out
    assert f"# generate_name: {cwd_name}" in out
    # No uncommented identity-tier line.
    assert "\nname:" not in out
    assert "\ngenerate_name:" not in out
    # Spec block does NOT contain the legacy display_name / generateName.
    assert "  # generateName" not in out
    assert "  display_name:" not in out

    # Other unset fields render as commented-out one-liners in declaration
    # order inside the spec block.
    assert "  # deployment_file_path:" in out
    assert "  # git_ref: main" in out
    assert "  # suspended: false" in out
    assert "  # secrets:" in out
    assert "    # MY_SECRET: ${MY_SECRET}" in out
    assert "  # personal_access_token:" in out

    # No "Optional fields" tail block.
    assert "Optional fields" not in out

    # YAML round-trip: required ~ parses to None; commented keys are absent.
    parsed = pyyaml.safe_load(out)
    assert "name" not in parsed
    assert "generate_name" not in parsed
    assert "display_name" not in parsed["spec"]
    assert parsed["spec"]["repo_url"] is None


def test_template_emits_blank_lines_between_output_sections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_git(monkeypatch, is_repo=False)

    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "template"])
    assert result.exit_code == 0, result.output

    lines = result.output.splitlines()
    assert lines[0] == "## Edit, then run: llamactl deployments apply -f <file>"
    assert lines[1] == "##"
    assert lines[3] == ""

    generate_name_idx = next(
        i for i, line in enumerate(lines) if line.startswith("# generate_name:")
    )
    spec_idx = lines.index("spec:")
    assert lines[spec_idx - 1] == ""
    assert spec_idx > generate_name_idx

    repo_doc_idx = next(
        i for i, line in enumerate(lines) if line.startswith("  ## REQUIRED.")
    )
    deploy_path_group_idx = next(
        i
        for i, line in enumerate(lines)
        if line.startswith("  # deployment_file_path:")
        or "pyproject.toml or llama_deploy.yaml" in line
    )
    assert lines[deploy_path_group_idx - 1] == ""
    assert deploy_path_group_idx > repo_doc_idx


def test_template_does_not_require_auth_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The command must work without an auth profile configured.

    The conftest's per-worker LLAMACTL_CONFIG_DIR is empty by default — no
    ``patched_auth`` fixture is applied here, so a successful invocation is
    proof the command did not call ``validate_authenticated_profile``.
    """
    monkeypatch.chdir(tmp_path)
    _patch_git(monkeypatch, is_repo=False)

    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "template"])
    assert result.exit_code == 0, result.output


def test_template_has_no_display_name_or_name_flag() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["deployments", "template", "--help"])
    assert result.exit_code == 0
    assert "--display-name" not in result.output
    assert "--name" not in result.output


def test_template_advertises_template_only_on_deployments_get() -> None:
    """`-o template` is local to ``deployments get`` — not advertised on
    other read commands that share the same output decorator."""
    runner = CliRunner()
    for argv in (
        ["auth", "organizations", "--help"],
        ["auth", "env", "list", "--help"],
        ["deployments", "history", "--help"],
    ):
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, result.output
        assert "template" not in result.output.lower(), (
            f"{argv} should not advertise template output: {result.output}"
        )
    result = runner.invoke(app, ["deployments", "get", "--help"])
    assert result.exit_code == 0
    assert "template" in result.output.lower()
