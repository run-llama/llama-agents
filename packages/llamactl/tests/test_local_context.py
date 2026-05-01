# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``cli.local_context.gather_local_context``."""

from __future__ import annotations

from pathlib import Path

import pytest
from llama_agents.cli.local_context import (
    LocalContext,
    gather_local_context,
    normalize_git_url_to_http,
)


def test_normalize_https_strip_creds_and_suffix() -> None:
    assert (
        normalize_git_url_to_http("https://user:pass@github.com/user/repo.git")
        == "https://github.com/user/repo"
    )


def test_normalize_scp_style() -> None:
    assert (
        normalize_git_url_to_http("git@github.com:user/repo.git")
        == "https://github.com/user/repo"
    )


def test_normalize_strips_ssh_port() -> None:
    assert (
        normalize_git_url_to_http("ssh://git@bitbucket.org:7999/team/repo.git")
        == "https://bitbucket.org/team/repo"
    )


def test_normalize_preserves_https_port() -> None:
    assert (
        normalize_git_url_to_http("https://git.example.com:8443/org/repo.git")
        == "https://git.example.com:8443/org/repo"
    )


def test_normalize_bare_host_path() -> None:
    assert (
        normalize_git_url_to_http("gitlab.com/group/sub/repo.git")
        == "https://gitlab.com/group/sub/repo"
    )


def test_normalize_rewrites_http_to_https() -> None:
    assert (
        normalize_git_url_to_http("http://github.com/user/repo")
        == "https://github.com/user/repo"
    )


def test_normalize_scp_style_no_dot_git_suffix() -> None:
    assert (
        normalize_git_url_to_http("github.com:user/repo")
        == "https://github.com/user/repo"
    )


def testpick_preferred_remote_prefers_github() -> None:
    from llama_agents.cli.local_context import pick_preferred_remote

    assert (
        pick_preferred_remote(
            [
                "ssh://git@bitbucket.org/team/repo.git",
                "git@github.com:user/repo.git",
            ]
        )
        == "https://github.com/user/repo"
    )


def testpick_preferred_remote_dedupes_and_handles_empty() -> None:
    from llama_agents.cli.local_context import pick_preferred_remote

    assert pick_preferred_remote([]) is None
    assert (
        pick_preferred_remote(
            [
                "git@github.com:user/repo.git",
                "https://github.com/user/repo.git",
            ]
        )
        == "https://github.com/user/repo"
    )


def test_gather_outside_git_repo_returns_safe_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("llama_agents.cli.local_context.is_git_repo", lambda: False)

    ctx = gather_local_context()

    assert isinstance(ctx, LocalContext)
    assert ctx.is_git_repo is False
    assert ctx.repo_url is None
    assert ctx.git_ref is None
    assert ctx.available_secrets == {}
    assert ctx.required_secret_names == []
    assert ctx.deployment_file_path is None
    # Missing config files → silent default; no warning is emitted.
    assert ctx.warnings == []


def test_gather_in_git_repo_with_env_and_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llama_deploy.yaml").write_text(
        """
name: my-app
workflows:
  svc: "module.workflow:flow"
required_env_vars: ["API_KEY", "PORT"]
""".strip()
    )
    (tmp_path / ".env").write_text("API_KEY=secret\nPORT=8080\n")

    monkeypatch.setattr("llama_agents.cli.local_context.is_git_repo", lambda: True)
    monkeypatch.setattr(
        "llama_agents.cli.local_context.list_remotes",
        lambda: [
            "ssh://git@bitbucket.org/team/repo.git",
            "git@github.com:user/repo.git",
        ],
    )
    monkeypatch.setattr(
        "llama_agents.cli.local_context.get_current_branch", lambda: "develop"
    )
    monkeypatch.setattr("llama_agents.cli.local_context.get_git_root", lambda: tmp_path)

    ctx = gather_local_context()

    assert ctx.is_git_repo is True
    # github URL is preferred over bitbucket.
    assert ctx.repo_url == "https://github.com/user/repo"
    assert ctx.git_ref == "develop"
    assert ctx.generate_name == "my-app"
    assert ctx.available_secrets == {"API_KEY": "secret", "PORT": "8080"}
    assert sorted(ctx.required_secret_names) == ["API_KEY", "PORT"]
    # cwd == git_root → no nested deployment_file_path.
    assert ctx.deployment_file_path is None


def test_gather_subdir_of_git_repo_records_relative_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub = tmp_path / "services" / "app"
    sub.mkdir(parents=True)
    (sub / "llama_deploy.yaml").write_text(
        """
name: app
workflows:
  svc: "x.y:flow"
""".strip()
    )
    monkeypatch.chdir(sub)

    monkeypatch.setattr("llama_agents.cli.local_context.is_git_repo", lambda: True)
    monkeypatch.setattr("llama_agents.cli.local_context.list_remotes", lambda: [])
    monkeypatch.setattr(
        "llama_agents.cli.local_context.get_current_branch", lambda: "main"
    )
    monkeypatch.setattr("llama_agents.cli.local_context.get_git_root", lambda: tmp_path)

    ctx = gather_local_context()

    assert ctx.deployment_file_path == str(Path("services") / "app")


def test_gather_with_invalid_config_records_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llama_deploy.yaml").write_text("not: [valid: yaml")

    monkeypatch.setattr("llama_agents.cli.local_context.is_git_repo", lambda: False)
    ctx = gather_local_context()
    assert any("Could not parse" in w for w in ctx.warnings)


def test_gather_skips_default_named_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the config name is the default, ``generate_name`` is left unset."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llama_deploy.yaml").write_text(
        """
workflows:
  svc: "x.y:flow"
""".strip()
    )
    monkeypatch.setattr("llama_agents.cli.local_context.is_git_repo", lambda: False)
    ctx = gather_local_context()
    assert ctx.generate_name is None
