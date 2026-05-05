import json
import os
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from llama_agents.cli.app import app

_INTERACTIVE_PATCH = "llama_agents.cli.commands.init.is_interactive_session"


@pytest.fixture(autouse=True)
def _non_interactive() -> Any:
    """Default all init tests to non-interactive mode."""
    with patch(_INTERACTIVE_PATCH, return_value=False):
        yield


def test_init_help_shows_options() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--help"])
    assert result.exit_code == 0
    # Basic sanity checks on help text
    assert "Create a new app repository from a template" in result.output
    assert "--update" in result.output
    assert "--template" in result.output
    assert "--dir" in result.output
    assert "--force" in result.output


def test_init_create_with_flags_calls_copier_and_git(tmp_path: Path) -> None:
    runner = CliRunner()

    target_dir = tmp_path / "my-app"

    def _mock_run_copy(
        repo_url: str, dst: Path, quiet: bool = True, *args: Any, **kwargs: Any
    ) -> None:
        # Simulate template copy by creating the directory
        Path(dst).mkdir(parents=True, exist_ok=True)
        # Create a trivial file so that `git add .` has something
        (Path(dst) / "README.md").write_text("# App\n")

    def _mock_subprocess_run(
        cmd: list[str],
        check: bool = True,
        capture_output: bool = False,
        text: bool = False,
    ) -> MagicMock:
        # Emulate success
        return MagicMock(returncode=0, stdout="")

    def _mock_copy_scaffold() -> None:
        (target_dir / "AGENTS.md").write_text("# Agents\n")
        (target_dir / ".claude").mkdir(exist_ok=True)
        (target_dir / ".cursor").mkdir(exist_ok=True)
        (target_dir / ".codex").mkdir(exist_ok=True)

    with (
        patch(
            "copier.run_copy",
            side_effect=_mock_run_copy,
        ) as mock_copy,
        patch(
            "llama_agents.cli.commands.init.subprocess.run",
            side_effect=_mock_subprocess_run,
        ) as mock_subproc,
        patch(
            "llama_agents.cli.commands.init._copy_scaffold",
            side_effect=_mock_copy_scaffold,
        ) as mock_scaffold,
    ):
        result = runner.invoke(
            app,
            [
                "init",
                "--template",
                "basic-ui",
                "--dir",
                str(target_dir),
            ],
        )

        assert result.exit_code == 0, result.output
        mock_copy.assert_called_once()
        mock_scaffold.assert_called_once()
        # Git commands should have been attempted
        calls = [" ".join(call_args.args[0]) for call_args in mock_subproc.mock_calls]
        assert any(cmd.startswith("git --version") for cmd in calls)
        assert target_dir.exists()
        # Symlinks to AGENTS.md should be created by the init flow
        claude_link = target_dir / "CLAUDE.md"
        gemini_link = target_dir / "GEMINI.md"
        agents_file = target_dir / "AGENTS.md"
        assert agents_file.exists()
        assert claude_link.exists()
        assert claude_link.is_symlink()
        assert gemini_link.is_symlink()
        assert claude_link.resolve() == agents_file.resolve()
        assert gemini_link.resolve() == agents_file.resolve()


def test_init_update_calls_copier_run_update() -> None:
    runner = CliRunner()

    with (
        patch(
            "copier.run_update",
            return_value=None,
        ) as mock_update,
        patch(
            "llama_agents.cli.commands.init.subprocess.run",
            return_value=MagicMock(returncode=0, stdout=""),
        ),
    ):
        result = runner.invoke(app, ["init", "--update"])
        assert result.exit_code == 0, result.output
        mock_update.assert_called_once()


def test_init_handles_missing_git_gracefully(tmp_path: Path) -> None:
    runner = CliRunner()

    target_dir = tmp_path / "my-app-missing-git"

    def _mock_subprocess_run(
        cmd: list[str],
        check: bool = True,
        capture_output: bool = False,
        text: bool = False,
    ) -> MagicMock:
        if cmd[:2] == ["git", "--version"]:
            raise FileNotFoundError("git not found")
        return MagicMock(returncode=0, stdout="")

    with patch(
        "llama_agents.cli.commands.init.subprocess.run",
        side_effect=_mock_subprocess_run,
    ):
        result = runner.invoke(
            app,
            [
                "init",
                "--template",
                "basic-ui",
                "--dir",
                str(target_dir),
            ],
        )

        assert result.exit_code == 1, result.output
        assert "git is required" in result.output
        assert len(result.output.split("\n")) < 6
        assert not target_dir.exists()


def test_init_handles_git_init_failure_gracefully(tmp_path: Path) -> None:
    runner = CliRunner()

    target_dir = tmp_path / "my-app-git-fails"

    def _mock_run_copy(
        repo_url: str, dst: Path, quiet: bool = True, *args: Any, **kwargs: Any
    ) -> None:
        Path(dst).mkdir(parents=True, exist_ok=True)
        (Path(dst) / "README.md").write_text("# App\n")

    def _mock_subprocess_run(
        cmd: list[str],
        check: bool = True,
        capture_output: bool = False,
        text: bool = False,
    ) -> MagicMock:
        if cmd[:2] == ["git", "--version"]:
            return MagicMock(returncode=0, stdout="git version 2.x\n")
        if cmd[:2] == ["git", "init"]:
            raise subprocess.CalledProcessError(
                returncode=1, cmd=cmd, stderr=b"fatal: not a git repo\n"
            )
        return MagicMock(returncode=0, stdout="")

    with (
        patch("copier.run_copy", side_effect=_mock_run_copy),
        patch(
            "llama_agents.cli.commands.init.subprocess.run",
            side_effect=_mock_subprocess_run,
        ),
        patch("llama_agents.cli.commands.init._copy_scaffold") as mock_scaffold,
    ):
        result = runner.invoke(
            app,
            [
                "init",
                "--template",
                "basic-ui",
                "--dir",
                str(target_dir),
            ],
        )

        assert result.exit_code == 0, result.output
        assert target_dir.exists()
        assert "Skipping git initialization" in result.output
        mock_scaffold.assert_called_once()


def test_init_skips_git_init_when_inside_parent_repo(tmp_path: Path) -> None:
    runner = CliRunner()

    target_dir = tmp_path / "my-app-inside-repo"

    def _mock_run_copy(
        repo_url: str, dst: Path, quiet: bool = True, *args: Any, **kwargs: Any
    ) -> None:
        Path(dst).mkdir(parents=True, exist_ok=True)
        (Path(dst) / "README.md").write_text("# App\n")

    def _mock_subprocess_run(
        cmd: list[str],
        check: bool = True,
        capture_output: bool = False,
        text: bool = False,
    ) -> MagicMock:
        # Simulate: git is available, and current directory is inside a repo
        if cmd[:2] == ["git", "--version"]:
            return MagicMock(returncode=0, stdout="git version 2.x\n")
        if cmd[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return MagicMock(returncode=0, stdout="true\n")
        # These commands should NOT be called when inside a parent repo
        if cmd[:2] in (["git", "init"], ["git", "add"], ["git", "commit"]):
            raise AssertionError(
                "git init/add/commit should not be called inside parent repo"
            )
        return MagicMock(returncode=0, stdout="")

    with (
        patch("copier.run_copy", side_effect=_mock_run_copy),
        patch(
            "llama_agents.cli.commands.init.subprocess.run",
            side_effect=_mock_subprocess_run,
        ),
        patch("llama_agents.cli.commands.init._copy_scaffold"),
    ):
        result = runner.invoke(
            app,
            [
                "init",
                "--template",
                "basic-ui",
                "--dir",
                str(target_dir),
            ],
        )

        assert result.exit_code == 0, result.output
        assert target_dir.exists()
        assert "Detected an existing Git repository" in result.output


def test_copy_scaffold(tmp_path: Path) -> None:
    from llama_agents.cli.commands.init import _copy_scaffold

    os.chdir(tmp_path)
    _copy_scaffold()

    # AGENTS.md
    content = (tmp_path / "AGENTS.md").read_text()
    assert "developers.llamaindex.ai/mcp" in content
    assert "search_docs" in content

    # Claude Code: .mcp.json + .claude/settings.json
    mcp_json = json.loads((tmp_path / ".mcp.json").read_text())
    assert "llama-index-docs" in mcp_json["mcpServers"]

    claude_settings = json.loads((tmp_path / ".claude" / "settings.json").read_text())
    assert "mcp__llama-index-docs" in claude_settings["permissions"]["allow"]
    assert claude_settings["enableAllProjectMcpServers"] is True

    # Cursor config
    cursor_mcp = json.loads((tmp_path / ".cursor" / "mcp.json").read_text())
    assert "llama-index-docs" in cursor_mcp["mcpServers"]

    # Codex config
    codex_toml = (tmp_path / ".codex" / "config.toml").read_text()
    assert "llama-index-docs" in codex_toml


def test_init_non_interactive_requires_template(tmp_path: Path) -> None:
    """Test that init in non-interactive mode requires a template to be specified."""
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["init"],
    )

    # Should exit with error
    assert result.exit_code == 1
    # Should mention template is required
    assert "No template selected" in result.output
    assert "template" in result.output.lower()


def test_init_non_interactive_defaults_directory(tmp_path: Path) -> None:
    """Test that init defaults to template name for directory in non-interactive mode."""
    runner = CliRunner()

    def _mock_run_copy(
        repo_url: str, dst: Path, quiet: bool = True, *args: Any, **kwargs: Any
    ) -> None:
        Path(dst).mkdir(parents=True, exist_ok=True)
        (Path(dst) / "README.md").write_text("# App\n")

    def _mock_subprocess_run(
        cmd: list[str],
        check: bool = True,
        capture_output: bool = False,
        text: bool = False,
    ) -> MagicMock:
        return MagicMock(returncode=0, stdout="")

    with (
        patch("copier.run_copy", side_effect=_mock_run_copy),
        patch(
            "llama_agents.cli.commands.init.subprocess.run",
            side_effect=_mock_subprocess_run,
        ),
        patch("llama_agents.cli.commands.init._copy_scaffold"),
    ):
        # Change to tmp_path to avoid creating directories in the real workspace
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)

            result = runner.invoke(
                app,
                ["init", "--template", "basic-ui"],
            )

            assert result.exit_code == 0, result.output
            # Should default to template name
            assert (tmp_path / "basic-ui").exists()
            assert "Defaulting to basic-ui" in result.output
        finally:
            os.chdir(original_cwd)


def test_init_force_flag_skips_confirmation(tmp_path: Path) -> None:
    """Test that --force flag skips overwrite confirmation."""
    runner = CliRunner()

    target_dir = tmp_path / "my-app"
    target_dir.mkdir()
    (target_dir / "existing.txt").write_text("existing content")

    def _mock_run_copy(
        repo_url: str, dst: Path, quiet: bool = True, *args: Any, **kwargs: Any
    ) -> None:
        Path(dst).mkdir(parents=True, exist_ok=True)
        (Path(dst) / "README.md").write_text("# App\n")

    def _mock_subprocess_run(
        cmd: list[str],
        check: bool = True,
        capture_output: bool = False,
        text: bool = False,
    ) -> MagicMock:
        return MagicMock(returncode=0, stdout="")

    with (
        patch("copier.run_copy", side_effect=_mock_run_copy),
        patch(
            "llama_agents.cli.commands.init.subprocess.run",
            side_effect=_mock_subprocess_run,
        ),
        patch("llama_agents.cli.commands.init._copy_scaffold"),
    ):
        result = runner.invoke(
            app,
            [
                "init",
                "--template",
                "basic-ui",
                "--dir",
                str(target_dir),
                "--force",
            ],
        )

        assert result.exit_code == 0, result.output
        # Directory should be overwritten
        assert not (target_dir / "existing.txt").exists()
        assert (target_dir / "README.md").exists()


def test_init_existing_directory_no_force_exits(tmp_path: Path) -> None:
    """Test that init exits when directory exists without --force in non-interactive mode."""
    runner = CliRunner()

    target_dir = tmp_path / "my-app"
    target_dir.mkdir()

    result = runner.invoke(
        app,
        [
            "init",
            "--template",
            "basic-ui",
            "--dir",
            str(target_dir),
        ],
    )

    # Should exit with error
    assert result.exit_code == 1
    assert "--force" in result.output or "force" in result.output.lower()
