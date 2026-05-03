from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from dulwich.errors import NotGitRepository
from llama_agents.cli.app import app


def _write_minimal_yaml(tmpdir: Path) -> Path:
    cfg = tmpdir / "llama_deploy.yaml"
    cfg.write_text(
        (
            "name: test\n"
            "llama_cloud: false\n"
            "workflows:\n  default: tests.fake_module:fake_workflow\n"
        ),
        encoding="utf-8",
    )
    # minimal python project structure for appserver prepare pre-check
    (tmpdir / "pyproject.toml").write_text(
        "[project]\nname='x'\nversion='0.0.0'\n", encoding="utf-8"
    )
    (tmpdir / "tests").mkdir(exist_ok=True)
    (tmpdir / "tests" / "__init__.py").write_text("", encoding="utf-8")
    (tmpdir / "tests" / "fake_module.py").write_text(
        "from workflows import Workflow\n\nfake_workflow = Workflow()\n",
        encoding="utf-8",
    )
    return cfg


def test_serve_does_not_crash_without_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ensure no leaked env causes branching behavior
    monkeypatch.delenv("LLAMA_CLOUD_API_KEY", raising=False)
    monkeypatch.delenv("LLAMA_AGENTS_PROJECT_ID", raising=False)
    monkeypatch.delenv("LLAMA_DEPLOY_PROJECT_ID", raising=False)

    cfg = _write_minimal_yaml(tmp_path)

    # Simulate "no git repo discoverable" — the dulwich-backed helpers
    # raise NotGitRepository when there is no .git directory along the
    # current path.
    with (
        patch(
            "llama_agents.core.git.git_util.Repo.discover",
            side_effect=NotGitRepository("no git here"),
        ),
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
    ):
        runner = CliRunner()
        res = runner.invoke(
            app, ["serve", str(cfg), "--no-install", "--no-reload", "--no-open-browser"]
        )
        assert res.exit_code == 0, res.output
