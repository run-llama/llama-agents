import os
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from llama_agents.cli.app import app


@pytest.fixture(autouse=True)
def _no_color(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setenv("NO_COLOR", "1")
    # Reset the global Rich console so it picks up NO_COLOR
    from rich import get_console

    monkeypatch.setattr(get_console(), "_color_system", None)
    monkeypatch.setattr(get_console(), "no_color", True)


def _write_yaml(tmpdir: Path) -> Path:
    cfg = tmpdir / "llama_deploy.yaml"
    cfg.write_text(
        (
            "name: test\n"
            "llama_cloud: true\n"
            "workflows:\n  default: tests.fake_module:fake_workflow\n"
        ),
        encoding="utf-8",
    )
    # minimal python project structure for appserver prepare
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


def test_connection_summary_uses_redaction(tmp_path: Path) -> None:
    cfg = _write_yaml(tmp_path)
    # Set env with spaces to verify cleaning and masking
    os.environ["LLAMA_CLOUD_API_KEY"] = "abc 123 456 789 000"
    os.environ["LLAMA_AGENTS_PROJECT_ID"] = "proj-1"
    os.environ["LLAMA_CLOUD_BASE_URL"] = "https://api.example.local"

    with (
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
        patch(
            "llama_agents.cli.commands.serve.is_interactive_session", return_value=False
        ),
    ):
        runner = CliRunner()
        res = runner.invoke(
            app,
            [
                "serve",
                str(cfg),
                "--no-install",
                "--no-reload",
                "--no-open-browser",
            ],
        )
        assert res.exit_code == 0, res.output
        # Expect first 6, mask, and last 4 of cleaned token
        assert "abc123****9000" in res.output
