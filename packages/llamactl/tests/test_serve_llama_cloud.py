import os
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from llama_agents.cli.app import app
from llama_agents.cli.config.schema import Auth


def _write_yaml(tmpdir: Path, llama_cloud: bool) -> Path:
    cfg = tmpdir / "llama_deploy.yaml"
    cfg.write_text(
        (
            "name: test\n"
            f"llama_cloud: {'true' if llama_cloud else 'false'}\n"
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


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # ensure tests don't leak credentials
    monkeypatch.delenv("LLAMA_CLOUD_API_KEY", raising=False)
    monkeypatch.delenv("LLAMA_CLOUD_BASE_URL", raising=False)
    monkeypatch.delenv("LLAMA_CLOUD_USE_PROFILE", raising=False)
    monkeypatch.delenv("LLAMA_AGENTS_PROJECT_ID", raising=False)
    monkeypatch.delenv("LLAMA_DEPLOY_PROJECT_ID", raising=False)


def test_injects_api_key_from_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _write_yaml(tmp_path, llama_cloud=True)

    authed = Auth(
        id="123",
        name="test",
        api_url="https://api.cloud.llamaindex.ai",
        project_id="proj-1",
        api_key="ABC123",
    )

    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
    ):
        mock_service.current_auth_service().get_current_profile.return_value = authed

        runner = CliRunner()
        res = runner.invoke(
            app, ["serve", str(cfg), "--no-install", "--no-reload", "--no-open-browser"]
        )
        assert res.exit_code == 0, res.output
        assert os.environ.get("LLAMA_CLOUD_API_KEY") == "ABC123"
        assert os.environ.get("LLAMA_AGENTS_PROJECT_ID") == "proj-1"
        assert os.environ.get("LLAMA_DEPLOY_PROJECT_ID") == "proj-1"


def test_prompts_login_when_interactive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _write_yaml(tmp_path, llama_cloud=True)

    authed = Auth(
        id="123",
        name="test",
        api_url="https://api.cloud.llamaindex.ai",
        project_id="proj-1",
        api_key="ZZZ999",
    )

    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.serve.click.confirm") as mock_confirm,
        patch(
            "llama_agents.cli.commands.serve.validate_authenticated_profile"
        ) as mock_validate,
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
        patch(
            "llama_agents.cli.commands.serve.is_interactive_session", return_value=True
        ),
    ):
        mock_service.current_auth_service().get_current_profile.return_value = None
        mock_confirm.return_value = True
        mock_validate.return_value = authed

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
        assert os.environ.get("LLAMA_CLOUD_API_KEY") == "ZZZ999"
        assert os.environ.get("LLAMA_AGENTS_PROJECT_ID") == "proj-1"
        assert os.environ.get("LLAMA_DEPLOY_PROJECT_ID") == "proj-1"


def test_interactive_serve_uses_env_key_without_profile_choice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _write_yaml(tmp_path, llama_cloud=True)
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "ENV123")
    monkeypatch.setenv("LLAMA_AGENTS_PROJECT_ID", "env-project")

    profile = Auth(
        id="123",
        name="test",
        api_url="https://api.cloud.llamaindex.ai",
        project_id="profile-project",
        api_key="PROFILE123",
    )

    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.cli.commands.serve.select_or_exit") as mock_select,
        patch(
            "llama_agents.cli.commands.serve.validate_authenticated_profile"
        ) as mock_validate,
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
        patch(
            "llama_agents.cli.commands.serve.is_interactive_session", return_value=True
        ),
    ):
        mock_service.current_auth_service().get_current_profile.return_value = profile

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
    assert os.environ.get("LLAMA_CLOUD_API_KEY") == "ENV123"
    assert os.environ.get("LLAMA_AGENTS_PROJECT_ID") == "env-project"
    assert os.environ.get("LLAMA_DEPLOY_PROJECT_ID") == "env-project"
    mock_select.assert_not_called()
    mock_validate.assert_not_called()


def test_serve_use_profile_env_bypasses_env_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _write_yaml(tmp_path, llama_cloud=True)
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "ENV123")
    monkeypatch.setenv("LLAMA_AGENTS_PROJECT_ID", "env-project")
    monkeypatch.setenv("LLAMA_CLOUD_USE_PROFILE", "1")

    profile = Auth(
        id="123",
        name="test",
        api_url="https://api.cloud.llamaindex.ai",
        project_id="profile-project",
        api_key="PROFILE123",
    )

    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
    ):
        mock_service.current_auth_service().get_current_profile.return_value = profile

        runner = CliRunner()
        res = runner.invoke(
            app, ["serve", str(cfg), "--no-install", "--no-reload", "--no-open-browser"]
        )

    assert res.exit_code == 0, res.output
    assert os.environ.get("LLAMA_CLOUD_API_KEY") == "PROFILE123"
    assert os.environ.get("LLAMA_AGENTS_PROJECT_ID") == "profile-project"
    assert os.environ.get("LLAMA_DEPLOY_PROJECT_ID") == "profile-project"


def test_injects_project_id_from_env_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = tmp_path / "llama_deploy.yaml"
    cfg.write_text(
        (
            "name: test\n"
            "llama_cloud: true\n"
            "env:\n  LLAMA_AGENTS_PROJECT_ID: proj-from-config\n"
            "workflows:\n  default: tests.fake_module:fake_workflow\n"
        ),
        encoding="utf-8",
    )
    # minimal python project structure for appserver prepare
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname='x'\nversion='0.0.0'\n", encoding="utf-8"
    )
    (tmp_path / "tests").mkdir(exist_ok=True)
    (tmp_path / "tests" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "tests" / "fake_module.py").write_text(
        "from workflows import Workflow\n\nfake_workflow = Workflow()\n",
        encoding="utf-8",
    )

    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
        patch(
            "llama_agents.cli.commands.serve.is_interactive_session", return_value=False
        ),
    ):
        # No profile necessary for this path; simulate no logged-in profile
        mock_service.current_auth_service().get_current_profile.return_value = None

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
        assert os.environ.get("LLAMA_AGENTS_PROJECT_ID") == "proj-from-config"
        assert os.environ.get("LLAMA_DEPLOY_PROJECT_ID") == "proj-from-config"


def test_warns_non_interactive_without_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _write_yaml(tmp_path, llama_cloud=True)
    with (
        patch("llama_agents.cli.config.env_service.service") as mock_service,
        patch("llama_agents.appserver.app.prepare_server"),
        patch("llama_agents.appserver.app.start_server_in_target_venv"),
        patch(
            "llama_agents.cli.commands.serve.is_interactive_session", return_value=False
        ),
    ):
        mock_service.current_auth_service().get_current_profile.return_value = None

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
        # Not set
        assert os.environ.get("LLAMA_CLOUD_API_KEY") is None
        # Warning present
        assert (
            "Warning: LLAMA_CLOUD_API_KEY is not set" in res.output
            or "Warning: No Llama Cloud credentials" in res.output
        )
