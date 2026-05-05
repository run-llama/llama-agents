from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from llama_agents.cli.app import app


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_dev_serve_aliases_serve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname='x'\nversion='0.0.0'\n", encoding="utf-8"
    )

    called = SimpleNamespace(prepare=False, start=False)

    def _mark_prepare(*_: object, **__: object) -> None:
        called.prepare = True

    def _mark_start(*_: object, **__: object) -> None:
        called.start = True

    monkeypatch.setattr(
        "llama_agents.cli.commands.serve._maybe_inject_llama_cloud_credentials",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr("llama_agents.appserver.app.prepare_server", _mark_prepare)
    monkeypatch.setattr(
        "llama_agents.appserver.app.start_server_in_target_venv", _mark_start
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.serve._print_connection_summary", lambda: None
    )

    result = runner.invoke(
        app,
        [
            "dev",
            "serve",
            str(tmp_path),
            "--no-install",
            "--no-reload",
            "--no-open-browser",
        ],
    )
    assert result.exit_code == 0, result.output
    assert called.prepare is True
    assert called.start is True


def test_dev_validate_runs_inside_project_venv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner
) -> None:
    called = SimpleNamespace(prepared=False, preflight=False)

    # Ensure layout passes
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._ensure_project_layout",
        lambda deployment_file, command_name: Path(deployment_file),
    )

    # Stub creds injection
    monkeypatch.setattr(
        "llama_agents.cli.commands.serve._maybe_inject_llama_cloud_credentials",
        lambda *args, **kwargs: None,
    )

    # Mark prepare and preflight calls
    def _mark_prepare(*_: object, **__: object) -> None:
        called.prepared = True

    def _mark_preflight(*_: object, **__: object) -> None:
        called.preflight = True

    monkeypatch.setattr("llama_agents.cli.commands.dev.prepare_server", _mark_prepare)
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.start_preflight_in_target_venv", _mark_preflight
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary", lambda: None
    )

    result = runner.invoke(app, ["dev", "validate", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert called.prepared is True
    assert called.preflight is True


def test_dev_validate_reports_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner
) -> None:
    # Skip real layout checks and creds injection
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._ensure_project_layout",
        lambda deployment_file, command_name: Path(deployment_file),
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.serve._maybe_inject_llama_cloud_credentials",
        lambda *args, **kwargs: None,
    )
    # Prepare server is a no-op
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.prepare_server", lambda *a, **k: None
    )

    # Simulate preflight failure propagated by the helper (CLI catches and prints friendly msg)
    import subprocess as _sp

    def _raise_called_process_error(*_: object, **__: object) -> None:
        raise _sp.CalledProcessError(1, ["uv", "run"])  # pragma: no cover - simple stub

    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.start_preflight_in_target_venv",
        _raise_called_process_error,
    )

    result = runner.invoke(app, ["dev", "validate", str(tmp_path)])
    assert result.exit_code != 0
    assert "Workflow validation failed" in result.output


@dataclass
class _Captured:
    cmd: tuple[str, ...] | None = None
    env: dict[str, str] | None = None


def test_dev_run_sets_env_and_invokes_subprocess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner
) -> None:
    captured = _Captured()

    def _fake_run(
        cmd: tuple[str, ...] | list[str],
        *,
        env: dict[str, str] | None = None,
        check: bool = False,
    ) -> SimpleNamespace:
        captured.cmd = tuple(cmd)
        captured.env = (env or {}).copy()
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._ensure_project_layout",
        lambda deployment_file, command_name: Path(deployment_file),
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._prepare_environment",
        lambda deployment_file, require_cloud: (
            SimpleNamespace(),
            Path("."),
        ),
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.parse_environment_variables",
        lambda config, parent: {
            "LOCAL_ONLY": "value-from-env-file",
            "LLAMA_DEPLOY_PROJECT_ID": "proj-from-config",
        },
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary", lambda: None
    )
    monkeypatch.setattr("llama_agents.cli.commands.dev.subprocess.run", _fake_run)

    result = runner.invoke(
        app, ["dev", "run", "--deployment-file", str(tmp_path), "--", "echo", "hello"]
    )
    assert result.exit_code == 0, result.output
    assert captured.cmd == ("echo", "hello")
    assert captured.env is not None
    assert captured.env["LOCAL_ONLY"] == "value-from-env-file"
    assert captured.env["LLAMA_DEPLOY_PROJECT_ID"] == "proj-from-config"


def test_dev_run_enables_auth_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner
) -> None:
    captured = SimpleNamespace(require_cloud=None)

    def _capture_prepare(
        deployment_file: Path, require_cloud: bool
    ) -> tuple[SimpleNamespace, Path]:
        captured.require_cloud = require_cloud
        return (SimpleNamespace(), Path("."))

    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._prepare_environment", _capture_prepare
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.parse_environment_variables",
        lambda config, parent: {},
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary", lambda: None
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0),
    )

    result = runner.invoke(
        app, ["dev", "run", "--deployment-file", str(tmp_path), "--", "true"]
    )
    assert result.exit_code == 0, result.output
    assert captured.require_cloud is True


def test_dev_run_disable_auth_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner
) -> None:
    captured = SimpleNamespace(require_cloud=None)

    def _capture_prepare(
        deployment_file: Path, require_cloud: bool
    ) -> tuple[SimpleNamespace, Path]:
        captured.require_cloud = require_cloud
        return (SimpleNamespace(), Path("."))

    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._prepare_environment", _capture_prepare
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.parse_environment_variables",
        lambda config, parent: {},
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary", lambda: None
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0),
    )

    result = runner.invoke(
        app,
        [
            "dev",
            "run",
            "--deployment-file",
            str(tmp_path),
            "--no-auth",
            "--",
            "true",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured.require_cloud is False


def test_dev_run_does_not_require_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner
) -> None:
    # No pyproject written to tmp_path on purpose
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._prepare_environment",
        lambda deployment_file, require_cloud: (
            SimpleNamespace(),
            Path("."),
        ),
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.parse_environment_variables",
        lambda config, parent: {},
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary", lambda: None
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0),
    )

    result = runner.invoke(
        app, ["dev", "run", "--deployment-file", str(tmp_path), "--", "true"]
    )
    assert result.exit_code == 0, result.output


def test_export_json_graph_defaults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    monkeypatch.chdir(tmp_path)
    deployment_file = tmp_path / "llama_deploy.yaml"
    deployment_file.write_text("name='x'\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname='x'\nversion='0.0.0'\n", encoding="utf-8"
    )

    called = SimpleNamespace(cwd=None, deployment_file=None, output=None)

    def _fake_start_export(*, cwd: Path, deployment_file: Path, output: Path) -> None:
        called.cwd = cwd
        called.deployment_file = deployment_file
        called.output = output
        output.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._ensure_project_layout",
        lambda deployment_file, command_name: tmp_path,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.serve._maybe_inject_llama_cloud_credentials",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.prepare_server",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.start_export_json_graph_in_target_venv",
        _fake_start_export,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary",
        lambda: None,
    )

    result = runner.invoke(app, ["dev", "export-json-graph"])
    assert result.exit_code == 0, result.output
    assert called.cwd == tmp_path
    # Default deployment_file argument points at the project root (".")
    assert called.deployment_file == tmp_path
    assert called.output == tmp_path / "workflows.json"
    assert (tmp_path / "workflows.json").exists()


def test_export_json_graph_with_output_arg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    monkeypatch.chdir(tmp_path)
    deployment_file = tmp_path / "llama_deploy.yaml"
    deployment_file.write_text("name='x'\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname='x'\nversion='0.0.0'\n", encoding="utf-8"
    )

    called = SimpleNamespace(cwd=None, deployment_file=None, output=None)

    def _fake_start_export(*, cwd: Path, deployment_file: Path, output: Path) -> None:
        called.cwd = cwd
        called.deployment_file = deployment_file
        called.output = output
        output.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._ensure_project_layout",
        lambda deployment_file, command_name: tmp_path,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.serve._maybe_inject_llama_cloud_credentials",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.prepare_server",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.start_export_json_graph_in_target_venv",
        _fake_start_export,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary",
        lambda: None,
    )

    result = runner.invoke(app, ["dev", "export-json-graph", "--output", "test.json"])
    assert result.exit_code == 0, result.output
    assert called.cwd == tmp_path
    assert called.deployment_file == tmp_path
    assert called.output == tmp_path / "test.json"
    assert (tmp_path / "test.json").exists()


def test_export_json_graph_reports_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    monkeypatch.chdir(tmp_path)
    deployment_file = tmp_path / "llama_deploy.yaml"
    deployment_file.write_text("name='x'\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname='x'\nversion='0.0.0'\n", encoding="utf-8"
    )

    import subprocess as _sp

    def _raise_called_process_error(
        *, cwd: Path, deployment_file: Path, output: Path
    ) -> None:
        raise _sp.CalledProcessError(1, ["uv", "run"])

    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._ensure_project_layout",
        lambda deployment_file, command_name: tmp_path,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.serve._maybe_inject_llama_cloud_credentials",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.prepare_server",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev.start_export_json_graph_in_target_venv",
        _raise_called_process_error,
    )
    monkeypatch.setattr(
        "llama_agents.cli.commands.dev._print_connection_summary",
        lambda: None,
    )

    result = runner.invoke(app, ["dev", "export-json-graph"])
    assert result.exit_code != 0, result.output
    assert "Workflow JSON graph export failed" in result.output


def test_export_json_graph_deployment_file_not_exist(
    tmp_path: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["dev", "export-json-graph", "hello.toml"])
    assert result.exit_code != 0, result.output
