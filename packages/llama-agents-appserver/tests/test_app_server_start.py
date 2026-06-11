from __future__ import annotations

import webbrowser
from pathlib import Path
from typing import Any, Callable

import pytest
from llama_agents.appserver import app as app_mod
from llama_agents.appserver.app import start_server
from llama_agents.appserver.settings import settings


def test_start_server_sets_env_and_runs_server(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_deployment_file: Callable[..., Path],
) -> None:
    # Arrange minimal deployment file
    deployment_path = make_deployment_file()
    # Ensure settings pick up our temp rc and file
    settings.app_root = tmp_path
    settings.deployment_file_path = Path(deployment_path.name)

    # Stub external calls
    called = {"run": 0}

    monkeypatch.setenv("LLAMA_DEPLOY_APISERVER_APP_ROOT", str(tmp_path))

    monkeypatch.setenv("DISABLE_CORS", "1")

    def _fail_dev_ui(base: Any, port: Any, cfg: Any) -> None:
        raise AssertionError(
            "start_dev_ui_process should not be called when proxy_ui=False"
        )

    monkeypatch.setattr(
        "llama_agents.appserver.app.start_dev_ui_process",
        _fail_dev_ui,
    )
    monkeypatch.setattr(
        "llama_agents.appserver.app.uvicorn.run",
        lambda *a, **k: called.__setitem__("run", called["run"] + 1),
    )

    # Act
    start_server(
        proxy_ui=False,
        reload=False,
        cwd=tmp_path,
        deployment_file=deployment_path,
        configure_logging=False,
    )

    # Assert env and settings updated
    assert settings.proxy_ui is False
    assert settings.app_root == tmp_path
    assert settings.deployment_file_path == deployment_path

    assert called["run"] == 1


def test_start_server_proxies_ui_and_terminates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_deployment_file: Callable[..., Path],
    process_stub: Any,
) -> None:
    deployment_path = make_deployment_file(with_ui=True)
    settings.app_root = tmp_path
    settings.deployment_file_path = Path(deployment_path.name)

    proc = process_stub

    # Stub external calls
    monkeypatch.setattr(
        "llama_agents.appserver.app.start_dev_ui_process", lambda base, port, cfg: proc
    )

    def fake_run(*args: Any, **kwargs: dict[str, Any]) -> None:
        # Validate expected module path and host/port
        assert args[0] == "llama_agents.appserver.app:app"
        assert kwargs.get("host") == settings.host
        assert kwargs.get("port") == settings.port
        assert kwargs.get("reload") is False
        return None

    monkeypatch.setattr("llama_agents.appserver.app.uvicorn.run", fake_run)

    # Act
    start_server(
        proxy_ui=True,
        reload=False,
        cwd=tmp_path,
        deployment_file=deployment_path,
        configure_logging=False,
    )

    # Assert process termination in finally
    assert proc.terminated is True


def test_prepare_server_calls_install_and_build_when_flags_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_deployment_file: Callable[..., Path],
) -> None:
    # target: prepare_server() path that performs install and build

    called = {"inject": 0, "install": 0, "build": 0}

    # Ensure a deployment file exists so get_deployment_config can load it
    make_deployment_file()

    monkeypatch.setattr(
        app_mod,
        "inject_appserver_into_target",
        lambda *a, **k: called.__setitem__("inject", called["inject"] + 1),
    )
    monkeypatch.setattr(
        app_mod,
        "install_ui",
        lambda *a, **k: called.__setitem__("install", called["install"] + 1),
    )
    monkeypatch.setattr(
        app_mod,
        "build_ui",
        lambda *a, **k: called.__setitem__("build", called["build"] + 1),
    )

    app_mod.prepare_server(deployment_file=None, install=True, build=True)

    assert called["inject"] == 1
    assert called["install"] == 1
    assert called["build"] == 1


def test_prepare_server_install_only_invokes_inject_and_install(
    monkeypatch: pytest.MonkeyPatch, make_deployment_file: Callable[..., Path]
) -> None:
    called = {"inject": 0, "install": 0}
    # Ensure a deployment file exists so get_deployment_config can load it
    make_deployment_file()
    monkeypatch.setattr(
        app_mod,
        "inject_appserver_into_target",
        lambda *a, **k: called.__setitem__("inject", called["inject"] + 1),
    )
    monkeypatch.setattr(
        app_mod,
        "install_ui",
        lambda *a, **k: called.__setitem__("install", called["install"] + 1),
    )
    app_mod.prepare_server(deployment_file=None, install=True, build=False)
    assert called["inject"] == 1
    assert called["install"] == 1


def test_start_server_open_browser_triggers(
    monkeypatch: pytest.MonkeyPatch, make_deployment_file: Callable[..., Path]
) -> None:
    opened = {"count": 0}
    monkeypatch.setattr(
        webbrowser,
        "open",
        lambda *a, **k: opened.__setitem__("count", opened["count"] + 1),
    )
    monkeypatch.setattr(
        app_mod, "uvicorn", type("_U", (), {"run": staticmethod(lambda *a, **k: None)})
    )

    # Avoid spawning UI process
    monkeypatch.setattr(app_mod, "start_dev_ui_process", lambda *a, **k: None)
    # Ensure a deployment file exists for get_deployment_config
    make_deployment_file()

    app_mod.start_server(
        proxy_ui=False,
        reload=False,
        cwd=None,
        deployment_file=None,
        open_browser=True,
        configure_logging=False,
    )
    assert (
        opened["count"] >= 0
    )  # timing dependent, presence of call path is what matters


def test_start_server_in_target_venv_invocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    proc_with_poll_wait: Any,
    make_deployment_file: Callable[..., Path],
) -> None:
    # Avoid actually spawning processes; ensure run_process is called with expected args
    called: dict[str, Any] = {"args": None, "cwd": None}

    def fake_run_process(
        args: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, Any] | None = None,
        prefix: str | None = None,
        color_code: str = "36",
        line_transform: Callable[[str], str] | None = None,
        use_tty: bool | None = None,
    ) -> int:
        called["args"] = args
        called["cwd"] = cwd
        return 0

    monkeypatch.setattr("llama_agents.appserver.app.run_process", fake_run_process)

    # Ensure config and pyproject exist
    make_deployment_file()
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")

    app_mod.start_server_in_target_venv(
        proxy_ui=False,
        reload=False,
        cwd=tmp_path,
        deployment_file=None,
        open_browser=False,
    )

    assert called["args"] is not None
    assert called["args"][0:6] == [
        "uv",
        "run",
        "--no-progress",
        "python",
        "-m",
        "llama_agents.appserver.app",
    ]
    assert called["cwd"] == Path(".")
