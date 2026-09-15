from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from llama_agents.appserver.settings import ApiserverSettings
from llama_agents.appserver.workflow_loader import (
    _ensure_compatible_workflows,
    _ensure_uv_available,
    _get_appserver_workflows_requirement,
    _get_installed_version_within_target,
    _install_and_add_appserver_if_missing,
    _is_missing_or_outdated,
    install_ui,
    start_dev_ui_process,
)
from llama_agents.core.deployment_config import DeploymentConfig, UIConfig
from llama_agents.core.path_util import validate_path_traversal
from packaging.version import Version


@pytest.fixture
def resolve_venv_to_pkg(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Stub ``_resolve_project_venv`` to return ``<source_root>/<path>/.venv``,
    mirroring uv's choice for a non-workspace target. Tests that simulate a
    workspace layout override this by patching ``_resolve_project_venv`` directly.
    """
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._resolve_project_venv",
        lambda source_root, path: source_root / path / ".venv",
    )


def test_ensure_uv_available_success_and_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Case 1: uv exists -> no pip install
    calls = {"check": 0, "run": 0}

    def ok_check_call(*a: Any, **k: dict[str, Any]) -> int:
        calls["check"] += 1
        return 0

    monkeypatch.setattr("subprocess.check_call", ok_check_call)
    _ensure_uv_available()
    assert calls["check"] == 1

    def raise_missing(*a: Any, **k: dict[str, Any]) -> None:
        raise FileNotFoundError("no uv")

    monkeypatch.setattr("subprocess.check_call", raise_missing)
    ran = {"called": False}
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process",
        lambda *a, **k: ran.__setitem__("called", True),
    )
    _ensure_uv_available()
    assert ran["called"] is True

    # Case 3: pip install fails -> RuntimeError
    monkeypatch.setattr("subprocess.check_call", raise_missing)

    class FakeCalledProcessError(subprocess.CalledProcessError):
        def __init__(self) -> None:
            super().__init__(returncode=1, cmd=["pip"], stderr="bad")

    def raising_run(*a: Any, **k: dict[str, Any]) -> None:
        raise FakeCalledProcessError()

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process",
        raising_run,
    )
    with pytest.raises(RuntimeError) as e:
        _ensure_uv_available()
    assert "Unable to install uv" in str(e.value)


def test_add_appserver_pypi_install_calls_uv_with_prefix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolve_venv_to_pkg: None
) -> None:
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.are_we_editable_mode", lambda: False
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._is_missing_or_outdated",
        lambda p: Version("1.2.3"),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        lambda *a, **k: None,
    )

    _install_and_add_appserver_if_missing(Path("pkg"), tmp_path)

    assert len(cmds) >= 2
    # Last call is the uv pip install
    install_cmd = cmds[-1]
    assert install_cmd[:3] == ["uv", "pip", "install"]
    assert any(arg == "llama-agents-appserver==1.2.3" for arg in install_cmd)
    assert "--prefix" in install_cmd
    assert install_cmd[install_cmd.index("--prefix") + 1] == str(pkg_dir / ".venv")


def test_add_appserver_install_targets_resolved_venv_when_outside_pkg(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    Install must target whichever venv ``_resolve_project_venv`` returns, even
    when that path is outside ``<pkg>/.venv`` (e.g. a uv workspace member whose
    venv lives at the workspace root). Regression guard for the install/runtime
    venv-path disagreement that broke ``llamactl dev validate`` in workspace
    layouts.
    """
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")
    # Simulate uv picking a venv outside the target dir (what happens when the
    # target is a workspace member).
    resolved_venv = tmp_path / "elsewhere" / ".venv"

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.are_we_editable_mode", lambda: False
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._is_missing_or_outdated",
        lambda p: Version("1.2.3"),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._resolve_project_venv",
        lambda source_root, path: resolved_venv,
    )

    _install_and_add_appserver_if_missing(Path("pkg"), tmp_path)

    install_cmd = cmds[-1]
    assert install_cmd[:3] == ["uv", "pip", "install"]
    assert "--prefix" in install_cmd
    assert install_cmd[install_cmd.index("--prefix") + 1] == str(resolved_venv)
    assert str(pkg_dir / ".venv") not in install_cmd


def test_add_appserver_sdists_install(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolve_venv_to_pkg: None
) -> None:
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        lambda *a, **k: None,
    )

    s1 = tmp_path / "d1" / "a-0.1.0.tar.gz"
    s2 = tmp_path / "d2" / "b-0.2.0.tar.gz"
    s1.parent.mkdir(parents=True, exist_ok=True)
    s2.parent.mkdir(parents=True, exist_ok=True)
    s1.write_text("x")
    s2.write_text("y")

    _install_and_add_appserver_if_missing(Path("pkg"), tmp_path, sdists=[s1, s2])

    assert len(cmds) >= 2
    install_cmd = cmds[-1]
    assert install_cmd[:3] == ["uv", "pip", "install"]
    assert str(s1.resolve()) in install_cmd and str(s2.resolve()) in install_cmd
    assert "--prefix" in install_cmd


def test_add_appserver_editable_install(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolve_venv_to_pkg: None
) -> None:
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.are_we_editable_mode", lambda: True
    )
    # set dev pyproject in sibling directory
    dev_dir = tmp_path / "appserver_src"
    dev_dir.mkdir()
    (dev_dir / "pyproject.toml").write_text("[project]\nname='app'\n")
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._find_development_pyproject",
        lambda: dev_dir / "pyproject.toml",
    )
    venv_path = pkg_dir / ".venv"
    venv_path.mkdir()
    (venv_path / "pyvenv.cfg").write_text(
        f"version_info={sys.version_info.major}.{sys.version_info.minor}\n"
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        lambda *a, **k: None,
    )

    _install_and_add_appserver_if_missing(Path("pkg"), tmp_path)

    assert len(cmds) >= 2
    install_cmd = cmds[-1]
    assert install_cmd[:5] == [
        "uv",
        "pip",
        "install",
        "--reinstall-package",
        "llama-agents-appserver",
    ]
    # file:// url should be present
    assert any(str(arg).startswith("file://") for arg in install_cmd)
    assert "--prefix" in install_cmd


def test_install_ui_runs_pnpm_and_validates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ui_root = tmp_path / "ui"
    ui_root.mkdir(parents=True, exist_ok=True)
    cfg = DeploymentConfig(
        name="n",
        ui=UIConfig(directory="ui", proxy_port=3001),
    )
    ran: dict[str, Path | None] = {"cwd": None}

    def run_capture(cmd: list[str], cwd: Path | None = None, **kwargs: Any) -> None:
        ran["cwd"] = cwd
        assert cmd[:2] == ["npm", "install"]
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process",
        run_capture,
    )
    install_ui(cfg, tmp_path)
    assert ran["cwd"] == ui_root


def test_start_dev_ui_process_port_open_and_spawn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = DeploymentConfig(name="n", ui=UIConfig(directory="ui"))
    # Configure API and UI proxy ports via env-backed settings
    monkeypatch.setenv("LLAMA_DEPLOY_APISERVER_PORT", "4501")
    monkeypatch.setenv("LLAMA_DEPLOY_APISERVER_PROXY_UI_PORT", "3001")
    api_settings = ApiserverSettings()

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.socket.socket.connect_ex",
        lambda *a, **k: 0,
    )
    assert start_dev_ui_process(tmp_path, api_settings, cfg) is None

    # Case: port not open -> spawn process and return immediately
    class FakeProc:
        def __init__(self) -> None:
            self.terminated = False

        def terminate(self) -> None:
            self.terminated = True

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.socket.socket.connect_ex",
        lambda *a, **k: 1,
    )
    fake = FakeProc()

    def make_proc(*a: Any, **k: dict[str, Any]) -> FakeProc:
        # env should include base path and port
        env = k.get("env", {})
        assert env.get("LLAMA_DEPLOY_DEPLOYMENT_BASE_PATH") == "/deployments/n/ui"
        assert env.get("LLAMA_DEPLOY_DEPLOYMENT_NAME") == "n"
        assert env.get("PORT") == "3001"
        return fake

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.spawn_process",
        make_proc,
    )
    p = start_dev_ui_process(tmp_path, api_settings, cfg)
    assert p is fake


def test_validate_path_is_safe_rejects_escape(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError):
        validate_path_traversal(Path("../bad"), tmp_path)


def test_get_installed_version_within_target_cases(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # success
    monkeypatch.setattr(
        "subprocess.check_output",
        lambda *a, **k: b"1.2.3\n",
    )
    v = _get_installed_version_within_target(tmp_path)
    assert str(v) == "1.2.3"

    # invalid version
    monkeypatch.setattr(
        "subprocess.check_output",
        lambda *a, **k: b"not-a-version\n",
    )
    assert _get_installed_version_within_target(tmp_path) is None

    # missing
    def raise_cpe(*a: Any, **k: dict[str, Any]) -> None:
        raise subprocess.CalledProcessError(1, cmd=["uv"])  # noqa: F841

    monkeypatch.setattr("subprocess.check_output", raise_cpe)
    assert _get_installed_version_within_target(tmp_path) is None


def test_current_and_outdated_logic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_current_version",
        lambda: Version("2.0.0"),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_installed_version_within_target",
        lambda p: None,
    )
    assert _is_missing_or_outdated(tmp_path) == Version("2.0.0")

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_installed_version_within_target",
        lambda p: Version("1.0.0"),
    )
    assert _is_missing_or_outdated(tmp_path) == Version("2.0.0")

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_installed_version_within_target",
        lambda p: Version("2.0.0"),
    )
    assert _is_missing_or_outdated(tmp_path) is None


def test_add_appserver_target_version_installs_from_pypi(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolve_venv_to_pkg: None
) -> None:
    """When target_version is set, install that exact version from PyPI."""
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.are_we_editable_mode", lambda: False
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        lambda *a, **k: None,
    )

    _install_and_add_appserver_if_missing(
        Path("pkg"), tmp_path, target_version="0.4.15"
    )

    assert len(cmds) >= 2
    install_cmd = cmds[-1]
    assert install_cmd[:3] == ["uv", "pip", "install"]
    # 0.4.15 <= 0.5.3 so it uses the old dist name
    assert any(arg == "llama-deploy-appserver==0.4.15" for arg in install_cmd)
    assert "--prefix" in install_cmd


def test_add_appserver_target_version_ignored_in_editable_mode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolve_venv_to_pkg: None
) -> None:
    """In editable mode, target_version is ignored — editable installs use local source."""
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.are_we_editable_mode", lambda: True
    )
    dev_dir = tmp_path / "appserver_src"
    dev_dir.mkdir()
    (dev_dir / "pyproject.toml").write_text("[project]\nname='app'\n")
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._find_development_pyproject",
        lambda: dev_dir / "pyproject.toml",
    )
    venv_path = pkg_dir / ".venv"
    venv_path.mkdir()
    (venv_path / "pyvenv.cfg").write_text(
        f"version_info={sys.version_info.major}.{sys.version_info.minor}\n"
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        lambda *a, **k: None,
    )

    _install_and_add_appserver_if_missing(
        Path("pkg"), tmp_path, target_version="0.4.15"
    )

    assert len(cmds) >= 2
    install_cmd = cmds[-1]
    assert install_cmd[:5] == [
        "uv",
        "pip",
        "install",
        "--reinstall-package",
        "llama-agents-appserver",
    ]
    assert not any("llama-agents-appserver==0.4.15" in str(arg) for arg in install_cmd)


def test_add_appserver_target_version_ignored_when_sdists_provided(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolve_venv_to_pkg: None
) -> None:
    """When sdists are provided, they take priority over target_version."""
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)
        return None

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        lambda *a, **k: None,
    )

    s1 = tmp_path / "d1" / "a-0.1.0.tar.gz"
    s2 = tmp_path / "d2" / "b-0.2.0.tar.gz"
    s1.parent.mkdir(parents=True, exist_ok=True)
    s2.parent.mkdir(parents=True, exist_ok=True)
    s1.write_text("x")
    s2.write_text("y")

    _install_and_add_appserver_if_missing(
        Path("pkg"), tmp_path, sdists=[s1, s2], target_version="0.4.15"
    )

    assert len(cmds) >= 2
    install_cmd = cmds[-1]
    assert install_cmd[:3] == ["uv", "pip", "install"]
    assert str(s1.resolve()) in install_cmd and str(s2.resolve()) in install_cmd
    assert "llama-agents-appserver==0.4.15" not in install_cmd


def test_get_workflows_version_in_target_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "subprocess.check_output",
        lambda *a, **k: b"2.14.0\n",
    )
    v = _get_installed_version_within_target(tmp_path, package="llama-index-workflows")
    assert v == Version("2.14.0")


def test_get_workflows_version_in_target_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "subprocess.check_output",
        lambda *a, **k: b"\n",
    )
    assert (
        _get_installed_version_within_target(tmp_path, package="llama-index-workflows")
        is None
    )


def test_get_workflows_version_in_target_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def raise_cpe(*a: Any, **k: dict[str, Any]) -> None:
        raise subprocess.CalledProcessError(1, cmd=["uv"])

    monkeypatch.setattr("subprocess.check_output", raise_cpe)
    assert (
        _get_installed_version_within_target(tmp_path, package="llama-index-workflows")
        is None
    )


def test_get_appserver_workflows_requirement() -> None:
    _get_appserver_workflows_requirement.cache_clear()
    req = _get_appserver_workflows_requirement()
    assert req is not None
    assert Version("2.21.0") in req
    assert Version("2.14.0") not in req
    assert Version("3.0.0") not in req


def test_get_appserver_workflows_requirement_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _get_appserver_workflows_requirement.cache_clear()
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.pkg_requires",
        lambda _: ["some-other-package>=1.0"],
    )
    assert _get_appserver_workflows_requirement() is None


def test_ensure_compatible_workflows_compatible_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Compatible version -> no uv add call."""
    from packaging.specifiers import SpecifierSet

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_appserver_workflows_requirement",
        lambda: SpecifierSet(">=2.16.0,<3.0.0"),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_installed_version_within_target",
        lambda p, **kw: Version("2.16.1"),
    )

    uv_calls: list[list[str]] = []

    def track_run_uv(
        source_root: Path, path: Path, cmd: str, args: list[str] = [], **kwargs: Any
    ) -> None:
        uv_calls.append([cmd] + args)

    monkeypatch.setattr("llama_agents.appserver.workflow_loader.run_uv", track_run_uv)

    _ensure_compatible_workflows(tmp_path, Path("."))
    assert len(uv_calls) == 0


def test_ensure_compatible_workflows_incompatible_auto_updates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Incompatible version -> calls uv add to update."""
    from packaging.specifiers import SpecifierSet

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_appserver_workflows_requirement",
        lambda: SpecifierSet(">=2.16.0,<3.0.0"),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_installed_version_within_target",
        lambda p, **kw: Version("2.14.0"),
    )

    uv_calls: list[tuple[str, list[str]]] = []

    def track_run_uv(
        source_root: Path,
        path: Path,
        cmd: str,
        args: list[str] = [],
        extra_env: dict[str, str] | None = None,
    ) -> None:
        uv_calls.append((cmd, args))

    monkeypatch.setattr("llama_agents.appserver.workflow_loader.run_uv", track_run_uv)

    _ensure_compatible_workflows(tmp_path, Path("."))
    assert len(uv_calls) == 1
    cmd, args = uv_calls[0]
    assert cmd == "add"
    assert any("llama-index-workflows" in a for a in args)


def test_ensure_compatible_workflows_not_installed_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Not installed -> no action (appserver install will bring it in)."""
    from packaging.specifiers import SpecifierSet

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_appserver_workflows_requirement",
        lambda: SpecifierSet(">=2.16.0,<3.0.0"),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_installed_version_within_target",
        lambda p, **kw: None,
    )

    uv_calls: list[Any] = []
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_uv",
        lambda *a, **k: uv_calls.append(1),
    )

    _ensure_compatible_workflows(tmp_path, Path("."))
    assert len(uv_calls) == 0


def test_ensure_compatible_workflows_update_fails_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If uv add fails, raises RuntimeError with helpful message."""
    from packaging.specifiers import SpecifierSet

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_appserver_workflows_requirement",
        lambda: SpecifierSet(">=2.16.0,<3.0.0"),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._get_installed_version_within_target",
        lambda p, **kw: Version("2.14.0"),
    )

    def fail_run_uv(*a: Any, **k: Any) -> None:
        raise subprocess.CalledProcessError(1, cmd=["uv", "add"])

    monkeypatch.setattr("llama_agents.appserver.workflow_loader.run_uv", fail_run_uv)

    with pytest.raises(RuntimeError, match="conflicting constraints"):
        _ensure_compatible_workflows(tmp_path, Path("."))


def test_install_calls_ensure_compatible_workflows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolve_venv_to_pkg: None
) -> None:
    """Integration: _install_and_add_appserver_if_missing calls _ensure_compatible_workflows."""
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname='x'\n")

    cmds: list[list[str]] = []

    def run_capture(cmd: list[str], **kwargs: Any) -> None:
        cmds.append(cmd)

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.run_process", run_capture
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader.are_we_editable_mode", lambda: False
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._is_missing_or_outdated",
        lambda p: Version("1.2.3"),
    )

    compat_called = {"called": False}

    def mock_ensure_compat(source_root: Path, path: Path) -> None:
        compat_called["called"] = True

    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_compatible_workflows",
        mock_ensure_compat,
    )

    _install_and_add_appserver_if_missing(Path("pkg"), tmp_path)
    assert compat_called["called"]
