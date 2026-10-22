# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from pathlib import Path
from unittest import mock

import httpx
import pytest
from llama_agents.appserver import bootstrap
from llama_agents.appserver.bootstrap import (
    _download_and_extract_artifact,
    _find_artifact,
    _upload_artifact,
    bootstrap_app_from_repo,
    run_build,
)


def _set_bootstrap_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_url: str,
    auth_token: str | None = None,
    git_ref: str | None = None,
    git_sha: str | None = None,
    deployment_file_path: str = "llama_deploy.yaml",
    bootstrap_sdists: str | None = None,
) -> None:
    monkeypatch.setenv("LLAMA_DEPLOY_REPO_URL", repo_url)
    if auth_token is not None:
        monkeypatch.setenv("LLAMA_DEPLOY_AUTH_TOKEN", auth_token)
    if git_ref is not None:
        monkeypatch.setenv("LLAMA_DEPLOY_GIT_REF", git_ref)
    if git_sha is not None:
        monkeypatch.setenv("LLAMA_DEPLOY_GIT_SHA", git_sha)
    if deployment_file_path is not None:
        monkeypatch.setenv("LLAMA_DEPLOY_DEPLOYMENT_FILE_PATH", deployment_file_path)
    if bootstrap_sdists is not None:
        monkeypatch.setenv("LLAMA_DEPLOY_BOOTSTRAP_SDISTS", bootstrap_sdists)


def _stub_bootstrap_pipeline(monkeypatch: pytest.MonkeyPatch) -> dict[str, mock.Mock]:
    """
    Patch side-effectful functions used by bootstrap to no-ops so tests can
    assert call wiring without performing real work. Returns a dict of mocks.
    """
    mocks: dict[str, mock.Mock] = {}
    for qualname in [
        "llama_agents.appserver.bootstrap.load_environment_variables",
        "llama_agents.appserver.bootstrap.inject_appserver_into_target",
        "llama_agents.appserver.bootstrap.install_ui",
        "llama_agents.appserver.bootstrap.build_ui",
        "llama_agents.appserver.bootstrap.configure_settings",
    ]:
        m = mock.Mock()
        monkeypatch.setattr(qualname, m)
        mocks[qualname] = m
    return mocks


def test_bootstrap_minimal_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """End-to-end happy path with minimal config and stubs to verify key calls and wiring."""
    _write_minimal_deployment_config(tmp_path)
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
        # No git_ref/sha on purpose to validate None behavior
    )

    pipeline_mocks = _stub_bootstrap_pipeline(monkeypatch)

    with mock.patch("llama_agents.appserver.bootstrap.clone_repo") as clone:
        # Execute
        bootstrap_app_from_repo(target_dir=str(tmp_path))

        # clone_repo was invoked with expected args
        clone.assert_called_once_with(
            repository_url="https://example.com/repo.git",
            git_ref=None,
            git_sha=None,
            basic_auth="tok",
            dest_dir=str(tmp_path),
            depth=None,
        )

    # configure_settings received proper app_root and deployment_file_path
    cfg_calls = pipeline_mocks[
        "llama_agents.appserver.bootstrap.configure_settings"
    ].call_args_list
    assert cfg_calls, "configure_settings should be called"
    _, cfg_kwargs = cfg_calls[0]
    assert cfg_kwargs.get("app_root") == Path(tmp_path)
    assert cfg_kwargs.get("deployment_file_path") == Path("llama_deploy.yaml")

    # build_ui now receives settings as third arg via bootstrap module
    args, kwargs = pipeline_mocks["llama_agents.appserver.bootstrap.build_ui"].call_args
    assert args[0] == Path(tmp_path)


def test_bootstrap_invokes_clone_repo_with_explicit_git_sha_and_git_ref(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    If both `LLAMA_DEPLOY_GIT_SHA` and `LLAMA_DEPLOY_GIT_REF` are set,
    bootstrap should pass them separately and avoid the shallow-clone path.
    """
    _write_minimal_deployment_config(tmp_path)
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
        git_ref="feature/branch",
        git_sha="deadbeef",
    )

    with mock.patch("llama_agents.appserver.bootstrap.clone_repo") as clone:
        bootstrap_app_from_repo(target_dir=str(tmp_path))

        clone.assert_called_once_with(
            repository_url="https://example.com/repo.git",
            git_ref="feature/branch",
            git_sha="deadbeef",
            basic_auth="tok",
            dest_dir=str(tmp_path),
            depth=None,
        )


def test_bootstrap_invokes_clone_repo_with_git_ref_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If only `LLAMA_DEPLOY_GIT_REF` is set, bootstrap should use the ref path."""
    _write_minimal_deployment_config(tmp_path)
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
        git_ref="feature/branch",
    )

    with mock.patch("llama_agents.appserver.bootstrap.clone_repo") as clone:
        bootstrap_app_from_repo(target_dir=str(tmp_path))

        clone.assert_called_once_with(
            repository_url="https://example.com/repo.git",
            git_ref="feature/branch",
            git_sha=None,
            basic_auth="tok",
            dest_dir=str(tmp_path),
            depth=1,
        )


def test_bootstrap_raises_when_repo_url_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure a ValueError is raised when `repo_url` is not provided via settings/env."""
    # Ensure repo_url is absent
    monkeypatch.delenv("LLAMA_DEPLOY_REPO_URL", raising=False)
    with pytest.raises(ValueError):
        bootstrap_app_from_repo(target_dir="/tmp/irrelevant")


def test_bootstrap_invokes_clone_repo_with_auth_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify `basic_auth` is passed to `clone_repo` when `auth_token` is set."""
    _write_minimal_deployment_config(tmp_path)
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="secret-token",
    )
    _stub_bootstrap_pipeline(monkeypatch)
    with mock.patch("llama_agents.appserver.bootstrap.clone_repo") as clone:
        bootstrap_app_from_repo(target_dir=str(tmp_path))
        clone.assert_called_once()
        kwargs = clone.call_args.kwargs
        assert kwargs["basic_auth"] == "secret-token"


def test_bootstrap_configure_settings_called_with_app_root_and_deployment_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Assert `configure_settings(app_root, deployment_file_path)` is called with resolved paths from `target_dir` and settings."""
    _write_minimal_deployment_config(tmp_path)
    # Use a non-default deployment file path to ensure it is passed through
    custom_path = "configs/deploy.yaml"
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        deployment_file_path=custom_path,
    )

    mocks = _stub_bootstrap_pipeline(monkeypatch)

    with mock.patch("llama_agents.appserver.bootstrap.clone_repo"):
        bootstrap_app_from_repo(target_dir=str(tmp_path))

    cfg = mocks["llama_agents.appserver.bootstrap.configure_settings"]
    assert cfg.called, "configure_settings should be called"
    _, kwargs = cfg.call_args
    assert kwargs["app_root"] == Path(tmp_path)
    assert kwargs["deployment_file_path"] == Path(custom_path)


def test_bootstrap_sdists_passed_when_tarballs_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Create a directory with mixed files; only .tar.gz files should be collected and passed to `inject_appserver_into_target`."""
    _write_minimal_deployment_config(tmp_path)
    sd_dir = tmp_path / "sdists"
    sd_dir.mkdir()
    good1 = sd_dir / "a-0.1.0.tar.gz"
    good2 = sd_dir / "b-0.2.0.tar.gz"
    bad1 = sd_dir / "c.txt"
    good1.write_text("x")
    good2.write_text("x")
    bad1.write_text("x")
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
        bootstrap_sdists=str(sd_dir),
    )

    mocks = _stub_bootstrap_pipeline(monkeypatch)
    with mock.patch("llama_agents.appserver.bootstrap.clone_repo"):
        bootstrap_app_from_repo(target_dir=str(tmp_path))

    inject_mock = mocks["llama_agents.appserver.bootstrap.inject_appserver_into_target"]
    assert inject_mock.called
    # Extract sdists arg (third positional or named)
    _, args, kwargs = inject_mock.mock_calls[0]
    # monkeypatch.Mock's call objects: (name, args, kwargs) via .mock_calls entries
    # But here we used default Mock(), so access via .call_args as safer:
    args, kwargs = inject_mock.call_args
    sdists = args[2] if len(args) >= 3 else kwargs.get("sdists")
    assert sdists is not None and len(sdists) == 2
    assert {p.name for p in sdists} == {"a-0.1.0.tar.gz", "b-0.2.0.tar.gz"}


def test_bootstrap_sdists_none_when_empty_or_no_tarballs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the sdists directory is empty or lacks .tar.gz, `inject_appserver_into_target` should receive `sdists=None`."""
    _write_minimal_deployment_config(tmp_path)
    sd_dir = tmp_path / "sdists"
    sd_dir.mkdir()
    # Create non-tarball files only
    (sd_dir / "note.txt").write_text("x")
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        bootstrap_sdists=str(sd_dir),
    )
    mocks = _stub_bootstrap_pipeline(monkeypatch)
    with mock.patch("llama_agents.appserver.bootstrap.clone_repo"):
        bootstrap_app_from_repo(target_dir=str(tmp_path))
    inject_mock = mocks["llama_agents.appserver.bootstrap.inject_appserver_into_target"]
    args, kwargs = inject_mock.call_args
    sdists = args[2] if len(args) >= 3 else kwargs.get("sdists")
    assert sdists is None


def test_bootstrap_propagates_errors_from_clone(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If `clone_repo` raises, the exception should bubble up (no swallowing)."""
    _write_minimal_deployment_config(tmp_path)
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
    )
    # Do not stub pipeline; we expect to raise before any other calls
    with mock.patch(
        "llama_agents.appserver.bootstrap.clone_repo",
        side_effect=RuntimeError("boom"),
    ):
        with pytest.raises(RuntimeError):
            bootstrap_app_from_repo(target_dir=str(tmp_path))


def _write_minimal_deployment_config(tmp_path: Path) -> None:
    (tmp_path / "llama_deploy.yaml").write_text("name: test\nservices: {}\n")


# ---------------------------------------------------------------------------
# Phase 3: Build artifact 503 handling tests
# ---------------------------------------------------------------------------


def test_find_artifact_raises_on_503() -> None:
    resp = mock.Mock(status_code=503)
    with mock.patch("llama_agents.appserver.bootstrap.httpx.head", return_value=resp):
        with pytest.raises(RuntimeError, match="Build artifact storage not configured"):
            _find_artifact("host:8000", "dep1", "build1", "tok")


def test_find_artifact_returns_none_on_404() -> None:
    resp = mock.Mock(status_code=404)
    with mock.patch("llama_agents.appserver.bootstrap.httpx.head", return_value=resp):
        assert _find_artifact("host:8000", "dep1", "build1", "tok") is None


def test_find_artifact_returns_headers_on_200() -> None:
    headers = httpx.Headers({"X-Static-Assets-Path": '"ui/dist"'})
    resp = mock.Mock(status_code=200, headers=headers)
    with mock.patch("llama_agents.appserver.bootstrap.httpx.head", return_value=resp):
        assert _find_artifact("host:8000", "dep1", "build1", "tok") is headers


@pytest.fixture
def build_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, mock.Mock]:
    """Run run_build against tmp_path with network and tarball steps stubbed."""
    monkeypatch.setenv("LLAMA_DEPLOY_BUILD_ID", "build1")
    monkeypatch.setenv("LLAMA_DEPLOY_BUILD_API_HOST", "host:8000")
    monkeypatch.setenv("LLAMA_DEPLOY_AUTH_TOKEN", "tok")
    monkeypatch.setenv("LLAMA_DEPLOY_DEPLOYMENT_NAME", "dep1")
    monkeypatch.setattr(bootstrap, "TERMINATION_LOG", tmp_path / "termination-log")
    mocks: dict[str, mock.Mock] = {}
    for name in [
        "setup_logging",
        "_find_artifact",
        "bootstrap_app_from_repo",
        "_create_tarball",
        "_upload_artifact",
    ]:
        mocks[name] = mock.Mock(return_value=None)
        monkeypatch.setattr(bootstrap, name, mocks[name])
    return mocks


def _termination_message(tmp_path: Path) -> str | None:
    log = tmp_path / "termination-log"
    return log.read_text() if log.exists() else None


def test_run_build_reports_built_ui_path(
    build_env: dict[str, mock.Mock], tmp_path: Path
) -> None:
    app_dir = tmp_path / "app"
    (app_dir / "src" / "ui" / "dist").mkdir(parents=True)
    build_env["bootstrap_app_from_repo"].return_value = app_dir / "src/ui/dist"

    run_build(str(app_dir))

    upload = build_env["_upload_artifact"].call_args.kwargs
    assert upload["static_assets_path"] == '"src/ui/dist"'
    assert _termination_message(tmp_path) == '"src/ui/dist"'


@pytest.mark.parametrize("ui_output", [None, "missing/dist"])
def test_run_build_reports_empty_path_without_built_ui(
    build_env: dict[str, mock.Mock], tmp_path: Path, ui_output: str | None
) -> None:
    app_dir = tmp_path / "app"
    app_dir.mkdir()
    build_env["bootstrap_app_from_repo"].return_value = (
        app_dir / ui_output if ui_output else None
    )

    run_build(str(app_dir))

    assert build_env["_upload_artifact"].call_args.kwargs["static_assets_path"] == '""'
    assert _termination_message(tmp_path) == '""'


def test_run_build_cache_hit_reports_stored_path(
    build_env: dict[str, mock.Mock], tmp_path: Path
) -> None:
    build_env["_find_artifact"].return_value = httpx.Headers(
        {"X-Static-Assets-Path": '"ui/dist"'}
    )

    run_build(str(tmp_path / "app"))

    build_env["bootstrap_app_from_repo"].assert_not_called()
    build_env["_upload_artifact"].assert_not_called()
    assert _termination_message(tmp_path) == '"ui/dist"'


def test_run_build_cache_hit_without_metadata_reports_nothing(
    build_env: dict[str, mock.Mock], tmp_path: Path
) -> None:
    build_env["_find_artifact"].return_value = httpx.Headers()

    run_build(str(tmp_path / "app"))

    build_env["bootstrap_app_from_repo"].assert_not_called()
    assert _termination_message(tmp_path) is None


def test_upload_artifact_raises_on_503(tmp_path: Path) -> None:
    tarball = tmp_path / "artifact.tar.gz"
    tarball.write_bytes(b"fake tarball content")
    resp = mock.Mock(status_code=503)
    with mock.patch("llama_agents.appserver.bootstrap.httpx.put", return_value=resp):
        with pytest.raises(RuntimeError, match="Build artifact storage not configured"):
            _upload_artifact("host:8000", "dep1", "build1", "tok", str(tarball), '""')


def test_bootstrap_discards_sdists_when_version_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When LLAMA_DEPLOY_APPSERVER_VERSION differs from bundled version, sdists are discarded."""
    _write_minimal_deployment_config(tmp_path)
    sd_dir = tmp_path / "sdists"
    sd_dir.mkdir()
    (sd_dir / "appserver-0.5.0.tar.gz").write_text("x")
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
        bootstrap_sdists=str(sd_dir),
    )
    monkeypatch.setenv("LLAMA_DEPLOY_APPSERVER_VERSION", "0.4.15")

    mocks = _stub_bootstrap_pipeline(monkeypatch)
    with (
        mock.patch("llama_agents.appserver.bootstrap.clone_repo"),
        mock.patch(
            "llama_agents.appserver.bootstrap.pkg_version", return_value="0.5.0"
        ),
    ):
        bootstrap_app_from_repo(target_dir=str(tmp_path))

    inject_mock = mocks["llama_agents.appserver.bootstrap.inject_appserver_into_target"]
    args, kwargs = inject_mock.call_args
    sdists = args[2] if len(args) >= 3 else kwargs.get("sdists")
    assert sdists is None
    assert kwargs.get("target_version") == "0.4.15"


def test_bootstrap_keeps_sdists_when_version_matches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When LLAMA_DEPLOY_APPSERVER_VERSION matches bundled version, sdists are kept."""
    _write_minimal_deployment_config(tmp_path)
    sd_dir = tmp_path / "sdists"
    sd_dir.mkdir()
    (sd_dir / "appserver-0.5.0.tar.gz").write_text("x")
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
        bootstrap_sdists=str(sd_dir),
    )
    monkeypatch.setenv("LLAMA_DEPLOY_APPSERVER_VERSION", "0.5.0")

    mocks = _stub_bootstrap_pipeline(monkeypatch)
    with (
        mock.patch("llama_agents.appserver.bootstrap.clone_repo"),
        mock.patch(
            "llama_agents.appserver.bootstrap.pkg_version", return_value="0.5.0"
        ),
    ):
        bootstrap_app_from_repo(target_dir=str(tmp_path))

    inject_mock = mocks["llama_agents.appserver.bootstrap.inject_appserver_into_target"]
    args, kwargs = inject_mock.call_args
    sdists = args[2] if len(args) >= 3 else kwargs.get("sdists")
    assert sdists is not None
    assert kwargs.get("target_version") == "0.5.0"


def test_bootstrap_no_appserver_version_env_uses_sdists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When LLAMA_DEPLOY_APPSERVER_VERSION is not set, sdists are passed through and target_version is None."""
    _write_minimal_deployment_config(tmp_path)
    sd_dir = tmp_path / "sdists"
    sd_dir.mkdir()
    (sd_dir / "appserver-0.5.0.tar.gz").write_text("x")
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
        bootstrap_sdists=str(sd_dir),
    )
    monkeypatch.delenv("LLAMA_DEPLOY_APPSERVER_VERSION", raising=False)

    mocks = _stub_bootstrap_pipeline(monkeypatch)
    with mock.patch("llama_agents.appserver.bootstrap.clone_repo"):
        bootstrap_app_from_repo(target_dir=str(tmp_path))

    inject_mock = mocks["llama_agents.appserver.bootstrap.inject_appserver_into_target"]
    args, kwargs = inject_mock.call_args
    sdists = args[2] if len(args) >= 3 else kwargs.get("sdists")
    assert sdists is not None
    assert kwargs.get("target_version") is None


def test_bootstrap_passes_auto_upgrade_false(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Bootstrap should pass auto_upgrade=False to inject_appserver_into_target
    so that dependencies like llama-index-workflows are not auto-upgraded
    during the container bootstrap process."""
    _write_minimal_deployment_config(tmp_path)
    _set_bootstrap_env(
        monkeypatch,
        repo_url="https://example.com/repo.git",
        auth_token="tok",
    )

    mocks = _stub_bootstrap_pipeline(monkeypatch)
    with mock.patch("llama_agents.appserver.bootstrap.clone_repo"):
        bootstrap_app_from_repo(target_dir=str(tmp_path))

    inject_mock = mocks["llama_agents.appserver.bootstrap.inject_appserver_into_target"]
    _, kwargs = inject_mock.call_args
    assert kwargs.get("auto_upgrade") is False


def test_download_artifact_raises_on_503() -> None:
    # httpx.stream returns a context manager; mock the response inside it.
    mock_response = mock.Mock(status_code=503)
    mock_response.__enter__ = mock.Mock(return_value=mock_response)
    mock_response.__exit__ = mock.Mock(return_value=False)
    with mock.patch(
        "llama_agents.appserver.bootstrap.httpx.stream", return_value=mock_response
    ):
        with pytest.raises(RuntimeError, match="Build artifact storage not configured"):
            _download_and_extract_artifact(
                "host:8000", "dep1", "build1", "tok", "/tmp/test-target"
            )
