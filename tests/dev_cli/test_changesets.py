# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import pytest

from dev_cli.changesets import (
    DockerBuildAction,
    DockerConfig,
    DockerManifestAction,
    HelmAction,
    HelmConfig,
    PackageJson,
    PypiAction,
    PyProjectContainer,
    _resolve_template,
    _write_toml_values,
    _write_yaml_values,
    apply_sync_values,
    current_version,
    docker_image_tags,
    execute_action,
    is_docker_image_published,
    is_helm_chart_published,
    is_published,
    is_rc_version,
    pep440_to_semver,
    plan_docker,
    plan_helm,
    semver_to_pep440,
)


def test_current_version(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "test-package"
version = "1.2.3"
dependencies = []
""".strip()
    )
    name, version = current_version(pyproject)
    assert name == "test-package"
    assert version == "1.2.3"


def test_current_version_normalizes_version(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "test-package"
version = "01.02.03"
dependencies = []
""".strip()
    )
    name, version = current_version(pyproject)
    assert name == "test-package"
    assert version == "1.2.3"


def test_pyproject_container_parse() -> None:
    toml_text = """
[project]
name = "my-package"
version = "0.1.0"
dependencies = ["requests>=2.0.0"]
""".strip()
    toml_doc, py_doc = PyProjectContainer.parse(toml_text)
    assert py_doc.project.name == "my-package"
    assert py_doc.project.version == "0.1.0"
    assert py_doc.project.dependencies == ["requests>=2.0.0"]


def test_is_published_returns_true_when_version_exists() -> None:
    mock_response = Mock()
    mock_response.read.return_value = json.dumps(
        {"releases": {"1.0.0": [], "1.1.0": []}}
    ).encode()

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = is_published("test-package", "1.0.0")
        assert result is True


def test_is_published_returns_false_when_version_missing() -> None:
    mock_response = Mock()
    mock_response.read.return_value = json.dumps({"releases": {"1.0.0": []}}).encode()

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = is_published("test-package", "1.1.0")
        assert result is False


def test_is_published_returns_false_when_package_not_found() -> None:
    mock_error = HTTPError("url", 404, "Not Found", {}, None)  # type: ignore[arg-type]

    with patch("urllib.request.urlopen", side_effect=mock_error):
        result = is_published("nonexistent-package", "1.0.0")
        assert result is False


def test_is_published_raises_on_other_http_errors() -> None:
    mock_error = HTTPError("url", 500, "Internal Server Error", {}, None)  # type: ignore[arg-type]

    with patch("urllib.request.urlopen", side_effect=mock_error):
        with pytest.raises(HTTPError) as exc_info:
            is_published("test-package", "1.0.0")
        assert exc_info.value.code == 500


def test_sync_package_version_updates_version(tmp_path: Path) -> None:
    package_dir = tmp_path / "package"
    package_dir.mkdir()
    pyproject = package_dir / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "test-package"
version = "1.0.0"
dependencies = []
""".strip()
    )

    pkg = PackageJson(
        name="test-js-package",
        version="2.0.0",
        path=package_dir,
        private=False,
    )

    apply_sync_values(pkg, {"test-js-package": pkg})

    _, py_doc = PyProjectContainer.parse(pyproject.read_text())
    assert py_doc.project.version == "2.0.0"


def test_sync_package_version_skips_when_no_pyproject(tmp_path: Path) -> None:
    package_dir = tmp_path / "package"
    package_dir.mkdir()

    pkg = PackageJson(
        name="test-js-package",
        version="2.0.0",
        path=package_dir,
        private=False,
    )

    changed = apply_sync_values(pkg, {"test-js-package": pkg})
    assert changed is False


def test_sync_package_version_skips_when_versions_match(tmp_path: Path) -> None:
    package_dir = tmp_path / "package"
    package_dir.mkdir()
    pyproject = package_dir / "pyproject.toml"
    original_content = """
[project]
name = "test-package"
version = "2.0.0"
dependencies = []
""".strip()
    pyproject.write_text(original_content)

    pkg = PackageJson(
        name="test-js-package",
        version="2.0.0",
        path=package_dir,
        private=False,
    )

    apply_sync_values(pkg, {"test-js-package": pkg})

    # Content should be unchanged
    assert pyproject.read_text() == original_content


def test_sync_package_version_converts_semver_prerelease(tmp_path: Path) -> None:
    package_dir = tmp_path / "package"
    package_dir.mkdir()
    pyproject = package_dir / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "test-package"
version = "1.0.0"
dependencies = []
""".strip()
    )

    pkg = PackageJson(
        name="test-js-package",
        version="1.2.3-a.4",
        path=package_dir,
        private=False,
    )

    apply_sync_values(pkg, {"test-js-package": pkg})

    _, py_doc = PyProjectContainer.parse(pyproject.read_text())
    assert py_doc.project.version == "1.2.3a4"


# -- semver_to_pep440 tests --


@pytest.mark.parametrize(
    ("semver", "expected"),
    [
        ("1.2.3-a.0", "1.2.3a0"),
        ("1.2.3-a.4", "1.2.3a4"),
        ("2.0.0-b.1", "2.0.0b1"),
        ("0.1.0-rc.3", "0.1.0rc3"),
        ("10.20.30-a.99", "10.20.30a99"),
    ],
)
def test_semver_to_pep440_prerelease(semver: str, expected: str) -> None:
    assert semver_to_pep440(semver) == expected


@pytest.mark.parametrize(
    "version",
    [
        "1.2.3",
        "0.0.1",
        "10.20.30",
    ],
)
def test_semver_to_pep440_stable_passthrough(version: str) -> None:
    assert semver_to_pep440(version) == version


def test_semver_to_pep440_rejects_non_pep440_label() -> None:
    with pytest.raises(ValueError, match="Unsupported pre-release label 'alpha'"):
        semver_to_pep440("1.2.3-alpha.1")


# -- pep440_to_semver tests --


@pytest.mark.parametrize(
    ("pep440", "expected"),
    [
        ("1.2.3a0", "1.2.3-a.0"),
        ("1.2.3a4", "1.2.3-a.4"),
        ("2.0.0b1", "2.0.0-b.1"),
        ("0.1.0rc3", "0.1.0-rc.3"),
        ("10.20.30a99", "10.20.30-a.99"),
    ],
)
def test_pep440_to_semver_prerelease(pep440: str, expected: str) -> None:
    assert pep440_to_semver(pep440) == expected


@pytest.mark.parametrize(
    "version",
    [
        "1.2.3",
        "0.0.1",
        "10.20.30",
    ],
)
def test_pep440_to_semver_stable_passthrough(version: str) -> None:
    assert pep440_to_semver(version) == version


# -- roundtrip tests --


@pytest.mark.parametrize(
    "semver",
    [
        "1.2.3-a.4",
        "2.0.0-b.1",
        "0.1.0-rc.3",
    ],
)
def test_roundtrip_semver_to_pep440_and_back(semver: str) -> None:
    pep440 = semver_to_pep440(semver)
    assert pep440_to_semver(pep440) == semver


@pytest.mark.parametrize(
    "pep440",
    [
        "1.2.3a4",
        "2.0.0b1",
        "0.1.0rc3",
    ],
)
def test_roundtrip_pep440_to_semver_and_back(pep440: str) -> None:
    semver = pep440_to_semver(pep440)
    assert semver_to_pep440(semver) == pep440


# -- chart version sync tests --


def _make_helm_package(
    path: Path, version: str = "0.4.14", name: str = "llama-agents"
) -> PackageJson:
    return PackageJson(
        name=name,
        version=version,
        path=path,
        private=True,
        helm=HelmConfig(registry="oci://docker.io/llamaindex"),
    )


def test_sync_chart_version_updates_chart_yaml(tmp_path: Path) -> None:
    chart_yaml = tmp_path / "Chart.yaml"
    chart_yaml.write_text('apiVersion: v2\nname: llama-agents\nversion: "0.4.13"\n')

    pkg = _make_helm_package(tmp_path, version="0.4.14")
    changed = apply_sync_values(pkg, {pkg.name: pkg})
    assert changed is True
    content = chart_yaml.read_text()
    assert "0.4.14" in content


def test_sync_chart_version_no_change_when_already_matching(tmp_path: Path) -> None:
    chart_yaml = tmp_path / "Chart.yaml"
    chart_yaml.write_text('apiVersion: v2\nname: llama-agents\nversion: "0.4.14"\n')

    pkg = _make_helm_package(tmp_path, version="0.4.14")
    changed = apply_sync_values(pkg, {pkg.name: pkg})
    assert changed is False


def test_sync_chart_version_returns_false_when_no_chart(tmp_path: Path) -> None:
    pkg = _make_helm_package(tmp_path, version="0.4.14")
    changed = apply_sync_values(pkg, {pkg.name: pkg})
    assert changed is False


def test_apply_sync_values_bakes_image_tags(tmp_path: Path) -> None:
    """syncValues on the chart resolves dependency docker tags into values.yaml."""
    chart_yaml = tmp_path / "Chart.yaml"
    chart_yaml.write_text('apiVersion: v2\nname: llama-agents\nversion: "0.4.13"\n')
    values_yaml = tmp_path / "values.yaml"
    values_yaml.write_text(
        """images:
  controlPlane:
    repository: llamaindex/llama-agents-control-plane
    tag: "0.4.13"
    pullPolicy: IfNotPresent
  operator:
    repository: llamaindex/llama-agents-operator
    tag: "operator-0.4.13"
    pullPolicy: IfNotPresent
  appserver:
    repository: llamaindex/llama-agents-appserver
    tag: "appserver-0.4.13"
    pullPolicy: IfNotPresent
"""
    )

    chart_pkg = PackageJson(
        name="llama-agents",
        version="0.4.14",
        path=tmp_path,
        private=True,
        helm=HelmConfig(registry="oci://docker.io/llamaindex"),
        syncValues={
            "values.yaml": {
                "images.controlPlane.tag": "{llama-agents-control-plane:dockerTag}",
                "images.operator.tag": "{llama-agents-operator:dockerTag}",
                "images.appserver.tag": "{llama-agents-appserver:dockerTag}",
            }
        },
    )
    workspace = {
        "llama-agents": chart_pkg,
        "llama-agents-control-plane": PackageJson(
            name="llama-agents-control-plane",
            version="0.4.14",
            path=Path("/fake/cp"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                target="controlplane",
                imageName="llamaindex/llama-agents-control-plane",
                platforms=["linux/amd64"],
            ),
        ),
        "llama-agents-operator": PackageJson(
            name="llama-agents-operator",
            version="0.4.15",
            path=Path("/fake/op"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/operator.Dockerfile",
                imageName="llamaindex/llama-agents-operator",
                platforms=["linux/amd64"],
            ),
        ),
        "llama-agents-appserver": PackageJson(
            name="llama-agents-appserver",
            version="0.4.16",
            path=Path("/fake/as"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                target="appserver",
                imageName="llamaindex/llama-agents-appserver",
                platforms=["linux/amd64"],
            ),
        ),
    }

    changed = apply_sync_values(chart_pkg, workspace)
    assert changed is True
    content = values_yaml.read_text()
    assert "0.4.14" in content  # controlPlane
    assert "0.4.15" in content
    assert "0.4.16" in content
    # Chart.yaml version also updated via implicit helm sync
    assert "0.4.14" in chart_yaml.read_text()


# -- is_rc_version tests --


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("0.4.14", False),
        ("1.0.0", False),
        ("0.4.14-rc.1", True),
        ("0.4.14-a.1", True),
        ("0.4.14-b.1", True),
        ("1.0.0rc1", True),
        ("1.0.0a1", True),
        ("1.0.0b1", True),
    ],
)
def test_is_rc_version(version: str, expected: bool) -> None:
    assert is_rc_version(version) == expected


# -- is_docker_image_published tests --


def test_is_docker_image_published_returns_true() -> None:
    with patch("urllib.request.urlopen"):
        result = is_docker_image_published(
            "llamaindex/llama-agents-appserver", "0.4.14"
        )
        assert result is True


def test_is_docker_image_published_returns_false_on_404() -> None:
    mock_error = HTTPError("url", 404, "Not Found", {}, None)  # type: ignore[arg-type]
    with patch("urllib.request.urlopen", side_effect=mock_error):
        result = is_docker_image_published(
            "llamaindex/llama-agents-appserver", "0.4.14"
        )
        assert result is False


def test_is_docker_image_published_raises_on_500() -> None:
    mock_error = HTTPError("url", 500, "Server Error", {}, None)  # type: ignore[arg-type]
    with patch("urllib.request.urlopen", side_effect=mock_error):
        with pytest.raises(HTTPError) as exc_info:
            is_docker_image_published("llamaindex/llama-agents-appserver", "0.4.14")
        assert exc_info.value.code == 500


# -- docker_image_tags tests --


def test_docker_image_tags_non_rc() -> None:
    image = DockerConfig(
        dockerfile="docker/Dockerfile",
        target="controlplane",
        platforms=["linux/amd64", "linux/arm64"],
        imageName="llamaindex/llama-agents-control-plane",
    )
    tags = docker_image_tags(image, "0.4.14", is_rc=False)
    assert tags == [
        "docker.io/llamaindex/llama-agents-control-plane:0.4.14",
        "docker.io/llamaindex/llama-agents-control-plane:latest",
        "docker.io/llamaindex/llama-agents-control-plane:0.4",
    ]


def test_docker_image_tags_rc() -> None:
    image = DockerConfig(
        dockerfile="docker/Dockerfile",
        target="controlplane",
        platforms=["linux/amd64", "linux/arm64"],
        imageName="llamaindex/llama-agents-control-plane",
    )
    tags = docker_image_tags(image, "0.4.14-rc.1", is_rc=True)
    assert tags == ["docker.io/llamaindex/llama-agents-control-plane:0.4.14-rc.1"]


def test_docker_image_tags_appserver() -> None:
    image = DockerConfig(
        dockerfile="docker/Dockerfile",
        target="appserver",
        platforms=["linux/amd64", "linux/arm64"],
        imageName="llamaindex/llama-agents-appserver",
    )
    tags = docker_image_tags(image, "0.4.14", is_rc=False)
    assert tags == [
        "docker.io/llamaindex/llama-agents-appserver:0.4.14",
        "docker.io/llamaindex/llama-agents-appserver:latest",
        "docker.io/llamaindex/llama-agents-appserver:0.4",
    ]


# -- _read_package_json_config tests --


def test_read_package_json_config_docker(tmp_path: Path) -> None:
    from dev_cli.changesets import _read_package_json_config

    pkg_json = tmp_path / "package.json"
    pkg_json.write_text(
        json.dumps(
            {
                "name": "test-pkg",
                "docker": {
                    "dockerfile": "docker/Dockerfile",
                    "target": "myapp",
                    "imageName": "llamaindex/test-myapp",
                    "platforms": ["linux/amd64"],
                },
            }
        )
    )
    config = _read_package_json_config(tmp_path)
    assert config is not None
    assert config.docker is not None
    assert config.docker.dockerfile == "docker/Dockerfile"
    assert config.docker.target == "myapp"
    assert config.docker.imageName == "llamaindex/test-myapp"
    assert config.docker.platforms == ["linux/amd64"]


def test_read_package_json_config_helm(tmp_path: Path) -> None:
    from dev_cli.changesets import _read_package_json_config

    pkg_json = tmp_path / "package.json"
    pkg_json.write_text(
        json.dumps(
            {"name": "my-chart", "helm": {"registry": "oci://docker.io/llamaindex"}}
        )
    )
    config = _read_package_json_config(tmp_path)
    assert config is not None
    assert config.helm is not None
    assert config.helm.registry == "oci://docker.io/llamaindex"


def test_read_package_json_config_defaults(tmp_path: Path) -> None:
    from dev_cli.changesets import _read_package_json_config

    pkg_json = tmp_path / "package.json"
    pkg_json.write_text(json.dumps({"name": "test-pkg"}))
    config = _read_package_json_config(tmp_path)
    assert config is not None
    assert config.docker is None
    assert config.helm is None


def test_read_package_json_config_returns_none_when_no_file(tmp_path: Path) -> None:
    from dev_cli.changesets import _read_package_json_config

    assert _read_package_json_config(tmp_path) is None


def test_read_package_json_config_docker_missing_dockerfile(tmp_path: Path) -> None:
    from pydantic import ValidationError

    from dev_cli.changesets import _read_package_json_config

    pkg_json = tmp_path / "package.json"
    pkg_json.write_text(json.dumps({"name": "test-pkg", "docker": {"target": "app"}}))
    with pytest.raises(ValidationError):
        _read_package_json_config(tmp_path)


def test_read_package_json_config_helm_missing_registry(tmp_path: Path) -> None:
    from pydantic import ValidationError

    from dev_cli.changesets import _read_package_json_config

    pkg_json = tmp_path / "package.json"
    pkg_json.write_text(json.dumps({"name": "my-chart", "helm": {}}))
    with pytest.raises(ValidationError):
        _read_package_json_config(tmp_path)


# -- plan_docker / execute_docker_build_action tests --


def _make_docker_packages() -> list[PackageJson]:
    """Create test PackageJson instances with Docker configs."""
    return [
        PackageJson(
            name="llama-agents-control-plane",
            version="0.4.14",
            path=Path("/fake/controlplane"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                target="controlplane",
                platforms=["linux/amd64", "linux/arm64"],
                imageName="llamaindex/llama-agents-control-plane",
            ),
        ),
        PackageJson(
            name="llama-agents-operator",
            version="0.4.14",
            path=Path("/fake/operator"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/operator.Dockerfile",
                platforms=["linux/amd64"],
                imageName="llamaindex/llama-agents-operator",
            ),
        ),
        PackageJson(
            name="llama-agents-appserver",
            version="0.4.14",
            path=Path("/fake/appserver"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                target="appserver",
                platforms=["linux/amd64", "linux/arm64"],
                imageName="llamaindex/llama-agents-appserver",
            ),
        ),
    ]


def test_plan_docker_skips_when_already_published() -> None:
    with patch("dev_cli.changesets.is_docker_image_published", return_value=True):
        builds, manifests = plan_docker(_make_docker_packages())
    assert builds == []
    assert manifests == []


def test_plan_docker_emits_per_platform_builds() -> None:
    with patch("dev_cli.changesets.is_docker_image_published", return_value=False):
        builds, manifests = plan_docker(_make_docker_packages())
    # 2 + 1 + 2 = 5 platform builds, 3 manifests
    assert len(builds) == 5
    assert len(manifests) == 3
    # Each manifest references the matching per-arch build tags
    cp_manifest = next(
        m for m in manifests if m.package == "llama-agents-control-plane"
    )
    assert sorted(cp_manifest.source_tags) == sorted(
        [
            "docker.io/llamaindex/llama-agents-control-plane:0.4.14-amd64",
            "docker.io/llamaindex/llama-agents-control-plane:0.4.14-arm64",
        ]
    )


def test_plan_docker_skips_packages_without_docker() -> None:
    packages = [
        PackageJson(
            name="no-docker-pkg",
            version="1.0.0",
            path=Path("/fake/no-docker"),
            private=True,
            docker=None,
        ),
    ]
    with patch("dev_cli.changesets.is_docker_image_published") as mock_check:
        builds, manifests = plan_docker(packages)
    mock_check.assert_not_called()
    assert builds == [] and manifests == []


def test_plan_docker_uses_per_package_version() -> None:
    packages = [
        PackageJson(
            name="pkg-a",
            version="1.0.0",
            path=Path("/fake/a"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                target="a",
                platforms=["linux/amd64"],
                imageName="llamaindex/test-a",
            ),
        ),
        PackageJson(
            name="pkg-b",
            version="2.0.0",
            path=Path("/fake/b"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                target="b",
                platforms=["linux/amd64"],
                imageName="llamaindex/test-b",
            ),
        ),
    ]
    with patch("dev_cli.changesets.is_docker_image_published", return_value=False):
        builds, _ = plan_docker(packages)
    by_pkg = {b.package: b for b in builds}
    assert by_pkg["pkg-a"].version == "1.0.0"
    assert by_pkg["pkg-a"].build_tag.endswith("1.0.0-amd64")
    assert by_pkg["pkg-b"].version == "2.0.0"
    assert by_pkg["pkg-b"].build_tag.endswith("2.0.0-amd64")


def _docker_build_action() -> DockerBuildAction:
    return DockerBuildAction(
        package="pkg",
        image="llamaindex/test",
        dockerfile="docker/Dockerfile",
        target="t",
        platform="linux/amd64",
        version="1.0.0",
        build_tag="docker.io/llamaindex/test:1.0.0-amd64",
        cache_scope="pkg-amd64",
    )


def test_execute_docker_build_action_dry_run_skips_run_command() -> None:
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(_docker_build_action(), dry_run=True)
    mock_run.assert_not_called()


def test_execute_docker_build_action_invokes_buildx() -> None:
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(_docker_build_action(), dry_run=False)
    cmd = mock_run.call_args[0][0]
    assert cmd[:3] == ["docker", "buildx", "build"]
    assert "--push" in cmd
    assert "--platform" in cmd and "linux/amd64" in cmd
    assert "--tag" in cmd and "docker.io/llamaindex/test:1.0.0-amd64" in cmd


def test_execute_docker_build_action_passes_security_updates_date() -> None:
    with (
        patch("dev_cli.changesets.run_command") as mock_run,
        patch("dev_cli.changesets._utc_today", return_value="2026-11-09"),
    ):
        execute_action(_docker_build_action(), dry_run=False)
    cmd = mock_run.call_args[0][0]
    assert cmd[cmd.index("--build-arg") + 1] == "SECURITY_UPDATES_DATE=2026-11-09"
    assert cmd[-1] == "."


def test_execute_docker_build_action_emits_gha_cache_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTIONS_RUNTIME_TOKEN", "token")
    monkeypatch.setenv("ACTIONS_CACHE_URL", "https://cache.example.com")
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(_docker_build_action(), dry_run=False)
    cmd = mock_run.call_args[0][0]
    assert "--cache-from" in cmd
    cache_to = cmd[cmd.index("--cache-to") + 1]
    # Default cache_mode is "max"; zstd + ignore-error are always set under GHA.
    assert "mode=max" in cache_to
    assert "scope=pkg-amd64" in cache_to
    assert "compression=zstd" in cache_to
    assert "ignore-error=true" in cache_to


def test_execute_docker_build_action_respects_cache_mode_min(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTIONS_RUNTIME_TOKEN", "token")
    monkeypatch.setenv("ACTIONS_CACHE_URL", "https://cache.example.com")
    action = _docker_build_action().model_copy(update={"cache_mode": "min"})
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(action, dry_run=False)
    cmd = mock_run.call_args[0][0]
    cache_to = cmd[cmd.index("--cache-to") + 1]
    assert "mode=min" in cache_to
    assert "mode=max" not in cache_to


def test_plan_docker_propagates_cache_mode() -> None:
    packages = [
        PackageJson(
            name="pkg-min",
            path=Path("/tmp/pkg-min"),
            version="1.0.0",
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                imageName="llamaindex/pkg-min",
                platforms=["linux/amd64"],
                cacheMode="min",
            ),
        ),
        PackageJson(
            name="pkg-default",
            path=Path("/tmp/pkg-default"),
            version="1.0.0",
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                imageName="llamaindex/pkg-default",
                platforms=["linux/amd64"],
            ),
        ),
    ]
    with patch("dev_cli.changesets.is_docker_image_published", return_value=False):
        builds, _ = plan_docker(packages)
    by_pkg = {b.package: b for b in builds}
    assert by_pkg["pkg-min"].cache_mode == "min"
    assert by_pkg["pkg-default"].cache_mode == "max"


def test_execute_docker_manifest_action_combines_source_tags() -> None:
    action = DockerManifestAction(
        package="pkg",
        image="llamaindex/test",
        version="1.0.0",
        final_tags=[
            "docker.io/llamaindex/test:1.0.0",
            "docker.io/llamaindex/test:latest",
        ],
        source_tags=[
            "docker.io/llamaindex/test:1.0.0-amd64",
            "docker.io/llamaindex/test:1.0.0-arm64",
        ],
    )
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(action, dry_run=False)
    cmd = mock_run.call_args[0][0]
    assert cmd[:4] == ["docker", "buildx", "imagetools", "create"]
    assert cmd.count("--tag") == 2
    assert "docker.io/llamaindex/test:1.0.0-amd64" in cmd
    assert "docker.io/llamaindex/test:1.0.0-arm64" in cmd


# -- _execute_pypi tests --


def _write_pyproject(path: Path, name: str, version: str) -> None:
    path.write_text(
        f'[project]\nname = "{name}"\nversion = "{version}"\ndependencies = []\n'
    )


def test_execute_pypi_dry_run_skips_run_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pkg_dir = tmp_path / "packages" / "pkg"
    pkg_dir.mkdir(parents=True)
    _write_pyproject(pkg_dir / "pyproject.toml", "pkg", "1.2.3")
    monkeypatch.chdir(tmp_path)
    action = PypiAction(package="pkg", version="1.2.3", path="packages/pkg")
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(action, dry_run=True)
    mock_run.assert_not_called()


def test_execute_pypi_raises_when_plan_version_mismatches_on_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Reproduces the publish_changesets.yml bug: the plan was computed
    # from a tree where ``pnpm -w run version`` had bumped pyproject.toml
    # to 0.8.10, but the publish job checks out the branch fresh and
    # sees the still-committed 0.8.9. Without this guard, uv build would
    # silently produce 0.8.9 artifacts that don't match the plan glob.
    pkg_dir = tmp_path / "packages" / "pkg"
    pkg_dir.mkdir(parents=True)
    _write_pyproject(pkg_dir / "pyproject.toml", "pkg", "0.8.9")
    monkeypatch.chdir(tmp_path)
    action = PypiAction(package="pkg", version="0.8.10", path="packages/pkg")
    with patch("dev_cli.changesets.run_command") as mock_run:
        with pytest.raises(RuntimeError, match="Plan expects pkg@0.8.10"):
            execute_action(action, dry_run=False)
    mock_run.assert_not_called()


# -- plan_helm / execute_helm_action tests --


def _make_helm_packages() -> list[PackageJson]:
    return [
        PackageJson(
            name="llama-agents",
            version="0.4.14",
            path=Path("/fake/charts/llama-agents"),
            private=False,
            helm=HelmConfig(registry="oci://docker.io/llamaindex"),
        ),
        PackageJson(
            name="llama-agents-crds",
            version="0.4.14",
            path=Path("/fake/charts/llama-agents-crds"),
            private=False,
            helm=HelmConfig(registry="oci://docker.io/llamaindex"),
        ),
    ]


def test_plan_helm_skips_already_published() -> None:
    with patch("dev_cli.changesets.is_helm_chart_published", return_value=True):
        actions = plan_helm(_make_helm_packages())
    assert actions == []


def test_plan_helm_emits_one_action_per_chart() -> None:
    with patch("dev_cli.changesets.is_helm_chart_published", return_value=False):
        actions = plan_helm(_make_helm_packages())
    assert [a.package for a in actions] == ["llama-agents", "llama-agents-crds"]
    assert all(a.version == "0.4.14" for a in actions)


def test_plan_helm_skips_packages_without_helm() -> None:
    packages = [
        PackageJson(
            name="no-helm-pkg",
            version="1.0.0",
            path=Path("/fake"),
            private=True,
            helm=None,
        ),
    ]
    with patch("dev_cli.changesets.is_helm_chart_published") as mock_check:
        assert plan_helm(packages) == []
    mock_check.assert_not_called()


def test_execute_helm_action_dry_run_skips_run_command() -> None:
    action = HelmAction(
        package="llama-agents",
        chart_path="/fake/charts/llama-agents",
        version="0.4.14",
        registry="oci://docker.io/llamaindex",
    )
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(action, dry_run=True)
    mock_run.assert_not_called()


def test_execute_helm_action_packages_and_pushes() -> None:
    action = HelmAction(
        package="llama-agents",
        chart_path="/fake/charts/llama-agents",
        version="0.4.14",
        registry="oci://docker.io/llamaindex",
    )
    with patch("dev_cli.changesets.run_command") as mock_run:
        execute_action(action, dry_run=False)
    assert mock_run.call_args_list[0][0][0] == [
        "helm",
        "package",
        "/fake/charts/llama-agents",
    ]
    assert mock_run.call_args_list[1][0][0] == [
        "helm",
        "push",
        "llama-agents-0.4.14.tgz",
        "oci://docker.io/llamaindex",
    ]


def test_is_helm_chart_published_uses_chart_name() -> None:
    with patch("urllib.request.urlopen") as mock_urlopen:
        result = is_helm_chart_published("llama-agents-crds", "0.7.1")
        assert result is True
        called_url = mock_urlopen.call_args[0][0]
        assert "llamaindex/llama-agents-crds" in called_url
        assert "0.7.1" in called_url


# -- template resolution tests --


def _make_workspace() -> tuple[PackageJson, dict[str, PackageJson]]:
    """Create a self_pkg and workspace for template tests."""
    self_pkg = PackageJson(
        name="my-chart",
        version="1.2.3",
        path=Path("/fake/chart"),
        private=True,
        helm=HelmConfig(registry="oci://docker.io/llamaindex"),
    )
    workspace = {
        "my-chart": self_pkg,
        "my-app": PackageJson(
            name="my-app",
            version="2.0.0",
            path=Path("/fake/app"),
            private=False,
            docker=DockerConfig(
                dockerfile="docker/Dockerfile",
                imageName="llamaindex/test-app",
                platforms=["linux/amd64"],
            ),
        ),
        "no-docker": PackageJson(
            name="no-docker",
            version="3.0.0",
            path=Path("/fake/nodock"),
            private=True,
        ),
    }
    return self_pkg, workspace


def test_resolve_template_version() -> None:
    self_pkg, workspace = _make_workspace()
    assert _resolve_template("{my-app:version}", self_pkg, workspace) == "2.0.0"


def test_resolve_template_self_version() -> None:
    self_pkg, workspace = _make_workspace()
    assert _resolve_template("{self:version}", self_pkg, workspace) == "1.2.3"


def test_resolve_template_docker_tag() -> None:
    self_pkg, workspace = _make_workspace()
    assert _resolve_template("{my-app:dockerTag}", self_pkg, workspace) == "2.0.0"


def test_resolve_template_pep440_version() -> None:
    self_pkg, workspace = _make_workspace()
    self_pkg.version = "1.2.3-a.4"
    assert _resolve_template("{self:pep440Version}", self_pkg, workspace) == "1.2.3a4"


def test_resolve_template_unknown_package() -> None:
    self_pkg, workspace = _make_workspace()
    with pytest.raises(ValueError, match="unknown package 'nonexistent'"):
        _resolve_template("{nonexistent:version}", self_pkg, workspace)


def test_resolve_template_docker_tag_no_docker() -> None:
    self_pkg, workspace = _make_workspace()
    with pytest.raises(ValueError, match="has no docker config"):
        _resolve_template("{no-docker:dockerTag}", self_pkg, workspace)


def test_resolve_template_unknown_property() -> None:
    self_pkg, workspace = _make_workspace()
    with pytest.raises(ValueError, match="unknown property 'badProp'"):
        _resolve_template("{my-app:badProp}", self_pkg, workspace)


# -- YAML writer tests --


def test_write_yaml_values_sets_dot_path(tmp_path: Path) -> None:
    yaml_file = tmp_path / "values.yaml"
    yaml_file.write_text(
        """# A comment
images:
  controlPlane:
    repository: llamaindex/llama-agents-control-plane
    tag: "0.4.13"
    pullPolicy: IfNotPresent
"""
    )
    changed = _write_yaml_values(yaml_file, {"images.controlPlane.tag": "0.4.14"})
    assert changed is True
    content = yaml_file.read_text()
    assert "0.4.14" in content
    assert "# A comment" in content  # comments preserved


def test_write_yaml_values_preserves_comments(tmp_path: Path) -> None:
    yaml_file = tmp_path / "values.yaml"
    original = """# Top comment
images:
  controlPlane: # the control plane
    repository: llamaindex/llama-agents-control-plane
    tag: "0.8.0"
    pullPolicy: IfNotPresent
  operator: # the operator
    repository: llamaindex/llama-agents-operator
    tag: "operator-0.8.0"
    pullPolicy: IfNotPresent
"""
    yaml_file.write_text(original)
    changed = _write_yaml_values(yaml_file, {"images.controlPlane.tag": "0.9.0"})
    assert changed is True
    content = yaml_file.read_text()
    assert "0.9.0" in content
    assert "# Top comment" in content
    assert "# the control plane" in content
    assert "# the operator" in content


def test_write_yaml_values_no_change(tmp_path: Path) -> None:
    yaml_file = tmp_path / "values.yaml"
    yaml_file.write_text('version: "0.8.0"\n')
    changed = _write_yaml_values(yaml_file, {"version": "0.8.0"})
    assert changed is False


def test_write_yaml_values_missing_file(tmp_path: Path) -> None:
    yaml_file = tmp_path / "nonexistent.yaml"
    changed = _write_yaml_values(yaml_file, {"key": "val"})
    assert changed is False


def test_write_yaml_values_creates_intermediate_keys(tmp_path: Path) -> None:
    yaml_file = tmp_path / "test.yaml"
    yaml_file.write_text("existing: value\n")
    changed = _write_yaml_values(yaml_file, {"new.nested.key": "hello"})
    assert changed is True
    content = yaml_file.read_text()
    assert "hello" in content


# -- TOML writer tests --


def test_write_toml_values_sets_dot_path(tmp_path: Path) -> None:
    toml_file = tmp_path / "pyproject.toml"
    toml_file.write_text(
        """[project]
name = "test"
version = "1.0.0"
"""
    )
    changed = _write_toml_values(toml_file, {"project.version": "2.0.0"})
    assert changed is True
    content = toml_file.read_text()
    assert '"2.0.0"' in content


def test_write_toml_values_no_change(tmp_path: Path) -> None:
    toml_file = tmp_path / "pyproject.toml"
    toml_file.write_text(
        """[project]
name = "test"
version = "1.0.0"
"""
    )
    changed = _write_toml_values(toml_file, {"project.version": "1.0.0"})
    assert changed is False


def test_write_toml_values_missing_file(tmp_path: Path) -> None:
    toml_file = tmp_path / "nonexistent.toml"
    changed = _write_toml_values(toml_file, {"key": "val"})
    assert changed is False


# -- apply_sync_values tests --


def test_apply_sync_values_explicit_yaml(tmp_path: Path) -> None:
    values_yaml = tmp_path / "values.yaml"
    values_yaml.write_text(
        """images:
  app:
    tag: "old"
"""
    )
    pkg = PackageJson(
        name="my-chart",
        version="1.0.0",
        path=tmp_path,
        private=True,
        syncValues={
            "values.yaml": {
                "images.app.tag": "{dep-app:dockerTag}",
            }
        },
    )
    workspace = {
        "my-chart": pkg,
        "dep-app": PackageJson(
            name="dep-app",
            version="2.0.0",
            path=Path("/fake"),
            private=False,
            docker=DockerConfig(
                dockerfile="Dockerfile",
                imageName="llamaindex/test-dep-app",
                platforms=["linux/amd64"],
            ),
        ),
    }
    changed = apply_sync_values(pkg, workspace)
    assert changed is True
    assert "2.0.0" in values_yaml.read_text()


def test_apply_sync_values_implicit_helm(tmp_path: Path) -> None:
    chart_yaml = tmp_path / "Chart.yaml"
    chart_yaml.write_text('apiVersion: v2\nversion: "0.1.0"\n')
    pkg = PackageJson(
        name="my-chart",
        version="0.2.0",
        path=tmp_path,
        private=True,
        helm=HelmConfig(registry="oci://docker.io/llamaindex"),
    )
    changed = apply_sync_values(pkg, {"my-chart": pkg})
    assert changed is True
    assert "0.2.0" in chart_yaml.read_text()


def test_apply_sync_values_implicit_pyproject(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "test"\nversion = "1.0.0"\n')
    pkg = PackageJson(
        name="test-pkg",
        version="2.0.0",
        path=tmp_path,
        private=False,
    )
    changed = apply_sync_values(pkg, {"test-pkg": pkg})
    assert changed is True
    content = pyproject.read_text()
    assert '"2.0.0"' in content
