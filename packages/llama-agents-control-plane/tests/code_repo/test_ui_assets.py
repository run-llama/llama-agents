# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from dulwich import porcelain
from llama_agents.control_plane.code_repo.storage import CodeRepoStorage
from llama_agents.control_plane.manage_api import deployments_service as service
from llama_agents.core.schema.deployments import (
    DeploymentResponse,
    DeploymentUpdate,
    LlamaDeploymentSpec,
    RollbackRequest,
    apply_deployment_update,
)

from .conftest import make_storage


@pytest.fixture
def pushed_ui(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[CodeRepoStorage, str, str]]:
    root = tmp_path / "source"
    root.mkdir()
    with porcelain.init(str(root)) as repo:
        app = root / "apps" / "example"
        (app / "ui").mkdir(parents=True)
        config = app / "pyproject.toml"
        config.write_text(
            '[project]\nname = "example"\n[tool.llamaagents.ui]\ndirectory = "ui"\nbuild_output_dir = "ui/out"\n'
        )
        (app / "ui" / "package.json").write_text('{"scripts":{"build":"next build"}}')
        porcelain.add(repo, paths=[str(config), str(app / "ui" / "package.json")])
        first = porcelain.commit(
            repo, message=b"Initial UI", author=b"Test <test@example.com>"
        ).decode()
        config.write_text(
            '[project]\nname = "example"\n[tool.llamaagents.ui]\ndirectory = "ui"\nbuild_output_dir = "ui/dist"\n'
        )
        porcelain.add(repo, paths=[str(config)])
        second = porcelain.commit(
            repo, message=b"Move output", author=b"Test <test@example.com>"
        ).decode()
    bare = tmp_path / "bare"
    porcelain.clone(str(root), str(bare), bare=True).close()
    downloads: list[Path] = []

    async def download_repo(deployment_id: str) -> Path:
        parent = Path(tempfile.mkdtemp(dir=tmp_path))
        downloads.append(parent)
        shutil.copytree(bare, parent / "repo")
        return parent / "repo"

    storage = make_storage()
    monkeypatch.setattr(storage, "download_repo", download_repo)
    monkeypatch.setattr(service, "code_repo_storage", storage)
    yield storage, first, second
    assert all(not path.exists() for path in downloads)


def deployment(sha: str | None = None) -> DeploymentResponse:
    return DeploymentResponse(
        id="dep",
        project_id="proj",
        display_name="dep",
        repo_url="internal://" if sha else "",
        git_sha=sha,
        deployment_file_path="apps/example",
        status="Running",
        has_personal_access_token=False,
    )


async def test_discover_assets_at_selected_commit(
    pushed_ui: tuple[CodeRepoStorage, str, str],
) -> None:
    storage, first, second = pushed_ui
    assert await storage.get_ui_build_output_path("dep", first, "apps/example") == Path(
        "apps/example/ui/out"
    )
    assert await storage.get_ui_build_output_path(
        "dep", second, "apps/example/pyproject.toml"
    ) == Path("apps/example/ui/dist")
    assert await storage.get_ui_build_output_path("dep", first, ".") is None


async def test_first_push_sets_assets_but_later_push_does_not_deploy(
    pushed_ui: tuple[CodeRepoStorage, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, first, second = pushed_ui
    current = deployment()
    monkeypatch.setattr(
        service.k8s_client, "get_deployment", AsyncMock(return_value=current)
    )
    update = AsyncMock(return_value=current)
    monkeypatch.setattr(service.k8s_client, "update_deployment", update)
    await service._on_push_complete("dep", first, "main")
    submitted = update.call_args.kwargs["update"]
    assert submitted.git_sha == first
    assert submitted.static_assets_path == Path("apps/example/ui/out")
    current.repo_url = "internal://"
    current.git_sha = first
    update.reset_mock()
    await service._on_push_complete("dep", second, "main")
    update.assert_not_awaited()


async def test_update_and_rollback_select_matching_assets(
    pushed_ui: tuple[CodeRepoStorage, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    storage, first, second = pushed_ui
    current = deployment(first)
    monkeypatch.setattr(
        service.k8s_client, "get_deployment", AsyncMock(return_value=current)
    )
    update = AsyncMock(return_value=current)
    monkeypatch.setattr(service.k8s_client, "update_deployment", update)
    # Resolve the actual pushed HEAD using the same downloaded bare repo.
    await service.deployments_service.update_deployment(
        "proj", "dep", DeploymentUpdate(repo_url="internal://", git_ref=second)
    )
    submitted = update.call_args.kwargs["update"]
    assert submitted.git_sha == second
    assert submitted.static_assets_path == Path("apps/example/ui/dist")
    await service.deployments_service.rollback_deployment(
        "proj", "dep", RollbackRequest(git_sha=first, image_tag="test")
    )
    assert update.call_args.kwargs["update"].static_assets_path == Path(
        "apps/example/ui/out"
    )


async def test_config_path_update_uses_deployed_commit(
    pushed_ui: tuple[CodeRepoStorage, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, first, _ = pushed_ui
    current = deployment(first)
    current.deployment_file_path = "."
    monkeypatch.setattr(
        service.k8s_client, "get_deployment", AsyncMock(return_value=current)
    )
    update = AsyncMock(return_value=current)
    monkeypatch.setattr(service.k8s_client, "update_deployment", update)
    await service.deployments_service.update_deployment(
        "proj", "dep", DeploymentUpdate(deployment_file_path="apps/example")
    )
    submitted = update.call_args.kwargs["update"]
    assert submitted.git_sha is None
    assert submitted.static_assets_path == Path("apps/example/ui/out")


async def test_update_to_config_without_ui_clears_asset_path(
    pushed_ui: tuple[CodeRepoStorage, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, first, _ = pushed_ui
    current = deployment(first)
    monkeypatch.setattr(
        service.k8s_client, "get_deployment", AsyncMock(return_value=current)
    )
    update = AsyncMock(return_value=current)
    monkeypatch.setattr(service.k8s_client, "update_deployment", update)
    await service.deployments_service.update_deployment(
        "proj", "dep", DeploymentUpdate(deployment_file_path=".")
    )
    submitted = update.call_args.kwargs["update"]
    spec = LlamaDeploymentSpec(
        projectId="proj", repoUrl="internal://", staticAssetsPath="apps/example/ui/out"
    )
    assert (
        apply_deployment_update(submitted, spec).updated_spec.staticAssetsPath is None
    )


async def test_discovery_rejects_config_outside_repository(
    pushed_ui: tuple[CodeRepoStorage, str, str],
) -> None:
    storage, first, _ = pushed_ui
    with pytest.raises(RuntimeError, match="not a subdirectory"):
        await storage.get_ui_build_output_path("dep", first, "../outside")
