"""Unit tests for BackupService."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from llama_agents.control_plane.backup.archive import create_backup_archive
from llama_agents.control_plane.backup.storage import BackupInfo
from llama_agents.control_plane.manage_api.backup_service import BackupService
from llama_agents.core.schema.backups import RestoreRequest

from .conftest import make_deployment, make_raw_crd

K8S = "llama_agents.control_plane.manage_api.backup_service.k8s_client"
SETTINGS = "llama_agents.control_plane.manage_api.backup_service.settings"


@pytest.fixture
def mock_k8s() -> MagicMock:
    """Return a mock of the k8s_client module with sensible defaults."""
    m = MagicMock()
    m.get_all_deployment_crds = AsyncMock(return_value=[])
    m.get_secret_data = AsyncMock(return_value=None)
    m.get_deployment_crd_raw = AsyncMock(return_value=None)
    m.apply_deployment_crd = AsyncMock()
    m.apply_secret = AsyncMock()
    m.delete_deployment_crd = AsyncMock()
    m.get_namespace.return_value = "default"
    return m


@pytest.fixture
def mock_settings() -> MagicMock:
    m = MagicMock()
    m.backup_encryption_password = None
    return m


# ---------------------------------------------------------------------------
# create_backup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_backup_success(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    crds = [make_raw_crd("app1", generation=2), make_raw_crd("app2", generation=1)]
    mock_k8s.get_all_deployment_crds = AsyncMock(return_value=crds)

    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.create_backup()

    assert resp.status == "completed"
    assert resp.deployment_count == 2
    assert resp.size_bytes is not None and resp.size_bytes > 0
    mock_storage.upload.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_backup_with_secrets(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    mock_k8s.get_all_deployment_crds = AsyncMock(return_value=[make_raw_crd("app1")])
    mock_k8s.get_secret_data = AsyncMock(return_value={"API_KEY": "secret"})

    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.create_backup()

    assert resp.status == "completed"
    mock_storage.upload.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_backup_failure(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    mock_k8s.get_all_deployment_crds = AsyncMock(side_effect=RuntimeError("k8s down"))

    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.create_backup()

    assert resp.status == "failed"
    assert resp.error is not None and "k8s down" in resp.error


# ---------------------------------------------------------------------------
# list_backups / get_backup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_backups(
    mock_storage: AsyncMock,
    backup_service: BackupService,
) -> None:
    mock_storage.list_backups.return_value = [
        BackupInfo(
            backup_id="b1",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
            size_bytes=100,
        ),
    ]
    resp = await backup_service.list_backups()
    assert len(resp.backups) == 1
    assert resp.backups[0].backup_id == "b1"


@pytest.mark.asyncio
async def test_get_backup_exists(
    mock_storage: AsyncMock,
    backup_service: BackupService,
) -> None:
    mock_storage.get_info.return_value = BackupInfo(
        backup_id="b1",
        timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        size_bytes=200,
    )
    resp = await backup_service.get_backup("b1")
    assert resp.status == "completed"
    assert resp.size_bytes == 200


@pytest.mark.asyncio
async def test_get_backup_not_found(
    mock_storage: AsyncMock,
    backup_service: BackupService,
) -> None:
    mock_storage.get_info.return_value = None
    resp = await backup_service.get_backup("nonexistent")
    assert resp.status == "failed"
    assert resp.error is not None and "not found" in resp.error.lower()


# ---------------------------------------------------------------------------
# delete_backup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_backup_exists(
    mock_storage: AsyncMock,
    backup_service: BackupService,
) -> None:
    mock_storage.get_info.return_value = BackupInfo(
        backup_id="b1",
        timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        size_bytes=100,
    )
    resp = await backup_service.delete_backup("b1")
    assert resp.status == "deleted"
    mock_storage.delete.assert_awaited_once_with("b1")


@pytest.mark.asyncio
async def test_delete_backup_not_found(
    mock_storage: AsyncMock,
    backup_service: BackupService,
) -> None:
    mock_storage.get_info.return_value = None
    resp = await backup_service.delete_backup("no-such")
    assert resp.status == "failed"
    assert resp.error is not None
    mock_storage.delete.assert_not_awaited()


# ---------------------------------------------------------------------------
# restore_backup
# ---------------------------------------------------------------------------


def _make_archive_bytes(
    entries: list[dict],
    secrets: dict | None = None,
    generations: dict | None = None,
) -> bytes:
    """Build a real archive for restore tests."""
    return create_backup_archive(
        deployments=entries,
        secrets=secrets or {},
        namespace="default",
        timestamp="2025-01-01T00:00:00Z",
        generations=generations,
        allow_plaintext_secrets=True,
    )


@pytest.mark.asyncio
async def test_restore_skip_existing(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1")
    mock_storage.download.return_value = _make_archive_bytes([dep])
    mock_k8s.get_deployment_crd_raw = AsyncMock(return_value=make_raw_crd("app1"))

    req = RestoreRequest(backup_id="b1", conflict_mode="skip")
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    assert resp.results is not None
    assert resp.results[0].action == "skipped"
    mock_k8s.apply_deployment_crd.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_overwrite_always(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1")
    mock_storage.download.return_value = _make_archive_bytes([dep])
    mock_k8s.get_deployment_crd_raw = AsyncMock(return_value=make_raw_crd("app1"))

    req = RestoreRequest(backup_id="b1", conflict_mode="overwrite-always")
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    assert resp.results is not None
    assert resp.results[0].action == "updated"
    mock_k8s.apply_deployment_crd.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_overwrite_if_newer_skips_when_cluster_newer(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1")
    # Backup has generation 2, cluster has generation 5 (newer)
    mock_storage.download.return_value = _make_archive_bytes(
        [dep], generations={"app1": 2}
    )
    mock_k8s.get_deployment_crd_raw = AsyncMock(
        return_value=make_raw_crd("app1", generation=5)
    )

    req = RestoreRequest(backup_id="b1", conflict_mode="overwrite-if-newer")
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    assert resp.results is not None
    assert resp.results[0].action == "skipped"
    mock_k8s.apply_deployment_crd.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_overwrite_if_newer_overwrites_when_backup_newer(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1")
    # Backup has generation 5, cluster has generation 2 (older)
    mock_storage.download.return_value = _make_archive_bytes(
        [dep], generations={"app1": 5}
    )
    mock_k8s.get_deployment_crd_raw = AsyncMock(
        return_value=make_raw_crd("app1", generation=2)
    )

    req = RestoreRequest(backup_id="b1", conflict_mode="overwrite-if-newer")
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    assert resp.results is not None
    assert resp.results[0].action == "updated"
    mock_k8s.apply_deployment_crd.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_project_mismatch(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1", project_id="proj-A")
    mock_storage.download.return_value = _make_archive_bytes([dep])
    mock_k8s.get_deployment_crd_raw = AsyncMock(
        return_value=make_raw_crd("app1", project_id="proj-B")
    )

    req = RestoreRequest(backup_id="b1", conflict_mode="overwrite-always")
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    assert resp.results is not None
    assert resp.results[0].action == "failed"
    assert "Project mismatch" in (resp.results[0].error or "")


@pytest.mark.asyncio
async def test_restore_create_new(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1")
    mock_storage.download.return_value = _make_archive_bytes([dep])
    mock_k8s.get_deployment_crd_raw = AsyncMock(return_value=None)

    req = RestoreRequest(backup_id="b1")
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    assert resp.results is not None
    assert resp.results[0].action == "created"
    mock_k8s.apply_deployment_crd.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_with_secrets(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1")
    mock_storage.download.return_value = _make_archive_bytes(
        [dep], secrets={"app1": {"KEY": "val"}}
    )
    mock_k8s.get_deployment_crd_raw = AsyncMock(return_value=None)

    req = RestoreRequest(backup_id="b1")
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    mock_k8s.apply_secret.assert_awaited_once()
    mock_k8s.apply_deployment_crd.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_with_deletions(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    dep = make_deployment("app1")
    mock_storage.download.return_value = _make_archive_bytes([dep])
    mock_k8s.get_deployment_crd_raw = AsyncMock(return_value=None)
    # Cluster has app1 + app2, backup only has app1
    mock_k8s.get_all_deployment_crds = AsyncMock(
        return_value=[make_raw_crd("app1"), make_raw_crd("app2")]
    )

    req = RestoreRequest(backup_id="b1", include_deletions=True)
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "completed"
    assert resp.results is not None
    actions = {r.name: r.action for r in resp.results}
    assert actions["app1"] == "created"
    assert actions["app2"] == "deleted"


@pytest.mark.asyncio
async def test_restore_empty_backup_with_deletions_refused(
    mock_storage: AsyncMock,
    backup_service: BackupService,
    mock_k8s: MagicMock,
    mock_settings: MagicMock,
) -> None:
    mock_storage.download.return_value = _make_archive_bytes([])

    req = RestoreRequest(backup_id="b1", include_deletions=True)
    with patch(K8S, mock_k8s), patch(SETTINGS, mock_settings):
        resp = await backup_service.restore_backup(req)

    assert resp.status == "failed"
    assert resp.error is not None and "zero entries" in resp.error.lower()
