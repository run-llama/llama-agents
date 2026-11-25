"""Tests for backup archive create/read and metadata cleaning."""

from __future__ import annotations

import io
import tarfile
from typing import Any

import pytest
from llama_agents.control_plane.backup.archive import (
    clean_crd_metadata,
    clean_secret_metadata,
    create_backup_archive,
    read_backup_archive,
)

from .conftest import make_deployment

# ---------------------------------------------------------------------------
# Archive round-trip
# ---------------------------------------------------------------------------


def test_round_trip() -> None:
    deployments = [make_deployment("app1"), make_deployment("app2")]
    secrets = {"app1": {"API_KEY": "secret123"}}
    archive = create_backup_archive(
        deployments=deployments,
        secrets=secrets,
        namespace="default",
        timestamp="2025-01-01T00:00:00Z",
        allow_plaintext_secrets=True,
    )
    contents = read_backup_archive(archive)
    assert contents.manifest.version == 1
    assert contents.manifest.namespace == "default"
    assert contents.manifest.deployment_count == 2
    assert contents.manifest.encrypted is False
    assert len(contents.entries) == 2

    by_name = {e.name: e for e in contents.entries}
    assert by_name["app1"].cr["spec"]["image"] == "registry/app1:latest"
    assert by_name["app1"].secret == {"API_KEY": "secret123"}
    assert by_name["app2"].secret is None


def test_encrypted_archive_round_trip() -> None:
    deployments = [make_deployment("secure")]
    secrets = {"secure": {"TOKEN": "abc"}}
    archive = create_backup_archive(
        deployments=deployments,
        secrets=secrets,
        namespace="ns",
        timestamp="2025-06-01T00:00:00Z",
        encryption_password="my-password",
    )
    contents = read_backup_archive(archive, encryption_password="my-password")
    assert contents.manifest.encrypted is True
    assert contents.entries[0].secret == {"TOKEN": "abc"}


def test_unencrypted_archive_round_trip() -> None:
    deployments = [make_deployment("plain")]
    secrets = {"plain": {"KEY": "val"}}
    archive = create_backup_archive(
        deployments=deployments,
        secrets=secrets,
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
        encryption_password=None,
        allow_plaintext_secrets=True,
    )
    contents = read_backup_archive(archive)
    assert contents.manifest.encrypted is False
    assert contents.entries[0].secret == {"KEY": "val"}


def test_unencrypted_archive_omits_secrets_by_default() -> None:
    deployments = [make_deployment("plain")]
    archive = create_backup_archive(
        deployments=deployments,
        secrets={"plain": {"LLAMA_CLOUD_API_KEY": "llx-secret"}},
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
    )
    blob = b""
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        names = tar.getnames()
        for member in tar.getmembers():
            extracted = tar.extractfile(member)
            if extracted is not None:
                blob += extracted.read()
    assert "plain.secret.yaml" not in names
    assert b"llx-secret" not in blob
    contents = read_backup_archive(archive)
    assert contents.manifest.secrets_omitted is True
    assert contents.entries[0].secret is None


def test_empty_deployments() -> None:
    archive = create_backup_archive(
        deployments=[],
        secrets={},
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
    )
    contents = read_backup_archive(archive)
    assert contents.manifest.deployment_count == 0
    assert contents.entries == []


def test_archive_extractable_with_tarfile() -> None:
    deployments = [make_deployment("d1")]
    secrets = {"d1": {"K": "V"}}
    archive = create_backup_archive(
        deployments=deployments,
        secrets=secrets,
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
        allow_plaintext_secrets=True,
    )
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        names = tar.getnames()
    assert "manifest.json" in names
    assert "d1.yaml" in names
    assert "d1.secret.yaml" in names


def test_encrypted_archive_has_enc_file() -> None:
    deployments = [make_deployment("d1")]
    secrets = {"d1": {"K": "V"}}
    archive = create_backup_archive(
        deployments=deployments,
        secrets=secrets,
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
        encryption_password="pw",
    )
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        names = tar.getnames()
    assert "d1.secret.enc" in names
    assert "d1.secret.yaml" not in names


def test_missing_manifest_raises() -> None:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        data = b"apiVersion: v1\nkind: Dummy\n"
        info = tarfile.TarInfo(name="something.yaml")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    with pytest.raises(ValueError, match="missing manifest"):
        read_backup_archive(buf.getvalue())


def test_encrypted_archive_without_password_raises() -> None:
    deployments = [make_deployment("secure")]
    secrets = {"secure": {"S": "val"}}
    archive = create_backup_archive(
        deployments=deployments,
        secrets=secrets,
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
        encryption_password="pw",
    )
    with pytest.raises(ValueError, match="no password provided"):
        read_backup_archive(archive, encryption_password=None)


def test_generation_stored_in_archive() -> None:
    deployments = [make_deployment("app1"), make_deployment("app2")]
    archive = create_backup_archive(
        deployments=deployments,
        secrets={},
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
        generations={"app1": 5, "app2": 3},
    )
    contents = read_backup_archive(archive)
    by_name = {e.name: e for e in contents.entries}
    assert by_name["app1"].generation == 5
    assert by_name["app2"].generation == 3


def test_generation_none_when_not_provided() -> None:
    deployments = [make_deployment("app1")]
    archive = create_backup_archive(
        deployments=deployments,
        secrets={},
        namespace="ns",
        timestamp="2025-01-01T00:00:00Z",
    )
    contents = read_backup_archive(archive)
    assert contents.entries[0].generation is None


# ---------------------------------------------------------------------------
# Metadata cleaning
# ---------------------------------------------------------------------------


def test_clean_crd_metadata_strips_cluster_fields() -> None:
    doc: dict[str, Any] = {
        "metadata": {
            "name": "my-app",
            "resourceVersion": "12345",
            "uid": "abc-def",
            "creationTimestamp": "2025-01-01T00:00:00Z",
            "generation": 3,
            "managedFields": [{"manager": "kubectl"}],
            "selfLink": "/apis/foo",
            "deletionTimestamp": "2025-01-02T00:00:00Z",
            "deletionGracePeriodSeconds": 30,
            "finalizers": ["some-finalizer"],
            "annotations": {
                "kubectl.kubernetes.io/last-applied": "{}",
                "deploy.llamaindex.ai/secret-hash": "abc123",
                "keep-this": "yes",
            },
        },
        "status": {"ready": True},
        "spec": {"image": "img"},
    }
    cleaned = clean_crd_metadata(doc)
    meta = cleaned["metadata"]
    assert meta["name"] == "my-app"
    for key in [
        "resourceVersion",
        "uid",
        "creationTimestamp",
        "generation",
        "managedFields",
        "selfLink",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
        "finalizers",
    ]:
        assert key not in meta
    assert "status" not in cleaned
    assert "keep-this" in meta["annotations"]
    assert "kubectl.kubernetes.io/last-applied" not in meta["annotations"]
    assert "deploy.llamaindex.ai/secret-hash" not in meta["annotations"]


def test_clean_crd_removes_empty_annotations() -> None:
    doc: dict[str, Any] = {
        "metadata": {
            "name": "x",
            "annotations": {
                "kubectl.kubernetes.io/something": "val",
            },
        },
    }
    cleaned = clean_crd_metadata(doc)
    assert "annotations" not in cleaned["metadata"]


def test_clean_secret_metadata_strips_owner_references() -> None:
    doc: dict[str, Any] = {
        "metadata": {
            "name": "my-secret",
            "ownerReferences": [{"kind": "Deployment", "name": "app"}],
            "resourceVersion": "999",
            "uid": "uid-1",
            "creationTimestamp": "2025-01-01T00:00:00Z",
            "generation": 1,
            "managedFields": [],
            "selfLink": "/api/v1/secrets/x",
            "annotations": {
                "kubectl.kubernetes.io/last-applied": "{}",
                "custom-annotation": "keep",
            },
        },
    }
    cleaned = clean_secret_metadata(doc)
    meta = cleaned["metadata"]
    assert "ownerReferences" not in meta
    assert "resourceVersion" not in meta
    assert "uid" not in meta
    assert meta["name"] == "my-secret"
    assert "custom-annotation" in meta["annotations"]
    assert "kubectl.kubernetes.io/last-applied" not in meta["annotations"]


def test_clean_secret_does_not_strip_finalizers() -> None:
    doc: dict[str, Any] = {
        "metadata": {
            "name": "sec",
            "finalizers": ["keep-me"],
        },
    }
    cleaned = clean_secret_metadata(doc)
    assert "finalizers" in cleaned["metadata"]


def test_clean_secret_strips_system_annotations() -> None:
    """Secret cleaning strips system annotation prefixes just like CRD cleaning."""
    doc: dict[str, Any] = {
        "metadata": {
            "name": "sec",
            "annotations": {
                "deploy.llamaindex.ai/secret-hash": "hash123",
                "custom-annotation": "keep",
            },
        },
    }
    cleaned = clean_secret_metadata(doc)
    assert "deploy.llamaindex.ai/secret-hash" not in cleaned["metadata"].get(
        "annotations", {}
    )
    assert cleaned["metadata"]["annotations"]["custom-annotation"] == "keep"
