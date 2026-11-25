"""Backup archive format: create and read .tar.gz archives of LlamaDeployment CRs and secrets."""

from __future__ import annotations

import io
import json
import tarfile
from dataclasses import dataclass, field
from typing import Any

import yaml

from .encryption import decrypt, encrypt

# Metadata keys to preserve when cleaning resources for backup.
# Everything else (resourceVersion, uid, creationTimestamp, generation,
# managedFields, selfLink, deletionTimestamp, etc.) is dropped automatically.
_CRD_METADATA_KEEP = {"name", "namespace", "labels", "annotations"}
_SECRET_METADATA_KEEP = {"name", "namespace", "labels", "annotations", "finalizers"}

# Annotation prefixes added by the cluster or operator — stripped even though
# we keep the annotations dict overall.
_SYSTEM_ANNOTATION_PREFIXES = (
    "kubectl.kubernetes.io/",
    "deploy.llamaindex.ai/",
)


@dataclass
class BackupManifest:
    version: int
    timestamp: str
    namespace: str
    deployment_count: int
    encrypted: bool
    # True when Secret data was left out because there was no encryption password.
    secrets_omitted: bool = False


@dataclass
class BackupEntry:
    name: str
    cr: dict[str, Any]
    secret: dict[str, str] | None = None
    generation: int | None = None


@dataclass
class BackupContents:
    manifest: BackupManifest
    entries: list[BackupEntry] = field(default_factory=list)


def clean_crd_metadata(doc: dict[str, Any]) -> dict[str, Any]:
    """Remove cluster-specific metadata from a CRD dict.

    Uses an allowlist of metadata keys to keep — any new system-added fields
    are automatically excluded without updating a blocklist.
    """
    return _clean_metadata(doc, keep_keys=_CRD_METADATA_KEEP)


def clean_secret_metadata(doc: dict[str, Any]) -> dict[str, Any]:
    """Remove cluster-specific metadata from a Secret dict."""
    return _clean_metadata(doc, keep_keys=_SECRET_METADATA_KEEP)


def _clean_metadata(
    doc: dict[str, Any],
    *,
    keep_keys: set[str],
) -> dict[str, Any]:
    """Strip cluster-specific metadata, keeping only allowlisted keys.

    This is safer than a blocklist because new system-added metadata fields
    (e.g. a future ownerReferences or managedFields variant) are automatically
    excluded without code changes.
    """
    doc.pop("status", None)

    meta = doc.get("metadata", {})
    for key in list(meta):
        if key not in keep_keys:
            del meta[key]

    # Strip system annotations even though we keep the annotations dict
    annotations = meta.get("annotations", {})
    for key in list(annotations):
        if any(key.startswith(p) for p in _SYSTEM_ANNOTATION_PREFIXES):
            del annotations[key]
    if not annotations:
        meta.pop("annotations", None)

    return doc


def create_backup_archive(
    deployments: list[dict[str, Any]],
    secrets: dict[str, dict[str, str]],
    namespace: str,
    timestamp: str,
    encryption_password: str | None = None,
    generations: dict[str, int] | None = None,
    allow_plaintext_secrets: bool = False,
) -> bytes:
    """Create a .tar.gz backup archive in memory.

    Secret data (deployment API keys and app env secrets) is written only encrypted.
    Without a password it is left out and the manifest says so, unless
    ``allow_plaintext_secrets`` opts in to the old plaintext ``.secret.yaml`` files.

    Args:
        deployments: List of cleaned CRD dicts.
        secrets: Map of deployment name to decoded secret key-value data.
        namespace: K8s namespace the backup was taken from.
        timestamp: ISO-8601 timestamp string.
        encryption_password: If set, encrypt secret data with this password.
        allow_plaintext_secrets: Write secrets unencrypted when there is no password.

    Returns:
        Bytes of the .tar.gz archive.
    """
    buf = io.BytesIO()
    omit_secrets = not encryption_password and not allow_plaintext_secrets

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        # Write manifest
        manifest = {
            "version": 1,
            "timestamp": timestamp,
            "namespace": namespace,
            "deployment_count": len(deployments),
            "encrypted": encryption_password is not None,
            "secrets_omitted": omit_secrets and bool(secrets),
        }
        _add_bytes_to_tar(tar, "manifest.json", json.dumps(manifest, indent=2).encode())

        for cr in deployments:
            name = cr.get("metadata", {}).get("name", "unknown")

            # Write cleaned CR as YAML
            cr_yaml = yaml.dump(cr, default_flow_style=False).encode()
            _add_bytes_to_tar(tar, f"{name}.yaml", cr_yaml)

            # Write secret data if available
            secret_data = secrets.get(name)
            if secret_data is not None and not omit_secrets:
                secret_yaml = yaml.dump(secret_data, default_flow_style=False).encode()
                if encryption_password:
                    encrypted = encrypt(secret_yaml, encryption_password)
                    _add_bytes_to_tar(tar, f"{name}.secret.enc", encrypted)
                else:
                    _add_bytes_to_tar(tar, f"{name}.secret.yaml", secret_yaml)

            # Write generation metadata if available
            if generations and name in generations:
                meta_json = json.dumps({"generation": generations[name]}).encode()
                _add_bytes_to_tar(tar, f"{name}.meta.json", meta_json)

    return buf.getvalue()


def read_backup_archive(
    data: bytes,
    encryption_password: str | None = None,
) -> BackupContents:
    """Read and parse a .tar.gz backup archive.

    Args:
        data: Raw bytes of the archive.
        encryption_password: Password for decrypting secret files.

    Returns:
        BackupContents with manifest and entries.

    Raises:
        ValueError: If manifest is missing or version is unsupported.
        cryptography.exceptions.InvalidTag: If decryption fails.
    """
    buf = io.BytesIO(data)
    cr_files: dict[str, dict[str, Any]] = {}
    secret_files: dict[str, dict[str, str]] = {}
    meta_files: dict[str, dict[str, Any]] = {}
    manifest_data: dict[str, Any] | None = None

    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            content = f.read()
            name = member.name

            if name == "manifest.json":
                manifest_data = json.loads(content)
            elif name.endswith(".secret.enc"):
                deploy_name = name.removesuffix(".secret.enc")
                if encryption_password is None:
                    raise ValueError(
                        f"Archive contains encrypted secrets but no password provided "
                        f"(file: {name})"
                    )
                decrypted = decrypt(content, encryption_password)
                secret_files[deploy_name] = yaml.safe_load(decrypted)
            elif name.endswith(".meta.json"):
                deploy_name = name.removesuffix(".meta.json")
                meta_files[deploy_name] = json.loads(content)
            elif name.endswith(".secret.yaml"):
                deploy_name = name.removesuffix(".secret.yaml")
                secret_files[deploy_name] = yaml.safe_load(content)
            elif name.endswith(".yaml"):
                deploy_name = name.removesuffix(".yaml")
                cr_files[deploy_name] = yaml.safe_load(content)

    if manifest_data is None:
        raise ValueError("Archive missing manifest.json")

    if manifest_data.get("version", 0) != 1:
        raise ValueError(f"Unsupported archive version: {manifest_data.get('version')}")

    manifest = BackupManifest(
        version=manifest_data["version"],
        timestamp=manifest_data["timestamp"],
        namespace=manifest_data["namespace"],
        deployment_count=manifest_data["deployment_count"],
        encrypted=manifest_data["encrypted"],
        secrets_omitted=manifest_data.get("secrets_omitted", False),
    )

    entries = []
    for name, cr in cr_files.items():
        meta = meta_files.get(name, {})
        entries.append(
            BackupEntry(
                name=name,
                cr=cr,
                secret=secret_files.get(name),
                generation=meta.get("generation"),
            )
        )

    return BackupContents(manifest=manifest, entries=entries)


def _add_bytes_to_tar(tar: tarfile.TarFile, name: str, data: bytes) -> None:
    """Add a bytes buffer as a file to a tar archive."""
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    tar.addfile(info, io.BytesIO(data))
