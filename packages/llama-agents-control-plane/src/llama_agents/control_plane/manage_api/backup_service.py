"""Concrete backup/restore service implementation."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from llama_agents.core import schema

from .. import k8s_client
from ..backup.archive import (
    BackupContents,
    clean_crd_metadata,
    create_backup_archive,
    read_backup_archive,
)
from ..backup.storage import BackupInfo, S3BackupStorage, generate_backup_id
from ..settings import settings

logger = logging.getLogger(__name__)


def _paired_secret_name(deployment_name: str) -> str:
    return f"{deployment_name}-secrets"


class BackupService:
    def __init__(self, storage: S3BackupStorage) -> None:
        self._storage = storage

    async def create_backup(self) -> schema.BackupResponse:
        backup_id = generate_backup_id()
        return await self._perform_backup(backup_id)

    async def _perform_backup(self, backup_id: str) -> schema.BackupResponse:
        try:
            timestamp = datetime.now(timezone.utc).isoformat()

            # Fetch all CRDs
            raw_crds = await k8s_client.get_all_deployment_crds()

            # Clean metadata and collect generations
            cleaned_crds: list[dict] = []
            generations: dict[str, int] = {}
            names: list[str] = []

            for crd in raw_crds:
                name = crd.get("metadata", {}).get("name", "")
                names.append(name)
                gen = crd.get("metadata", {}).get("generation")
                if gen is not None:
                    generations[name] = int(gen)

                cleaned = clean_crd_metadata(crd)
                cleaned_crds.append(cleaned)

            # Fetch secrets concurrently
            secret_results = await asyncio.gather(
                *(k8s_client.get_secret_data(_paired_secret_name(n)) for n in names)
            )
            secrets: dict[str, dict[str, str]] = {}
            for name, secret_data in zip(names, secret_results):
                if secret_data is not None:
                    secrets[name] = secret_data

            # Create archive
            namespace = k8s_client.get_namespace()
            archive_data = create_backup_archive(
                deployments=cleaned_crds,
                secrets=secrets,
                namespace=namespace,
                timestamp=timestamp,
                encryption_password=settings.backup_encryption_password,
                generations=generations,
                allow_plaintext_secrets=settings.backup_allow_plaintext_secrets,
            )
            if (
                secrets
                and not settings.backup_encryption_password
                and not settings.backup_allow_plaintext_secrets
            ):
                logger.warning(
                    "Backup %s omits secrets for %d deployments: set BACKUP_ENCRYPTION_PASSWORD "
                    "to back them up encrypted",
                    backup_id,
                    len(secrets),
                )

            # Upload to S3
            await self._storage.upload(backup_id, archive_data)

            return schema.BackupResponse(
                backup_id=backup_id,
                status="completed",
                timestamp=datetime.fromisoformat(timestamp),
                deployment_count=len(cleaned_crds),
                size_bytes=len(archive_data),
            )
        except Exception as e:
            logger.exception("Backup %s failed", backup_id)
            return schema.BackupResponse(
                backup_id=backup_id,
                status="failed",
                error=str(e),
            )

    async def list_backups(self) -> schema.BackupListResponse:
        infos = await self._storage.list_backups()
        return schema.BackupListResponse(
            backups=[self._info_to_response(info) for info in infos]
        )

    async def get_backup(self, backup_id: str) -> schema.BackupResponse:
        info = await self._storage.get_info(backup_id)
        if info is not None:
            return self._info_to_response(info)
        return schema.BackupResponse(
            backup_id=backup_id,
            status="failed",
            error="Backup not found",
        )

    async def delete_backup(self, backup_id: str) -> schema.BackupResponse:
        if await self._storage.get_info(backup_id) is None:
            return schema.BackupResponse(
                backup_id=backup_id,
                status="failed",
                error="Backup not found",
            )
        await self._storage.delete(backup_id)
        return schema.BackupResponse(
            backup_id=backup_id,
            status="deleted",
        )

    async def restore_backup(
        self, request: schema.RestoreRequest
    ) -> schema.RestoreResponse:
        return await self._perform_restore(request)

    async def _perform_restore(
        self, request: schema.RestoreRequest
    ) -> schema.RestoreResponse:
        try:
            # Download archive
            archive_data = await self._storage.download(request.backup_id)
            contents = read_backup_archive(
                archive_data, settings.backup_encryption_password
            )

            # Safety check: refuse include_deletions with empty backup
            if request.include_deletions and len(contents.entries) == 0:
                return schema.RestoreResponse(
                    backup_id=request.backup_id,
                    status="failed",
                    error="Refusing to delete all deployments: backup contains zero entries",
                )

            results = await self._restore_entries(contents, request)

            # Handle deletions
            if request.include_deletions:
                deletion_results = await self._handle_deletions(contents)
                results.extend(deletion_results)

            return schema.RestoreResponse(
                backup_id=request.backup_id,
                status="completed",
                results=results,
            )
        except Exception as e:
            logger.exception("Restore from %s failed", request.backup_id)
            return schema.RestoreResponse(
                backup_id=request.backup_id,
                status="failed",
                error=str(e),
            )

    async def _restore_entries(
        self,
        contents: BackupContents,
        request: schema.RestoreRequest,
    ) -> list[schema.RestoreDeploymentResult]:
        results: list[schema.RestoreDeploymentResult] = []

        for entry in contents.entries:
            try:
                existing = await k8s_client.get_deployment_crd_raw(entry.name)

                if existing is not None:
                    # Safety: refuse to overwrite a deployment belonging to a different project
                    existing_project = existing.get("spec", {}).get("projectId")
                    entry_project = entry.cr.get("spec", {}).get("projectId")
                    if (
                        existing_project
                        and entry_project
                        and existing_project != entry_project
                    ):
                        results.append(
                            schema.RestoreDeploymentResult(
                                name=entry.name,
                                action="failed",
                                error=f"Project mismatch: existing deployment belongs to project "
                                f"'{existing_project}', backup entry belongs to '{entry_project}'",
                            )
                        )
                        continue

                    if request.conflict_mode == "skip":
                        results.append(
                            schema.RestoreDeploymentResult(
                                name=entry.name, action="skipped"
                            )
                        )
                        continue

                    if request.conflict_mode == "overwrite-if-newer":
                        existing_gen = existing.get("metadata", {}).get("generation")
                        if (
                            existing_gen is not None
                            and entry.generation is not None
                            and int(existing_gen) > entry.generation
                        ):
                            results.append(
                                schema.RestoreDeploymentResult(
                                    name=entry.name, action="skipped"
                                )
                            )
                            continue

                # Apply secret first (if present)
                if entry.secret is not None:
                    secret_name = _paired_secret_name(entry.name)
                    await k8s_client.apply_secret(secret_name, entry.secret)

                # Apply CRD
                await k8s_client.apply_deployment_crd(entry.cr)

                action = "updated" if existing is not None else "created"
                results.append(
                    schema.RestoreDeploymentResult(name=entry.name, action=action)
                )

            except Exception as e:
                logger.error("Failed to restore deployment %s: %s", entry.name, e)
                results.append(
                    schema.RestoreDeploymentResult(
                        name=entry.name, action="failed", error=str(e)
                    )
                )

        return results

    async def _handle_deletions(
        self, contents: BackupContents
    ) -> list[schema.RestoreDeploymentResult]:
        """Delete deployments present in cluster but absent from backup."""
        results: list[schema.RestoreDeploymentResult] = []
        backup_names = {entry.name for entry in contents.entries}

        current_crds = await k8s_client.get_all_deployment_crds()
        for crd in current_crds:
            name = crd.get("metadata", {}).get("name", "")
            if name not in backup_names:
                try:
                    await k8s_client.delete_deployment_crd(name)
                    results.append(
                        schema.RestoreDeploymentResult(name=name, action="deleted")
                    )
                except Exception as e:
                    logger.error("Failed to delete deployment %s: %s", name, e)
                    results.append(
                        schema.RestoreDeploymentResult(
                            name=name, action="failed", error=str(e)
                        )
                    )

        return results

    @staticmethod
    def _info_to_response(info: BackupInfo) -> schema.BackupResponse:
        return schema.BackupResponse(
            backup_id=info.backup_id,
            status="completed",
            timestamp=info.timestamp,
            size_bytes=info.size_bytes,
        )


def create_backup_service() -> BackupService | None:
    """Create a BackupService if S3 is configured, else return None."""
    if not settings.s3_bucket:
        return None

    storage = S3BackupStorage(
        bucket=settings.s3_bucket,
        endpoint_url=settings.s3_endpoint_url,
        region=settings.s3_region,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        key_prefix=settings.backup_s3_key_prefix,
        unsigned=settings.s3_unsigned,
    )
    return BackupService(storage)


backup_service = create_backup_service()
