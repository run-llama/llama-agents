"""S3-compatible build artifact storage backend."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import IO, Any

from botocore.exceptions import ClientError
from llama_agents.control_plane.storage import S3ObjectStorage

logger = logging.getLogger(__name__)


@dataclass
class ArtifactInfo:
    """Metadata about a build artifact in S3."""

    deployment_name: str
    build_id: str
    timestamp: datetime
    size_bytes: int


class BuildArtifactStorage(S3ObjectStorage):
    """Upload, download, list, and delete build artifacts in S3-compatible storage."""

    class NotFoundError(Exception):
        """Raised when an artifact is not found in S3."""

    def __init__(
        self,
        bucket: str,
        endpoint_url: str | None = None,
        region: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        key_prefix: str = "builds",
        unsigned: bool = False,
    ) -> None:
        super().__init__(
            bucket=bucket,
            endpoint_url=endpoint_url,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            key_prefix=key_prefix,
            unsigned=unsigned,
        )

    def _key(self, deployment_name: str, build_id: str) -> str:
        return f"{self._key_prefix}/{deployment_name}/{build_id}.tar.gz"

    async def upload_artifact(
        self, deployment_name: str, build_id: str, data: bytes
    ) -> None:
        """Upload a build artifact to S3."""
        async with self._client() as client:
            await client.put_object(
                Bucket=self._bucket,
                Key=self._key(deployment_name, build_id),
                Body=data,
            )

    async def upload_artifact_fileobj(
        self, deployment_name: str, build_id: str, fileobj: IO[Any]
    ) -> None:
        """Upload a build artifact to S3 from a file-like object."""
        async with self._client() as client:
            await client.upload_fileobj(
                fileobj,
                self._bucket,
                self._key(deployment_name, build_id),
            )

    async def download_artifact_streaming(
        self, deployment_name: str, build_id: str, chunk_size: int = 65536
    ) -> tuple[int, AsyncIterator[bytes]]:
        """Stream a build artifact from S3.

        Returns (content_length, async_iterator_of_chunks).
        Raises NotFoundError if the artifact does not exist.

        The caller must consume the iterator within the same context — the S3
        client session stays open until the iterator is exhausted.
        """
        client_cm = self._client()
        client = await client_cm.__aenter__()
        try:
            response = await client.get_object(
                Bucket=self._bucket,
                Key=self._key(deployment_name, build_id),
            )
        except ClientError as e:
            await client_cm.__aexit__(type(e), e, e.__traceback__)
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                raise self.NotFoundError(
                    f"Artifact not found: {deployment_name}/{build_id}"
                ) from e
            raise

        content_length: int = response["ContentLength"]
        body = response["Body"]

        async def _stream() -> AsyncIterator[bytes]:
            try:
                async for chunk in body.iter_chunks(chunk_size):
                    yield chunk
            finally:
                await client_cm.__aexit__(None, None, None)

        return content_length, _stream()

    async def artifact_exists(self, deployment_name: str, build_id: str) -> bool:
        """Check if a build artifact exists in S3."""
        async with self._client() as client:
            try:
                await client.head_object(
                    Bucket=self._bucket,
                    Key=self._key(deployment_name, build_id),
                )
                return True
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "404":
                    return False
                raise

    async def delete_artifact(self, deployment_name: str, build_id: str) -> None:
        """Delete a build artifact from S3."""
        async with self._client() as client:
            await client.delete_object(
                Bucket=self._bucket,
                Key=self._key(deployment_name, build_id),
            )

    async def list_artifacts(self, deployment_name: str) -> list[ArtifactInfo]:
        """List all build artifacts for a deployment, sorted by timestamp descending."""
        prefix = f"{self._key_prefix}/{deployment_name}/"
        async with self._client() as client:
            paginator = client.get_paginator("list_objects_v2")
            artifacts: list[ArtifactInfo] = []
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key")
                    if key is None or not key.endswith(".tar.gz"):
                        continue
                    last_modified = obj.get("LastModified")
                    size = obj.get("Size")
                    if last_modified is None or size is None:
                        continue
                    build_id = key.removeprefix(prefix).removesuffix(".tar.gz")
                    artifacts.append(
                        ArtifactInfo(
                            deployment_name=deployment_name,
                            build_id=build_id,
                            timestamp=last_modified,
                            size_bytes=size,
                        )
                    )
            artifacts.sort(key=lambda a: a.timestamp, reverse=True)
            return artifacts

    async def delete_all_artifacts(self, deployment_name: str) -> int:
        """Delete all build artifacts for a deployment. Returns count of deleted artifacts."""
        artifacts = await self.list_artifacts(deployment_name)
        for artifact in artifacts:
            await self.delete_artifact(deployment_name, artifact.build_id)
        return len(artifacts)
