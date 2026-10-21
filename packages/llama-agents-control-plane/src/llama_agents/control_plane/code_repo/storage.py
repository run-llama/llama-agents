"""S3 storage for bare git repositories as tarballs."""

from __future__ import annotations

import io
import logging
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path

from botocore.exceptions import ClientError
from dulwich import porcelain
from dulwich.objects import ObjectID
from dulwich.porcelain import gc as dulwich_gc
from dulwich.repo import Repo
from llama_agents.core.config import DEFAULT_DEPLOYMENT_FILE_PATH
from llama_agents.core.deployment_config import (
    read_deployment_config,
    resolve_config_parent,
)
from llama_agents.core.git.git_util import GitAccessError, resolve_ref_in_repo
from llama_agents.core.path_util import validate_path_traversal
from llama_agents.core.ui_build import ui_build_output_path
from starlette.concurrency import run_in_threadpool

from ..storage import S3ObjectStorage

logger = logging.getLogger(__name__)


class CodeRepoStorage(S3ObjectStorage):
    """Stores bare git repositories as tarballs in S3.

    Each deployment gets one bare repo stored as a gzipped tarball at:
        {key_prefix}/{deployment_id}/repo.tar.gz
    """

    def _s3_key(self, deployment_id: str) -> str:
        if self._key_prefix:
            return f"{self._key_prefix}/{deployment_id}/repo.tar.gz"
        return f"{deployment_id}/repo.tar.gz"

    async def download_repo(self, deployment_id: str) -> Path | None:
        """Download and extract repo tarball from S3 to a temp dir.

        Returns the path to the bare repo directory, or None if no repo exists.
        The caller is responsible for cleaning up the temp dir.
        """
        key = self._s3_key(deployment_id)
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"code-repo-{deployment_id}-"))
        tar_path = tmp_dir / "repo.tar.gz"
        try:
            async with self._client() as client:
                response = await client.get_object(Bucket=self._bucket, Key=key)
                with open(tar_path, "wb") as f:
                    async for chunk in response["Body"].iter_chunks():
                        f.write(chunk)
            with tarfile.open(tar_path, "r:gz") as tar:
                if sys.version_info >= (3, 12):
                    tar.extractall(path=tmp_dir, filter="data")
                else:
                    # filter param added in 3.12; safe here since we
                    # create the tarballs ourselves in upload_repo.
                    tar.extractall(path=tmp_dir)
            tar_path.unlink()
            repo_path = tmp_dir / "repo"
            if not repo_path.exists():
                logger.error("Tarball for %s missing 'repo' directory", deployment_id)
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return None
            return repo_path
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey"):
                # No repo has been pushed yet
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return None
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise
        except Exception:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

    async def upload_repo(self, deployment_id: str, repo_path: Path) -> None:
        """Run dulwich GC on the repo, tar+gzip it, and upload to S3."""
        await run_in_threadpool(dulwich_gc, str(repo_path))

        key = self._s3_key(deployment_id)
        tar_path = repo_path.parent / "repo-upload.tar.gz"
        try:
            await run_in_threadpool(self._create_tarball, repo_path, tar_path)
            with open(tar_path, "rb") as f:
                async with self._client() as client:
                    await client.upload_fileobj(f, self._bucket, key)
            logger.info(
                "Uploaded repo for deployment %s (%d bytes)",
                deployment_id,
                tar_path.stat().st_size,
            )
        finally:
            if tar_path.exists():
                tar_path.unlink()

    async def delete_repo(self, deployment_id: str) -> None:
        """Delete the repo tarball from S3."""
        key = self._s3_key(deployment_id)
        async with self._client() as client:
            await client.delete_object(Bucket=self._bucket, Key=key)
        logger.info("Deleted repo for deployment %s", deployment_id)

    async def repo_exists(self, deployment_id: str) -> bool:
        """Check if a repo tarball exists in S3."""
        key = self._s3_key(deployment_id)
        try:
            async with self._client() as client:
                await client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code", "") in ("404", "NoSuchKey"):
                return False
            raise

    @staticmethod
    def _create_tarball(repo_path: Path, tar_path: Path) -> None:
        """Create a gzipped tarball of the repo directory."""
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(str(repo_path), arcname="repo")

    async def resolve_ref(self, deployment_id: str, git_ref: str) -> str | None:
        """Resolve a branch, tag, or commit SHA from the S3-stored bare repo.

        Downloads the repo tarball, reads the ref, and cleans up.
        Returns the SHA hex string, or None if the ref or repo doesn't exist.
        """
        repo_path = await self.download_repo(deployment_id)
        if repo_path is None:
            return None
        try:
            with Repo(str(repo_path)) as repo:
                try:
                    target_sha = resolve_ref_in_repo(repo, git_ref)
                except GitAccessError:
                    return None
                if target_sha is None:
                    return None
                # Only accept commit objects — reject trees, blobs, etc.
                try:
                    obj = repo.get_object(ObjectID(target_sha))
                except KeyError:
                    return None
                if obj.type_name != b"commit":
                    return None
                return target_sha.decode()
        finally:
            shutil.rmtree(repo_path.parent, ignore_errors=True)

    async def get_ui_build_output_path(
        self,
        deployment_id: str,
        git_sha: str,
        deployment_file_path: str | None,
    ) -> Path | None:
        """Discover UI assets from the exact pushed commit being deployed."""
        repo_path = await self.download_repo(deployment_id)
        if repo_path is None:
            raise GitAccessError(f"No code repository for deployment {deployment_id}")
        try:
            return await run_in_threadpool(
                self._read_ui_build_output_path,
                repo_path,
                git_sha,
                deployment_file_path,
            )
        finally:
            shutil.rmtree(repo_path.parent, ignore_errors=True)

    @staticmethod
    def _read_ui_build_output_path(
        repo_path: Path, git_sha: str, deployment_file_path: str | None
    ) -> Path | None:
        with tempfile.TemporaryDirectory(prefix="code-repo-ui-") as temp_dir:
            root = Path(temp_dir)
            # This is a trusted local download, not a user-supplied clone URL.
            with porcelain.clone(
                str(repo_path), str(root), checkout=False, errstream=io.BytesIO()
            ) as repo:
                porcelain.reset(repo, "hard", git_sha.encode())
            config_path = root / (deployment_file_path or DEFAULT_DEPLOYMENT_FILE_PATH)
            validate_path_traversal(config_path, root, "deployment_file_path")
            config = read_deployment_config(root, config_path)
            config_parent = resolve_config_parent(root, config_path)
            output = ui_build_output_path(config_parent, config)
            return (config_parent / output).relative_to(root) if output else None

    @staticmethod
    def init_bare_repo(deployment_id: str) -> Path:
        """Create an empty bare repo in a temp directory.

        Returns the path to the bare repo. The caller is responsible for
        cleaning up the temp dir.
        """
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"code-repo-{deployment_id}-"))
        repo_path = tmp_dir / "repo"
        repo_path.mkdir()
        Repo.init_bare(str(repo_path))
        return repo_path
