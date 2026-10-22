"""
Bootstraps an application from a remote github repository given environment variables.

Supports two modes of operation:

- **Init container**: If a pre-built artifact exists (LLAMA_DEPLOY_BUILD_ID is set),
  downloads and extracts it into the target directory. Otherwise, runs the full
  bootstrap (clone, install deps, build UI).
- **Build job**: Runs the full bootstrap, then packages the result into a tarball
  and uploads it to the Build API for S3 storage.
"""

import logging
import os
import tarfile
import time
from importlib.metadata import version as pkg_version
from pathlib import Path

import httpx
from llama_agents.appserver.configure_logging import setup_logging
from llama_agents.appserver.deployment_config_parser import get_deployment_config
from llama_agents.appserver.settings import (
    BootstrapSettings,
    configure_settings,
    settings,
)
from llama_agents.appserver.workflow_loader import (
    build_ui,
    inject_appserver_into_target,
    install_ui,
    load_environment_variables,
    validate_required_env_vars,
)
from llama_agents.core.git.git_util import clone_repo_sync as clone_repo

logger = logging.getLogger(__name__)

# Symlink at the app root pointing at the built UI directory. nginx serves it
# for deployments that have no spec.staticAssetsPath, so the artifact itself
# records where its UI lives.
UI_LINK_NAME = ".llama-ui"


def _extract_filter(member: tarfile.TarInfo, dest_path: str) -> tarfile.TarInfo:
    """Extraction filter like ``data`` but permits symlinks with absolute targets.

    Virtualenvs contain symlinks like ``.venv/bin/python -> /usr/local/bin/python3.12``.
    The built-in ``data`` filter rejects these (AbsoluteLinkError /
    LinkOutsideDestinationError).  We skip the link-target checks for symlinks
    while keeping every other safety check: path-traversal prevention, special-file
    rejection, mode clamping, and ownership reset.
    """
    if not member.issym():
        # Non-symlink entries: delegate entirely to data_filter.
        return tarfile.data_filter(member, dest_path)

    # --- Symlink-specific path: replicate data_filter sans link-target checks ---

    name = member.name

    # Strip leading slashes (same as data_filter step 2).
    if name.startswith(("/", os.sep)):
        name = member.path.lstrip("/" + os.sep)

    # Reject still-absolute names (Windows drive letters, etc.).
    if os.path.isabs(name):
        raise tarfile.AbsolutePathError(member)

    # Ensure the *entry itself* (not its target) resolves inside dest_path.
    dest_path = os.path.realpath(dest_path)
    target_path = os.path.realpath(os.path.join(dest_path, name), strict=False)
    if os.path.commonpath([target_path, dest_path]) != dest_path:
        raise tarfile.OutsideDestinationError(member, target_path)

    # Nullify ownership (same as data_filter) and normalise name if needed.
    if name != member.name:
        return member.replace(name=name, uid=0, gid=0, uname="", gname="", deep=False)
    return member.replace(uid=0, gid=0, uname="", gname="", deep=False)


def _download_and_extract_artifact(
    build_api_host: str,
    deployment_name: str,
    build_id: str,
    auth_token: str,
    target_dir: str,
) -> None:
    """Download a pre-built artifact from the Build API and extract into target_dir."""
    url = f"http://{build_api_host}/deployments/{deployment_name}/builds/{build_id}"
    logger.info("Downloading build artifact from %s", url)

    tarball_path = "/tmp/build-artifact-download.tar.gz"
    with httpx.stream(
        "GET",
        url,
        headers={"Authorization": f"Bearer {auth_token}"},
        timeout=600.0,
    ) as response:
        if response.status_code == 503:
            raise RuntimeError(
                "Build artifact storage not configured on the control plane. "
                "Cannot download build artifact. Ensure S3_BUCKET is set."
            )
        response.raise_for_status()
        with open(tarball_path, "wb") as f:
            for chunk in response.iter_bytes(chunk_size=65536):
                f.write(chunk)

    tarball_size = os.path.getsize(tarball_path)
    logger.info("Downloaded artifact: %.1f MB", tarball_size / (1024 * 1024))

    os.makedirs(target_dir, exist_ok=True)
    logger.info("Extracting artifact...")
    extract_start = time.monotonic()
    with tarfile.open(tarball_path, "r:*") as tf:
        tf.extractall(path=target_dir, filter=_extract_filter)
    extract_elapsed = time.monotonic() - extract_start
    logger.info("Extracted artifact into %s (%.1fs)", target_dir, extract_elapsed)
    os.remove(tarball_path)


_TARBALL_EXCLUDE_DIRS = {".git", "node_modules", "__pycache__", ".pnpm-store"}


def _create_tarball(source_dir: str, output_path: str) -> None:
    """Create an uncompressed tarball of the source directory.

    Excludes .git, node_modules, and __pycache__ directories.
    """
    logger.info("Creating tarball of %s -> %s", source_dir, output_path)

    def _tar_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
        parts = Path(tarinfo.name).parts
        if any(part in _TARBALL_EXCLUDE_DIRS for part in parts):
            return None
        return tarinfo

    with tarfile.open(output_path, "w:") as tf:
        tf.add(source_dir, arcname=".", filter=_tar_filter)


def _artifact_exists(
    build_api_host: str,
    deployment_name: str,
    build_id: str,
    auth_token: str,
) -> bool:
    """Check if a build artifact already exists in S3 via the Build API."""
    url = f"http://{build_api_host}/deployments/{deployment_name}/builds/{build_id}"
    try:
        response = httpx.head(
            url,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=30.0,
        )
        if response.status_code == 503:
            raise RuntimeError(
                "Build artifact storage not configured on the control plane. "
                "Ensure S3_BUCKET is set."
            )
        if response.status_code == 404:
            return False
        if response.status_code == 200:
            return True
        # Unexpected status — treat as an error
        raise RuntimeError(
            f"Unexpected status {response.status_code} checking artifact existence"
        )
    except httpx.HTTPError:
        logger.debug("Artifact existence check failed, will proceed with build")
        return False


def _upload_artifact(
    build_api_host: str,
    deployment_name: str,
    build_id: str,
    auth_token: str,
    tarball_path: str,
) -> None:
    """Upload a build artifact tarball to the Build API (streamed from disk)."""
    url = f"http://{build_api_host}/deployments/{deployment_name}/builds/{build_id}"
    logger.info("Uploading artifact to %s", url)

    size_bytes = os.path.getsize(tarball_path)
    size_mb = size_bytes / (1024 * 1024)
    logger.info("Artifact size: %.1f MB", size_mb)

    with open(tarball_path, "rb") as f:
        response = httpx.put(
            url,
            content=f,
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/x-tar",
                "Content-Length": str(size_bytes),
            },
            timeout=600.0,  # 10 minute timeout for large artifacts
        )
    if response.status_code == 503:
        raise RuntimeError(
            "Build artifact storage not configured on the control plane. "
            "Ensure S3_BUCKET is set."
        )
    response.raise_for_status()
    logger.info("Artifact uploaded successfully")


def bootstrap_app_from_repo(
    target_dir: str = "/opt/app",
) -> Path | None:
    """Prepare the app in target_dir. Returns the built UI directory, if any."""
    bootstrap_settings = BootstrapSettings()

    # Download mode: if BUILD_ID is set, download pre-built artifact instead of building
    if bootstrap_settings.build_id:
        if not bootstrap_settings.build_api_host:
            raise ValueError(
                "LLAMA_DEPLOY_BUILD_API_HOST is required when BUILD_ID is set"
            )
        if not bootstrap_settings.auth_token:
            raise ValueError("LLAMA_DEPLOY_AUTH_TOKEN is required when BUILD_ID is set")
        if not bootstrap_settings.deployment_name:
            raise ValueError(
                "LLAMA_DEPLOY_DEPLOYMENT_NAME is required when BUILD_ID is set"
            )
        logger.info(
            "Download mode: build_id=%s deployment=%s",
            bootstrap_settings.build_id,
            bootstrap_settings.deployment_name,
        )
        _download_and_extract_artifact(
            build_api_host=bootstrap_settings.build_api_host,
            deployment_name=bootstrap_settings.deployment_name,
            build_id=bootstrap_settings.build_id,
            auth_token=bootstrap_settings.auth_token,
            target_dir=target_dir,
        )
        return None

    # Full bootstrap: clone and build in-place
    repo_url = bootstrap_settings.repo_url
    if repo_url is None:
        raise ValueError("repo_url is required to bootstrap")
    clone_repo(
        repository_url=repo_url,
        git_ref=bootstrap_settings.git_ref,
        git_sha=bootstrap_settings.git_sha,
        basic_auth=bootstrap_settings.auth_token,
        dest_dir=target_dir,
        depth=1
        if bootstrap_settings.git_sha is None and bootstrap_settings.git_ref is not None
        else None,
    )
    # Ensure target_dir exists locally when running tests outside a container
    os.makedirs(target_dir, exist_ok=True)
    os.chdir(target_dir)
    configure_settings(
        app_root=Path(target_dir),
        deployment_file_path=Path(bootstrap_settings.deployment_file_path),
    )
    config = get_deployment_config()
    load_environment_variables(config, settings.resolved_config_parent)
    # Fail fast if required env vars are missing
    validate_required_env_vars(config)

    sdists = None
    if bootstrap_settings.bootstrap_sdists:
        sdists = [
            Path(bootstrap_settings.bootstrap_sdists) / f
            for f in os.listdir(bootstrap_settings.bootstrap_sdists)
        ]
        sdists = [f for f in sdists if f.is_file() and f.name.endswith(".tar.gz")]
        if not sdists:
            sdists = None

    # If a specific appserver version is requested and it differs from the
    # version bundled in this image, discard baked-in sdists so the install
    # step fetches the correct version from PyPI instead.
    target_version: str | None = bootstrap_settings.appserver_version
    if target_version and sdists:
        bundled_version = pkg_version("llama-agents-appserver")
        if bundled_version != target_version:
            logger.info(
                "Pinned appserver version %s differs from bundled %s; "
                "will fetch from PyPI instead of using sdists",
                target_version,
                bundled_version,
            )
            sdists = None

    # Use the explicit base path rather than relying on global settings so tests
    # can safely mock configure_settings without affecting call arguments.
    inject_appserver_into_target(
        config,
        settings.resolved_config_parent,
        sdists,
        target_version=target_version,
        auto_upgrade=False,
    )
    install_ui(config, settings.resolved_config_parent)
    return build_ui(settings.resolved_config_parent, config, settings)


def link_ui_output(ui_output: Path | None, target_dir: str) -> None:
    """Point target_dir/.llama-ui at the built UI directory, if one exists under target_dir."""
    root = Path(target_dir).resolve()
    output = ui_output.resolve() if ui_output else None
    if output is None or not output.is_dir() or not output.is_relative_to(root):
        return
    link = root / UI_LINK_NAME
    link.unlink(missing_ok=True)
    link.symlink_to(output.relative_to(root), target_is_directory=True)


def run_build(target_dir: str = "/opt/app") -> None:
    """Run the full build process: bootstrap + package + upload."""
    setup_logging()

    settings = BootstrapSettings()
    build_id = settings.build_id
    build_api_host = settings.build_api_host
    auth_token = settings.auth_token
    deployment_name = settings.deployment_name

    if not build_id:
        raise ValueError("LLAMA_DEPLOY_BUILD_ID is required for build mode")
    if not build_api_host:
        raise ValueError("LLAMA_DEPLOY_BUILD_API_HOST is required for build mode")
    if not auth_token:
        raise ValueError("LLAMA_DEPLOY_AUTH_TOKEN is required for build mode")
    if not deployment_name:
        raise ValueError("LLAMA_DEPLOY_DEPLOYMENT_NAME is required for build mode")

    logger.info("Starting build: deployment=%s build_id=%s", deployment_name, build_id)

    # Step 0: Check if the artifact already exists in S3 — skip the build if so
    if _artifact_exists(build_api_host, deployment_name, build_id, auth_token):
        logger.info("Artifact already exists, skipping build: build_id=%s", build_id)
        return

    # Step 1: Run the standard bootstrap (clone, install deps, build UI).
    # Temporarily clear BUILD_ID so bootstrap_app_from_repo takes the full
    # clone-and-build path instead of the download-artifact shortcut.
    saved_build_id = os.environ.pop("LLAMA_DEPLOY_BUILD_ID", None)
    try:
        ui_output = bootstrap_app_from_repo(target_dir)
    finally:
        if saved_build_id is not None:
            os.environ["LLAMA_DEPLOY_BUILD_ID"] = saved_build_id
    link_ui_output(ui_output, target_dir)

    # Step 2: Package into tarball
    tarball_path = "/tmp/build-artifact.tar.gz"
    _create_tarball(target_dir, tarball_path)

    # Step 3: Upload to Build API
    _upload_artifact(
        build_api_host=build_api_host,
        deployment_name=deployment_name,
        build_id=build_id,
        auth_token=auth_token,
        tarball_path=tarball_path,
    )

    logger.info("Build completed successfully: build_id=%s", build_id)


if __name__ == "__main__":
    setup_logging()
    bootstrap_app_from_repo()
