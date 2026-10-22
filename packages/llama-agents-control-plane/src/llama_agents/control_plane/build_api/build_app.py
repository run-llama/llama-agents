import json
import logging
import re
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse

import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from llama_agents.control_plane import k8s_client
from llama_agents.control_plane.build_api.build_auth import (
    authenticate_deployment,
    authenticate_deployment_basic,
)
from llama_agents.control_plane.build_api.build_gc import gc_build_artifacts
from llama_agents.control_plane.build_api.build_service import build_artifact_storage
from llama_agents.control_plane.code_repo.git_server import handle_git_request_readonly
from llama_agents.control_plane.code_repo.service import code_repo_storage
from llama_agents.control_plane.git import git_service
from llama_agents.control_plane.git._git_service import (
    GitHubAppAccess,
    GitRepository,
    InaccessibleRepository,
)
from llama_agents.core.git.git_util import GitAccessError, _check_hostname_not_private
from llama_agents.core.schema.deployments import (
    INTERNAL_CODE_REPO_SCHEME,
    LlamaDeploymentCRD,
)
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    if build_artifact_storage is None:
        logger.warning(
            "S3_BUCKET is not set — build artifact storage is disabled, /health will report unhealthy"
        )
    else:
        logger.info("Build artifact storage configured")
    yield


# Build API app
build_app = FastAPI(title="LlamaDeploy Build API", lifespan=_lifespan)
Instrumentator().instrument(build_app).expose(build_app, include_in_schema=False)


class HelloResponse(BaseModel):
    message: str
    deployment_id: str
    timestamp: str


# temp for validating connectivity
@build_app.get("/deployments/{deployment_id}/hello", response_model=HelloResponse)
async def hello(
    deployment: Annotated[LlamaDeploymentCRD, Depends(authenticate_deployment)],
) -> HelloResponse:
    """Hello endpoint with token authentication - proof of concept"""
    return HelloResponse(
        message=f"Hello from deployment {deployment.metadata.name}!",
        deployment_id=deployment.metadata.name,
        timestamp=datetime.now().isoformat(),
    )


@build_app.get("/health")
async def health() -> Response:
    """Health check endpoint — reports 503 when S3 is not configured."""
    if build_artifact_storage is None:
        return Response(
            content='{"status": "unhealthy", "reason": "Build artifact storage not configured (S3_BUCKET not set)"}',
            status_code=503,
            media_type="application/json",
        )
    return Response(
        content='{"status": "ok", "service": "build-api"}',
        status_code=200,
        media_type="application/json",
    )


async def _k8s_health_response() -> Response:
    """Shares the same k8s client and thread pool as the manage API, so a wedged
    apiserver connection shows up on both. See `k8s_health_check` for the check
    itself, shared with the manage API so pass/fail logic can't drift between them.
    """
    status_code, body = await k8s_client.k8s_health_check()
    return Response(
        content=json.dumps(body),
        status_code=status_code,
        media_type="application/json",
    )


@build_app.get("/readyz")
async def readyz() -> Response:
    """Readiness: exercises the kube-apiserver path so a wedged pod leaves rotation."""
    return await _k8s_health_response()


# Build Artifact Endpoints
# ========================


@build_app.head("/deployments/{deployment_id}/builds/{build_id}")
async def artifact_exists(
    deployment: Annotated[LlamaDeploymentCRD, Depends(authenticate_deployment)],
    build_id: str,
) -> Response:
    """Check if a build artifact exists."""
    if build_artifact_storage is None:
        raise HTTPException(
            status_code=503, detail="Build artifact storage not configured"
        )
    exists = await build_artifact_storage.artifact_exists(
        deployment.metadata.name, build_id
    )
    if not exists:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return Response(status_code=200)


@build_app.get("/deployments/{deployment_id}/builds/{build_id}")
async def download_artifact(
    deployment: Annotated[LlamaDeploymentCRD, Depends(authenticate_deployment)],
    build_id: str,
) -> StreamingResponse:
    """Download a build artifact (streamed from S3)."""
    if build_artifact_storage is None:
        raise HTTPException(
            status_code=503, detail="Build artifact storage not configured"
        )
    try:
        (
            content_length,
            stream,
        ) = await build_artifact_storage.download_artifact_streaming(
            deployment.metadata.name, build_id
        )
    except build_artifact_storage.NotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return StreamingResponse(
        stream,
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={build_id}.tar.gz",
            "Content-Length": str(content_length),
        },
    )


@build_app.put("/deployments/{deployment_id}/builds/{build_id}")
async def upload_artifact(
    request: Request,
    deployment: Annotated[LlamaDeploymentCRD, Depends(authenticate_deployment)],
    build_id: str,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    """Upload a build artifact (streamed via temp file to S3)."""
    if build_artifact_storage is None:
        raise HTTPException(
            status_code=503, detail="Build artifact storage not configured"
        )
    # Stream request body to a temp file to avoid buffering the full artifact
    # in memory, then upload the temp file to S3.
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tar.gz")
    try:
        size = 0
        async for chunk in request.stream():
            size += len(chunk)
            tmp.write(chunk)
        tmp.close()

        with open(tmp.name, "rb") as f:
            await build_artifact_storage.upload_artifact_fileobj(
                deployment.metadata.name, build_id, f
            )
    finally:
        Path(tmp.name).unlink(missing_ok=True)

    logger.info(
        "Artifact uploaded deployment=%s build_id=%s size=%d",
        deployment.metadata.name,
        build_id,
        size,
    )
    # GC old artifacts in the background. Retain the just-uploaded build_id to
    # avoid a race where it's GC'd before the operator creates the new ReplicaSet.
    background_tasks.add_task(
        gc_build_artifacts, deployment.metadata.name, keep_build_ids={build_id}
    )
    return {"status": "uploaded", "build_id": build_id}


# Git HTTP Protocol Endpoints
# ==========================


@build_app.get("/deployments/{deployment_id}/{git_path:path}")
async def git_proxy_get(
    request: Request,
    deployment: Annotated[LlamaDeploymentCRD, Depends(authenticate_deployment_basic)],
    git_path: str,
) -> Response:
    """
    Proxy all Git HTTP GET requests.

    Handles all Git HTTP protocol GET operations including:
    - info/refs (reference discovery)
    - HEAD (default branch)
    - objects/* (loose objects, pack files, pack indexes)
    - refs/* (individual reference files)
    """
    return await proxy_git_request(request, deployment, git_path)


@build_app.post("/deployments/{deployment_id}/{git_path:path}")
async def git_proxy_post(
    request: Request,
    deployment: Annotated[LlamaDeploymentCRD, Depends(authenticate_deployment_basic)],
    git_path: str,
) -> Response:
    """
    Proxy all Git HTTP POST requests.

    Handles all Git HTTP protocol POST operations including:
    - git-upload-pack (fetch/clone operations)
    - git-receive-pack (push operations)
    """
    return await proxy_git_request(request, deployment, git_path)


# Allowed git HTTP protocol paths
_GIT_PATH_PATTERN = re.compile(
    r"^("
    r"info/refs"
    r"|HEAD"
    r"|objects/.+"
    r"|git-upload-pack"
    r"|git-receive-pack"
    r")$"
)


def _validate_git_path(git_path: str) -> None:
    """Validate that git_path matches expected git protocol paths."""
    normalized = git_path.strip("/")
    if not _GIT_PATH_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid git path: {git_path}",
        )


def _validate_url_not_private(url: str) -> None:
    """Resolve URL hostname and block private/internal IP addresses (SSRF protection).

    Must be called at proxy time (not creation time) to prevent DNS rebinding.
    """
    parsed = urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(status_code=400, detail="Invalid URL: no hostname")

    try:
        _check_hostname_not_private(hostname)
    except GitAccessError as e:
        status = 403 if "private network" in e.message else 400
        raise HTTPException(status_code=status, detail=e.message) from e


async def proxy_git_request(
    request: Request, deployment: LlamaDeploymentCRD, git_path: str
) -> Response:
    """Proxy Git requests.

    Internal repos (repoUrl == INTERNAL_CODE_REPO_SCHEME) are served directly
    via dulwich from S3. External repos are proxied to the upstream URL.
    """
    # Route internal repos to dulwich
    if deployment.spec.repoUrl == INTERNAL_CODE_REPO_SCHEME:
        if code_repo_storage is None:
            raise HTTPException(
                status_code=503,
                detail="Code repo storage not configured (S3_BUCKET not set).",
            )
        return await handle_git_request_readonly(
            request=request,
            deployment_id=deployment.metadata.name,
            git_path=git_path,
            storage=code_repo_storage,
        )

    _validate_git_path(git_path)
    _validate_url_not_private(deployment.spec.repoUrl)

    existing_pat = await k8s_client.get_deployment_pat(deployment.metadata.name)
    auth, access = await git_service.obtain_basic_auth_token(
        deployment.spec.repoUrl, deployment.metadata.name, pat=existing_pat
    )
    if isinstance(access, InaccessibleRepository):
        logger.warning(
            "Git access denied for deployment=%s repo=%s: %s",
            deployment.metadata.name,
            deployment.spec.repoUrl,
            access.message,
        )
        raise HTTPException(status_code=403, detail=access.message)

    # Log which auth method was resolved for this proxy request
    access_label = _describe_access(access)
    logger.info(
        "Git proxy deployment=%s repo=%s auth=%s path=%s",
        deployment.metadata.name,
        deployment.spec.repoUrl,
        access_label,
        git_path,
    )

    structured_auth = None
    if auth:
        splits = auth.split(":")
        structured_auth = httpx.BasicAuth(
            splits[0], splits[1] if len(splits) > 1 else ""
        )

    # Build the target URL properly
    target_url = f"{deployment.spec.repoUrl.rstrip('/')}/{git_path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    async with httpx.AsyncClient(auth=structured_auth, timeout=120.0) as http_client:
        try:
            # Read request body once
            body = await request.body()

            # Forward headers, excluding host and other problematic ones
            forward_headers = {
                k: v
                for k, v in request.headers.items()
                if k.lower()
                not in {"host", "content-length", "transfer-encoding", "authorization"}
            }

            response = await http_client.request(
                request.method,
                target_url,
                content=body,
                headers=forward_headers,
            )

            if response.status_code >= 400:
                rate_limit_remaining = response.headers.get("x-ratelimit-remaining")
                rate_limit_reset = response.headers.get("x-ratelimit-reset")
                rate_info = ""
                if rate_limit_remaining is not None:
                    rate_info = (
                        f" ratelimit_remaining={rate_limit_remaining}"
                        f" ratelimit_reset={rate_limit_reset}"
                    )
                logger.warning(
                    "Upstream error proxying deployment=%s auth=%s: %s %s returned %d%s",
                    deployment.metadata.name,
                    access_label,
                    request.method,
                    target_url,
                    response.status_code,
                    rate_info,
                )

            # Return response with proper status code and headers
            response_headers = {
                k: v
                for k, v in response.headers.items()
                if k.lower()
                not in {"content-length", "transfer-encoding", "connection"}
            }

            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=response_headers,
            )

        except httpx.TimeoutException:
            logger.error(f"Timeout proxying to {target_url}")
            raise HTTPException(status_code=504, detail="Gateway Timeout")
        except httpx.RequestError as e:
            logger.error(f"Request error proxying to {target_url}: {e}")
            raise HTTPException(status_code=502, detail="Bad Gateway")


def _describe_access(
    access: GitHubAppAccess | GitRepository | InaccessibleRepository,
) -> str:
    match access:
        case GitHubAppAccess() as a:
            return f"github_app(installation={a.installation_id})"
        case GitRepository() as r:
            return "public" if r.access_token is None else "pat"
        case _:
            return "none"
