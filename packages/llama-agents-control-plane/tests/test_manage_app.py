from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from llama_agents.control_plane.manage_api.manage_app import app
from llama_agents.core.schema.deployments import DeploymentResponse
from llama_agents.core.schema.git_validation import GitApplicationValidationResponse


@pytest.mark.asyncio
async def test_health_does_not_touch_k8s() -> None:
    """`/health` stays a cheap process check, unrelated to k8s; backs startup and
    liveness so an apiserver outage can't restart every replica at once."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz_returns_200_when_k8s_is_healthy() -> None:
    with patch(
        "llama_agents.control_plane.k8s_client.check_k8s_connectivity",
        AsyncMock(return_value=None),
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_readyz_returns_503_when_k8s_check_fails() -> None:
    """A wedged kube-apiserver connection must fail /readyz, not report 200."""
    with patch(
        "llama_agents.control_plane.k8s_client.check_k8s_connectivity",
        AsyncMock(side_effect=TimeoutError("simulated wedge")),
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"


def _existing_deployment() -> DeploymentResponse:
    return DeploymentResponse(
        id="my-app",
        display_name="my-app",
        project_id="proj",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        status="Running",
        has_personal_access_token=False,
        secret_names=[],
        apiserver_url=None,
    )


@pytest.mark.asyncio
async def test_patch_with_rejected_repo_url_returns_400_and_does_not_update() -> None:
    with (
        patch(
            "llama_agents.control_plane.k8s_client.get_deployment",
            AsyncMock(return_value=_existing_deployment()),
        ),
        patch(
            "llama_agents.control_plane.k8s_client.get_deployment_pat",
            AsyncMock(return_value=None),
        ),
        patch(
            "llama_agents.control_plane.k8s_client.update_deployment", AsyncMock()
        ) as update_deployment,
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.patch(
                "/api/v1beta1/deployments/my-app",
                params={"project_id": "proj"},
                json={"repo_url": "file:///tmp/repo"},
            )
    assert response.status_code == 400
    assert "Disallowed git URL scheme 'file'" in response.json()["detail"]
    update_deployment.assert_not_called()


@pytest.mark.asyncio
async def test_create_with_taken_deployment_id_returns_409() -> None:
    with (
        patch(
            "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application",
            AsyncMock(
                return_value=GitApplicationValidationResponse(
                    is_valid=True,
                    git_sha="12345678",
                    git_ref="main",
                    valid_deployment_file_path="llama_deploy.yaml",
                )
            ),
        ),
        patch(
            "llama_agents.control_plane.k8s_client.validate_deployment_id",
            AsyncMock(return_value=False),
        ),
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1beta1/deployments",
                params={"project_id": "proj"},
                json={
                    "display_name": "My App",
                    "id": "my-app",
                    "repo_url": "https://github.com/user/repo.git",
                },
            )
    assert response.status_code == 409
    assert response.json()["detail"] == "Deployment ID 'my-app' is already in use."
