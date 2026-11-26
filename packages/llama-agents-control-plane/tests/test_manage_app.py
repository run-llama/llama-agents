from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Iterator
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio
from kubernetes.client.exceptions import ApiException
from llama_agents.control_plane.manage_api.manage_app import app
from llama_agents.core.schema.deployments import DeploymentResponse
from llama_agents.core.schema.git_validation import GitApplicationValidationResponse

PROJECT = {"project_id": "proj"}


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    # raise_app_exceptions=False so unhandled errors come back as the app's 500
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def valid_git_application() -> Iterator[None]:
    with patch(
        "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application",
        AsyncMock(
            return_value=GitApplicationValidationResponse(
                is_valid=True,
                git_sha="12345678",
                git_ref="main",
                valid_deployment_file_path="llama_deploy.yaml",
            )
        ),
    ):
        yield


async def _create(client: httpx.AsyncClient, deployment_id: str) -> httpx.Response:
    return await client.post(
        "/api/v1beta1/deployments",
        params=PROJECT,
        json={
            "display_name": "My App",
            "id": deployment_id,
            "repo_url": "https://github.com/user/repo.git",
        },
    )


async def _validate_repository(
    client: httpx.AsyncClient, repository_url: str
) -> httpx.Response:
    return await client.post(
        "/api/v1beta1/deployments/validate-repository",
        params=PROJECT,
        json={"repository_url": repository_url},
    )


@pytest.mark.asyncio
async def test_health_does_not_touch_k8s(client: httpx.AsyncClient) -> None:
    """`/health` stays a cheap process check, unrelated to k8s; backs startup and
    liveness so an apiserver outage can't restart every replica at once."""
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz_returns_200_when_k8s_is_healthy(
    client: httpx.AsyncClient,
) -> None:
    with patch(
        "llama_agents.control_plane.k8s_client.check_k8s_connectivity",
        AsyncMock(return_value=None),
    ):
        response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_readyz_returns_503_when_k8s_check_fails(
    client: httpx.AsyncClient,
) -> None:
    """A wedged kube-apiserver connection must fail /readyz, not report 200."""
    with patch(
        "llama_agents.control_plane.k8s_client.check_k8s_connectivity",
        AsyncMock(side_effect=TimeoutError("simulated wedge")),
    ):
        response = await client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"


@pytest.mark.asyncio
async def test_patch_with_rejected_repo_url_returns_400_and_does_not_update(
    client: httpx.AsyncClient,
) -> None:
    existing = DeploymentResponse(
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
    with (
        patch(
            "llama_agents.control_plane.k8s_client.get_deployment",
            AsyncMock(return_value=existing),
        ),
        patch(
            "llama_agents.control_plane.k8s_client.get_deployment_pat",
            AsyncMock(return_value=None),
        ),
        patch(
            "llama_agents.control_plane.k8s_client.update_deployment", AsyncMock()
        ) as update_deployment,
    ):
        response = await client.patch(
            "/api/v1beta1/deployments/my-app",
            params=PROJECT,
            json={"repo_url": "file:///tmp/repo"},
        )
    assert response.status_code == 400
    assert "Disallowed git URL scheme 'file'" in response.json()["detail"]
    update_deployment.assert_not_called()


@pytest.mark.asyncio
async def test_unresolvable_and_private_hosts_get_the_same_400_detail(
    client: httpx.AsyncClient,
) -> None:
    private = await _validate_repository(client, "http://localhost/repo.git")
    unresolvable = await _validate_repository(
        client, "http://no-such-host.invalid/repo.git"
    )
    assert private.status_code == unresolvable.status_code == 400
    assert private.json() == unresolvable.json()


@pytest.mark.parametrize(
    ("repository_url", "redacted"),
    [
        ("ssh://bot:s3cret@git.example.com/repo.git", "ssh://***@git.example.com"),
        ("bot:s3cret@git.example.com/repo.git", "***@git.example.com"),
        ("ssh://bot:s3@cret@git.example.com/repo.git", "ssh://***@git.example.com"),
    ],
)
@pytest.mark.asyncio
async def test_rejected_url_credentials_are_redacted_from_response_and_log(
    client: httpx.AsyncClient,
    caplog: pytest.LogCaptureFixture,
    repository_url: str,
    redacted: str,
) -> None:
    with caplog.at_level(logging.WARNING):
        response = await _validate_repository(client, repository_url)
    assert response.status_code == 400
    assert redacted in response.json()["detail"]
    assert "s3" not in response.text
    assert "s3" not in caplog.text


@pytest.mark.asyncio
async def test_create_with_taken_deployment_id_returns_409(
    client: httpx.AsyncClient, valid_git_application: None
) -> None:
    with patch(
        "llama_agents.control_plane.k8s_client.get_deployment_crd",
        AsyncMock(return_value=MagicMock()),
    ):
        response = await _create(client, "my-app")
    assert response.status_code == 409
    assert response.json()["detail"] == "Deployment ID 'my-app' is already in use."


@pytest.mark.asyncio
async def test_create_with_reserved_deployment_id_returns_400(
    client: httpx.AsyncClient, valid_git_application: None
) -> None:
    response = await _create(client, "version")
    assert response.status_code == 400
    assert "is reserved" in response.json()["detail"]


@pytest.mark.asyncio
async def test_create_when_id_lookup_fails_returns_500_not_409(
    client: httpx.AsyncClient, valid_git_application: None
) -> None:
    with patch(
        "llama_agents.control_plane.k8s_client.get_deployment_crd",
        AsyncMock(side_effect=ApiException(status=503)),
    ):
        response = await _create(client, "my-app")
    assert response.status_code == 500
