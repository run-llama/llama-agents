"""Unit tests for API endpoints"""

import contextlib
from collections.abc import AsyncGenerator
from datetime import datetime
from importlib.metadata import version as pkg_version
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from llama_agents.control_plane.manage_api.manage_app import app
from llama_agents.core.schema import LogEvent
from llama_agents.core.schema.deployments import (
    DeploymentEvent,
    DeploymentHistoryResponse,
    DeploymentResponse,
)
from llama_agents.core.schema.git_validation import GitApplicationValidationResponse
from llama_agents.core.server.manage_api import (
    DeploymentNotFoundError,
    ReplicaSetNotFoundError,
)
from packaging.version import Version
from pydantic import HttpUrl

client = TestClient(app)


def test_public_version_endpoint_returns_extended_fields() -> None:
    resp = client.get("/api/v1beta1/deployments-public/version")
    assert resp.status_code == 200
    data = resp.json()
    # Version is dynamic from package; just ensure it is present and non-empty
    assert isinstance(data.get("version"), str) and data["version"]
    assert data.get("requires_auth") is False
    assert Version(data.get("min_llamactl_version")) <= Version(
        pkg_version("llama-agents-control-plane")
    )


@pytest.fixture
def mock_existing_deployment() -> Generator[MagicMock, None, None]:
    """Fixture that mocks k8s_client.get_deployment to return an existing deployment"""

    existing_deployment = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="abc123",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=["DATABASE_URL"],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    with patch("llama_agents.control_plane.k8s_client.get_deployment") as mock_get:
        mock_get.return_value = existing_deployment
        yield mock_get


@pytest.fixture(autouse=True)
def default_mock_git_ref() -> Generator[MagicMock, None, None]:
    with mock_git_ref(
        GitApplicationValidationResponse(
            git_sha="12345678",
            git_ref="main",
            is_valid=True,
            valid_deployment_file_path="llama_deploy.yaml",
        )
    ) as mock:
        yield mock


@contextlib.contextmanager
def mock_git_ref(
    value: GitApplicationValidationResponse,
) -> Generator[MagicMock, None, None]:
    """Fixture that mocks git_service.validate_git_ref to return some git ref"""
    with patch(
        "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application"
    ) as mock_validate:
        mock_validate.return_value = value
        yield mock_validate


@patch("llama_agents.control_plane.k8s_client.get_deployments")
def test_list_deployments(mock_get_deployments: MagicMock) -> None:
    """Test listing deployments for a project"""

    mock_get_deployments.return_value = [
        DeploymentResponse(
            id="deploy1-123",
            display_name="deploy1",
            project_id="test-project",
            repo_url="https://github.com/user/repo.git",
            git_ref="abc123",
            deployment_file_path="deploy.yml",
            status="Running",
            has_personal_access_token=False,
            secret_names=["DATABASE_URL"],
            apiserver_url=HttpUrl("http://deploy1.example.com"),
        )
    ]

    response = client.get(
        "/api/v1beta1/deployments", params={"project_id": "test-project"}
    )
    assert response.status_code == 200

    data = response.json()
    assert len(data["deployments"]) == 1
    assert data["deployments"][0]["name"] == "deploy1"


@patch("llama_agents.control_plane.k8s_client.get_deployment")
def test_get_deployment_success(mock_get_deployment: MagicMock) -> None:
    """Test getting a single deployment"""

    mock_get_deployment.return_value = DeploymentResponse(
        id="test-deploy-123",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="abc123",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=True,
        secret_names=["API_KEY", "DATABASE_URL"],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    response = client.get(
        "/api/v1beta1/deployments/test-deploy", params={"project_id": "test-project"}
    )
    assert response.status_code == 200

    data = response.json()
    assert data["name"] == "test-deploy"


@patch("llama_agents.control_plane.k8s_client.get_deployment_events")
@patch("llama_agents.control_plane.k8s_client.get_deployment")
def test_get_deployment_with_events(
    mock_get_deployment: MagicMock, mock_get_events: MagicMock
) -> None:
    """Test getting a single deployment"""

    mock_get_deployment.return_value = DeploymentResponse(
        id="test-deploy-123",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="abc123",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=True,
        secret_names=["API_KEY", "DATABASE_URL"],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    mock_get_events.return_value = [
        DeploymentEvent(
            message="Deployment created",
            reason="Normal",
            type="Normal",
        )
    ]

    response = client.get(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project", "include_events": True},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["name"] == "test-deploy"
    assert len(data["events"]) == 1
    assert data["events"][0]["message"] == "Deployment created"
    assert data["events"][0]["reason"] == "Normal"
    assert data["events"][0]["type"] == "Normal"


@patch("llama_agents.control_plane.k8s_client.get_deployment")
def test_get_deployment_not_found(mock_get_deployment: MagicMock) -> None:
    """Test 404 when deployment doesn't exist"""
    mock_get_deployment.return_value = None

    response = client.get(
        "/api/v1beta1/deployments/nonexistent", params={"project_id": "test-project"}
    )
    assert response.status_code == 404


@patch("llama_agents.control_plane.k8s_client.create_deployment")
def test_create_deployment_success(mock_create_deployment: MagicMock) -> None:
    """Test deployment creation"""

    mock_create_deployment.return_value = DeploymentResponse(
        id="new-deploy-123",
        display_name="new-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="abc123",
        deployment_file_path="deploy.yml",
        status="Pending",
        has_personal_access_token=True,
        secret_names=["API_KEY"],
        apiserver_url=None,
    )

    request_data = {
        "name": "New Deploy",
        "repo_url": "https://github.com/user/repo.git",
        "personal_access_token": "ghp_token123",
        "secrets": {"API_KEY": "secret_value"},
        "appserver_version": "0.3.0",
    }

    response = client.post(
        "/api/v1beta1/deployments",
        params={"project_id": "test-project"},
        json=request_data,
    )
    assert response.status_code == 201

    data = response.json()
    assert data["display_name"] == "new-deploy"
    assert data["has_personal_access_token"] is True
    assert data["secret_names"] == ["API_KEY"]

    # Verify the correct parameters were passed to create_deployment
    mock_create_deployment.assert_called_once_with(
        project_id="test-project",
        display_name="New Deploy",
        repo_url="https://github.com/user/repo.git",
        deployment_file_path="llama_deploy.yaml",
        git_ref="main",
        git_sha="12345678",
        pat="ghp_token123",
        secrets={"API_KEY": "secret_value"},
        ui_build_output_path=None,
        image_tag="0.3.0",
        explicit_id=None,
    )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
    new=None,
)
@patch("llama_agents.control_plane.k8s_client.create_deployment")
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application"
)
def test_create_deployment_empty_repo_without_storage_fails_fast(
    mock_validate_git_application: MagicMock,
    mock_create_deployment: MagicMock,
) -> None:
    response = client.post(
        "/api/v1beta1/deployments",
        params={"project_id": "test-project"},
        json={"name": "Push Mode Deploy", "repo_url": ""},
    )

    assert response.status_code == 503
    assert (
        response.json()["detail"]
        == "Code repo storage not configured (S3_BUCKET not set)."
    )
    mock_validate_git_application.assert_not_called()
    mock_create_deployment.assert_not_called()


@patch("llama_agents.control_plane.k8s_client.create_deployment")
def test_create_deployment_with_git_ref(mock_create_deployment: MagicMock) -> None:
    """Test deployment creation with git_ref parameter"""

    mock_create_deployment.return_value = DeploymentResponse(
        id="new-deploy-456",
        display_name="new-deploy-with-ref",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="feature-branch",
        deployment_file_path="deploy.yml",
        status="Pending",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )

    request_data = {
        "name": "New Deploy with Ref",
        "repo_url": "https://github.com/user/repo.git",
        "git_ref": "feature-branch",
        "deployment_file_path": "deploy.yml",
    }

    response = client.post(
        "/api/v1beta1/deployments",
        params={"project_id": "test-project"},
        json=request_data,
    )
    assert response.status_code == 201

    data = response.json()
    assert data["display_name"] == "new-deploy-with-ref"
    assert data["git_ref"] == "feature-branch"
    assert data["has_personal_access_token"] is False

    # Verify the correct parameters were passed to create_deployment
    mock_create_deployment.assert_called_once_with(
        project_id="test-project",
        display_name="New Deploy with Ref",
        repo_url="https://github.com/user/repo.git",
        deployment_file_path="deploy.yml",
        git_ref="feature-branch",
        git_sha="12345678",
        pat=None,
        secrets=None,
        ui_build_output_path=None,
        image_tag=None,
        explicit_id=None,
    )


@patch("llama_agents.control_plane.k8s_client.create_deployment")
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application"
)
def test_create_deployment_git_ref_validation_error(
    mock_validate_git_application: MagicMock,
    mock_create_deployment: MagicMock,
) -> None:
    """Test deployment creation with invalid git_ref returns 201 with warning header"""

    # Create a deployment response to return with the error
    deployment_response = DeploymentResponse(
        id="new-deploy-123",
        display_name="New Deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="invalid-ref",
        deployment_file_path="llama_deploy.yaml",
        status="Pending",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )

    # Mock git validation error - return tuple with warning
    mock_create_deployment.return_value = deployment_response
    mock_validate_git_application.return_value = GitApplicationValidationResponse(
        git_ref="invalid-ref",
        git_sha=None,
        is_valid=False,
        valid_deployment_file_path=None,
        error_message="failed to resolve the git repository or reference",
    )

    request_data = {
        "name": "New Deploy",
        "repo_url": "https://github.com/user/repo.git",
        "git_ref": "invalid-ref",
    }

    response = client.post(
        "/api/v1beta1/deployments",
        params={"project_id": "test-project"},
        json=request_data,
    )
    assert response.status_code == 201

    # Check that we get the deployment response
    data = response.json()
    assert data["display_name"] == "New Deploy"
    assert data["git_ref"] == "invalid-ref"
    # Check that warning is embedded in response
    assert "warning" in data and data["warning"]
    assert "failed to resolve the git repository or reference" in data["warning"]

    # Verify the function was called with invalid git_ref
    mock_create_deployment.assert_called_once_with(
        project_id="test-project",
        display_name="New Deploy",
        repo_url="https://github.com/user/repo.git",
        deployment_file_path=None,
        git_ref="invalid-ref",
        git_sha=None,
        pat=None,
        secrets=None,
        ui_build_output_path=None,
        image_tag=None,
        explicit_id=None,
    )


@patch("llama_agents.control_plane.k8s_client.update_deployment")
def test_update_deployment_success(
    mock_update_deployment: MagicMock, mock_existing_deployment: MagicMock
) -> None:
    """Test deployment update via PATCH"""

    mock_update_deployment.return_value = DeploymentResponse(
        id="test-deploy-123",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/updated-repo.git",  # updated
        git_ref="main",  # updated
        deployment_file_path="new_deploy.yml",  # updated
        status="Running",
        has_personal_access_token=True,
        secret_names=["NEW_SECRET"],  # updated
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
        appserver_version="0.3.1",
    )

    update_data = {
        "repo_url": "https://github.com/user/updated-repo.git",
        "deployment_file_path": "new_deploy.yml",
        "personal_access_token": "ghp_newtoken",
        "secrets": {
            "NEW_SECRET": "new_value",
            "OLD_SECRET": None,  # remove this secret
        },
        "appserver_version": "0.3.1",
    }

    response = client.patch(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project"},
        json=update_data,
    )
    assert response.status_code == 200

    data = response.json()
    assert data["repo_url"] == "https://github.com/user/updated-repo.git"
    assert data["deployment_file_path"] == "new_deploy.yml"
    assert data["git_ref"] == "main"
    assert data["has_personal_access_token"] is True
    assert data["secret_names"] == ["NEW_SECRET"]
    assert data["appserver_version"] == "0.3.1"
    # Old clients reading the response see the deprecated key populated
    assert data["llama_deploy_version"] == "0.3.1"

    # Verify the update function was called with correct parameters
    mock_update_deployment.assert_called_once()
    call_args = mock_update_deployment.call_args
    assert call_args[1]["deployment_id"] == "test-deploy"
    update_arg = call_args[1]["update"]
    assert update_arg.repo_url == "https://github.com/user/updated-repo.git"
    assert update_arg.deployment_file_path == "new_deploy.yml"
    assert update_arg.personal_access_token == "ghp_newtoken"
    assert update_arg.secrets == {"NEW_SECRET": "new_value", "OLD_SECRET": None}
    assert update_arg.appserver_version == "0.3.1"


@patch("llama_agents.control_plane.k8s_client.update_deployment")
@patch("llama_agents.control_plane.k8s_client.get_deployment")
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application"
)
def test_update_deployment_suspend_skips_git_validation(
    mock_validate: MagicMock,
    mock_get_deployment: MagicMock,
    mock_update_deployment: MagicMock,
) -> None:
    """Test that a suspend-only PATCH does not call git validation."""
    mock_get_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="abc123",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=["DATABASE_URL"],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    mock_update_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="abc123",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=["DATABASE_URL"],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
        suspended=True,
    )

    response = client.patch(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project"},
        json={"suspended": True},
    )
    assert response.status_code == 200
    mock_validate.assert_not_called()
    mock_update_deployment.assert_called_once()


@patch("llama_agents.control_plane.k8s_client.update_deployment")
@patch("llama_agents.control_plane.k8s_client.get_deployment")
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application"
)
def test_update_deployment_internal_repo_skips_git_validation(
    mock_validate: MagicMock,
    mock_get_deployment: MagicMock,
    mock_update_deployment: MagicMock,
) -> None:
    """Test that updating a deployment with internal:// repo skips git validation."""
    mock_get_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="internal://",
        git_ref="abc123",
        git_sha="deadbeef",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=[],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    mock_update_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="internal://",
        git_ref="abc123",
        git_sha="deadbeef",
        deployment_file_path="new_path/deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=[],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    response = client.patch(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project"},
        json={"deployment_file_path": "new_path/deploy.yml"},
    )
    assert response.status_code == 200
    mock_validate.assert_not_called()
    mock_update_deployment.assert_called_once()


@patch("llama_agents.control_plane.k8s_client.update_deployment")
@patch("llama_agents.control_plane.k8s_client.get_deployment")
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application"
)
def test_update_deployment_empty_repo_url_preserves_internal(
    mock_validate: MagicMock,
    mock_get_deployment: MagicMock,
    mock_update_deployment: MagicMock,
) -> None:
    """Empty repo_url in update should not overwrite an existing internal:// value."""
    mock_get_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="internal://",
        git_ref="main",
        git_sha="deadbeef",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=[],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    mock_update_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="internal://",
        git_ref="main",
        git_sha="deadbeef",
        deployment_file_path="new_path/deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=[],
        apiserver_url=HttpUrl("http://test-deploy.example.com"),
    )

    response = client.patch(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project"},
        json={"repo_url": "", "deployment_file_path": "new_path/deploy.yml"},
    )
    assert response.status_code == 200
    mock_validate.assert_not_called()
    # Verify repo_url was dropped (set to None) so it doesn't overwrite internal://
    update_arg = mock_update_deployment.call_args[1]["update"]
    assert update_arg.repo_url is None


@patch("llama_agents.control_plane.k8s_client.update_deployment")
def test_update_deployment_partial(
    mock_update_deployment: MagicMock, mock_existing_deployment: MagicMock
) -> None:
    """Test partial deployment update (only some fields)"""
    from llama_agents.core.schema.deployments import DeploymentResponse

    mock_update_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/new-repo.git",  # only this changed
        git_ref="abc123",  # unchanged
        deployment_file_path="deploy.yml",  # unchanged
        status="Running",
        has_personal_access_token=False,  # unchanged
        secret_names=None,  # unchanged
        apiserver_url=None,
    )

    # Only update repo_url
    update_data = {"repo_url": "https://github.com/user/new-repo.git"}

    response = client.patch(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project"},
        json=update_data,
    )
    assert response.status_code == 200

    data = response.json()
    assert data["repo_url"] == "https://github.com/user/new-repo.git"

    # Verify partial update was called
    mock_update_deployment.assert_called_once()
    call_args = mock_update_deployment.call_args
    update_arg = call_args[1]["update"]
    assert update_arg.repo_url == "https://github.com/user/new-repo.git"
    # Other fields should be None (not updated)
    assert update_arg.deployment_file_path is None
    assert update_arg.personal_access_token is None
    assert update_arg.secrets is None


@patch("llama_agents.control_plane.k8s_client.update_deployment")
def test_update_deployment_secrets_only(
    mock_update_deployment: MagicMock, mock_existing_deployment: MagicMock
) -> None:
    """Test updating only secrets"""

    mock_update_deployment.return_value = DeploymentResponse(
        id="test-deploy",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",  # unchanged
        git_ref="abc123",  # unchanged
        deployment_file_path="deploy.yml",  # unchanged
        status="Running",
        has_personal_access_token=False,  # unchanged
        secret_names=["API_KEY"],  # updated
        apiserver_url=None,
    )

    # Only update secrets
    update_data = {
        "secrets": {
            "API_KEY": "new_api_key_value",
            "DATABASE_URL": None,  # remove this
        }
    }

    response = client.patch(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project"},
        json=update_data,
    )
    assert response.status_code == 200

    # Verify secrets-only update
    mock_update_deployment.assert_called_once()
    call_args = mock_update_deployment.call_args
    update_arg = call_args[1]["update"]
    assert update_arg.secrets == {"API_KEY": "new_api_key_value", "DATABASE_URL": None}
    # Other fields should be None
    assert update_arg.repo_url is None
    assert update_arg.deployment_file_path is None
    assert update_arg.personal_access_token is None


@patch("llama_agents.control_plane.k8s_client.get_projects_with_deployment_count")
def test_list_projects(mock_get_projects: MagicMock) -> None:
    """Test listing projects with deployment counts"""
    mock_get_projects.return_value = [
        {"project_id": "project1", "project_name": "project1", "deployment_count": 2},
        {"project_id": "project2", "project_name": "project2", "deployment_count": 1},
    ]

    response = client.get("/api/v1beta1/deployments/list-projects")
    assert response.status_code == 200

    data = response.json()
    assert len(data["projects"]) == 2
    assert data["projects"][0]["project_id"] == "project1"
    assert data["projects"][0]["deployment_count"] == 2


def test_create_deployment_validation_error() -> None:
    """Test validation error on missing required fields"""
    # Missing required field: name
    request_data = {"repo_url": "https://github.com/example/repo"}

    response = client.post(
        "/api/v1beta1/deployments",
        params={"project_id": "test-project"},
        json=request_data,
    )
    assert response.status_code == 422  # Validation error


def test_create_deployment_rejects_invalid_appserver_version() -> None:
    response = client.post(
        "/api/v1beta1/deployments",
        params={"project_id": "test-project"},
        json={
            "name": "New Deploy",
            "repo_url": "",
            "appserver_version": "tilt-dev",
        },
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail[0]["loc"] == ["body", "appserver_version"]
    assert "invalid appserver_version" in detail[0]["msg"]


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.deployments_service.stream_deployment_logs"
)
def test_stream_deployment_logs_success(mock_stream: MagicMock) -> None:
    """Stream logs endpoint returns SSE frames with LogEvent payloads and headers."""

    events = [
        LogEvent(pod="pod-1", container="c", text="hello", timestamp=datetime.now()),
        LogEvent(pod="pod-1", container="c", text="world", timestamp=datetime.now()),
    ]

    async def agen() -> AsyncGenerator[LogEvent, None]:
        for e in events:
            yield e

    mock_stream.return_value = agen()

    resp = client.get(
        "/api/v1beta1/deployments/deploy-1/logs",
        params={
            "project_id": "proj-1",
            "include_init_containers": True,
            "since_seconds": 10,
            "tail_lines": 5,
        },
    )

    assert resp.status_code == 200
    # content-type may include charset; just check prefix
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert resp.headers.get("X-Accel-Buffering") == "no"
    body = resp.text
    assert "event: log\n" in body
    # Verify payloads appear as JSON lines
    assert "\n\n" in body
    assert '"pod":"pod-1"' in body and '"text":"hello"' in body
    mock_stream.assert_called_once_with(
        project_id="proj-1",
        deployment_id="deploy-1",
        include_init_containers=True,
        since_seconds=10,
        tail_lines=5,
        follow=True,
    )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.deployments_service.stream_deployment_logs"
)
def test_stream_deployment_logs_follow_false_threads_through(
    mock_stream: MagicMock,
) -> None:
    """``?follow=false`` should be threaded into the service call."""

    async def empty_gen() -> AsyncGenerator[LogEvent, None]:
        if False:
            yield LogEvent(pod="x", container="c", text="", timestamp=datetime.now())
        return

    mock_stream.return_value = empty_gen()

    resp = client.get(
        "/api/v1beta1/deployments/deploy-1/logs",
        params={"project_id": "proj-1", "follow": "false"},
    )
    assert resp.status_code == 200
    mock_stream.assert_called_once_with(
        project_id="proj-1",
        deployment_id="deploy-1",
        include_init_containers=False,
        since_seconds=None,
        tail_lines=None,
        follow=False,
    )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.deployments_service.stream_deployment_logs",
    side_effect=Exception("unexpected"),
)
def test_stream_deployment_logs_unexpected_error(mock_stream: MagicMock) -> None:
    """Unexpected errors from service should propagate as 500 from FastAPI."""

    local_client = TestClient(app, raise_server_exceptions=False)
    resp = local_client.get(
        "/api/v1beta1/deployments/deploy-err/logs",
        params={"project_id": "proj-1"},
    )
    # FastAPI default for uncaught exceptions is 500
    assert resp.status_code == 500


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.deployments_service.stream_deployment_logs",
    side_effect=DeploymentNotFoundError("not found"),
)
def test_stream_deployment_logs_not_found(mock_stream: MagicMock) -> None:
    resp = client.get(
        "/api/v1beta1/deployments/deploy-missing/logs",
        params={"project_id": "proj-1"},
    )
    assert resp.status_code == 404
    data = resp.json()
    assert data["detail"] == "not found"


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.deployments_service.stream_deployment_logs",
    side_effect=ReplicaSetNotFoundError("no rs"),
)
def test_stream_deployment_logs_no_replicaset(mock_stream: MagicMock) -> None:
    resp = client.get(
        "/api/v1beta1/deployments/deploy-nors/logs",
        params={"project_id": "proj-1"},
    )
    assert resp.status_code == 409
    data = resp.json()
    assert data["detail"] == "no rs"


@patch("llama_agents.control_plane.manage_api.deployments_service._handle_git_request")
@patch("llama_agents.control_plane.k8s_client.get_deployment")
def test_git_endpoint_rejects_external_repo_deployments(
    mock_get_deployment: MagicMock,
    mock_handle_git_request: MagicMock,
) -> None:
    mock_get_deployment.return_value = DeploymentResponse(
        id="deploy-1",
        display_name="deploy-1",
        project_id="proj-1",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )

    resp = client.get(
        "/api/v1beta1/deployments/deploy-1/git/info/refs",
        params={"project_id": "proj-1", "service": "git-receive-pack"},
    )

    assert resp.status_code == 409
    assert "external repository" in resp.json()["detail"]
    mock_handle_git_request.assert_not_called()


@patch("llama_agents.control_plane.k8s_client.get_deployment")
@patch("llama_agents.control_plane.k8s_client.get_deployment_history")
def test_get_deployment_history(
    mock_get_history: MagicMock, mock_get_deployment: MagicMock
) -> None:
    mock_get_deployment.return_value = DeploymentResponse(
        id="deploy-1",
        display_name="deploy-1",
        project_id="proj-1",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )
    mock_get_history.return_value = DeploymentHistoryResponse(
        deployment_id="deploy-1",
        history=[],
    )

    resp = client.get(
        "/api/v1beta1/deployments/deploy-1/history",
        params={"project_id": "proj-1"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["deployment_id"] == "deploy-1"


@patch("llama_agents.control_plane.k8s_client.get_deployment")
@patch("llama_agents.control_plane.k8s_client.get_deployment_history")
@patch("llama_agents.control_plane.k8s_client.update_deployment")
def test_rollback(
    mock_update_deployment: MagicMock,
    mock_get_history: MagicMock,
    mock_get_deployment: MagicMock,
) -> None:
    from llama_agents.core.schema.deployments import ReleaseHistoryItem

    mock_get_deployment.return_value = DeploymentResponse(
        id="deploy-1",
        display_name="deploy-1",
        project_id="proj-1",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )
    mock_get_history.return_value = DeploymentHistoryResponse(
        deployment_id="deploy-1",
        history=[
            ReleaseHistoryItem(
                git_sha="deadbeef",
                image_tag="appserver-0.4.1",
                released_at=datetime(2025, 1, 1),
            ),
        ],
    )
    mock_update_deployment.return_value = DeploymentResponse(
        id="deploy-1",
        display_name="deploy-1",
        project_id="proj-1",
        repo_url="https://github.com/user/repo.git",
        git_ref=None,
        git_sha="deadbeef",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )

    resp = client.post(
        "/api/v1beta1/deployments/deploy-1/rollback",
        params={"project_id": "proj-1"},
        json={"git_sha": "deadbeef"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["git_sha"] == "deadbeef"

    # Verify that the update included the imageTag from history directly
    update_arg = mock_update_deployment.call_args
    update_data = update_arg.kwargs.get("update") or update_arg[1].get("update")
    assert update_data.image_tag == "appserver-0.4.1"


OPERATOR_DEFAULT_PATCH = patch(
    "llama_agents.control_plane.manage_api.deployments_service.settings"
    ".default_appserver_image_tag",
    "operator-default",
)


@OPERATOR_DEFAULT_PATCH
@patch("llama_agents.control_plane.k8s_client.create_deployment")
def test_create_deployment_operator_default_ignores_client_version(
    mock_create_deployment: MagicMock,
) -> None:
    """When DEFAULT_APPSERVER_IMAGE_TAG=operator-default, client-sent version is ignored."""
    mock_create_deployment.return_value = DeploymentResponse(
        id="new-deploy-123",
        display_name="new-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="deploy.yml",
        status="Pending",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )

    response = client.post(
        "/api/v1beta1/deployments",
        params={"project_id": "test-project"},
        json={
            "name": "New Deploy",
            "repo_url": "https://github.com/user/repo.git",
            "llama_deploy_version": "0.3.0",
        },
    )
    assert response.status_code == 201

    mock_create_deployment.assert_called_once()
    call_kwargs = mock_create_deployment.call_args[1]
    assert call_kwargs["image_tag"] is None


@OPERATOR_DEFAULT_PATCH
@patch("llama_agents.control_plane.k8s_client.update_deployment")
def test_update_deployment_operator_default_ignores_client_version(
    mock_update_deployment: MagicMock, mock_existing_deployment: MagicMock
) -> None:
    """When DEFAULT_APPSERVER_IMAGE_TAG=operator-default, update strips version/tag."""
    mock_update_deployment.return_value = DeploymentResponse(
        id="test-deploy-123",
        display_name="test-deploy",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )

    response = client.patch(
        "/api/v1beta1/deployments/test-deploy",
        params={"project_id": "test-project"},
        json={
            "repo_url": "https://github.com/user/repo.git",
            "llama_deploy_version": "0.3.1",
        },
    )
    assert response.status_code == 200

    update_arg = mock_update_deployment.call_args[1]["update"]
    assert update_arg.appserver_version is None
    assert update_arg.image_tag is None


@OPERATOR_DEFAULT_PATCH
@patch("llama_agents.control_plane.k8s_client.get_deployment")
@patch("llama_agents.control_plane.k8s_client.get_deployment_history")
@patch("llama_agents.control_plane.k8s_client.update_deployment")
def test_rollback_operator_default_ignores_history_tag(
    mock_update_deployment: MagicMock,
    mock_get_history: MagicMock,
    mock_get_deployment: MagicMock,
) -> None:
    """When DEFAULT_APPSERVER_IMAGE_TAG=operator-default, rollback doesn't restore imageTag."""
    from llama_agents.core.schema.deployments import ReleaseHistoryItem

    mock_get_deployment.return_value = DeploymentResponse(
        id="deploy-1",
        display_name="deploy-1",
        project_id="proj-1",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )
    mock_get_history.return_value = DeploymentHistoryResponse(
        deployment_id="deploy-1",
        history=[
            ReleaseHistoryItem(
                git_sha="deadbeef",
                image_tag="appserver-0.4.1",
                released_at=datetime(2025, 1, 1),
            ),
        ],
    )
    mock_update_deployment.return_value = DeploymentResponse(
        id="deploy-1",
        display_name="deploy-1",
        project_id="proj-1",
        repo_url="https://github.com/user/repo.git",
        git_ref=None,
        git_sha="deadbeef",
        deployment_file_path="deploy.yml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )

    resp = client.post(
        "/api/v1beta1/deployments/deploy-1/rollback",
        params={"project_id": "proj-1"},
        json={"git_sha": "deadbeef"},
    )
    assert resp.status_code == 200

    update_data = mock_update_deployment.call_args[1]["update"]
    assert update_data.image_tag is None


if __name__ == "__main__":
    pytest.main([__file__])
