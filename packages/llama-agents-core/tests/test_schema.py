"""Unit tests for Pydantic schemas"""

from datetime import datetime, timezone

import pytest
from llama_agents.core.schema.deployments import (
    APPSERVER_TAG_PREFIX,
    DeploymentCreate,
    DeploymentHistoryResponse,
    DeploymentResponse,
    DeploymentsListResponse,
    DeploymentUpdate,
    LlamaDeploymentPhase,
    LlamaDeploymentSpec,
    ReleaseHistoryEntry,
    ReleaseHistoryItem,
    apply_deployment_update,
    image_tag_to_version,
    version_to_image_tag,
)
from llama_agents.core.schema.projects import ProjectsListResponse, ProjectSummary
from pydantic import HttpUrl, ValidationError


def test_deployment_create_valid() -> None:
    """Test valid DeploymentCreate data"""
    deployment = DeploymentCreate(
        display_name="Test Service",
        repo_url="https://github.com/user/repo.git",
        secrets={"GITHUB_PAT": "ghp_token123"},
    )
    assert deployment.display_name == "Test Service"
    assert deployment.repo_url == "https://github.com/user/repo.git"
    secrets = deployment.secrets
    assert secrets is not None
    assert secrets["GITHUB_PAT"] == "ghp_token123"


# -- Backwards compatibility: name <-> display_name --


def test_deployment_create_accepts_deprecated_name() -> None:
    """Old callers passing 'name' should still work."""
    deployment = DeploymentCreate(name="Legacy Name", repo_url="https://example.com")  # type: ignore[call-arg]  # ty: ignore[missing-argument, unknown-argument]
    assert deployment.display_name == "Legacy Name"


def test_deployment_create_serializes_name_for_old_servers() -> None:
    """Serialized payload must include 'name' so old servers accept it."""
    deployment = DeploymentCreate(display_name="My App", repo_url="https://example.com")
    data = deployment.model_dump(exclude_none=True)
    assert data["name"] == "My App"
    assert data["display_name"] == "My App"


def test_deployment_update_accepts_deprecated_name() -> None:
    """Old callers passing 'name' should still work."""
    update = DeploymentUpdate(name="Legacy Name")  # type: ignore[call-arg]  # ty: ignore[unknown-argument]
    assert update.display_name == "Legacy Name"


def test_deployment_update_serializes_name_for_old_servers() -> None:
    """Serialized payload must include 'name' so old servers accept it."""
    update = DeploymentUpdate(display_name="Renamed")
    data = update.model_dump()
    assert data["name"] == "Renamed"
    assert data["display_name"] == "Renamed"


def test_deployment_update_name_is_none_when_display_name_unset() -> None:
    """When display_name is not set, name should also be None."""
    update = DeploymentUpdate()
    data = update.model_dump()
    assert data["name"] is None
    assert data["display_name"] is None


def test_deployment_response_deserializes_old_server_name() -> None:
    """Server responses with only 'name' (no display_name) should work."""
    resp = DeploymentResponse.model_validate(
        {
            "id": "dep-1",
            "name": "Old Server Deploy",
            "project_id": "proj-1",
            "repo_url": "https://example.com",
            "git_ref": "main",
            "deployment_file_path": "",
            "status": "Running",
            "has_personal_access_token": False,
        }
    )
    assert resp.display_name == "Old Server Deploy"


# -- Backwards compatibility: llama_deploy_version <-> appserver_version --


def test_deployment_create_accepts_deprecated_llama_deploy_version() -> None:
    """Old callers passing 'llama_deploy_version' should still work."""
    deployment = DeploymentCreate(
        display_name="App",
        repo_url="https://example.com",
        llama_deploy_version="0.4.2",  # type: ignore[call-arg]  # ty: ignore[unknown-argument]
    )
    assert deployment.appserver_version == "0.4.2"


def test_deployment_create_canonical_wins_on_conflict() -> None:
    """When both fields are sent, canonical 'appserver_version' wins silently."""
    deployment = DeploymentCreate.model_validate(
        {
            "display_name": "App",
            "repo_url": "https://example.com",
            "appserver_version": "0.4.2",
            "llama_deploy_version": "0.3.0",
        }
    )
    assert deployment.appserver_version == "0.4.2"


def test_deployment_create_neither_version_field_set() -> None:
    """With neither field set, appserver_version is None."""
    deployment = DeploymentCreate(display_name="App", repo_url="https://example.com")
    assert deployment.appserver_version is None


@pytest.mark.parametrize(
    "version",
    [
        "0.11.3",
        "1.0.0",
        "0.12.0rc1",
        "0.12.0.dev1",
        "0.12.0.post1",
    ],
)
def test_deployment_create_accepts_public_pep440_appserver_version(
    version: str,
) -> None:
    deployment = DeploymentCreate(
        display_name="App",
        repo_url="https://example.com",
        appserver_version=version,
    )
    assert deployment.appserver_version == version


@pytest.mark.parametrize(
    "version",
    [
        "latest",
        "tilt-dev",
        "definitely-not-a-version",
        "0.12.0+local",
        "1!2.0",
    ],
)
def test_deployment_create_rejects_non_public_pep440_appserver_version(
    version: str,
) -> None:
    with pytest.raises(ValidationError, match="appserver_version"):
        DeploymentCreate(
            display_name="App",
            repo_url="https://example.com",
            appserver_version=version,
        )


def test_deployment_create_serializes_llama_deploy_version_for_old_servers() -> None:
    """Serialized payload must include 'llama_deploy_version' so old servers accept it."""
    deployment = DeploymentCreate(
        display_name="App",
        repo_url="https://example.com",
        appserver_version="0.4.2",
    )
    data = deployment.model_dump()
    assert data["appserver_version"] == "0.4.2"
    assert data["llama_deploy_version"] == "0.4.2"


def test_deployment_update_accepts_deprecated_llama_deploy_version() -> None:
    """Old callers patching 'llama_deploy_version' should still work."""
    update = DeploymentUpdate(llama_deploy_version="0.4.2")  # type: ignore[call-arg]  # ty: ignore[unknown-argument]
    assert update.appserver_version == "0.4.2"


def test_deployment_update_canonical_wins_on_conflict() -> None:
    """When both fields are sent, canonical 'appserver_version' wins silently."""
    update = DeploymentUpdate.model_validate(
        {"appserver_version": "0.4.2", "llama_deploy_version": "0.3.0"}
    )
    assert update.appserver_version == "0.4.2"


def test_deployment_update_rejects_non_public_pep440_appserver_version() -> None:
    with pytest.raises(ValidationError, match="appserver_version"):
        DeploymentUpdate(appserver_version="tilt-dev")


def test_deployment_update_allows_internal_image_tag_escape_hatch() -> None:
    update = DeploymentUpdate(image_tag="tilt-dev")
    assert update.image_tag == "tilt-dev"


def test_deployment_update_serializes_llama_deploy_version_for_old_servers() -> None:
    """Serialized PATCH payload must include 'llama_deploy_version' so old servers accept it."""
    update = DeploymentUpdate(appserver_version="0.4.2")
    data = update.model_dump()
    assert data["appserver_version"] == "0.4.2"
    assert data["llama_deploy_version"] == "0.4.2"


def test_deployment_update_llama_deploy_version_none_when_unset() -> None:
    """When appserver_version is not set, llama_deploy_version should also be None."""
    update = DeploymentUpdate()
    data = update.model_dump()
    assert data["appserver_version"] is None
    assert data["llama_deploy_version"] is None


def test_deployment_response_accepts_old_server_llama_deploy_version() -> None:
    """An old server emits only llama_deploy_version; new client should map it through."""
    resp = DeploymentResponse.model_validate(
        {
            "id": "dep-1",
            "display_name": "App",
            "project_id": "proj-1",
            "repo_url": "https://example.com",
            "git_ref": "main",
            "deployment_file_path": "",
            "status": "Running",
            "has_personal_access_token": False,
            "llama_deploy_version": "0.4.2",
        }
    )
    assert resp.appserver_version == "0.4.2"


def test_deployment_response_serializes_both_version_fields() -> None:
    """Responses include both keys so old clients keep working."""
    resp = DeploymentResponse(
        id="dep-1",
        display_name="App",
        project_id="proj-1",
        repo_url="https://example.com",
        git_ref="main",
        deployment_file_path="",
        status="Running",
        appserver_version="0.4.2",
    )
    data = resp.model_dump()
    assert data["appserver_version"] == "0.4.2"
    assert data["llama_deploy_version"] == "0.4.2"


def test_deployment_create_optional_fields() -> None:
    """Test DeploymentCreate with optional fields"""

    deployment = DeploymentCreate(
        display_name="Test Service",
        repo_url="https://github.com/user/repo.git",
        secrets={"GITHUB_PAT": "token"},
        deployment_file_path="custom_deploy.py",
        personal_access_token="ghp_token123",
    )
    assert deployment.deployment_file_path == "custom_deploy.py"
    assert deployment.personal_access_token == "ghp_token123"


def test_deployment_response() -> None:
    """Test DeploymentResponse creation"""
    response = DeploymentResponse(
        id="deploy1-123",
        display_name="deploy1",
        project_id="test-project",
        repo_url="https://github.com/user/repo1.git",
        git_ref="abc123",
        deployment_file_path="llama_deployment.yml",
        status="Running",
        apiserver_url=HttpUrl("http://test-deploy.127.0.0.1.nip.io"),
    )
    assert response.display_name == "deploy1"
    assert response.project_id == "test-project"
    assert response.status == "Running"
    assert response.apiserver_url == HttpUrl("http://test-deploy.127.0.0.1.nip.io")


def test_apply_deployment_update_basic_fields() -> None:
    """Test updating basic spec fields"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/old-repo.git",
        deploymentFilePath="old_deploy.yml",
    )

    update = DeploymentUpdate(
        repo_url="https://github.com/user/new-repo.git",
        deployment_file_path="new_deploy.yml",
    )

    result = apply_deployment_update(update, existing_spec)

    # Check updated spec
    assert result.updated_spec.projectId == "test-project"  # unchanged
    assert (
        result.updated_spec.repoUrl == "https://github.com/user/new-repo.git"
    )  # updated
    assert result.updated_spec.deploymentFilePath == "new_deploy.yml"  # updated

    # No secret changes for basic field updates
    assert result.secret_adds == {}
    assert result.secret_removes == []

    # Original spec should be unchanged
    assert existing_spec.repoUrl == "https://github.com/user/old-repo.git"
    assert existing_spec.deploymentFilePath == "old_deploy.yml"


def test_apply_deployment_update_personal_access_token() -> None:
    """Test PAT updates (stored as GITHUB_PAT secret)"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
    )

    # Test adding/updating PAT
    update_add = DeploymentUpdate(personal_access_token="ghp_newtoken123")
    result = apply_deployment_update(update_add, existing_spec)

    assert result.secret_adds == {"GITHUB_PAT": "ghp_newtoken123"}
    assert result.secret_removes == []

    # Test removing PAT (empty string)
    update_remove = DeploymentUpdate(personal_access_token="")
    result = apply_deployment_update(update_remove, existing_spec)

    assert result.secret_adds == {}
    assert result.secret_removes == ["GITHUB_PAT"]


def test_apply_deployment_update_secrets() -> None:
    """Test explicit secret additions and removals"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
    )

    update = DeploymentUpdate(
        secrets={
            "DATABASE_URL": "postgresql://new-db",  # add/update
            "API_KEY": "new-key-123",  # add/update
            "OLD_SECRET": None,  # remove
        }
    )

    result = apply_deployment_update(update, existing_spec)

    assert result.secret_adds == {
        "DATABASE_URL": "postgresql://new-db",
        "API_KEY": "new-key-123",
    }
    assert result.secret_removes == ["OLD_SECRET"]


def test_apply_deployment_update_combined() -> None:
    """Test combining field updates, PAT, and secrets"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/old-repo.git",
        deploymentFilePath="old_deploy.yml",
    )

    update = DeploymentUpdate(
        repo_url="https://github.com/user/new-repo.git",
        personal_access_token="ghp_newtoken",
        secrets={
            "DATABASE_URL": "postgresql://db",
            "REMOVE_ME": None,
        },
    )

    result = apply_deployment_update(update, existing_spec)

    # Spec updates
    assert result.updated_spec.repoUrl == "https://github.com/user/new-repo.git"
    assert result.updated_spec.deploymentFilePath == "old_deploy.yml"  # unchanged

    # Secret changes
    assert result.secret_adds == {
        "GITHUB_PAT": "ghp_newtoken",
        "DATABASE_URL": "postgresql://db",
    }
    assert result.secret_removes == ["REMOVE_ME"]


def test_apply_deployment_update_no_changes() -> None:
    """Test update with no actual changes"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
    )

    update = DeploymentUpdate()  # All fields None

    result = apply_deployment_update(update, existing_spec)

    # Spec should be unchanged
    assert result.updated_spec.projectId == existing_spec.projectId
    assert result.updated_spec.repoUrl == existing_spec.repoUrl

    # No secret changes
    assert result.secret_adds == {}
    assert result.secret_removes == []


def test_apply_deployment_update_none_fields() -> None:
    """Test that None values in update don't override spec fields"""
    existing_spec = LlamaDeploymentSpec(
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
        deploymentFilePath="deploy.yml",
        gitRef="main",
        displayName="my-deployment",
    )

    update = DeploymentUpdate(
        repo_url=None,  # Should not override existing value
        deployment_file_path=None,  # Should not override existing value
        personal_access_token=None,  # Should not add/remove PAT
        secrets=None,  # Should not modify secrets
    )

    result = apply_deployment_update(update, existing_spec)

    # All spec fields should remain unchanged
    assert result.updated_spec.projectId == "test-project"
    assert result.updated_spec.repoUrl == "https://github.com/user/repo.git"
    assert result.updated_spec.deploymentFilePath == "deploy.yml"
    assert result.updated_spec.gitRef == "main"
    assert result.updated_spec.displayName == "my-deployment"

    # No secret changes
    assert result.secret_adds == {}
    assert result.secret_removes == []


def test_project_summary() -> None:
    """Test ProjectSummary creation"""
    project = ProjectSummary(
        project_id="test-project", deployment_count=5, project_name="test-project"
    )
    assert project.project_id == "test-project"
    assert project.deployment_count == 5


def test_deployments_list_response() -> None:
    """Test DeploymentsListResponse creation"""
    deployments = [
        DeploymentResponse(
            id="deploy1",
            display_name="deploy1",
            project_id="test-project",
            repo_url="https://github.com/user/repo1.git",
            git_ref="abc123",
            deployment_file_path="llama_deployment.yml",
            status="Running",
            apiserver_url=HttpUrl("http://deploy1.example.com"),
        ),
        DeploymentResponse(
            id="deploy2",
            display_name="deploy2",
            project_id="test-project",
            repo_url="https://github.com/user/repo2.git",
            git_ref="def456",
            deployment_file_path="llama_deployment.yml",
            status="Pending",
            apiserver_url=HttpUrl("http://deploy2.example.com"),
        ),
    ]

    response = DeploymentsListResponse(deployments=deployments)
    assert len(response.deployments) == 2
    assert response.deployments[0].display_name == "deploy1"
    assert response.deployments[1].display_name == "deploy2"


def test_apply_deployment_update_git_ref() -> None:
    """Test updating git_ref field"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
        gitRef="main",
    )

    update = DeploymentUpdate(git_ref="feature-branch")
    result = apply_deployment_update(update, existing_spec)

    # Check that git_ref was updated
    assert result.updated_spec.gitRef == "feature-branch"
    assert (
        result.updated_spec.repoUrl == "https://github.com/user/repo.git"
    )  # unchanged
    assert result.updated_spec.projectId == "test-project"  # unchanged

    # No secret changes
    assert result.secret_adds == {}
    assert result.secret_removes == []

    # Original spec should be unchanged
    assert existing_spec.gitRef == "main"


def test_release_history_models_roundtrip() -> None:
    # CRD-style entry
    dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    entry = ReleaseHistoryEntry(gitSha="abc", releasedAt=dt)
    assert entry.gitSha == "abc"
    # API response types
    item = ReleaseHistoryItem(git_sha="abc", released_at=dt)
    resp = DeploymentHistoryResponse(deployment_id="d1", history=[item])
    assert resp.deployment_id == "d1"
    assert resp.history[0].git_sha == "abc"


def test_deployment_create_with_git_ref() -> None:
    """Test DeploymentCreate with git_ref field"""
    deployment = DeploymentCreate(
        display_name="Test Service",
        repo_url="https://github.com/user/repo.git",
        git_ref="feature-branch",
        secrets={"API_KEY": "secret_value"},
    )

    assert deployment.display_name == "Test Service"
    assert deployment.repo_url == "https://github.com/user/repo.git"
    assert deployment.git_ref == "feature-branch"
    assert deployment.secrets == {"API_KEY": "secret_value"}


def test_projects_list_response() -> None:
    """Test ProjectsListResponse creation"""
    projects = [
        ProjectSummary(
            project_id="project1", deployment_count=3, project_name="project1"
        ),
        ProjectSummary(
            project_id="project2", deployment_count=1, project_name="project2"
        ),
    ]

    response = ProjectsListResponse(projects=projects)
    assert len(response.projects) == 2
    assert response.projects[0].project_id == "project1"
    assert response.projects[1].deployment_count == 1


def test_deployment_phases_all_valid() -> None:
    """Test that all deployment phases are valid literal values"""
    valid_phases: list[LlamaDeploymentPhase] = [
        "Pending",
        "Running",
        "Failed",
        "RollingOut",
        "RolloutFailed",
    ]

    for phase in valid_phases:
        # Test that we can create DeploymentResponse with each phase
        response = DeploymentResponse(
            id=f"deploy-{phase.lower()}",
            display_name=f"test-{phase.lower()}",
            project_id="test-project",
            repo_url="https://github.com/user/repo.git",
            git_ref="main",
            deployment_file_path="llama_deployment.yml",
            status=phase,
            apiserver_url=HttpUrl("http://test.example.com"),
        )
        assert response.status == phase


def test_unknown_phase_value_accepted() -> None:
    """An unknown phase value (e.g. emitted by a newer server) is accepted as a
    plain string instead of failing validation on older clients."""
    response = DeploymentResponse(
        id="deploy-future",
        display_name="future-phase-deployment",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="main",
        deployment_file_path="deploy.yml",
        status="SomeFuturePhaseTheClientDoesNotKnow",
        apiserver_url=HttpUrl("http://future.example.com"),
    )
    assert response.status == "SomeFuturePhaseTheClientDoesNotKnow"


def test_rollingout_phase_response() -> None:
    """Test DeploymentResponse with RollingOut phase"""
    response = DeploymentResponse(
        id="deploy-rolling",
        display_name="rolling-deployment",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="feature-branch",
        deployment_file_path="deploy.yml",
        status="RollingOut",
        apiserver_url=HttpUrl("http://rolling.example.com"),
    )
    assert response.status == "RollingOut"
    assert response.display_name == "rolling-deployment"


def test_rolloutfailed_phase_response() -> None:
    """Test DeploymentResponse with RolloutFailed phase"""
    response = DeploymentResponse(
        id="deploy-failed",
        display_name="failed-deployment",
        project_id="test-project",
        repo_url="https://github.com/user/repo.git",
        git_ref="broken-branch",
        deployment_file_path="deploy.yml",
        status="RolloutFailed",
        apiserver_url=HttpUrl("http://failed.example.com"),
    )
    assert response.status == "RolloutFailed"
    assert response.display_name == "failed-deployment"


def test_deployment_phases_in_list_response() -> None:
    """Test that new phases work in DeploymentsListResponse"""
    deployments = [
        DeploymentResponse(
            id="deploy1",
            display_name="running-deploy",
            project_id="test-project",
            repo_url="https://github.com/user/repo1.git",
            git_ref="main",
            deployment_file_path="deploy.yml",
            status="Running",
            apiserver_url=HttpUrl("http://running.example.com"),
        ),
        DeploymentResponse(
            id="deploy2",
            display_name="rolling-deploy",
            project_id="test-project",
            repo_url="https://github.com/user/repo2.git",
            git_ref="feature",
            deployment_file_path="deploy.yml",
            status="RollingOut",
            apiserver_url=HttpUrl("http://rolling.example.com"),
        ),
        DeploymentResponse(
            id="deploy3",
            display_name="failed-deploy",
            project_id="test-project",
            repo_url="https://github.com/user/repo3.git",
            git_ref="broken",
            deployment_file_path="deploy.yml",
            status="RolloutFailed",
            apiserver_url=HttpUrl("http://failed.example.com"),
        ),
    ]

    response = DeploymentsListResponse(deployments=deployments)
    assert len(response.deployments) == 3
    assert response.deployments[0].status == "Running"
    assert response.deployments[1].status == "RollingOut"
    assert response.deployments[2].status == "RolloutFailed"


# ===== Image tag / version conversion helpers =====


def test_version_to_image_tag() -> None:
    assert version_to_image_tag("0.4.2") == "0.4.2"
    assert version_to_image_tag("1.0.0") == "1.0.0"
    assert version_to_image_tag("latest") == "latest"


def test_image_tag_to_version_plain() -> None:
    """New-style plain version tags are recognized."""
    assert image_tag_to_version("0.4.2") == "0.4.2"
    assert image_tag_to_version("1.0.0") == "1.0.0"


def test_image_tag_to_version_legacy_prefix() -> None:
    """Legacy appserver-prefixed tags still work for backward compat."""
    assert image_tag_to_version("appserver-0.4.2") == "0.4.2"
    assert image_tag_to_version("appserver-latest") == "latest"


def test_image_tag_to_version_non_conforming() -> None:
    # Hash-based tags from dev builds, custom tags, etc.
    assert image_tag_to_version("abc123def") is None
    assert image_tag_to_version("my-custom-tag") is None
    assert image_tag_to_version("") is None


def test_appserver_tag_prefix_constant() -> None:
    assert APPSERVER_TAG_PREFIX == "appserver-"


def test_apply_deployment_update_image_tag_precedence() -> None:
    """image_tag takes precedence over appserver_version"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
    )

    update = DeploymentUpdate(
        appserver_version="0.3.0",
        image_tag="appserver-0.4.2",
    )

    result = apply_deployment_update(update, existing_spec)
    assert result.updated_spec.imageTag == "appserver-0.4.2"


def test_apply_deployment_update_image_tag_only() -> None:
    """image_tag alone sets the spec imageTag"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
    )

    update = DeploymentUpdate(image_tag="custom-hash-tag")
    result = apply_deployment_update(update, existing_spec)
    assert result.updated_spec.imageTag == "custom-hash-tag"


def test_apply_deployment_update_suspended() -> None:
    spec = LlamaDeploymentSpec(projectId="p", repoUrl="https://r", displayName="n")

    # Suspend
    result = apply_deployment_update(DeploymentUpdate(suspended=True), spec)
    assert result.updated_spec.suspended is True

    # Unsuspend
    result2 = apply_deployment_update(
        DeploymentUpdate(suspended=False), result.updated_spec
    )
    assert result2.updated_spec.suspended is False

    # No change (None = don't touch)
    result3 = apply_deployment_update(DeploymentUpdate(), result2.updated_spec)
    assert result3.updated_spec.suspended is False


def test_apply_deployment_update_auto_resume_git_ref() -> None:
    """PATCH with git_ref on a suspended deployment auto-resumes it."""
    spec = LlamaDeploymentSpec(
        projectId="p", repoUrl="https://r", displayName="n", suspended=True
    )
    update = DeploymentUpdate(git_ref="new-branch")
    result = apply_deployment_update(update, spec)
    assert result.updated_spec.suspended is False
    assert result.updated_spec.gitRef == "new-branch"


def test_apply_deployment_update_auto_resume_secrets() -> None:
    """PATCH with secrets on a suspended deployment auto-resumes it."""
    spec = LlamaDeploymentSpec(
        projectId="p", repoUrl="https://r", displayName="n", suspended=True
    )
    update = DeploymentUpdate(secrets={"KEY": "val"})
    result = apply_deployment_update(update, spec)
    assert result.updated_spec.suspended is False


def test_apply_deployment_update_auto_resume_image_tag() -> None:
    """PATCH with image_tag on a suspended deployment auto-resumes it."""
    spec = LlamaDeploymentSpec(
        projectId="p", repoUrl="https://r", displayName="n", suspended=True
    )
    update = DeploymentUpdate(image_tag="appserver-0.5.0")
    result = apply_deployment_update(update, spec)
    assert result.updated_spec.suspended is False
    assert result.updated_spec.imageTag == "appserver-0.5.0"


def test_apply_deployment_update_auto_resume_explicit_suspended_true() -> None:
    """PATCH with git_ref + suspended=True stays suspended."""
    spec = LlamaDeploymentSpec(
        projectId="p", repoUrl="https://r", displayName="n", suspended=True
    )
    update = DeploymentUpdate(git_ref="new-branch", suspended=True)
    result = apply_deployment_update(update, spec)
    assert result.updated_spec.suspended is True
    assert result.updated_spec.gitRef == "new-branch"


def test_apply_deployment_update_auto_resume_only_suspended_true() -> None:
    """PATCH with only suspended=True stays suspended (no auto-resume)."""
    spec = LlamaDeploymentSpec(
        projectId="p", repoUrl="https://r", displayName="n", suspended=True
    )
    update = DeploymentUpdate(suspended=True)
    result = apply_deployment_update(update, spec)
    assert result.updated_spec.suspended is True


def test_deployment_update_has_git_fields() -> None:
    """Test that has_git_fields() returns True for git-affecting fields and False for non-git fields."""
    assert DeploymentUpdate(git_ref="main").has_git_fields() is True
    assert DeploymentUpdate(repo_url="https://github.com/u/r").has_git_fields() is True
    assert DeploymentUpdate(deployment_file_path="deploy.yml").has_git_fields() is True
    assert DeploymentUpdate(personal_access_token="ghp_tok").has_git_fields() is True
    assert DeploymentUpdate(suspended=True).has_git_fields() is False
    assert DeploymentUpdate(image_tag="appserver-0.5.0").has_git_fields() is False
    assert DeploymentUpdate(secrets={"K": "V"}).has_git_fields() is False
    assert DeploymentUpdate().has_git_fields() is False


def test_apply_deployment_update_static_assets_path_not_cleared() -> None:
    """Test that static_assets_path is NOT cleared when update doesn't set it."""
    spec = LlamaDeploymentSpec(
        projectId="p",
        repoUrl="https://r",
        displayName="n",
        staticAssetsPath="/existing/path",
    )
    update = DeploymentUpdate(suspended=True)
    result = apply_deployment_update(update, spec)
    assert result.updated_spec.staticAssetsPath == "/existing/path"


def test_apply_deployment_update_version_sets_image_tag() -> None:
    """appserver_version is converted to imageTag when image_tag is not set"""
    existing_spec = LlamaDeploymentSpec(
        displayName="my-deployment",
        projectId="test-project",
        repoUrl="https://github.com/user/repo.git",
    )

    update = DeploymentUpdate(appserver_version="0.3.1")
    result = apply_deployment_update(update, existing_spec)
    assert result.updated_spec.imageTag == "0.3.1"
