import types
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.responses import Response
from llama_agents.control_plane.manage_api.deployments_service import (
    deployments_service,
)
from llama_agents.core import schema
from llama_agents.core.schema.deployments import (
    INTERNAL_CODE_REPO_SCHEME,
    DeploymentResponse,
)
from llama_agents.core.server.manage_api import DeploymentNotFoundError


def _make_deployment(
    project_id: str = "proj-1", display_name: str = "dep-1"
) -> DeploymentResponse:
    return DeploymentResponse(
        id=display_name,
        display_name=display_name,
        project_id=project_id,
        repo_url="https://example.com/repo.git",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        status="Running",
        has_personal_access_token=False,
        secret_names=None,
        apiserver_url=None,
    )


def _rs(uid: str) -> types.SimpleNamespace:
    obj = types.SimpleNamespace()
    obj.metadata = types.SimpleNamespace(uid=uid)
    return obj


def _line(pod: str, container: str, text: str) -> types.SimpleNamespace:
    # Include timestamp to match real LogLine shape used by the service
    return types.SimpleNamespace(
        pod=pod,
        container=container,
        text=text,
        timestamp=datetime.now(timezone.utc),
    )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_stream_logs_missing_deployment(mock_get_deployment: AsyncMock) -> None:
    mock_get_deployment.return_value = None
    with pytest.raises(DeploymentNotFoundError):
        _ = [
            item
            async for item in deployments_service.stream_deployment_logs(
                project_id="proj-1", deployment_id="dep-1"
            )
        ]


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_stream_logs_project_mismatch(mock_get_deployment: AsyncMock) -> None:
    mock_get_deployment.return_value = _make_deployment(project_id="other")
    with pytest.raises(DeploymentNotFoundError):
        _ = [
            item
            async for item in deployments_service.stream_deployment_logs(
                project_id="proj-1", deployment_id="dep-1"
            )
        ]


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.stream_build_job_logs"
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_latest_replicaset_for_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_stream_logs_no_replicaset(
    mock_get_deployment: AsyncMock,
    mock_get_rs: AsyncMock,
    mock_build_logs: MagicMock,
) -> None:
    mock_get_deployment.return_value = _make_deployment()
    mock_get_rs.return_value = None

    async def empty_gen() -> AsyncGenerator[types.SimpleNamespace, None]:
        return
        yield  # make it a generator

    mock_build_logs.return_value = empty_gen()

    items = [
        item
        async for item in deployments_service.stream_deployment_logs(
            project_id="proj-1", deployment_id="dep-1"
        )
    ]
    assert len(items) == 0


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.stream_build_job_logs"
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.stream_replicaset_logs"
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_latest_replicaset_for_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_stream_logs_happy_path(
    mock_get_deployment: AsyncMock,
    mock_get_rs: AsyncMock,
    mock_stream_logs: MagicMock,
    mock_build_logs: MagicMock,
) -> None:
    mock_get_deployment.return_value = _make_deployment()
    mock_get_rs.return_value = _rs("uid-1")

    async def agen() -> AsyncGenerator[types.SimpleNamespace, None]:
        yield _line("pod-1", "c", "a")
        yield _line("pod-1", "c", "b")

    async def empty_gen() -> AsyncGenerator[types.SimpleNamespace, None]:
        return
        yield  # make it a generator

    mock_stream_logs.return_value = agen()
    mock_build_logs.return_value = empty_gen()

    items = [
        item
        async for item in deployments_service.stream_deployment_logs(
            project_id="proj-1", deployment_id="dep-1", include_init_containers=True
        )
    ]

    assert len(items) == 2
    assert items[0].pod == "pod-1" and items[0].text == "a"
    assert items[1].pod == "pod-1" and items[1].text == "b"

    mock_stream_logs.assert_called_once_with(
        deployment_id="dep-1",
        include_init_containers=True,
        since_seconds=None,
        tail_lines=None,
        stop_event=mock.ANY,
        follow=True,
    )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.stream_build_job_logs"
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.stream_replicaset_logs"
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_latest_replicaset_for_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_stream_logs_follow_false_threads_through_and_terminates(
    mock_get_deployment: AsyncMock,
    mock_get_rs: AsyncMock,
    mock_stream_logs: MagicMock,
    mock_build_logs: MagicMock,
) -> None:
    """``follow=False`` skips the RS-change watcher and ends after one pass."""
    mock_get_deployment.return_value = _make_deployment()
    mock_get_rs.return_value = _rs("uid-1")

    async def agen() -> AsyncGenerator[types.SimpleNamespace, None]:
        yield _line("pod-1", "c", "a")

    async def empty_gen() -> AsyncGenerator[types.SimpleNamespace, None]:
        return
        yield  # make it a generator

    mock_stream_logs.return_value = agen()
    mock_build_logs.return_value = empty_gen()

    items = [
        item
        async for item in deployments_service.stream_deployment_logs(
            project_id="proj-1", deployment_id="dep-1", follow=False
        )
    ]

    # Stream completes after one pass.
    assert len(items) == 1
    # ``follow=False`` should propagate to the K8s read.
    mock_stream_logs.assert_called_once_with(
        deployment_id="dep-1",
        include_init_containers=False,
        since_seconds=None,
        tail_lines=None,
        stop_event=mock.ANY,
        follow=False,
    )
    mock_build_logs.assert_called_once_with(
        deployment_id="dep-1",
        since_seconds=None,
        tail_lines=None,
        stop_event=mock.ANY,
        follow=False,
    )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.stream_build_job_logs"
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.stream_replicaset_logs"
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_latest_replicaset_for_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_stream_logs_restarts_on_rs_change(
    mock_get_deployment: AsyncMock,
    mock_get_rs: AsyncMock,
    mock_stream_logs: MagicMock,
    mock_build_logs: MagicMock,
) -> None:
    """When the RS changes, the stream restarts with the new RS's pods."""
    mock_get_deployment.return_value = _make_deployment()
    # Provide enough RS values for both iterations:
    # Iteration 1: initial_rs_uid check, _when_replicaset_changes initial + polls
    # Iteration 2: initial_rs_uid check, _when_replicaset_changes initial + polls
    mock_get_rs.side_effect = [
        _rs("uid-1"),  # iter 1: initial_rs_uid
        _rs("uid-1"),  # iter 1: _when_replicaset_changes initial
        _rs("uid-1"),  # iter 1: poll
        _rs("uid-2"),  # iter 1: poll → change detected, rs_changed=True
        _rs("uid-2"),  # iter 2: initial_rs_uid
        _rs("uid-2"),  # iter 2: _when_replicaset_changes initial
    ] + [_rs("uid-2")] * 50  # iter 2: polls (stable, no change)

    async def gen1() -> AsyncGenerator[types.SimpleNamespace, None]:
        import asyncio

        for i in range(100):
            yield _line("pod-1", "c", f"line-{i}")
            await asyncio.sleep(0)

    async def gen2() -> AsyncGenerator[types.SimpleNamespace, None]:
        yield _line("pod-2", "c", "after-rs-change")

    async def empty_gen() -> AsyncGenerator[types.SimpleNamespace, None]:
        return
        yield

    # First call returns gen1 (cut short by RS change), second returns gen2
    mock_stream_logs.side_effect = [gen1(), gen2()]
    mock_build_logs.return_value = empty_gen()

    items = [
        item
        async for item in deployments_service.stream_deployment_logs(
            project_id="proj-1", deployment_id="dep-1"
        )
    ]

    # Should have some items from gen1 (cut short) + 1 item from gen2
    first_rs_items = [i for i in items if i.pod == "pod-1"]
    second_rs_items = [i for i in items if i.pod == "pod-2"]
    assert len(first_rs_items) < 100
    assert len(second_rs_items) == 1
    assert second_rs_items[0].text == "after-rs-change"


# --- Project mismatch / missing deployment tests ---


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_get_deployment_project_mismatch(mock_get_deployment: AsyncMock) -> None:
    mock_get_deployment.return_value = _make_deployment(project_id="other")
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.get_deployment(
            project_id="proj-1", deployment_id="dep-1"
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_get_deployment_missing(mock_get_deployment: AsyncMock) -> None:
    mock_get_deployment.return_value = None
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.get_deployment(
            project_id="proj-1", deployment_id="dep-1"
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_delete_deployment_project_mismatch(
    mock_get_deployment: AsyncMock,
) -> None:
    mock_get_deployment.return_value = _make_deployment(project_id="other")
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.delete_deployment(
            project_id="proj-1", deployment_id="dep-1"
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_delete_deployment_missing(mock_get_deployment: AsyncMock) -> None:
    mock_get_deployment.return_value = None
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.delete_deployment(
            project_id="proj-1", deployment_id="dep-1"
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
    new=None,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.delete_all_artifacts_for_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.delete_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_delete_deployment_success_no_code_repo(
    mock_get_deployment: AsyncMock,
    mock_k8s_delete: AsyncMock,
    mock_delete_artifacts: AsyncMock,
) -> None:
    """Happy path: deletes k8s resources and artifacts, skips repo when storage is None."""
    mock_get_deployment.return_value = _make_deployment()
    await deployments_service.delete_deployment(
        project_id="proj-1", deployment_id="dep-1"
    )
    mock_k8s_delete.assert_awaited_once_with(deployment_id="dep-1")
    mock_delete_artifacts.assert_awaited_once_with("dep-1")


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.delete_all_artifacts_for_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.delete_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_delete_deployment_cleans_up_code_repo(
    mock_get_deployment: AsyncMock,
    mock_k8s_delete: AsyncMock,
    mock_delete_artifacts: AsyncMock,
    mock_code_repo_storage: MagicMock,
) -> None:
    """When code_repo_storage is configured, delete_repo is called for the deployment."""
    mock_code_repo_storage.delete_repo = AsyncMock()
    mock_get_deployment.return_value = _make_deployment()
    await deployments_service.delete_deployment(
        project_id="proj-1", deployment_id="dep-1"
    )
    mock_k8s_delete.assert_awaited_once_with(deployment_id="dep-1")
    mock_delete_artifacts.assert_awaited_once_with("dep-1")
    mock_code_repo_storage.delete_repo.assert_awaited_once_with("dep-1")


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.delete_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_delete_deployment_k8s_error_propagates(
    mock_get_deployment: AsyncMock,
    mock_delete_deployment: AsyncMock,
) -> None:
    """Non-404 K8s errors during delete must propagate, not be swallowed."""
    from kubernetes.client.exceptions import ApiException

    mock_get_deployment.return_value = _make_deployment()
    mock_delete_deployment.side_effect = ApiException(
        status=500, reason="Internal Server Error"
    )
    with pytest.raises(ApiException):
        await deployments_service.delete_deployment(
            project_id="proj-1", deployment_id="dep-1"
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_get_deployment_history_project_mismatch(
    mock_get_deployment: AsyncMock,
) -> None:
    mock_get_deployment.return_value = _make_deployment(project_id="other")
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.get_deployment_history(
            project_id="proj-1", deployment_id="dep-1"
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_get_deployment_history_missing(
    mock_get_deployment: AsyncMock,
) -> None:
    mock_get_deployment.return_value = None
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.get_deployment_history(
            project_id="proj-1", deployment_id="dep-1"
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_rollback_deployment_project_mismatch(
    mock_get_deployment: AsyncMock,
) -> None:
    mock_get_deployment.return_value = _make_deployment(project_id="other")
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.rollback_deployment(
            project_id="proj-1",
            deployment_id="dep-1",
            request=schema.RollbackRequest(git_sha="abc123"),
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_rollback_deployment_missing(mock_get_deployment: AsyncMock) -> None:
    mock_get_deployment.return_value = None
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.rollback_deployment(
            project_id="proj-1",
            deployment_id="dep-1",
            request=schema.RollbackRequest(git_sha="abc123"),
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_update_deployment_project_mismatch(
    mock_get_deployment: AsyncMock,
) -> None:
    mock_get_deployment.return_value = _make_deployment(project_id="other")
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.update_deployment(
            project_id="proj-1",
            deployment_id="dep-1",
            update_data=schema.DeploymentUpdate(),
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_update_deployment_missing(mock_get_deployment: AsyncMock) -> None:
    mock_get_deployment.return_value = None
    with pytest.raises(DeploymentNotFoundError):
        await deployments_service.update_deployment(
            project_id="proj-1",
            deployment_id="dep-1",
            update_data=schema.DeploymentUpdate(),
        )


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_update_deployment_unresolvable_ref_returns_400(
    mock_get_deployment: AsyncMock,
    mock_code_repo_storage: MagicMock,
) -> None:
    """Updating an internal deployment to a ref that can't be resolved returns 400."""
    dep = _make_deployment()
    dep.repo_url = INTERNAL_CODE_REPO_SCHEME
    mock_get_deployment.return_value = dep
    mock_code_repo_storage.resolve_ref = AsyncMock(return_value=None)

    with pytest.raises(HTTPException) as exc_info:
        await deployments_service.update_deployment(
            project_id="proj-1",
            deployment_id="dep-1",
            update_data=schema.DeploymentUpdate(git_ref="nonexistent-branch"),
        )
    assert exc_info.value.status_code == 400
    assert "nonexistent-branch" in str(exc_info.value.detail)


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
    new=None,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_update_deployment_internal_ref_requires_storage(
    mock_get_deployment: AsyncMock,
) -> None:
    """Updating an internal deployment ref requires configured code repo storage."""
    dep = _make_deployment()
    dep.repo_url = INTERNAL_CODE_REPO_SCHEME
    mock_get_deployment.return_value = dep

    with pytest.raises(HTTPException) as exc_info:
        await deployments_service.update_deployment(
            project_id="proj-1",
            deployment_id="dep-1",
            update_data=schema.DeploymentUpdate(git_ref="main"),
        )

    assert exc_info.value.status_code == 503
    assert "S3_BUCKET" in str(exc_info.value.detail)


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
    new=None,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.update_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_update_deployment_internal_path_change_skips_storage_resolution(
    mock_get_deployment: AsyncMock,
    mock_update_deployment: AsyncMock,
) -> None:
    """Changing only deployment_file_path should not resolve internal repo refs."""
    dep = _make_deployment()
    dep.repo_url = INTERNAL_CODE_REPO_SCHEME
    mock_get_deployment.return_value = dep
    mock_update_deployment.return_value = dep.model_copy(
        update={"deployment_file_path": "new/deploy.yml"}
    )

    response = await deployments_service.update_deployment(
        project_id="proj-1",
        deployment_id="dep-1",
        update_data=schema.DeploymentUpdate(deployment_file_path="new/deploy.yml"),
    )

    assert response.deployment_file_path == "new/deploy.yml"
    mock_update_deployment.assert_awaited_once()


@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
    new=None,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.create_deployment",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.git_service.validate_git_application",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_create_deployment_empty_repo_requires_internal_storage(
    mock_validate_git_application: AsyncMock,
    mock_create_deployment: AsyncMock,
) -> None:
    with pytest.raises(HTTPException) as exc_info:
        await deployments_service.create_deployment(
            project_id="proj-1",
            deployment_data=schema.DeploymentCreate(display_name="dep-1", repo_url=""),
        )

    assert exc_info.value.status_code == 503
    assert "S3_BUCKET" in str(exc_info.value.detail)
    mock_validate_git_application.assert_not_awaited()
    mock_create_deployment.assert_not_awaited()


@patch(
    "llama_agents.control_plane.manage_api.deployments_service._handle_git_request",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_handle_git_request_rejects_external_repo_deployments(
    mock_get_deployment: AsyncMock,
    mock_code_repo_storage: MagicMock,
    mock_handle_git_request: AsyncMock,
) -> None:
    mock_get_deployment.return_value = _make_deployment()

    with pytest.raises(HTTPException) as exc_info:
        await deployments_service.handle_git_request(
            request=MagicMock(),
            project_id="proj-1",
            deployment_id="dep-1",
            git_path="info/refs",
        )

    assert exc_info.value.status_code == 409
    assert "external repository" in str(exc_info.value.detail)
    mock_handle_git_request.assert_not_awaited()
    assert mock_code_repo_storage is not None


@patch(
    "llama_agents.control_plane.manage_api.deployments_service._handle_git_request",
    new_callable=AsyncMock,
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.code_repo_storage",
)
@patch(
    "llama_agents.control_plane.manage_api.deployments_service.k8s_client.get_deployment",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_handle_git_request_allows_push_mode_deployments(
    mock_get_deployment: AsyncMock,
    mock_code_repo_storage: MagicMock,
    mock_handle_git_request: AsyncMock,
) -> None:
    deployment = _make_deployment()
    deployment.repo_url = ""
    mock_get_deployment.return_value = deployment
    mock_handle_git_request.return_value = Response(status_code=200)

    response = await deployments_service.handle_git_request(
        request=MagicMock(),
        project_id="proj-1",
        deployment_id="dep-1",
        git_path="info/refs",
    )

    assert response.status_code == 200
    mock_handle_git_request.assert_awaited_once()
    await_args = mock_handle_git_request.await_args
    assert await_args is not None
    assert await_args.kwargs["storage"] is mock_code_repo_storage
