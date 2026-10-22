import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from importlib.metadata import version as pkg_version
from typing import AsyncGenerator, Literal, cast

from fastapi import HTTPException, Request
from fastapi.responses import Response
from llama_agents.control_plane import k8s_client
from llama_agents.control_plane.build_api.build_gc import (
    delete_all_artifacts_for_deployment,
)
from llama_agents.control_plane.code_repo.git_server import (
    handle_git_request as _handle_git_request,
)
from llama_agents.control_plane.code_repo.service import code_repo_storage
from llama_agents.control_plane.git import git_service
from llama_agents.control_plane.lifecycle import shutdown_event
from llama_agents.control_plane.settings import settings
from llama_agents.core import schema
from llama_agents.core.iter_utils import (
    debounced_sorted_prefix,
    merge_generators,
)
from llama_agents.core.schema import LogEvent
from llama_agents.core.schema.deployments import (
    INTERNAL_CODE_REPO_SCHEME,
    DeploymentHistoryResponse,
    DeploymentResponse,
    DeploymentUpdate,
    RollbackRequest,
    version_to_image_tag,
)
from llama_agents.core.server.manage_api import (
    AbstractDeploymentsService,
    AbstractPublicDeploymentsService,
    DeploymentNotFoundError,
)
from overrides import override

logger = logging.getLogger(__name__)


DEFAULT_ORG = schema.OrgSummary(org_id="default", org_name="Default", is_default=True)


async def _on_push_complete(
    deployment_id: str,
    new_sha: str | None,
    git_ref: str | None,
) -> None:
    if new_sha is None:
        logger.warning(
            "Push to deployment %s completed but could not determine HEAD SHA",
            deployment_id,
        )
        return

    # Only update the CRD on the first push — when the deployment
    # doesn't yet have an internal repo_url or is missing a git_sha.
    # Subsequent pushes just upload code to S3; the user must use
    # `llamactl deploy update` to explicitly advance the ref/sha.
    current = await k8s_client.get_deployment(deployment_id)
    if (
        current is not None
        and current.repo_url == INTERNAL_CODE_REPO_SCHEME
        and current.git_sha
    ):
        logger.info(
            "Push to deployment %s uploaded to S3; skipping CRD update "
            "(already has repo_url and git_sha)",
            deployment_id,
        )
        return

    logger.info(
        "First push to deployment %s: setting sha=%s ref=%s",
        deployment_id,
        new_sha,
        git_ref,
    )
    update = DeploymentUpdate(
        repo_url=INTERNAL_CODE_REPO_SCHEME,
        git_sha=new_sha,
        git_ref=git_ref,
    )
    result = await k8s_client.update_deployment(
        deployment_id=deployment_id,
        update=update,
    )
    if result is None:
        logger.error(
            "Failed to update deployment %s after push (not found)",
            deployment_id,
        )


class PublicDeploymentService(AbstractPublicDeploymentsService):
    @override
    async def get_version(self) -> schema.VersionResponse:
        capabilities: list[schema.Capability] = [schema.Capabilities.ORGANIZATIONS]
        if code_repo_storage is not None:
            capabilities.append(schema.Capabilities.CODE_PUSH)
        return schema.VersionResponse(
            version=pkg_version("llama-agents-control-plane"),
            requires_auth=False,
            min_llamactl_version="0.3.0a13",
            capabilities=capabilities,
        )


class DeploymentService(AbstractDeploymentsService):
    async def _get_deployment_or_raise(
        self, project_id: str, deployment_id: str, include_events: bool = False
    ) -> DeploymentResponse:
        deployment = await k8s_client.get_deployment(deployment_id)
        if deployment is None or deployment.project_id != project_id:
            raise DeploymentNotFoundError(
                f"Deployment with id {deployment_id} not found"
            )
        if include_events:
            deployment.events = await k8s_client.get_deployment_events(deployment_id)
        return deployment

    @override
    async def get_organizations(self) -> schema.OrganizationsListResponse:
        return schema.OrganizationsListResponse(organizations=[DEFAULT_ORG])

    @override
    async def get_projects(
        self, org_id: str | None = None
    ) -> schema.ProjectsListResponse:
        return schema.ProjectsListResponse(
            projects=await k8s_client.get_projects_with_deployment_count()
        )

    @override
    async def validate_repository(
        self,
        project_id: str,
        request: schema.RepositoryValidationRequest,
    ) -> schema.RepositoryValidationResponse:
        """Validate repository access and return unified response."""
        return await git_service.validate_repository(
            repository_url=request.repository_url,
            project_id=project_id,
            deployment_id=request.deployment_id,
            pat=request.pat,
        )

    @override
    async def create_deployment(
        self,
        project_id: str,
        deployment_data: schema.DeploymentCreate,
    ) -> DeploymentResponse:
        # Skip git validation for empty repo_url (pending deployment, code will be pushed later)
        if not deployment_data.repo_url and code_repo_storage is None:
            raise HTTPException(
                status_code=503,
                detail="Code repo storage not configured (S3_BUCKET not set).",
            )

        if deployment_data.repo_url:
            validated = await git_service.validate_git_application(
                repository_url=deployment_data.repo_url,
                git_ref=deployment_data.git_ref,
                deployment_file_path=deployment_data.deployment_file_path,
                pat=deployment_data.personal_access_token,
            )
        else:
            validated = None

        requested_version = deployment_data.appserver_version
        if not settings.should_stamp_image_tag:
            # Defer to operator's LLAMA_DEPLOY_IMAGE_TAG env var
            requested_image_tag = None
        elif requested_version:
            requested_image_tag = version_to_image_tag(requested_version)
        elif settings.default_appserver_image_tag:
            requested_image_tag = settings.default_appserver_image_tag
        else:
            requested_image_tag = None

        deployment_response = await k8s_client.create_deployment(
            project_id=project_id,
            display_name=deployment_data.display_name or "",
            repo_url=deployment_data.repo_url,
            deployment_file_path=(
                deployment_data.deployment_file_path
                or (validated.valid_deployment_file_path if validated else None)
            ),
            git_ref=(
                deployment_data.git_ref or (validated.git_ref if validated else None)
            ),
            git_sha=validated.git_sha if validated else None,
            pat=deployment_data.personal_access_token,
            secrets=deployment_data.secrets,
            ui_build_output_path=validated.ui_build_output_path if validated else None,
            image_tag=requested_image_tag,
            explicit_id=deployment_data.id,
        )

        # Propagate version in response for clients
        deployment_response.appserver_version = requested_version

        # Return deployment response with warning header if there are git issues
        if validated is not None and validated.error_message:
            deployment_response.warning = validated.error_message
        return deployment_response

    @override
    async def get_deployments(
        self,
        project_id: str,
    ) -> schema.DeploymentsListResponse:
        deployments = await k8s_client.get_deployments(project_id=project_id)
        return schema.DeploymentsListResponse(deployments=deployments)

    @override
    async def get_deployment(
        self,
        project_id: str,
        deployment_id: str,
        include_events: bool = False,
    ) -> DeploymentResponse:
        return await self._get_deployment_or_raise(
            project_id, deployment_id, include_events
        )

    @override
    async def delete_deployment(
        self,
        project_id: str,
        deployment_id: str,
    ) -> None:
        await self._get_deployment_or_raise(project_id, deployment_id)
        await k8s_client.delete_deployment(deployment_id=deployment_id)
        await delete_all_artifacts_for_deployment(deployment_id)
        if code_repo_storage is not None:
            await code_repo_storage.delete_repo(deployment_id)

    async def get_deployment_history(
        self, project_id: str, deployment_id: str
    ) -> DeploymentHistoryResponse:
        await self._get_deployment_or_raise(project_id, deployment_id)
        result = await k8s_client.get_deployment_history(deployment_id)
        if result is None:
            raise DeploymentNotFoundError(
                f"Deployment with id {deployment_id} not found"
            )
        return result

    async def rollback_deployment(
        self, project_id: str, deployment_id: str, request: RollbackRequest
    ) -> DeploymentResponse:
        """Rollback by updating the CRD's git_ref/git_sha to a previous sha."""
        await self._get_deployment_or_raise(project_id, deployment_id)
        # Determine imageTag to restore: use explicit override, or look up from history
        if not settings.should_stamp_image_tag:
            # Defer to operator's LLAMA_DEPLOY_IMAGE_TAG env var
            image_tag = None
        else:
            image_tag = request.image_tag
            if image_tag is None:
                history = await k8s_client.get_deployment_history(deployment_id)
                if history is not None:
                    for item in history.history:
                        if item.git_sha == request.git_sha and item.image_tag:
                            image_tag = item.image_tag
                            break

        # Build an update to set git_sha (and clear git_ref to pin exact commit)
        update = schema.DeploymentUpdate(
            git_ref=None,
            git_sha=request.git_sha,
            image_tag=image_tag,
        )
        updated = await k8s_client.update_deployment(
            deployment_id=deployment_id, update=update
        )
        if updated is None:
            raise DeploymentNotFoundError(
                f"Deployment with id {deployment_id} not found"
            )
        return updated

    @override
    async def update_deployment(
        self,
        project_id: str,
        deployment_id: str,
        update_data: schema.DeploymentUpdate,
    ) -> DeploymentResponse:
        """Update an existing deployment with patch-style changes

        Args:
            project_id: The project ID
            deployment_id: The deployment ID to update
            update_data: The patch-style update data
        """

        current_deployment = await self._get_deployment_or_raise(
            project_id, deployment_id
        )

        if not settings.should_stamp_image_tag:
            # Defer to operator's LLAMA_DEPLOY_IMAGE_TAG env var
            # Local-dev only: this does not clear an already pinned CRD spec.imageTag.
            # Existing dev deployments may need one-time recreation/manual field clear.
            update_data.appserver_version = None
            update_data.image_tag = None
        elif (
            update_data.bump_to_latest_appserver
            and not update_data.appserver_version
            and not update_data.image_tag
        ):
            update_data.image_tag = settings.default_appserver_image_tag

        # If the client sends an empty repo_url but the deployment already uses
        # the internal code repo, drop the field so we don't overwrite it.
        if (
            update_data.repo_url is not None
            and update_data.repo_url.strip() == ""
            and current_deployment.repo_url == INTERNAL_CODE_REPO_SCHEME
        ):
            update_data.repo_url = None

        # If the client is clearing repo_url (switching to push mode), skip
        # git validation — the push callback will set repo_url to internal://
        # after the first successful push.
        clearing_repo_url = (
            update_data.repo_url is not None and update_data.repo_url.strip() == ""
        )

        validated = None
        needs_internal_ref_resolution = (
            update_data.repo_url is not None or update_data.git_ref is not None
        )
        resolved_repo_url = update_data.repo_url or current_deployment.repo_url
        if clearing_repo_url:
            # Switching to push mode — no git validation needed
            pass
        elif (
            needs_internal_ref_resolution
            and resolved_repo_url == INTERNAL_CODE_REPO_SCHEME
        ):
            # Internal repo: resolve the ref from the S3-stored bare repo
            git_ref_to_resolve = update_data.git_ref or current_deployment.git_ref
            if git_ref_to_resolve:
                storage = code_repo_storage
                if storage is None:
                    raise HTTPException(
                        status_code=503,
                        detail="Code repo storage not configured (S3_BUCKET not set).",
                    )
                resolved_sha = await storage.resolve_ref(
                    deployment_id, git_ref_to_resolve
                )
                if resolved_sha:
                    update_data.git_sha = resolved_sha
                else:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Could not resolve ref '{git_ref_to_resolve}' in the internal repo for deployment {deployment_id}. "
                        "Push the ref first or check for typos.",
                    )
        elif (
            update_data.has_git_fields()
            and resolved_repo_url != INTERNAL_CODE_REPO_SCHEME
        ):
            # External repo: validate and resolve via git service
            git_ref_to_resolve = update_data.git_ref or current_deployment.git_ref

            validated = await git_service.validate_git_application(
                repository_url=resolved_repo_url,
                git_ref=git_ref_to_resolve,
                deployment_file_path=update_data.deployment_file_path
                or current_deployment.deployment_file_path,
                pat=update_data.personal_access_token,
                deployment_id=deployment_id,
            )
            update_data.git_sha = validated.git_sha
            update_data.static_assets_path = validated.ui_build_output_path

        updated_deployment = await k8s_client.update_deployment(
            deployment_id=deployment_id,
            update=update_data,
        )
        if updated_deployment is None:
            raise DeploymentNotFoundError(
                f"Deployment with id {deployment_id} not found"
            )

        # Return deployment response with warning header if there are git issues
        if validated is not None and validated.error_message:
            updated_deployment.warning = validated.error_message
        return updated_deployment

    @override
    async def handle_git_request(
        self,
        request: Request,
        project_id: str,
        deployment_id: str,
        git_path: str,
    ) -> Response:
        """Handle git HTTP requests for a deployment's internal code repo."""
        deployment = await self._get_deployment_or_raise(project_id, deployment_id)

        if deployment.repo_url not in ("", INTERNAL_CODE_REPO_SCHEME):
            raise HTTPException(
                status_code=409,
                detail=(
                    "Deployment is configured with an external repository; "
                    "the internal git endpoint is only available for push-mode "
                    "deployments."
                ),
            )

        if code_repo_storage is None:
            return Response(
                content="Code repo storage not configured (S3_BUCKET not set).",
                status_code=503,
            )

        return await _handle_git_request(
            request=request,
            deployment_id=deployment_id,
            git_path=git_path,
            storage=code_repo_storage,
            on_push_complete=_on_push_complete,
        )

    async def _when_replicaset_changes(
        self,
        deployment_id: str,
        interval_seconds: float,
    ) -> AsyncIterator[Literal["__RS_CHANGED__"]]:
        initial_uid = await self._current_rs_uid(deployment_id)
        while True:
            current_uid = await self._current_rs_uid(deployment_id)
            if current_uid != initial_uid:
                yield "__RS_CHANGED__"
                break
            await asyncio.sleep(interval_seconds)

    async def _current_rs_uid(
        self,
        deployment_id: str,
    ) -> str | None:
        rs = await k8s_client.get_latest_replicaset_for_deployment(deployment_id)
        return rs.metadata.uid if rs is not None and rs.metadata is not None else None

    async def _build_log_events(
        self,
        deployment_id: str,
        since_seconds: int | None,
        tail_lines: int | None,
        follow: bool = True,
    ) -> AsyncGenerator[LogEvent, None]:
        """Yield log events from the build Job, if one exists."""
        async for line in k8s_client.stream_build_job_logs(
            deployment_id=deployment_id,
            since_seconds=since_seconds,
            tail_lines=tail_lines,
            stop_event=shutdown_event,
            follow=follow,
        ):
            timestamp = line.timestamp or datetime.now(timezone.utc)
            yield LogEvent(
                pod=line.pod,
                container=line.container,
                text=line.text,
                timestamp=timestamp,
            )

    @override
    async def stream_deployment_logs(
        self,
        project_id: str,
        deployment_id: str,
        include_init_containers: bool = False,
        since_seconds: int | None = None,
        tail_lines: int | None = None,
        follow: bool = True,
    ) -> AsyncGenerator[LogEvent, None]:
        """Stream logs for a deployment.

        Build job logs (if any) are automatically merged into the stream.
        App logs stream from the latest ReplicaSet. When ``follow`` is True
        and the RS changes (e.g. build finishes and operator creates a
        Deployment), the inner generators restart so logs continue seamlessly
        without requiring the client to reconnect. When ``follow`` is False,
        the generator returns whatever logs are currently available and ends.
        """

        await self._get_deployment_or_raise(project_id, deployment_id)

        include_build_logs = True
        while True:
            initial_rs_uid = await self._current_rs_uid(deployment_id)

            app_logs = k8s_client.stream_replicaset_logs(
                deployment_id=deployment_id,
                include_init_containers=include_init_containers,
                since_seconds=since_seconds,
                tail_lines=tail_lines,
                stop_event=shutdown_event,
                follow=follow,
            )

            if include_build_logs:
                build_logs: AsyncGenerator[LogEvent, None] = self._build_log_events(
                    deployment_id=deployment_id,
                    since_seconds=since_seconds,
                    tail_lines=tail_lines,
                    follow=follow,
                )
                include_build_logs = False
            else:
                build_logs = _empty_log_gen()

            # Merge build + app logs; build logs are finite, app logs are ongoing.
            # stop_on_first_completion=False so app logs continue after build finishes.
            merged_logs = merge_generators(
                build_logs,
                app_logs,
                stop_on_first_completion=False,
            )

            debounced_logs = debounced_sorted_prefix(
                merged_logs,
                key=lambda x: (x.timestamp, x.pod, x.container, x.text),
                debounce_seconds=0.2,
                max_window_seconds=0.5,
            )

            if follow:
                when_changes = self._when_replicaset_changes(deployment_id, 0.05)

                rs_changed = False
                async for ev in merge_generators(
                    debounced_logs,
                    cast(AsyncGenerator[LogEvent | str, None], when_changes),
                    stop_on_first_completion=True,
                ):
                    if isinstance(ev, str):
                        # RS-change sentinel — restart inner generators
                        rs_changed = True
                        break
                    timestamp = ev.timestamp or datetime.now(timezone.utc)
                    yield LogEvent(
                        pod=ev.pod,
                        container=ev.container,
                        text=ev.text,
                        timestamp=timestamp,
                    )

                if rs_changed:
                    # RS changed (e.g. build→deploy transition). Loop to pick up
                    # the new RS's pods without dropping the SSE connection.
                    continue

                # All log generators completed without an RS change. Check if an
                # RS appeared while we were streaming build logs (race window
                # where debounced_logs finishes before _when_replicaset_changes
                # polls).
                if initial_rs_uid is None:
                    current_uid = await self._current_rs_uid(deployment_id)
                    if current_uid is not None:
                        continue

                # No new RS appeared — nothing more to stream.
                break

            # follow=False: drain whatever's available and exit. No RS-change
            # watcher, no reconnect — the underlying read uses follow=False
            # against k8s and terminates on its own.
            async for ev in debounced_logs:
                timestamp = ev.timestamp or datetime.now(timezone.utc)
                yield LogEvent(
                    pod=ev.pod,
                    container=ev.container,
                    text=ev.text,
                    timestamp=timestamp,
                )
            break


async def _empty_log_gen() -> AsyncGenerator[LogEvent, None]:
    return
    yield  # make it a generator


deployments_service = DeploymentService()
public_service = PublicDeploymentService()
