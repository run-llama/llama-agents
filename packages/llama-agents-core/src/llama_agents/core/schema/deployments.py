import re
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, HttpUrl, computed_field, model_validator

from .base import Base

APPSERVER_TAG_PREFIX = "appserver-"

# DNS-1035 label: starts with lowercase letter, lowercase alphanumeric + hyphens,
# max 63 chars, no trailing hyphen.
_DNS_1035_RE = re.compile(r"^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")


def validate_dns_1035_label(value: str) -> str:
    """Validate that a string is a valid DNS-1035 label.

    Rules: starts with lowercase letter, only lowercase alphanumeric and hyphens,
    max 63 chars, no trailing hyphen.

    Raises ValueError if invalid.
    """
    if not _DNS_1035_RE.match(value):
        raise ValueError(
            f"Invalid DNS-1035 label: {value!r}. Must start with a lowercase letter, "
            "contain only lowercase alphanumeric characters and hyphens, "
            "max 63 characters, and not end with a hyphen."
        )
    return value


# Sentinel URL scheme used in the CRD's repoUrl to indicate
# the deployment's code is stored in the internal S3-backed repo.
INTERNAL_CODE_REPO_SCHEME = "internal://"


def version_to_image_tag(version: str) -> str:
    """Convert an appserver_version like '0.4.2' to an image tag like '0.4.2'."""
    return version


def image_tag_to_version(tag: str) -> str | None:
    """Extract version from image tag.

    Handles both new plain tags ('0.4.2') and legacy prefixed tags ('appserver-0.4.2').
    Returns the version string, or None if the tag is not a recognized version format.
    """
    if tag.startswith(APPSERVER_TAG_PREFIX):
        return tag.removeprefix(APPSERVER_TAG_PREFIX)
    # New-style plain version tags — check if it looks like a version (starts with digit)
    if tag and tag[0].isdigit():
        return tag
    return None


# K8s CRD phase values
LlamaDeploymentPhase = Literal[
    "Pending",  # Waiting for deployment resources to be ready (pods starting up)
    "Running",  # Deployment is healthy and serving traffic
    "Failed",  # Complete deployment failure - no pods available
    "RollingOut",  # Rolling update in progress - new pods being created while old ones still serve traffic
    "RolloutFailed",  # New deployment failed but old pods are still available and serving traffic
    "Suspended",  # Deployment is suspended (scaled to 0 replicas)
    "Building",  # Build Job is in progress
    "BuildFailed",  # Build Job failed
]


class DeploymentEvent(Base):
    message: str | None = Field(
        default=None, description="Human-readable event message"
    )
    reason: str | None = Field(
        default=None, description="Machine-readable reason string"
    )
    type: str | None = Field(default=None, description="Event type (Normal or Warning)")
    first_timestamp: datetime | None = Field(
        default=None, description="When this event was first observed"
    )
    last_timestamp: datetime | None = Field(
        default=None, description="When this event was last observed"
    )
    count: int | None = Field(
        default=None, description="Number of times this event has occurred"
    )


class DeploymentResponse(Base):
    id: str = Field(description="Stable DNS-safe identifier for the deployment")
    display_name: str = Field(description="User-facing display label")
    repo_url: str = Field(description="Git repository URL for the deployment source")
    deployment_file_path: str = Field(
        description="Path to the deployment config file within the repository"
    )
    git_ref: str | None = Field(
        default=None, description="Git reference (branch, tag, or commit) to deploy"
    )
    git_sha: str | None = Field(
        default=None, description="Resolved git commit SHA of the current deployment"
    )
    has_personal_access_token: bool = Field(
        default=False,
        description="Whether a personal access token is configured for repo access",
    )
    project_id: str = Field(description="ID of the project this deployment belongs to")
    secret_names: list[str] | None = Field(
        default=None,
        description="Names of configured secrets (excluding GITHUB_PAT)",
    )
    apiserver_url: HttpUrl | None = Field(
        default=None, description="URL of the deployment's API server"
    )
    # `LlamaDeploymentPhase | str`: tolerate unknown phase values from a newer
    # server. Known phases still narrow to the Literal arm; unknown ones fall
    # through to `str` instead of failing validation on older clients.
    status: LlamaDeploymentPhase | str = Field(description="Current deployment phase")
    warning: str | None = Field(
        default=None,
        description="Warning message about the deployment state",
    )
    events: list[DeploymentEvent] | None = Field(
        default=None, description="Recent Kubernetes events for this deployment"
    )
    appserver_version: str | None = Field(
        default=None, description="Appserver version (e.g. '0.4.2')"
    )
    suspended: bool = Field(
        default=False, description="Whether the deployment is scaled to 0 replicas"
    )

    @computed_field(description="Deprecated: use display_name")
    @property
    def name(self) -> str:
        return self.display_name

    @computed_field(description="Deprecated: use appserver_version")
    @property
    def llama_deploy_version(self) -> str | None:
        return self.appserver_version

    @model_validator(mode="before")
    @classmethod
    def _compat_aliases(cls, data: dict) -> dict:  # type: ignore[type-arg]
        """Accept deprecated 'name' and 'llama_deploy_version' input aliases."""
        if isinstance(data, dict):
            if not data.get("display_name") and data.get("name"):
                data["display_name"] = data["name"]
            if not data.get("appserver_version") and data.get("llama_deploy_version"):
                data["appserver_version"] = data["llama_deploy_version"]
        return data


class DeploymentsListResponse(Base):
    deployments: list[DeploymentResponse] = Field(
        description="List of deployments in the project"
    )


class DeploymentCreate(Base):
    display_name: str = Field(description="User-facing display label")
    id: str | None = Field(
        default=None,
        description="Optional explicit DNS-safe identifier. "
        "If omitted, generated from display_name.",
    )
    repo_url: str = Field(default="", description="Git repository URL to deploy from")
    deployment_file_path: str | None = Field(
        default=None,
        description="Path to the deployment config file within the repository",
    )
    git_ref: str | None = Field(
        default=None,
        description="Git reference (branch, tag, or commit) to deploy",
    )
    personal_access_token: str | None = Field(
        default=None, description="Personal access token for private repo access"
    )
    secrets: dict[str, str] | None = Field(
        default=None,
        description="Key-value pairs to store as deployment secrets",
    )
    appserver_version: str | None = Field(
        default=None,
        description="Appserver version to use (e.g. '0.4.2'). "
        "If omitted, server may set based on client version.",
    )

    @model_validator(mode="before")
    @classmethod
    def _compat_aliases(cls, data: dict) -> dict:  # type: ignore[type-arg]
        """Accept deprecated 'name' and 'llama_deploy_version' input aliases."""
        if isinstance(data, dict):
            if data.get("name") and not data.get("display_name"):
                data["display_name"] = data.pop("name")
            if data.get("llama_deploy_version") and not data.get("appserver_version"):
                data["appserver_version"] = data.pop("llama_deploy_version")
        return data

    @computed_field(description="Deprecated: use display_name")
    @property
    def name(self) -> str:
        return self.display_name

    @computed_field(description="Deprecated: use appserver_version")
    @property
    def llama_deploy_version(self) -> str | None:
        return self.appserver_version

    @model_validator(mode="after")
    def _require_id_format(self) -> "DeploymentCreate":
        if self.id is not None:
            validate_dns_1035_label(self.id)
        return self


class LlamaDeploymentMetadata(Base):
    name: str
    namespace: str
    uid: str | None = None
    resourceVersion: str | None = None
    creationTimestamp: datetime | None = None
    annotations: dict[str, str] | None = None
    labels: dict[str, str] | None = None


class LlamaDeploymentSpec(Base):
    """
    LlamaDeployment spec fields as defined in the Kubernetes CRD.

    Maps to the spec section of the LlamaDeployment custom resource.
    Field names match exactly with the K8s CRD for direct conversion.
    """

    projectId: str
    repoUrl: str
    deploymentFilePath: str = "."
    gitRef: str | None = None
    gitSha: str | None = None
    displayName: str | None = None
    secretName: str | None = None
    # when true, the deployment will prebuild the UI assets and serve them from a static file server
    staticAssetsPath: str | None = None
    # explicit imageTag (operator will use this if provided)
    imageTag: str | None = None
    suspended: bool = False
    # monotonically increasing counter to force a new build (retry failed builds)
    buildGeneration: int = 0

    @model_validator(mode="before")
    @classmethod
    def _migrate_name(cls, data: dict) -> dict:  # type: ignore[type-arg]
        """Migrate deprecated spec.name → spec.displayName from old CRDs."""
        if isinstance(data, dict):
            if data.get("name") and not data.get("displayName"):
                data["displayName"] = data.pop("name")
        return data

    def get_display_name(self) -> str:
        """Return the display name."""
        return self.displayName or ""


class LlamaDeploymentStatus(Base):
    """
    LlamaDeployment status fields as defined in the Kubernetes CRD.

    Maps to the status section of the LlamaDeployment custom resource.
    """

    phase: str | None = None
    message: str | None = None
    lastUpdated: datetime | None = None
    authToken: str | None = None
    # Historical list of released versions from the CRD (camelCase fields)
    releaseHistory: list["ReleaseHistoryEntry"] | None = None


class LlamaDeploymentCRD(Base):
    metadata: LlamaDeploymentMetadata
    spec: LlamaDeploymentSpec
    status: LlamaDeploymentStatus = Field(default_factory=LlamaDeploymentStatus)


class DeploymentUpdate(Base):
    """
    Patch-style update model for deployments.

    Fields set to None remain unchanged.

    For secrets: provide a dict where string values add/update secrets
    and null values remove secrets.
    """

    display_name: str | None = Field(
        default=None, description="Updated user-facing display label"
    )
    repo_url: str | None = Field(default=None, description="Updated git repository URL")
    deployment_file_path: str | None = Field(
        default=None,
        description="Updated path to the deployment config file",
    )
    git_ref: str | None = Field(
        default=None, description="Updated git reference to deploy"
    )
    git_sha: str | None = Field(
        default=None, description="Resolved git commit SHA (set by service layer)"
    )
    personal_access_token: str | None = Field(
        default=None,
        description="Updated personal access token. Empty string removes it.",
    )
    secrets: dict[str, str | None] | None = Field(
        default=None,
        description="Secret updates: string values add/update, null values remove",
    )
    static_assets_path: Path | None = Field(
        default=None, description="Path to prebuilt UI assets (set by service layer)"
    )
    appserver_version: str | None = Field(
        default=None, description="Updated appserver version selector"
    )
    image_tag: str | None = Field(
        default=None,
        description="Explicit image tag (takes precedence over appserver_version)",
    )
    bump_to_latest_appserver: bool = Field(
        default=False,
        description="Bump the appserver image to the cluster's current default version",
    )
    suspended: bool | None = Field(
        default=None, description="Set to true to suspend (scale to 0), false to resume"
    )
    rebuild: bool = Field(
        default=False,
        description="Force a rebuild (e.g. retry after transient network failure)",
    )

    @computed_field(description="Deprecated: use display_name")
    @property
    def name(self) -> str | None:
        return self.display_name

    @computed_field(description="Deprecated: use appserver_version")
    @property
    def llama_deploy_version(self) -> str | None:
        return self.appserver_version

    @model_validator(mode="before")
    @classmethod
    def _compat_aliases(cls, data: dict) -> dict:  # type: ignore[type-arg]
        """Accept deprecated 'name' and 'llama_deploy_version' input aliases."""
        if isinstance(data, dict):
            if data.get("name") and not data.get("display_name"):
                data["display_name"] = data.pop("name")
            if data.get("llama_deploy_version") and not data.get("appserver_version"):
                data["appserver_version"] = data.pop("llama_deploy_version")
        return data

    def has_git_fields(self) -> bool:
        """Return True if any git-affecting fields are set."""
        return any(
            [
                self.repo_url is not None,
                self.deployment_file_path is not None,
                self.git_ref is not None,
                self.personal_access_token is not None,
            ]
        )

    def _has_substantive_fields(self) -> bool:
        """Return True if update contains fields that affect the running deployment."""
        return any(
            [
                self.repo_url is not None,
                self.deployment_file_path is not None,
                self.git_ref is not None,
                self.personal_access_token is not None,
                self.secrets is not None,
                self.image_tag is not None,
                self.appserver_version is not None,
                self.bump_to_latest_appserver,
            ]
        )


class DeploymentUpdateResult(Base):
    """
    Result of applying a DeploymentUpdate to a LlamaDeploymentSpec.

    Contains the updated spec and lists of secret changes to apply.
    """

    updated_spec: LlamaDeploymentSpec
    secret_adds: dict[str, str]
    secret_removes: list[str]


def apply_deployment_update(
    update: DeploymentUpdate,
    existing_spec: LlamaDeploymentSpec,
) -> DeploymentUpdateResult:
    """
    Apply a DeploymentUpdate to an existing LlamaDeploymentSpec.

    Returns the updated spec and lists of secret changes.

    Args:
        update: The update to apply (snake_case fields from API)
        existing_spec: The current LlamaDeploymentSpec (camelCase fields)
        git_sha: The resolved git SHA to set

    Returns:
        DeploymentUpdateResult with updated spec and secret changes
    """
    # Start with a copy of the existing spec
    updated_spec = existing_spec.model_copy()

    # Apply direct field updates (only if not None)
    # Convert from snake_case API fields to camelCase spec fields
    if update.display_name is not None:
        updated_spec.displayName = update.display_name

    if update.repo_url is not None:
        updated_spec.repoUrl = update.repo_url

    if update.deployment_file_path is not None:
        updated_spec.deploymentFilePath = update.deployment_file_path

    if update.git_ref is not None:
        updated_spec.gitRef = update.git_ref

    # Update gitSha if provided
    if update.git_sha is not None:
        updated_spec.gitSha = None if update.git_sha == "" else update.git_sha

    if update.static_assets_path is not None:
        updated_spec.staticAssetsPath = str(update.static_assets_path)

    # Track secret changes
    secret_adds: dict[str, str] = {}
    secret_removes: list[str] = []

    # Handle personal access token (stored as GITHUB_PAT secret)
    if update.personal_access_token is not None:
        if update.personal_access_token == "":
            # Empty string means remove the PAT
            secret_removes.append("GITHUB_PAT")
        else:
            # Non-empty string means add/update the PAT
            secret_adds["GITHUB_PAT"] = update.personal_access_token

    # Handle explicit secret updates
    secrets = update.secrets
    if secrets is not None:
        for key, value in secrets.items():
            if value is None:
                # None means remove this secret
                secret_removes.append(key)
            else:
                # String value means add/update this secret
                secret_adds[key] = value

    if update.suspended is not None:
        updated_spec.suspended = update.suspended

    # Auto-resume: if the deployment was suspended and this update contains
    # substantive fields (but doesn't explicitly set suspended), resume it.
    if (
        update.suspended is None
        and existing_spec.suspended
        and update._has_substantive_fields()
    ):
        updated_spec.suspended = False

    # Handle image tag / version selector (image_tag takes precedence)
    if update.image_tag is not None:
        updated_spec.imageTag = update.image_tag
    elif update.appserver_version is not None:
        updated_spec.imageTag = version_to_image_tag(update.appserver_version)

    # Bump buildGeneration to force a rebuild (e.g. retry after transient failure)
    if update.rebuild:
        updated_spec.buildGeneration = existing_spec.buildGeneration + 1

    return DeploymentUpdateResult(
        updated_spec=updated_spec,
        secret_adds=secret_adds,
        secret_removes=secret_removes,
    )


class LogEvent(Base):
    pod: str = Field(description="Name of the Kubernetes pod")
    container: str = Field(description="Name of the container within the pod")
    text: str = Field(description="Log line content")
    timestamp: datetime = Field(description="When the log line was emitted")


# ===== Release history models =====


class ReleaseHistoryEntry(Base):
    """
    Mirrors the CRD status.releaseHistory entry with camelCase keys.
    """

    gitSha: str
    imageTag: str | None = None
    releasedAt: datetime


class ReleaseHistoryItem(Base):
    """
    API-exposed release history item with snake_case keys for clients.
    """

    git_sha: str = Field(description="Git commit SHA for this release")
    image_tag: str | None = Field(
        default=None, description="Appserver image tag used for this release"
    )
    released_at: datetime = Field(description="When this version was released")


class DeploymentHistoryResponse(Base):
    deployment_id: str = Field(description="ID of the deployment")
    history: list[ReleaseHistoryItem] = Field(
        description="List of released versions, newest first"
    )


class RollbackRequest(Base):
    git_sha: str = Field(description="Git commit SHA to roll back to")
    image_tag: str | None = Field(
        default=None, description="Image tag to use for the rollback"
    )
