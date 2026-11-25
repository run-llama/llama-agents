from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# When DEFAULT_APPSERVER_IMAGE_TAG is set to this value, the control plane will
# not stamp any image tag on CRDs. The operator's LLAMA_DEPLOY_IMAGE_TAG env var
# will be used instead. This is useful for local development with tilt, where
# the operator's env var is kept in sync with locally-built images.
OPERATOR_DEFAULT_IMAGE_TAG = "operator-default"


class ControlPlaneSettings(BaseSettings):
    model_config = SettingsConfigDict()

    # Kubernetes settings
    kubernetes_namespace: str = Field(
        default="",
        description="Kubernetes namespace to operate in (empty for auto-detection)",
        alias="KUBERNETES_NAMESPACE",
    )

    # Kubernetes client connection pools. Short control-plane reads (CRDs, secrets,
    # events, pod lists) share one pool; long-lived log streams get their own so they
    # cannot starve reads. urllib3 defaults to 4 per pool, which is too small once a
    # handful of log streams are open concurrently.
    k8s_connection_pool_maxsize: int = Field(
        default=32,
        description="Max warm connections for the shared control-plane kube-apiserver client",
        alias="K8S_CONNECTION_POOL_MAXSIZE",
    )
    k8s_streaming_connection_pool_maxsize: int = Field(
        default=16,
        description="Max warm connections for the dedicated log-streaming kube-apiserver client",
        alias="K8S_STREAMING_CONNECTION_POOL_MAXSIZE",
    )

    # Kubernetes client request timeouts. The generated client blocks forever on a
    # half-open apiserver connection unless a timeout is passed on every call; these
    # apply a default so a dead connection fails fast and gets retried on a fresh one
    # instead of wedging the calling thread.
    k8s_request_timeout_seconds: float = Field(
        default=20.0,
        description=(
            "Default request timeout (seconds) for short control-plane kube-apiserver "
            "reads/writes (CRDs, secrets, events, pod lists). Generous on purpose: the "
            "failure mode is infinite hangs, not slow reads."
        ),
        alias="K8S_REQUEST_TIMEOUT_SECONDS",
    )
    k8s_streaming_connect_timeout_seconds: float = Field(
        default=10.0,
        description=(
            "Connect-only timeout (seconds) for the log-streaming kube-apiserver "
            "client. Only bounds establishing the connection; the read timeout stays "
            "unbounded so a live `follow=True` log tail is never killed mid-stream."
        ),
        alias="K8S_STREAMING_CONNECT_TIMEOUT_SECONDS",
    )
    k8s_health_check_timeout_seconds: float = Field(
        default=5.0,
        description=(
            "Timeout (seconds) for the kube-apiserver round-trip behind `/readyz`. "
            "Deliberately short and separate from `k8s_request_timeout_seconds` so "
            "it fits inside the probe's `timeoutSeconds`: a merely-slow apiserver "
            "stays healthy, a dead one fails fast."
        ),
        alias="K8S_HEALTH_CHECK_TIMEOUT_SECONDS",
    )

    # Default appserver image tag (set by Helm chart via DEFAULT_APPSERVER_IMAGE_TAG)
    default_appserver_image_tag: str = Field(
        default="",
        description=(
            "Default appserver image tag to stamp on new deployments. "
            "Empty = don't stamp. "
            f"'{OPERATOR_DEFAULT_IMAGE_TAG}' = defer to operator env var, "
            "ignoring client-requested versions."
        ),
        alias="DEFAULT_APPSERVER_IMAGE_TAG",
    )

    @property
    def should_stamp_image_tag(self) -> bool:
        """Whether the control plane should stamp image tags on CRDs.

        Returns False only when explicitly set to OPERATOR_DEFAULT_IMAGE_TAG.
        Empty string or any real tag value means stamping is enabled.
        """
        return self.default_appserver_image_tag != OPERATOR_DEFAULT_IMAGE_TAG

    # Development settings
    fastapi_env: str = Field(
        default="production",
        description="FastAPI environment mode",
        alias="FASTAPI_ENV",
    )

    # Local development ingress settings
    local_dev_ingress: bool = Field(
        default=False,
        description="Enable ingress for local development",
        alias="LOCAL_DEV_INGRESS",
    )
    local_dev_domain: str = Field(
        default="127.0.0.1.nip.io",
        description="Domain for local development ingress",
        alias="LOCAL_DEV_DOMAIN",
    )

    # Object storage settings
    s3_endpoint_url: str | None = Field(
        default=None,
        description="S3 endpoint URL (for MinIO, R2, etc.)",
        alias="S3_ENDPOINT_URL",
    )
    s3_bucket: str | None = Field(
        default=None,
        description="S3 bucket name",
        alias="S3_BUCKET",
    )
    s3_region: str | None = Field(
        default=None,
        description="S3 region",
        alias="S3_REGION",
    )
    s3_access_key: str | None = Field(
        default=None,
        description="S3 access key",
        alias="S3_ACCESS_KEY",
    )
    s3_secret_key: str | None = Field(
        default=None,
        description="S3 secret key",
        alias="S3_SECRET_KEY",
    )
    s3_unsigned: bool = Field(
        default=False,
        description=(
            "Send S3 requests unsigned (no Authorization header). "
            "Enable for authless S3-compatible backends (s3proxy, LocalStack, "
            "public buckets). Overrides any configured credentials."
        ),
        alias="S3_UNSIGNED",
    )

    # Backup-specific settings
    backup_s3_key_prefix: str = Field(
        default="backups",
        description="S3 key prefix (path) for backup archives",
        alias="BACKUP_S3_KEY_PREFIX",
    )
    backup_encryption_password: str | None = Field(
        default=None,
        description="Password for encrypting secrets in backups. Without it, backups "
        "leave deployment secrets out (unless BACKUP_ALLOW_PLAINTEXT_SECRETS is set).",
        alias="BACKUP_ENCRYPTION_PASSWORD",
    )
    backup_allow_plaintext_secrets: bool = Field(
        default=False,
        description="Write deployment secrets (agent API keys, app env secrets) into "
        "backups unencrypted when no BACKUP_ENCRYPTION_PASSWORD is set. Local use only: "
        "anyone who can read the backup bucket gets every deployment's credentials.",
        alias="BACKUP_ALLOW_PLAINTEXT_SECRETS",
    )

    # Build artifact settings
    build_s3_key_prefix: str = Field(
        default="builds",
        description="S3 key prefix (path) for build artifacts",
        alias="BUILD_S3_KEY_PREFIX",
    )

    # Must exceed the operator's build Job TTLSecondsAfterFinished (3600s) so
    # an artifact whose Job is still within its TTL is guaranteed to exist.
    build_artifact_gc_grace_seconds: int = Field(
        default=4500,
        description="Grace window (seconds) before an unreferenced build artifact is eligible for GC.",
        alias="BUILD_ARTIFACT_GC_GRACE_SECONDS",
    )

    # Code repo settings
    code_repo_s3_key_prefix: str = Field(
        default="git",
        description="S3 key prefix (path) for code repository archives",
        alias="CODE_REPO_S3_KEY_PREFIX",
    )


# Global settings instance
settings = ControlPlaneSettings()
