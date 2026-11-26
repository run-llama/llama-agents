import asyncio
import base64
import functools
import hashlib
import logging
import queue as thread_queue
import random
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    List,
    Literal,
    ParamSpec,
    TypeVar,
    cast,
)

from kubernetes import client
from kubernetes import config as k8s_config
from kubernetes.client import (
    AppsV1Api,
    CoreV1Api,
    CoreV1Event,
    CustomObjectsApi,
    NetworkingV1Api,
    V1Pod,
    V1ReplicaSet,
    VersionApi,
)
from kubernetes.client.api_client import ApiClient
from kubernetes.client.configuration import Configuration
from kubernetes.client.exceptions import ApiException
from llama_agents.core.config import DEFAULT_DEPLOYMENT_FILE_PATH
from llama_agents.core.iter_utils import merge_generators
from llama_agents.core.schema.deployments import (
    DeploymentEvent,
    DeploymentHistoryResponse,
    DeploymentResponse,
    DeploymentUpdate,
    LlamaDeploymentCRD,
    LlamaDeploymentMetadata,
    LlamaDeploymentPhase,
    LlamaDeploymentSpec,
    LlamaDeploymentStatus,
    ReleaseHistoryItem,
    apply_deployment_update,
    image_tag_to_version,
)
from llama_agents.core.schema.projects import ProjectSummary
from pydantic import HttpUrl
from urllib3 import HTTPResponse
from urllib3.exceptions import HTTPError, ProtocolError

from .settings import settings

logger = logging.getLogger(__name__)


P = ParamSpec("P")
R = TypeVar("R")


class _TimeoutApiClient(ApiClient):
    """ApiClient that applies a default (connect, read) timeout to every call.

    The kubernetes client only bounds a request when the caller passes
    ``_request_timeout``; with nothing set, urllib3 blocks forever and a half-open
    apiserver connection wedges the calling thread. No Configuration-level default
    exists, and rest.py passes ``timeout=None`` explicitly (clobbering any pool
    default), so fill it in at ``request()`` — the chokepoint every generated call
    funnels through — instead of threading ``_request_timeout=`` through each site.
    """

    def __init__(self, default_request_timeout: float, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # rest.py builds a urllib3 Timeout only from an int or a 2-tuple, never a
        # bare float (`isinstance(20.0, int)` is False), so store the tuple form.
        self._default_timeout = (default_request_timeout, default_request_timeout)

    def request(self, *args: Any, _request_timeout: Any = None, **kwargs: Any) -> Any:
        # `_request_timeout` is always keyword (see ApiClient.__call_api). Stubs omit
        # `request` from the curated surface, so cast like the rest of this module.
        if _request_timeout is None:
            _request_timeout = self._default_timeout
        return cast(Any, super()).request(
            *args, _request_timeout=_request_timeout, **kwargs
        )


def to_async(func: Callable[P, R]) -> Callable[P, Coroutine[Any, Any, R]]:
    """Decorator that exposes a synchronous function as an async callable.

    Runs the original function in a separate thread using ``asyncio.to_thread``.
    Type hints are preserved using ``ParamSpec``/``TypeVar`` (Python 3.12+).
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return await asyncio.to_thread(func, *args, **kwargs)

    return wrapper


class K8sClient:
    def __init__(self) -> None:
        # Configure namespace
        self.namespace = self._get_namespace()

        # Configure ingress settings from settings
        self.enable_ingress = settings.local_dev_ingress
        self.domain = settings.local_dev_domain

        # Initialize Kubernetes client attributes (will be set lazily)
        self._control_api_client: ApiClient | None = None
        self._streaming_api_client: ApiClient | None = None
        self._k8s_core_v1: CoreV1Api | None = None
        self._k8s_core_v1_streaming: CoreV1Api | None = None
        self._k8s_custom_objects: CustomObjectsApi | None = None
        self._k8s_networking_v1: NetworkingV1Api | None = None
        self._k8s_apps_v1: AppsV1Api | None = None
        self._k8s_version: VersionApi | None = None
        self._k8s_initialized = False

    def _get_namespace(self) -> str:
        """Get the namespace from settings or current pod namespace"""
        # Check settings first
        namespace = settings.kubernetes_namespace
        if namespace:
            return namespace

        # Try to read from service account token (when running in pod)
        try:
            with open(
                "/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r"
            ) as f:
                return f.read().strip()
        except (FileNotFoundError, PermissionError):
            # Fall back to default namespace
            return "llama-agents"

    @staticmethod
    def _build_api_client(
        pool_maxsize: int, default_request_timeout: float | None = None
    ) -> ApiClient:
        """Build an ApiClient with its own urllib3 connection pool.

        Each ApiClient gets a distinct RESTClientObject and therefore a distinct
        PoolManager, so callers backed by different clients cannot contend for the
        same connections. Host/TLS/auth are inherited from the loaded default
        Configuration; only the pool size is overridden.

        When ``default_request_timeout`` is set, the returned client fills in a
        request timeout on every call that doesn't specify one (see
        ``_TimeoutApiClient``). Leave it unset for clients backing long-lived reads
        (log streaming) that must not be killed by a default read timeout.
        """
        # kubernetes stubs omit the get_default_copy classmethod; cast like the
        # rest of this module does for the client's incomplete typing.
        config = cast(Any, Configuration).get_default_copy()
        config.connection_pool_maxsize = pool_maxsize
        if default_request_timeout is None:
            return ApiClient(configuration=config)
        return _TimeoutApiClient(
            default_request_timeout=default_request_timeout, configuration=config
        )

    def _ensure_k8s_client(self) -> None:
        """Initialize Kubernetes client if not already initialized"""
        if not self._k8s_initialized:
            try:
                # Try to load in-cluster config first
                _cfg = cast(Any, k8s_config)
                _cfg.load_incluster_config()
            except Exception:
                # Fall back to local kubeconfig
                _cfg = cast(Any, k8s_config)
                _cfg.load_kube_config()

            # Shared pool for short control-plane reads/writes (CRDs, secrets,
            # events, pod lists) — none are long-lived. Gets a default request
            # timeout so a dead connection fails fast instead of hanging forever.
            self._control_api_client = self._build_api_client(
                settings.k8s_connection_pool_maxsize,
                default_request_timeout=settings.k8s_request_timeout_seconds,
            )
            # Dedicated pool for long-lived log streams so they cannot check out
            # every warm connection and starve the reads above. No default request
            # timeout here — `stream_container_logs` sets its own connect-only
            # timeout per call so `follow=True` reads are never killed mid-stream.
            self._streaming_api_client = self._build_api_client(
                settings.k8s_streaming_connection_pool_maxsize
            )

            self._k8s_core_v1 = client.CoreV1Api(api_client=self._control_api_client)
            self._k8s_core_v1_streaming = client.CoreV1Api(
                api_client=self._streaming_api_client
            )
            self._k8s_custom_objects = client.CustomObjectsApi(
                api_client=self._control_api_client
            )
            self._k8s_networking_v1 = client.NetworkingV1Api(
                api_client=self._control_api_client
            )
            self._k8s_apps_v1 = client.AppsV1Api(api_client=self._control_api_client)
            self._k8s_version = client.VersionApi(api_client=self._control_api_client)
            self._k8s_initialized = True

    @property
    def k8s_core_v1(self) -> CoreV1Api:
        """Lazily initialized CoreV1Api client (shared control pool)"""
        self._ensure_k8s_client()
        assert self._k8s_core_v1 is not None
        return self._k8s_core_v1

    @property
    def k8s_core_v1_streaming(self) -> CoreV1Api:
        """CoreV1Api backed by the dedicated streaming pool.

        Use for long-lived calls (e.g. `read_namespaced_pod_log` with
        `_preload_content=False`) that hold a connection for the stream's
        lifetime, so they don't starve the shared control pool.
        """
        self._ensure_k8s_client()
        assert self._k8s_core_v1_streaming is not None
        return self._k8s_core_v1_streaming

    @property
    def k8s_custom_objects(self) -> CustomObjectsApi:
        """Lazily initialized CustomObjectsApi client"""
        self._ensure_k8s_client()
        assert self._k8s_custom_objects is not None
        return self._k8s_custom_objects

    @property
    def k8s_networking_v1(self) -> NetworkingV1Api:
        """Lazily initialized NetworkingV1Api client"""
        self._ensure_k8s_client()
        assert self._k8s_networking_v1 is not None
        return self._k8s_networking_v1

    @property
    def k8s_apps_v1(self) -> AppsV1Api:
        """Lazily initialized AppsV1Api client"""
        self._ensure_k8s_client()
        assert self._k8s_apps_v1 is not None
        return self._k8s_apps_v1

    @property
    def k8s_version(self) -> VersionApi:
        """VersionApi backed by the control pool, for the `/version` health probe."""
        self._ensure_k8s_client()
        assert self._k8s_version is not None
        return self._k8s_version


# Global k8s client instance
_k8s_client = K8sClient()


async def check_k8s_connectivity() -> None:
    """Round-trip the kube-apiserver through the control pool for `/readyz`.

    Raises on failure or timeout; `/readyz` treats any exception as unhealthy. `GET
    /version` is the lightest real apiserver call — no etcd list, no RBAC — so it
    catches a dead or wedged connection without load. A blind `/health` never touches
    k8s, so a wedged pod would otherwise keep reporting 200. The short per-call
    timeout keeps a slow-but-alive apiserver from reading as a wedge; `wait_for`
    bounds the coroutine even if the underlying thread can't be cancelled.
    """
    timeout = settings.k8s_health_check_timeout_seconds
    # Stubs omit `_request_timeout` from the curated signature; cast to pass it.
    get_code = cast(Callable[..., Any], _k8s_client.k8s_version.get_code)
    await asyncio.wait_for(
        asyncio.to_thread(get_code, _request_timeout=(timeout, timeout)),
        timeout=timeout + 2,
    )


async def k8s_health_check() -> tuple[int, dict[str, str]]:
    """Shared `/readyz` body for both ASGI apps: (status_code, response body).

    Both apps wrap this in their own Response type rather than sharing one, since
    they use different response classes; centralizing the check itself is what
    keeps their pass/fail logic from drifting independently.
    """
    try:
        await check_k8s_connectivity()
    except Exception:
        logger.warning("kube-apiserver health check failed", exc_info=True)
        return 503, {"status": "unhealthy", "reason": "kube-apiserver check failed"}
    return 200, {"status": "ok"}


async def validate_deployment_id(deployment_id: str) -> bool:
    """Check if a deployment ID is available (returns True if available).

    Re-raises any apiserver error other than 404, so a failed lookup is not
    reported as a taken ID.
    """
    try:
        await get_deployment_crd(deployment_id)
        # If we get here, the deployment exists, so ID is not available
        return False
    except ApiException as e:
        if e.status == 404:
            # Deployment doesn't exist, so ID is available
            return True
        logger.error(f"Error checking deployment ID {deployment_id}: {e}")
        raise


async def validate_deployment_token(
    deployment_id: str, token: str
) -> None | LlamaDeploymentCRD:
    """Validate that the token belongs to the specified deployment (O(1) lookup)"""

    try:
        result = await get_deployment_crd(deployment_id)
    except ApiException as e:
        if e.status == 404:
            # Deployment doesn't exist
            return None
        else:
            raise

    if result.status.authToken is None or result.status.authToken != token:
        return None

    return result


def _append_random_suffix(deployment_id: str, max_length: int) -> str:
    randomness = 5
    hex_suffix = "".join(random.choices("0123456789abcdef", k=randomness))
    if not deployment_id:
        # DNS-1035: must start with an alphabetic character
        if hex_suffix[0].isdigit():
            hex_suffix = random.choice("abcdef") + hex_suffix[1:]
        return hex_suffix
    else:
        to_take = max_length - randomness - 1
        return f"{deployment_id[:to_take]}-{hex_suffix}"


def _compute_secret_hash(secrets: dict[str, str]) -> str:
    """Compute a deterministic hash of secret contents to trigger rolling updates"""
    # Sort keys to ensure consistent hash regardless of dict ordering
    sorted_items = sorted(secrets.items())
    content = "|".join(f"{k}={v}" for k, v in sorted_items)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


async def find_deployment_id(name: str, force_suffix: bool = False) -> str:
    max_length = 63  # DNS-1035 label max length
    deployment_id = name.lower()
    deployment_id = re.sub(r"[^a-z0-9]", "-", deployment_id)
    deployment_id = re.sub(r"-+", "-", deployment_id)
    deployment_id = re.sub(r"^-|-$", "", deployment_id)
    # DNS-1035: must start with an alphabetic character
    if deployment_id and not deployment_id[0].isalpha():
        deployment_id = "d-" + deployment_id
    deployment_id = deployment_id[:max_length].rstrip("-")
    base_deployment_id = deployment_id
    if len(deployment_id) < 3 or force_suffix:
        deployment_id = _append_random_suffix(deployment_id, max_length)

    # Try to find a deployment id that is not in use
    for i in range(1, 100):
        if await validate_deployment_id(deployment_id):
            return deployment_id
        deployment_id = _append_random_suffix(base_deployment_id, max_length)

    raise ValueError(
        f"Deployment id {deployment_id} already in use. Could not find alternative"
    )


class DeploymentIdUnavailableError(ValueError):
    """An explicitly requested deployment ID is reserved (400) or already taken (409)."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


# in order to not conflict in URI routes
reserved_deployment_ids = [
    "validate-repository",
    "list-projects",
    "organizations",
    "version",
]


async def create_deployment(
    project_id: str,
    display_name: str,
    repo_url: str,
    deployment_file_path: str | None = None,
    git_ref: str | None = None,
    git_sha: str | None = None,
    pat: str | None = None,
    secrets: dict[str, str] | None = None,
    ui_build_output_path: Path | None = None,
    image_tag: str | None = None,
    explicit_id: str | None = None,
) -> DeploymentResponse:
    """
    Returns a tuple of a DeploymentResponse and a warning message if there were any issues identified
    """

    deployment_file_path = deployment_file_path or DEFAULT_DEPLOYMENT_FILE_PATH

    if explicit_id is not None:
        # Already validated by DeploymentCreate schema; check reserved + uniqueness
        if explicit_id.lower() in reserved_deployment_ids:
            raise DeploymentIdUnavailableError(
                f"Deployment ID {explicit_id!r} is reserved. "
                f"Reserved IDs: {reserved_deployment_ids}",
                status_code=400,
            )
        if not await validate_deployment_id(explicit_id):
            raise DeploymentIdUnavailableError(
                f"Deployment ID {explicit_id!r} is already in use.",
                status_code=409,
            )
        deployment_id = explicit_id
    else:
        is_reserved = display_name.lower() in reserved_deployment_ids
        deployment_id = await find_deployment_id(display_name, force_suffix=is_reserved)

    # Create the secret first if we have secrets
    secret_name = None
    secret_hash = None
    all_secrets = {**({"GITHUB_PAT": pat} if pat else {}), **(secrets or {})}
    if len(all_secrets) > 0:
        secret_name = f"{deployment_id}-secrets"
        await _create_k8s_secret(secret_name, all_secrets)
        secret_hash = _compute_secret_hash(all_secrets)

    # Create the LlamaDeployment custom resource
    llama_metadata = LlamaDeploymentMetadata(
        name=deployment_id,
        namespace=_k8s_client.namespace,
        annotations={}
        if not secret_hash
        else {"deploy.llamaindex.ai/secret-hash": secret_hash},
        labels={
            "deploy.llamaindex.ai/project-id": project_id,
        },
    )
    spec = LlamaDeploymentSpec(
        displayName=display_name,
        projectId=project_id,
        repoUrl=repo_url,
        gitRef=git_ref,
        gitSha=git_sha,
        deploymentFilePath=deployment_file_path,
        secretName=secret_name,
        staticAssetsPath=str(ui_build_output_path) if ui_build_output_path else None,
        imageTag=image_tag,
    )
    llamadeployment = {
        "apiVersion": "deploy.llamaindex.ai/v1",
        "kind": "LlamaDeployment",
        "metadata": llama_metadata.model_dump(),
        "spec": spec.model_dump(),
    }

    await asyncio.to_thread(
        _k8s_client.k8s_custom_objects.create_namespaced_custom_object,
        group="deploy.llamaindex.ai",
        version="v1",
        namespace=_k8s_client.namespace,
        plural="llamadeployments",
        body=llamadeployment,
    )
    logger.info(f"Created LlamaDeployment: {deployment_id}")

    # Create ingress if enabled
    if _k8s_client.enable_ingress:
        await _create_ingress(deployment_id)

    deployment_response = _llamadeployment_to_response(
        LlamaDeploymentCRD(
            metadata=llama_metadata,
            spec=spec,
            status=LlamaDeploymentStatus(
                phase="Pending",
            ),
        ),
        secret_names=list(all_secrets.keys()),
    )
    return deployment_response


async def _create_k8s_secret(secret_name: str, secrets: dict[str, str]) -> None:
    """Create or update a Kubernetes secret with the given secrets"""
    # Encode secrets as base64 (Kubernetes requirement)
    encoded_secrets = {}
    for key, value in secrets.items():
        encoded_secrets[key] = base64.b64encode(value.encode()).decode()

    secret_manifest = client.V1Secret(
        api_version="v1",
        kind="Secret",
        metadata=client.V1ObjectMeta(name=secret_name, namespace=_k8s_client.namespace),
        type="Opaque",
        data=encoded_secrets,
    )

    try:
        # Try to get existing secret
        existing_secret = await asyncio.to_thread(
            _k8s_client.k8s_core_v1.read_namespaced_secret,
            name=secret_name,
            namespace=_k8s_client.namespace,
        )

        # Update existing secret
        if (
            secret_manifest.metadata is not None
            and existing_secret.metadata is not None
        ):
            secret_manifest.metadata.resource_version = (
                existing_secret.metadata.resource_version
            )
        result = await asyncio.to_thread(
            _k8s_client.k8s_core_v1.replace_namespaced_secret,
            name=secret_name,
            namespace=_k8s_client.namespace,
            body=secret_manifest,
        )
        logger.debug(
            f"Updated secret: {result.metadata.name if result and result.metadata else 'unknown'}"
        )

    except ApiException as e:
        if e.status == 404:
            # Secret doesn't exist, create it
            try:
                result = await asyncio.to_thread(
                    _k8s_client.k8s_core_v1.create_namespaced_secret,
                    namespace=_k8s_client.namespace,
                    body=secret_manifest,
                )
                logger.debug(
                    f"Created secret: {result.metadata.name if result and result.metadata else 'unknown'}"
                )
            except ApiException as create_e:
                logger.error(f"Failed to create secret {secret_name}: {create_e}")
                raise
        else:
            # Some other error occurred while trying to read existing secret
            logger.error(f"Failed to read existing secret {secret_name}: {e}")
            raise


async def _create_ingress(service_name: str) -> None:
    """Create or update an ingress for local development"""
    host = f"{service_name}.{_k8s_client.domain}"

    ingress_manifest = client.V1Ingress(
        api_version="networking.k8s.io/v1",
        kind="Ingress",
        metadata=client.V1ObjectMeta(
            name=service_name,
            namespace=_k8s_client.namespace,
            annotations={"kubernetes.io/ingress.class": "nginx"},
        ),
        spec=client.V1IngressSpec(
            rules=[
                client.V1IngressRule(
                    host=host,
                    http=client.V1HTTPIngressRuleValue(
                        paths=[
                            client.V1HTTPIngressPath(
                                path="/",
                                path_type="Prefix",
                                backend=client.V1IngressBackend(
                                    service=client.V1IngressServiceBackend(
                                        name=service_name,
                                        port=client.V1ServiceBackendPort(number=80),
                                    )
                                ),
                            )
                        ]
                    ),
                )
            ]
        ),
    )

    try:
        # Try to get existing ingress
        existing_ingress = await asyncio.to_thread(
            _k8s_client.k8s_networking_v1.read_namespaced_ingress,
            name=service_name,
            namespace=_k8s_client.namespace,
        )

        # Update existing ingress
        if (
            ingress_manifest.metadata is not None
            and existing_ingress.metadata is not None
        ):
            ingress_manifest.metadata.resource_version = (
                existing_ingress.metadata.resource_version
            )
        result = await asyncio.to_thread(
            _k8s_client.k8s_networking_v1.replace_namespaced_ingress,
            name=service_name,
            namespace=_k8s_client.namespace,
            body=ingress_manifest,
        )
        logger.debug(
            f"Updated ingress: {result.metadata.name if result and result.metadata else 'unknown'} at {host}"
        )

    except ApiException as e:
        if e.status == 404:
            # Ingress doesn't exist, create it
            try:
                result = await asyncio.to_thread(
                    _k8s_client.k8s_networking_v1.create_namespaced_ingress,
                    namespace=_k8s_client.namespace,
                    body=ingress_manifest,
                )
                logger.info(
                    f"Created ingress: {result.metadata.name if result and result.metadata else 'unknown'} at {host}"
                )
            except ApiException as create_e:
                # Don't fail the whole deployment if ingress creation fails
                logger.warning(f"Failed to create ingress {service_name}: {create_e}")
        else:
            # Some other error occurred while trying to read existing ingress
            logger.warning(f"Failed to read existing ingress {service_name}: {e}")
    except Exception as e:
        # Don't fail the whole deployment if ingress operations fail
        logger.warning(f"Failed to create/update ingress {service_name}: {e}")


async def delete_deployment(deployment_id: str) -> None:
    """Delete a LlamaDeployment and its associated secret and ingress.

    Raises ApiException for non-404 errors so callers know the delete failed.
    404 errors are swallowed (idempotent delete — resource already gone).
    """
    # Delete the ingress if it exists (and if we create them)
    if _k8s_client.enable_ingress:
        try:
            await asyncio.to_thread(
                _k8s_client.k8s_networking_v1.delete_namespaced_ingress,
                deployment_id,
                _k8s_client.namespace,
            )
            logger.debug(f"Deleted ingress: {deployment_id}")
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete ingress {deployment_id}: {e}")

    # Delete the LlamaDeployment (this will trigger the operator to clean up other resources)
    try:
        await asyncio.to_thread(
            _k8s_client.k8s_custom_objects.delete_namespaced_custom_object,
            group="deploy.llamaindex.ai",
            version="v1",
            namespace=_k8s_client.namespace,
            plural="llamadeployments",
            name=deployment_id,
        )
        logger.info(f"Deleted LlamaDeployment: {deployment_id}")
    except ApiException as e:
        if e.status == 404:
            logger.debug(f"Deployment {deployment_id} already deleted")
        else:
            raise


async def update_deployment(
    deployment_id: str,
    update: DeploymentUpdate,
) -> DeploymentResponse | None:
    """Update an existing LlamaDeployment with the provided changes

    Args:
        deployment_id: The ID of the deployment to update
        update: The changes to apply

    Returns a tuple of a DeploymentResponse and a warning message if there were any issues identified
    """

    # Get the existing deployment - fail if it doesn't exist
    try:
        existing_deployment = await get_deployment_crd(deployment_id)
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise

    existing_spec = existing_deployment.spec

    update_result = apply_deployment_update(update, existing_spec)

    # Convert updated spec back to dict for K8s API
    updated_spec = update_result.updated_spec

    # Handle secret updates if there are any changes
    if update_result.secret_adds or update_result.secret_removes:
        secret_name = existing_spec.secretName

        # Lazily create secret if none exists
        if not secret_name:
            secret_name = f"{deployment_id}-secrets"
            updated_spec.secretName = secret_name

        existing_secrets = {}
        try:
            # Get existing secrets
            secret = await asyncio.to_thread(
                _k8s_client.k8s_core_v1.read_namespaced_secret,
                name=secret_name,
                namespace=_k8s_client.namespace,
            )

            if secret.data:
                for key, value in secret.data.items():
                    existing_secrets[key] = base64.b64decode(value).decode()
        except ApiException as e:
            if e.status != 404:
                raise
            # Secret doesn't exist, start with empty secrets

        # Apply secret changes
        updated_secrets = existing_secrets.copy()

        # Remove secrets
        for secret_key in update_result.secret_removes:
            updated_secrets.pop(secret_key, None)

        # Add/update secrets
        updated_secrets.update(update_result.secret_adds)

        # Update the secret
        await _create_k8s_secret(secret_name, updated_secrets)

        # Add annotation to trigger rolling update when secrets change
        secret_hash = _compute_secret_hash(updated_secrets)

        if existing_deployment.metadata.annotations is None:
            existing_deployment.metadata.annotations = {}
        existing_deployment.metadata.annotations["deploy.llamaindex.ai/secret-hash"] = (
            secret_hash
        )

    # Update the LlamaDeployment CRD
    existing_deployment.spec = updated_spec

    # Construct the full Kubernetes object with required apiVersion and kind
    k8s_object = {
        "apiVersion": "deploy.llamaindex.ai/v1",
        "kind": "LlamaDeployment",
        **existing_deployment.model_dump(exclude_none=True),
    }

    await asyncio.to_thread(
        _k8s_client.k8s_custom_objects.replace_namespaced_custom_object,
        group="deploy.llamaindex.ai",
        version="v1",
        namespace=_k8s_client.namespace,
        plural="llamadeployments",
        name=deployment_id,
        body=k8s_object,
    )
    logger.info(f"Updated LlamaDeployment: {deployment_id}")

    # Return the updated deployment and any warning
    updated_deployment = await get_deployment(deployment_id)
    return updated_deployment


@to_async
def get_deployment_crd(
    deployment_id: str,
) -> LlamaDeploymentCRD:
    """Get the spec of a LlamaDeployment by ID"""
    result = _k8s_client.k8s_custom_objects.get_namespaced_custom_object(
        group="deploy.llamaindex.ai",
        version="v1",
        namespace=_k8s_client.namespace,
        plural="llamadeployments",
        name=deployment_id,
    )
    return LlamaDeploymentCRD.model_validate(result)


async def get_deployment_events(deployment_id: str) -> list[DeploymentEvent]:
    """Get the kubernetes events for a LlamaDeployment by ID"""
    result = await asyncio.to_thread(
        _k8s_client.k8s_core_v1.list_namespaced_event,
        namespace=_k8s_client.namespace,
        field_selector=f"involvedObject.name={deployment_id}",
    )

    items: list[CoreV1Event] = result.items
    return [_event_to_deployment_event(event) for event in items]


def _event_to_deployment_event(event: CoreV1Event) -> DeploymentEvent:
    return DeploymentEvent(
        message=event.message,
        reason=event.reason,
        type=event.type,
        first_timestamp=event.first_timestamp,
        last_timestamp=event.last_timestamp,
        count=event.count,
    )


async def get_deployment(deployment_id: str) -> DeploymentResponse | None:
    """Get a single LlamaDeployment by ID"""
    try:
        result = await get_deployment_crd(deployment_id)

        # Get secret names if secret exists
        secret_names = None
        secret_name = result.spec.secretName
        if secret_name:
            secret_names = await get_secret_names(secret_name)

        return _llamadeployment_to_response(result, secret_names)

    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise


async def get_deployment_history(
    deployment_id: str,
) -> DeploymentHistoryResponse | None:
    """Return the recorded release history for a deployment from its CRD status."""
    try:
        result = await get_deployment_crd(deployment_id)
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise

    status = result.status
    history_crd = status.releaseHistory if status is not None else None
    items: list[ReleaseHistoryItem] = []
    for entry in history_crd or []:
        # Map camelCase to snake_case response
        items.append(
            ReleaseHistoryItem(
                git_sha=entry.gitSha,
                image_tag=entry.imageTag,
                released_at=entry.releasedAt,
            )
        )
    return DeploymentHistoryResponse(deployment_id=deployment_id, history=items)


async def get_deployments(project_id: str) -> List[DeploymentResponse]:
    """Get all LlamaDeployments for a project"""

    # Use label selector to filter by project ID
    label_selector = f"deploy.llamaindex.ai/project-id={project_id}"

    result = await asyncio.to_thread(
        _k8s_client.k8s_custom_objects.list_namespaced_custom_object,
        group="deploy.llamaindex.ai",
        version="v1",
        namespace=_k8s_client.namespace,
        plural="llamadeployments",
        label_selector=label_selector,
    )

    items = result.get("items", [])
    item_crds = [LlamaDeploymentCRD.model_validate(item) for item in items]

    # Collect all unique secret names for batch fetching
    secret_names_to_fetch = set()
    for item in item_crds:
        secret_name = item.spec.secretName
        if secret_name:
            secret_names_to_fetch.add(secret_name)

    # Batch fetch secret names
    secrets_data = await get_secret_names_batch(list(secret_names_to_fetch))

    # Create deployment responses
    deployments = []
    for item in item_crds:
        secret_name = item.spec.secretName
        secret_names = secrets_data.get(secret_name) if secret_name else None
        deployments.append(_llamadeployment_to_response(item, secret_names))

    return deployments


async def get_projects_with_deployment_count() -> List[ProjectSummary]:
    """Get all unique projects with their deployment counts"""
    try:
        # Get all LlamaDeployments
        result = await asyncio.to_thread(
            _k8s_client.k8s_custom_objects.list_namespaced_custom_object,
            "deploy.llamaindex.ai",
            "v1",
            _k8s_client.namespace,
            "llamadeployments",
        )

        # Count deployments by project ID
        project_counts: dict[str, int] = {}
        for item in result.get("items", []):
            project_id = item.get("spec", {}).get("projectId")
            if project_id:
                project_counts[project_id] = project_counts.get(project_id, 0) + 1

        # Convert to ProjectSummary objects and sort
        projects = []
        for project_id, count in sorted(project_counts.items()):
            projects.append(
                ProjectSummary(
                    project_id=project_id,
                    project_name=project_id,
                    deployment_count=count,
                )
            )

        return projects

    except ApiException as e:
        logger.error(f"Failed to get projects with deployment count: {e}")
        return []


@to_async
def get_secret_names(secret_name: str) -> list[str]:
    """Get the key names from a Kubernetes secret"""
    try:
        secret = _k8s_client.k8s_core_v1.read_namespaced_secret(
            name=secret_name, namespace=_k8s_client.namespace
        )
        return list(secret.data.keys()) if secret.data else []
    except ApiException as e:
        if e.status == 404:
            return []
        raise


async def get_secret_names_batch(
    secret_names: list[str],
) -> dict[str, list[str] | None]:
    """Batch fetch secret names for multiple secrets"""
    result: dict[str, list[str] | None] = {}

    async def with_secret_names(secret_name: str) -> tuple[str, list[str] | None]:
        return secret_name, await get_secret_names(secret_name)

    results = await asyncio.gather(*[with_secret_names(name) for name in secret_names])
    for secret_name, names in results:
        result[secret_name] = names
    return result


def _llamadeployment_to_response(
    llamadeployment: LlamaDeploymentCRD, secret_names: list[str] | None = None
) -> DeploymentResponse:
    """Convert a LlamaDeployment custom resource to a DeploymentResponse"""
    metadata = llamadeployment.metadata
    spec = llamadeployment.spec
    status = llamadeployment.status

    # Try to get the apiserver URL from the service or ingress
    apiserver_url = None
    service_name = metadata.name
    if service_name:
        if _k8s_client.enable_ingress:
            # Use ingress URL for local development
            apiserver_url = f"http://{service_name}.{_k8s_client.domain}:8090"
        else:
            # Use service URL for cluster access
            apiserver_url = (
                f"http://{service_name}.{_k8s_client.namespace}.svc.cluster.local"
            )

    # Check if PAT is configured (stored as GITHUB_PAT in the secret)
    has_pat = secret_names is not None and "GITHUB_PAT" in secret_names

    # Filter out GITHUB_PAT from secret names since we have has_personal_access_token flag
    filtered_secret_names: list[str] | None = None
    if secret_names:
        filtered_secret_names = [name for name in secret_names if name != "GITHUB_PAT"]
        if not filtered_secret_names:
            filtered_secret_names = None

    # Derive appserver_version from imageTag when tag follows appserver-X.Y.Z convention
    derived_version = image_tag_to_version(spec.imageTag) if spec.imageTag else None

    # Map internal operator phases to backward-compatible values for old clients
    # that only know the original LlamaDeploymentPhase Literal values.
    raw_phase = status.phase or "Pending"
    warning = None
    if raw_phase == "Building":
        warning = status.message or "Build phase: Building"
        phase: LlamaDeploymentPhase = "Pending"
    elif raw_phase == "BuildFailed":
        warning = status.message or "Build phase: BuildFailed"
        phase = "Failed"
    elif raw_phase == "AwaitingCode":
        warning = status.message or "Waiting for code push"
        phase = "Pending"
    else:
        phase = cast(LlamaDeploymentPhase, raw_phase)

    return DeploymentResponse(
        id=metadata.name,
        display_name=spec.get_display_name(),
        repo_url=spec.repoUrl,
        deployment_file_path=spec.deploymentFilePath,
        git_ref=spec.gitRef,
        git_sha=spec.gitSha,
        has_personal_access_token=has_pat,
        project_id=spec.projectId,
        secret_names=filtered_secret_names,
        apiserver_url=HttpUrl(apiserver_url) if apiserver_url else None,
        status=phase,
        warning=warning,
        appserver_version=derived_version,
        suspended=spec.suspended,
    )


async def has_deployment_pat(deployment_id: str) -> bool:
    """Check if a deployment has an existing PAT configured."""
    return await get_deployment_pat(deployment_id) is not None


async def get_deployment_pat(deployment_id: str) -> str | None:
    """Get the PAT from a deployment's secret if it exists."""
    try:
        # Get the deployment
        deployment = await get_deployment_crd(deployment_id)

        # Get the secret
        secret_name = deployment.spec.secretName
        if not secret_name:
            return None

        secret = await asyncio.to_thread(
            _k8s_client.k8s_core_v1.read_namespaced_secret,
            secret_name,
            _k8s_client.namespace,
        )

        if secret.data and "GITHUB_PAT" in secret.data:
            return base64.b64decode(secret.data["GITHUB_PAT"]).decode()

        return None

    except ApiException as e:
        if e.status == 404:
            return None
        raise
    except Exception:
        return None


def _list_replicasets_for_deployment_sync(deployment_id: str) -> list[Any]:
    """List all ReplicaSets owned by a deployment (sync core).

    Returns an empty list if the deployment is not found.
    """
    try:
        deployment = _k8s_client.k8s_apps_v1.read_namespaced_deployment(
            name=deployment_id, namespace=_k8s_client.namespace
        )
    except ApiException as e:
        if e.status == 404:
            return []
        raise

    if not deployment or not deployment.metadata:
        return []

    deployment_uid = deployment.metadata.uid

    rs_list = _k8s_client.k8s_apps_v1.list_namespaced_replica_set(
        namespace=_k8s_client.namespace,
        label_selector=f"app={deployment_id}",
    )

    result = []
    for rs in rs_list.items or []:
        if not rs.metadata or not rs.metadata.owner_references:
            continue
        for owner in rs.metadata.owner_references:
            if owner.kind == "Deployment" and owner.uid == deployment_uid:
                result.append(rs)
                break

    return result


@to_async
def get_latest_replicaset_for_deployment(
    deployment_id: str,
) -> V1ReplicaSet | None:
    """Return the latest ReplicaSet object for a given apps/v1 Deployment name.

    The operator names the Deployment the same as the LlamaDeployment metadata.name.
    We determine the latest by the highest deployment.kubernetes.io/revision annotation.
    """
    replicasets = _list_replicasets_for_deployment_sync(deployment_id)
    if not replicasets:
        return None

    latest_rs = None
    latest_revision = -1

    for rs in replicasets:
        revision_str = None
        if rs.metadata and rs.metadata.annotations:
            revision_str = rs.metadata.annotations.get(
                "deployment.kubernetes.io/revision"
            )
        try:
            revision = int(revision_str) if revision_str is not None else 0
        except ValueError:
            revision = 0

        if revision > latest_revision:
            latest_revision = revision
            latest_rs = rs

    return latest_rs


@to_async
def list_replicasets_for_deployment(deployment_id: str) -> list[Any]:
    """List all ReplicaSets owned by a deployment."""
    return _list_replicasets_for_deployment_sync(deployment_id)


@to_async
def list_all_deployments() -> list[Any]:
    """List all Deployments managed by the operator."""
    result = _k8s_client.k8s_apps_v1.list_namespaced_deployment(
        namespace=_k8s_client.namespace,
        label_selector="app.kubernetes.io/managed-by=llama-deploy-operator",
    )
    return list(result.items)


@dataclass
class LogLine:
    pod: str
    container: str
    text: str
    # Make timestamp optional and excluded from equality to simplify tests/consumers
    timestamp: datetime | None = field(default=None, compare=False)


async def get_replicaset_pods_for_deployment(deployment_id: str) -> list[V1Pod]:
    """Return pods owned by the latest ReplicaSet for the given deployment ID."""
    latest_rs = await get_latest_replicaset_for_deployment(deployment_id)
    if latest_rs is None or latest_rs.metadata is None:
        return []
    rs_uid = latest_rs.metadata.uid
    if not rs_uid:
        return []

    pods = await asyncio.to_thread(
        _k8s_client.k8s_core_v1.list_namespaced_pod,
        namespace=_k8s_client.namespace,
        label_selector=f"app={deployment_id}",
    )

    target_pods: list[V1Pod] = []
    for pod in pods.items or []:
        if pod.metadata and pod.metadata.owner_references:
            for owner in pod.metadata.owner_references:
                if owner.kind == "ReplicaSet" and owner.uid == rs_uid:
                    target_pods.append(pod)
                    break
    return target_pods


CancelFn = Callable[[], Coroutine[Any, Any, None]]


async def stream_container_logs(
    pod_name: str,
    container_name: str,
    *,
    since_seconds: int | None = None,
    tail_lines: int | None = None,
    follow: bool = True,
) -> tuple[CancelFn, AsyncGenerator[str, None]]:
    """generator for a single container's logs.

    When ``follow=False``, the underlying K8s read returns the currently
    buffered log content and the generator ends naturally; no streaming.
    """

    try:
        read_pod_log = cast(
            Callable[..., HTTPResponse | str],
            _k8s_client.k8s_core_v1_streaming.read_namespaced_pod_log,
        )
        resp = await asyncio.to_thread(
            read_pod_log,
            name=pod_name,
            namespace=_k8s_client.namespace,
            container=container_name,
            follow=follow,
            since_seconds=since_seconds,
            tail_lines=tail_lines,
            timestamps=True,
            _preload_content=False,
            # Connect-only timeout: fail fast if the apiserver is unreachable, but
            # `read=None` keeps the read unbounded so a live `follow=True` tail is
            # never killed mid-stream. This is the one call site that must not have a
            # read timeout.
            _request_timeout=(settings.k8s_streaming_connect_timeout_seconds, None),
        )

        async def cancel() -> None:
            if isinstance(resp, HTTPResponse):
                if not resp.closed:
                    await asyncio.to_thread(resp.shutdown)

        return cancel, _to_generator(resp)
    except (ApiException, HTTPError) as e:
        # Retry the same way for two non-fatal cases: the container isn't ready yet
        # (400/404), or the connect-only timeout tripped opening the stream (a
        # urllib3 HTTPError, not an ApiException). Any other ApiException propagates.
        if isinstance(e, ApiException) and e.status not in (400, 404):
            raise

        # In non-follow mode, just return an empty generator — the caller
        # asked for "what's available now" and there's nothing yet.
        if not follow:

            async def empty() -> AsyncGenerator[str, None]:
                if False:
                    yield ""  # marker to keep this an async generator
                return

            async def noop_cancel() -> None:
                return None

            return noop_cancel, empty()

        async def wait_and_retry() -> tuple[CancelFn, AsyncGenerator[str, None]]:
            await asyncio.sleep(5)

            return await stream_container_logs(
                pod_name,
                container_name,
                since_seconds=since_seconds,
                tail_lines=tail_lines,
                follow=follow,
            )

        task = asyncio.create_task(wait_and_retry())

        async def cancel() -> None:
            if not task.done():
                task.cancel()
            else:
                cancel, _ = task.result()
                await cancel()

        async def gen() -> AsyncGenerator[str, None]:
            _, gen = await task
            async for line in gen:
                yield line

        return cancel, gen()


def _to_generator(resp: str | HTTPResponse) -> AsyncGenerator[str, None]:
    # When _preload_content is not provided, the client returns a full string (or blocks with follow=True)
    # For type-checking simplicity, handle both string and HTTPResponse at runtime.
    if isinstance(resp, str):

        async def gen_from_str() -> AsyncGenerator[str, None]:
            for line in resp.splitlines():
                yield line

        return gen_from_str()

    response: HTTPResponse = resp

    # Use a background thread to read from the blocking HTTPResponse.stream
    # and push decoded lines into a threadsafe queue consumed by an async generator.
    q: thread_queue.Queue[str | None] = thread_queue.Queue(maxsize=100)
    stop_event = threading.Event()

    def reader_thread() -> None:
        buffer = b""
        try:
            for chunk in response.stream(amt=1024, decode_content=False):
                if stop_event.is_set():
                    break
                if not chunk:
                    continue
                buffer += chunk
                while b"\n" in buffer:
                    line_bytes, buffer = buffer.split(b"\n", 1)
                    try:
                        q.put(line_bytes.decode(errors="ignore"), timeout=0.1)
                    except Exception:
                        # Drop if the consumer is not keeping up
                        pass
        except (AttributeError, ProtocolError):
            # Connection hung up or response object doesn't support stream
            pass
        finally:
            if buffer:
                try:
                    q.put(buffer.decode(errors="ignore"), timeout=0.1)
                except Exception:
                    pass
            # Signal end of stream
            try:
                q.put(None, timeout=0.1)
            except Exception:
                pass

    t = threading.Thread(target=reader_thread, daemon=True)
    t.start()

    async def gen_from_http() -> AsyncGenerator[str, None]:
        try:
            while True:
                try:
                    item = await asyncio.to_thread(q.get, True, 0.5)
                except Exception:
                    # Periodically wake to notice cancellation
                    if stop_event.is_set():
                        break
                    continue
                if item is None:
                    break
                yield item
        except asyncio.CancelledError:
            stop_event.set()
            # Best-effort unblock the reader
            try:
                await asyncio.to_thread(q.put, None)
            except Exception:
                pass
            raise
        finally:
            stop_event.set()
            try:
                t.join(timeout=0.2)
            except Exception:
                pass

    return gen_from_http()


_K8S_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z ")


async def _parse_raw_log_lines(
    pod_name: str, container_name: str, raw_gen: AsyncGenerator[str, None]
) -> AsyncGenerator[LogLine, None]:
    """Parse raw K8s log lines (with leading timestamps) into LogLine objects."""
    async for text in raw_gen:
        timestamp_match = _K8S_TIMESTAMP_RE.match(text)
        if timestamp_match:
            raw_ts = timestamp_match.group(0).strip()
            try:
                timestamp = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            except Exception:
                timestamp = datetime.now(timezone.utc)
            text = text[len(timestamp_match.group(0)) :]
        else:
            timestamp = datetime.now(timezone.utc)
        yield LogLine(
            pod=pod_name,
            container=container_name,
            text=text,
            timestamp=timestamp,
        )


async def _stream_pod_container_logs(
    pod_containers: list[tuple[str, str]],
    since_seconds: int | None = None,
    tail_lines: int | None = None,
    stop_event: asyncio.Event | None = None,
    follow: bool = True,
) -> AsyncGenerator[LogLine, None]:
    """Stream and merge log lines from multiple pod/container pairs with shutdown support."""
    generators: list[AsyncGenerator[LogLine, None]] = []
    cancel_fns: list[CancelFn] = []
    for pod_name, container_name in pod_containers:
        cancel, iterator = await stream_container_logs(
            pod_name,
            container_name,
            since_seconds=since_seconds,
            tail_lines=tail_lines,
            follow=follow,
        )
        cancel_fns.append(cancel)
        generators.append(_parse_raw_log_lines(pod_name, container_name, iterator))

    async def when_shutdown() -> AsyncGenerator[Literal["__SHUTDOWN__"], None]:
        if stop_event is None:
            return
        await stop_event.wait()
        yield "__SHUTDOWN__"
        return

    gen_args: list[AsyncGenerator[LogLine | Literal["__SHUTDOWN__"], None]] = [
        *generators
    ]
    if follow:
        gen_args.append(when_shutdown())
    merged = merge_generators(*gen_args)

    try:
        async for item in merged:
            if item == "__SHUTDOWN__":
                break
            yield item
    finally:
        for cancel in cancel_fns:
            await cancel()


async def stream_replicaset_logs(
    deployment_id: str,
    include_init_containers: bool = False,
    since_seconds: int | None = None,
    tail_lines: int | None = None,
    stop_event: asyncio.Event | None = None,
    follow: bool = True,
) -> AsyncGenerator[LogLine, None]:
    """Blocking generator that streams log lines for all pods/containers in the latest ReplicaSet.

    Yields `LogLine` objects until the consumer closes the generator (or, when
    ``follow=False``, until the underlying K8s reads finish).
    """
    target_pods = await get_replicaset_pods_for_deployment(deployment_id)

    if not target_pods:
        return

    pod_containers: list[tuple[str, str]] = []
    for pod in target_pods:
        if not pod.metadata or not pod.spec:
            continue
        pod_name = pod.metadata.name or ""
        containers = [c.name for c in (pod.spec.containers or [])]
        if include_init_containers and pod.spec.init_containers:
            containers.extend(c.name for c in pod.spec.init_containers)
        for c in containers:
            pod_containers.append((pod_name, c))

    async for line in _stream_pod_container_logs(
        pod_containers,
        since_seconds=since_seconds,
        tail_lines=tail_lines,
        stop_event=stop_event,
        follow=follow,
    ):
        yield line


@to_async
def _list_pods_by_label(label_selector: str) -> list[Any]:
    """List pods matching a label selector."""
    result = _k8s_client.k8s_core_v1.list_namespaced_pod(
        namespace=_k8s_client.namespace,
        label_selector=label_selector,
    )
    return list(result.items)


async def stream_build_job_logs(
    deployment_id: str,
    build_id: str | None = None,
    since_seconds: int | None = None,
    tail_lines: int | None = None,
    stop_event: asyncio.Event | None = None,
    follow: bool = True,
) -> AsyncGenerator[LogLine, None]:
    """Stream log lines from a build Job's pod.

    If build_id is not provided, finds the most recent build Job for the deployment.
    """
    label_selector = f"deploy.llamaindex.ai/deployment={deployment_id}"
    if build_id:
        label_selector += f",deploy.llamaindex.ai/build-id={build_id}"

    pods = await _list_pods_by_label(label_selector)

    if not pods:
        return

    pod_containers: list[tuple[str, str]] = []
    for pod in pods:
        if not pod.metadata or not pod.spec:
            continue
        pod_name = pod.metadata.name or ""
        for c in pod.spec.containers or []:
            pod_containers.append((pod_name, c.name))

    async for line in _stream_pod_container_logs(
        pod_containers,
        since_seconds=since_seconds,
        tail_lines=tail_lines,
        stop_event=stop_event,
        follow=follow,
    ):
        yield line


# === Backup/Restore helpers ===


@to_async
def get_secret_data(secret_name: str) -> dict[str, str] | None:
    """Read full secret data, base64-decoding values. Returns None if not found."""
    try:
        secret = _k8s_client.k8s_core_v1.read_namespaced_secret(
            name=secret_name, namespace=_k8s_client.namespace
        )
        if not secret.data:
            return {}
        return {k: base64.b64decode(v).decode() for k, v in secret.data.items()}
    except ApiException as e:
        if e.status == 404:
            return None
        raise


@to_async
def get_all_deployment_crds() -> list[dict[str, Any]]:
    """List all LlamaDeployment CRDs as raw dicts."""
    result = _k8s_client.k8s_custom_objects.list_namespaced_custom_object(
        group="deploy.llamaindex.ai",
        version="v1",
        namespace=_k8s_client.namespace,
        plural="llamadeployments",
    )
    return list(result.get("items", []))


async def apply_deployment_crd(crd: dict[str, Any]) -> None:
    """Create or replace a LlamaDeployment CRD from a raw dict."""
    name = crd["metadata"]["name"]
    try:
        await asyncio.to_thread(
            _k8s_client.k8s_custom_objects.create_namespaced_custom_object,
            group="deploy.llamaindex.ai",
            version="v1",
            namespace=_k8s_client.namespace,
            plural="llamadeployments",
            body=crd,
        )
        logger.info("Created LlamaDeployment from backup: %s", name)
    except ApiException as e:
        if e.status == 409:
            # Already exists — fetch current resourceVersion and replace
            existing = await asyncio.to_thread(
                _k8s_client.k8s_custom_objects.get_namespaced_custom_object,
                group="deploy.llamaindex.ai",
                version="v1",
                namespace=_k8s_client.namespace,
                plural="llamadeployments",
                name=name,
            )
            crd["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
            await asyncio.to_thread(
                _k8s_client.k8s_custom_objects.replace_namespaced_custom_object,
                group="deploy.llamaindex.ai",
                version="v1",
                namespace=_k8s_client.namespace,
                plural="llamadeployments",
                name=name,
                body=crd,
            )
            logger.info("Replaced LlamaDeployment from backup: %s", name)
        else:
            raise


async def apply_secret(name: str, data: dict[str, str]) -> None:
    """Create or replace a K8s secret with pre-decoded string values."""
    await _create_k8s_secret(name, data)


def get_namespace() -> str:
    """Return the namespace the control plane is running in."""
    return _k8s_client.namespace


@to_async
def get_deployment_crd_raw(name: str) -> dict[str, Any] | None:
    """Get a LlamaDeployment CR as a raw dict, or None if not found."""
    try:
        return _k8s_client.k8s_custom_objects.get_namespaced_custom_object(
            group="deploy.llamaindex.ai",
            version="v1",
            namespace=_k8s_client.namespace,
            plural="llamadeployments",
            name=name,
        )
    except ApiException as e:
        if e.status == 404:
            return None
        raise


@to_async
def delete_deployment_crd(name: str) -> None:
    """Delete a LlamaDeployment CR by name."""
    _k8s_client.k8s_custom_objects.delete_namespaced_custom_object(
        group="deploy.llamaindex.ai",
        version="v1",
        namespace=_k8s_client.namespace,
        plural="llamadeployments",
        name=name,
    )
