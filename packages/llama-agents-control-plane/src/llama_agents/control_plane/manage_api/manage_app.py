import logging
import os
import re
import signal
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import FrameType
from typing import cast

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from llama_agents.core.git.git_util import GitAccessError, GitHostNotAllowedError
from prometheus_fastapi_instrumentator import Instrumentator

from ..k8s_client import DeploymentIdUnavailableError, k8s_health_check
from ..lifecycle import shutdown_event
from .backup_v1beta1 import router as backup_v1beta1
from .deployments_v1beta1 import router as deployments_v1beta1

logger = logging.getLogger(__name__)


_PREV_SIGNAL_HANDLERS: dict[int, signal.Handlers] = {}


# Register early signal handlers so long-running generators can exit promptly
def _handle_shutdown_signal(signum: int, frame: FrameType | None = None) -> None:
    logger.info(f"manage_api signal received: setting shutdown_event ({signum})")
    shutdown_event.set()

    # Chain to any previously-registered handler so the server can still shut down
    prev = _PREV_SIGNAL_HANDLERS.get(signum)
    if callable(prev):
        try:
            prev(signum, frame)
        except Exception:
            # Let exceptions propagate to allow normal shutdown behavior
            raise
    elif prev == signal.SIG_DFL:
        # Restore default then re-emit the signal to trigger default termination
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)
    elif prev == signal.SIG_IGN:
        # Respect ignore; nothing else to do
        pass


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Startup
    global _PREV_SIGNAL_HANDLERS
    shutdown_event.clear()
    for _sig in (signal.SIGINT, signal.SIGTERM):
        prev_handler = signal.getsignal(_sig)
        if prev_handler is not None:
            _PREV_SIGNAL_HANDLERS[_sig] = cast(signal.Handlers, prev_handler)
        signal.signal(_sig, _handle_shutdown_signal)

    yield
    # Ensure shutdown flag is set during app shutdown as a fallback
    shutdown_event.set()


app = FastAPI(title="LlamaDeploy on Cloud", lifespan=lifespan)
Instrumentator().instrument(app).expose(app, include_in_schema=False)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    correlation_id = uuid.uuid4().hex
    logger.exception(
        "Unhandled error on %s %s [correlation_id=%s]",
        request.method,
        request.url.path,
        correlation_id,
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "correlation_id": correlation_id,
        },
    )


_HOST_NOT_ALLOWED_DETAIL = (
    "Could not reach the repository host, or the host is not allowed."
)
# The `user:password@` part of a URL, e.g. a token in `https://bot:token@host/r`.
_URL_USERINFO_RE = re.compile(r"(?<=://)[^/@\s]+@")


def _redact_url_userinfo(text: str) -> str:
    return _URL_USERINFO_RE.sub("***@", text)


@app.exception_handler(GitAccessError)
async def git_access_error_handler(
    request: Request, exc: GitAccessError
) -> JSONResponse:
    """A repository URL or ref the user supplied was rejected or unreachable.

    Host rejections share one generic detail; the log keeps the host and reason.
    """
    message = _redact_url_userinfo(exc.message)
    logger.warning(
        "Rejected git request on %s %s: %s",
        request.method,
        request.url.path,
        message,
    )
    detail = (
        _HOST_NOT_ALLOWED_DETAIL if isinstance(exc, GitHostNotAllowedError) else message
    )
    return JSONResponse(status_code=400, content={"detail": detail})


@app.exception_handler(DeploymentIdUnavailableError)
async def deployment_id_unavailable_handler(
    request: Request, exc: DeploymentIdUnavailableError
) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})


# Include API routers

app.include_router(deployments_v1beta1)
app.include_router(backup_v1beta1)


@app.get("/health")
async def health() -> dict[str, str]:
    """Process check with no k8s dependency; backs the startup and liveness probes.
    Restarting doesn't fix an apiserver outage, so liveness stays independent of it —
    only `/readyz` checks k8s."""
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> JSONResponse:
    """Readiness: exercises the kube-apiserver path so a pod with a wedged
    connection is pulled from Service rotation instead of serving errors."""
    status_code, body = await k8s_health_check()
    return JSONResponse(status_code=status_code, content=body)
