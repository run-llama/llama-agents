import argparse
import json
import logging
import os
import threading
import time
import webbrowser
from contextlib import asynccontextmanager
from importlib.metadata import version
from pathlib import Path
from typing import Any, AsyncGenerator, Literal, cast

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from llama_agents.appserver.configure_logging import (
    add_log_middleware,
    setup_logging,
)
from llama_agents.appserver.deployment_config_parser import (
    get_deployment_config,
)
from llama_agents.appserver.routers.deployments import (
    create_base_router,
    create_deployments_router,
)
from llama_agents.appserver.routers.ui_proxy import (
    create_ui_proxy_router,
    mount_static_files,
)
from llama_agents.appserver.settings import configure_settings, settings
from llama_agents.appserver.workflow_loader import (
    _exclude_venv_warning,
    build_ui,
    inject_appserver_into_target,
    install_ui,
    load_environment_variables,
    load_workflow_server,
    load_workflows,
    start_dev_ui_process,
    validate_required_env_vars,
)
from llama_agents.core.config import DEFAULT_DEPLOYMENT_FILE_PATH
from llama_agents.server import WorkflowServer
from prometheus_fastapi_instrumentator import Instrumentator

from .deployment import Deployment
from .interrupts import shutdown_event
from .process_utils import run_process
from .routers import health_router
from .stats import apiserver_state

logger = logging.getLogger("uvicorn.info")

# Auto-configure logging on import when requested (e.g., uvicorn reload workers)
if os.getenv("LLAMA_DEPLOY_AUTO_LOGGING", "0") == "1":
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, Any]:
    shutdown_event.clear()
    apiserver_state.state("starting")
    config = get_deployment_config()

    source_server = load_workflow_server(config)
    deployment = Deployment(
        source_server.get_workflows(), serializer=source_server.serializer
    )
    base_router = create_base_router(config.name)
    server = deployment.mount_workflow_server(app)
    deploy_router = create_deployments_router(config.name, deployment)

    app.include_router(base_router)
    app.include_router(deploy_router)

    _setup_openapi(config.name, app, server)

    if config.ui is not None:
        if settings.proxy_ui:
            ui_router = create_ui_proxy_router(config.name, settings.proxy_ui_port)
            app.include_router(ui_router)
        else:
            # otherwise serve the pre-built if available
            mount_static_files(app, config, settings)

        @app.get(f"/deployments/{config.name}", include_in_schema=False)
        @app.get(f"/deployments/{config.name}/", include_in_schema=False)
        @app.get(f"/deployments/{config.name}/ui", include_in_schema=False)
        def redirect_to_ui() -> RedirectResponse:
            return RedirectResponse(f"/deployments/{config.name}/ui/")
    else:

        @app.get(f"/deployments/{config.name}", include_in_schema=False)
        @app.get(f"/deployments/{config.name}/", include_in_schema=False)
        def redirect_to_docs() -> RedirectResponse:
            return RedirectResponse(f"/deployments/{config.name}/docs")

    apiserver_state.state("running")
    # terrible sad cludge
    async with server.contextmanager():
        yield

    apiserver_state.state("stopped")


def _setup_openapi(name: str, app: FastAPI, server: WorkflowServer) -> None:
    """
    extends the fastapi based openapi schema with starlette generated schema
    """
    schema_title = "Llama Deploy App Server"
    app_version = version("llama-agents-appserver")

    prefix = f"/deployments/{name}"

    schema = server.openapi_schema()
    schema["info"]["title"] = schema_title
    schema["info"]["version"] = app_version
    paths = cast(dict, schema["paths"])
    new_paths = {}
    for path, methods in list(paths.items()):
        if "head" in methods:
            methods.pop("head")
        new_paths[prefix + path] = methods

    schema["paths"] = new_paths

    def custom_openapi() -> dict[str, object]:
        return schema

    app.openapi = custom_openapi  # ty: ignore[invalid-assignment] - doesn't like us overwriting the method


_config = get_deployment_config()
_prefix = f"/deployments/{_config.name}"
app = FastAPI(
    lifespan=lifespan,
    docs_url=_prefix + "/docs",
    redoc_url=_prefix + "/redoc",
    openapi_url=_prefix + "/openapi.json",
)
Instrumentator(
    excluded_handlers=[
        "/health.*",
        "/livez",
        "/readyz",
        "/metrics",
        "^/$",
        "/deployments/.+/ui",
        "/deployments/[^/]+/?$",
    ],
).instrument(
    app,
    latency_highr_buckets=(0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30),
    latency_lowr_buckets=(0.1, 0.5, 1),
).expose(app, include_in_schema=False)


def _configure_cors(app: FastAPI) -> None:
    """Attach CORS middleware in a way that keeps type-checkers happy."""
    # Use a cast here because ty's view of Starlette's middleware factory
    # protocol is stricter than FastAPI's runtime expectations.
    app.add_middleware(
        cast(Any, CORSMiddleware),
        allow_origins=["*"],  # Allows all origins
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "Authorization"],
    )


if not os.environ.get("DISABLE_CORS", False):
    _configure_cors(app)

app.include_router(health_router)
add_log_middleware(app)


def open_browser_async(host: str, port: int) -> None:
    def _open_with_delay() -> None:
        time.sleep(1)
        webbrowser.open(f"http://{host}:{port}")

    threading.Thread(target=_open_with_delay).start()


def prepare_server(
    deployment_file: Path | None = None,
    install: bool = False,
    build: bool = False,
    install_ui_deps: bool = True,
    skip_env_validation: bool = False,
) -> None:
    configure_settings(
        deployment_file_path=deployment_file or Path(DEFAULT_DEPLOYMENT_FILE_PATH)
    )
    cfg = get_deployment_config()
    load_environment_variables(cfg, settings.resolved_config_parent)
    validate_required_env_vars(cfg, fill_missing=skip_env_validation)
    if install:
        config = get_deployment_config()
        inject_appserver_into_target(config, settings.resolved_config_parent)
        if install_ui_deps:
            install_ui(config, settings.resolved_config_parent)
    if build:
        build_ui(settings.resolved_config_parent, get_deployment_config(), settings)


def start_server(
    proxy_ui: bool = False,
    reload: bool = False,
    cwd: Path | None = None,
    deployment_file: Path | None = None,
    open_browser: bool = False,
    configure_logging: bool = True,
) -> None:
    # Configure via environment so uvicorn reload workers inherit the values
    configure_settings(
        proxy_ui=proxy_ui,
        app_root=cwd,
        deployment_file_path=deployment_file or Path(DEFAULT_DEPLOYMENT_FILE_PATH),
        reload=reload,
    )
    cfg = get_deployment_config()
    load_environment_variables(cfg, settings.resolved_config_parent)
    validate_required_env_vars(cfg)

    ui_process = None
    if proxy_ui:
        ui_process = start_dev_ui_process(
            settings.resolved_config_parent, settings, get_deployment_config()
        )
    try:
        if open_browser:
            open_browser_async(settings.host, settings.port)
        # Ensure reload workers configure logging on import
        os.environ["LLAMA_DEPLOY_AUTO_LOGGING"] = "1"
        # Configure logging for the launcher process as well
        if configure_logging:
            setup_logging(os.getenv("LOG_LEVEL", "INFO"))

        uvicorn.run(
            "llama_agents.appserver.app:app",
            host=settings.host,
            port=settings.port,
            reload=reload,
            reload_dirs=["src"] if reload else None,
            timeout_graceful_shutdown=1,
            access_log=False,
            log_config=None,
        )
    finally:
        if ui_process is not None:
            ui_process.terminate()


def start_server_in_target_venv(
    proxy_ui: bool = False,
    reload: bool = False,
    cwd: Path | None = None,
    deployment_file: Path | None = None,
    open_browser: bool = False,
    port: int | None = None,
    ui_port: int | None = None,
    log_level: str | None = None,
    log_format: str | None = None,
    persistence: Literal["memory", "local", "cloud"] | None = None,
    local_persistence_path: str | None = None,
    cloud_persistence_name: str | None = None,
    host: str | None = None,
) -> None:
    # Ensure settings reflect the intended working directory before computing paths

    configure_settings(
        app_root=cwd,
        deployment_file_path=deployment_file,
        reload=reload,
        proxy_ui=proxy_ui,
        persistence=persistence,
        local_persistence_path=local_persistence_path,
        cloud_persistence_name=cloud_persistence_name,
        host=host,
    )
    base_dir = cwd or Path.cwd()
    path = settings.resolved_config_parent.relative_to(base_dir)
    args = ["uv", "run", "--no-progress", "python", "-m", "llama_agents.appserver.app"]
    if proxy_ui:
        args.append("--proxy-ui")
    if reload:
        args.append("--reload")
    if deployment_file:
        args.append("--deployment-file")
        args.append(str(deployment_file))
    if open_browser:
        args.append("--open-browser")

    env = os.environ.copy()
    if port:
        env["LLAMA_DEPLOY_APISERVER_PORT"] = str(port)
    if ui_port:
        env["LLAMA_DEPLOY_APISERVER_PROXY_UI_PORT"] = str(ui_port)
    if log_level:
        env["LOG_LEVEL"] = log_level
    if log_format:
        env["LOG_FORMAT"] = log_format

    run_process(
        args,
        cwd=path,
        env=env,
        line_transform=_exclude_venv_warning,
    )


def start_preflight_in_target_venv(
    cwd: Path | None = None,
    deployment_file: Path | None = None,
    skip_env_validation: bool = False,
) -> None:
    """
    Run preflight validation inside the target project's virtual environment using uv.
    Mirrors the venv targeting and invocation strategy used by start_server_in_target_venv.

    Args:
        cwd: Working directory for the validation.
        deployment_file: Path to the deployment configuration file.
        skip_env_validation: If True, skip validation of required environment variables.
    """
    configure_settings(
        app_root=cwd,
        deployment_file_path=deployment_file or Path(DEFAULT_DEPLOYMENT_FILE_PATH),
    )
    base_dir = cwd or Path.cwd()
    path = settings.resolved_config_parent.relative_to(base_dir)
    args = [
        "uv",
        "run",
        "--no-progress",
        "python",
        "-m",
        "llama_agents.appserver.app",
        "--preflight",
    ]
    if deployment_file:
        args.extend(["--deployment-file", str(deployment_file)])
    if skip_env_validation:
        args.append("--skip-env-validation")

    run_process(
        args,
        cwd=path,
        env=os.environ.copy(),
        line_transform=_exclude_venv_warning,
    )
    # Note: run_process doesn't return exit code; process runs to completion or raises


def start_export_json_graph_in_target_venv(
    cwd: Path | None = None,
    deployment_file: Path | None = None,
    output: Path | None = None,
) -> None:
    """
    Run workflow graph export inside the target project's virtual environment using uv.
    Mirrors the venv targeting and invocation strategy used by start_preflight_in_target_venv.
    """

    configure_settings(
        app_root=cwd,
        deployment_file_path=deployment_file or Path(DEFAULT_DEPLOYMENT_FILE_PATH),
    )
    base_dir = cwd or Path.cwd()
    path = settings.resolved_config_parent.relative_to(base_dir)
    args = [
        "uv",
        "run",
        "--no-progress",
        "python",
        "-m",
        "llama_agents.appserver.app",
        "--export-json-graph",
    ]
    if deployment_file:
        args.extend(["--deployment-file", str(deployment_file)])
    if output is not None:
        args.extend(["--export-output", str(output)])

    run_process(
        args,
        cwd=path,
        env=os.environ.copy(),
        line_transform=_exclude_venv_warning,
    )


class PreflightValidationError(Exception):
    """Raised when workflow validations fail during preflight.

    Attributes:
        errors: List of (workflow/service name, error message)
    """

    def __init__(self, errors: list[tuple[str, str]]):
        self.errors = errors
        error_messages = [f"{name}: {msg}" for name, msg in errors]
        message = "Workflow validation failed:\n" + "\n".join(error_messages)
        super().__init__(message)


def preflight_validate(
    cwd: Path | None = None,
    deployment_file: Path | None = None,
    configure_logging: bool = False,
    skip_env_validation: bool = False,
) -> None:
    """
    Perform the same initialization path as starting the server, without serving.
    This catches import errors and runs workflow validations.

    Args:
        cwd: Working directory for the validation.
        deployment_file: Path to the deployment configuration file.
        configure_logging: Whether to configure logging.
        skip_env_validation: If True, fill missing required env vars with placeholder
            values instead of raising an error. Useful when validating workflow structure
            without actual environment variable values.
    """
    configure_settings(
        app_root=cwd,
        deployment_file_path=deployment_file or Path(DEFAULT_DEPLOYMENT_FILE_PATH),
    )
    cfg = get_deployment_config()
    load_environment_variables(cfg, settings.resolved_config_parent)
    validate_required_env_vars(cfg, fill_missing=skip_env_validation)

    source_server = load_workflow_server(cfg)
    workflows = source_server.get_workflows()
    # Instantiate Deployment to ensure server wiring doesn't raise
    _ = Deployment(source_server.get_workflows(), serializer=source_server.serializer)
    # Run workflow-level validations if present
    errors: list[tuple[str, str]] = []
    for service_name, workflow in workflows.items():
        method = getattr(workflow, "_validate", None)
        if callable(method):
            try:
                method()
            except Exception as exc:
                errors.append((service_name, str(exc)))
    if errors:
        raise PreflightValidationError(errors)


def export_json_graph(
    cwd: Path | None = None,
    deployment_file: Path | None = None,
    output: Path | None = None,
) -> None:
    """
    Export a JSON representation of the registered workflows' graph.

    This follows the same initialization path as preflight validation and writes
    a workflows.json-style structure compatible with the CLI expectations.
    """
    from workflows.representation.build import get_workflow_representation

    configure_settings(
        app_root=cwd,
        deployment_file_path=deployment_file or Path(DEFAULT_DEPLOYMENT_FILE_PATH),
    )
    cfg = get_deployment_config()
    load_environment_variables(cfg, settings.resolved_config_parent)

    workflows = load_workflows(cfg)

    graph: dict[str, dict[str, Any]] = {}
    for name, workflow in workflows.items():
        wf_repr_dict = get_workflow_representation(workflow).model_dump()
        graph[name] = wf_repr_dict

    output_path = output or (Path.cwd() / "workflows.json")
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(graph, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy-ui", action="store_true")
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--deployment-file", type=Path)
    parser.add_argument("--open-browser", action="store_true")
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--skip-env-validation", action="store_true")
    parser.add_argument("--export-json-graph", action="store_true")
    parser.add_argument("--export-output", type=Path)

    args = parser.parse_args()
    if args.preflight:
        preflight_validate(
            cwd=Path.cwd(),
            deployment_file=args.deployment_file,
            skip_env_validation=args.skip_env_validation,
        )
    elif args.export_json_graph:
        export_json_graph(
            cwd=Path.cwd(),
            deployment_file=args.deployment_file,
            output=args.export_output,
        )
    else:
        start_server(
            proxy_ui=args.proxy_ui,
            reload=args.reload,
            deployment_file=args.deployment_file,
            open_browser=args.open_browser,
        )
