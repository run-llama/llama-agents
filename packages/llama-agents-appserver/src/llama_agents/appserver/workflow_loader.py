import configparser
import functools
import importlib
import logging
import os
import socket
import subprocess
import sys
from dataclasses import dataclass
from importlib.metadata import requires as pkg_requires
from importlib.metadata import version as pkg_version
from pathlib import Path
from textwrap import dedent

from dotenv import dotenv_values
from llama_agents.appserver.process_utils import (
    BootstrapHandler,
    run_process,
    spawn_process,
)
from llama_agents.appserver.settings import ApiserverSettings, settings
from llama_agents.core.deployment_config import DeploymentConfig
from llama_agents.core.ui_build import ui_build_output_path
from llama_agents.server import WorkflowServer
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version
from workflows import Workflow

logger = logging.getLogger(__name__)
logger.addHandler(BootstrapHandler(prefix="[install]", color_code="33"))
logger.setLevel(logging.INFO)

DEFAULT_SERVICE_ID = "default"

# The last version published under the old "llama-deploy-appserver" dist name.
# Versions after this are published as "llama-agents-appserver".
_LAST_OLD_DIST_VERSION = Version("0.5.3")
_OLD_DIST_NAME = "llama-deploy-appserver"
_NEW_DIST_NAME = "llama-agents-appserver"


def _dist_name_for_version(version: Version) -> str:
    """Return the correct PyPI dist name for a given appserver version."""
    if version <= _LAST_OLD_DIST_VERSION:
        return _OLD_DIST_NAME
    return _NEW_DIST_NAME


def load_workflows(config: DeploymentConfig) -> dict[str, Workflow]:
    """
    Creates WorkflowService instances according to the configuration object.

    """
    workflow_services: dict[str, Workflow] = {}

    if config.app:
        module_name, app_name = config.app.split(":", 1)
        module = importlib.import_module(module_name)
        if not hasattr(module, app_name):
            raise AttributeError(
                f"Module '{module_name}' has no attribute '{app_name}'"
            )
        workflow = getattr(module, app_name)
        if not isinstance(workflow, WorkflowServer):
            raise ValueError(
                f"Workflow {app_name} in {module_name} is not a WorkflowServer object"
            )
        workflow_services = workflow.get_workflows()
    else:
        for service_id, workflow_name in config.workflows.items():
            module_name, workflow_name = workflow_name.split(":", 1)
            module = importlib.import_module(module_name)
            if not hasattr(module, workflow_name):
                raise AttributeError(
                    f"Module '{module_name}' has no attribute '{workflow_name}'"
                )
            workflow = getattr(module, workflow_name)
            if not isinstance(workflow, Workflow):
                logger.warning(
                    f"Workflow {workflow_name} in {module_name} is not a Workflow object",
                )
            workflow_services[service_id] = workflow

    return workflow_services


def load_environment_variables(config: DeploymentConfig, source_root: Path) -> None:
    """
    Load environment variables from the deployment config.
    """
    for key, value in parse_environment_variables(config, source_root).items():
        if value:
            os.environ[key] = value


def validate_required_env_vars(
    config: DeploymentConfig, *, fill_missing: bool = False
) -> None:
    """
    Validate that all required environment variables are present and non-empty.

    Args:
        config: The deployment configuration containing required_env_vars.
        fill_missing: If True, fill missing env vars with placeholder values instead
            of raising an error. This is useful for validation where env vars may be
            read statically during import time.

    Raises:
        RuntimeError: If any required env vars are missing or empty and fill_missing is False.
    """
    required = config.required_env_vars
    if not required:
        return
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        if fill_missing:
            for name in missing:
                os.environ[name] = f"__PLACEHOLDER_{name}__"
        else:
            missing_list = ", ".join(sorted(missing))
            raise RuntimeError(
                (
                    "Missing required environment variables defined in required_env_vars: "
                    f"{missing_list}. Provide them via your environment, .env files, or the deployment secrets."
                )
            )


def parse_environment_variables(
    config: DeploymentConfig, source_root: Path
) -> dict[str, str]:
    """
    Parse environment variables from the deployment config.
    """
    env_vars = {**config.env} if config.env else {}
    for env_file in config.env_files or []:
        env_file_path = source_root / env_file
        values = dotenv_values(env_file_path)
        str_values = {k: v for k, v in values.items() if isinstance(v, str)}
        env_vars.update(str_values)
    return env_vars


@functools.cache
def are_we_editable_mode() -> bool:
    """
    Check if we're in editable mode.
    """
    # Heuristic: if the package path does not include 'site-packages', treat as editable
    top_level_pkg = "llama_agents.appserver"
    try:
        pkg = importlib.import_module(top_level_pkg)
        pkg_path = Path(getattr(pkg, "__file__", "")).resolve()
        if not pkg_path.exists():
            return False

        return "site-packages" not in pkg_path.parts
    except Exception:
        return False


def inject_appserver_into_target(
    config: DeploymentConfig,
    source_root: Path,
    sdists: list[Path] | None = None,
    target_version: str | None = None,
    auto_upgrade: bool = True,
) -> None:
    """
    Ensures uv, and uses it to add the appserver as a dependency to the target app.
    - If sdists are provided, they will be installed directly for offline-ish installs (still fetches dependencies)
    - If the appserver is currently editable, it will be installed directly from the source repo
    - otherwise fetches the current version from pypi

    Args:
        config: The deployment config
        source_root: The root directory of the deployment
        sdists: A list of tar.gz sdists files to install instead of installing the appserver
        auto_upgrade: If True, auto-upgrade dependencies (e.g. llama-index-workflows) to
            be compatible with the appserver. Should be False during container bootstrap
            to avoid modifying the target project's pyproject.toml.
    """
    path = settings.resolved_config_parent
    logger.info(f"Ensuring venv at {path} and adding appserver")
    _ensure_uv_available()
    _install_and_add_appserver_if_missing(
        path,
        source_root,
        sdists=sdists,
        target_version=target_version,
        auto_upgrade=auto_upgrade,
    )


def _uv_run_python(cwd: Path, snippet: str, *, stderr: int | None = None) -> str:
    """
    Run a python snippet via ``uv run`` inside the project venv uv resolves for
    ``cwd``. Returns stripped stdout. Use this anywhere we need to probe the
    target venv (installed version, ``sys.prefix``, etc.) so all probes agree
    with what the start-time runners see.
    """
    result = subprocess.check_output(
        ["uv", "run", "--no-progress", "python", "-c", snippet],
        cwd=cwd,
        stderr=stderr,
    )
    return result.decode("utf-8").strip()


def _get_installed_version_within_target(
    path: Path, package: str = "llama-agents-appserver"
) -> Version | None:
    packages = [package, _OLD_DIST_NAME] if package == _NEW_DIST_NAME else [package]
    for pkg_name in packages:
        try:
            output = _uv_run_python(
                path,
                dedent(f"""
                        from importlib.metadata import version
                        try:
                            print(version("{pkg_name}"))
                        except Exception:
                            pass
                       """),
                stderr=subprocess.DEVNULL,
            )
            try:
                return Version(output)
            except InvalidVersion:
                continue
        except subprocess.CalledProcessError:
            continue
    return None


def _get_current_version() -> Version:
    return Version(pkg_version("llama-agents-appserver"))


def _is_missing_or_outdated(path: Path) -> Version | None:
    """
    returns the current version if the installed version is missing or outdated, otherwise None
    """
    installed = _get_installed_version_within_target(path)
    current = _get_current_version()
    if installed is None or installed < current:
        return current
    return None


@functools.cache
def _get_appserver_workflows_requirement() -> SpecifierSet | None:
    """Read the appserver's version requirement for llama-index-workflows from package metadata."""
    reqs = pkg_requires("llama-agents-appserver") or []
    for req_str in reqs:
        req = Requirement(req_str)
        if req.name == "llama-index-workflows":
            return req.specifier
    return None


def _ensure_compatible_workflows(
    source_root: Path,
    path: Path,
) -> None:
    """Check if the user's llama-index-workflows version is compatible with the appserver.

    If incompatible, auto-updates via ``uv add``. If the update fails (e.g. conflicting
    constraints), raises RuntimeError with a clear message.
    """
    requirement = _get_appserver_workflows_requirement()
    if requirement is None:
        return

    installed = _get_installed_version_within_target(
        source_root / path, package="llama-index-workflows"
    )
    if installed is None:
        # Not installed — the appserver install will bring it in
        return

    if installed in requirement:
        return

    # Version is incompatible — auto-update
    req_str = str(requirement)
    logger.warning(
        f"⚠️ Updating llama-index-workflows from {installed} to {req_str} "
        f"(required by llama-agents-appserver). "
        f"You can add llamactl as a dev dependency to resolve version conflicts: "
        f"uv add llamactl --dev (then run with uv run llamactl)"
    )
    try:
        run_uv(
            source_root,
            path,
            "add",
            [f"llama-index-workflows{req_str}"],
        )
    except subprocess.CalledProcessError:
        raise RuntimeError(
            f"Your project has llama-index-workflows=={installed} which is incompatible "
            f"with this version of the appserver (requires {req_str}). "
            f"Automatic update failed — your project may have conflicting constraints. "
            f"Please update manually: uv add 'llama-index-workflows{req_str}'"
        )


def run_uv(
    source_root: Path,
    path: Path,
    cmd: str,
    args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
) -> None:
    args = args or []
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    run_process(
        ["uv", cmd] + args,
        cwd=source_root / path,
        prefix=f"[uv {cmd}]",
        color_code="36",
        use_tty=False,
        line_transform=_exclude_venv_warning,
        env=env,
    )


def _resolve_project_venv(source_root: Path, path: Path) -> Path:
    """
    Return the venv path uv would use for this project.

    Must be called after ``uv sync`` so the venv is guaranteed to exist. Asks uv
    itself rather than reimplementing uv's workspace / project resolution, so the
    install side stays aligned with ``start_*_in_target_venv`` (also bare ``uv run``).
    """
    return Path(_uv_run_python(source_root / path, "import sys; print(sys.prefix)"))


def _install_and_add_appserver_if_missing(
    path: Path,
    source_root: Path,
    save_version: bool = False,
    sdists: list[Path] | None = None,
    target_version: str | None = None,
    auto_upgrade: bool = True,
) -> None:
    """
    Sync project deps (letting uv pick the venv location, so we agree with uv run
    in workspace and non-workspace layouts) and install the appserver if missing
    or outdated.
    """

    if not (source_root / path / "pyproject.toml").exists():
        logger.warning(
            f"No pyproject.toml found at {source_root / path}, skipping appserver injection. The server will likely not be able to install your workflows."
        )
        return

    editable = are_we_editable_mode()
    run_uv(
        source_root,
        path,
        "sync",
        ["--no-dev", "--inexact"],
    )
    venv_path = _resolve_project_venv(source_root, path)

    if auto_upgrade:
        _ensure_compatible_workflows(source_root, path)

    if sdists:
        run_uv(
            source_root,
            path,
            "pip",
            ["install"]
            + [str(s.absolute()) for s in sdists]
            + ["--prefix", str(venv_path)],
        )
    elif editable:
        same_python_version = _same_python_version(venv_path)
        if not same_python_version.is_same:
            msg = (
                f"Python version mismatch at {venv_path}: runtime "
                f"{same_python_version.current_version} != venv "
                f"{same_python_version.target_version}"
            )
            logger.error(
                f"{msg}. In editable-appserver mode the target venv must run "
                f"the same Python as the appserver process, otherwise the "
                f"appserver cannot be installed."
            )
            raise RuntimeError(msg)
        pyproject = _find_development_pyproject()
        if pyproject is None:
            raise RuntimeError("No pyproject.toml found in llama-agents-appserver")
        base = (source_root.resolve() / path).resolve()
        rel = Path(os.path.relpath(pyproject, start=base))
        target = f"file://{str(rel)}"

        run_uv(
            source_root,
            path,
            "pip",
            [
                "install",
                "--reinstall-package",
                "llama-agents-appserver",
                target,
                "--prefix",
                str(venv_path),
            ],
        )

    else:
        if target_version:
            # Explicit version requested (e.g. pinned deployment) — install
            # exactly that version from PyPI, skipping the outdated check.
            install_version = Version(target_version)
        else:
            install_version = _is_missing_or_outdated(path)
        if install_version is not None:
            dist_name = _dist_name_for_version(install_version)
            if save_version and not target_version:
                run_uv(
                    source_root,
                    path,
                    "add",
                    [f"{dist_name}>={install_version}"],
                )
            else:
                run_uv(
                    source_root,
                    path,
                    "pip",
                    [
                        "install",
                        f"{dist_name}=={install_version}",
                        "--prefix",
                        str(venv_path),
                    ],
                )


def _find_development_pyproject() -> Path | None:
    dir = Path(__file__).parent.resolve()
    while not (dir / "pyproject.toml").exists():
        dir = dir.parent
        if dir == dir.root:
            return None
    return dir


def _exclude_venv_warning(line: str) -> str | None:
    if "use `--active` to target the active environment instead" in line:
        return None
    return line


def _ensure_uv_available() -> None:
    # Check if uv is available on the path
    uv_available = False
    try:
        subprocess.check_call(
            ["uv", "--version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        uv_available = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    if not uv_available:
        # bootstrap uv with pip
        try:
            run_process(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "uv",
                ],
                prefix="[python -m pip]",
                color_code="31",  # red
            )
        except subprocess.CalledProcessError as e:
            msg = f"Unable to install uv. Environment must include uv, or uv must be installed with pip: {e.stderr}"
            raise RuntimeError(msg)


@dataclass
class SamePythonVersionResult:
    is_same: bool
    current_version: str
    target_version: str | None


def _same_python_version(venv_path: Path) -> SamePythonVersionResult:
    current_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    target_version = None
    cfg = venv_path / "pyvenv.cfg"
    if cfg.exists():
        parser = configparser.ConfigParser()
        parser.read_string("[venv]\n" + cfg.read_text())
        ver_str = parser["venv"].get("version_info", "").strip()
        if ver_str:
            try:
                v = Version(ver_str)
                target_version = f"{v.major}.{v.minor}"
            except InvalidVersion:
                pass
    return SamePythonVersionResult(
        is_same=current_version == target_version,
        current_version=current_version,
        target_version=target_version,
    )


def install_ui(config: DeploymentConfig, config_parent: Path) -> None:
    if config.ui is None:
        return
    package_manager = config.ui.package_manager
    try:
        run_process(
            [package_manager, "install"],
            cwd=config_parent / config.ui.directory,
            prefix=f"[{package_manager} install]",
            color_code="33",
            # auto download the package manager
            env={**os.environ.copy(), "COREPACK_ENABLE_DOWNLOAD_PROMPT": "0"},
        )
    except BaseException as e:
        if "No such file or directory" in str(e):
            raise RuntimeError(
                f"Package manager {package_manager} not found. Please download and enable corepack, or install the package manager manually."
            )
        raise e


def _ui_env(config: DeploymentConfig, settings: ApiserverSettings) -> dict[str, str]:
    env = os.environ.copy()
    # Set new canonical name while preserving legacy URL_ID for backwards compatibility
    if "LLAMA_DEPLOY_DEPLOYMENT_NAME" not in env:
        env["LLAMA_DEPLOY_DEPLOYMENT_NAME"] = config.name
    env["LLAMA_DEPLOY_DEPLOYMENT_URL_ID"] = config.name
    env["LLAMA_DEPLOY_DEPLOYMENT_BASE_PATH"] = f"/deployments/{config.name}/ui"
    if config.ui is not None:
        env["PORT"] = str(settings.proxy_ui_port)
    env["LLAMA_DEPLOY_SERVER_PORT"] = str(settings.port)
    # Apply PUBLIC_* overlays: PUBLIC_X overrides X in the UI build env
    public_prefix = "PUBLIC_"
    public_keys = [k for k in env if k.startswith(public_prefix)]
    for key in public_keys:
        base_key = key[len(public_prefix) :]
        env[base_key] = env[key]
        del env[key]
    return env


def build_ui(
    config_parent: Path, config: DeploymentConfig, settings: ApiserverSettings
) -> bool:
    """
    Returns True if the UI was built (and supports building), otherwise False if there's no build command
    """
    if config.ui is None:
        return False
    path = Path(config.ui.directory)
    env = _ui_env(config, settings)

    has_build = ui_build_output_path(config_parent, config)
    if has_build is None:
        return False

    run_process(
        ["npm", "run", "build"],
        cwd=config_parent / path,
        env=env,
        prefix="[npm run build]",
        color_code="34",
    )
    return True


def start_dev_ui_process(
    root: Path, settings: ApiserverSettings, config: DeploymentConfig
) -> None | subprocess.Popen:
    ui_port = settings.proxy_ui_port
    ui = config.ui
    if ui is None:
        return None

    # If a UI dev server is already listening on the configured port, do not start another
    def _is_port_open(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            try:
                return sock.connect_ex(("127.0.0.1", port)) == 0
            except Exception:
                return False

    if _is_port_open(ui_port):
        logger.info(
            f"Detected process already running on port {ui_port}; not starting a new one."
        )
        return None
    # start the ui process
    env = _ui_env(config, settings)
    # Transform first 20 lines to replace the default UI port with the main server port
    line_counter = 0

    def _transform(line: str) -> str:
        nonlocal line_counter
        if line_counter < 20:
            line = line.replace(f":{ui_port}", f":{settings.port}")
        line_counter += 1
        return line

    return spawn_process(
        ["npm", "run", ui.serve_command],
        cwd=root / (ui.directory),
        env=env,
        prefix=f"[npm run {ui.serve_command}]",
        color_code="35",
        line_transform=_transform,
        use_tty=False,
    )
