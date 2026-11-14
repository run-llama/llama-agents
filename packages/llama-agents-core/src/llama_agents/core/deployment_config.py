from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypeVar

import yaml
from llama_agents.core._compat import load_toml_file
from llama_agents.core.git.git_util import get_git_root, is_git_repo
from llama_agents.core.path_util import validate_path_traversal
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

DEFAULT_DEPLOYMENT_NAME = "default"


def read_deployment_config_from_git_root_or_cwd(
    cwd: Path, config_path: Path
) -> "DeploymentConfig":
    """
    Read the deployment config from the git root or cwd.
    """
    if is_git_repo():
        git_root = get_git_root()
        relative_cwd_path = cwd.relative_to(git_root)
        return read_deployment_config(git_root, relative_cwd_path / config_path)
    return read_deployment_config(cwd, config_path)


def read_deployment_config(source_root: Path, config_path: Path) -> "DeploymentConfig":
    """
    Read the deployment config from the config directory.

    - first checks for a llama_agents.toml (or llama_deploy.toml) in the config_path
    - then checks for a tool.llamaagents (or tool.llamadeploy) config in the pyproject.toml
    - then check for a legacy yaml config (if config_path is a file, uses that, otherwise uses the config_path/llama_agents.yaml or llama_deploy.yaml)
    - based on what was resolved here, discovers the package.json, if any ui, and resolves its values from the llamaagents (or llamadeploy) key in the package.json

    In all cases, the llama_agents/llamaagents variant takes precedence over the llama_deploy/llamadeploy variant.

    Args:
        source_root: path to the root of the source code. References should not exit this directory.
        config_path: path to a deployment config file, or directory containing a deployment config file.

    Returns:
        DeploymentConfig: the deployment config
    """
    config_file: Path | None = None
    if (source_root / config_path).is_file():
        config_file = Path(config_path.name)
        if str(config_file) in {
            "llama_agents.toml",
            "llama_deploy.toml",
            "pyproject.toml",
        }:
            config_file = None
        config_path = config_path.parent
    local_agents_toml = source_root / config_path / "llama_agents.toml"
    local_deploy_toml = source_root / config_path / "llama_deploy.toml"
    local_toml_path = (
        local_agents_toml if local_agents_toml.exists() else local_deploy_toml
    )
    pyproject_path = source_root / config_path / "pyproject.toml"
    toml_config: DeploymentConfig = DeploymentConfig()
    # local TOML format
    if local_toml_path.exists():
        with open(local_toml_path, "rb") as toml_file:
            toml_data = load_toml_file(toml_file)
            if isinstance(toml_data, dict):
                toml_config = DeploymentConfig.model_validate(toml_data)
    # pyproject.toml format
    elif pyproject_path.exists():
        with open(pyproject_path, "rb") as pyproject_file:
            pyproject = load_toml_file(pyproject_file)
            tool = pyproject.get("tool", {})
            project_name: str | None = None
            project_metadata = pyproject.get("project", {})
            if isinstance(project_metadata, dict):
                name = project_metadata.get("name")
                if isinstance(name, str):
                    project_name = name
            if isinstance(tool, dict):
                llama_deploy = tool.get("llamaagents") or tool.get("llamadeploy", {})
                if isinstance(llama_deploy, dict):
                    if "name" not in llama_deploy:
                        llama_deploy["name"] = project_name
                    toml_config = DeploymentConfig.model_validate(llama_deploy)
    # legacy yaml format, (and why not support yaml in the new format too, since this is doing everything all the ways)
    if toml_config.has_no_workflows():
        agents_yaml = (
            source_root / config_path / (config_file or Path("llama_agents.yaml"))
        )
        deploy_yaml = (
            source_root / config_path / (config_file or Path("llama_deploy.yaml"))
        )
        yaml_path = agents_yaml if agents_yaml.exists() else deploy_yaml
        if yaml_path.exists():
            with open(yaml_path, "r", encoding="utf-8") as yaml_file:
                yaml_loaded = yaml.safe_load(yaml_file) or {}

            old_config: DeploymentConfig | None = None
            new_config: DeploymentConfig | None = None
            try:
                old_config = DeprecatedDeploymentConfig.model_validate(
                    yaml_loaded
                ).to_deployment_config()
            except ValidationError:
                pass
            try:
                new_config = DeploymentConfig.model_validate(yaml_loaded)
            except ValidationError:
                pass
            loaded: DeploymentConfig | None = new_config
            if (
                old_config is not None
                and old_config.is_valid()
                and (new_config is None or not new_config.is_valid())
            ):
                loaded = old_config
            if loaded is not None:
                toml_config = toml_config.merge_config(loaded)

    # package.json format
    if toml_config.ui is not None:
        package_json_path = (
            source_root / config_path / toml_config.ui.directory / "package.json"
        )
        if package_json_path.exists():
            with open(package_json_path, "r", encoding="utf-8") as package_json_file:
                package_json = json.load(package_json_file)
            if isinstance(package_json, dict):
                # Standard packageManager fallback, e.g. "pnpm@9.0.0" -> "pnpm"
                pkg_manager_value = package_json.get("packageManager")
                pkg_manager_name: str | None = None
                if isinstance(pkg_manager_value, str) and pkg_manager_value:
                    pkg_manager_name = pkg_manager_value.split("@", 1)[0] or None

                llama_deploy = package_json.get("llamaagents") or package_json.get(
                    "llamadeploy", {}
                )

                if isinstance(llama_deploy, dict):
                    # Prepare payload without leaking Path objects into Pydantic
                    ui_dir = toml_config.ui.directory if toml_config.ui else None
                    ui_payload: dict[str, object] = {**llama_deploy}
                    if "directory" not in ui_payload and ui_dir is not None:
                        ui_payload["directory"] = ui_dir
                    if (
                        "package_manager" not in ui_payload
                        and pkg_manager_name is not None
                    ):
                        ui_payload["package_manager"] = pkg_manager_name

                    ui_config = UIConfig.model_validate(ui_payload)
                    if ui_config.build_output_dir is not None:
                        ui_config.build_output_dir = str(
                            Path(toml_config.ui.directory) / ui_config.build_output_dir
                        )
                    toml_config.ui = ui_config.merge_config(toml_config.ui)

    if toml_config.ui is not None:
        validate_path_traversal(
            config_path / toml_config.ui.directory, source_root, "ui_source"
        )
        if toml_config.ui.build_output_dir:
            validate_path_traversal(
                config_path / toml_config.ui.build_output_dir,
                source_root,
                "ui_build_output_dir",
            )

    return toml_config


def resolve_config_parent(root: Path, deployment_path: Path) -> Path:
    path = root / deployment_path
    if path.is_file():
        return path.parent
    else:
        return path


DEFAULT_UI_PACKAGE_MANAGER = "npm"
DEFAULT_UI_BUILD_COMMAND = "build"
DEFAULT_UI_SERVE_COMMAND = "dev"
DEFAULT_UI_PROXY_PORT = 4502


class DeploymentConfig(BaseModel):
    name: str = Field(
        default=DEFAULT_DEPLOYMENT_NAME,
        description="The url safe path name of the deployment.",
    )
    llama_cloud: bool = Field(
        default=False,
        description="If true, serving locally expects Llama Cloud access and will inject credentials when possible.",
    )
    app: str | None = Field(
        default=None,
        description="A full bundle of all workflows as an 'app'. \"path.to_import:app_name\"",
    )
    workflows: dict[str, str] = Field(
        default_factory=dict,
        description='Deprecated: A map of workflow names to their import paths. "nice_name": "path.to_import:workflow_name"',
    )
    env_files: list[str] = Field(
        default_factory=list,
        description="The environment files to load. Defaults to ['.env']",
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Arbitrary environment variables to set. Defaults to {}",
    )
    required_env_vars: list[str] = Field(
        default_factory=list,
        description=(
            "A list of environment variable names that must be defined at runtime. "
            "If any are missing or empty, the app server will fail fast with an informative error."
        ),
    )
    ui: UIConfig | None = Field(
        default=None,
        description="The UI configuration.",
    )

    def merge_config(self, config: "DeploymentConfig") -> "DeploymentConfig":
        """Merge the config with another config."""

        return DeploymentConfig(
            name=_pick_non_default(self.name, config.name, "default"),
            llama_cloud=self.llama_cloud or config.llama_cloud,
            app=self.app or config.app,
            workflows={**self.workflows, **config.workflows},
            # Preserve declaration order: env files are loaded in list order
            # with later files overriding earlier ones, so a set() here would
            # scramble the precedence (e.g. ".env.local" before ".env").
            env_files=list(dict.fromkeys(self.env_files + config.env_files)),
            env={**self.env, **config.env},
            required_env_vars=list(
                dict.fromkeys(
                    [
                        *self.required_env_vars,
                        *config.required_env_vars,
                    ]
                )
            ),
            ui=self.ui.merge_config(config.ui)
            if config.ui is not None and self.ui is not None
            else self.ui or config.ui,
        )

    def has_no_workflows(self) -> bool:
        """Check if the config has no workflows."""
        return len(self.workflows) == 0 and self.app is None

    def has_both_app_and_workflows(self) -> bool:
        """Check if the config has both app and workflows."""
        return self.app is not None and len(self.workflows) > 0

    def is_valid(self) -> bool:
        """Check if the config is valid."""
        try:
            self.validate_config()
            return True
        except ValueError:
            return False

    def validate_config(self) -> None:
        """Validate the config."""
        if self.has_no_workflows():
            raise ValueError("Config must have at least one workflow.")
        if self.has_both_app_and_workflows():
            raise ValueError("Config cannot have both app and workflows configured.")

    def build_output_path(self) -> Path | None:
        """get the build output path, or default to the ui directory/dist"""
        if self.ui is None:
            return None
        return (
            Path(self.ui.build_output_dir)
            if self.ui.build_output_dir
            else Path(self.ui.directory) / "dist"
        )


T = TypeVar("T")


def _pick_non_default(a: T, b: T, default: T) -> T:
    if a != default:
        return a
    return b or default


class UIConfig(BaseModel):
    directory: str = Field(
        ...,
        description="The directory containing the UI, relative to the pyproject.toml directory",
    )
    build_output_dir: str | None = Field(
        default=None,
        description="The directory containing the built UI, relative to the pyproject.toml directory. Defaults to 'dist' relative to the ui_directory, if defined",
    )
    package_manager: str = Field(
        default=DEFAULT_UI_PACKAGE_MANAGER,
        description=f"The package manager to use to build the UI. Defaults to '{DEFAULT_UI_PACKAGE_MANAGER}'",
    )
    build_command: str = Field(
        default=DEFAULT_UI_BUILD_COMMAND,
        description=f"The npm script command to build the UI. Defaults to '{DEFAULT_UI_BUILD_COMMAND}' if not specified",
    )
    serve_command: str = Field(
        default=DEFAULT_UI_SERVE_COMMAND,
        description=f"The command to serve the UI. Defaults to '{DEFAULT_UI_SERVE_COMMAND}' if not specified",
    )
    proxy_port: int = Field(
        default=DEFAULT_UI_PROXY_PORT,
        description=f"The port to proxy the UI to. Defaults to '{DEFAULT_UI_PROXY_PORT}' if not specified",
    )

    def merge_config(self, config: "UIConfig") -> "UIConfig":
        """Merge the config with the default config."""

        return UIConfig(
            directory=self.directory,
            build_output_dir=self.build_output_dir or config.build_output_dir,
            package_manager=_pick_non_default(
                self.package_manager, config.package_manager, DEFAULT_UI_PACKAGE_MANAGER
            ),
            build_command=_pick_non_default(
                self.build_command, config.build_command, DEFAULT_UI_BUILD_COMMAND
            ),
            serve_command=_pick_non_default(
                self.serve_command, config.serve_command, DEFAULT_UI_SERVE_COMMAND
            ),
            proxy_port=_pick_non_default(
                self.proxy_port, config.proxy_port, DEFAULT_UI_PROXY_PORT
            ),
        )


class ServiceSourceV0(BaseModel):
    """Configuration for where to load the workflow or other source. Path is relative to the config file its declared within."""

    location: str

    @model_validator(mode="before")
    @classmethod
    def validate_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "name" in data:
                data["location"] = data.pop("name")
        return data


class DerecatedService(BaseModel):
    """Configuration for a single service."""

    source: ServiceSourceV0 | None = Field(default=None)
    import_path: str | None = Field(default=None)
    env: dict[str, str] | None = Field(default=None)
    env_files: list[str] | None = Field(default=None)
    python_dependencies: list[str] | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def validate_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Handle YAML aliases
            if "path" in data:
                data["import_path"] = data.pop("path")
            if "import-path" in data:
                data["import_path"] = data.pop("import-path")
            if "env-files" in data:
                data["env_files"] = data.pop("env-files")

        return data

    def module_location(self) -> tuple[str, str]:
        """
        Parses the import path, and target, discarding legacy file path portion, if any

        "src/module.workflow:my_workflow" -> ("module.workflow", "my_workflow")
        """
        if self.import_path is None:
            raise ValueError("import_path is required to compute module_location")
        module_name, workflow_name = self.import_path.split(":")
        return Path(module_name).name, workflow_name


class DeprecatedDeploymentConfig(BaseModel):
    """Model definition mapping a deployment config file."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    name: str
    default_service: str | None = Field(default=None)
    services: dict[str, DerecatedService]
    ui: DerecatedService | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_fields(cls, data: Any) -> Any:
        # Handle YAML aliases
        if isinstance(data, dict):
            if "default-service" in data:
                data["default_service"] = data.pop("default-service")

        return data

    @classmethod
    def from_yaml(
        cls,
        path: Path,
    ) -> "DeprecatedDeploymentConfig":
        """Read config data from a yaml file."""
        with open(path, "r", encoding="utf-8") as yaml_file:
            config = yaml.safe_load(yaml_file) or {}

        instance = cls.model_validate(config)
        return instance

    def to_deployment_config(self) -> DeploymentConfig:
        """Convert the deployment config to a DeploymentConfig."""
        workflows = {}
        env_files = []
        env = {}
        ui_directory: str | None = None
        for service_name, service in self.services.items():
            if service.import_path:
                path, name = service.module_location()
                workflows[service_name] = f"{path}:{name}"
            if service.env_files:
                env_files.extend(service.env_files)
            if service.env:
                env.update(service.env)
        if self.default_service:
            workflows["default"] = workflows[self.default_service]
        env_files = list(set(env_files))

        if self.ui:
            ui_directory = self.ui.source.location if self.ui.source else None

        return DeploymentConfig(
            name=self.name,
            workflows=workflows,
            env_files=env_files,
            env=env,
            ui=UIConfig(directory=ui_directory) if ui_directory else None,
        )
