from pathlib import Path

import click
from llama_agents.cli.pkg import (
    DEFAULT_DOCKER_IGNORE,
    build_dockerfile_content,
    infer_python_version,
    pkg_container_options,
)
from llama_agents.core.deployment_config import (
    read_deployment_config_from_git_root_or_cwd,
)

from ..app import app

SUPPORTED_FORMATS = ["Docker", "Podman"]
SUPPORTED_FORMATS_STR = ", ".join(SUPPORTED_FORMATS)


@app.group(
    help=f"Package your application in different formats. Currently supported: {SUPPORTED_FORMATS_STR}.",
    no_args_is_help=True,
    context_settings={"max_content_width": None},
)
def pkg() -> None:
    """Package application in different formats (Dockerfile, Podman config, Nixpack...)"""
    pass


@pkg.command(
    "container",
    help="Generate a minimal, build-ready file to containerize your workflows through Docker or Podman (currently frontend is not supported).",
)
@pkg_container_options
def create_container_file(
    deployment_file: Path,
    python_version: str | None = None,
    port: int = 4501,
    exclude: tuple[str, ...] | None = None,
    output_file: str = "Dockerfile",
    dockerignore_path: str = ".dockerignore",
    overwrite: bool = False,
) -> None:
    _create_file_for_container(
        deployment_file=deployment_file,
        python_version=python_version,
        port=port,
        exclude=exclude,
        output_file=output_file,
        dockerignore_path=dockerignore_path,
        overwrite=overwrite,
    )


def _check_deployment_config(deployment_file: Path) -> Path:
    if not deployment_file.exists():
        raise click.ClickException(f"Deployment file '{deployment_file}' not found")

    # Early check: appserver requires a pyproject.toml in the config directory
    config_dir = deployment_file if deployment_file.is_dir() else deployment_file.parent
    if not (config_dir / "pyproject.toml").exists():
        raise click.ClickException(
            f"No pyproject.toml found at {config_dir}.\n"
            "Add a pyproject.toml to your project and re-run 'llamactl serve'."
        )

    try:
        config = read_deployment_config_from_git_root_or_cwd(
            Path.cwd(), deployment_file
        )
    except Exception:
        raise click.ClickException(
            "Could not read a deployment config. This doesn't appear to be a valid llama-deploy project."
        )
    if config.ui:
        raise click.ClickException(
            "Containerized UI builds are currently not supported. Please remove the UI configuration from your deployment file if you wish to proceed."
        )
    return config_dir


def _create_file_for_container(
    deployment_file: Path,
    output_file: str = "Dockerfile",
    python_version: str | None = None,
    port: int = 4501,
    exclude: tuple[str, ...] | None = None,
    dockerignore_path: str = ".dockerignore",
    overwrite: bool = False,
) -> None:
    config_dir = _check_deployment_config(deployment_file=deployment_file)

    if not python_version:
        python_version = infer_python_version(config_dir)

    dockerignore_content = DEFAULT_DOCKER_IGNORE
    if exclude:
        for item in exclude:
            dockerignore_content += "\n" + item

    dockerfile_content = build_dockerfile_content(python_version, port)

    if Path(output_file).exists() and not overwrite:
        raise click.ClickException(
            f"{output_file} already exists. If you wish to overwrite the file, pass `--overwrite` as a flag to the command."
        )
    with open(output_file, "w") as f:
        f.write(dockerfile_content)
    if Path(dockerignore_path).exists() and not overwrite:
        raise click.ClickException(
            f"{dockerignore_path} already exists. If you wish to overwrite the file, pass `--overwrite` as a flag to the command."
        )
    with open(dockerignore_path, "w") as f:
        f.write(dockerignore_content)
