import os
from pathlib import Path

import click
import pytest
from llama_agents.cli.commands.pkg import (
    _check_deployment_config,
    _create_file_for_container,
)
from llama_agents.cli.pkg import (
    DEFAULT_DOCKER_IGNORE,
    build_dockerfile_content,
    infer_python_version,
)


@pytest.fixture()
def pyproject_toml() -> str:
    return """
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "test"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.12,<4"
dependencies = [
    "llama-index-workflows>=2.7.1",
]

[tool.hatch.build.targets.wheel]
only-include = ["src/test_agent"]

[tool.hatch.build.targets.wheel.sources]
"src" = ""

[tool.llamadeploy.workflows]
test = "test_agent.workflow.main:workflow"

[tool.llamadeploy]
name = "test-agent"
env_files = [".env"]
llama_cloud = true
"""


@pytest.fixture()
def pyproject_toml_with_ui(pyproject_toml: str) -> str:
    return pyproject_toml + '\n[tool.llamadeploy.ui]\ndirectory = "./ui"\n'


@pytest.fixture()
def dockerfile() -> str:
    return """
FROM python:3.14-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY . /app/

ENV PATH=/root/.local/bin:$PATH

RUN uv sync --locked
RUN uv tool install llamactl

EXPOSE 4502

ENTRYPOINT [ "uv", "run", "llamactl", "serve", "--host", "0.0.0.0", "--port", "4502" ]
"""


def test_infer_python_version(tmp_path: Path, pyproject_toml: str) -> None:
    with open(tmp_path / ".python-version", "w") as f:
        f.write("3.13\n")
    with open(tmp_path / "pyproject.toml", "w") as f:
        f.write(pyproject_toml)
    assert infer_python_version(tmp_path) == "3.13"
    os.remove(tmp_path / ".python-version")
    assert infer_python_version(tmp_path) == "3.12"


def test_build_dockerfile_content(dockerfile: str) -> None:
    assert build_dockerfile_content("3.14", 4502) == dockerfile


def test__check_deployment_config(
    pyproject_toml: str, pyproject_toml_with_ui: str, tmp_path: Path
) -> None:
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        (tmp_path / ".env").touch()
        with open(tmp_path / "pyproject.toml", "w") as f:
            f.write(pyproject_toml)
        conf_dir = _check_deployment_config(tmp_path)
        assert str(conf_dir) == str(tmp_path)
        with open(tmp_path / "pyproject.toml", "w") as f:
            f.write(pyproject_toml_with_ui)
        with pytest.raises(click.ClickException):
            _check_deployment_config(tmp_path)
    except Exception as e:
        raise e
    finally:
        os.chdir(cwd)


def test__create_file_for_container(
    dockerfile: str, tmp_path: Path, pyproject_toml: str
) -> None:
    cwd = Path.cwd()
    os.chdir(tmp_path)
    (tmp_path / ".env").touch()
    with open(tmp_path / "pyproject.toml", "w") as f:
        f.write(pyproject_toml)
    _create_file_for_container(
        output_file="Dockerfile",
        deployment_file=tmp_path,
        python_version="3.14",
        port=4502,
    )
    try:
        assert (tmp_path / "Dockerfile").exists()
        assert (tmp_path / ".dockerignore").exists()
        assert (tmp_path / "Dockerfile").read_text(encoding="utf-8") == dockerfile
        content = (tmp_path / ".dockerignore").read_text()
        assert DEFAULT_DOCKER_IGNORE in content
        with pytest.raises(click.ClickException):
            _create_file_for_container(
                output_file="Dockerfile",
                deployment_file=tmp_path,
            )
        _create_file_for_container(
            output_file="Containerfile.llamactl",
            deployment_file=tmp_path,
            exclude=(".env.local",),
            overwrite=True,
        )
        expected_default_content = build_dockerfile_content("3.12", 4501)
        assert (tmp_path / "Containerfile.llamactl").read_text(
            encoding="utf-8"
        ) == expected_default_content
        content = (tmp_path / ".dockerignore").read_text()
        assert DEFAULT_DOCKER_IGNORE in content
        assert ".env.local" in content
    except Exception as e:
        raise e
    finally:
        os.chdir(cwd)
