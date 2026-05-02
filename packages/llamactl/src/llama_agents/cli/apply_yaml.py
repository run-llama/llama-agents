# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""YAML parsing for ``llamactl deployments apply -f``.

Pure parsing + validation — no network, no client calls.  Takes YAML text,
resolves ``${VAR}`` environment variables, strips ``********`` mask sentinels,
and validates against :class:`DeploymentDisplay`.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

import yaml
from llama_agents.cli.display import DeploymentDisplay, DeploymentSpec
from pydantic import ValidationError
from yaml.nodes import MappingNode

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")
_ANNOTATION_RE = re.compile(r"^\s*## (ERROR|WARNING):")
_APPLY_SPEC_FIELDS = {
    "repo_url",
    "deployment_file_path",
    "git_ref",
    "appserver_version",
    "suspended",
    "personal_access_token",
}


@dataclass(frozen=True)
class FieldError:
    path: tuple[str | int, ...]
    severity: str
    message: str


class ApplyYamlError(Exception):
    """Base error for YAML apply parsing/validation failures."""

    def __init__(
        self,
        message: str,
        *,
        errors: list[FieldError] | None = None,
        original_error: Exception | None = None,
    ) -> None:
        self.errors = errors or [FieldError((), "error", message)]
        self.original_error = original_error
        super().__init__(message)


class UnresolvedEnvVarsError(ApplyYamlError):
    """Raised when ``${VAR}`` references cannot be resolved."""

    def __init__(self, unresolved: list[str]) -> None:
        self.unresolved = unresolved
        message = f"unresolved environment variables: {', '.join(sorted(unresolved))}"
        super().__init__(message)


_ENV_STRING_SPEC_FIELDS = (
    "repo_url",
    "deployment_file_path",
    "git_ref",
    "appserver_version",
    "personal_access_token",
)


def _resolve_string(text: str, unresolved: list[str]) -> str:
    def _replacer(match: re.Match[str]) -> str:
        var = match.group(1)
        env_val = os.environ.get(var)
        if env_val is None:
            unresolved.append(var)
            return match.group(0)  # leave ${VAR} as-is
        return env_val

    return _ENV_VAR_RE.sub(_replacer, text)


def _resolve_spec_env_vars(spec: DeploymentSpec) -> DeploymentSpec:
    unresolved: list[str] = []
    updates: dict[str, Any] = {}

    for field in _ENV_STRING_SPEC_FIELDS:
        value = getattr(spec, field)
        if isinstance(value, str):
            updates[field] = _resolve_string(value, unresolved)

    if spec.secrets is not None:
        updates["secrets"] = {
            name: _resolve_string(value, unresolved)
            if isinstance(value, str)
            else value
            for name, value in spec.secrets.items()
        }

    if unresolved:
        raise UnresolvedEnvVarsError(unresolved)
    return spec.model_copy(update=updates)


def _load_yaml_mapping(text: str) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ApplyYamlError(f"invalid YAML: {exc}", original_error=exc) from exc

    if not isinstance(raw, dict):
        raise ApplyYamlError(
            f"expected a YAML mapping at the top level, got {type(raw).__name__}"
        )
    return raw


def _strip_existing_annotations(text: str) -> str:
    lines = text.splitlines(keepends=True)
    return "".join(line for line in lines if not _ANNOTATION_RE.match(line))


def _path_label(path: tuple[str | int, ...]) -> str:
    return ".".join(str(part) for part in path)


def _annotation_line(error: FieldError, *, include_path: bool = False) -> str:
    severity = error.severity.upper()
    message = error.message
    if include_path and error.path:
        message = f"{_path_label(error.path)}: {message}"
    return f"## {severity}: {message}\n"


def _key_insert_line(lines: list[str], key_line: int) -> int:
    indent = len(lines[key_line]) - len(lines[key_line].lstrip(" "))
    current = key_line
    while current > 0:
        previous = lines[current - 1]
        previous_indent = len(previous) - len(previous.lstrip(" "))
        if previous_indent != indent or not previous.lstrip(" ").startswith("## "):
            break
        current -= 1
    return current


def _index_mapping_node(
    node: MappingNode,
    *,
    path: tuple[str | int, ...] = (),
    index: dict[tuple[str | int, ...], int],
) -> None:
    for key_node, value_node in node.value:
        if not hasattr(key_node, "value"):
            continue
        key = key_node.value
        child_path = (*path, key)

        if child_path in {("name",), ("generate_name",)}:
            index[child_path] = key_node.start_mark.line
        elif len(child_path) == 2 and child_path[0] == "spec":
            if key in _APPLY_SPEC_FIELDS or key == "secrets":
                index[child_path] = key_node.start_mark.line
        elif len(child_path) == 3 and child_path[:2] == ("spec", "secrets"):
            index[child_path] = key_node.start_mark.line

        if isinstance(value_node, MappingNode):
            _index_mapping_node(value_node, path=child_path, index=index)


def _index_apply_paths(text: str) -> dict[tuple[str | int, ...], int] | None:
    try:
        root = yaml.compose(text)
    except yaml.YAMLError:
        return None
    if not isinstance(root, MappingNode):
        return None

    index: dict[tuple[str | int, ...], int] = {}
    _index_mapping_node(root, index=index)
    return index


def annotate_yaml_with_errors(text: str, errors: list[FieldError]) -> str:
    stripped = _strip_existing_annotations(text)
    if not errors:
        return stripped

    lines = stripped.splitlines(keepends=True)
    index = _index_apply_paths(stripped)
    if index is None:
        return (
            "".join(_annotation_line(error, include_path=True) for error in errors)
            + stripped
        )

    file_errors: list[FieldError] = []
    grouped: dict[int, list[FieldError]] = {}
    for error in errors:
        key_line = index.get(error.path)
        if key_line is None:
            file_errors.append(error)
            continue
        grouped.setdefault(_key_insert_line(lines, key_line), []).append(error)

    output: list[str] = []
    output.extend(_annotation_line(error, include_path=True) for error in file_errors)
    for line_no, line in enumerate(lines):
        for error in grouped.get(line_no, []):
            indent = len(line) - len(line.lstrip(" "))
            output.append(" " * indent + _annotation_line(error))
        output.append(line)
    return "".join(output)


# ---------------------------------------------------------------------------
# Main parse entry point
# ---------------------------------------------------------------------------


def parse_apply_yaml(text: str) -> DeploymentDisplay:
    """Parse YAML apply input into a validated :class:`DeploymentDisplay`.

    1. ``yaml.safe_load`` → dict.
    2. Drop ``status`` (round-trip artifact from ``get -o yaml``).
    3. Validate against :class:`DeploymentDisplay` (pydantic handles
       ``extra="forbid"`` rejection for typos / excluded fields).
    4. Resolve ``${VAR}`` env vars in typed string fields under ``spec``.
    5. Strip ``********`` mask sentinels from ``spec.secrets`` and
       ``spec.personal_access_token``.
    6. Wrap :class:`~pydantic.ValidationError` into :class:`ApplyYamlError`.
    """
    raw = _load_yaml_mapping(text)

    # Drop read-only status block.
    raw.pop("status", None)

    try:
        display = DeploymentDisplay.model_validate(raw)
    except ValidationError as exc:
        errors = [
            FieldError(
                path=tuple(
                    part for part in error["loc"] if isinstance(part, (str, int))
                ),
                severity="error",
                message=str(error["msg"]),
            )
            for error in exc.errors()
        ]
        raise ApplyYamlError(str(exc), errors=errors, original_error=exc) from exc

    display = display.model_copy(update={"spec": _resolve_spec_env_vars(display.spec)})
    return display.without_mask_sentinels()


# ---------------------------------------------------------------------------
# Lightweight delete helper
# ---------------------------------------------------------------------------


def parse_delete_yaml_name(text: str) -> str:
    """Extract the ``name`` field from YAML for a delete operation.

    No env resolution, no model validation — just pull the top-level
    ``name`` string.
    """
    raw = _load_yaml_mapping(text)

    name = raw.get("name")
    if name is None:
        raise ApplyYamlError("missing required field: name")
    if not isinstance(name, str):
        raise ApplyYamlError(f"name must be a string, got {type(name).__name__}")
    return name
