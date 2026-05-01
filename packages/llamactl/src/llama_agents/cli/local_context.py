# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Gather local context for deployment scaffolding.

Reads the current working directory: git remote / branch, deployment config,
``.env`` secrets, and the installed appserver version. Produces a
:class:`LocalContext` consumable by ``deployments template`` and the Textual
deployment form.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from llama_agents.cli.env import load_env_secrets_from_string
from llama_agents.cli.utils.version import get_installed_appserver_version
from llama_agents.core.deployment_config import (
    DEFAULT_DEPLOYMENT_NAME,
    read_deployment_config,
)
from llama_agents.core.git.git_util import (
    get_current_branch,
    get_git_root,
    is_git_repo,
    list_remotes,
)

# Module-level — reused per remote in :func:`normalize_git_url_to_http`.
_SCP_URL_RE = re.compile(r"^(?:(?P<user>[^@]+)@)?(?P<host>[^:/\s]+):(?P<path>[^/].+)$")


@dataclass(frozen=True)
class LocalContext:
    """Local-machine signals used to scaffold a deployment YAML.

    All fields are passive: this is a snapshot of what we observed, not a
    decision about what the resulting deployment should look like.
    """

    is_git_repo: bool = False
    repo_url: str | None = None
    git_ref: str | None = None
    generate_name: str | None = None
    deployment_file_path: str | None = None
    available_secrets: dict[str, str] = field(default_factory=dict)
    required_secret_names: list[str] = field(default_factory=list)
    installed_appserver_version: str | None = None
    warnings: list[str] = field(default_factory=list)


def gather_local_context() -> LocalContext:
    """Read cwd-rooted signals into a :class:`LocalContext`.

    Failures while parsing the deployment config become a warning rather than
    an exception so the scaffolding command stays useful in broken trees.
    """

    warnings: list[str] = []
    generate_name: str | None = None
    deployment_file_path: str | None = None
    required_secret_names: list[str] = []

    has_git = is_git_repo()
    try:
        config = read_deployment_config(Path("."), Path("."))
        if config.name != DEFAULT_DEPLOYMENT_NAME:
            generate_name = config.name
        required_secret_names = list(config.required_env_vars)
    except Exception:
        warnings.append("Could not parse local deployment config. It may be invalid.")

    repo_url: str | None = None
    git_ref: str | None = None
    if has_git:
        repo_url = pick_preferred_remote(list_remotes())
        git_ref = get_current_branch()
        try:
            root = get_git_root()
            if root != Path.cwd():
                deployment_file_path = str(Path.cwd().relative_to(root))
        except Exception:
            pass

    available_secrets: dict[str, str] = {}
    try:
        available_secrets = load_env_secrets_from_string(Path(".env").read_text())
    except FileNotFoundError:
        # No `.env` is the common offline path — no warning.
        pass
    except OSError as exc:
        warnings.append(f"Could not read .env: {exc.strerror or exc}")

    return LocalContext(
        is_git_repo=has_git,
        repo_url=repo_url,
        git_ref=git_ref,
        generate_name=generate_name,
        deployment_file_path=deployment_file_path,
        available_secrets=available_secrets,
        required_secret_names=required_secret_names,
        installed_appserver_version=get_installed_appserver_version(),
        warnings=warnings,
    )


def pick_preferred_remote(remotes: list[str]) -> str | None:
    """Normalize and dedupe remotes, preferring github.com over others."""
    seen: set[str] = set()
    best: str | None = None
    for remote in remotes:
        normalized = normalize_git_url_to_http(remote)
        if normalized in seen:
            continue
        seen.add(normalized)
        if best is None or ("github.com" in normalized and "github.com" not in best):
            best = normalized
    return best


def normalize_git_url_to_http(url: str) -> str:
    """Best-effort normalize a git URL to an https URL.

    Handles the common SSH/SCP shapes (``git@host:path``,
    ``ssh://git@host:port/path``) and bare ``host/path`` strings. Strips
    credentials and any explicit port.
    """
    candidate = (url or "").strip()

    has_scheme = "://" in candidate
    if not has_scheme:
        scp_match = _SCP_URL_RE.match(candidate)
        if scp_match:
            host = scp_match.group("host")
            path = scp_match.group("path").lstrip("/")
            if path.endswith(".git"):
                path = path[:-4]
            return f"https://{host}/{path}"

    parsed = urlsplit(candidate if has_scheme else f"https://{candidate}")
    netloc = parsed.netloc.split("@", 1)[-1]
    scheme = parsed.scheme.lower()
    # SSH transport ports (e.g. :7999) are meaningless over HTTPS — drop them.
    # HTTP/HTTPS ports are intentional (self-hosted on a non-standard port).
    if scheme not in ("http", "https") and ":" in netloc:
        netloc = netloc.split(":", 1)[0]
    path = parsed.path.lstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    if path:
        return f"https://{netloc}/{path}"
    return f"https://{netloc}"
