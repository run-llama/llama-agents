"""Git push utilities for deployment code pushing."""

from __future__ import annotations

import subprocess

from llama_agents.core.git.git_util import FULL_SHA_RE


def _git_remote_name(deployment_id: str) -> str:
    return f"llamaagents-{deployment_id}"


def has_deployment_git_remote(deployment_id: str) -> bool:
    """Return whether the standard deployment git remote exists."""
    result = subprocess.run(
        ["git", "remote", "get-url", _git_remote_name(deployment_id)],
        capture_output=True,
    )
    return result.returncode == 0


def get_deployment_git_url(base_url: str, deployment_id: str) -> str:
    """Build the git endpoint URL for a deployment."""
    api_url = base_url.rstrip("/")
    return f"{api_url}/api/v1beta1/deployments/{deployment_id}/git"


def get_api_key() -> str | None:
    """Get the API key from the current profile.

    Returns None when the backend does not require auth and no key is configured.
    Raises RuntimeError when auth is required but no valid profile/key exists.
    """
    from llama_agents.cli.config.env_service import service

    auth_svc = service.current_auth_service()
    profile = auth_svc.get_current_profile()
    if profile is not None and profile.api_key is not None:
        return profile.api_key
    if auth_svc.env.requires_auth:
        raise RuntimeError("Not authenticated. Run `llamactl auth login` first.")
    return None


def _set_extra_headers(git_url: str, api_key: str | None, project_id: str) -> None:
    """Configure git http.extraHeader entries for auth and project-id.

    Clears existing headers first to avoid duplicates on repeated calls.
    """
    config_key = f"http.{git_url}.extraHeader"
    subprocess.run(
        ["git", "config", "--local", "--unset-all", config_key],
        capture_output=True,
    )
    headers = [f"project-id: {project_id}"]
    if api_key:
        headers.append(f"Authorization: Bearer {api_key}")
    for header in headers:
        subprocess.run(
            ["git", "config", "--local", "--add", config_key, header],
            check=True,
            capture_output=True,
        )


def configure_git_remote(
    git_url: str, api_key: str | None, project_id: str, deployment_id: str
) -> str:
    """Configure a deployment-scoped git remote and extraHeaders.

    Returns the remote name (e.g. 'llamaagents-my-deploy').
    """
    _set_extra_headers(git_url, api_key, project_id)

    remote_name = _git_remote_name(deployment_id)
    result = subprocess.run(
        ["git", "remote", "get-url", remote_name],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        subprocess.run(
            ["git", "remote", "set-url", remote_name, git_url],
            check=True,
            capture_output=True,
        )
    else:
        subprocess.run(
            ["git", "remote", "add", remote_name, git_url],
            check=True,
            capture_output=True,
        )
    return remote_name


def push_to_remote(
    remote_name: str,
    local_ref: str = "HEAD",
    target_ref: str = "refs/heads/main",
) -> subprocess.CompletedProcess[bytes]:
    """Push to an already-configured remote.

    Call ``configure_git_remote`` first to set up auth headers and the remote.
    Returns the CompletedProcess from git push. Caller should check returncode.
    """
    return subprocess.run(
        ["git", "push", remote_name, f"{local_ref}:{target_ref}"],
        capture_output=True,
    )


def git_ref_exists(ref_name: str) -> bool:
    result = subprocess.run(
        ["git", "show-ref", "--verify", "--quiet", ref_name],
        capture_output=True,
    )
    return result.returncode == 0


def internal_push_refspec(git_ref: str | None) -> tuple[str, str]:
    """Compute (local_ref, target_ref) for pushing to an internal code repo.

    Handles branches, tags, full refs, and pinned SHAs.
    """
    if git_ref is None:
        return "main", "refs/heads/main"

    if FULL_SHA_RE.fullmatch(git_ref):
        return git_ref, f"refs/llamactl/pins/{git_ref}"

    if git_ref.startswith("refs/"):
        return git_ref, git_ref

    branch_ref = f"refs/heads/{git_ref}"
    if git_ref_exists(branch_ref):
        return branch_ref, branch_ref

    tag_ref = f"refs/tags/{git_ref}"
    if git_ref_exists(tag_ref):
        return tag_ref, tag_ref

    return git_ref, branch_ref
