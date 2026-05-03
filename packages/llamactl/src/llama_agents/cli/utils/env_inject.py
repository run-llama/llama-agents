from __future__ import annotations

from typing import Dict

from llama_agents.cli.config.schema import Auth


def env_vars_from_profile(profile: Auth) -> Dict[str, str]:
    """Return env var values derived strictly from the given profile.

    Produces the three keys expected by CLI commands:
    - LLAMA_CLOUD_API_KEY
    - LLAMA_CLOUD_BASE_URL
    - LLAMA_AGENTS_PROJECT_ID
    """
    values: Dict[str, str] = {}
    if profile.api_key:
        values["LLAMA_CLOUD_API_KEY"] = profile.api_key
    if profile.api_url:
        values["LLAMA_CLOUD_BASE_URL"] = profile.api_url
    if profile.project_id:
        values["LLAMA_AGENTS_PROJECT_ID"] = profile.project_id
    return values
