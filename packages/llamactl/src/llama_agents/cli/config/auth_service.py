import asyncio

import httpx
from llama_agents.cli.auth.client import PlatformAuthClient, RefreshMiddleware
from llama_agents.cli.config._config import ConfigManager
from llama_agents.cli.config.schema import Auth, DeviceOIDC, Environment
from llama_agents.cli.utils.redact import redact_api_key
from llama_agents.core.client.manage_client import ControlPlaneClient
from llama_agents.core.schema import VersionResponse
from llama_agents.core.schema.projects import ProjectSummary


class AuthService:
    def __init__(self, config_manager: ConfigManager, env: Environment):
        self.config_manager = config_manager
        self.env = env

    def list_profiles(self) -> list[Auth]:
        return self.config_manager.list_profiles(self.env.api_url)

    def get_profile(self, name: str) -> Auth | None:
        return self.config_manager.get_profile(name, self.env.api_url)

    def get_profile_by_id(self, id: str) -> Auth | None:
        return self.config_manager.get_profile_by_id(id)

    def set_current_profile(self, name: str) -> None:
        self.config_manager.set_settings_current_profile(name)

    def select_any_profile(self) -> None:
        # best effort to select a profile within the environment
        profiles = self.list_profiles()
        if profiles:
            self.set_current_profile(profiles[0].name)

    def get_current_profile(self) -> Auth | None:
        return self.config_manager.get_current_profile(self.env.api_url)

    def create_profile_from_token(self, project_id: str, api_key: str | None) -> Auth:
        base = _auto_profile_name_from_token(api_key or "") if api_key else "default"
        auth = self.config_manager.create_profile(
            base, self.env.api_url, project_id, api_key
        )
        self.config_manager.set_settings_current_profile(auth.name)
        return auth

    def create_or_update_profile_from_oidc(
        self, project_id: str, device_oidc: DeviceOIDC
    ) -> Auth:
        base = device_oidc.email
        existing = self.config_manager.get_profile_by_device_user_id(
            self.env.api_url, device_oidc.user_id
        )
        if existing:
            existing.device_oidc = device_oidc
            self.config_manager.update_profile(existing)
            auth = existing
        else:
            auth = self.config_manager.create_profile(
                base, self.env.api_url, project_id, device_oidc=device_oidc
            )
        self.config_manager.set_settings_current_profile(auth.name)
        return auth

    def update_profile(self, profile: Auth) -> None:
        self.config_manager.update_profile(profile)

    async def delete_profile(self, name: str) -> bool:
        profile = self.get_profile(name)
        if profile and profile.api_key_id:
            async with self.profile_client(profile) as client:
                try:
                    await client.delete_api_key(profile.api_key_id)
                except Exception:
                    pass
        return self.config_manager.delete_profile(name, self.env.api_url)

    def set_project(self, name: str, project_id: str) -> None:
        self.config_manager.set_project(name, self.env.api_url, project_id)

    def fetch_server_version(self) -> VersionResponse:
        async def _fetch_server_version() -> VersionResponse:
            async with ControlPlaneClient.ctx(self.env.api_url) as client:
                version = await client.server_version()
                return version

        return asyncio.run(_fetch_server_version())

    def _validate_token_and_list_projects(self, api_key: str) -> list[ProjectSummary]:
        async def _run() -> list[ProjectSummary]:
            async with ControlPlaneClient.ctx(self.env.api_url, api_key) as client:
                return await client.list_projects()

        return asyncio.run(_run())

    def auth_middleware(self, profile: Auth | None = None) -> httpx.Auth | None:
        profile = profile or self.get_current_profile()
        if profile and profile.device_oidc:
            _profile = profile  # copy to assist type checker being inflexible

            async def _on_refresh(updated: DeviceOIDC) -> None:
                # Persist refreshed tokens to the database synchronously within async wrapper
                self.refresh_to_db(_profile.id, updated)

            return RefreshMiddleware(
                profile.device_oidc,
                _on_refresh,
            )
        return None

    def refresh_to_db(self, profile_id: str, device_oidc: DeviceOIDC) -> None:
        profile = self.get_profile_by_id(profile_id)
        if profile:
            profile.device_oidc = device_oidc
            self.config_manager.update_profile(profile)

    def profile_client(self, profile: Auth | None = None) -> PlatformAuthClient:
        profile = profile or self.get_current_profile()
        if not profile:
            raise ValueError("No active profile")
        return PlatformAuthClient(
            profile.api_url, profile.api_key, self.auth_middleware(profile)
        )


def _auto_profile_name_from_token(api_key: str) -> str:
    token = api_key or "token"
    return redact_api_key(token)
