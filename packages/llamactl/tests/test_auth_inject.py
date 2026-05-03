from pathlib import Path

from click.testing import CliRunner
from llama_agents.cli.app import app
from llama_agents.cli.config._config import config_manager
from llama_agents.cli.config.schema import Environment
from pytest import MonkeyPatch


def test_auth_inject_writes_env_file(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    # Isolate config dir to temp
    cfg_dir = tmp_path / ".config" / "llamactl"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("LLAMACTL_CONFIG_DIR", str(cfg_dir))

    # Create environment and profile
    # Ensure the global ConfigManager used by the CLI resolves to this temp dir
    config_manager.cache_clear()  # reset cached instance to honor env var
    cm = config_manager()
    env = Environment(
        api_url="https://api.example.com", requires_auth=True, min_llamactl_version=None
    )
    cm.create_or_update_environment(
        env.api_url, env.requires_auth, env.min_llamactl_version
    )

    created = cm.create_profile(
        name="default",
        api_url=env.api_url,
        project_id="proj-123",
        api_key="sk-test-abc",
    )
    cm.set_settings_current_environment(env.api_url)
    cm.set_settings_current_profile(created.name)

    # Run command
    runner = CliRunner()
    env_file = tmp_path / ".env"
    result = runner.invoke(app, ["auth", "inject", "--env-file", str(env_file)])

    assert result.exit_code == 0, result.output
    contents = env_file.read_text()
    assert "LLAMA_CLOUD_API_KEY='sk-test-abc'" in contents
    assert "LLAMA_CLOUD_BASE_URL='https://api.example.com'" in contents
    assert "LLAMA_AGENTS_PROJECT_ID='proj-123'" in contents
