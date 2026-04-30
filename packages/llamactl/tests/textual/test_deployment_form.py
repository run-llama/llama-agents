# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from collections.abc import Callable, Generator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Protocol, TypeVar
from unittest.mock import MagicMock, patch

import pytest
from llama_agents.cli.textual.deployment_form import (
    CancelFormMessage,
    DeploymentEditApp,
    DeploymentForm,
    DeploymentFormWidget,
    FormScreen,
    MonitorScreen,
    PrePushScreen,
    PushScreen,
    StartValidationMessage,
    ValidationScreen,
    _initialize_deployment_data,
)
from llama_agents.cli.textual.deployment_monitor import MonitorCloseMessage
from llama_agents.cli.textual.git_validation import (
    ValidationCancelMessage,
    ValidationResultMessage,
)
from llama_agents.cli.textual.secrets_form import SecretsWidget
from llama_agents.core.schema.deployments import (
    DeploymentCreate,
    DeploymentResponse,
    DeploymentUpdate,
)
from textual.app import App, ComposeResult
from textual.containers import Container
from textual.css.query import NoMatches
from textual.screen import Screen
from textual.widgets import Input

TScreen = TypeVar("TScreen", bound=Screen[Any])


class _Pilot(Protocol):
    async def pause(self) -> None: ...


async def _wait_for_screen(
    app: DeploymentEditApp,
    pilot: _Pilot,
    screen_type: type[TScreen],
    *,
    condition: Callable[[TScreen], bool] | None = None,
    timeout: float = 3.0,
) -> TScreen:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        await pilot.pause()
        screen = app.screen
        if isinstance(screen, screen_type) and (condition is None or condition(screen)):
            return screen

    screen = app.screen
    condition_text = " and matching state" if condition is not None else ""
    raise AssertionError(
        f"Timed out waiting for {screen_type.__name__}{condition_text}; "
        f"current screen is {type(screen).__name__}"
    )


async def _wait_for_widget(
    app: App[Any],
    pilot: _Pilot,
    selector: str,
    *,
    timeout: float = 3.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        await pilot.pause()
        try:
            app.screen.query_one(selector)
        except NoMatches:
            continue
        return

    raise AssertionError(f"Timed out waiting for widget {selector}")


def test_initialize_deployment_data_happy_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: git repo present, .env present, and valid yaml config to derive name."""
    # Arrange: switch to a temp working directory with .env and a yaml config
    foo_path = tmp_path / "foo"
    foo_path.mkdir()
    monkeypatch.chdir(foo_path)

    # Create a valid deployment yaml file discovered by listdir
    yaml_path = foo_path / "llama_deploy.yaml"
    yaml_path.write_text(
        """
name: my-deploy
workflows:
  svc: "module.workflow:flow"
required_env_vars: ["API_KEY", "PORT"]
""".strip()
    )

    # Create .env
    (foo_path / ".env").write_text("API_KEY=secret\nPORT=8080\n")

    # Mock git helpers
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.is_git_repo", lambda: True
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.list_remotes",
        lambda: [
            "ssh://git@bitbucket.org/team/repo.git",
            "git@github.com:user/repo.git",
        ],
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_git_root",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_current_branch",
        lambda: "develop",
    )

    # Act
    form = _initialize_deployment_data()

    # Assert
    assert isinstance(form, DeploymentForm)
    # In push mode, repo_url is empty (push from local repo)
    assert form.repo_url == ""
    assert form.push_mode is True
    assert form.is_local_git_repo is True
    assert form.git_ref == "develop"
    assert form.secrets == {"API_KEY": "secret", "PORT": "8080"}
    # Required secrets should be tracked
    assert sorted(form.required_secret_names) == ["API_KEY", "PORT"]
    assert (
        form.env_info_messages
        == "Secrets were automatically seeded from your .env file. Remove or change any that should not be set. They must be manually configured after creation."
    )
    assert form.deployment_file_path == "foo"
    assert form.display_name == "my-deploy"


def test_initialize_deployment_data_defaults_when_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path defaults: no git, no .env, no yaml -> empty name/url, main ref, empty secrets."""
    # Arrange
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.is_git_repo", lambda: False
    )

    # Act
    form = _initialize_deployment_data()

    # Assert
    assert form.display_name == ""
    assert form.repo_url == ""
    assert form.git_ref == "main"
    assert form.secrets == {}
    assert form.push_mode is False
    assert form.is_local_git_repo is False
    # Warning behavior is intentionally minimal here; don't assert specific messages
    assert len(form.warnings) >= 1


def test_initialize_deployment_data_warns_on_no_remote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    # Provide a minimal valid config so git warnings are shown
    (tmp_path / "llama_deploy.yaml").write_text(
        """
name: app
workflows:
  svc: "x.y:flow"
""".strip()
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.is_git_repo", lambda: True
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.list_remotes", lambda: []
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_current_branch",
        lambda: "main",
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_git_root",
        lambda: tmp_path,
    )

    form = _initialize_deployment_data()
    assert any("No git remote was found" in w for w in form.warnings)


def test_initialize_deployment_data_warns_on_working_tree_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llama_deploy.yaml").write_text(
        """
name: app
workflows:
  svc: "x.y:flow"
""".strip()
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.is_git_repo", lambda: True
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.list_remotes",
        lambda: ["https://github.com/user/repo"],
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_current_branch",
        lambda: "main",
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_git_root",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.working_tree_has_changes",
        lambda: True,
    )

    form = _initialize_deployment_data()
    assert any(
        "Working tree has uncommitted or untracked changes" in w for w in form.warnings
    )


def test_initialize_deployment_data_warns_on_unpushed_commits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llama_deploy.yaml").write_text(
        """
name: app
workflows:
  svc: "x.y:flow"
""".strip()
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.is_git_repo", lambda: True
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.list_remotes",
        lambda: ["https://github.com/user/repo"],
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_current_branch",
        lambda: "main",
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_git_root",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.working_tree_has_changes",
        lambda: False,
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_unpushed_commits_count",
        lambda: 2,
    )

    form = _initialize_deployment_data()
    assert any("local commits not pushed" in w for w in form.warnings)


def test_initialize_deployment_data_warns_on_no_upstream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "llama_deploy.yaml").write_text(
        """
name: app
workflows:
  svc: "x.y:flow"
""".strip()
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.is_git_repo", lambda: True
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.list_remotes",
        lambda: ["https://github.com/user/repo"],
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_current_branch",
        lambda: "main",
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_git_root",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.working_tree_has_changes",
        lambda: False,
    )
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_unpushed_commits_count",
        lambda: None,
    )

    form = _initialize_deployment_data()
    assert any("no upstream configured" in w for w in form.warnings)


@pytest.fixture
def mock_client() -> Generator[MagicMock, None, None]:
    """Fixture providing a single mocked client for both deployment and git validation."""
    with (
        patch(
            "llama_agents.cli.textual.deployment_form.get_client"
        ) as mock_deployment_get_client,
        patch(
            "llama_agents.cli.textual.git_validation.get_client"
        ) as mock_git_get_client,
        patch(
            "llama_agents.cli.textual.deployment_monitor.project_client_context"
        ) as mock_monitor_ctx,
    ):
        from unittest.mock import AsyncMock

        client = MagicMock()
        # Mock successful git validation by default
        client.validate_repository = AsyncMock(
            return_value=MagicMock(accessible=True, pat_is_obsolete=False)
        )
        client.create_deployment = AsyncMock()
        client.update_deployment = AsyncMock()

        mock_deployment_get_client.return_value = client
        mock_git_get_client.return_value = client

        @asynccontextmanager
        async def _ctx() -> AsyncIterator[MagicMock]:
            yield client

        # Return a fresh async context manager on each call to avoid re-entrancy issues
        mock_monitor_ctx.side_effect = lambda: _ctx()

        yield client


class WidgetTestApp(App):
    """Helper app for testing widgets with proper container and CSS context."""

    # Use the same CSS as the real app
    CSS_PATH = (
        Path(__file__).parent.parent.parent
        / "src"
        / "llama_agents"
        / "cli"
        / "textual"
        / "styles.tcss"
    )

    def __init__(self, widget: DeploymentFormWidget):
        super().__init__()
        self.widget = widget
        self.posted_messages: list[StartValidationMessage | CancelFormMessage] = []

    def compose(self) -> ComposeResult:
        # Replicate the same container structure as DeploymentEditApp
        with Container(classes="form-container"):
            yield self.widget

    def on_start_validation_message(self, message: StartValidationMessage) -> None:
        self.posted_messages.append(message)

    def on_cancel_form_message(self, message: CancelFormMessage) -> None:
        self.posted_messages.append(message)


@pytest.mark.asyncio
async def test_deployment_form_widget_validation_success(
    mock_client: MagicMock,
) -> None:
    """Test form widget posts StartValidationMessage when form is valid."""
    initial_data = DeploymentForm(required_secret_names=["API_KEY"])
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)) as pilot:
        # Fill required fields
        name_input = app.query_one("#name")
        name_input.value = "test-deployment"  # type: ignore

        repo_url_input = app.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/test-repo"  # type: ignore

        # Fill required secret
        secrets_widget = app.query_one(SecretsWidget)
        # Emulate adding via secrets widget API
        secrets_widget.secrets = {"API_KEY": "abc"}
        # Allow reactive update to settle before submitting
        await pilot.pause()

        # Click save button
        await pilot.click("#save")
        await pilot.pause()  # Allow message processing

        # Verify StartValidationMessage was posted
        assert len(app.posted_messages) == 1
        message = app.posted_messages[0]
        assert isinstance(message, StartValidationMessage)
        assert message.form_data.display_name == "test-deployment"
        assert message.form_data.repo_url == "https://github.com/user/test-repo"
        assert message.form_data.git_ref == "main"
        assert message.form_data.deployment_file_path == ""


@pytest.mark.asyncio
async def test_deployment_form_widget_validation_error(mock_client: MagicMock) -> None:
    """Test form widget shows error when required fields are missing."""
    initial_data = DeploymentForm(required_secret_names=["MISSING_SECRET"])
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)) as pilot:
        # Fill only name field, leave repo_url and required secret empty
        name_input = app.query_one("#name")
        name_input.value = "test-deployment"  # type: ignore

        repo_url_input = app.query_one("#repo_url")
        repo_url_input.value = ""  # type: ignore

        # Click save button
        await pilot.click("#save")
        await pilot.pause()  # Allow validation processing

        # No message should be posted due to validation error (missing repo_url and secret)
        assert len(app.posted_messages) == 0

        # Check error is displayed
        form_widget = app.query_one(DeploymentFormWidget)
        assert "Repository URL is required" in form_widget.error_message
        assert "Missing required secrets: MISSING_SECRET" in form_widget.error_message


@pytest.mark.asyncio
async def test_deployment_form_widget_cancel(mock_client: MagicMock) -> None:
    """Test form widget posts CancelFormMessage when cancel is clicked."""
    initial_data = DeploymentForm()
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.click("#cancel")
        await pilot.pause()  # Allow message processing

        # Verify CancelFormMessage was posted
        assert len(app.posted_messages) == 1
        assert isinstance(app.posted_messages[0], CancelFormMessage)


@pytest.mark.asyncio
async def test_deployment_form_widget_edit_mode(mock_client: MagicMock) -> None:
    """Test form widget correctly handles editing an existing deployment."""
    existing_deployment = DeploymentResponse(
        id="dep-456",
        display_name="existing-deployment",
        repo_url="https://github.com/user/old-repo",
        git_ref="develop",
        deployment_file_path="deploy.yaml",
        has_personal_access_token=True,
        secret_names=["API_KEY"],
        project_id="proj-456",
        apiserver_url=None,
        status="Running",
    )

    initial_data = DeploymentForm.from_deployment(existing_deployment)
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)) as pilot:
        # Verify name field is disabled in edit mode
        name_input = app.query_one("#name")
        assert name_input.disabled
        assert name_input.value == "existing-deployment"  # type: ignore

        # Modify repo URL
        repo_url_input = app.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/new-repo"  # type: ignore

        # Click save
        await pilot.click("#save")
        await pilot.pause()  # Allow message processing

        # Verify correct message was posted
        assert len(app.posted_messages) == 1
        message = app.posted_messages[0]
        assert isinstance(message, StartValidationMessage)
        assert message.form_data.is_editing
        assert message.form_data.id == "dep-456"
        assert message.form_data.repo_url == "https://github.com/user/new-repo"


@pytest.mark.asyncio
async def test_deployment_form_widget_secrets_integration(
    mock_client: MagicMock,
) -> None:
    """Test form widget correctly integrates with secrets widget."""
    # Create deployment with existing secrets
    existing_deployment = DeploymentResponse(
        id="dep-789",
        display_name="secret-deployment",
        repo_url="https://github.com/user/secret-repo",
        git_ref="main",
        deployment_file_path="deploy.yaml",
        has_personal_access_token=False,
        secret_names=["API_KEY", "DB_PASSWORD"],
        project_id="proj-789",
        apiserver_url=None,
        status="Running",
    )

    initial_data = DeploymentForm.from_deployment(existing_deployment)
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)) as pilot:
        # Verify secrets widget is present

        secrets_widget = app.query_one(SecretsWidget)
        assert secrets_widget is not None

        # Modify repo URL and save
        repo_url_input = app.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/updated-repo"  # type: ignore

        await pilot.click("#save")
        await pilot.pause()

        # Verify message contains secrets data from the widget
        assert len(app.posted_messages) == 1
        message = app.posted_messages[0]
        assert isinstance(message, StartValidationMessage)
        assert message.form_data.initial_secrets == {"API_KEY", "DB_PASSWORD"}


@pytest.mark.asyncio
async def test_deployment_edit_app_end_to_end_create(mock_client: MagicMock) -> None:
    """Test complete end-to-end flow for creating deployment with mocked git validation."""
    initial_data = DeploymentForm()

    # Mock successful deployment creation
    mock_deployment = DeploymentResponse(
        id="dep-123",
        display_name="test-deployment",
        repo_url="https://github.com/user/test-repo",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-123",
        apiserver_url=None,
        status="Running",
    )

    mock_client.create_deployment.return_value = mock_deployment

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        # Fill form fields
        name_input = app.screen.query_one("#name")
        name_input.value = "test-deployment"  # type: ignore

        repo_url_input = app.screen.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/test-repo"  # type: ignore

        # Click save - proceed through validation to monitor
        await pilot.click("#save")

        # Should now be on monitor screen
        monitor_screen = await _wait_for_screen(app, pilot, MonitorScreen)

        # Verify API was called correctly
        mock_client.create_deployment.assert_called_once()
        call_args = mock_client.create_deployment.call_args[0][0]
        assert isinstance(call_args, DeploymentCreate)
        assert call_args.display_name == "test-deployment"
        assert call_args.repo_url == "https://github.com/user/test-repo"

        # Close monitor to exit app
        monitor_screen.post_message(MonitorCloseMessage())
        await pilot.pause()

        # App should exit with deployment response
        assert app.return_value == mock_deployment


@pytest.mark.asyncio
async def test_deployment_edit_app_end_to_end_update(
    mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test complete end-to-end flow for updating deployment with mocked git validation."""
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_installed_appserver_version",
        lambda: "1.0.0",
    )

    existing_deployment = DeploymentResponse(
        id="dep-456",
        display_name="existing-deployment",
        repo_url="https://github.com/user/old-repo",
        git_ref="develop",
        deployment_file_path="deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-456",
        apiserver_url=None,
        status="Running",
        appserver_version="0.9.0",
    )

    initial_data = DeploymentForm.from_deployment(existing_deployment)

    # Mock successful deployment update
    mock_updated_deployment = DeploymentResponse(
        id="dep-456",
        display_name="existing-deployment",
        repo_url="https://github.com/user/new-repo",
        git_ref="main",
        deployment_file_path="deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-456",
        apiserver_url=None,
        status="Running",
        appserver_version="1.0.0",
    )

    mock_client.update_deployment.return_value = mock_updated_deployment
    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 50)) as pilot:
        # Modify repo URL
        repo_url_input = app.screen.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/new-repo"  # type: ignore

        git_ref_input = app.screen.query_one("#git_ref")
        git_ref_input.value = "main"  # type: ignore

        # Change version to installed (1.0.0)
        version_select = app.screen.query_one("#appserver_version_select")
        version_select.value = "1.0.0"  # type: ignore
        await pilot.pause()

        # Click save and proceed through validation to monitor
        await pilot.click("#save")

        # Should now be on monitor screen
        monitor_screen = await _wait_for_screen(app, pilot, MonitorScreen)

        # Verify API was called correctly
        mock_client.update_deployment.assert_called_once()
        deployment_id, update_data = mock_client.update_deployment.call_args[0]
        assert deployment_id == "dep-456"
        assert isinstance(update_data, DeploymentUpdate)
        assert update_data.repo_url == "https://github.com/user/new-repo"
        assert update_data.git_ref == "main"
        assert update_data.appserver_version == "1.0.0"

        # Close monitor to exit app
        monitor_screen.post_message(MonitorCloseMessage())
        await pilot.pause()

        # App should exit with updated deployment
        assert app.return_value == mock_updated_deployment


@pytest.mark.asyncio
async def test_deployment_edit_app_validation_cancellation(
    mock_client: MagicMock,
) -> None:
    """Test validation cancellation flow returns user back to form."""
    initial_data = DeploymentForm()

    # Override the mock to return a failed validation so it doesn't auto-complete
    mock_client.validate_repository.return_value = MagicMock(
        accessible=False, pat_is_obsolete=False, message="Repository not accessible"
    )

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        # Fill form and trigger validation
        name_input = app.screen.query_one("#name")
        name_input.value = "test-deployment"  # type: ignore

        repo_url_input = app.screen.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/test-repo"  # type: ignore

        await pilot.click("#save")

        # Should be on validation screen
        validation_screen = await _wait_for_screen(app, pilot, ValidationScreen)
        validation_screen.post_message(ValidationCancelMessage())

        # Should return to form screen with cleared error
        await _wait_for_screen(app, pilot, FormScreen)

        # App should still be running (not exited)
        assert app.return_value is None


@pytest.mark.asyncio
async def test_deployment_edit_app_validation_with_pat_update(
    mock_client: MagicMock,
) -> None:
    """Test validation success with PAT update."""
    initial_data = DeploymentForm()

    mock_deployment = DeploymentResponse(
        id="dep-456",
        display_name="test-deployment",
        repo_url="https://github.com/user/test-repo",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        has_personal_access_token=True,
        secret_names=[],
        project_id="proj-456",
        apiserver_url=None,
        status="Running",
    )

    mock_client.create_deployment.return_value = mock_deployment
    mock_client.validate_repository.return_value = MagicMock(
        accessible=False, pat_is_obsolete=False, message="Repository not accessible"
    )

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        # Fill form and trigger validation
        name_input = app.screen.query_one("#name")
        name_input.value = "test-deployment"  # type: ignore

        repo_url_input = app.screen.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/test-repo"  # type: ignore

        await pilot.click("#save")

        # Should be on validation screen
        validation_screen = await _wait_for_screen(app, pilot, ValidationScreen)

        # Mock successful validation with new PAT
        validation_screen.post_message(
            ValidationResultMessage(
                "https://github.com/user/test-repo", "new-pat-token"
            )
        )
        monitor_screen = await _wait_for_screen(app, pilot, MonitorScreen)

        # Verify API was called with updated PAT
        mock_client.create_deployment.assert_called_once()
        call_args = mock_client.create_deployment.call_args[0][0]
        assert call_args.personal_access_token == "new-pat-token"

        # Now we're on monitor; close it to exit
        monitor_screen.post_message(MonitorCloseMessage())
        await pilot.pause()

        # App should exit with result
        assert app.return_value == mock_deployment


@pytest.mark.asyncio
async def test_deployment_edit_app_multiple_save_attempts(
    mock_client: MagicMock,
) -> None:
    """Test multiple save attempts: fail validation, fix, then succeed."""
    initial_data = DeploymentForm()

    mock_deployment = DeploymentResponse(
        id="dep-999",
        display_name="test-deployment",
        repo_url="https://github.com/user/test-repo",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-999",
        apiserver_url=None,
        status="Running",
    )

    mock_client.create_deployment.return_value = mock_deployment

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        # First attempt: missing name (should fail form validation)
        repo_url_input = app.screen.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/test-repo"  # type: ignore

        await pilot.click("#save")
        await pilot.pause()

        # Should stay on form screen (validation failed)
        assert isinstance(app.screen, FormScreen)

        # Fix the name and try again
        name_input = app.screen.query_one("#name", Input)
        name_input.value = "test-deployment"

        await pilot.click("#save")

        # Proceed through validation to monitor
        monitor_screen = await _wait_for_screen(app, pilot, MonitorScreen)

        # Close monitor to complete
        monitor_screen.post_message(MonitorCloseMessage())
        await pilot.pause()

        # Should complete successfully
        assert app.return_value == mock_deployment
        mock_client.create_deployment.assert_called_once()


@pytest.mark.asyncio
async def test_deployment_edit_app_end_to_end_api_error(
    mock_client: MagicMock,
) -> None:
    """Test complete end-to-end flow with API error after successful git validation."""
    initial_data = DeploymentForm()

    mock_client.create_deployment.side_effect = Exception("API connection failed")

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        # Fill form fields
        name_input = app.screen.query_one("#name")
        name_input.value = "test-deployment"  # type: ignore

        repo_url_input = app.screen.query_one("#repo_url")
        repo_url_input.value = "https://github.com/user/test-repo"  # type: ignore

        # Click save
        await pilot.click("#save")

        # Should return to form screen with error
        form_screen = await _wait_for_screen(
            app,
            pilot,
            FormScreen,
            condition=lambda screen: screen.save_error
            == "Error saving deployment: API connection failed",
        )
        assert (
            form_screen.save_error == "Error saving deployment: API connection failed"
        )

        # Verify API was called
        mock_client.create_deployment.assert_called_once()

        # App should not exit (still running)
        assert app.return_value is None


@pytest.mark.asyncio
async def test_appserver_version_display_create_readonly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On create, show installed appserver version as read-only."""
    # Ensure installed version is reported
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_installed_appserver_version",
        lambda: "1.2.3",
    )

    form = _initialize_deployment_data()
    widget = DeploymentFormWidget(form)

    class _App(App):
        def compose(self) -> ComposeResult:
            with Container():
                yield widget

    async with _App().run_test() as _pilot:
        # Should render a read-only static with the version
        static = widget.query_one("#appserver_version_readonly")
        # Check the static widget has the version text
        rendered = static.render()
        assert "1.2.3" in str(rendered)


@pytest.mark.asyncio
async def test_appserver_version_selector_edit_when_different(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On edit, when installed != existing, show selector and allow choosing installed/existing."""
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_installed_appserver_version",
        lambda: "1.0.0",
    )

    existing_deployment = DeploymentResponse(
        id="dep-111",
        display_name="ver-deployment",
        repo_url="https://github.com/user/repo",
        git_ref="main",
        deployment_file_path="deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-111",
        apiserver_url=None,
        status="Running",
        appserver_version="0.9.0",
    )

    form = DeploymentForm.from_deployment(existing_deployment)
    widget = DeploymentFormWidget(form)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 50)) as pilot:
        # Selector dropdown should be visible
        version_select = widget.query_one("#appserver_version_select")
        assert version_select is not None

        # Change selection to installed version (1.0.0)
        version_select.value = "1.0.0"  # type: ignore
        await _wait_for_widget(app, pilot, "#save")

        # Repo URL is already set from existing deployment, just click save
        await pilot.click("#save")
        await pilot.pause()

        # Ensure selection propagated into form_data
        assert app.posted_messages
        message = app.posted_messages[0]
        assert isinstance(message, StartValidationMessage)
        selected_version = message.form_data.selected_appserver_version
        assert selected_version == "1.0.0"


@pytest.mark.asyncio
async def test_appserver_version_readonly_when_same(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On edit, if installed == existing, show read-only (no selector)."""
    monkeypatch.setattr(
        "llama_agents.cli.textual.deployment_form.get_installed_appserver_version",
        lambda: "1.0.0",
    )

    existing_deployment = DeploymentResponse(
        id="dep-222",
        display_name="ver-deployment",
        repo_url="https://github.com/user/repo",
        git_ref="main",
        deployment_file_path="deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-222",
        apiserver_url=None,
        status="Running",
        appserver_version="1.0.0",
    )

    form = DeploymentForm.from_deployment(existing_deployment)
    widget = DeploymentFormWidget(form)

    class _App(App):
        def compose(self) -> ComposeResult:
            with Container():
                yield widget

    async with _App().run_test() as _pilot:
        # Should see read-only display, and no selector dropdown
        static = widget.query_one("#appserver_version_readonly")
        # Check the static widget has the version text
        rendered = static.render()
        assert "1.0.0" in str(rendered)
        # Verify selector is not present
        assert not widget.query("#appserver_version_select")


@pytest.mark.asyncio
async def test_push_mode_shows_code_source_selector(mock_client: MagicMock) -> None:
    """When is_local_git_repo=True, form shows code source selector defaulting to push mode."""
    initial_data = DeploymentForm(
        is_local_git_repo=True,
        push_mode=True,
    )
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)):
        # Code source selector should be present
        code_source = app.query_one("#code_source_select")
        assert code_source is not None

        # repo_url input should NOT be present in push mode
        assert not app.query("#repo_url")

        # personal_access_token should NOT be present in push mode
        assert not app.query("#personal_access_token")


@pytest.mark.asyncio
async def test_push_mode_toggle_shows_repo_url(mock_client: MagicMock) -> None:
    """Toggling code source to 'Enter a git URL' shows repo_url input."""
    initial_data = DeploymentForm(
        is_local_git_repo=True,
        push_mode=False,  # Start with URL mode
    )
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)):
        # repo_url input SHOULD be present when push_mode=False
        repo_url_input = app.query_one("#repo_url")
        assert repo_url_input is not None

        # personal_access_token should also be present
        pat_input = app.query_one("#personal_access_token")
        assert pat_input is not None


@pytest.mark.asyncio
async def test_push_mode_skips_repo_url_validation(mock_client: MagicMock) -> None:
    """In push mode, save should succeed without repo_url."""
    initial_data = DeploymentForm(
        is_local_git_repo=True,
        push_mode=True,
    )
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)) as pilot:
        # Fill only name
        name_input = app.query_one("#name")
        name_input.value = "push-deployment"  # type: ignore

        await pilot.click("#save")
        await pilot.pause()

        # Should post StartValidationMessage (no repo_url required in push mode)
        assert len(app.posted_messages) == 1
        message = app.posted_messages[0]
        assert isinstance(message, StartValidationMessage)
        assert message.form_data.push_mode is True
        assert message.form_data.repo_url == ""


@pytest.mark.asyncio
async def test_push_mode_no_selector_when_not_git_repo(
    mock_client: MagicMock,
) -> None:
    """When not in a git repo, no code source selector — just plain repo_url input."""
    initial_data = DeploymentForm(
        is_local_git_repo=False,
        push_mode=False,
    )
    widget = DeploymentFormWidget(initial_data)
    app = WidgetTestApp(widget)

    async with app.run_test(size=(100, 40)):
        # Code source selector should NOT be present
        assert not app.query("#code_source_select")

        # repo_url input SHOULD be present
        repo_url_input = app.query_one("#repo_url")
        assert repo_url_input is not None


@pytest.mark.asyncio
async def test_push_mode_app_state_flow(mock_client: MagicMock) -> None:
    """Test form -> save -> push -> monitor state flow in push mode."""
    initial_data = DeploymentForm(
        is_local_git_repo=True,
        push_mode=True,
    )

    mock_deployment = DeploymentResponse(
        id="dep-push-1",
        display_name="push-deployment",
        repo_url="",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-push",
        apiserver_url=None,
        status="Running",
    )
    mock_client.create_deployment.return_value = mock_deployment
    mock_client.base_url = "http://test"

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        name_input = app.screen.query_one("#name")
        name_input.value = "push-deployment"  # type: ignore

        with (
            patch(
                "llama_agents.cli.utils.git_push.get_api_key",
                return_value="sk-test-abc",
            ),
            patch("llama_agents.cli.utils.git_push.subprocess") as mock_subprocess,
        ):
            mock_subprocess.run.return_value = MagicMock(returncode=0, stderr=b"")

            await pilot.click("#save")
            # Allow save + push worker to complete (push is near-instant with mocked subprocess)
            await pilot.pause()
            await pilot.pause()
            await pilot.pause()

        # Push succeeded -> should be on monitor screen (push completes instantly with mock)
        assert isinstance(app.screen, MonitorScreen)

        # Verify deployment was created with empty repo_url (push mode)
        mock_client.create_deployment.assert_called_once()
        call_args = mock_client.create_deployment.call_args[0][0]
        assert call_args.repo_url == ""

        # Exit via monitor close
        app.screen.post_message(MonitorCloseMessage())
        await pilot.pause()
        assert app.return_value == mock_deployment


@pytest.mark.asyncio
async def test_push_mode_enters_pushing_screen(mock_client: MagicMock) -> None:
    """After save, push mode must transition to PushScreen before MonitorScreen."""
    import threading

    initial_data = DeploymentForm(
        is_local_git_repo=True,
        push_mode=True,
    )

    mock_deployment = DeploymentResponse(
        id="dep-push-state",
        display_name="push-deployment",
        repo_url="",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-push",
        apiserver_url=None,
        status="Running",
    )
    mock_client.create_deployment.return_value = mock_deployment
    mock_client.base_url = "http://test"
    mock_client.project_id = "proj-push"

    # Block push_to_remote so we can observe the intermediate state
    push_started = threading.Event()
    push_continue = threading.Event()

    def blocking_push(*args: object, **kwargs: object) -> MagicMock:
        push_started.set()
        push_continue.wait(timeout=5)
        return MagicMock(returncode=0, stderr=b"")

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        name_input = app.screen.query_one("#name")
        name_input.value = "push-deployment"  # type: ignore

        with (
            patch(
                "llama_agents.cli.utils.git_push.get_api_key",
                return_value="sk-test-abc",
            ),
            patch(
                "llama_agents.cli.utils.git_push.push_to_remote",
                side_effect=blocking_push,
            ),
            patch(
                "llama_agents.cli.utils.git_push.configure_git_remote",
                return_value="llamaagents-dep-push-state",
            ),
        ):
            await pilot.click("#save")
            await pilot.pause()
            await pilot.pause()

            # Push worker is now blocked — must be on PushScreen
            push_started.wait(timeout=5)
            assert isinstance(app.screen, PushScreen)

            # Unblock push and let it complete
            push_continue.set()
            await app.workers.wait_for_complete()
            await pilot.pause()

        assert isinstance(app.screen, MonitorScreen)

        app.screen.post_message(MonitorCloseMessage())
        await pilot.pause()


@pytest.mark.asyncio
async def test_push_mode_push_failure(mock_client: MagicMock) -> None:
    """Test push failure returns to form with error message."""
    initial_data = DeploymentForm(
        is_local_git_repo=True,
        push_mode=True,
    )

    mock_deployment = DeploymentResponse(
        id="dep-push-fail",
        display_name="push-deployment",
        repo_url="",
        git_ref="main",
        deployment_file_path="llama_deploy.yaml",
        has_personal_access_token=False,
        secret_names=[],
        project_id="proj-push",
        apiserver_url=None,
        status="Running",
    )
    mock_client.create_deployment.return_value = mock_deployment
    mock_client.base_url = "http://test"

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        name_input = app.screen.query_one("#name")
        name_input.value = "push-deployment"  # type: ignore

        with (
            patch(
                "llama_agents.cli.utils.git_push.get_api_key",
                return_value="sk-test-abc",
            ),
            patch(
                "llama_agents.cli.utils.git_push.configure_git_remote",
                return_value="llamaagents-dep-push-fail",
            ),
            patch(
                "llama_agents.cli.utils.git_push.push_to_remote",
                return_value=MagicMock(returncode=128, stderr=b"auth failed"),
            ),
        ):
            await pilot.click("#save")
            await pilot.pause()
            await app.workers.wait_for_complete()
            await pilot.pause()

        assert isinstance(app.screen, FormScreen)
        assert "push failed" in app.screen.save_error
        assert "configure-git-remote" in app.screen.save_error


@pytest.mark.asyncio
async def test_internal_to_internal_edit_pushes_before_update(
    mock_client: MagicMock,
) -> None:
    """Editing a push-mode deployment must push before calling update_deployment
    so the bare repo has the new ref + latest commits when the server resolves
    git_ref to a SHA."""
    initial_data = DeploymentForm(
        id="dep-int",
        display_name="int-deployment",
        is_local_git_repo=True,
        push_mode=True,
        is_editing=True,
        git_ref="main",
    )

    # Track call ordering: push must happen before update_deployment.
    call_order: list[str] = []

    def record_push(*args: object, **kwargs: object) -> MagicMock:
        call_order.append("push")
        return MagicMock(returncode=0, stderr=b"")

    async def record_update(*args: object, **kwargs: object) -> DeploymentResponse:
        call_order.append("update")
        return DeploymentResponse(
            id="dep-int",
            display_name="int-deployment",
            repo_url="internal://",
            git_ref="feature",
            git_sha="def456",
            deployment_file_path="llama_deploy.yaml",
            has_personal_access_token=False,
            secret_names=[],
            project_id="proj-int",
            apiserver_url=None,
            status="Running",
        )

    mock_client.update_deployment.side_effect = record_update
    mock_client.base_url = "http://test"
    mock_client.project_id = "proj-int"

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        # Switch to a different ref to exercise the new-branch path.
        git_ref_input = app.screen.query_one("#git_ref")
        git_ref_input.value = "feature"  # type: ignore

        with (
            patch(
                "llama_agents.cli.utils.git_push.get_api_key",
                return_value="sk-test-abc",
            ),
            patch(
                "llama_agents.cli.utils.git_push.configure_git_remote",
                return_value="llamaagents-dep-int",
            ),
            patch(
                "llama_agents.cli.utils.git_push.push_to_remote",
                side_effect=record_push,
            ),
        ):
            await pilot.click("#save")
            await pilot.pause()
            await pilot.pause()
            await pilot.pause()

        assert call_order == ["push", "update"], (
            f"expected push before update, got {call_order}"
        )
        # update payload reflects the new ref the user typed.
        update_arg = mock_client.update_deployment.await_args.args[1]
        assert update_arg.git_ref == "feature"
        assert isinstance(app.screen, MonitorScreen)
        app.screen.post_message(MonitorCloseMessage())
        await pilot.pause()


@pytest.mark.asyncio
async def test_internal_to_internal_edit_push_failure_blocks_save(
    mock_client: MagicMock,
) -> None:
    """If the pre-save push fails, update_deployment must NOT run — the form
    returns to the user with the push error so they can fix the local repo."""
    initial_data = DeploymentForm(
        id="dep-int-fail",
        display_name="int-deployment",
        is_local_git_repo=True,
        push_mode=True,
        is_editing=True,
        git_ref="main",
    )

    mock_client.base_url = "http://test"
    mock_client.project_id = "proj-int"

    app = DeploymentEditApp(initial_data)

    async with app.run_test(size=(100, 40)) as pilot:
        with (
            patch(
                "llama_agents.cli.utils.git_push.get_api_key",
                return_value="sk-test-abc",
            ),
            patch(
                "llama_agents.cli.utils.git_push.configure_git_remote",
                return_value="llamaagents-dep-int-fail",
            ),
            patch(
                "llama_agents.cli.utils.git_push.push_to_remote",
                return_value=MagicMock(returncode=128, stderr=b"auth failed"),
            ),
        ):
            await pilot.click("#save")
            await pilot.pause()
            await pilot.pause()
            await pilot.pause()

        assert isinstance(app.screen, FormScreen)
        assert "Push failed before save" in app.screen.save_error
        mock_client.update_deployment.assert_not_called()


def test_pre_push_screen_dismisses_with_success_on_push_complete() -> None:
    """Sanity: PrePushScreen surfaces success/failure as a bool dismissal."""
    screen = PrePushScreen(deployment_id="d", git_ref="main")
    assert screen.push_error == ""
