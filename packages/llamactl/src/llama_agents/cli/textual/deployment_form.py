"""Textual-based deployment forms for CLI interactions"""

import dataclasses
import logging
from dataclasses import dataclass, field
from pathlib import Path
from textwrap import dedent
from typing import cast

from llama_agents.cli.client import get_project_client as get_client
from llama_agents.cli.env import load_env_secrets_from_string
from llama_agents.cli.local_context import pick_preferred_remote
from llama_agents.cli.textual.deployment_help import (
    DeploymentHelpBackMessage,
    DeploymentHelpWidget,
)
from llama_agents.cli.textual.deployment_monitor import (
    DeploymentMonitorWidget,
    MonitorCloseMessage,
)
from llama_agents.cli.textual.git_validation import (
    GitValidationWidget,
    ValidationCancelMessage,
    ValidationResultMessage,
)
from llama_agents.cli.textual.secrets_form import SecretsWidget
from llama_agents.cli.utils.git_push import get_deployment_git_url
from llama_agents.cli.utils.version import get_installed_appserver_version
from llama_agents.core.deployment_config import (
    DEFAULT_DEPLOYMENT_NAME,
    read_deployment_config,
)
from llama_agents.core.git.git_util import (
    get_current_branch,
    get_git_root,
    get_unpushed_commits_count,
    is_git_repo,
    list_remotes,
    working_tree_has_changes,
)
from llama_agents.core.schema.deployments import (
    INTERNAL_CODE_REPO_SCHEME,
    DeploymentCreate,
    DeploymentResponse,
    DeploymentUpdate,
)
from packaging.version import Version
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Container, HorizontalGroup
from textual.content import Content
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.screen import Screen
from textual.timer import Timer
from textual.validation import Length
from textual.widget import Widget
from textual.widgets import Button, Input, Label, Select, Static


@dataclass
class DeploymentForm:
    """Form data for deployment editing/creation"""

    display_name: str = ""
    # unique id, generated from the display_name
    id: str | None = None
    repo_url: str = ""
    git_ref: str = "main"
    git_sha: str | None = None
    deployment_file_path: str = ""
    personal_access_token: str = ""
    # indicates if the deployment has a personal access token (value is unknown)
    has_existing_pat: bool = False
    # secrets that have been added
    secrets: dict[str, str] = field(default_factory=dict)
    # initial secrets, values unknown
    initial_secrets: set[str] = field(default_factory=set)
    # initial secrets that have been removed
    removed_secrets: set[str] = field(default_factory=set)
    # if the deployment is being edited
    is_editing: bool = False
    # warnings shown to the user
    warnings: list[str] = field(default_factory=list)
    # env info
    env_info_messages: str | None = None
    # appserver version fields
    installed_appserver_version: str | None = None
    existing_appserver_version: str | None = None
    selected_appserver_version: str | None = None
    # required secret names from config
    required_secret_names: list[str] = field(default_factory=list)
    # push mode: push code from local git repo instead of providing a URL
    push_mode: bool = False
    # whether the current directory is a git repo (controls push mode availability)
    is_local_git_repo: bool = False
    # whether the server advertises code_push capability (None = unknown/probe failed)
    server_supports_code_push: bool | None = None

    @classmethod
    def from_deployment(
        cls,
        deployment: DeploymentResponse,
        server_supports_code_push: bool | None = None,
    ) -> "DeploymentForm":
        secret_names = deployment.secret_names or []

        installed = get_installed_appserver_version()
        existing = deployment.appserver_version
        # If versions match (or existing is None), treat as non-editable like create
        selected = existing or installed

        is_internal = deployment.repo_url == INTERNAL_CODE_REPO_SCHEME
        has_git = is_git_repo()

        return DeploymentForm(
            display_name=deployment.display_name,
            id=deployment.id,
            repo_url=deployment.repo_url,
            git_ref=deployment.git_ref or "main",
            git_sha=deployment.git_sha or "-",
            deployment_file_path=deployment.deployment_file_path,
            personal_access_token="",  # Always start empty for security
            has_existing_pat=deployment.has_personal_access_token,
            secrets={},
            initial_secrets=set(secret_names),
            is_editing=True,
            installed_appserver_version=installed,
            existing_appserver_version=existing,
            selected_appserver_version=selected,
            push_mode=is_internal,
            is_local_git_repo=has_git,
            server_supports_code_push=server_supports_code_push,
        )

    @staticmethod
    def appserver_version() -> str | None:
        return get_installed_appserver_version()

    def to_update(self) -> DeploymentUpdate:
        """Convert form data to API format"""

        secrets: dict[str, str | None] = cast(
            # dict isn't covariant, so whatever, make it work
            dict[str, str | None],
            self.secrets.copy(),
        )
        for secret in self.removed_secrets:
            secrets[secret] = None

        appserver_version = self.selected_appserver_version

        # In push mode, explicitly send empty string so the server clears any
        # external repo URL (the server-side guard preserves "internal://" when
        # it receives "").  Without this, the empty string collapses to None and
        # the existing external URL is preserved, causing a 409 on push.
        if self.push_mode:
            repo_url: str | None = ""
        else:
            repo_url = self.repo_url or None

        data = DeploymentUpdate(
            repo_url=repo_url,
            git_ref=self.git_ref or "main",
            deployment_file_path=self.deployment_file_path or None,
            personal_access_token=(
                ""
                if self.personal_access_token is None and not self.has_existing_pat
                else self.personal_access_token
            ),
            secrets=secrets,
            appserver_version=appserver_version,
        )

        return data

    def to_create(self) -> DeploymentCreate:
        """Convert form data to API format"""
        appserver_version = self.selected_appserver_version

        return DeploymentCreate(
            display_name=self.display_name,
            repo_url=self.repo_url,
            deployment_file_path=self.deployment_file_path or None,
            git_ref=self.git_ref or "main",
            personal_access_token=self.personal_access_token,
            secrets=self.secrets,
            appserver_version=appserver_version,
        )


class DeploymentFormWidget(Widget):
    """Widget containing all deployment form logic and reactive state"""

    DEFAULT_CSS = """
    DeploymentFormWidget {
        layout: vertical;
        height: auto;
    }
    """

    form_data: reactive[DeploymentForm] = reactive(DeploymentForm(), recompose=True)
    error_message: reactive[str] = reactive("", recompose=True)

    def __init__(self, initial_data: DeploymentForm, save_error: str | None = None):
        super().__init__()
        self.form_data = initial_data
        self.original_form_data = initial_data
        self.error_message = save_error or ""

    def compose(self) -> ComposeResult:
        title = "Edit Deployment" if self.form_data.is_editing else "Create Deployment"

        with HorizontalGroup(
            classes="primary-message",
        ):
            yield Static(
                Content.from_markup(
                    f"{title} [italic][@click=app.show_help()]More info[/][/]"
                ),
                classes="w-1fr",
            )
            yield Static(
                Content.from_markup(
                    dedent("""
                [italic]Tab or click to navigate.[/]
                """).strip()
                ),
                classes="text-right w-1fr",
            )
        yield Static(
            self.error_message,
            id="error-message",
            classes="error-message " + ("visible" if self.error_message else "hidden"),
        )
        # Top-of-form warnings banner
        yield Static(
            "Note: " + " ".join(f"{w}" for w in self.form_data.warnings),
            id="warning-list",
            classes="warning-message mb-1 hidden "
            + ("visible" if self.form_data.warnings else ""),
        )

        # Main deployment fields
        with Widget(classes="two-column-form-grid"):
            yield Label(
                "Deployment Name: *", classes="required form-label", shrink=True
            )
            yield Input(
                value=self.form_data.display_name,
                placeholder="Enter deployment name",
                validators=[Length(minimum=1)],
                id="name",
                disabled=self.form_data.is_editing,
                classes="disabled" if self.form_data.is_editing else "",
                compact=True,
            )

            yield Label("Code Source: *", classes="required form-label", shrink=True)
            # Show push-mode option when:
            # - editing an existing internal-repo deployment (push_mode=True), OR
            # - local git repo AND server supports code_push (None = unknown, fail open)
            show_push_option = (
                self.form_data.push_mode and self.form_data.is_editing
            ) or (
                self.form_data.is_local_git_repo
                and self.form_data.server_supports_code_push is not False
            )
            if show_push_option:
                yield Select(
                    [
                        ("Local repo", True),
                        ("Enter a git URL", False),
                    ],
                    value=self.form_data.push_mode,
                    id="code_source_select",
                    allow_blank=False,
                    compact=True,
                )
                if not self.form_data.push_mode:
                    yield Label(
                        "Repository URL: *",
                        classes="required form-label",
                        shrink=True,
                    )
                    yield Input(
                        value=self.form_data.repo_url,
                        placeholder="https://github.com/user/repo",
                        validators=[Length(minimum=1)],
                        id="repo_url",
                        compact=True,
                    )
            else:
                yield Input(
                    value=self.form_data.repo_url,
                    placeholder="https://github.com/user/repo",
                    validators=[Length(minimum=1)],
                    id="repo_url",
                    compact=True,
                )

            yield Label("Git Reference:", classes="form-label", shrink=True)
            yield Input(
                value=self.form_data.git_ref,
                placeholder="main, develop, v1.0.0, etc.",
                id="git_ref",
                compact=True,
            )

            yield Label("Last Deployed Commit:", classes="form-label", shrink=True)
            yield Input(
                value=(self.form_data.git_sha or "-")[:7],
                placeholder="-",
                id="git_sha",
                compact=True,
                disabled=True,
            )

            yield Static(classes="full-width")
            yield Static(
                Content.from_markup("[italic]Advanced[/]"),
                classes="text-center full-width",
            )
            yield Label("Config File:", classes="form-label", shrink=True)
            yield Input(
                value=self.form_data.deployment_file_path,
                placeholder="Optional path to config dir/file",
                id="deployment_file_path",
                compact=True,
            )

            if not self.form_data.push_mode:
                yield Label("Personal Access Token:", classes="form-label", shrink=True)
                if self.form_data.has_existing_pat:
                    yield Button(
                        "Change / Delete",
                        variant="default",
                        id="change_pat",
                        compact=True,
                    )
                else:
                    yield Input(
                        value=self.form_data.personal_access_token,
                        placeholder="Leave blank to clear"
                        if self.form_data.has_existing_pat
                        else "Optional",
                        password=True,
                        id="personal_access_token",
                        compact=True,
                    )

            # Appserver version display/selector
            yield Label("Appserver Version:", classes="form-label", shrink=True)
            versions_differ = (
                self.form_data.is_editing
                and self.form_data.installed_appserver_version
                and self.form_data.existing_appserver_version
                and self.form_data.installed_appserver_version
                != self.form_data.existing_appserver_version
            )
            if versions_differ:
                # Show dropdown selector for version choice
                installed_version = self.form_data.installed_appserver_version
                existing_version = self.form_data.existing_appserver_version
                current_selection = (
                    self.form_data.selected_appserver_version
                    or existing_version
                    or installed_version
                )
                is_upgrade = (
                    installed_version
                    and existing_version
                    and Version(installed_version) > Version(existing_version)
                )
                label = "Upgrade" if is_upgrade else "Downgrade"
                yield Select(
                    [
                        (f"{label} to {installed_version}", installed_version),
                        (f"Keep {existing_version}", existing_version),
                    ],
                    value=current_selection,
                    id="appserver_version_select",
                    allow_blank=False,
                    compact=True,
                )
            else:
                # Non-editable display of version
                readonly_version = (
                    self.form_data.installed_appserver_version
                    or self.form_data.existing_appserver_version
                    or "unknown"
                )
                yield Static(readonly_version, id="appserver_version_readonly")

        # Secrets section
        yield SecretsWidget(
            initial_secrets=self.form_data.secrets,
            prior_secrets=self.form_data.initial_secrets,
            info_message=self.form_data.env_info_messages,
        )

        with HorizontalGroup(classes="button-row"):
            yield Button("Save", variant="primary", id="save", compact=True)
            yield Button("Cancel", variant="default", id="cancel", compact=True)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save":
            # Ensure latest input values are captured by blurring current focus first
            try:
                if self.screen.focused is not None:
                    self.screen.focused.blur()
            except Exception:
                pass
            self._save()
        elif event.button.id == "change_pat":
            updated_form = dataclasses.replace(self.resolve_form_data())
            updated_form.has_existing_pat = False
            updated_form.personal_access_token = ""
            self.form_data = updated_form
        elif event.button.id == "cancel":
            # Post message to parent app to handle cancel
            self.post_message(CancelFormMessage())

    def on_select_changed(self, event: Select.Changed) -> None:
        """Handle selection changes for code source and version"""
        if event.select.id == "code_source_select":
            updated_form = dataclasses.replace(self.resolve_form_data())
            updated_form.push_mode = bool(event.value)
            if updated_form.push_mode:
                updated_form.repo_url = ""
            elif updated_form.repo_url == INTERNAL_CODE_REPO_SCHEME:
                # Clear the internal sentinel when switching to URL mode
                updated_form.repo_url = ""
            self.form_data = updated_form
        elif event.select.id == "appserver_version_select" and event.value:
            updated_form = dataclasses.replace(self.resolve_form_data())
            updated_form.selected_appserver_version = str(event.value)
            self.form_data = updated_form

    def _save(self) -> None:
        self.form_data = self.resolve_form_data()
        if self._validate_form():
            # Post message to parent app to start validation
            self.post_message(StartValidationMessage(self.form_data))

    def _validate_form(self) -> bool:
        """Validate required fields from the current UI state"""
        name_input = self.query_one("#name", Input)

        errors: list[str] = []

        # Clear previous error state
        name_input.remove_class("error")

        if not name_input.value.strip():
            name_input.add_class("error")
            errors.append("Deployment name is required")

        if not self.form_data.push_mode:
            try:
                repo_url_input = self.query_one("#repo_url", Input)
                repo_url_input.remove_class("error")
                if not repo_url_input.value.strip():
                    repo_url_input.add_class("error")
                    errors.append("Repository URL is required")
            except NoMatches:
                errors.append("Repository URL is required")

        missing_required: list[str] = []
        for secret_name in sorted(self.form_data.required_secret_names):
            value = (self.form_data.secrets.get(secret_name) or "").strip()
            if value == "":
                missing_required.append(secret_name)
        if missing_required:
            errors.append("Missing required secrets: " + ", ".join(missing_required))

        if errors:
            self._show_error("; ".join(errors))
            return False
        self._show_error("")
        return True

    def _show_error(self, message: str) -> None:
        """Show an error message"""
        self.error_message = message

    def resolve_form_data(self) -> DeploymentForm:
        """Extract form data from inputs"""
        name_input = self.query_one("#name", Input)
        git_ref_input = self.query_one("#git_ref", Input)
        deployment_file_input = self.query_one("#deployment_file_path", Input)

        try:
            repo_url_input = self.query_one("#repo_url", Input)
            repo_url_value = repo_url_input.value.strip()
        except NoMatches:
            repo_url_value = "" if self.form_data.push_mode else self.form_data.repo_url

        try:
            pat_input = self.query_one("#personal_access_token", Input)
            pat_value = pat_input.value.strip()
        except NoMatches:
            pat_value = self.form_data.personal_access_token or ""

        # Get updated secrets from the secrets widget
        secrets_widget = self.query_one(SecretsWidget)
        updated_secrets = secrets_widget.get_updated_secrets()
        updated_prior_secrets = secrets_widget.get_updated_prior_secrets()

        return DeploymentForm(
            display_name=name_input.value.strip(),
            id=self.form_data.id,
            repo_url=repo_url_value,
            git_ref=git_ref_input.value.strip() or "main",
            git_sha=self.form_data.git_sha,
            deployment_file_path=deployment_file_input.value.strip(),
            personal_access_token=pat_value,
            secrets=updated_secrets,
            initial_secrets=self.original_form_data.initial_secrets,
            is_editing=self.original_form_data.is_editing,
            has_existing_pat=self.form_data.has_existing_pat,
            removed_secrets=self.original_form_data.initial_secrets.difference(
                updated_prior_secrets
            ),
            installed_appserver_version=self.form_data.installed_appserver_version,
            existing_appserver_version=self.form_data.existing_appserver_version,
            selected_appserver_version=self.form_data.selected_appserver_version,
            required_secret_names=self.form_data.required_secret_names,
            push_mode=self.form_data.push_mode,
            is_local_git_repo=self.form_data.is_local_git_repo,
        )


# Messages for communication between form widget and screens
class CancelFormMessage(Message):
    pass


class StartValidationMessage(Message):
    def __init__(self, form_data: DeploymentForm):
        super().__init__()
        self.form_data = form_data


class PushCompleteMessage(Message):
    def __init__(self, success: bool, error: str = ""):
        super().__init__()
        self.success = success
        self.error = error


class PushingWidget(Widget):
    """Widget shown while pushing code to the deployment."""

    DEFAULT_CSS = """
    PushingWidget {
        layout: vertical;
        height: auto;
    }
    """

    def __init__(
        self, deployment_id: str, git_url: str, project_id: str, git_ref: str = "main"
    ) -> None:
        super().__init__()
        self.deployment_id = deployment_id
        self.git_url = git_url
        self.project_id = project_id
        self.git_ref = git_ref

    def compose(self) -> ComposeResult:
        from llama_agents.cli.textual.llama_loader import PixelLlamaLoader

        yield Static("Pushing code...", classes="primary-message")
        yield PixelLlamaLoader(classes="mb-1")

    def on_mount(self) -> None:
        self.run_worker(self._push, thread=True)

    def _push(self) -> None:
        from llama_agents.cli.utils.git_push import (
            configure_git_remote,
            get_api_key,
            internal_push_refspec,
            push_to_remote,
        )

        try:
            api_key = get_api_key()
            remote_name = configure_git_remote(
                self.git_url, api_key, self.project_id, self.deployment_id
            )
            local_ref, target_ref = internal_push_refspec(self.git_ref)
            result = push_to_remote(
                remote_name,
                local_ref=local_ref,
                target_ref=target_ref,
            )
            if result.returncode != 0:
                stderr = (
                    result.stderr.decode("utf-8", errors="replace")
                    if result.stderr
                    else ""
                )
                self.post_message(
                    PushCompleteMessage(
                        success=False,
                        error=f"Git push failed (exit {result.returncode}). {stderr}".strip(),
                    )
                )
                return
            self.post_message(PushCompleteMessage(success=True))
        except Exception as e:
            self.post_message(PushCompleteMessage(success=False, error=str(e)))


class HelpScreen(Screen[DeploymentForm]):
    """Screen showing deployment help text."""

    def __init__(self, form_data: DeploymentForm) -> None:
        super().__init__()
        self.form_data = form_data

    def compose(self) -> ComposeResult:
        with Container(classes="form-container"):
            yield DeploymentHelpWidget()

    def on_deployment_help_back_message(
        self, message: DeploymentHelpBackMessage
    ) -> None:
        self.dismiss(self.form_data)


class ValidationScreen(Screen[DeploymentForm | None]):
    """Screen wrapping GitValidationWidget for repo validation."""

    def __init__(self, form_data: DeploymentForm) -> None:
        super().__init__()
        self.form_data = form_data

    def compose(self) -> ComposeResult:
        with Container(classes="form-container"):
            yield GitValidationWidget(
                repo_url=self.form_data.repo_url,
                deployment_id=self.form_data.id if self.form_data.is_editing else None,
                pat=self.form_data.personal_access_token
                if self.form_data.personal_access_token
                else None,
            )

    def on_validation_result_message(self, message: ValidationResultMessage) -> None:
        if message.pat is not None:
            updated_form = dataclasses.replace(self.form_data)
            updated_form.personal_access_token = message.pat
            if message.pat == "":
                updated_form.has_existing_pat = False
            self.dismiss(updated_form)
        else:
            self.dismiss(self.form_data)

    def on_validation_cancel_message(self, message: ValidationCancelMessage) -> None:
        self.dismiss(None)


class PushScreen(Screen[DeploymentResponse | None]):
    """Screen shown while pushing code to the deployment after a successful save.

    Used for first-push bootstrap (external→internal switch or fresh create with
    push_mode): the CRD already has the placeholder repo_url, and this push
    populates the bare repo so `_on_push_complete` can stamp gitSha/repoUrl.
    """

    def __init__(self, deployment: DeploymentResponse) -> None:
        super().__init__()
        self.deployment = deployment
        self.push_error: str = ""

    def compose(self) -> ComposeResult:
        deployment_id = self.deployment.id or ""
        client = get_client()
        git_url = get_deployment_git_url(client.base_url, deployment_id)
        with Container(classes="form-container"):
            git_ref = self.deployment.git_ref or "main"
            yield PushingWidget(
                deployment_id, git_url, client.project_id, git_ref=git_ref
            )

    def on_push_complete_message(self, message: PushCompleteMessage) -> None:
        if message.success:
            self.dismiss(self.deployment)
        else:
            from textual.markup import escape

            deployment_id = self.deployment.id or "unknown"
            escaped_error = escape(message.error) if message.error else ""
            self.push_error = (
                f"Deployment created but push failed: {escaped_error}. "
                f"Configure the remote manually: "
                f"llamactl deploy configure-git-remote {deployment_id}"
            )
            self.dismiss(None)


class PrePushScreen(Screen[bool]):
    """Screen shown while pushing code BEFORE saving an existing internal-repo
    deployment.

    Used for internal→internal edits: the bare repo must contain the new ref
    and latest commits before `update_deployment` resolves git_ref to a SHA,
    otherwise the server either 400s on a new branch or silently records a
    stale SHA on the current branch.

    Dismisses with True on success, False on push failure (push_error set).
    """

    def __init__(self, deployment_id: str, git_ref: str) -> None:
        super().__init__()
        self.deployment_id = deployment_id
        self.git_ref = git_ref
        self.push_error: str = ""

    def compose(self) -> ComposeResult:
        client = get_client()
        git_url = get_deployment_git_url(client.base_url, self.deployment_id)
        with Container(classes="form-container"):
            yield PushingWidget(
                self.deployment_id,
                git_url,
                client.project_id,
                git_ref=self.git_ref,
            )

    def on_push_complete_message(self, message: PushCompleteMessage) -> None:
        if message.success:
            self.dismiss(True)
        else:
            from textual.markup import escape

            escaped_error = escape(message.error) if message.error else ""
            self.push_error = (
                f"Push failed before save: {escaped_error}. "
                "Fix the local repo and try again."
            )
            self.dismiss(False)


class MonitorScreen(Screen[DeploymentResponse | None]):
    """Screen wrapping the deployment monitor."""

    def __init__(self, deployment: DeploymentResponse) -> None:
        super().__init__()
        self.deployment = deployment

    def compose(self) -> ComposeResult:
        with Container():
            yield DeploymentMonitorWidget(self.deployment.id)

    def on_monitor_close_message(self, _: MonitorCloseMessage) -> None:
        self.dismiss(self.deployment)


class FormScreen(Screen[DeploymentResponse | None]):
    """Screen containing the deployment form and orchestrating save/validation flow."""

    _FOCUS_RETRY_DELAYS = (0.01, 0.05, 0.15, 0.3)

    def __init__(self, initial_data: DeploymentForm, save_error: str = "") -> None:
        super().__init__()
        self.form_data = initial_data
        self.save_error = save_error
        self._initial_focus_applied = False
        self._focus_bootstrap_timers: list[Timer] = []
        self._push_screen: PushScreen | None = None
        self._pre_push_screen: PrePushScreen | None = None
        # Snapshot of the deployment's push-mode state at the time the form was
        # opened. Used to distinguish internal→internal edits (where we must
        # push before save) from external→internal switches (where we save
        # first to write the placeholder repo_url, then push to bootstrap).
        self._original_push_mode = initial_data.is_editing and initial_data.push_mode

    def compose(self) -> ComposeResult:
        with Container(classes="form-container"):
            yield DeploymentFormWidget(self.form_data, self.save_error)

    def on_mount(self) -> None:
        self._restart_focus_bootstrap()

    def on_screen_resume(self, _: events.ScreenResume) -> None:
        self._restart_focus_bootstrap()

    def ensure_initial_focus(self) -> None:
        self._restart_focus_bootstrap()

    def _stop_focus_bootstrap(self) -> None:
        for timer in self._focus_bootstrap_timers:
            timer.stop()
        self._focus_bootstrap_timers.clear()

    def _restart_focus_bootstrap(self) -> None:
        self._stop_focus_bootstrap()
        for delay in self._FOCUS_RETRY_DELAYS:
            self._focus_bootstrap_timers.append(
                self.set_timer(
                    delay,
                    self._ensure_initial_focus,
                )
            )

    def _ensure_initial_focus(self) -> None:
        if not self._initial_focus_applied:
            self._focus_initial_field()

    def _focus_initial_field(self) -> None:
        focus_targets = [
            "#name",
            "#repo_url",
            "#git_ref",
            "#deployment_file_path",
            "#personal_access_token",
            "#code_source_select",
        ]
        for selector in focus_targets:
            try:
                widget = self.query_one(selector)
            except NoMatches:
                continue
            if widget.focusable and not widget.disabled:
                widget.focus()
                self._initial_focus_applied = True
                break

    def action_show_help(self) -> None:
        widget = self.query("DeploymentFormWidget")
        if widget:
            first_widget = widget[0]
            if isinstance(first_widget, DeploymentFormWidget):
                self.form_data = first_widget.resolve_form_data()
        self.app.push_screen(HelpScreen(self.form_data), self._on_help_back)

    def _on_help_back(self, form_data: DeploymentForm | None) -> None:
        if form_data is not None:
            self.form_data = form_data

    async def on_start_validation_message(
        self, message: StartValidationMessage
    ) -> None:
        self.form_data = message.form_data
        self.save_error = ""
        if self.form_data.push_mode:
            from llama_agents.cli.utils.git_push import get_api_key

            try:
                get_api_key()
            except RuntimeError as e:
                self._return_to_form(save_error=str(e))
                return
            await self._perform_save()
        else:
            self.app.push_screen(
                ValidationScreen(self.form_data), self._on_validation_result
            )

    async def _on_validation_result(self, result: DeploymentForm | None) -> None:
        if result is None:
            # Validation cancelled — return to form with cleared error
            self._return_to_form(save_error="")
            return
        self.form_data = result
        await self._perform_save()

    async def _perform_save(self) -> None:
        logging.info("saving form data %s", self.form_data)
        result = self.form_data
        # internal→internal edit with a local git repo: push first so the bare
        # repo has the new ref and latest commits before the server resolves
        # git_ref to a SHA. Without this, switching to an unpushed branch 400s
        # and re-saving the current branch silently records a stale SHA.
        if (
            self._original_push_mode
            and result.push_mode
            and result.is_editing
            and result.is_local_git_repo
            and result.id
        ):
            self._pre_push_screen = PrePushScreen(
                deployment_id=result.id,
                git_ref=result.git_ref or "main",
            )
            self.app.push_screen(self._pre_push_screen, self._on_pre_push_result)
            return

        await self._do_save()

    async def _on_pre_push_result(self, success: bool | None) -> None:
        if success:
            await self._do_save()
            return
        push_error = self._pre_push_screen.push_error if self._pre_push_screen else ""
        self._return_to_form(save_error=push_error or "Push failed")

    async def _do_save(self) -> None:
        result = self.form_data
        client = get_client()
        try:
            if result.is_editing:
                if not result.id:
                    raise ValueError("Deployment ID is required for update")
                deployment = await client.update_deployment(
                    result.id, result.to_update()
                )
            else:
                deployment = await client.create_deployment(result.to_create())
            if not result.is_editing and deployment.id:
                updated_form = dataclasses.replace(self.form_data)
                updated_form.id = deployment.id
                updated_form.is_editing = True
                self.form_data = updated_form
            logging.info(
                "save complete: push_mode=%s is_editing=%s original_push_mode=%s",
                result.push_mode,
                result.is_editing,
                self._original_push_mode,
            )
            # Post-save bootstrap push only fires for the transition into push
            # mode (external→internal or fresh create with push_mode). For
            # internal→internal, the push already happened in _perform_save
            # before we got here.
            needs_post_save_push = (
                result.push_mode
                and result.is_local_git_repo
                and not self._original_push_mode
            )
            if needs_post_save_push:
                self._push_screen = PushScreen(deployment)
                self.app.push_screen(self._push_screen, self._on_push_result)
            else:
                self.app.push_screen(MonitorScreen(deployment), self._on_monitor_close)
        except Exception as e:
            self._return_to_form(save_error=f"Error saving deployment: {e}")

    async def _on_push_result(self, result: DeploymentResponse | None) -> None:
        if result is not None:
            # Push succeeded — go to monitor
            self.app.push_screen(MonitorScreen(result), self._on_monitor_close)
        else:
            # Push failed — get error from the stored PushScreen reference
            push_error = self._push_screen.push_error if self._push_screen else ""
            self._return_to_form(save_error=push_error or "Push failed")

    def _on_monitor_close(self, deployment: DeploymentResponse | None) -> None:
        self.dismiss(deployment)

    def _return_to_form(self, save_error: str = "") -> None:
        """Re-mount the form widget with updated state."""
        self.save_error = save_error
        self._initial_focus_applied = False
        self._stop_focus_bootstrap()
        # Remove old form widget and mount a fresh one
        try:
            container = self.query_one(Container)
            old_widget = container.query_one(DeploymentFormWidget)
            old_widget.remove()
            container.mount(DeploymentFormWidget(self.form_data, self.save_error))
            self.call_after_refresh(self._restart_focus_bootstrap)
        except NoMatches:
            pass

    def on_cancel_form_message(self, message: CancelFormMessage) -> None:
        self.dismiss(None)


class DeploymentEditApp(App[DeploymentResponse | None]):
    """Textual app for editing/creating deployments.

    A thin shell that pushes FormScreen and handles exit.
    """

    CSS_PATH = Path(__file__).parent / "styles.tcss"

    def __init__(self, initial_data: DeploymentForm):
        super().__init__()
        self.initial_data = initial_data

    def on_mount(self) -> None:
        self.theme = "tokyo-night"
        self.push_screen(
            FormScreen(self.initial_data),
            self._on_form_done,
        )

    def on_app_focus(self, _: events.AppFocus) -> None:
        screen = self.screen
        if isinstance(screen, FormScreen):
            screen.ensure_initial_focus()

    def _on_form_done(self, result: DeploymentResponse | None) -> None:
        self.exit(result)

    def on_key(self, event: events.Key) -> None:
        if event.key == "ctrl+c":
            self.exit(None)


def edit_deployment_form(
    deployment: DeploymentResponse,
    server_supports_code_push: bool | None = None,
) -> DeploymentResponse | None:
    """Launch deployment edit form and return result"""
    initial_data = DeploymentForm.from_deployment(
        deployment, server_supports_code_push=server_supports_code_push
    )
    app = DeploymentEditApp(initial_data)
    return app.run()


def create_deployment_form(
    server_supports_code_push: bool | None = None,
) -> DeploymentResponse | None:
    """Launch deployment creation form and return result"""
    initial_data = _initialize_deployment_data(
        server_supports_code_push=server_supports_code_push,
    )
    app = DeploymentEditApp(initial_data)
    return app.run()


def _initialize_deployment_data(
    server_supports_code_push: bool | None = None,
) -> DeploymentForm:
    """
    initialize the deployment form data from the current git repo and .env file
    """

    repo_url: str | None = None
    git_ref: str | None = None
    secrets: dict[str, str] = {}
    name: str | None = None
    config_file_path: str | None = None
    warnings: list[str] = []
    has_git = is_git_repo()
    has_no_workflows = False
    required_secret_names: list[str] = []
    try:
        config = read_deployment_config(Path("."), Path("."))
        if config.name != DEFAULT_DEPLOYMENT_NAME:
            name = config.name
        has_no_workflows = config.has_no_workflows()
        # Seed required secret names from config if present
        required_secret_names = config.required_env_vars

    except Exception:
        warnings.append("Could not parse local deployment config. It may be invalid.")
    if not has_git and has_no_workflows:
        warnings = [
            "Run from within a git repository to automatically generate a deployment config."
        ]
    elif has_no_workflows:
        warnings = [
            "The current project has no workflows configured. It may be invalid."
        ]
    elif not has_git:
        warnings.append(
            "Current directory is not a git repository. If you are trying to deploy this directory, you will need to create a git repository and push it before creating a deployment."
        )
    else:
        repo_url = pick_preferred_remote(list_remotes())
        git_ref = get_current_branch()
        root = get_git_root()
        if root != Path.cwd():
            config_file_path = str(Path.cwd().relative_to(root))

        if repo_url is None:
            warnings.append(
                "No git remote was found. You will need to push your changes to a remote repository before creating a deployment from this repository."
            )
        else:
            # Working tree changes
            if working_tree_has_changes():
                warnings.append(
                    "Working tree has uncommitted or untracked changes. You may want to push them before creating a deployment from this branch."
                )
            else:
                # Unpushed commits (ahead of upstream)
                ahead = get_unpushed_commits_count()
                if ahead is None:
                    warnings.append(
                        "Current branch has no upstream configured. You will need to push them or choose a different branch."
                    )
                elif ahead > 0:
                    warnings.append(
                        f"There are {ahead} local commits not pushed to upstream. They won't be included in the deployment unless you push them first."
                    )
    env_info_message = None
    if Path(".env").exists():
        secrets = load_env_secrets_from_string(Path(".env").read_text())
        if len(secrets) > 0:
            env_info_message = "Secrets were automatically seeded from your .env file. Remove or change any that should not be set. They must be manually configured after creation."

    installed = get_installed_appserver_version()

    form = DeploymentForm(
        display_name=name or "",
        repo_url="" if has_git else (repo_url or ""),
        git_ref=git_ref or "main",
        secrets=secrets,
        deployment_file_path=config_file_path or "",
        warnings=warnings,
        env_info_messages=env_info_message,
        installed_appserver_version=installed,
        selected_appserver_version=installed,
        required_secret_names=required_secret_names,
        push_mode=has_git and server_supports_code_push is not False,
        is_local_git_repo=has_git,
        server_supports_code_push=server_supports_code_push,
    )
    return form
