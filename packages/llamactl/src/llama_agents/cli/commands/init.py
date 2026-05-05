from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import click
from click.exceptions import Exit
from llama_agents.cli.app import app
from llama_agents.cli.interactive import select_or_exit
from llama_agents.cli.options import (
    global_options,
    interactive_option,
)
from llama_agents.cli.param_types import TemplateType
from llama_agents.cli.templates import (
    ALL_TEMPLATES,
    HEADLESS_TEMPLATES,
    UI_TEMPLATES,
    TemplateOption,
)
from rich import print as rprint
from rich.text import Text

_ClickPath = getattr(click, "Path")


@app.command()
@click.option(
    "--update",
    is_flag=True,
    help="Instead of creating a new app, update the current app to the latest version. Other options will be ignored.",
)
@click.option(
    "--template",
    type=TemplateType(),
    help="The template to use for the new app",
)
@click.option(
    "--dir",
    help="The directory to create the new app in",
    type=_ClickPath(
        file_okay=False,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
@click.option(
    "--force",
    is_flag=True,
    help="Force overwrite the directory if it exists",
)
@global_options
@interactive_option
def init(
    update: bool,
    template: str | None,
    dir: Path | None,
    force: bool,
    interactive: bool,
) -> None:
    """Create a new app repository from a template"""
    if update:
        _update()
    else:
        _create(template, dir, force, interactive)


def _create(
    template: str | None, dir: Path | None, force: bool, interactive: bool
) -> None:
    # Initialize git repository if git is available
    has_git = False
    git_initialized = False
    try:
        subprocess.run(["git", "--version"], check=True, capture_output=True)
        has_git = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        # git is not available or broken; continue without git
        has_git = False

    if not has_git:
        rprint(
            "git is required to initialize a template. Make sure you have it installed and available in your PATH."
        )
        raise Exit(1)

    if template is None and interactive:
        rprint(
            "[bold]Select a template to start from.[/bold] Either with javascript frontend UI, or just a python workflow that can be used as an API."
        )
        template = (
            select_or_exit(
                [
                    ("", ""),
                    *[(o.id, f"{o.name} - {o.description}") for o in UI_TEMPLATES],
                    ("", ""),
                    *[
                        (o.id, f"{o.name} - {o.description}")
                        for o in HEADLESS_TEMPLATES
                    ],
                ],
                "",
                hint_flag="--template",
                hint_command="llamactl init --help",
                interactive=interactive,
            )
            or None
        )  # empty-string separators are not valid selections
    if template is None:
        options = [o.id for o in ALL_TEMPLATES]
        rprint(
            Text(
                f"No template selected. Select a template or pass a template name with --template <{'|'.join(options)}>"
            )
        )
        raise Exit(1)
    if dir is None:
        if interactive:
            dir_str = click.prompt(
                "Enter the directory to create the new app in",
                default=template,
            )
            if dir_str:
                dir = Path(dir_str)
            else:
                return
        else:
            rprint(f"[yellow]No directory provided. Defaulting to {template}[/]")
            dir = Path(template)

    resolved_template: TemplateOption | None = next(
        (o for o in ALL_TEMPLATES if o.id == template), None
    )
    if resolved_template is None:
        rprint(f"Template {template} not found")
        raise Exit(1)
    if dir.exists():
        is_ok = force or (
            interactive and click.confirm("Directory exists. Overwrite?", default=False)
        )

        if not is_ok:
            rprint(
                f"[yellow]Try again with another directory or pass --force to overwrite the existing directory '{str(dir)}'[/]"
            )
            raise Exit(1)
        else:
            shutil.rmtree(dir, ignore_errors=True)

    # Import copier lazily at call time to keep CLI startup light while still
    # allowing tests to patch ``copier.run_copy`` directly.
    import copier

    copier.run_copy(
        resolved_template.source.url,
        dir,
        quiet=True,
        defaults=not interactive,
    )

    # Change to the new directory and initialize git repo
    original_cwd = Path.cwd()
    os.chdir(dir)

    try:
        # Copy agent instructions and MCP configs from scaffold
        _copy_scaffold()
        # Create symlinks so all agents find the instructions
        for alternate in ["CLAUDE.md", "GEMINI.md"]:
            alt_path = Path(alternate)
            agents_path = Path("AGENTS.md")
            if agents_path.exists() and not alt_path.exists():
                alt_path.symlink_to("AGENTS.md")

        # Initialize a git repo unless we're already inside one.
        if has_git:
            # Detect whether the target directory is already inside a git work tree
            inside_existing_repo = False
            try:
                result = subprocess.run(
                    ["git", "rev-parse", "--is-inside-work-tree"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                inside_existing_repo = result.stdout.strip().lower() == "true"
            except (subprocess.CalledProcessError, FileNotFoundError):
                inside_existing_repo = False

            if inside_existing_repo:
                # Do not create a nested repo; user likely wants this within the parent repo
                rprint(
                    "[yellow]Detected an existing Git repository in a parent directory; skipping git initialization for this app.[/]"
                )
                # Treat as initialized for purposes of what instructions to show later
                git_initialized = True
            else:
                try:
                    subprocess.run(["git", "init"], check=True, capture_output=True)
                    subprocess.run(["git", "add", "."], check=True, capture_output=True)
                    subprocess.run(
                        ["git", "commit", "-m", "Initial commit"],
                        check=True,
                        capture_output=True,
                    )
                    git_initialized = True
                except (subprocess.CalledProcessError, FileNotFoundError) as e:
                    # Extract a short error message if present
                    err_msg = ""
                    if isinstance(e, subprocess.CalledProcessError):
                        stderr_bytes = e.stderr or b""
                        if isinstance(stderr_bytes, (bytes, bytearray)):
                            try:
                                stderr_text = stderr_bytes.decode("utf-8", "ignore")
                            except Exception:
                                stderr_text = ""
                        else:
                            stderr_text = str(stderr_bytes)
                        if stderr_text.strip():
                            err_msg = stderr_text.strip().split("\n")[-1]
                    elif isinstance(e, FileNotFoundError):
                        err_msg = "git executable not found"

                    rprint("")
                    rprint("⚠️  [bold]Skipping git initialization due to an error.[/]")
                    if err_msg:
                        rprint(f"    {err_msg}")
                    rprint("    You can initialize it manually:")
                    rprint(
                        "      git init && git add . && git commit -m 'Initial commit'"
                    )
                    rprint("")
    finally:
        os.chdir(original_cwd)

    # If git is not available at all, let the user know how to proceed
    if not has_git:
        rprint("")
        rprint("⚠️  [bold]Skipping git initialization due to an error.[/]")
        rprint("    git executable not found")
        rprint("    You can initialize it manually:")
        rprint("      git init && git add . && git commit -m 'Initial commit'")
        rprint("")

    rprint(
        f"Successfully created [blue]{dir}[/] using the [blue]{resolved_template.name}[/] template! 🎉 🦙 💾"
    )
    rprint("")
    rprint("[bold]To run locally:[/]")
    rprint(f"    [orange3]cd[/] {dir}")
    rprint("    [orange3]uvx[/] llamactl serve")
    rprint("")
    rprint("[bold]To deploy:[/]")
    # Only show manual git init steps if repository failed to initialize earlier
    if not git_initialized:
        rprint("    [orange3]git[/] init")
        rprint("    [orange3]git[/] add .")
        rprint("    [orange3]git[/] commit -m 'Initial commit'")
        rprint("")
    rprint("[dim](Create a new repo and add it as a remote)[/]")
    rprint("")
    rprint("    [orange3]git[/] remote add origin <your-repo-url>")
    rprint("    [orange3]git[/] push -u origin main")
    rprint("")
    # rprint("  [orange3]uvx[/] llamactl login")
    rprint("    [orange3]uvx[/] llamactl deploy create")
    rprint("")


def _update() -> None:
    """Update the app to the latest version"""
    try:
        # Import copier lazily so the init command remains lightweight when
        # unused, while tests can patch ``copier.run_update`` directly.
        import copier

        copier.run_update(
            overwrite=True,
            skip_answered=True,
            quiet=True,
        )
    except Exception as e:  # scoped to copier errors; type opaque here
        rprint(f"{e}")
        raise Exit(1)

    # Check git status and warn about conflicts
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        )

        if result.stdout.strip():
            conflicted_files = []
            modified_files = []

            for line in result.stdout.strip().split("\n"):
                status = line[:2]
                filename = line[3:]

                if "UU" in status or "AA" in status or "DD" in status:
                    conflicted_files.append(filename)
                elif status.strip():
                    modified_files.append(filename)

            if conflicted_files:
                rprint("")
                rprint("⚠️  [bold]Files with conflicts detected:[/]")
                for file in conflicted_files:
                    rprint(f"    {file}")
                rprint("")
                rprint(
                    "Please manually resolve conflicts with a merge editor before proceeding."
                )

    except (subprocess.CalledProcessError, FileNotFoundError):
        # Git not available or not in a git repo - continue silently
        pass


_SCAFFOLD_DIR = Path(__file__).resolve().parent.parent / "scaffold"


def _copy_scaffold() -> None:
    """Copy scaffold files (AGENTS.md, MCP configs) into the current directory."""
    for item in _SCAFFOLD_DIR.iterdir():
        dest = Path(item.name)
        if item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dest)
