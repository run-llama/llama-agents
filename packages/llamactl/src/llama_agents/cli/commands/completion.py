# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import os
import re
from pathlib import Path

import click
from click.shell_completion import get_completion_class
from llama_agents.cli.app import app
from llama_agents.cli.options import global_options
from llama_agents.cli.output import status
from llama_agents.cli.paths import (
    bash_completion_dir,
    bash_rc_path,
    fish_completion_dir,
    zsh_completion_dir,
    zsh_rc_path,
)


@app.group(help="Shell completion helpers.", no_args_is_help=True)
@global_options
def completion() -> None:
    pass


def _completion_source(shell: str) -> str:
    """Build and return the shell completion script for the given shell."""
    ctx = click.get_current_context()
    root_cmd = ctx.find_root().command
    cls = get_completion_class(shell)
    if cls is None:
        raise click.ClickException(f"Unsupported shell: {shell}")
    comp = cls(root_cmd, {}, "llamactl", "_LLAMACTL_COMPLETE")
    return comp.source()


@completion.command("generate")
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish"]))
@global_options
def generate(shell: str) -> None:
    """Print shell completion script to stdout.

    Example: llamactl completion generate zsh > ~/.zfunc/_llamactl
    """
    click.echo(_completion_source(shell))


@completion.command("install")
@click.option(
    "--shell",
    type=click.Choice(["bash", "zsh", "fish"]),
    default=None,
    help="Override auto-detected shell.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without modifying anything.",
)
@global_options
def install(shell: str | None, dry_run: bool) -> None:
    """Auto-detect your shell and install completions.

    Example: llamactl completion install
    """
    if shell is None:
        shell = _detect_shell()

    source = _completion_source(shell)

    if shell == "bash":
        _install_bash(source, dry_run)
    elif shell == "zsh":
        _install_zsh(source, dry_run)
    elif shell == "fish":
        _install_fish(source, dry_run)

    if not dry_run:
        status(f"detected shell {shell}")
        status("restart your shell or source the config to activate completions")


# ---------------------------------------------------------------------------
# Shell-specific install helpers
# ---------------------------------------------------------------------------

_MARKER = "# llamactl shell completion"
_ZSH_BLOCK_START = "# >>> llamactl completion >>>"
_ZSH_BLOCK_END = "# <<< llamactl completion <<<"
_ZSH_FPATH_LINE = "fpath=(~/.zfunc $fpath)"
_ZSH_COMPINIT_LINE = "autoload -Uz compinit && compinit"


def _detect_shell() -> str:
    shell_env = os.environ.get("SHELL", "")
    name = os.path.basename(shell_env)
    if name in ("bash", "zsh", "fish"):
        return name
    return "bash"


def _install_bash(source: str, dry_run: bool) -> None:
    comp_dir = bash_completion_dir()
    target = comp_dir / "llamactl"

    if dry_run:
        status(f"would write completion script to {target}")
        return

    comp_dir.mkdir(parents=True, exist_ok=True)
    target.write_text(source)
    status(f"wrote completions to {target}")

    # Ensure .bashrc sources the completion dir
    bashrc = bash_rc_path()
    _ensure_source_line(
        bashrc,
        f"source {target}",
        dry_run,
    )


def _install_zsh(source: str, dry_run: bool) -> None:
    zfunc = zsh_completion_dir()
    target = zfunc / "_llamactl"

    if dry_run:
        status(f"would write completion script to {target}")
        status("would ensure ~/.zfunc is in fpath in ~/.zshrc")
        return

    zfunc.mkdir(parents=True, exist_ok=True)
    target.write_text(source)
    status(f"wrote completions to {target}")

    zshrc = zsh_rc_path()
    _ensure_zsh_fpath(zshrc, dry_run)


def _install_fish(source: str, dry_run: bool) -> None:
    comp_dir = fish_completion_dir()
    target = comp_dir / "llamactl.fish"

    if dry_run:
        status(f"would write completion script to {target}")
        return

    comp_dir.mkdir(parents=True, exist_ok=True)
    target.write_text(source)
    status(f"wrote completions to {target}")


def _ensure_zsh_fpath(zshrc: Path, dry_run: bool) -> None:
    """Ensure llamactl's zsh block appears before the first live compinit."""
    if zshrc.exists():
        content = zshrc.read_text()
    else:
        content = ""

    updated_content, actions = _plan_zshrc_update(content)
    if not actions:
        return

    if dry_run:
        for action in actions:
            status(f"would update {zshrc}: {action}")
        return

    zshrc.write_text(updated_content)
    for action in actions:
        status(f"updated {zshrc}: {action}")


def _plan_zshrc_update(content: str) -> tuple[str, list[str]]:
    lines = content.splitlines()
    had_trailing_newline = content.endswith("\n")
    sanitized_lines, removed_block, removed_compinit = _strip_managed_zsh_lines(lines)

    first_compinit_index = _first_live_compinit_index(sanitized_lines)
    first_fpath_index = _first_live_zfunc_fpath_index(sanitized_lines)
    needs_fpath_block = (
        first_fpath_index is None
        or first_compinit_index is not None
        and first_fpath_index > first_compinit_index
    )

    updated_lines = list(sanitized_lines)
    actions: list[str] = []
    if needs_fpath_block:
        insert_at = (
            first_compinit_index
            if first_compinit_index is not None
            else len(updated_lines)
        )
        updated_lines[insert_at:insert_at] = _managed_zsh_fpath_block()
        actions.append(
            "ensure ~/.zfunc is added before compinit via the llamactl block"
        )
    elif removed_block:
        actions.append("remove stale llamactl zsh block")

    has_compinit = _first_live_compinit_index(updated_lines) is not None
    if not has_compinit:
        if updated_lines and updated_lines[-1] != "":
            updated_lines.append("")
        updated_lines.append(f"{_ZSH_COMPINIT_LINE}  {_MARKER}")
        actions.append("add compinit because none was configured")
    elif removed_compinit:
        actions.append("remove stale llamactl-managed compinit line")

    updated_content = "\n".join(updated_lines)
    if updated_content and had_trailing_newline:
        updated_content += "\n"
    elif updated_content and not had_trailing_newline:
        updated_content += "\n"
    return updated_content, actions


def _strip_managed_zsh_lines(
    lines: list[str],
) -> tuple[list[str], bool, bool]:
    sanitized: list[str] = []
    in_block = False
    removed_block = False
    removed_compinit = False

    for line in lines:
        stripped = line.strip()
        if stripped == _ZSH_BLOCK_START:
            in_block = True
            removed_block = True
            continue
        if stripped == _ZSH_BLOCK_END:
            in_block = False
            continue
        if in_block:
            continue
        if stripped == f"{_ZSH_FPATH_LINE}  {_MARKER}":
            removed_block = True
            continue
        if stripped == f"{_ZSH_COMPINIT_LINE}  {_MARKER}":
            removed_compinit = True
            continue
        sanitized.append(line)
    return sanitized, removed_block, removed_compinit


def _first_live_compinit_index(lines: list[str]) -> int | None:
    for index, line in enumerate(lines):
        if _is_live_compinit_line(line):
            return index
    return None


def _first_live_zfunc_fpath_index(lines: list[str]) -> int | None:
    for index, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if "~/.zfunc" in line and "fpath" in line:
            return index
    return None


def _is_live_compinit_line(line: str) -> bool:
    stripped = line.lstrip()
    if stripped.startswith("#"):
        return False
    return bool(re.search(r"\bcompinit\b", stripped))


def _managed_zsh_fpath_block() -> list[str]:
    return [
        _ZSH_BLOCK_START,
        f"{_ZSH_FPATH_LINE}  {_MARKER}",
        _ZSH_BLOCK_END,
    ]


def _ensure_source_line(rc_file: Path, line: str, dry_run: bool) -> None:
    """Append line to rc_file if not already present."""
    if rc_file.exists():
        content = rc_file.read_text()
        if line in content:
            return
    else:
        content = ""

    if dry_run:
        status(f"would update {rc_file}: add {line}")
        return

    with rc_file.open("a") as f:
        f.write(f"\n{line}  {_MARKER}\n")
    status(f"updated {rc_file}: add {line}")
