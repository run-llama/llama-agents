# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import functools
import os
import sys
from collections.abc import Sequence
from typing import TypeVar

import click

T = TypeVar("T")


@functools.cache
def is_interactive_session() -> bool:
    """Return whether the current CLI session can prompt the user."""
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    return True


def select_or_exit(
    items: Sequence[tuple[T, str]],
    title: str,
    hint_flag: str,
    hint_command: str | None = None,
    empty_message: str | None = None,
    interactive: bool | None = None,
) -> T:
    entries = list(items)
    if not entries:
        raise click.ClickException(empty_message or "No items to select")

    should_prompt = is_interactive_session() if interactive is None else interactive
    if not should_prompt:
        click.echo(title, err=True)
        for _, label in entries:
            if label:
                click.echo(f"- {label}", err=True)
        hint = f"Pass {hint_flag} to choose one."
        if hint_command is not None:
            hint += f" To inspect choices, run: {hint_command}"
        raise click.ClickException(hint)

    # Deferred for CLI startup: only commands that actually show a menu need STM.
    from simple_term_menu import TerminalMenu

    menu = TerminalMenu(
        [label for _, label in entries],
        title=title,
        menu_cursor="→ ",
        menu_cursor_style=(),
        menu_highlight_style=(),
        search_highlight_style=(),
        search_key=None,
        show_search_hint=True,
        skip_empty_entries=True,
    )
    selected_index = menu.show()
    if selected_index is None:
        raise click.ClickException("Cancelled")
    if not isinstance(selected_index, int):
        raise click.ClickException("Cancelled")
    return entries[selected_index][0]
