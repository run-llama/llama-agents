# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from typing import TypeVar

import click

T = TypeVar("T")

_TERM_MENU_UNSUPPORTED = sys.platform == "win32"


def is_interactive_session() -> bool:
    """Return whether the current CLI session can prompt the user."""
    if os.environ.get("CI"):
        return False
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    return True


def _raise_non_interactive(
    entries: list[tuple[T, str]],
    title: str,
    hint_flag: str,
    hint_command: str | None,
) -> T:
    """Print available choices to stderr and raise with a hint."""
    if title:
        click.echo(title, err=True)
    for _, label in entries:
        if label:
            click.echo(f"- {label}", err=True)
    hint = f"Pass {hint_flag} to choose one."
    if hint_command is not None:
        hint += f" To inspect choices, run: {hint_command}"
    raise click.ClickException(hint)


def require_or_list_choices(
    items: Sequence[tuple[str, str]],
    hint_command: str,
    empty_message: str | None = None,
) -> None:
    """Print available choices to stderr and raise with an actionable hint.

    Unlike ``select_or_exit`` this never shows a picker — it always lists
    the available items and tells the user what command to run.  Use this
    for action commands where the user should already know the target.
    """
    if not items:
        raise click.ClickException(empty_message or "No items available")
    click.echo("Available:", err=True)
    for _, label in items:
        if label:
            click.echo(f"  {label}", err=True)
    raise click.ClickException(f"Run: {hint_command}")


def _blessed_select(labels: list[str], title: str, selected: int = 0) -> int | None:
    """Show an interactive menu with type-to-filter. Returns selected index or None."""
    # Deferred for CLI startup: only commands that actually show a menu pay the cost.
    from blessed import Terminal

    term = Terminal()
    query = ""
    cursor = "> "
    max_visible = min(15, term.height - 4)
    selected = max(0, min(selected, len(labels) - 1))
    scroll_offset = max(0, selected - max_visible + 1) if selected >= max_visible else 0
    out = sys.stdout

    def filtered() -> list[tuple[int, str]]:
        if not query:
            return list(enumerate(labels))
        q = query.lower()
        return [(i, label) for i, label in enumerate(labels) if q in label.lower()]

    def highlight_matches(text: str) -> str:
        if not query:
            return text
        q = query.lower()
        pos = text.lower().find(q)
        if pos == -1:
            return text
        before = text[:pos]
        match = text[pos : pos + len(query)]
        after = text[pos + len(query) :]
        return before + term.yellow + term.bold + match + term.normal + after

    def writeln(text: str) -> None:
        out.write(term.move_x(0) + term.clear_eol + text + "\r\n")

    def render() -> int:
        matches = filtered()
        visible = matches[scroll_offset : scroll_offset + max_visible]
        lines = 0

        if title:
            writeln(term.bold + title + term.normal)
            lines += 1

        count = term.dim + f" [{len(matches)}/{len(labels)}]" + term.normal
        if query:
            writeln("/ " + query + count)
        else:
            writeln(term.dim + "type to filter..." + term.normal + count)
        lines += 1

        for i, (_orig_idx, label) in enumerate(visible):
            pos = i + scroll_offset
            if pos == selected:
                writeln(term.bold + cursor + highlight_matches(label) + term.normal)
            else:
                writeln(" " * len(cursor) + highlight_matches(label))
            lines += 1

        out.write(term.clear_eos)
        out.flush()
        return lines

    with term.cbreak(), term.hidden_cursor():
        lines_drawn = render()

        while True:
            key = term.inkey()

            if key.name == "KEY_ESCAPE" or key == "\x03":
                out.write("\n")
                out.flush()
                return None

            if key.name == "KEY_ENTER":
                matches = filtered()
                if matches and 0 <= selected < len(matches):
                    out.write("\n")
                    out.flush()
                    return matches[selected][0]
                continue

            if key.name == "KEY_UP":
                if selected > 0:
                    selected -= 1
                    if selected < scroll_offset:
                        scroll_offset = selected
            elif key.name == "KEY_DOWN":
                matches = filtered()
                if selected < len(matches) - 1:
                    selected += 1
                    if selected >= scroll_offset + max_visible:
                        scroll_offset = selected - max_visible + 1
            elif key.name == "KEY_BACKSPACE" or key == "\x7f":
                if query:
                    query = query[:-1]
                    selected = 0
                    scroll_offset = 0
            elif key and not key.is_sequence and key.isprintable():
                query += str(key)
                selected = 0
                scroll_offset = 0
            else:
                continue

            if lines_drawn > 0:
                out.write(f"\x1b[{lines_drawn}A")
            out.write(term.move_x(0))
            lines_drawn = render()


def select_or_exit(
    items: Sequence[tuple[T, str]],
    title: str,
    hint_flag: str,
    hint_command: str | None = None,
    empty_message: str | None = None,
    interactive: bool | None = None,
    selected: int = 0,
) -> T:
    entries = list(items)
    if not entries:
        raise click.ClickException(empty_message or "No items to select")

    should_prompt = is_interactive_session() if interactive is None else interactive

    if not should_prompt or _TERM_MENU_UNSUPPORTED:
        return _raise_non_interactive(entries, title, hint_flag, hint_command)

    try:
        selected_index = _blessed_select(
            [label for _, label in entries],
            title,
            selected=selected,
        )
    except ImportError:
        return _raise_non_interactive(entries, title, hint_flag, hint_command)

    if selected_index is None:
        raise click.ClickException("Cancelled")
    return entries[selected_index][0]
