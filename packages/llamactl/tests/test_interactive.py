# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest
from llama_agents.cli.interactive import select_or_exit


def test_select_or_exit_interactive_returns_selected_item() -> None:
    terminal_menu = MagicMock()
    terminal_menu.show.return_value = 1
    terminal_menu_cls = MagicMock(return_value=terminal_menu)

    with patch.dict(
        sys.modules,
        {"simple_term_menu": SimpleNamespace(TerminalMenu=terminal_menu_cls)},
    ):
        selected = select_or_exit(
            [(1, "one"), (2, "two")],
            "Pick one",
            "--item",
            interactive=True,
        )

    assert selected == 2
    terminal_menu_cls.assert_called_once_with(
        ["one", "two"],
        title="Pick one",
        menu_cursor="> ",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("fg_cyan",),
        search_highlight_style=("fg_yellow", "bold"),
        search_key=None,
        skip_empty_entries=True,
    )


def test_select_or_exit_interactive_cancel_raises() -> None:
    terminal_menu = MagicMock()
    terminal_menu.show.return_value = None

    with patch.dict(
        sys.modules,
        {
            "simple_term_menu": SimpleNamespace(
                TerminalMenu=MagicMock(return_value=terminal_menu)
            )
        },
    ):
        with pytest.raises(click.ClickException, match="Cancelled"):
            select_or_exit([(1, "one")], "Pick one", "--item", interactive=True)


def test_select_or_exit_non_interactive_lists_items_and_hints(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(click.ClickException) as exc_info:
        select_or_exit(
            [(1, "one"), (2, "two")],
            "Pick one",
            "--item",
            hint_command="llamactl things list",
            interactive=False,
        )

    captured = capsys.readouterr()
    assert "Pick one" in captured.err
    assert "- one" in captured.err
    assert "- two" in captured.err
    assert "--item" in str(exc_info.value)
    assert "llamactl things list" in str(exc_info.value)


def test_select_or_exit_empty_raises_custom_message() -> None:
    with pytest.raises(click.ClickException, match="Nothing available"):
        select_or_exit(
            [],
            "Pick one",
            "--item",
            empty_message="Nothing available",
            interactive=True,
        )
