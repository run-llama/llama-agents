# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from unittest.mock import patch

import click
import pytest
from llama_agents.cli.interactive import require_or_list_choices, select_or_exit


def test_select_or_exit_interactive_returns_selected_item() -> None:
    with patch("llama_agents.cli.interactive._blessed_select", return_value=1):
        selected = select_or_exit(
            [(1, "one"), (2, "two")],
            "Pick one",
            "--item",
            interactive=True,
        )

    assert selected == 2


def test_select_or_exit_interactive_cancel_raises() -> None:
    with patch("llama_agents.cli.interactive._blessed_select", return_value=None):
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


def test_select_or_exit_falls_back_when_blessed_unavailable(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with patch(
        "llama_agents.cli.interactive._blessed_select",
        side_effect=ImportError("no blessed"),
    ):
        with pytest.raises(click.ClickException, match="--item"):
            select_or_exit(
                [(1, "one"), (2, "two")],
                "Pick one",
                "--item",
                interactive=True,
            )

    captured = capsys.readouterr()
    assert "- one" in captured.err


def test_require_or_list_choices_lists_and_hints(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(click.ClickException) as exc_info:
        require_or_list_choices(
            [("abc123", "abc123 - running"), ("def456", "def456 - stopped")],
            hint_command="llamactl deployments delete <deployment_id>",
        )

    captured = capsys.readouterr()
    assert "abc123 - running" in captured.err
    assert "def456 - stopped" in captured.err
    assert "llamactl deployments delete <deployment_id>" in str(exc_info.value)


def test_require_or_list_choices_empty_raises() -> None:
    with pytest.raises(click.ClickException, match="No deployments"):
        require_or_list_choices(
            [],
            hint_command="llamactl deployments delete <deployment_id>",
            empty_message="No deployments",
        )
