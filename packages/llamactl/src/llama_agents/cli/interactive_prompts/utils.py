"""Shared utilities for CLI operations"""

import click
from llama_agents.cli.interactive import is_interactive_session
from rich.console import Console

console = Console()


def confirm_action(message: str, default: bool = False) -> bool:
    """
    Ask for confirmation with a consistent interface.

    In non-interactive sessions, returns the default value without prompting.
    """
    if not is_interactive_session():
        return default

    return click.confirm(message, default=default)
