# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from typing import Any

import click


def status(message: Any = "", *, nl: bool = True) -> None:
    """Write human-facing CLI status text to stderr."""
    click.echo(str(message), err=True, nl=nl)


def warning(message: Any, *, nl: bool = True) -> None:
    """Write a human-facing CLI warning to stderr."""
    status(f"warning: {message}", nl=nl)
