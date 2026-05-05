# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import re
from typing import Any

import click

_RICH_TAG_RE = re.compile(r"\[/?[a-zA-Z0-9_ #=/.-]+\]")


def echo_status(message: Any = "", **kwargs: Any) -> None:
    """Write human-facing CLI status text to stderr without Rich markup."""
    kwargs.setdefault("err", True)
    click.echo(_RICH_TAG_RE.sub("", str(message)), **kwargs)
