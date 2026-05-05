# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Pure body parser for structlog-formatted log lines.

The control plane's log SSE endpoint emits ``LogEvent`` envelopes whose
``text`` field is a single log line — typically a structlog JSON record like
``{"event": "...", "level": "info", "timestamp": "...", "logger": "..."}``,
but sometimes a plain string (e.g. application stdout). This module owns the
parsing of that body and the plain-text renderer used by the
``deployments logs`` CLI command.

This module knows nothing about ``LogEvent.pod`` / ``LogEvent.container`` /
``LogEvent.timestamp`` — those live on the envelope and are formatted by
callers. The K8s leading timestamp is already stripped before bodies reach
this parser (see ``k8s_client._parse_raw_log_lines``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

# structlog keys handled inline; everything else goes into ``extras``.
_KNOWN_KEYS = frozenset({"event", "level", "timestamp", "logger", "request_id"})


@dataclass
class ParsedLogBody:
    """Structured form of one structlog body line.

    ``raw`` holds the original input. If the line wasn't structlog JSON,
    ``event`` carries the raw text and ``structured`` is False.
    """

    raw: str
    structured: bool
    timestamp: str = ""
    level: str = ""
    logger: str = ""
    event: str = ""
    request_id: str = ""
    extras: dict[str, object] = field(default_factory=dict)


def parse_log_body(line: str) -> ParsedLogBody:
    """Parse a structlog JSON body into a ``ParsedLogBody``.

    Falls back to a non-structured ``ParsedLogBody`` (with ``event=line``)
    when the input is not a structlog dict — which is fine, downstream
    renderers just emit it as-is.
    """
    raw = line
    stripped = line.strip()
    if not stripped.startswith("{"):
        return ParsedLogBody(raw=raw, structured=False, event=line)
    try:
        data = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return ParsedLogBody(raw=raw, structured=False, event=line)

    if not isinstance(data, dict) or "event" not in data:
        return ParsedLogBody(raw=raw, structured=False, event=line)

    return ParsedLogBody(
        raw=raw,
        structured=True,
        timestamp=str(data.get("timestamp", "")),
        level=str(data.get("level", "")).lower(),
        logger=str(data.get("logger", "")),
        event=str(data.get("event", "")),
        request_id=str(data.get("request_id", "")),
        extras={k: v for k, v in data.items() if k not in _KNOWN_KEYS},
    )


def trim_timestamp(ts: str) -> str:
    """Trim ISO timestamp to its time portion."""
    if "T" in ts:
        ts = ts.split("T", 1)[1]
        for suffix in ("Z", "+00:00"):
            ts = ts.removesuffix(suffix)
    return ts


def render_plain(parsed: ParsedLogBody) -> str:
    """Render ``parsed`` as a plain string (no Rich/ANSI).

    Used by ``llamactl deployments logs`` text mode.
    """
    if not parsed.structured:
        return parsed.event or parsed.raw

    parts: list[str] = []
    ts = trim_timestamp(parsed.timestamp)
    if ts:
        parts.append(ts)
    if parsed.level:
        parts.append(f"{parsed.level.upper():8s}")
    if parsed.logger:
        parts.append(parsed.logger)
    parts.append(parsed.event)

    line = " ".join(p for p in parts if p)
    if parsed.request_id:
        line += f" req={parsed.request_id}"
    if parsed.extras:
        line += " " + " ".join(f"{k}={v}" for k, v in parsed.extras.items())
    return line
