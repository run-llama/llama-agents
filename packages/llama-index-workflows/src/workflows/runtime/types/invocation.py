# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

INVOCATION_SEPARATOR = "#"


def mint_slot_segment(slot: str, seq: int) -> str:
    """One invocation-path segment for the ``seq``-th invocation of ``slot``.

    Deterministic and counter-based (``slot#0``, ``slot#1``): minted from a
    per-slot counter persisted on the parent broker, so snapshot-resume and
    full-journal replay produce identical ids.
    """
    return f"{slot}{INVOCATION_SEPARATOR}{seq}"


def slot_segment(segment: str) -> str:
    """Static slot name of one path segment (``slot#3`` -> ``slot``)."""
    return segment.split(INVOCATION_SEPARATOR, 1)[0]


def slot_namespace(
    invocation_namespace: tuple[str, ...],
) -> tuple[str, ...]:
    """Project a runtime invocation path onto its static slot-name path."""
    return tuple(slot_segment(segment) for segment in invocation_namespace)


def namespace_startswith(namespace: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    return namespace[: len(prefix)] == prefix
