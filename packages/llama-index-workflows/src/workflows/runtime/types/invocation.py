# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from workflows.utils import _nanoid

INVOCATION_SEPARATOR = "#"


def mint_child_invocation_namespace(
    parent_invocation_namespace: tuple[str, ...], child_slot: str
) -> tuple[str, ...]:
    return (
        *parent_invocation_namespace,
        f"{child_slot}{INVOCATION_SEPARATOR}{_nanoid()}",
    )


def slot_segment(segment: str) -> str:
    return segment.split(INVOCATION_SEPARATOR, 1)[0]


def slot_namespace(
    invocation_namespace: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(slot_segment(segment) for segment in invocation_namespace)


def namespace_startswith(namespace: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    return namespace[: len(prefix)] == prefix
