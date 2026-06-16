# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Static stream-level type traversal.

A ``list[E]`` fan-out execution opens a *stream level*: the level contains the
member types it emits plus everything reachable from them through 1:1 steps
without crossing into another level. A nested fan-out consumer opens a child
level; what it contributes back to the current level is the output of the
collect steps bound to that child stream — collapsed recursively when a collect
step itself fans out.

This single traversal feeds both static validation
(:mod:`workflows.representation.validate`) and runtime binding computation
(:mod:`workflows.runtime.types.internal_state`); keep them on this one
implementation so they cannot drift.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from workflows._event_matching import step_accepts_type
from workflows.decorators import StepConfig
from workflows.events import Event


def event_types(types: Iterable[Any]) -> list[type[Event]]:
    """Filter a type sequence down to concrete Event subclasses."""
    return [t for t in types if isinstance(t, type) and issubclass(t, Event)]


def same_level_types(
    steps: dict[str, StepConfig],
    seed_types: Iterable[Any],
    guard: frozenset[str],
) -> set[type[Event]]:
    """Event types reachable from ``seed_types`` without changing stream level.

    The ``guard`` holds producer step names already on the recursion path so
    self-feeding fan-out loops terminate. Collect steps never continue the
    walk directly: their members are absorbed at this level, and their outputs
    surface one level up (handled by the caller's collapse).
    """
    collects: dict[str, tuple[Any, ...]] = {
        name: cfg.collection_param[1]
        for name, cfg in steps.items()
        if cfg.collection_param is not None
    }

    def collapsed_outputs(producer: str, guard: frozenset[str]) -> set[type[Event]]:
        """Types a fan-out execution surfaces at the level that triggered it.

        Its members live in a child stream; what comes back to the trigger
        level is the output of collect steps bound to that child stream. A
        collect step that itself fans out opens yet another child stream, so
        its contribution collapses recursively instead of injecting its raw
        member types at this level.
        """
        child = walk(steps[producer].return_types, guard | {producer})
        out: set[type[Event]] = set()
        for collect_name, collect_event_types in collects.items():
            collect_cfg = steps[collect_name]
            if not any(
                step_accepts_type(
                    produced,
                    collect_event_types,
                    allow_subclasses=collect_cfg.accept_event_subclasses,
                )
                for produced in child
            ):
                continue
            if collect_cfg.is_fan_out:
                if collect_name not in guard:
                    out |= collapsed_outputs(collect_name, guard | {collect_name})
            else:
                out |= set(event_types(collect_cfg.return_types))
        return out

    def walk(seeds: Iterable[Any], guard: frozenset[str]) -> set[type[Event]]:
        seen: set[type[Event]] = set()
        frontier: list[type[Event]] = list(event_types(seeds))
        while frontier:
            event_type = frontier.pop()
            if event_type in seen:
                continue
            seen.add(event_type)
            for name, cfg in steps.items():
                if not step_accepts_type(
                    event_type,
                    cfg.accepted_events,
                    allow_subclasses=cfg.accept_event_subclasses,
                ):
                    continue
                if cfg.collection_param is not None:
                    continue
                if cfg.is_fan_out:
                    if name in guard:
                        continue
                    frontier.extend(collapsed_outputs(name, guard))
                    continue
                frontier.extend(event_types(cfg.return_types))
        return seen

    return walk(seed_types, guard)


def stream_level_types_by_producer(
    steps: dict[str, StepConfig],
) -> dict[str, set[type[Event]]]:
    """Event types produced inside each returned-list producer's stream level."""
    return {
        name: same_level_types(steps, cfg.return_types, frozenset({name}))
        for name, cfg in steps.items()
        if cfg.is_fan_out
    }
