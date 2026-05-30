# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Collect selection algebra (Phase L3).

The ``Collect`` marker and ``Cardinality`` hierarchy let a step declare *how* a
batch fan-in parameter is released. They are used inside ``Annotated`` on a
``list[E]`` parameter::

    async def fastest(
        events: Annotated[list[Result], Collect(Take(1))],
    ) -> StopEvent: ...

A bare ``list[E]`` parameter is exactly equivalent to ``Collect(All())`` — fire
once when the batch closes with every collected event. ``Annotated[list[E],
Collect()]`` is an explicit, grep-able synonym for the same default.

Only the v1 cardinalities (``All`` / ``Take`` / ``AtLeast``) are implemented.
``Buffer`` / ``Window`` (streaming aggregation) are deferred to v2. The ``at`` /
``from_`` / ``where`` knobs are accepted on the marker but not yet wired into
the runtime — declaring them raises a clear validation error.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Union

# A reference to another step, by name or by the decorated step function itself.
# Used by ``Collect(at=..., from_=...)``. Resolution to a concrete step is a
# later phase; the marker only stores the raw reference for now.
StepRef = Union[str, Callable[..., Any]]


@dataclass(frozen=True)
class Cardinality:
    """Base class for a batch-collect release strategy.

    Subclasses describe *when* a collect-mode step fires and *which* members it
    receives. Instantiate one of ``All`` / ``Take`` / ``AtLeast`` — the base
    class itself is not a usable strategy.
    """


@dataclass(frozen=True)
class All(Cardinality):
    """Fire once when the batch closes, with every collected event (default)."""


@dataclass(frozen=True)
class Take(Cardinality):
    """Fire once on the ``n``-th arrival with the first ``n`` events.

    The remaining siblings are dropped — they keep running (cancellation is a
    separate, future feature) but never reach this step. If the batch closes
    before ``n`` members arrive, the step fires once with whatever did arrive.
    """

    n: int

    def __post_init__(self) -> None:
        if not isinstance(self.n, int) or self.n < 1:
            raise ValueError("Take(n) requires an integer n >= 1")


@dataclass(frozen=True)
class AtLeast(Cardinality):
    """Fire once on the ``n``-th arrival; keep accepting later siblings.

    Quorum semantics: release as soon as ``n`` members have arrived. Unlike
    ``Take``, later siblings are not dropped — but in v1 they do not trigger a
    second fire (re-firing / windowing is v2). If the batch closes before ``n``
    members arrive, the step fires once with whatever did arrive.
    """

    n: int

    def __post_init__(self) -> None:
        if not isinstance(self.n, int) or self.n < 1:
            raise ValueError("AtLeast(n) requires an integer n >= 1")


@dataclass(frozen=True)
class Collect:
    """Marker for a batch fan-in parameter's selection behavior.

    Wrap it around a ``list[E]`` parameter via ``Annotated``::

        events: Annotated[list[E], Collect(Take(1))]

    Attributes:
        cardinality: When to release and which members to deliver. Defaults to
            ``All()`` (fire on batch close with everything).
        at: Promote scope to this step's batch instead of the nearest enclosing
            one. Not yet implemented (declaring it raises a validation error).
        from_: Restrict provenance to events produced by this step. Not yet
            implemented (declaring it raises a validation error).
        where: Narrowing predicate over members. Deferred to v2 (declaring it
            raises a validation error).
    """

    cardinality: Cardinality = field(default_factory=All)
    at: StepRef | None = None
    from_: StepRef | None = None
    where: Callable[[Any], bool] | None = None
