# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Collect selection algebra.

The ``Collect`` marker and ``Cardinality`` hierarchy let a step declare *how* a
collection fan-in parameter is released. They are used inside ``Annotated`` on a
``list[E]`` parameter::

    async def fastest(
        events: Annotated[list[Result], Collect(Take(1))],
    ) -> StopEvent: ...

A bare ``list[E]`` parameter is exactly equivalent to ``Collect(All())`` — fire
once when the stream closes with every collected event. ``Annotated[list[E],
Collect()]`` is an explicit, grep-able synonym for the same default.

Only ``All`` and ``Take`` are supported; any other cardinality raises a
validation error instead of silently picking the wrong semantics. ``Collect``
takes keyword-style construction, so future selection knobs can be added
without breaking existing signatures.

Semantics worth knowing:

- Streams are runtime facts: a stream exists only for an execution that
  actually returned a list. ``return []`` opens (and immediately closes) an
  empty stream, so downstream joins fire once with ``[]``. ``return None``
  (under ``list[E] | None``) emits nothing — no stream opens and joins do not
  fire; the branch is dead like any other step returning ``None``.
- A union producer (``-> list[A] | B``) returning a bare ``B`` is ordinary
  dispatch; ``list[A]`` joins do not fire for that execution. When a type
  appears both listed and bare (``-> list[A] | A``), a bare return is the
  declared single member, not a one-element stream. A bare event whose type is
  only declared inside the list (``-> list[A]`` returning ``A``) is a runtime
  error.
- ``ctx.send_event`` cannot add members to a collection stream. An untargeted
  send that merely overlaps an open stream is ignored with a warning (it may
  be legitimate traffic for other steps); a send *targeted* at the collect
  step via ``step=...`` is an explicit instruction the runtime cannot honor
  and raises a ``WorkflowRuntimeError``. The static validation error only
  applies when no ``list[E]`` producer exists for the collect step at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Cardinality:
    """Base class for a collection-collect release strategy.

    Subclasses describe *when* a collect-mode step fires and *which* members it
    receives. Instantiate one of ``All`` / ``Take`` — the base class itself is
    not a usable strategy.
    """


@dataclass(frozen=True)
class All(Cardinality):
    """Fire once when the stream closes, with every collected event (default)."""


@dataclass(frozen=True)
class Take(Cardinality):
    """Fire once on the ``n``-th arrival with the first ``n`` events.

    The remaining siblings keep running but never reach this step. If the stream
    closes before ``n`` members arrive, the step fires once with whatever did
    arrive.
    """

    n: int

    def __post_init__(self) -> None:
        if not isinstance(self.n, int) or self.n < 1:
            raise ValueError("Take(n) requires an integer n >= 1")


@dataclass(frozen=True)
class Collect:
    """Marker for a collection fan-in parameter's selection behavior.

    Wrap it around a ``list[E]`` parameter via ``Annotated``::

        events: Annotated[list[E], Collect(Take(1))]

    Attributes:
        cardinality: When to release and which members to deliver. Defaults to
            ``All()`` (fire on stream close with everything).
    """

    cardinality: Cardinality = field(default_factory=All)
