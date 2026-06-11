# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Centralized event-type matching.

A workflow step declares the event types it accepts. Whether a given event
routes to that step is decided in one of two modes:

* **exact** (the default): the event matches only when its concrete type is
  exactly one of the declared accepted types.
* **subclass-aware** (``accept_event_subclasses=True``): the event also matches
  when its type is a subclass of a declared accepted type.

Every place that answers "does this event/type belong to this step?" — graph
construction, static validation, and runtime routing — funnels through the
helpers below so the rule stays identical across all of them.

Step signatures may also carry annotations that are not classes at all
(``dict``, ``list[str]``, ``typing.Any``, ...). ``issubclass`` raises
``TypeError`` on those, so :func:`is_subclass` is used as a safe replacement
that treats non-classes as "not a subclass" rather than letting them blow up.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def is_subclass(candidate: Any, parents: type | tuple[type, ...]) -> bool:
    """``issubclass`` that tolerates non-class annotations.

    Returns ``False`` (instead of raising ``TypeError``) when ``candidate`` is
    not a class, e.g. a parametrized generic or ``typing.Any``.
    """
    return isinstance(candidate, type) and issubclass(candidate, parents)


def type_matches(produced: Any, accepted: type, *, allow_subclasses: bool) -> bool:
    """Whether a produced event *type* routes to a step accepting ``accepted``."""
    if allow_subclasses:
        return is_subclass(produced, accepted)
    return produced is accepted


def event_matches(event: Any, accepted: type, *, allow_subclasses: bool) -> bool:
    """Whether an event *instance* routes to a step accepting ``accepted``."""
    if allow_subclasses:
        return isinstance(event, accepted)
    return type(event) is accepted


def step_accepts_type(
    produced: Any, accepted_events: Iterable[type], *, allow_subclasses: bool
) -> bool:
    """Whether a produced event *type* is accepted by any of ``accepted_events``."""
    return any(
        type_matches(produced, accepted, allow_subclasses=allow_subclasses)
        for accepted in accepted_events
    )


def step_accepts_event(
    event: Any, accepted_events: Iterable[type], *, allow_subclasses: bool
) -> bool:
    """Whether an event *instance* is accepted by any of ``accepted_events``."""
    return any(
        event_matches(event, accepted, allow_subclasses=allow_subclasses)
        for accepted in accepted_events
    )
