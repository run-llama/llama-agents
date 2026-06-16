# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Workflow control loop.

Split across three modules along the reducer pattern's natural seam:

- ``runner``  — the async runtime: tasks, the scheduled-wakeup heap, the
  adapter, and turning reducer commands into side effects. The stateful half.
- ``reduce``  — the pure reducer: ``_reduce_tick`` and every per-tick
  processor, plus retry/collect/replay helpers. ``State + Tick -> (State, Commands)``.
- ``streams`` — collection-stream accounting: fan-out stream lifecycle,
  open-work-item counting, and collect-batch release.

This package facade exports only the cross-package API. White-box tests import
the internals they exercise straight from the owning submodule. ``import time``
is kept here because tests patch ``control_loop.time.time``.
"""

from __future__ import annotations

import time  # noqa: F401  -- patched as control_loop.time.time in tests

from workflows.runtime.control_loop.reduce import (
    rebuild_state_from_ticks,
    rebuild_state_from_ticks_stream,
    replay_ticks_stream,
)
from workflows.runtime.control_loop.runner import control_loop

__all__ = [
    "control_loop",
    "rebuild_state_from_ticks",
    "rebuild_state_from_ticks_stream",
    "replay_ticks_stream",
]
