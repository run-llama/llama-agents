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

This package re-exports the public and test-facing surface so
``workflows.runtime.control_loop`` stays a single import target. ``import time``
is kept here because tests patch ``control_loop.time.time``.
"""

from __future__ import annotations

import time  # noqa: F401  -- patched as control_loop.time.time in tests

from workflows.runtime.control_loop.reduce import (
    ExitCommand,
    ReplayResult,
    _add_or_enqueue_event,
    _check_idle_state,
    _consume_superseded_delayed_attempt,
    _decide_retry_delay,
    _drain_eligible_queue,
    _is_eligible,
    _process_add_event_tick,
    _process_cancel_run_tick,
    _process_publish_event_tick,
    _process_step_result_tick,
    _process_timeout_tick,
    _process_waiter_timeout_tick,
    _process_wakeup_tick,
    _reduce_tick,
    _root_step_key,
    _static_collect_events,
    rebuild_state_from_ticks,
    rebuild_state_from_ticks_stream,
    replay_ticks_stream,
    rewind_in_progress,
)
from workflows.runtime.control_loop.runner import (
    _ControlLoopRunner,
    _is_shutdown_error,
    _single_pull,
    control_loop,
)
from workflows.runtime.control_loop.streams import (
    WorkDisposition,
    _adjust_open_work_items,
    _classify_work_item,
    _clear_collection_state,
    _close_collection_stream,
    _count_accepting_steps,
    _detect_stuck_streams,
    _fire_collection_release,
    _mint_stream_id,
    _release_on_close,
    _release_on_item,
    _release_state_for,
)

# Re-export the prior single-module surface (public API + the private symbols
# that tests and sibling packages import) so static analyzers treat them as
# exported from this package.
__all__ = [
    "ExitCommand",
    "ReplayResult",
    "WorkDisposition",
    "_ControlLoopRunner",
    "_add_or_enqueue_event",
    "_adjust_open_work_items",
    "_check_idle_state",
    "_classify_work_item",
    "_clear_collection_state",
    "_close_collection_stream",
    "_consume_superseded_delayed_attempt",
    "_count_accepting_steps",
    "_decide_retry_delay",
    "_detect_stuck_streams",
    "_drain_eligible_queue",
    "_fire_collection_release",
    "_is_eligible",
    "_is_shutdown_error",
    "_mint_stream_id",
    "_process_add_event_tick",
    "_process_cancel_run_tick",
    "_process_publish_event_tick",
    "_process_step_result_tick",
    "_process_timeout_tick",
    "_process_waiter_timeout_tick",
    "_process_wakeup_tick",
    "_reduce_tick",
    "_release_on_close",
    "_release_on_item",
    "_release_state_for",
    "_root_step_key",
    "_single_pull",
    "_static_collect_events",
    "control_loop",
    "rebuild_state_from_ticks",
    "rebuild_state_from_ticks_stream",
    "replay_ticks_stream",
    "rewind_in_progress",
]
