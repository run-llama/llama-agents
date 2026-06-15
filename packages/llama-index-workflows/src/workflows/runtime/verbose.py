# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
Verbose runtime decorator that logs tick-level workflow activity in real time.

Intercepts both ``write_to_event_stream`` (for step state changes) and
``on_tick`` (for all other tick types: events added, publishes, timeouts,
cancellation, idle releases).

Output destination is auto-detected: if the ``"workflows.verbose"`` logger is
configured to emit DEBUG or INFO messages, those levels are used (in that
priority order).  Otherwise output falls back to :func:`print`.
"""

from __future__ import annotations

import logging
from typing import Callable

from workflows._event_summary import summarize_event
from workflows.events import Event, StepState, StepStateChanged, StopEvent
from workflows.runtime.runtime_decorators import (
    BaseInternalRunAdapterDecorator,
    BaseRuntimeDecorator,
)
from workflows.runtime.types.plugin import InternalRunAdapter, Runtime
from workflows.runtime.types.results import StepWorkerResult
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickCancelRun,
    TickIdleRelease,
    TickPublishEvent,
    TickStepResult,
    TickTimeout,
    TickWaiterTimeout,
    WorkflowTick,
)
from workflows.workflow import Workflow

verbose_logger = logging.getLogger("workflows.verbose")


def _clean_event_name(raw: str) -> str:
    """Extract a short class name from ``str(type(...))`` format.

    Handles both ``"<class 'pkg.module.Cls'>"`` and plain ``"Cls"`` strings.
    Returns ``None`` when the underlying type is ``NoneType``.
    """
    if raw.startswith("<class '") and raw.endswith("'>"):
        raw = raw[8:-2].rsplit(".", 1)[-1]
    return raw


def _resolve_output() -> Callable[[str], None]:
    """Pick the best output sink based on current logging configuration."""
    if verbose_logger.isEnabledFor(logging.DEBUG):
        return verbose_logger.debug
    if verbose_logger.isEnabledFor(logging.INFO):
        return verbose_logger.info
    return print


class _VerboseInternalRunAdapter(BaseInternalRunAdapterDecorator):
    """Intercepts write_to_event_stream and on_tick to print/log workflow activity."""

    def __init__(
        self,
        decorated: InternalRunAdapter,
        output: Callable[[str], None],
    ) -> None:
        super().__init__(decorated)
        self._output = output

    async def write_to_event_stream(self, event: Event) -> None:
        if isinstance(event, StepStateChanged):
            prefix = f"[{event.name}:{event.worker_id}]"
            if event.step_state == StepState.RUNNING:
                self._output(f"{prefix} started from {event.input_event_name}")
            elif event.step_state == StepState.NOT_RUNNING:
                name = (
                    _clean_event_name(event.output_event_name)
                    if event.output_event_name
                    else None
                )
                if name and name != "NoneType":
                    self._output(f"{prefix} complete with {name}")
                else:
                    self._output(f"{prefix} complete with no result")
            elif event.step_state == StepState.PREPARING:
                self._output(f"[{event.name}] enqueued (waiting for capacity)")
        await super().write_to_event_stream(event)

    async def on_tick(self, tick: WorkflowTick) -> None:
        if isinstance(tick, TickAddEvent):
            summary = summarize_event(tick.event)
            target = f" -> {tick.step_id}" if tick.step_id else ""
            self._output(f"[tick] add: {summary}{target}")
        elif isinstance(tick, TickPublishEvent):
            self._output(f"[tick] publish: {summarize_event(tick.event)}")
        elif isinstance(tick, TickTimeout):
            self._output(f"[tick] timeout: {tick.timeout}s")
        elif isinstance(tick, TickWaiterTimeout):
            self._output(
                f"[tick] waiter timeout: step {tick.step_id} waiter {tick.waiter_id}"
            )
        elif isinstance(tick, TickCancelRun):
            self._output("[tick] cancelled")
        elif isinstance(tick, TickIdleRelease):
            self._output("[tick] idle release")
        elif isinstance(tick, TickStepResult):
            for result in tick.result:
                if isinstance(result, StepWorkerResult) and isinstance(
                    result.result, StopEvent
                ):
                    self._output(f"[result] {summarize_event(result.result)}")
        await super().on_tick(tick)


class VerboseDecorator(BaseRuntimeDecorator):
    """Runtime decorator that prints step starts and completions.

    Output destination is auto-detected at construction time based on the
    ``"workflows.verbose"`` logger's effective level.  If DEBUG or INFO
    messages would be emitted, the logger is used; otherwise falls back to
    :func:`print`.
    """

    def __init__(self, decorated: Runtime) -> None:
        super().__init__(decorated)
        self._output = _resolve_output()

    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        inner = self._decorated.get_internal_adapter(workflow)
        return _VerboseInternalRunAdapter(inner, self._output)
