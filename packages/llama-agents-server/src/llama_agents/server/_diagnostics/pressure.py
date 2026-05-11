# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
import traceback
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import psutil
from typing_extensions import override
from workflows.context.serializers import BaseSerializer
from workflows.events import Event, StartEvent, StepState, StepStateChanged
from workflows.runtime.runtime_decorators import (
    BaseExternalRunAdapterDecorator,
    BaseInternalRunAdapterDecorator,
    BaseRuntimeDecorator,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    Runtime,
)
from workflows.workflow import Workflow

pressure_logger = logging.getLogger("llama_agents.server.diagnostics.pressure")

MB = 1024 * 1024


@dataclass(frozen=True)
class PressureDiagnosticsConfig:
    """Configuration for opt-in workflow pressure diagnostics."""

    enabled: bool = False
    sample_interval: float = 1.0
    event_loop_lag_threshold_ms: float | None = 250.0
    memory_rss_threshold_mb: float | None = None
    memory_growth_threshold_mb: float | None = None
    memory_growth_window_seconds: float = 60.0
    capture_lag_stacks: bool = False
    lag_stack_max_frames: int = 40
    max_events: int = 500
    event_ttl_seconds: float | None = 3600.0
    activity_buffer_size: int = 1000
    memory_sample_buffer_size: int = 300
    max_active_intervals_in_event: int = 25
    warning_rate_limit_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.sample_interval <= 0:
            raise ValueError("sample_interval must be greater than 0")
        if self.memory_growth_window_seconds <= 0:
            raise ValueError("memory_growth_window_seconds must be greater than 0")
        if self.max_events <= 0:
            raise ValueError("max_events must be greater than 0")
        if self.activity_buffer_size <= 0:
            raise ValueError("activity_buffer_size must be greater than 0")
        if self.memory_sample_buffer_size <= 0:
            raise ValueError("memory_sample_buffer_size must be greater than 0")
        if self.max_active_intervals_in_event <= 0:
            raise ValueError("max_active_intervals_in_event must be greater than 0")
        if self.warning_rate_limit_seconds < 0:
            raise ValueError("warning_rate_limit_seconds must be non-negative")


@dataclass(frozen=True)
class _ActivityInterval:
    run_id: str
    workflow_name: str | None
    step_name: str
    worker_id: str
    input_event_name: str
    started_at: str
    started_monotonic: float
    output_event_name: str | None = None
    ended_at: str | None = None
    ended_monotonic: float | None = None
    duration_seconds: float | None = None
    finish_reason: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.run_id, self.step_name, self.worker_id)

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _MemorySample:
    timestamp: str
    monotonic: float
    rss_bytes: int
    rss_mb: float

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)


class PressureDiagnosticsRecorder:
    """Process-local recorder and sampler for workflow pressure diagnostics."""

    def __init__(
        self,
        config: PressureDiagnosticsConfig,
        *,
        monotonic_clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] | None = None,
        rss_sampler: Callable[[], int] | None = None,
        logger: logging.Logger = pressure_logger,
    ) -> None:
        self.config = config
        self._clock = monotonic_clock
        self._wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self._rss_sampler = rss_sampler or (lambda: psutil.Process().memory_info().rss)
        self._logger = logger
        self._pid = os.getpid()
        self._lock = threading.RLock()
        self._events: deque[dict[str, Any]] = deque(maxlen=config.max_events)
        self._active_intervals: dict[tuple[str, str, str], _ActivityInterval] = {}
        self._completed_intervals: deque[_ActivityInterval] = deque(
            maxlen=config.activity_buffer_size
        )
        self._memory_samples: deque[_MemorySample] = deque(
            maxlen=config.memory_sample_buffer_size
        )
        self._run_workflow_names: dict[str, str] = {}
        self._subscribers: list[Callable[[dict[str, Any]], None]] = []
        self._sampler_task: asyncio.Task[None] | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._loop_thread_id: int | None = None
        self._last_heartbeat_monotonic: float | None = None
        self._last_warning_monotonic: dict[str, float] = {}
        self._is_started = False

    @property
    def is_started(self) -> bool:
        return self._is_started

    async def start(self) -> None:
        if self._is_started or not self.config.enabled:
            return
        self._is_started = True
        self._stop_event.clear()
        self._loop_thread_id = threading.get_ident()
        self._last_heartbeat_monotonic = self._clock()
        self._sampler_task = asyncio.create_task(
            self._sample_loop(), name="pressure-diagnostics-sampler"
        )
        if self.config.event_loop_lag_threshold_ms is not None:
            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop,
                name="pressure-diagnostics-watchdog",
                daemon=True,
            )
            self._watchdog_thread.start()

    async def stop(self) -> None:
        if not self._is_started:
            return
        self._is_started = False
        self._stop_event.set()
        task = self._sampler_task
        self._sampler_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        thread = self._watchdog_thread
        self._watchdog_thread = None
        if thread is not None:
            thread.join(timeout=max(1.0, self.config.sample_interval * 2))
        self.finish_run_intervals("*", reason="recorder_stopped")

    def subscribe(
        self, callback: Callable[[dict[str, Any]], None]
    ) -> Callable[[], None]:
        with self._lock:
            self._subscribers.append(callback)

        def unsubscribe() -> None:
            with self._lock:
                if callback in self._subscribers:
                    self._subscribers.remove(callback)

        return unsubscribe

    def record_run_start(self, run_id: str, workflow_name: str) -> None:
        with self._lock:
            self._run_workflow_names[run_id] = workflow_name

    def record_run_end(self, run_id: str, *, reason: str = "run_ended") -> None:
        self.finish_run_intervals(run_id, reason=reason)
        with self._lock:
            self._run_workflow_names.pop(run_id, None)

    def record_step_state(self, run_id: str, event: StepStateChanged) -> None:
        if event.step_state == StepState.RUNNING:
            self.record_step_start(
                run_id=run_id,
                workflow_name=self._workflow_name_for_run(run_id),
                step_name=event.name,
                worker_id=event.worker_id,
                input_event_name=event.input_event_name,
            )
        elif event.step_state == StepState.NOT_RUNNING:
            self.record_step_end(
                run_id=run_id,
                step_name=event.name,
                worker_id=event.worker_id,
                output_event_name=event.output_event_name,
            )

    def record_step_start(
        self,
        *,
        run_id: str,
        workflow_name: str | None,
        step_name: str,
        worker_id: str,
        input_event_name: str,
    ) -> None:
        now = self._clock()
        interval = _ActivityInterval(
            run_id=run_id,
            workflow_name=workflow_name,
            step_name=step_name,
            worker_id=worker_id,
            input_event_name=input_event_name,
            started_at=self._timestamp(),
            started_monotonic=now,
        )
        key = interval.key
        with self._lock:
            replaced = self._active_intervals.pop(key, None)
            if replaced is not None:
                self._completed_intervals.append(
                    self._finish_interval(
                        replaced, now=now, reason="replaced_by_new_start"
                    )
                )
            self._active_intervals[key] = interval
        self._record_event(
            "workflow_step_interval_started",
            interval.as_payload(),
            level=logging.DEBUG,
        )

    def record_step_end(
        self,
        *,
        run_id: str,
        step_name: str,
        worker_id: str,
        output_event_name: str | None = None,
        reason: str = "step_not_running",
    ) -> None:
        key = (run_id, step_name, worker_id)
        now = self._clock()
        with self._lock:
            interval = self._active_intervals.pop(key, None)
            if interval is None:
                return
            finished = self._finish_interval(
                interval,
                now=now,
                reason=reason,
                output_event_name=output_event_name,
            )
            self._completed_intervals.append(finished)
        self._record_event(
            "workflow_step_interval_finished",
            finished.as_payload(),
            level=logging.DEBUG,
        )

    def finish_run_intervals(self, run_id: str, *, reason: str) -> None:
        now = self._clock()
        finished: list[_ActivityInterval] = []
        with self._lock:
            for key, interval in list(self._active_intervals.items()):
                if run_id != "*" and interval.run_id != run_id:
                    continue
                self._active_intervals.pop(key, None)
                completed = self._finish_interval(interval, now=now, reason=reason)
                self._completed_intervals.append(completed)
                finished.append(completed)
        for interval in finished:
            self._record_event(
                "workflow_step_interval_finished",
                interval.as_payload(),
                level=logging.DEBUG,
            )

    def record_pressure_event(self, event_type: str, payload: dict[str, Any]) -> None:
        self._record_event(event_type, payload, level=logging.WARNING)

    def sample_memory_once(self) -> _MemorySample:
        return self.record_memory_sample()

    def record_memory_sample(self, rss_bytes: int | None = None) -> _MemorySample:
        rss = rss_bytes if rss_bytes is not None else self._rss_sampler()
        sample = _MemorySample(
            timestamp=self._timestamp(),
            monotonic=self._clock(),
            rss_bytes=rss,
            rss_mb=round(rss / MB, 3),
        )
        with self._lock:
            self._memory_samples.append(sample)
        self._check_memory_thresholds(sample)
        return sample

    def record_event_loop_lag(
        self, lag_ms: float, stack_frames: list[str] | None = None
    ) -> None:
        threshold_ms = self.config.event_loop_lag_threshold_ms
        if threshold_ms is not None and lag_ms < threshold_ms:
            return
        frames = stack_frames or []
        if self.config.capture_lag_stacks:
            frames = frames[: self.config.lag_stack_max_frames]
        else:
            frames = []
        active = self._active_intervals_payload()
        payload = {
            "lag_ms": lag_ms,
            "threshold_ms": threshold_ms,
            "active_interval_count": active["count"],
            "active_intervals": active["items"],
            "active_intervals_truncated": active["truncated"],
            "stack_frames": frames,
            "stack_captured": bool(frames),
        }
        self._record_event(
            "event_loop_lag_threshold_exceeded",
            payload,
            level=logging.WARNING,
        )

    def record_lag(
        self,
        lag_seconds: float,
        *,
        source: str,
        stack_frames: list[str] | None = None,
    ) -> None:
        threshold_ms = self.config.event_loop_lag_threshold_ms
        lag_ms = round(lag_seconds * 1000, 3)
        if threshold_ms is None or lag_ms < threshold_ms:
            return
        if not self._can_emit_warning("event_loop_lag_threshold_exceeded"):
            return
        frames = stack_frames or []
        if self.config.capture_lag_stacks:
            frames = frames[: self.config.lag_stack_max_frames]
        else:
            frames = []
        active = self._active_intervals_payload()
        self._record_event(
            "event_loop_lag_threshold_exceeded",
            {
                "lag_ms": lag_ms,
                "threshold_ms": threshold_ms,
                "source": source,
                "active_interval_count": active["count"],
                "active_intervals": active["items"],
                "active_intervals_truncated": active["truncated"],
                "stack_frames": frames,
                "stack_captured": bool(frames),
            },
            level=logging.WARNING,
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            samples = list(self._memory_samples)
            active = [
                interval.as_payload() for interval in self._active_intervals.values()
            ]
            return {
                "pid": self._pid,
                "is_started": self._is_started,
                "active_interval_count": len(active),
                "active_intervals": active[: self.config.max_active_intervals_in_event],
                "active_intervals_truncated": max(
                    0, len(active) - self.config.max_active_intervals_in_event
                ),
                "latest_memory_sample": samples[-1].as_payload() if samples else None,
                "recent_event_count": len(self._events),
            }

    def recent_events(self, limit: int | None = None) -> list[dict[str, Any]]:
        self._prune_events()
        with self._lock:
            events = list(self._events)
        if limit is None:
            return events
        return events[-limit:]

    def memory_samples(self) -> list[dict[str, Any]]:
        with self._lock:
            return [sample.as_payload() for sample in self._memory_samples]

    def overlapping_intervals(
        self, start_monotonic: float, end_monotonic: float
    ) -> list[dict[str, Any]]:
        with self._lock:
            intervals = list(self._completed_intervals) + list(
                self._active_intervals.values()
            )
        overlapping = [
            interval.as_payload()
            for interval in intervals
            if interval.started_monotonic <= end_monotonic
            and (
                interval.ended_monotonic is None
                or interval.ended_monotonic >= start_monotonic
            )
        ]
        return overlapping[: self.config.max_active_intervals_in_event]

    async def _sample_loop(self) -> None:
        expected = self._clock() + self.config.sample_interval
        while True:
            await asyncio.sleep(self.config.sample_interval)
            now = self._clock()
            lag_seconds = max(0.0, now - expected)
            self._last_heartbeat_monotonic = now
            self.record_lag(lag_seconds, source="heartbeat")
            self.record_memory_sample()
            expected = now + self.config.sample_interval

    def _watchdog_loop(self) -> None:
        threshold = (self.config.event_loop_lag_threshold_ms or 0) / 1000
        poll_interval = min(self.config.sample_interval, max(0.05, threshold / 2))
        while not self._stop_event.wait(poll_interval):
            last = self._last_heartbeat_monotonic
            if last is None:
                continue
            lag_seconds = self._clock() - last - self.config.sample_interval
            if lag_seconds < threshold:
                continue
            stack_frames = None
            if self.config.capture_lag_stacks:
                stack_frames = self._capture_loop_stack()
            self.record_lag(lag_seconds, source="watchdog", stack_frames=stack_frames)

    def _capture_loop_stack(self) -> list[str]:
        if self._loop_thread_id is None:
            return []
        frame = sys._current_frames().get(self._loop_thread_id)
        if frame is None:
            return []
        formatted = traceback.format_stack(frame)
        return formatted[-self.config.lag_stack_max_frames :]

    def _check_memory_thresholds(self, sample: _MemorySample) -> None:
        threshold_mb = self.config.memory_rss_threshold_mb
        if threshold_mb is not None and sample.rss_mb >= threshold_mb:
            if self._can_emit_warning("memory_rss_threshold_exceeded"):
                active = self._active_intervals_payload()
                self._record_event(
                    "memory_rss_threshold_exceeded",
                    {
                        "rss_bytes": sample.rss_bytes,
                        "rss_mb": sample.rss_mb,
                        "threshold_mb": threshold_mb,
                        "sample": sample.as_payload(),
                        "active_interval_count": active["count"],
                        "active_intervals": active["items"],
                        "active_intervals_truncated": active["truncated"],
                    },
                    level=logging.WARNING,
                )

        growth_threshold_mb = self.config.memory_growth_threshold_mb
        if growth_threshold_mb is None:
            return
        window_start = sample.monotonic - self.config.memory_growth_window_seconds
        with self._lock:
            candidates = [
                s for s in self._memory_samples if s.monotonic >= window_start
            ]
        if not candidates:
            return
        baseline = candidates[0]
        growth_mb = sample.rss_mb - baseline.rss_mb
        if growth_mb < growth_threshold_mb:
            return
        if not self._can_emit_warning("memory_growth_threshold_exceeded"):
            return
        overlapping = self.overlapping_intervals(baseline.monotonic, sample.monotonic)
        self._record_event(
            "memory_growth_threshold_exceeded",
            {
                "growth_mb": round(growth_mb, 3),
                "threshold_mb": growth_threshold_mb,
                "window_seconds": self.config.memory_growth_window_seconds,
                "baseline_sample": baseline.as_payload(),
                "current_sample": sample.as_payload(),
                "overlapping_interval_count": len(overlapping),
                "overlapping_intervals": overlapping,
            },
            level=logging.WARNING,
        )

    def _record_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        level: int,
    ) -> None:
        event = {
            "event_type": event_type,
            "timestamp": self._timestamp(),
            "monotonic": self._clock(),
            "pid": self._pid,
            **payload,
        }
        self._prune_events()
        with self._lock:
            self._events.append(event)
            subscribers = list(self._subscribers)
        self._logger.log(
            level,
            event_type,
            extra={
                "pressure_event_type": event_type,
                "pressure_event": event,
            },
        )
        for subscriber in subscribers:
            subscriber(event)

    def _active_intervals_payload(self) -> dict[str, Any]:
        with self._lock:
            active = [
                interval.as_payload() for interval in self._active_intervals.values()
            ]
        cap = self.config.max_active_intervals_in_event
        return {
            "count": len(active),
            "items": active[:cap],
            "truncated": max(0, len(active) - cap),
        }

    def _can_emit_warning(self, event_type: str) -> bool:
        now = self._clock()
        with self._lock:
            last = self._last_warning_monotonic.get(event_type)
            if last is not None and now - last < self.config.warning_rate_limit_seconds:
                return False
            self._last_warning_monotonic[event_type] = now
        return True

    def _prune_events(self) -> None:
        ttl = self.config.event_ttl_seconds
        if ttl is None:
            return
        cutoff = self._clock() - ttl
        with self._lock:
            while self._events and self._events[0]["monotonic"] < cutoff:
                self._events.popleft()

    def _workflow_name_for_run(self, run_id: str) -> str | None:
        with self._lock:
            return self._run_workflow_names.get(run_id)

    def _finish_interval(
        self,
        interval: _ActivityInterval,
        *,
        now: float,
        reason: str,
        output_event_name: str | None = None,
    ) -> _ActivityInterval:
        return _ActivityInterval(
            run_id=interval.run_id,
            workflow_name=interval.workflow_name,
            step_name=interval.step_name,
            worker_id=interval.worker_id,
            input_event_name=interval.input_event_name,
            started_at=interval.started_at,
            started_monotonic=interval.started_monotonic,
            output_event_name=output_event_name,
            ended_at=self._timestamp(),
            ended_monotonic=now,
            duration_seconds=round(now - interval.started_monotonic, 6),
            finish_reason=reason,
        )

    def _timestamp(self) -> str:
        return self._wall_clock().isoformat()


class _PressureDiagnosticsInternalRunAdapter(BaseInternalRunAdapterDecorator):
    def __init__(
        self,
        decorated: InternalRunAdapter,
        recorder: PressureDiagnosticsRecorder,
    ) -> None:
        super().__init__(decorated)
        self._recorder = recorder

    @override
    async def write_to_event_stream(self, event: Event) -> None:
        if isinstance(event, StepStateChanged):
            self._recorder.record_step_state(self.run_id, event)
        await super().write_to_event_stream(event)

    @override
    async def close(self) -> None:
        try:
            await super().close()
        finally:
            self._recorder.record_run_end(self.run_id, reason="internal_adapter_closed")


class _PressureDiagnosticsExternalRunAdapter(BaseExternalRunAdapterDecorator):
    def __init__(
        self,
        decorated: ExternalRunAdapter,
        recorder: PressureDiagnosticsRecorder,
    ) -> None:
        super().__init__(decorated)
        self._recorder = recorder

    @override
    async def close(self) -> None:
        try:
            await super().close()
        finally:
            self._recorder.record_run_end(self.run_id, reason="external_adapter_closed")

    @override
    async def cancel(self) -> None:
        try:
            await super().cancel()
        finally:
            self._recorder.finish_run_intervals(self.run_id, reason="external_cancel")


class PressureDiagnosticsDecorator(BaseRuntimeDecorator):
    """Runtime decorator that records workflow activity for pressure diagnostics."""

    def __init__(
        self,
        decorated: Runtime,
        recorder: PressureDiagnosticsRecorder,
    ) -> None:
        super().__init__(decorated)
        self._recorder = recorder

    @override
    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> ExternalRunAdapter:
        self._recorder.record_run_start(run_id, workflow.workflow_name)
        adapter = super().run_workflow(
            run_id,
            workflow,
            init_state,
            start_event=start_event,
            serialized_state=serialized_state,
            serializer=serializer,
        )
        return _PressureDiagnosticsExternalRunAdapter(adapter, self._recorder)

    @override
    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        inner = self._decorated.get_internal_adapter(workflow)
        return _PressureDiagnosticsInternalRunAdapter(inner, self._recorder)
