"""Internal context - for step execution within workflows."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Coroutine, Generic, TypeVar, cast

from workflows.context.context_types import MODEL_T
from workflows.context.state_store import StateStore
from workflows.errors import WorkflowRuntimeError
from workflows.retry_policy import RetryInfo
from workflows.runtime.types.results import (
    AddCollectedEvent,
    AddWaiter,
    DeleteCollectedEvent,
    DeleteWaiter,
    StepWorkerContext,
    StepWorkerStateContextVar,
    WaitingForEvent,
)
from workflows.runtime.types.ticks import TickAddEvent

if TYPE_CHECKING:
    from workflows.events import Event
    from workflows.runtime.types.plugin import InternalRunAdapter
    from workflows.workflow import Workflow

T = TypeVar("T", bound="Event")

logger = logging.getLogger(__name__)


class InternalContext(Generic[MODEL_T]):
    """Context for code running inside workflow step functions.

    Provides access to state store, event collection, waiting for events,
    and publishing to the event stream.
    """

    _internal_adapter: InternalRunAdapter
    _workflow: Workflow
    _workers: list[asyncio.Task[Any]]

    def __init__(
        self,
        internal_adapter: InternalRunAdapter,
        workflow: Workflow,
    ) -> None:
        self._internal_adapter = internal_adapter
        self._workflow = workflow
        self._workers = []

    def _execute_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Execute a coroutine as a tracked background task."""
        task = asyncio.create_task(coro)
        self._workers.append(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            try:
                self._workers.remove(t)
            except ValueError:
                # Task was already cleared during shutdown or cleanup.
                pass
            # Log exceptions from fire-and-forget tasks (cancelled is not an error)
            if not t.cancelled():
                exc = t.exception()
                if exc is not None:
                    logger.error(
                        "Background task failed with exception",
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )

        task.add_done_callback(_on_done)
        return task

    def cancel_background_tasks(self) -> None:
        """Cancel all tracked background tasks."""
        for worker in self._workers:
            worker.cancel()
        self._workers.clear()

    async def _finalize_step(self) -> None:
        """Await all background tasks and finalize the step.

        Called after a step function completes to ensure all fire-and-forget
        operations (e.g., write_event_to_stream, send_event) complete before
        returning control to the control loop. This prevents non-deterministic
        ordering of durable operations on replay.
        """
        workers = self._workers[:]
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        await self._internal_adapter.finalize_step()

    @staticmethod
    def _get_step_ctx(fn: str) -> StepWorkerContext:
        """Get the current step worker context. Raises if not in a step."""
        try:
            return StepWorkerStateContextVar.get()
        except LookupError:
            raise WorkflowRuntimeError(
                f"{fn} may only be called from within a step function"
            )

    @property
    def store(self) -> StateStore[MODEL_T]:
        """Access workflow state store."""
        state_store = self._internal_adapter.get_state_store()
        if state_store is None:
            raise RuntimeError("State store not available from adapter")
        return state_store  # type: ignore[return-value]

    def collect_events(
        self,
        ev: Event,
        expected: list[type[Event]],
        buffer_id: str | None = None,
    ) -> list[Event] | None:
        """Collect events until all expected types are received."""
        step_ctx = self._get_step_ctx(fn="collect_events")

        # If no events are expected, return an empty list immediately
        if not expected:
            return []

        buffer_id = buffer_id or "default"
        collected_events = step_ctx.state.collected_events.get(buffer_id, [])

        remaining_event_types = Counter(expected) - Counter(
            [type(e) for e in collected_events]
        )

        if remaining_event_types != Counter([type(ev)]):
            if type(ev) in remaining_event_types:
                step_ctx.returns.return_values.append(
                    AddCollectedEvent(event_id=buffer_id, event=ev)
                )
            return None

        total = []
        by_type: dict[type[Event], list[Event]] = defaultdict(list)
        for e in collected_events + [ev]:
            by_type[type(e)].append(e)
        # order by expected type
        for e_type in expected:
            total.append(by_type[e_type].pop(0))
        # Clear the collected events when the step is complete
        step_ctx.returns.return_values.append(DeleteCollectedEvent(event_id=buffer_id))
        return total

    def send_event(self, message: Event, step: str | None = None) -> None:
        """Send an event to trigger another step."""
        if step is not None:
            self._workflow._validate_valid_step_message(step, message)

        recovery_counts: dict[str, int] = {}
        try:
            recovery_counts = dict(
                StepWorkerStateContextVar.get().retry.recovery_counts
            )
        except LookupError:
            pass

        self._execute_task(
            self._internal_adapter.send_event(
                TickAddEvent(
                    event=message,
                    step_name=step,
                    recovery_counts=recovery_counts,
                )
            )
        )

    async def wait_for_event(
        self,
        event_type: type[T],
        waiter_event: Event | None = None,
        waiter_id: str | None = None,
        requirements: dict[str, Any] | None = None,
        timeout: float | None = 2000,
    ) -> T:
        """Wait for an event of the specified type."""
        step_ctx = self._get_step_ctx(fn="wait_for_event")

        collected_waiters = step_ctx.state.collected_waiters
        requirements = requirements or {}

        # Generate a unique key for the waiter
        event_str = f"{event_type.__module__}.{event_type.__name__}"
        requirements_str = str(requirements)
        waiter_id = waiter_id or f"waiter_{event_str}_{requirements_str}"

        waiter = next((w for w in collected_waiters if w.waiter_id == waiter_id), None)
        if waiter is not None and waiter.timed_out:
            step_ctx.returns.return_values.append(DeleteWaiter(waiter_id=waiter_id))
            raise asyncio.TimeoutError(f"Timed out waiting for {event_type.__name__}")
        if waiter is None or waiter.resolved_event is None:
            raise WaitingForEvent(
                AddWaiter(
                    waiter_id=waiter_id,
                    requirements=requirements,
                    timeout=timeout,
                    event_type=event_type,
                    waiter_event=waiter_event,
                )
            )
        else:
            step_ctx.returns.return_values.append(DeleteWaiter(waiter_id=waiter_id))
            return cast(T, waiter.resolved_event)

    def write_event_to_stream(self, ev: Event | None) -> None:
        """Write an event to the published event stream."""
        if ev is not None:
            self._execute_task(self._internal_adapter.write_to_event_stream(ev))

    def retry_info(self) -> RetryInfo:
        """Snapshot of the currently-executing step's retry state.

        Returns a `RetryInfo(retry_number=0, elapsed_seconds=0.0,
        last_exception=None, last_failed_at=None)` on the first attempt. After a
        retry it reflects the current retry number, seconds since the first
        attempt, and the most recent failure.

        Raises:
            WorkflowRuntimeError: If called outside of a step function.
        """
        step_ctx = self._get_step_ctx(fn="retry_info")
        retry = step_ctx.retry
        if retry.retry_number <= 0 or not retry.first_attempt_at:
            elapsed = 0.0
        else:
            elapsed = max(0.0, time.time() - retry.first_attempt_at)
        last_failed_at: datetime | None = (
            datetime.fromtimestamp(retry.last_failed_at, tz=timezone.utc)
            if retry.last_failed_at is not None
            else None
        )
        return RetryInfo(
            retry_number=retry.retry_number,
            elapsed_seconds=elapsed,
            last_exception=retry.last_exception,
            last_failed_at=last_failed_at,
        )
