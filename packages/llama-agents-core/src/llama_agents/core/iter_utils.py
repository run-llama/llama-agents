"""Iterator utilities for buffering, sorting, and debouncing streams."""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncGenerator, Callable, TypeVar, cast

from typing_extensions import Literal

T = TypeVar("T")


async def debounced_sorted_prefix(
    inner: AsyncGenerator[T, None],
    *,
    key: Callable[[T], Any],
    debounce_seconds: float = 0.1,
    max_window_seconds: float = 0.1,
) -> AsyncGenerator[T, None]:
    """Yield a stream where the initial burst is sorted, then passthrough.

    Behavior:
    - Buffer early items and sort them by the provided key.
    - Flush the buffer when either:
      - No new item arrives for `debounce_seconds`, or
      - `max_window_seconds` elapses from the first buffered item, or
    - After the first flush, subsequent items are yielded passthrough, in arrival order.

    This async variant uses an asyncio.Queue and a background task to pump `inner`.
    """

    try:
        first = await anext(inner)
    except StopAsyncIteration:
        return

    buffer: list[T] = [first]
    debouncer = Debouncer(debounce_seconds, max_window_seconds)
    merged = merge_generators(inner, debouncer.aiter())

    async for item in merged:
        if item == "__COMPLETE__":
            buffer.sort(key=key)
            for buffered_item in buffer:
                yield buffered_item
            buffer = []
        else:
            # item is T after checking != "__COMPLETE__"
            actual_item = cast(T, item)
            if debouncer.is_complete:
                yield actual_item
            else:
                debouncer.extend_window()
                buffer.append(actual_item)


COMPLETE = Literal["__COMPLETE__"]


async def merge_generators(
    *generators: AsyncGenerator[T, None],
    stop_on_first_completion: bool = False,
) -> AsyncGenerator[T, None]:
    """
    Merge multiple async iterators into a single async iterator, yielding items as
    soon as any source produces them.

    - If stop_on_first_completion is False (default), continues until all inputs are exhausted.
    - If stop_on_first_completion is True, stops as soon as any input completes.
    - Propagates exceptions from any input immediately.
    """
    if not generators:
        return

    active_generators: dict[int, AsyncGenerator[T, None]] = {
        index: generator for index, generator in enumerate(generators)
    }

    next_item_tasks: dict[int, asyncio.Task[T]] = {}
    exception_to_raise: BaseException | None = None
    stopped_on_first_completion = False

    # Prime one pending task per generator to maintain fairness
    for index, generator in active_generators.items():
        next_item_tasks[index] = asyncio.create_task(anext(generator))

    try:
        while next_item_tasks and exception_to_raise is None:
            done, _ = await asyncio.wait(
                set(next_item_tasks.values()),
                return_when=asyncio.FIRST_COMPLETED,
            )

            completed_results: list[tuple[int, T]] = []
            for finished in done:
                # Locate which generator this task belonged to
                task_index: int | None = None
                for index, task in next_item_tasks.items():
                    if task is finished:
                        task_index = index
                        break

                if task_index is None:
                    # Should not happen, but continue defensively
                    continue

                try:
                    value = finished.result()
                except StopAsyncIteration:
                    # Generator exhausted
                    if stop_on_first_completion:
                        stopped_on_first_completion = True
                        # Break out of the inner loop; the outer loop will
                        # observe the stop flag and exit to the finally block
                        # where pending tasks are cancelled and generators closed.
                        break
                    else:
                        next_item_tasks.pop(task_index, None)
                        active_generators.pop(task_index, None)
                        continue
                except Exception as exc:  # noqa: BLE001 - propagate specific generator error
                    exception_to_raise = exc
                    break
                else:
                    completed_results.append((task_index, value))
            if stopped_on_first_completion:
                break
            for task_index, value in completed_results:
                # Remove the finished task before yielding
                next_item_tasks.pop(task_index, None)
                yield value
                # Schedule the next item fetch for this generator
                active_gen: AsyncGenerator[T, None] | None = active_generators.get(
                    task_index
                )
                if active_gen is not None:
                    next_item_tasks[task_index] = asyncio.create_task(anext(active_gen))
    finally:
        # Ensure we do not leak tasks or open generators
        for task in next_item_tasks.values():
            task.cancel()
        if next_item_tasks:
            try:
                await asyncio.gather(*next_item_tasks.values(), return_exceptions=True)
            except Exception:
                pass
        for gen in active_generators.values():
            try:
                await gen.aclose()
            except Exception:
                pass

    if exception_to_raise is not None:
        raise exception_to_raise
    if stopped_on_first_completion:
        return


class Debouncer:
    """
    Continually extends a complete time while extend is called, up to a max window.
    Exposes methods that notify on completion
    """

    def __init__(
        self,
        debounce_seconds: float = 0.1,
        max_window_seconds: float = 1,
        get_time: Callable[[], float] = time.monotonic,
    ):
        self.debounce_seconds = debounce_seconds
        self.max_window_seconds = max_window_seconds
        self.complete_signal = asyncio.Event()
        self.get_time = get_time
        self.start_time = self.get_time()
        self.complete_time = self.start_time + self.debounce_seconds
        self.max_complete_time = self.start_time + self.max_window_seconds
        asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while not self.complete_signal.is_set():
            now = self.get_time()
            remaining = min(self.complete_time, self.max_complete_time) - now
            if remaining <= 0:
                self.complete_signal.set()
            else:
                await asyncio.sleep(remaining)

    @property
    def is_complete(self) -> bool:
        return self.complete_signal.is_set()

    def extend_window(self) -> None:
        """Mark a new item has arrived, extending the debounce window."""
        now = self.get_time()
        self.complete_time = now + self.debounce_seconds

    async def wait(self) -> None:
        """Wait for the debounce window to expire, or the max window to elapse."""
        await self.complete_signal.wait()

    async def aiter(self) -> AsyncGenerator[COMPLETE, None]:
        """Yield a stream that emits an element when the wait event occurs."""
        await self.wait()
        yield "__COMPLETE__"
