# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Independent named collectors retain every input across concurrent reruns."""

from __future__ import annotations

import asyncio

import pytest
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class Item(Event):
    n: int


@pytest.mark.parametrize(
    ("workers", "count", "buffers"),
    [
        (1, 2, ("left",)),
        (2, 2, ("left",)),
        (1, 2, ("left", "right")),
        (2, 2, ("left", "right")),
        (2, 2, ("right", "left")),
        (3, 3, ("left", "right")),
        (3, 3, ("left", "middle", "right")),
    ],
)
async def test_named_collect_buffers_keep_all_inputs(
    workers: int, count: int, buffers: tuple[str, ...]
) -> None:
    started: set[int] = set()
    gate = asyncio.Event()

    class CollectWorkflow(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> Item | None:
            for n in range(count):
                ctx.send_event(Item(n=n))
            return None

        @step(num_workers=workers)
        async def gather(self, ctx: Context, ev: Item) -> StopEvent | None:
            started.add(ev.n)
            if len(started) >= min(workers, count):
                gate.set()
            await gate.wait()

            batches: dict[str, list[int] | None] = {}
            for name in buffers:
                batch = ctx.collect_events(ev, [Item] * count, buffer_id=name)
                batches[name] = (
                    sorted(item.n for item in batch) if batch is not None else None
                )
            if any(batch is not None for batch in batches.values()):
                return StopEvent(result=batches)
            return None

    handler = CollectWorkflow(timeout=3).run()
    async for _ in handler.stream_events():
        pass
    assert await handler == {name: list(range(count)) for name in buffers}
