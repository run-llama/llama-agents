# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Collection

import pytest
from llama_agents.core.iter_utils import (
    debounced_sorted_prefix,
    merge_generators,
)


async def _gen_with_delays(
    values: list[str], delays: list[float]
) -> AsyncGenerator[str, None]:
    for value, delay in zip(values, delays):
        if delay:
            await asyncio.sleep(delay)
        yield value


async def _gen_with_gates(
    values: list[str], gates: list[asyncio.Event]
) -> AsyncGenerator[str, None]:
    for value, gate in zip(values, gates):
        await gate.wait()
        yield value


async def _gen_raises(after_items: int = 0) -> AsyncGenerator[str, None]:
    produced = 0
    while produced < after_items:
        await asyncio.sleep(0)
        produced += 1
        yield f"ok-{produced}"
    raise RuntimeError("boom")


async def _gen_with_close_flag(
    flag: asyncio.Event, gate: asyncio.Event
) -> AsyncGenerator[str, None]:
    try:
        await gate.wait()
        yield "slow-1"
    finally:
        flag.set()


async def _wait_for_count(values: Collection[object], expected_count: int) -> None:
    for _ in range(100):
        if len(values) == expected_count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {expected_count} items, got {values}")


@pytest.mark.asyncio
async def test_merge_generators_interleaves_in_arrival_order() -> None:
    gate_a = asyncio.Event()
    gate_b = asyncio.Event()
    gate_c = asyncio.Event()
    gate_d = asyncio.Event()
    g1 = _gen_with_gates(["a", "c"], [gate_a, gate_c])
    g2 = _gen_with_gates(["b", "d"], [gate_b, gate_d])

    merged: list[str] = []

    async def collect() -> None:
        async for item in merge_generators(g1, g2):
            merged.append(item)

    task = asyncio.create_task(collect())
    gate_a.set()
    await _wait_for_count(merged, 1)
    gate_b.set()
    await _wait_for_count(merged, 2)
    gate_c.set()
    await _wait_for_count(merged, 3)
    gate_d.set()
    await task

    assert merged == ["a", "b", "c", "d"]


@pytest.mark.asyncio
async def test_merge_generators_propagates_exception_immediately() -> None:
    g_ok = _gen_with_delays(["x", "y"], [0.0, 0.1])
    g_err = _gen_raises(after_items=1)

    items: list[str] = []
    with pytest.raises(RuntimeError, match="boom"):
        async for item in merge_generators(g_ok, g_err):
            items.append(item)

    # We should have received only items produced before the error
    assert items[0] in {"x", "ok-1"}
    assert len(items) <= 2


@pytest.mark.asyncio
async def test_merge_generators_stop_on_first_completion_cancels_others() -> None:
    closed_event = asyncio.Event()
    slow_gate = asyncio.Event()
    fast_gate = asyncio.Event()
    g_slow = _gen_with_close_flag(closed_event, slow_gate)
    g_fast = _gen_with_gates(["done"], [fast_gate])

    collected: list[str] = []

    async def collect() -> None:
        async for item in merge_generators(
            g_slow, g_fast, stop_on_first_completion=True
        ):
            collected.append(item)

    task = asyncio.create_task(collect())
    fast_gate.set()
    await _wait_for_count(collected, 1)
    await task

    # Only the fast item should be seen deterministically
    assert collected == ["done"]
    # The slow generator should have been closed/cancelled
    assert closed_event.is_set()


@pytest.mark.asyncio
async def test_debounced_sorted_prefix_sorts_then_passthrough() -> None:
    async def inner() -> AsyncGenerator[int, None]:
        # Initial burst (unsorted order)
        for value in [3, 1, 2]:
            yield value
        # Wait longer than debounce to ensure the first flush occurs
        await asyncio.sleep(0.12)
        # Subsequent items should pass through in arrival order
        for value in [4, 5]:
            await asyncio.sleep(0.01)
            yield value

    output: list[int] = []
    async for item in debounced_sorted_prefix(
        inner(), key=lambda x: x, debounce_seconds=0.05, max_window_seconds=0.1
    ):
        output.append(item)

    # First three should be sorted, rest in order
    assert output == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_debounced_prefix_starts_with_first_log_entry() -> None:
    logs = _gen_with_delays(["later", "earlier"], [0.1, 0])
    result = [
        entry
        async for entry in debounced_sorted_prefix(
            logs, key=str, debounce_seconds=0.02, max_window_seconds=0.04
        )
    ]
    assert result == ["earlier", "later"]
