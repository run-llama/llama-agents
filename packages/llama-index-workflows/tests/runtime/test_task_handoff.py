# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from workflows.plugins.basic import AsyncioAdapterQueues, InternalAsyncioAdapter
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.named_task import PendingPull, PendingWorker, WorkerTask
from workflows.runtime.types.step_id import StepId
from workflows.workflow import Workflow

# Task.cancel() only propagates its message to awaiters on Python 3.11+.
_CANCEL_MESSAGE = "original cancellation" if sys.version_info >= (3, 11) else None


@pytest.fixture()
def adapter(workflow: Workflow) -> InternalAsyncioAdapter:
    return InternalAsyncioAdapter(
        AsyncioAdapterQueues("task-handoff", BrokerState.from_workflow(workflow))
    )


@pytest.fixture()
async def owned_tasks() -> AsyncGenerator[list[asyncio.Task[Any]], None]:
    tasks: list[asyncio.Task[Any]] = []
    yield tasks
    for task in tasks:
        task.cancel()
    if tasks:
        _, remaining = await asyncio.wait(tasks, timeout=0.1)
        for task in remaining:
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("kind", ["worker", "pull"])
async def test_cancel_before_handoff_cleans_started_tasks_only(
    adapter: InternalAsyncioAdapter,
    owned_tasks: list[asyncio.Task[Any]],
    kind: str,
) -> None:
    ready, finished, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def work() -> None:
        task = asyncio.current_task()
        assert task is not None
        owned_tasks.append(task)
        ready.set()
        try:
            await release.wait()
        finally:
            finished.set()

    running = asyncio.create_task(release.wait())
    owned_tasks.append(running)
    pending = (
        PendingWorker(StepId.root("work"), 0, work())
        if kind == "worker"
        else PendingPull(0, work())
    )
    waiter = asyncio.create_task(
        adapter.wait_for_next_task(
            [WorkerTask(StepId.root("existing"), 0, running)], [pending]
        )
    )
    owned_tasks.append(waiter)
    await ready.wait()
    waiter.cancel("original cancellation")
    with pytest.raises(asyncio.CancelledError, match=_CANCEL_MESSAGE):
        await waiter
    assert finished.is_set()
    assert not running.done()


async def test_timeout_hands_back_live_tasks(
    adapter: InternalAsyncioAdapter, owned_tasks: list[asyncio.Task[Any]]
) -> None:
    release = asyncio.Event()
    result = await adapter.wait_for_next_task(
        [], [PendingPull(0, release.wait())], timeout=0
    )
    assert result.completed is None
    assert len(result.started) == 1
    task = result.started[0].task
    owned_tasks.append(task)
    assert not task.done()
    release.set()
    assert await task is True


async def test_completed_task_is_handed_back(
    adapter: InternalAsyncioAdapter, owned_tasks: list[asyncio.Task[Any]]
) -> None:
    async def work() -> str:
        return "notebook"

    result = await adapter.wait_for_next_task(
        [], [PendingWorker(StepId.root("work"), 0, work())]
    )
    assert len(result.started) == 1
    owned_tasks.append(result.started[0].task)
    assert result.completed is result.started[0].task
    assert result.completed is not None
    assert result.completed.result() == "notebook"


async def test_repeated_cancellation_waits_for_cleanup(
    adapter: InternalAsyncioAdapter, owned_tasks: list[asyncio.Task[Any]]
) -> None:
    ready, cleaning, finish_cleanup, finished = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )

    async def work() -> None:
        task = asyncio.current_task()
        assert task is not None
        owned_tasks.append(task)
        ready.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await finish_cleanup.wait()
            finished.set()

    waiter = asyncio.create_task(
        adapter.wait_for_next_task([], [PendingPull(0, work())])
    )
    owned_tasks.append(waiter)
    await ready.wait()
    waiter.cancel("original cancellation")
    await asyncio.wait_for(cleaning.wait(), timeout=1)
    waiter.cancel("second cancellation")
    asyncio.get_running_loop().call_soon(finish_cleanup.set)
    with pytest.raises(asyncio.CancelledError, match=_CANCEL_MESSAGE):
        await waiter
    assert finished.is_set()


@pytest.mark.parametrize("raise_late", [False, True])
async def test_uncooperative_task_does_not_block_cancellation(
    adapter: InternalAsyncioAdapter,
    owned_tasks: list[asyncio.Task[Any]],
    caplog: pytest.LogCaptureFixture,
    raise_late: bool,
) -> None:
    ready, release = asyncio.Event(), asyncio.Event()
    child: asyncio.Task[Any] | None = None

    async def work() -> None:
        nonlocal child
        child = asyncio.current_task()
        assert child is not None
        owned_tasks.append(child)
        ready.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
            if raise_late:
                raise ValueError("late cleanup failure")

    waiter = asyncio.create_task(
        adapter.wait_for_next_task([], [PendingPull(0, work())])
    )
    owned_tasks.append(waiter)
    await ready.wait()
    waiter.cancel("original cancellation")
    done, _ = await asyncio.wait({waiter}, timeout=2)
    assert waiter in done
    with pytest.raises(asyncio.CancelledError, match=_CANCEL_MESSAGE):
        await waiter
    assert child is not None and not child.done()
    assert "outlived" in caplog.text
    release.set()
    await asyncio.gather(child, return_exceptions=True)
    if raise_late:
        assert "late cleanup failure" in caplog.text
