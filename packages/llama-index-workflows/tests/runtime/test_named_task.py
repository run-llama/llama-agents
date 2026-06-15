# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Tests for NamedTask (WorkerTask | PullTask).

NamedTask associates asyncio tasks with stable string keys, providing:
- Task identification for DBOS journaling
- Task lookup by key for replay scenarios
- Priority-based task selection (by list order)
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from workflows.runtime.types.named_task import (
    PULL_PREFIX,
    PullTask,
    WorkerTask,
    all_tasks,
    find_by_key,
    get_key,
    pick_highest_priority,
)
from workflows.runtime.types.step_id import StepId


async def _never_completes() -> None:
    """Coroutine that never completes, for creating pending tasks."""
    await asyncio.Future()


def create_pending_task() -> asyncio.Task[Any]:
    """Create a pending task that never completes."""
    return asyncio.create_task(_never_completes())


# --- NamedTask creation ---


async def test_worker_task_creates_correct_key() -> None:
    """WorkerTask should create key as 'step_name:worker_id'."""
    task = create_pending_task()
    try:
        nt = WorkerTask(StepId.root("my_step"), 42, task)
        assert nt.key == "my_step:42"
        assert nt.task is task
        assert isinstance(nt, WorkerTask)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_pull_task_creates_correct_key() -> None:
    """PullTask should create key as '__pull__:sequence'."""
    task = create_pending_task()
    try:
        nt = PullTask(7, task)
        assert nt.key == f"{PULL_PREFIX}:7"
        assert nt.task is task
        assert isinstance(nt, PullTask)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --- all_tasks ---


async def test_all_tasks_returns_set_of_tasks() -> None:
    """all_tasks should return a set of all tasks."""
    w1 = create_pending_task()
    w2 = create_pending_task()
    pull = create_pending_task()
    try:
        named_tasks = [
            WorkerTask(StepId.root("step_a"), 0, w1),
            WorkerTask(StepId.root("step_b"), 0, w2),
            PullTask(0, pull),
        ]
        result = all_tasks(named_tasks)
        assert len(result) == 3
        assert w1 in result
        assert w2 in result
        assert pull in result
    finally:
        for t in [w1, w2, pull]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


async def test_all_tasks_empty_list() -> None:
    """all_tasks should return empty set for empty list."""
    assert all_tasks([]) == set()


async def test_all_tasks_works_with_asyncio_wait() -> None:
    """all_tasks result should work with asyncio.wait."""
    task = create_pending_task()
    try:
        named_tasks = [WorkerTask(StepId.root("step"), 0, task)]
        result = all_tasks(named_tasks)
        done, pending = await asyncio.wait(result, timeout=0.001)
        assert task in pending
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --- find_by_key ---


async def test_find_by_key_returns_worker_task() -> None:
    """find_by_key should return the correct worker task."""
    w1 = create_pending_task()
    w2 = create_pending_task()
    try:
        named_tasks = [
            WorkerTask(StepId.root("step_a"), 0, w1),
            WorkerTask(StepId.root("step_b"), 1, w2),
        ]
        assert find_by_key(named_tasks, "step_a:0") is w1
        assert find_by_key(named_tasks, "step_b:1") is w2
    finally:
        for t in [w1, w2]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


async def test_find_by_key_returns_pull_task() -> None:
    """find_by_key should return the pull task."""
    pull = create_pending_task()
    try:
        named_tasks = [PullTask(5, pull)]
        assert find_by_key(named_tasks, f"{PULL_PREFIX}:5") is pull
    finally:
        pull.cancel()
        try:
            await pull
        except asyncio.CancelledError:
            pass


async def test_find_by_key_returns_none_for_unknown() -> None:
    """find_by_key should return None for unknown key."""
    task = create_pending_task()
    try:
        named_tasks = [WorkerTask(StepId.root("step"), 0, task)]
        assert find_by_key(named_tasks, "unknown:99") is None
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_find_by_key_empty_list() -> None:
    """find_by_key should return None for empty list."""
    assert find_by_key([], "any:key") is None


# --- get_key ---


async def test_get_key_returns_worker_key() -> None:
    """get_key should return the key for a worker task."""
    task = create_pending_task()
    try:
        named_tasks = [WorkerTask(StepId.root("my_step"), 3, task)]
        assert get_key(named_tasks, task) == "my_step:3"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_get_key_returns_pull_key() -> None:
    """get_key should return the key for a pull task."""
    task = create_pending_task()
    try:
        named_tasks = [PullTask(2, task)]
        assert get_key(named_tasks, task) == f"{PULL_PREFIX}:2"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_get_key_raises_for_unknown_task() -> None:
    """get_key should raise KeyError for unknown task."""
    known = create_pending_task()
    unknown = create_pending_task()
    try:
        named_tasks = [WorkerTask(StepId.root("step"), 0, known)]
        with pytest.raises(KeyError):
            get_key(named_tasks, unknown)
    finally:
        for t in [known, unknown]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


# --- get_key / find_by_key round trip ---


async def test_round_trip_worker() -> None:
    """get_key and find_by_key should be inverses for workers."""
    task = create_pending_task()
    try:
        named_tasks = [WorkerTask(StepId.root("step"), 5, task)]
        key = get_key(named_tasks, task)
        found = find_by_key(named_tasks, key)
        assert found is task
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_round_trip_pull() -> None:
    """get_key and find_by_key should be inverses for pull."""
    task = create_pending_task()
    try:
        named_tasks = [PullTask(9, task)]
        key = get_key(named_tasks, task)
        found = find_by_key(named_tasks, key)
        assert found is task
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --- pick_highest_priority ---


async def test_pick_highest_priority_respects_list_order() -> None:
    """pick_highest_priority should return first completed task in list order."""
    t1 = create_pending_task()
    t2 = create_pending_task()
    t3 = create_pending_task()
    try:
        named_tasks = [
            WorkerTask(StepId.root("step_b"), 0, t2),
            WorkerTask(StepId.root("step_a"), 0, t1),
            PullTask(0, t3),
        ]
        done = {t2, t3}
        result = pick_highest_priority(named_tasks, done)
        assert result is t2
    finally:
        for t in [t1, t2, t3]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


async def test_pick_highest_priority_workers_before_pull() -> None:
    """Workers listed first should have priority over pull."""
    worker = create_pending_task()
    pull = create_pending_task()
    try:
        named_tasks = [
            WorkerTask(StepId.root("step"), 0, worker),
            PullTask(0, pull),
        ]
        done = {worker, pull}
        result = pick_highest_priority(named_tasks, done)
        assert result is worker
    finally:
        for t in [worker, pull]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


async def test_pick_highest_priority_returns_pull_when_only_pull_done() -> None:
    """Should return pull if it's the only completed task."""
    worker = create_pending_task()
    pull = create_pending_task()
    try:
        named_tasks = [
            WorkerTask(StepId.root("step"), 0, worker),
            PullTask(0, pull),
        ]
        done = {pull}
        result = pick_highest_priority(named_tasks, done)
        assert result is pull
    finally:
        for t in [worker, pull]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


async def test_pick_highest_priority_empty_done() -> None:
    """Should return None when done set is empty."""
    task = create_pending_task()
    try:
        named_tasks = [WorkerTask(StepId.root("step"), 0, task)]
        result = pick_highest_priority(named_tasks, set())
        assert result is None
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_pick_highest_priority_no_match_raises() -> None:
    """Should raise ValueError when done is non-empty but no tasks match."""
    task1 = create_pending_task()
    task2 = create_pending_task()
    try:
        named_tasks = [WorkerTask(StepId.root("step"), 0, task1)]
        done = {task2}
        with pytest.raises(ValueError, match="No tasks in done set match"):
            pick_highest_priority(named_tasks, done)
    finally:
        for t in [task1, task2]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


# --- Integration ---


async def test_integration_with_asyncio_wait() -> None:
    """Full integration: create tasks, wait, pick priority, get key."""

    async def quick() -> str:
        return "done"

    worker = asyncio.create_task(quick())
    pull = create_pending_task()
    try:
        named_tasks = [
            WorkerTask(StepId.root("fast_step"), 0, worker),
            PullTask(0, pull),
        ]

        tasks = all_tasks(named_tasks)
        done, _ = await asyncio.wait(tasks, timeout=1.0)

        assert worker in done
        completed = pick_highest_priority(named_tasks, done)
        assert completed is not None
        assert completed is worker

        key = get_key(named_tasks, completed)
        assert key == "fast_step:0"
    finally:
        pull.cancel()
        try:
            await pull
        except asyncio.CancelledError:
            pass


async def test_multiple_workers_same_step() -> None:
    """Should handle multiple workers for the same step (num_workers > 1)."""
    w0 = create_pending_task()
    w1 = create_pending_task()
    try:
        named_tasks = [
            WorkerTask(StepId.root("parallel_step"), 0, w0),
            WorkerTask(StepId.root("parallel_step"), 1, w1),
        ]

        assert find_by_key(named_tasks, "parallel_step:0") is w0
        assert find_by_key(named_tasks, "parallel_step:1") is w1
        assert get_key(named_tasks, w0) == "parallel_step:0"
        assert get_key(named_tasks, w1) == "parallel_step:1"
    finally:
        for t in [w0, w1]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
