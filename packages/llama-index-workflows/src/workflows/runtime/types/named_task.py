# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""NamedTask associates asyncio tasks with stable string keys for journaling."""

from __future__ import annotations

from asyncio import Task
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Coroutine

from workflows.runtime.types.step_id import StepId

# Key prefix for pull tasks
PULL_PREFIX = "__pull__"


@dataclass
class WorkerTask:
    """An asyncio worker task with structured identity."""

    step_id: StepId
    worker_id: int
    task: Task[Any]

    @property
    def key(self) -> str:
        return f"{self.step_id}:{self.worker_id}"


@dataclass
class PullTask:
    """An asyncio pull task with sequence identity."""

    sequence: int
    task: Task[Any]

    @property
    def key(self) -> str:
        return f"{PULL_PREFIX}:{self.sequence}"


NamedTask = WorkerTask | PullTask


def all_tasks(named_tasks: Sequence[NamedTask]) -> set[Task[Any]]:
    """Extract all tasks for use with asyncio.wait."""
    return {nt.task for nt in named_tasks}


def find_by_key(named_tasks: Sequence[NamedTask], key: str) -> Task[Any] | None:
    """Find a task by its key, returns None if not found."""
    for nt in named_tasks:
        if nt.key == key:
            return nt.task
    return None


def get_key(named_tasks: Sequence[NamedTask], task: Task[Any]) -> str:
    """Get the key for a task. Raises KeyError if not found."""
    for nt in named_tasks:
        if nt.task is task:
            return nt.key
    raise KeyError(f"Task {task} not found")


def pick_highest_priority(
    named_tasks: Sequence[NamedTask], done: set[Task[Any]]
) -> Task[Any] | None:
    """Return highest priority completed task from done set.

    Priority is determined by list order - tasks earlier in the list
    have higher priority. Workers should be listed before pull.

    Returns None if done is empty.
    Raises ValueError if done is non-empty but no tasks match (indicates bug).
    """
    if not done:
        return None
    for nt in named_tasks:
        if nt.task in done:
            return nt.task
    raise ValueError(
        f"No tasks in done set match named_tasks. "
        f"done={done}, named_tasks={[nt.key for nt in named_tasks]}"
    )


@dataclass
class PendingWorker:
    """A worker coroutine that hasn't been started yet."""

    step_id: StepId
    worker_id: int
    coro: Coroutine[Any, Any, Any]

    @property
    def key(self) -> str:
        return f"{self.step_id}:{self.worker_id}"

    def start(self, task: Task[Any]) -> WorkerTask:
        """Convert to a started WorkerTask."""
        return WorkerTask(self.step_id, self.worker_id, task)


@dataclass
class PendingPull:
    """A pull coroutine that hasn't been started yet."""

    sequence: int
    coro: Coroutine[Any, Any, Any]

    @property
    def key(self) -> str:
        return f"{PULL_PREFIX}:{self.sequence}"

    def start(self, task: Task[Any]) -> PullTask:
        """Convert to a started PullTask."""
        return PullTask(self.sequence, task)


PendingStart = PendingWorker | PendingPull
