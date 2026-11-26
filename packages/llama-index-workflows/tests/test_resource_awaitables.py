# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Annotated, Literal

import pytest
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent
from workflows.resource import Resource, ResourceManager


@pytest.mark.parametrize("cache", [True, False])
@pytest.mark.parametrize("kind", ["coroutine", "future"])
async def test_factory_returning_awaitable(
    cache: bool, kind: Literal["coroutine", "future"]
) -> None:
    expected = object()
    calls = 0

    async def create_value() -> object:
        return expected

    def factory() -> Awaitable[object]:
        nonlocal calls
        calls += 1
        if kind == "coroutine":
            return create_value()
        future = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_soon(future.set_result, expected)
        return future

    manager = ResourceManager()
    resource = Resource(factory, cache=cache)

    assert await manager.get(resource) is expected
    assert await manager.get(resource) is expected
    assert calls == (1 if cache else 2)


async def test_awaitable_factory_error_is_not_cached() -> None:
    calls = 0

    async def create_value() -> str:
        if calls == 1:
            raise ValueError("factory failed")
        return "ready"

    def factory() -> Awaitable[str]:
        nonlocal calls
        calls += 1
        return create_value()

    manager = ResourceManager()
    resource = Resource(factory)

    with pytest.raises(ValueError, match="factory failed"):
        await manager.get(resource)

    assert await manager.get(resource) == "ready"
    assert await manager.get(resource) == "ready"
    assert calls == 2


async def test_workflow_awaits_decorated_async_resource() -> None:
    calls = 0

    def decorated(
        factory: Callable[[], Awaitable[str]],
    ) -> Callable[[], Awaitable[str]]:
        @wraps(factory)
        def wrapper() -> Awaitable[str]:
            nonlocal calls
            calls += 1
            return factory()

        return wrapper

    @decorated
    async def get_value() -> str:
        return "ready"

    class ResourceWorkflow(Workflow):
        @step
        async def start(
            self, ev: StartEvent, value: Annotated[str, Resource(get_value)]
        ) -> StopEvent:
            return StopEvent(result=value.upper())

    assert await ResourceWorkflow(timeout=3).run() == "READY"
    assert calls == 1
