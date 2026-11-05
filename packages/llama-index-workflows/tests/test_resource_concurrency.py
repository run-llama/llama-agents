# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from typing import Annotated

import pytest
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.resource import Resource, ResourceManager


@pytest.mark.asyncio
@pytest.mark.parametrize("cache", [True, False])
@pytest.mark.parametrize("in_flight", [True, False])
async def test_concurrent_step_resources(cache: bool, in_flight: bool) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    created: list[object] = []
    received: list[object] = []

    async def get_resource() -> object:
        value = object()
        created.append(value)
        if in_flight and len(created) == 1:
            started.set()
            await release.wait()
        return value

    async def hold_first_scope() -> bool:
        if not in_flight:
            started.set()
            await release.wait()
        return True

    async def release_first_scope() -> bool:
        await started.wait()
        release.set()
        return True

    class Done(Event):
        pass

    class ConcurrentResources(Workflow):
        @step
        async def first(
            self,
            ev: StartEvent,
            value: Annotated[object, Resource(get_resource, cache=cache)],
            ready: Annotated[bool, Resource(hold_first_scope)],
        ) -> Done:
            received.append(value)
            return Done()

        @step
        async def second(
            self,
            ev: StartEvent,
            ready: Annotated[bool, Resource(release_first_scope)],
            value: Annotated[object, Resource(get_resource, cache=cache)],
        ) -> Done:
            received.append(value)
            return Done()

        @step
        async def finish(self, ctx: Context, ev: Done) -> StopEvent | None:
            if ctx.collect_events(ev, [Done, Done]) is None:
                return None
            return StopEvent()

    await ConcurrentResources().run()

    assert len(received) == 2
    assert len(created) == (1 if cache else 2)
    assert (received[0] is received[1]) is cache


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_owner", [True, False])
async def test_failed_resource_initialization_can_be_retried(
    cancel_owner: bool,
) -> None:
    manager = ResourceManager()
    started = asyncio.Event()
    waiting = asyncio.Event()
    release = asyncio.Event()
    value = object()
    calls = 0

    async def factory() -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            raise ValueError("factory failed")
        return value

    resource = Resource(factory)

    async def wait_for_resource() -> object:
        waiting.set()
        return await manager.get(resource)

    owner = asyncio.create_task(manager.get(resource))
    await started.wait()
    waiter = asyncio.create_task(wait_for_resource())
    await waiting.wait()
    if cancel_owner:
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
    else:
        release.set()
        with pytest.raises(ValueError, match="factory failed"):
            await owner

    assert await waiter is value
    assert await manager.get(resource) is value
    assert calls == 2


@pytest.mark.asyncio
async def test_cancelled_resource_waiter_does_not_cancel_factory() -> None:
    manager = ResourceManager()
    started = asyncio.Event()
    waiting = asyncio.Event()
    release = asyncio.Event()
    value = object()
    calls = 0

    async def factory() -> object:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return value

    resource = Resource(factory)

    async def wait_for_resource() -> object:
        waiting.set()
        return await manager.get(resource)

    owner = asyncio.create_task(manager.get(resource))
    await started.wait()
    waiter = asyncio.create_task(wait_for_resource())
    await waiting.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    release.set()

    assert await owner is value
    assert await manager.get(resource) is value
    assert calls == 1


@pytest.mark.asyncio
async def test_resource_cycle_across_workers_raises_instead_of_deadlocking() -> None:
    manager = ResourceManager()
    first_started = asyncio.Event()
    second_started = asyncio.Event()

    async def ready_first() -> None:
        first_started.set()
        await second_started.wait()

    async def ready_second() -> None:
        second_started.set()
        await first_started.wait()

    async def first(
        ready: Annotated[None, Resource(ready_first, cache=False)], second: object
    ) -> object:
        return object()

    async def second(
        ready: Annotated[None, Resource(ready_second, cache=False)], first: object
    ) -> object:
        return object()

    first_resource = Resource(first)
    second_resource = Resource(second)
    first.__annotations__["second"] = Annotated[object, second_resource]
    second.__annotations__["first"] = Annotated[object, first_resource]

    results = await asyncio.wait_for(
        asyncio.gather(
            manager.get(first_resource),
            manager.get(second_resource),
            return_exceptions=True,
        ),
        timeout=1,
    )
    for result in results:
        assert isinstance(result, ValueError)
        assert "Circular resource dependency detected" in str(result)
