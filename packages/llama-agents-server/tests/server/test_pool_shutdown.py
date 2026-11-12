# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Literal, cast

import asyncpg
import pytest
from llama_agents.server._pool import PoolProvider
from llama_agents.server._store.postgres_workflow_store import PostgresWorkflowStore


@pytest.fixture
async def disconnected_pool() -> AsyncIterator[asyncpg.Pool]:
    # min_size=0 exercises real asyncpg ownership without opening a connection.
    pool = await asyncpg.create_pool(
        "postgresql://unused:unused@127.0.0.1:1/unused", min_size=0, max_size=1
    )
    try:
        yield pool
    finally:
        pool.terminate()


async def begin_close(provider: PoolProvider) -> asyncio.Task[None]:
    entered = asyncio.Event()

    async def close() -> None:
        entered.set()
        await provider.close()

    task = asyncio.create_task(close())
    await entered.wait()
    return task


@pytest.mark.parametrize("shutdown", ["close", "terminate"])
async def test_shutdown_rejects_and_disposes_late_pool(
    disconnected_pool: asyncpg.Pool, shutdown: Literal["close", "terminate"]
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def factory() -> asyncpg.Pool:
        started.set()
        await release.wait()
        return disconnected_pool

    provider = PoolProvider(factory, owns_pool=True)
    getter = asyncio.create_task(provider.get())
    await started.wait()
    closer = await begin_close(provider) if shutdown == "close" else None
    if shutdown == "terminate":
        provider.terminate()
    try:
        release.set()
        with pytest.raises(RuntimeError, match="closed|terminated"):
            await getter
        if closer is not None:
            await closer
        assert disconnected_pool.is_closing()
        with pytest.raises(asyncpg.InterfaceError, match="closed"):
            await disconnected_pool.acquire()
    finally:
        release.set()
        if closer is not None:
            await closer
        await asyncio.gather(getter, return_exceptions=True)


async def test_cancelled_close_during_initialization_still_disposes_late_pool(
    disconnected_pool: asyncpg.Pool,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def factory() -> asyncpg.Pool:
        started.set()
        await release.wait()
        return disconnected_pool

    provider = PoolProvider(factory, owns_pool=True)
    getter = asyncio.create_task(provider.get())
    await started.wait()
    closer = await begin_close(provider)
    try:
        closer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closer
    finally:
        release.set()
        await asyncio.gather(getter, return_exceptions=True)
    assert disconnected_pool.is_closing()
    with pytest.raises(RuntimeError, match="closed|terminated"):
        await provider.get()


class ClosingPool:
    """Gate the network-dependent drain of checked-out pool connections."""

    def __init__(self, *, fail: bool = False) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False
        self.close_calls = 0
        self.terminate_calls = 0
        self.fail = fail

    async def close(self) -> None:
        self.close_calls += 1
        self.started.set()
        await self.release.wait()
        if self.fail:
            raise ValueError("drain failed")
        self.closed = True

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.closed = True
        self.release.set()


async def owned_closing_pool(pool: ClosingPool) -> PoolProvider:
    async def factory() -> asyncpg.Pool:
        return cast(asyncpg.Pool, pool)

    provider = PoolProvider(factory, owns_pool=True)
    await provider.get()
    return provider


async def test_concurrent_close_waits_for_connection_drain() -> None:
    pool = ClosingPool()
    provider = await owned_closing_pool(pool)
    first = await begin_close(provider)
    await pool.started.wait()
    second = await begin_close(provider)
    try:
        assert not second.done()
        with pytest.raises(RuntimeError, match="closed"):
            await provider.get()
    finally:
        pool.release.set()
        await asyncio.gather(first, second)
    assert pool.closed
    assert pool.close_calls == 1
    assert pool.terminate_calls == 0


async def test_cancelling_connection_drain_terminates_pool() -> None:
    pool = ClosingPool()
    provider = await owned_closing_pool(pool)
    closer = await begin_close(provider)
    await pool.started.wait()
    closer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closer
    try:
        assert pool.closed
        await provider.close()
        assert pool.terminate_calls == 1
    finally:
        pool.terminate()


async def test_cancelling_second_close_does_not_cancel_connection_drain() -> None:
    pool = ClosingPool()
    provider = await owned_closing_pool(pool)
    first = await begin_close(provider)
    await pool.started.wait()
    second = await begin_close(provider)
    try:
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert not first.done()
        assert not pool.closed
    finally:
        pool.release.set()
        await asyncio.gather(first, second, return_exceptions=True)
    assert pool.closed
    assert pool.close_calls == 1
    assert pool.terminate_calls == 0


async def test_terminate_interrupts_in_progress_close() -> None:
    pool = ClosingPool()
    provider = await owned_closing_pool(pool)
    closer = await begin_close(provider)
    await pool.started.wait()
    try:
        provider.terminate()
        assert pool.closed
        await closer
        await provider.close()
        provider.terminate()
        assert pool.terminate_calls == 1
    finally:
        pool.release.set()
        await asyncio.gather(closer, return_exceptions=True)


async def test_failed_connection_drain_terminates_pool() -> None:
    pool = ClosingPool(fail=True)
    provider = await owned_closing_pool(pool)
    pool.release.set()
    with pytest.raises(ValueError, match="drain failed"):
        await provider.close()
    try:
        assert pool.closed
        await provider.close()
        assert pool.terminate_calls == 1
    finally:
        pool.terminate()


@pytest.mark.parametrize("shutdown", ["close", "terminate"])
async def test_borrowed_pool_survives_shutdown_during_initialization(
    disconnected_pool: asyncpg.Pool, shutdown: Literal["close", "terminate"]
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def factory() -> asyncpg.Pool:
        started.set()
        await release.wait()
        return disconnected_pool

    provider = PoolProvider.borrowed(factory)
    getter = asyncio.create_task(provider.get())
    await started.wait()
    if shutdown == "close":
        await provider.close()
    else:
        provider.terminate()
    release.set()
    assert await getter is disconnected_pool
    assert not disconnected_pool.is_closing()
    assert await provider.get() is disconnected_pool


async def test_store_cannot_publish_pool_initialized_after_close(
    monkeypatch: pytest.MonkeyPatch, disconnected_pool: asyncpg.Pool
) -> None:
    started, release, closing = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def create_pool(*args: object, **kwargs: object) -> asyncpg.Pool:
        started.set()
        await release.wait()
        return disconnected_pool

    async def setup_listener(self: PostgresWorkflowStore, pool: asyncpg.Pool) -> None:
        # LISTEN is the only remaining operation requiring a database server.
        pass

    monkeypatch.setattr(asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(PostgresWorkflowStore, "_setup_listener", setup_listener)
    store = PostgresWorkflowStore(dsn="postgresql://unused", auto_migrate=False)
    starter = asyncio.create_task(store.start())
    await started.wait()

    async def close() -> None:
        closing.set()
        await store.close()

    closer = asyncio.create_task(close())
    await closing.wait()
    try:
        release.set()
        with pytest.raises(RuntimeError, match="closed|terminated"):
            await starter
        await closer
        with pytest.raises(RuntimeError, match="not initialized"):
            store.create_state_store("after-close")
        assert disconnected_pool.is_closing()
    finally:
        release.set()
        await asyncio.gather(starter, closer, return_exceptions=True)


@pytest.mark.parametrize("shutdown", ["close", "terminate"])
async def test_shutdown_rejects_all_initialization_waiters(
    disconnected_pool: asyncpg.Pool, shutdown: Literal["close", "terminate"]
) -> None:
    started, release, waiting = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    async def factory() -> asyncpg.Pool:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return disconnected_pool

    provider = PoolProvider(factory, owns_pool=True)
    first = asyncio.create_task(provider.get())
    await started.wait()

    async def get() -> asyncpg.Pool:
        waiting.set()
        return await provider.get()

    second = asyncio.create_task(get())
    await waiting.wait()
    closer = await begin_close(provider) if shutdown == "close" else None
    if shutdown == "terminate":
        provider.terminate()
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    if closer is not None:
        await closer
    assert all(isinstance(result, RuntimeError) for result in results)
    assert disconnected_pool.is_closing()
    assert calls == 1
