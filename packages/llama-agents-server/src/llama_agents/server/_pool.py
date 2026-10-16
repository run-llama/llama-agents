# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import asyncpg


class PoolProvider:
    """Lazy asyncpg pool provider with explicit ownership semantics."""

    def __init__(
        self,
        factory: Callable[[], Awaitable[asyncpg.Pool]],
        *,
        owns_pool: bool,
    ) -> None:
        self._factory = factory
        self._owns_pool = owns_pool
        self._lock = asyncio.Lock()
        self._pool: asyncpg.Pool | None = None
        self._closed = False
        self._terminated = False

    @classmethod
    def create(cls, dsn: str, min_size: int, max_size: int) -> PoolProvider:
        async def factory() -> asyncpg.Pool:
            return await asyncpg.create_pool(
                dsn,
                min_size=min_size,
                max_size=max_size,
            )

        return cls(factory, owns_pool=True)

    @classmethod
    def borrowed(
        cls,
        factory: Callable[[], Awaitable[asyncpg.Pool]],
    ) -> PoolProvider:
        return cls(factory, owns_pool=False)

    async def get(self) -> asyncpg.Pool:
        self._raise_if_shutdown()
        if self._pool is not None:
            return self._pool

        async with self._lock:
            self._raise_if_shutdown()
            if self._pool is None:
                pool = await self._factory()
                if self._closed or self._terminated:
                    # Shutdown may have begun while the factory was suspended.
                    # Do not publish a late pool, even if close() was cancelled.
                    pool.terminate()
                    self._raise_if_shutdown()
                self._pool = pool
            return self._pool

    async def close(self) -> None:
        if not self._owns_pool:
            return
        self._closed = True
        # Serialize drain with initialization and other close callers. The flag
        # above rejects new gets before we wait for either operation to finish.
        async with self._lock:
            if self._terminated or self._pool is None:
                return
            try:
                await self._pool.close()
            except BaseException:
                self.terminate()
                raise
            self._pool = None

    def terminate(self) -> None:
        if not self._owns_pool:
            return
        if self._terminated:
            return
        self._terminated = True
        if self._pool is not None:
            self._pool.terminate()
            self._pool = None

    def _raise_if_shutdown(self) -> None:
        if self._terminated:
            raise RuntimeError("PoolProvider has been terminated.")
        if self._closed:
            raise RuntimeError("PoolProvider has been closed.")
