# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import anyio
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send
from typing_extensions import override


class EventStreamingResponse(StreamingResponse):
    """Own a workflow subscription through response completion or disconnect."""

    def __init__(
        self,
        source: AsyncGenerator[tuple[int, EventEnvelopeWithMetadata], None],
        *,
        sse: bool,
        heartbeat_interval: float | None,
    ) -> None:
        self._stream = self._format_stream(
            source, sse=sse, heartbeat_interval=heartbeat_interval if sse else None
        )
        super().__init__(
            self._stream,
            media_type="text/event-stream" if sse else "application/x-ndjson",
        )

    @override
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            # A disconnect during send leaves the body suspended at yield.
            # Close it outside Starlette's disconnect-listener cancel scope.
            with anyio.CancelScope(shield=True):
                await self._stream.aclose()

    @staticmethod
    async def _format_stream(
        source: AsyncGenerator[tuple[int, EventEnvelopeWithMetadata], None],
        *,
        sse: bool,
        heartbeat_interval: float | None,
    ) -> AsyncGenerator[str, None]:
        # One producer owns all reads and source cleanup, including context
        # managers held across yields by a custom store subscription.
        queue: asyncio.Queue[tuple[int, EventEnvelopeWithMetadata]] = asyncio.Queue(
            maxsize=1
        )

        async def feed() -> None:
            try:
                async for item in source:
                    await queue.put(item)
            finally:
                with anyio.CancelScope(shield=True):
                    await source.aclose()

        producer = asyncio.create_task(feed())
        pending: asyncio.Task[tuple[int, EventEnvelopeWithMetadata]] | None = None
        try:
            while True:
                if pending is None:
                    pending = asyncio.create_task(queue.get())
                # Do not cancel either task on a heartbeat timeout. Watching the
                # producer also makes exhaustion and errors observable to readers.
                await asyncio.wait(
                    {pending, producer},
                    timeout=heartbeat_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if pending.done():
                    sequence, envelope = pending.result()
                elif producer.done():
                    if queue.empty():
                        producer.result()
                        return
                    # Drain a buffered event before surfacing a producer error.
                    sequence, envelope = await pending
                else:
                    yield ": heartbeat\n\n"
                    continue
                pending = None
                payload = envelope.model_dump_json()
                if sse:
                    yield f"id: {sequence}\ndata: {payload}\n\n"
                else:
                    yield f"{payload}\n"
        finally:
            # ASGI <2.4 disconnects use level cancellation. Shield teardown so
            # repeated cancellation cannot interrupt the subscription's finally.
            with anyio.CancelScope(shield=True):
                if pending is not None:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)
