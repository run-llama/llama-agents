# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Phase 5: child streams are tagged by namespace origin and filtered out of the
parent stream by default.

A parent consumer calling ``handler.stream_events()`` sees only root-origin
events; ``stream_events(include_children=True)`` surfaces child events tagged
with the namespace path of the child execution that produced them.
"""

from __future__ import annotations

import pytest
from workflows import Context, Workflow
from workflows.decorators import step
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
    get_event_origin_namespace,
)


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    pass


class ChildPing(Event):
    msg: str = ""


class ParentPing(Event):
    msg: str = ""


class StreamChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: ChildStart) -> ChildStop:
        ctx.write_event_to_stream(ChildPing(msg="from-child"))
        return ChildStop()


class StreamParent(Workflow):
    child: StreamChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        ctx.write_event_to_stream(ParentPing(msg="from-parent"))
        return ChildStart()

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> StopEvent:
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_child_events_hidden_from_parent_stream_by_default() -> None:
    """The default stream is backward compatible: a parent consumer sees its own
    streamed events but none published from inside the child."""
    handler = StreamParent(child=StreamChild()).run()
    collected: list[Event] = []
    async for ev in handler.stream_events():
        collected.append(ev)
    await handler

    assert any(isinstance(ev, ParentPing) for ev in collected)
    assert not any(isinstance(ev, ChildPing) for ev in collected)
    # Every surfaced event is root-origin.
    assert all(get_event_origin_namespace(ev) == () for ev in collected)


@pytest.mark.asyncio
async def test_child_events_surfaced_tagged_with_include_children() -> None:
    """Opt-in surfaces child events, tagged with the child's namespace path."""
    handler = StreamParent(child=StreamChild()).run()
    collected: list[Event] = []
    async for ev in handler.stream_events(include_children=True):
        collected.append(ev)
    await handler

    parent_ping = next(ev for ev in collected if isinstance(ev, ParentPing))
    child_ping = next(ev for ev in collected if isinstance(ev, ChildPing))

    assert get_event_origin_namespace(parent_ping) == ()
    child_origin = get_event_origin_namespace(child_ping)
    assert len(child_origin) == 1
    assert child_origin[0].startswith("child#")


# --- Grandchild: compound namespace tag ---------------------------------------


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    pass


class GrandPing(Event):
    pass


class MidStart(StartEvent):
    pass


class MidStop(StopEvent):
    pass


class GrandStream(Workflow):
    @step
    async def run_grand(self, ctx: Context, ev: GrandStart) -> GrandStop:
        ctx.write_event_to_stream(GrandPing())
        return GrandStop()


class MidStream(Workflow):
    grand: GrandStream

    @step
    async def begin(self, ev: MidStart) -> GrandStart:
        return GrandStart()

    @step
    async def end(self, ev: GrandStop) -> MidStop:
        return MidStop()


class TopStream(Workflow):
    mid: MidStream

    @step
    async def start(self, ev: StartEvent) -> MidStart:
        return MidStart()

    @step
    async def finish(self, ev: MidStop) -> StopEvent:
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_grandchild_event_tagged_with_compound_namespace() -> None:
    handler = TopStream(mid=MidStream(grand=GrandStream())).run()
    collected: list[Event] = []
    async for ev in handler.stream_events(include_children=True):
        collected.append(ev)
    await handler

    grand_ping = next(ev for ev in collected if isinstance(ev, GrandPing))
    grand_origin = get_event_origin_namespace(grand_ping)
    assert len(grand_origin) == 2
    assert grand_origin[0].startswith("mid#")
    assert grand_origin[1].startswith("grand#")


@pytest.mark.asyncio
async def test_grandchild_event_hidden_by_default() -> None:
    handler = TopStream(mid=MidStream(grand=GrandStream())).run()
    collected: list[Event] = []
    async for ev in handler.stream_events():
        collected.append(ev)
    await handler

    assert not any(isinstance(ev, GrandPing) for ev in collected)
