# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import pytest
from workflows import Context, Workflow, step
from workflows.context.internal_context import InternalContext
from workflows.context.serializers import JsonSerializer
from workflows.decorators import step as free_step
from workflows.errors import WorkflowValidationError
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerState, EventAttempt


class Header(Event):
    value: str


class Body(Event):
    value: str


class Footer(Event):
    value: str


class HeaderChild(Header):
    pass


class BodyChild(Body):
    pass


@pytest.mark.asyncio
async def test_three_param_heterogeneous_join_fires_once() -> None:
    class AssembleWorkflow(Workflow):
        @step
        async def emit(
            self, ctx: Context, ev: StartEvent
        ) -> Header | Body | Footer | None:
            ctx.send_event(Header(value="h"))
            ctx.send_event(Body(value="b"))
            ctx.send_event(Footer(value="f"))
            return None

        @step
        async def assemble(self, h: Header, b: Body, f: Footer) -> StopEvent:
            return StopEvent(result=f"{h.value}{b.value}{f.value}")

    result = await AssembleWorkflow(timeout=10).run()
    assert result == "hbf"


@pytest.mark.asyncio
async def test_heterogeneous_join_binds_by_parameter_type() -> None:
    seen: dict[str, str] = {}

    class OrderWorkflow(Workflow):
        @step
        async def emit(
            self, ctx: Context, ev: StartEvent
        ) -> Header | Body | Footer | None:
            ctx.send_event(Footer(value="F"))
            ctx.send_event(Header(value="H"))
            ctx.send_event(Body(value="B"))
            return None

        @step
        async def assemble(self, h: Header, b: Body, f: Footer) -> StopEvent:
            seen["h"] = h.value
            seen["b"] = b.value
            seen["f"] = f.value
            return StopEvent(result="ok")

    await OrderWorkflow(timeout=10).run()
    assert seen == {"h": "H", "b": "B", "f": "F"}


@pytest.mark.asyncio
async def test_heterogeneous_join_with_context_param() -> None:
    class CtxWorkflow(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> Header | Body | None:
            ctx.send_event(Header(value="x"))
            ctx.send_event(Body(value="y"))
            return None

        @step
        async def assemble(self, ctx: Context, h: Header, b: Body) -> StopEvent:
            await ctx.store.set("joined", h.value + b.value)
            return StopEvent(result=await ctx.store.get("joined"))

    result = await CtxWorkflow(timeout=10).run()
    assert result == "xy"


@pytest.mark.asyncio
async def test_same_type_join_binds_by_arrival_order() -> None:
    class SameTypeWorkflow(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> Header | None:
            ctx.send_event(Header(value="first"))
            ctx.send_event(Header(value="second"))
            return None

        @step
        async def assemble(self, first: Header, second: Header) -> StopEvent:
            return StopEvent(result=f"{first.value},{second.value}")

    result = await SameTypeWorkflow(timeout=10).run()
    assert result == "first,second"


@pytest.mark.asyncio
async def test_same_type_join_releases_repeated_batches() -> None:
    pairs: list[tuple[str, str]] = []

    class SameTypeBatchWorkflow(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> Header | None:
            for value in ["a", "b", "c", "d"]:
                ctx.send_event(Header(value=value))
            return None

        @step
        async def assemble(self, first: Header, second: Header, ctx: Context) -> Body:
            pairs.append((first.value, second.value))
            count = await ctx.store.get("count", default=0) + 1
            await ctx.store.set("count", count)
            return Body(value=str(count))

        @step
        async def finish(self, ev: Body) -> StopEvent | None:
            if ev.value == "2":
                return StopEvent(result=list(pairs))
            return None

    result = await SameTypeBatchWorkflow(timeout=10).run()
    assert result == [("a", "b"), ("c", "d")]


@pytest.mark.asyncio
async def test_collect_mode_honors_accept_event_subclasses() -> None:
    class SubclassWorkflow(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> Header | Body | None:
            ctx.send_event(BodyChild(value="B"))
            ctx.send_event(HeaderChild(value="H"))
            return None

        @step(accept_event_subclasses=True)
        async def assemble(self, h: Header, b: Body) -> StopEvent:
            return StopEvent(result=f"{h.value}{b.value}")

    result = await SubclassWorkflow(timeout=10).run()
    assert result == "HB"


@pytest.mark.asyncio
async def test_collect_mode_subclass_matching_handles_overlapping_slots() -> None:
    class OverlapWorkflow(Workflow):
        @step
        async def emit(
            self, ctx: Context, ev: StartEvent
        ) -> Header | HeaderChild | None:
            ctx.send_event(Header(value="parent"))
            ctx.send_event(HeaderChild(value="child"))
            return None

        @step(accept_event_subclasses=True)
        async def assemble(self, parent: Header, child: HeaderChild) -> StopEvent:
            return StopEvent(result=f"{parent.value},{child.value}")

    result = await OverlapWorkflow(timeout=10).run()
    assert result == "parent,child"


@pytest.mark.asyncio
async def test_collect_mode_uses_private_buffer() -> None:
    class BufferWorkflow(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> Header | Body | None:
            ctx.send_event(Header(value="h"))
            ctx.send_event(Body(value="b"))
            return None

        @step
        async def assemble(self, ctx: Context, h: Header, b: Body) -> StopEvent:
            user_buffer = ctx.collect_events(b, [Header, Body])
            return StopEvent(result=user_buffer is None)

    result = await BufferWorkflow(timeout=10).run()
    assert result is True


@pytest.mark.asyncio
async def test_collect_mode_does_not_call_collect_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_collect_events(*args: object, **kwargs: object) -> None:
        raise AssertionError("static fan-in should not call collect_events")

    monkeypatch.setattr(InternalContext, "collect_events", fail_collect_events)

    class StaticFanInWorkflow(Workflow):
        @step
        async def emit(self, ctx: Context, ev: StartEvent) -> Header | Body | None:
            ctx.send_event(Header(value="h"))
            ctx.send_event(Body(value="b"))
            return None

        @step
        async def assemble(self, h: Header, b: Body) -> StopEvent:
            return StopEvent(result=f"{h.value}{b.value}")

    result = await StaticFanInWorkflow(timeout=10).run()
    assert result == "hb"


def test_collect_mode_state_serializes_static_buffers() -> None:
    class SerializeWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> None:
            return None

        @step
        async def assemble(self, h: Header, b: Body) -> StopEvent:
            return StopEvent(result=f"{h.value}{b.value}")

    workflow = SerializeWorkflow()
    serializer = JsonSerializer()
    state = BrokerState.from_workflow(workflow)
    worker = state.workers["assemble"]
    worker.static_collect_events.append(Header(value="pending"))
    worker.queue.append(
        EventAttempt(
            event=Body(value="trigger"),
            bound_events={
                "h": Header(value="bound-h"),
                "b": Body(value="bound-b"),
            },
        )
    )

    restored = BrokerState.from_serialized(
        state.to_serialized(serializer),
        workflow,
        serializer,
    )
    restored_worker = restored.workers["assemble"]

    assert restored_worker.static_collect_events == [Header(value="pending")]
    assert restored_worker.queue[0].bound_events == {
        "h": Header(value="bound-h"),
        "b": Body(value="bound-b"),
    }


def test_union_collect_param_rejected() -> None:
    class _UnionWorkflow(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="single event type"):

        @free_step(workflow=_UnionWorkflow)
        async def assemble(h: Header, b: Body | Footer) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")


def test_list_event_param_rejected_with_forward_pointing_error() -> None:
    class _ListWorkflow(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="not supported yet"):

        @free_step(workflow=_ListWorkflow)
        async def collect(events: list[Header]) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")
