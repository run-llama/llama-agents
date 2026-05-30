# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import pytest
from workflows import Context, Workflow, step
from workflows.decorators import step as free_step
from workflows.errors import WorkflowValidationError
from workflows.events import Event, StartEvent, StopEvent


class Header(Event):
    value: str


class Body(Event):
    value: str


class Footer(Event):
    value: str


@pytest.mark.asyncio
async def test_three_param_heterogeneous_join_fires_once() -> None:
    """A step with three single-event params fires once with all three bound."""

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
    """Parameters are bound to the event matching their declared type, not order."""

    seen: dict[str, str] = {}

    class OrderWorkflow(Workflow):
        @step
        async def emit(
            self, ctx: Context, ev: StartEvent
        ) -> Header | Body | Footer | None:
            # Emit in an order different from the assemble signature.
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
    """A collect-mode step may also take a Context parameter."""

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


def test_union_collect_param_rejected() -> None:
    """A union-typed parameter in a collect-mode step is rejected for now."""

    class _UnionWorkflow(Workflow):
        pass

    with pytest.raises(WorkflowValidationError, match="single event type"):

        @free_step(workflow=_UnionWorkflow)
        async def assemble(h: Header, b: Body | Footer) -> StopEvent:  # type: ignore[unused-ignore]
            return StopEvent(result="x")


def test_list_event_param_accepted_as_batch_collect() -> None:
    """A single ``list[E]`` parameter is now a batch-collect step (Phase L2)."""

    class _ListWorkflow(Workflow):
        pass

    @free_step(workflow=_ListWorkflow)
    async def collect(events: list[Header]) -> StopEvent:  # type: ignore[unused-ignore]
        return StopEvent(result="x")

    cfg = collect._step_config
    assert cfg.batch_collect_param is not None
    assert cfg.batch_collect_param[0] == "events"
    assert cfg.batch_collect_param[1] == (Header,)
    # The step routes on the element event type.
    assert Header in cfg.accepted_events


def test_list_union_event_param_accepted_as_flat_batch() -> None:
    """A ``list[A | B]`` collect parameter is a flat heterogeneous batch (L3)."""

    class _ListUnionWorkflow(Workflow):
        pass

    @free_step(workflow=_ListUnionWorkflow)
    async def collect(events: list[Header | Body]) -> StopEvent:  # type: ignore[unused-ignore]
        return StopEvent(result="x")

    cfg = collect._step_config
    assert cfg.batch_collect_param is not None
    assert cfg.batch_collect_param[1] == (Header, Body)
    # Both member types route to the step.
    assert Header in cfg.accepted_events
    assert Body in cfg.accepted_events
