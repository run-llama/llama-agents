# ty: ignore[unknown-argument]
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Load-and-replay checks for current golden serialization fixtures.

These fixtures pin the current ``origin/main`` snapshot and journal wire shapes
(see ``fixtures/golden_serialization/README.md``). The tests assert that those shapes
load and replay unchanged at the behavioral level: load-compat plus round-trip
equivalence, not byte identity.
"""

from __future__ import annotations

import base64
import json
import pickle
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from workflows import Context, Workflow
from workflows.context.serializers import JsonSerializer
from workflows.decorators import step
from workflows.events import Event, HumanResponseEvent, StartEvent, StopEvent
from workflows.runtime.control_loop.reduce import _reduce_tick, rewind_in_progress
from workflows.runtime.types.commands import CommandCompleteRun, CommandRunWorker
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import WorkflowTickAdapter

_FIXTURES = Path(__file__).parent / "fixtures" / "golden_serialization"


class Item(Event):
    n: int


class Partial(Event):
    n: int


# The journal fixture was captured from a script run as ``__main__``, so its
# serialized events carry the qualified names ``__main__.Item`` / ``__main__.Partial``.
# Bind these classes there so the serializer resolves them on deserialize.
setattr(sys.modules["__main__"], "Item", Item)
setattr(sys.modules["__main__"], "Partial", Partial)


class GoldenJournalWorkflow(Workflow):
    """Fan-out + in-stream ``ctx.collect_events`` join (journal fixture shape)."""

    @step
    async def start(self, ev: StartEvent) -> list[Item]:
        return [Item(n=i) for i in range(4)]

    @step
    async def work(self, ev: Item) -> Partial:
        return Partial(n=ev.n * 2)

    @step
    async def collect(self, ctx: Context, ev: Partial) -> StopEvent | None:
        got = ctx.collect_events(ev, [Partial, Partial, Partial, Partial])
        if got is None:
            return None
        return StopEvent(result=sum(p.n for p in got))


class HumanResponse(HumanResponseEvent):
    """Typed ``HumanResponseEvent`` so the fixture constructs a response without
    an untyped ``response=`` call (the base event declares no fields)."""

    response: str


class GoldenSnapshotWorkflow(Workflow):
    """HITL workflow suspended on a ``wait_for_event`` waiter (snapshot fixture)."""

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
        response = await ctx.wait_for_event(HumanResponse)
        return StopEvent(result=response.response)


class GoldenPersistedWorkflow(Workflow):
    """Stable event types and step names for the old broker pickle fixture."""

    @step
    async def source(self, ev: StartEvent) -> list[Item]:
        return [Item(n=1), Item(n=2)]

    @step
    async def process(self, ev: Item) -> Partial:
        return Partial(n=ev.n)

    @step
    async def target(self, ctx: Context, events: list[Partial]) -> StopEvent:
        response = await ctx.wait_for_event(HumanResponse)
        return StopEvent(result=(sum(event.n for event in events), response.response))


def _load(name: str) -> dict[str, Any]:
    return json.loads((_FIXTURES / name).read_text())


def test_golden_journal_replays_from_canonical_state() -> None:
    """Replaying the golden tick journal from a fresh ``from_workflow`` state
    reaches the same terminal ``StopEvent`` the live run recorded."""
    journal = _load("journal.json")
    ticks = [WorkflowTickAdapter.validate_python(t) for t in journal["ticks"]]

    state = BrokerState.from_workflow(GoldenJournalWorkflow())
    state, _ = rewind_in_progress(state, time.time())
    result: Any = None
    for tick in ticks:
        state, commands = _reduce_tick(tick, state, time.time())
        for command in commands:
            if isinstance(command, CommandCompleteRun):
                result = command.result

    assert result is not None
    assert result.result == journal["result"]


@pytest.mark.asyncio
async def test_golden_snapshot_loads_and_resumes() -> None:
    """A golden mid-run snapshot loads and resumes to completion."""
    snapshot = _load("snapshot.json")
    meta = _load("snapshot_meta.json")

    workflow = GoldenSnapshotWorkflow(
        serializer=JsonSerializer(allowed_types=[HumanResponse])
    )
    ctx = Context.from_dict(workflow, snapshot)
    handler = workflow.run(ctx=ctx)
    handler.ctx.send_event(HumanResponse(response="42"))

    result = await handler
    assert result == meta["expected_result_after_resume"]


@pytest.mark.parametrize("name", ["broker_state_main_py314.b64"])
def test_golden_broker_pickle_restores_pending_work(name: str) -> None:
    """DBOS persists the initial broker as a pickled workflow argument."""
    # DBOS's default serializer decodes base64 and unpickles workflow inputs.
    inputs = pickle.loads(base64.b64decode((_FIXTURES / name).read_text()))
    (state, _, _) = inputs["args"]
    assert isinstance(state, BrokerState)

    source = StepId.root("source")
    process = StepId.root("process")
    target = StepId.root("target")

    (binding,) = state.config.bindings_for_source(source)
    assert binding.target_step == target
    assert state.config.binding_for_target("stream", target, state.streams) == binding

    (waiter,) = state.workers[target].collected_waiters
    assert waiter.waiting_for_event is HumanResponse
    assert waiter.work_item_id == "collect-stream"

    recovered, commands = rewind_in_progress(state, 200.0)
    assert {
        command.event.n
        for command in commands
        if isinstance(command, CommandRunWorker) and command.step_id == process
    } == {1, 2}
    assert (
        recovered.config.binding_for_target("stream", target, recovered.streams)
        == binding
    )
    assert (
        recovered.workers[target].collected_waiters[0].work_item_id == "collect-stream"
    )
