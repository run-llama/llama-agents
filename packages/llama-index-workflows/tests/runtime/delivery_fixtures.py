# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Workflows and helpers for delivery tests.

Tests identify events by their unique ``label`` values.
"""

from __future__ import annotations

import asyncio
import itertools
from collections import Counter
from typing import Any

import pytest
from workflows import Context, Workflow, catch_error, step
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.events import (
    CollectionReleaseEvent,
    Event,
    StartEvent,
    StepFailedEvent,
    StopEvent,
)
from workflows.handler import WorkflowHandler
from workflows.retry_policy import retry_policy, stop_after_attempt, wait_fixed
from workflows.runtime.control_loop import rebuild_state_from_ticks
from workflows.runtime.control_loop.reduce import _reduce_tick
from workflows.runtime.types.commands import (
    CommandQueueEvent,
    CommandRunWorker,
    WorkflowCommand,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.plugin import as_snapshottable_adapter
from workflows.runtime.types.results import (
    EmissionKey,
    SentEvent,
    StepFunctionResult,
    StepWorkerFailed,
    StepWorkerResult,
)
from workflows.runtime.types.step_id import StepId
from workflows.runtime.types.ticks import (
    TickAddEvent,
    TickSessionStart,
    TickStepResult,
    WorkflowTick,
)


class Labeled(Event):
    label: str


def label(ev: Event) -> str:
    return getattr(ev, "label", type(ev).__name__)


# Track completed steps and pause the relay for resume tests.

# Each completed step appends its name and event label.
LEDGER: list[tuple[str, str]] = []
# Use a new nonce on each invocation to distinguish sends after a rerun.
_NONCE = itertools.count()
# Pause the relay here to cancel and resume it during recording.
RELAY_GATE: dict[str, bool] = {"open": True, "parked": False}


def _consume(step_name: str, *events: Event) -> None:
    LEDGER.extend((step_name, label(ev)) for ev in events)


class Item(Labeled):
    n: int


class Done(Labeled):
    n: int


class Shared(Labeled):
    total: int


class Returned(Labeled):
    total: int


class Relayed(Labeled):
    tag: str
    total: int


class RelayPing(Labeled):
    tag: str


class RelayPinged(Labeled):
    tag: str


class SenderPing(Labeled):
    pass


class SenderPinged(Labeled):
    pass


class RelayPair(Labeled):
    total: int


class EveryCutWorkflow(Workflow):
    """Exercise delivery across retries, collection and fan-out.

    ``sender`` sends an event and ``returner`` returns one for the same input.
    ``relay`` does both. ``pair`` uses the relay tag as its buffer key so it
    can only complete with a ping from a completed relay invocation.
    """

    @step
    async def start(self, ev: StartEvent) -> list[Item]:
        _consume("start", ev)
        return [Item(label=f"item-{n}", n=n) for n in range(3)]

    @step(retry_policy=retry_policy(wait=wait_fixed(0.0), stop=stop_after_attempt(3)))
    async def work(self, ctx: Context, ev: Item) -> Done:
        if ev.n == 1 and ctx.retry_info().retry_number == 0:
            raise ValueError("first attempt fails")
        _consume("work", ev)
        return Done(label=f"done-{ev.n}", n=ev.n)

    @step
    async def join(self, events: list[Done]) -> Shared:
        _consume("join", *events)
        return Shared(label="shared", total=sum(e.n for e in events))

    @step
    async def sender(self, ctx: Context, ev: Shared) -> SenderPing | None:
        ctx.send_event(SenderPing(label=f"sender-ping-{next(_NONCE)}"))
        _consume("sender", ev)

    @step
    async def returner(self, ev: Shared) -> Returned:
        _consume("returner", ev)
        return Returned(label="returned", total=ev.total)

    @step
    async def relay(self, ctx: Context, ev: Returned) -> Relayed | RelayPing:
        if not RELAY_GATE["open"]:
            RELAY_GATE["parked"] = True
            await asyncio.sleep(3600)
        tag = f"relay-{next(_NONCE)}"
        ctx.send_event(RelayPing(label=f"{tag}-ping", tag=tag))
        _consume("relay", ev)
        return Relayed(label=tag, tag=tag, total=ev.total)

    @step
    async def sink_relay(self, ev: RelayPing) -> RelayPinged:
        _consume("sink_relay", ev)
        return RelayPinged(label=f"{ev.tag}-pinged", tag=ev.tag)

    @step
    async def sink_sender(self, ev: SenderPing) -> SenderPinged:
        _consume("sink_sender", ev)
        return SenderPinged(label=f"{ev.label}-ed")

    @step(num_workers=1)
    async def pair(self, ctx: Context, ev: Relayed | RelayPinged) -> RelayPair | None:
        collected = ctx.collect_events(ev, [Relayed, RelayPinged], buffer_id=ev.tag)
        _consume("pair", ev)
        if collected is None:
            return None
        relayed = collected[0]
        assert isinstance(relayed, Relayed)
        return RelayPair(label="pair", total=relayed.total)

    @step(num_workers=1)
    async def finish(
        self, ctx: Context, ev: RelayPair | SenderPinged
    ) -> StopEvent | None:
        collected = ctx.collect_events(ev, [RelayPair, SenderPinged])
        _consume("finish", ev)
        if collected is None:
            return None
        pair = collected[0]
        assert isinstance(pair, RelayPair)
        return StopEvent(result=f"total={pair.total}")


# Expected consumer and label pairs for events with fixed labels.
EVERY_CUT_DETERMINISTIC = {
    ("start", "StartEvent"),
    *(("work", f"item-{n}") for n in range(3)),
    *(("join", f"done-{n}") for n in range(3)),
    ("sender", "shared"),
    ("returner", "shared"),
    ("relay", "returned"),
    ("finish", "pair"),
}


class Unjsonable:
    """A non-JSON payload for pickle tests."""

    def __init__(self, value: int) -> None:
        self.value = value


class Opaque(Labeled):
    payload: Any


class _Uncomparable(Labeled):
    """Raise on equality checks to catch accidental event comparisons."""

    def __eq__(self, other: object) -> bool:
        raise AssertionError("events must not be compared")


class Touchy(_Uncomparable):
    pass


class TouchyEcho(_Uncomparable):
    pass


class TouchyDone(Labeled):
    pass


class PickledWorkflow(Workflow):
    @step
    async def begin(self, ev: StartEvent) -> Opaque:
        _consume("begin", ev)
        return Opaque(label="opaque", payload=Unjsonable(7))

    @step
    async def poke(self, ctx: Context, ev: Opaque) -> TouchyDone | Touchy:
        ctx.send_event(Touchy(label=f"touchy-{next(_NONCE)}"))
        _consume("poke", ev)
        return TouchyDone(label="poked")

    @step
    async def touched(self, ev: Touchy) -> TouchyEcho:
        _consume("touched", ev)
        return TouchyEcho(label=f"{ev.label}-echo")

    @step(num_workers=1)
    async def end(self, ctx: Context, ev: TouchyDone | TouchyEcho) -> StopEvent | None:
        collected = ctx.collect_events(ev, [TouchyDone, TouchyEcho])
        _consume("end", ev)
        if collected is None:
            return None
        return StopEvent(result="done")


PICKLED_DETERMINISTIC = {("begin", "StartEvent"), ("poke", "opaque"), ("end", "poked")}


class Ask(Labeled):
    pass


class Answer(Labeled):
    pass


class SendThenWaitWorkflow(Workflow):
    @step
    async def ask(self, ctx: Context, ev: StartEvent) -> StopEvent | Ask:
        ctx.send_event(Ask(label="ask"))
        answer = await ctx.wait_for_event(Answer, waiter_id="answer")
        return StopEvent(result=answer.label)

    @step
    async def answer(self, ev: Ask) -> Answer:
        return Answer(label="answered")


def adapter_ticks(
    wf: Workflow, handler: WorkflowHandler
) -> tuple[BrokerState, list[WorkflowTick]]:
    adapter = as_snapshottable_adapter(wf._runtime.get_external_adapter(handler.run_id))
    assert adapter is not None
    return adapter.init_state, list(adapter.replay())


async def record_with_resume(
    wf: Workflow,
) -> tuple[BrokerState, list[WorkflowTick], Any]:
    """Cancel and resume a run while ``relay`` is active.

    Return the initial state and ticks from both sessions for replay.
    """
    RELAY_GATE.update(open=False, parked=False)
    first = wf.run()
    for _ in range(500):
        if RELAY_GATE["parked"]:
            break
        await asyncio.sleep(0.01)
    assert RELAY_GATE["parked"], "relay never started"
    await asyncio.sleep(0.05)  # Wait for pending ticks to reach the journal.
    snapshot = first.ctx.to_dict()
    await first.cancel_run()
    with pytest.raises(Exception):
        await first
    init, first_ticks = adapter_ticks(wf, first)

    RELAY_GATE["open"] = True
    second = wf.run(ctx=Context.from_dict(wf, snapshot))
    result = await asyncio.wait_for(second, timeout=5)
    _, second_ticks = adapter_ticks(wf, second)
    return init, [*first_ticks, *second_ticks], result


async def record_once(
    wf: Workflow, serializer: BaseSerializer
) -> tuple[BrokerState, list[WorkflowTick], Any]:
    """Record a run, then serialize and reload its journal."""
    handler = wf.run()
    result = await asyncio.wait_for(handler, timeout=5)
    init, ticks = adapter_ticks(wf, handler)
    persisted: list[WorkflowTick] = []
    for tick in ticks:
        with serializer.validation_context():
            persisted.append(serializer.deserialize(serializer.serialize(tick)))
    return init, persisted, result


def prefix_consumption(ticks: list[WorkflowTick]) -> Counter[tuple[str, str]]:
    """Find completed steps in a journal prefix."""
    counts: Counter[tuple[str, str]] = Counter()
    for tick in ticks:
        if not isinstance(tick, TickStepResult) or not any(
            isinstance(r, StepWorkerResult) for r in tick.result
        ):
            continue
        events = (
            tick.event.events
            if isinstance(tick.event, CollectionReleaseEvent)
            else [tick.event]
        )
        counts.update((tick.step_id.name, label(ev)) for ev in events)
    return counts


def finished(ticks: list[WorkflowTick]) -> bool:
    return any(
        isinstance(r, StepWorkerResult) and isinstance(r.result, StopEvent)
        for t in ticks
        if isinstance(t, TickStepResult)
        for r in t.result
    )


def assert_settled(data: dict[str, Any]) -> None:
    assert data["deliveries"] == []
    for name, worker in data["workers"].items():
        assert worker["in_progress"] == [], name


# Drive the reducer directly for delivery tests.


class Job(Labeled):
    pass


class Both(Labeled):
    pass


class Out(Labeled):
    pass


class Ping(Labeled):
    pass


class Boom(Labeled):
    pass


class Flow(Workflow):
    @step(num_workers=2)
    async def producer(self, ctx: Context, ev: Job) -> Out:
        return Out(label=ev.label)

    @step
    async def sender(self, ctx: Context, ev: Both) -> None:
        return None

    @step
    async def returner(self, ev: Both) -> Out:
        return Out(label=ev.label)

    @step(num_workers=8)
    async def consumer(self, ev: Out) -> None:
        return None

    @step(num_workers=8)
    async def sink(self, ev: Ping) -> None:
        return None

    @step
    async def failing(self, ev: Boom) -> None:
        return None

    @step
    async def begin(self, ev: StartEvent) -> StopEvent:
        return StopEvent()


class Recovering(Workflow):
    @step
    async def begin(self, ev: StartEvent) -> Boom:
        return Boom(label="boom")

    @step
    async def failing(self, ev: Boom) -> StopEvent:
        return StopEvent()

    @catch_error(for_steps=["failing"])
    async def recover(self, ev: StepFailedEvent) -> StopEvent:
        return StopEvent()


SERIALIZER = JsonSerializer(allowed_types=[Job, Both, Out, Ping, Boom])


def flow_state() -> BrokerState:
    broker = BrokerState.from_workflow(Flow(disable_validation=True))
    broker.is_running = True
    return broker


def reduce(
    state: BrokerState, tick: WorkflowTick
) -> tuple[BrokerState, list[WorkflowCommand]]:
    return _reduce_tick(tick, state, now_seconds=100.0)


def fold(
    state: BrokerState, ticks: list[WorkflowTick]
) -> tuple[BrokerState, list[WorkflowCommand]]:
    commands: list[WorkflowCommand] = []
    for tick in ticks:
        state, new = reduce(state, tick)
        commands.extend(new)
    return state, commands


def runs(commands: list[WorkflowCommand]) -> list[CommandRunWorker]:
    return [c for c in commands if isinstance(c, CommandRunWorker)]


def derived(commands: list[WorkflowCommand]) -> list[WorkflowTick]:
    """Build the ticks the runner would queue for these commands."""
    return [
        TickAddEvent(
            event=c.event,
            step_id=c.step_id,
            origin_namespace=c.origin_namespace,
            recovery_counts=dict(c.recovery_counts),
            scope_path=c.scope_path,
            emission=c.emission,
        )
        for c in commands
        if isinstance(c, CommandQueueEvent)
    ]


def dispatch(state: BrokerState, event: Event) -> tuple[BrokerState, CommandRunWorker]:
    state, commands = reduce(state, TickAddEvent(event=event))
    [run] = runs(commands)
    return state, run


def result(
    run: CommandRunWorker,
    *results: StepFunctionResult,
    sends: list[Event] | None = None,
) -> TickStepResult:
    return TickStepResult(
        step_id=run.step_id,
        worker_id=run.id,
        event=run.event,
        result=list(results),
        invocation_id=run.invocation_id,
        sends=[SentEvent(index=i, event=ev) for i, ev in enumerate(sends or [])],
    )


def ok(event: Event | None) -> StepWorkerResult:
    return StepWorkerResult(result=event)


def send(run: CommandRunWorker, index: int, event: Event) -> TickAddEvent:
    """Build the mailbox tick for a send during ``run``."""
    assert run.invocation_id is not None
    return TickAddEvent(
        event=event,
        emission=EmissionKey(invocation_id=run.invocation_id, index=index),
    )


def restore(
    state: BrokerState,
    workflow: Workflow | None = None,
    serializer: BaseSerializer = SERIALIZER,
) -> BrokerState:
    """Serialize and restore broker state using the context format."""
    return BrokerState.from_serialized(
        state.to_serialized(serializer),
        workflow or Flow(disable_validation=True),
        serializer,
    )


def routed(state: BrokerState, step_name: str) -> list[str]:
    """List labels for events waiting for a step to finish."""
    worker = state.workers[StepId.root(step_name)]
    events = [a.event for a in worker.queue] + [ip.event for ip in worker.in_progress]
    return sorted(label(ev) for ev in events)


def keys(state: BrokerState) -> list[tuple[str, int]]:
    return [(k.invocation_id, k.index) for k in state.deliveries]


def fold_snapshot(
    init: BrokerState, ticks: list[WorkflowTick], serializer: BaseSerializer
) -> tuple[BrokerState, dict[str, Any]]:
    """Replay a journal prefix and serialize the resulting state."""
    state = rebuild_state_from_ticks(init, ticks)
    return state, state.to_serialized(serializer).model_dump()


SESSION_START = TickSessionStart(stamped_at=200.0)


def resume(state: BrokerState) -> BrokerState:
    """Resume a snapshot and route its pending deliveries."""
    state, reemitted = reduce(restore(state), SESSION_START)
    state, _ = fold(state, derived(reemitted))
    return state


def owing(state: BrokerState) -> tuple[BrokerState, TickStepResult]:
    """Build state with a pending send and a step failure to reduce."""
    state, commands = reduce(state, TickAddEvent(event=Both(label="s")))
    sender = next(r for r in runs(commands) if r.step_id.name == "sender")
    state, _ = reduce(state, result(sender, ok(None), sends=[Ping(label="owed")]))
    state, failing = dispatch(state, Boom(label="boom"))
    assert len(state.deliveries) == 1
    failure = StepWorkerFailed(exception=ValueError("x"), failed_at=100.0)
    return state, result(failing, failure)
