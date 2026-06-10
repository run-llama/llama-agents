# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Unit tests for core's namespaced-state builder, seed reconvert, and the
flat-vs-nested serialization gate (Phase 1 surface)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel
from workflows import Context, Workflow
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import (
    CHILD_STATES_KEY,
    ROOT_STATE_KEY,
    DictState,
    InMemoryStateStore,
    build_namespaced_state,
    namespaced_seed_blob,
    namespaced_seed_payload,
    namespaced_state_types,
    namespaced_underlying_state_type,
)
from workflows.decorators import step
from workflows.events import StartEvent, StopEvent


class RootState(BaseModel):
    root_value: str = ""


class ChildStart(StartEvent):
    payload: str = ""


class ChildStop(StopEvent):
    out: str = ""


class Child(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: ChildStart) -> ChildStop:
        await ctx.store.set("child_value", ev.payload)
        return ChildStop(out=ev.payload.upper())


class Parent(Workflow):
    child: Child

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        return ChildStart(payload="hi")

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result=ev.out)


class Childless(Workflow):
    @step
    async def go(self, ctx: Context[RootState], ev: StartEvent) -> StopEvent:
        await ctx.store.set("root_value", "x")
        return StopEvent(result="done")


def test_state_types_and_underlying_type_for_childless() -> None:
    wf = Childless()
    assert set(namespaced_state_types(wf)) == {()}
    assert namespaced_underlying_state_type(wf) is RootState


def test_state_types_and_underlying_type_for_child_ful() -> None:
    wf = Parent(child=Child())
    types = namespaced_state_types(wf)
    assert set(types) == {(), ("child",)}
    # Child-ful runs use a DictState blob as the single durable row.
    assert namespaced_underlying_state_type(wf) is DictState


def test_single_namespace_view_resolves_to_underlying_flat() -> None:
    wf = Childless()
    underlying = InMemoryStateStore(RootState(root_value="x"))
    namespaced = build_namespaced_state(wf, underlying, JsonSerializer())

    assert namespaced.is_single_namespace
    # view(()) is the plain underlying store; serialization stays flat.
    assert namespaced.view(()) is underlying
    serializer = JsonSerializer()
    assert namespaced.serialize_tree(serializer) == underlying.to_dict(serializer)
    assert CHILD_STATES_KEY not in namespaced.serialize_tree(serializer)


@pytest.mark.asyncio
async def test_multi_namespace_serialize_tree_is_per_slot_nested() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    underlying = InMemoryStateStore(DictState())
    namespaced = build_namespaced_state(wf, underlying, serializer)

    assert not namespaced.is_single_namespace
    await namespaced.view(()).set("root_value", "R")
    await namespaced.view(("child",)).set("child_value", "C")

    tree = namespaced.serialize_tree(serializer)
    # Root payload is flat at the top level; children nested per slot.
    assert tree["state_data"]["_data"]["root_value"] == serializer.serialize("R")
    child_states = tree[CHILD_STATES_KEY]
    assert set(child_states) == {"child"}
    assert child_states["child"]["state_data"]["_data"]["child_value"] == (
        serializer.serialize("C")
    )


@pytest.mark.asyncio
async def test_seed_round_trip_rebuilds_slots() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    underlying = InMemoryStateStore(DictState())
    namespaced = build_namespaced_state(wf, underlying, serializer)
    await namespaced.view(()).set("root_value", "R")
    await namespaced.view(("child",)).set("child_value", "C")

    tree = namespaced.serialize_tree(serializer)

    # Reconvert the portable tree into a fresh durable blob and rebuild.
    blob = namespaced_seed_blob(tree, child_ful=True)
    assert blob is not None
    assert ROOT_STATE_KEY in blob and CHILD_STATES_KEY in blob

    rebuilt = build_namespaced_state(wf, InMemoryStateStore(blob), serializer)
    assert await rebuilt.view(()).get("root_value") == "R"
    assert await rebuilt.view(("child",)).get("child_value") == "C"


def test_seed_helpers_noop_for_childless_and_references() -> None:
    serializer = JsonSerializer()
    # Childless target keeps the flat format byte-for-byte: nothing to seed.
    flat = InMemoryStateStore(RootState(root_value="x")).to_dict(serializer)
    assert namespaced_seed_blob(flat, child_ful=False) is None
    assert namespaced_seed_payload(flat, serializer, child_ful=False) is None
    # Persisted reference: row already exists, nothing to seed (even if child-ful).
    assert (
        namespaced_seed_blob({"store_type": "postgres", "run_id": "r"}, child_ful=True)
        is None
    )
    assert namespaced_seed_blob(None, child_ful=True) is None


@pytest.mark.asyncio
async def test_flat_childless_checkpoint_carries_root_state_into_child_ful_run() -> (
    None
):
    """Adding children to a previously-childless workflow must not orphan the
    root ``ctx.store``. A flat checkpoint resumed into a child-ful run is lifted
    into the root slot, with children starting empty."""
    serializer = JsonSerializer()
    # A flat checkpoint as written by the childless version of the workflow.
    flat = InMemoryStateStore(DictState(_data={"counter": 5})).to_dict(serializer)
    assert CHILD_STATES_KEY not in flat

    blob = namespaced_seed_blob(flat, child_ful=True)
    assert blob is not None
    assert ROOT_STATE_KEY in blob and CHILD_STATES_KEY in blob
    assert blob[CHILD_STATES_KEY] == {}

    wf = Parent(child=Child())
    rebuilt = build_namespaced_state(wf, InMemoryStateStore(blob), serializer)
    # Root state survived; the child's namespace is a fresh default.
    assert await rebuilt.view(()).get("counter") == 5
    assert await rebuilt.view(("child",)).get("child_value", None) is None


@pytest.mark.asyncio
async def test_childless_dict_state_to_dict_is_flat_byte_stable() -> None:
    """A childless run serializes to the flat ``InMemorySerializedState`` contract
    -- no ``__root__`` / ``__child_states__`` wrapping. This is the shipped format."""

    class W(Workflow):
        @step
        async def go(self, ctx: Context, ev: StartEvent) -> StopEvent:
            await ctx.store.set("k", "v")
            return StopEvent(result="ok")

    wf = W()
    ctx = Context(wf)
    await wf.run(ctx=ctx)

    state = ctx.to_dict()["state"]
    serializer = JsonSerializer()
    assert state == {
        "store_type": "in_memory",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": {"_data": {"k": serializer.serialize("v")}},
    }
    assert CHILD_STATES_KEY not in state
    assert ROOT_STATE_KEY not in state["state_data"]["_data"]


@pytest.mark.asyncio
async def test_childless_typed_state_to_dict_is_flat_byte_stable() -> None:
    """A childless typed run keeps its typed flat format (not a DictState blob)."""
    wf = Childless()
    ctx = Context(wf)
    await wf.run(ctx=ctx)

    state = ctx.to_dict()["state"]
    assert state["store_type"] == "in_memory"
    assert state["state_type"] == "RootState"
    assert CHILD_STATES_KEY not in state
    # Round-trips back into a usable context.
    assert Context.from_dict(wf, ctx.to_dict()) is not None


def test_seed_payload_matches_blob_dump() -> None:
    serializer = JsonSerializer()
    tree = {
        "store_type": "in_memory",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": {"_data": {"root_value": serializer.serialize("R")}},
        CHILD_STATES_KEY: {"child": {"state_data": {"_data": {}}}},
    }
    blob = namespaced_seed_blob(tree, child_ful=True)
    assert blob is not None
    payload = namespaced_seed_payload(tree, serializer, child_ful=True)
    assert payload == InMemoryStateStore(blob).to_dict(serializer)
