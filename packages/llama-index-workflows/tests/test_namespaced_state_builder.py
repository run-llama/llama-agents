# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Unit tests for the per-namespace state router and seed distribution."""

from __future__ import annotations

import pytest
from pydantic import BaseModel
from workflows import Context, Workflow
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import (
    CHILD_STATES_KEY,
    DictState,
    InMemoryStateStore,
    NamespacedStateStores,
    build_namespaced_state,
    in_memory_namespace_factory,
    namespaced_seed_payloads,
    namespaced_state_types,
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


class TypedParent(Workflow):
    child: Child

    @step
    async def start(self, ctx: Context[RootState], ev: StartEvent) -> StopEvent:
        await ctx.store.set("root_value", "typed")
        return StopEvent(result="done")


class Childless(Workflow):
    @step
    async def go(self, ctx: Context[RootState], ev: StartEvent) -> StopEvent:
        await ctx.store.set("root_value", "x")
        return StopEvent(result="done")


def _namespaced(wf: Workflow, serializer: JsonSerializer) -> NamespacedStateStores:
    """Build an in-memory per-namespace router for *wf*."""
    types = namespaced_state_types(wf)
    return build_namespaced_state(wf, in_memory_namespace_factory(types), serializer)


def test_state_types_for_childless() -> None:
    wf = Childless()
    types = namespaced_state_types(wf)
    assert types == {(): RootState}


def test_state_types_for_child_ful() -> None:
    wf = Parent(child=Child())
    types = namespaced_state_types(wf)
    assert set(types) == {(), ("child",)}
    # Each namespace carries its own inferred type; an untyped child is DictState.
    assert types[("child",)] is DictState


def test_single_namespace_router_is_flat() -> None:
    wf = Childless()
    serializer = JsonSerializer()
    namespaced = _namespaced(wf, serializer)

    assert namespaced.is_single_namespace
    # view() is memoized per namespace.
    assert namespaced.view(()) is namespaced.view(())
    assert isinstance(namespaced.view(()), InMemoryStateStore)
    assert CHILD_STATES_KEY not in namespaced.serialize_tree(serializer)


@pytest.mark.asyncio
async def test_multi_namespace_serialize_tree_is_per_slot_nested() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    namespaced = _namespaced(wf, serializer)

    assert not namespaced.is_single_namespace
    await namespaced.view(()).set("root_value", "R")
    await namespaced.view(("child",)).set("child_value", "C")

    tree = namespaced.serialize_tree(serializer)
    assert tree["state_data"]["_data"]["root_value"] == serializer.serialize("R")
    child_states = tree[CHILD_STATES_KEY]
    assert set(child_states) == {"child"}
    assert child_states["child"]["_data"]["child_value"] == serializer.serialize("C")
    # children carry only raw state_data, not a full record/payload.
    assert "data" not in child_states["child"]
    assert "state_type" not in child_states["child"]
    assert "state_module" not in child_states["child"]


@pytest.mark.asyncio
async def test_namespaces_are_isolated() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    namespaced = _namespaced(wf, serializer)

    await namespaced.view(()).set("shared_key", "root")
    await namespaced.view(("child",)).set("shared_key", "child")

    # Same path, different records: no cross-namespace bleed.
    assert await namespaced.view(()).get("shared_key") == "root"
    assert await namespaced.view(("child",)).get("shared_key") == "child"


@pytest.mark.asyncio
async def test_seed_round_trip_rebuilds_slots() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    namespaced = _namespaced(wf, serializer)
    await namespaced.view(()).set("root_value", "R")
    await namespaced.view(("child",)).set("child_value", "C")

    tree = namespaced.serialize_tree(serializer)

    rebuilt = _namespaced(wf, serializer)
    rebuilt.add_seed_tree(tree, serializer)
    assert await rebuilt.view(()).get("root_value") == "R"
    assert await rebuilt.view(("child",)).get("child_value") == "C"


@pytest.mark.asyncio
async def test_typed_root_round_trip_keeps_root_metadata_with_children() -> None:
    wf = TypedParent(child=Child())
    serializer = JsonSerializer()
    namespaced = _namespaced(wf, serializer)

    await namespaced.view(()).set("root_value", "R")
    await namespaced.view(("child",)).set("child_value", "C")

    tree = namespaced.serialize_tree(serializer)
    assert tree["state_type"] == "RootState"
    assert tree["state_module"] == __name__
    assert CHILD_STATES_KEY in tree

    rebuilt = _namespaced(wf, serializer)
    rebuilt.add_seed_tree(tree, serializer)
    root_state = await rebuilt.view(()).get_state()
    assert isinstance(root_state, RootState)
    assert root_state.root_value == "R"
    assert await rebuilt.view(("child",)).get("child_value") == "C"


@pytest.mark.asyncio
async def test_child_view_uses_standard_facade_operations() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    namespaced = _namespaced(wf, serializer)
    child_store = namespaced.view(("child",))

    await child_store.set_state(DictState(_data={"a": 1}))
    async with child_store.edit_state() as state:
        state._data["b"] = 2
    assert await child_store.get("a") == 1
    assert await child_store.get("b") == 2

    await child_store.clear()
    assert await child_store.get("a", None) is None
    assert await child_store.get("b", None) is None


def test_seed_payloads_none_for_empty_and_durable() -> None:
    types = namespaced_state_types(Parent(child=Child()))
    assert namespaced_seed_payloads(None, types) is None
    assert namespaced_seed_payloads({}, types) is None
    assert (
        namespaced_seed_payloads({"store_type": "postgres", "run_id": "r"}, types)
        is None
    )


def test_seed_payloads_flat_childless_is_root_only() -> None:
    serializer = JsonSerializer()
    flat = InMemoryStateStore(DictState(_data={"counter": 5})).to_dict(serializer)
    assert CHILD_STATES_KEY not in flat

    seeds = namespaced_seed_payloads(flat, namespaced_state_types(Childless()))
    assert seeds is not None
    assert set(seeds) == {()}
    assert seeds[()] == flat


def test_seed_payloads_splits_children_per_namespace() -> None:
    serializer = JsonSerializer()
    tree = {
        "store_type": "in_memory",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": {"_data": {"root_value": serializer.serialize("R")}},
        CHILD_STATES_KEY: {
            "child": {"_data": {"child_value": serializer.serialize("C")}}
        },
    }
    types = namespaced_state_types(Parent(child=Child()))
    seeds = namespaced_seed_payloads(tree, types)
    assert seeds is not None
    assert set(seeds) == {(), ("child",)}
    assert CHILD_STATES_KEY not in seeds[()]
    assert seeds[()]["state_data"]["_data"]["root_value"] == serializer.serialize("R")
    child_seed = seeds[("child",)]
    assert child_seed["store_type"] == "in_memory"
    assert child_seed["state_data"]["_data"]["child_value"] == serializer.serialize("C")


@pytest.mark.asyncio
async def test_flat_childless_checkpoint_carries_root_state_into_child_ful_run() -> (
    None
):
    serializer = JsonSerializer()
    flat = InMemoryStateStore(DictState(_data={"counter": 5})).to_dict(serializer)
    assert CHILD_STATES_KEY not in flat

    wf = Parent(child=Child())
    rebuilt = _namespaced(wf, serializer)
    rebuilt.add_seed_tree(flat, serializer)
    assert await rebuilt.view(()).get("counter") == 5
    assert await rebuilt.view(("child",)).get("child_value", None) is None


@pytest.mark.asyncio
async def test_childless_dict_state_to_dict_stays_flat() -> None:
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


@pytest.mark.asyncio
async def test_childless_typed_state_to_dict_stays_flat() -> None:
    wf = Childless()
    ctx = Context(wf)
    await wf.run(ctx=ctx)

    state = ctx.to_dict()["state"]
    assert state["store_type"] == "in_memory"
    assert state["state_type"] == "RootState"
    assert CHILD_STATES_KEY not in state
    assert Context.from_dict(wf, ctx.to_dict()) is not None
