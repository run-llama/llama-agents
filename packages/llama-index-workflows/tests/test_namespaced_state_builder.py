# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Unit tests for namespaced-state stores and seed conversion."""

from __future__ import annotations

import pytest
from pydantic import BaseModel
from workflows import Context, Workflow
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import (
    BUNDLE_ROOT_KEY,
    BUNDLE_VERSION,
    BUNDLE_VERSION_KEY,
    CHILD_STATES_KEY,
    DictState,
    InMemoryStateStore,
    StateRecord,
    build_namespaced_state,
    join_state_bundle,
    namespaced_seed_blob,
    namespaced_seed_payload,
    namespaced_state_types,
    namespaced_underlying_state_type,
    split_state_bundle,
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


def test_state_types_and_underlying_type_for_childless() -> None:
    wf = Childless()
    assert set(namespaced_state_types(wf)) == {()}
    assert namespaced_underlying_state_type(wf) is RootState


def test_state_types_and_underlying_type_for_child_ful() -> None:
    wf = Parent(child=Child())
    types = namespaced_state_types(wf)
    assert set(types) == {(), ("child",)}
    assert namespaced_underlying_state_type(wf) is DictState


def test_single_namespace_view_resolves_to_underlying_flat() -> None:
    wf = Childless()
    underlying = InMemoryStateStore(RootState(root_value="x"))
    namespaced = build_namespaced_state(wf, underlying, JsonSerializer())

    assert namespaced.is_single_namespace
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
    assert tree["state_data"]["_data"]["root_value"] == serializer.serialize("R")
    child_states = tree[CHILD_STATES_KEY]
    assert set(child_states) == {"child"}
    assert child_states["child"]["_data"]["child_value"] == (serializer.serialize("C"))
    assert "data" not in child_states["child"]
    assert "state_type" not in child_states["child"]
    assert "state_module" not in child_states["child"]


@pytest.mark.asyncio
async def test_seed_round_trip_rebuilds_slots() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    underlying = InMemoryStateStore(DictState())
    namespaced = build_namespaced_state(wf, underlying, serializer)
    await namespaced.view(()).set("root_value", "R")
    await namespaced.view(("child",)).set("child_value", "C")

    tree = namespaced.serialize_tree(serializer)

    seed = namespaced_seed_payload(tree, serializer, child_ful=True)
    assert seed is not None
    assert seed == tree

    rebuilt = build_namespaced_state(
        wf, InMemoryStateStore.from_dict(seed, serializer), serializer
    )
    assert await rebuilt.view(()).get("root_value") == "R"
    assert await rebuilt.view(("child",)).get("child_value") == "C"


@pytest.mark.asyncio
async def test_typed_root_round_trip_keeps_root_metadata_with_children() -> None:
    wf = TypedParent(child=Child())
    serializer = JsonSerializer()
    underlying = InMemoryStateStore(RootState(root_value="initial"))
    namespaced = build_namespaced_state(wf, underlying, serializer)

    await namespaced.view(()).set("root_value", "R")
    await namespaced.view(("child",)).set("child_value", "C")

    tree = namespaced.serialize_tree(serializer)
    assert tree["state_type"] == "RootState"
    assert tree["state_module"] == __name__
    assert CHILD_STATES_KEY in tree

    rebuilt = build_namespaced_state(
        wf, InMemoryStateStore.from_dict(tree, serializer), serializer
    )
    root_state = await rebuilt.view(()).get_state()
    assert isinstance(root_state, RootState)
    assert root_state.root_value == "R"
    assert await rebuilt.view(("child",)).get("child_value") == "C"


@pytest.mark.asyncio
async def test_child_view_uses_standard_facade_operations() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    namespaced = build_namespaced_state(wf, InMemoryStateStore(DictState()), serializer)
    child_store = namespaced.view(("child",))

    await child_store.set_state(DictState(_data={"a": 1}))
    async with child_store.edit_state() as state:
        state._data["b"] = 2
    assert await child_store.get("a") == 1
    assert await child_store.get("b") == 2

    await child_store.clear()
    assert await child_store.get("a", None) is None
    assert await child_store.get("b", None) is None


def test_seed_helpers_noop_for_childless_and_references() -> None:
    serializer = JsonSerializer()
    flat = InMemoryStateStore(RootState(root_value="x")).to_dict(serializer)
    assert namespaced_seed_blob(flat, child_ful=False) is None
    assert namespaced_seed_payload(flat, serializer, child_ful=False) is None
    assert (
        namespaced_seed_blob({"store_type": "postgres", "run_id": "r"}, child_ful=True)
        is None
    )
    assert namespaced_seed_blob(None, child_ful=True) is None


@pytest.mark.asyncio
async def test_flat_childless_checkpoint_carries_root_state_into_child_ful_run() -> (
    None
):
    serializer = JsonSerializer()
    flat = InMemoryStateStore(DictState(_data={"counter": 5})).to_dict(serializer)
    assert CHILD_STATES_KEY not in flat

    blob = namespaced_seed_blob(flat, child_ful=True)
    assert blob is not None
    assert blob == flat

    wf = Parent(child=Child())
    rebuilt = build_namespaced_state(
        wf, InMemoryStateStore.from_dict(blob, serializer), serializer
    )
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


def test_seed_payload_matches_blob_dump() -> None:
    serializer = JsonSerializer()
    tree = {
        "store_type": "in_memory",
        "state_type": "DictState",
        "state_module": "workflows.context.state_store",
        "state_data": {"_data": {"root_value": serializer.serialize("R")}},
        CHILD_STATES_KEY: {"child": {"_data": {}}},
    }
    blob = namespaced_seed_blob(tree, child_ful=True)
    assert blob == tree
    payload = namespaced_seed_payload(tree, serializer, child_ful=True)
    assert payload == tree


def test_bundle_split_join_appends_children_to_object_root() -> None:
    root = StateRecord(
        data={"_data": {"root_value": '"R"'}},
        state_type="DictState",
        state_module="workflows.context.state_store",
    )
    child = {"_data": {"child_value": '"C"'}}

    bundle = split_state_bundle(root).model_copy(update={"children": {"child": child}})
    joined = join_state_bundle(bundle)

    assert joined.state_type == "DictState"
    assert joined.data["_data"]["root_value"] == '"R"'
    assert joined.data[CHILD_STATES_KEY]["child"] == child
    split = split_state_bundle(joined)
    assert split.root.data == {"_data": {"root_value": '"R"'}}
    assert split.children["child"] == {"_data": {"child_value": '"C"'}}


def test_bundle_split_join_wraps_opaque_root() -> None:
    root = StateRecord(data="not-json", state_type="Opaque", state_module="app")
    child = {"_data": {"child_value": '"C"'}}

    bundle = split_state_bundle(root).model_copy(update={"children": {"child": child}})
    joined = join_state_bundle(bundle)

    assert isinstance(joined.data, str)
    split = split_state_bundle(joined)
    assert split.root.data == "not-json"
    assert split.root.state_type == "Opaque"
    assert split.children["child"] == {"_data": {"child_value": '"C"'}}

    parsed = JsonSerializer().deserialize(joined.data)
    assert parsed[BUNDLE_VERSION_KEY] == BUNDLE_VERSION
    assert parsed[BUNDLE_ROOT_KEY] == "not-json"
    assert parsed[CHILD_STATES_KEY]["child"] == child


def test_bundle_split_accepts_previous_child_record_shape() -> None:
    root = StateRecord(
        data={
            "_data": {"root_value": '"R"'},
            CHILD_STATES_KEY: {
                "child": {
                    "data": {"_data": {"child_value": '"C"'}},
                    "state_type": "DictState",
                    "state_module": "workflows.context.state_store",
                }
            },
        },
        state_type="DictState",
        state_module="workflows.context.state_store",
    )

    split = split_state_bundle(root)

    assert split.root.data == {"_data": {"root_value": '"R"'}}
    assert split.children["child"] == {"_data": {"child_value": '"C"'}}


def test_bundle_split_keeps_compact_child_payload_with_state_data_key() -> None:
    child_data = {"state_data": "user value", "other": "field"}
    root = StateRecord(
        data={
            "_data": {"root_value": '"R"'},
            CHILD_STATES_KEY: {"child": child_data},
        },
        state_type="DictState",
        state_module="workflows.context.state_store",
    )

    split = split_state_bundle(root)

    assert split.children["child"] == child_data


@pytest.mark.asyncio
async def test_child_first_write_stores_compact_json_child_and_root_metadata() -> None:
    wf = Parent(child=Child())
    serializer = JsonSerializer()
    namespaced = build_namespaced_state(wf, InMemoryStateStore(DictState()), serializer)

    await namespaced.view(("child",)).set("child_value", "C")

    underlying = namespaced.underlying
    assert isinstance(underlying, InMemoryStateStore)
    record = underlying._memory_storage.load_sync()
    assert record is not None
    assert record.state_type == "DictState"
    assert record.state_module == "workflows.context.state_store"
    bundle = split_state_bundle(record)
    assert bundle.children["child"]["_data"]["child_value"] == (
        serializer.serialize("C")
    )
    assert "data" not in bundle.children["child"]
