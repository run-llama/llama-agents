# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Test checkpoint patches and the JSON state tree."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

import pytest
from pydantic import BaseModel, Field
from workflows.context import Context
from workflows.context.serializers import JsonSerializer
from workflows.context.state_store import (
    DictState,
    InMemoryStateStore,
    StateCheckpoint,
    apply_state_patch,
    decode_state,
    encode_state_tree,
)
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent
from workflows.plugins.basic import JournalRecord, basic_runtime
from workflows.workflow import Workflow

EQ_CALLS: list[str] = []


class Message(BaseModel):
    text: str
    tags: list[str] = Field(default_factory=list)

    def __eq__(self, other: object) -> bool:
        EQ_CALLS.append(self.text)
        return super().__eq__(other)

    __hash__ = None  # type: ignore[assignment]


class Conversation(BaseModel):
    title: str = ""
    messages: list[Message] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)


Mutation = Callable[[InMemoryStateStore[Any]], Awaitable[None]]


def seed(kind: str) -> BaseModel:
    messages = [Message(text=f"m{i}", tags=[f"t{i}"]) for i in range(4)]
    meta = {"a": 1, "b": {"c": [1, 2]}}
    if kind == "typed":
        return Conversation(title="hi", messages=messages, meta=meta)
    state = DictState()
    state["title"] = "hi"
    state["messages"] = messages
    state["meta"] = meta
    return state


def with_updates(state: BaseModel, **updates: Any) -> BaseModel:
    if isinstance(state, DictState):
        return DictState(_data={**state._data, **updates})
    return state.model_copy(update=updates)


def fresh_copy(state: BaseModel) -> BaseModel:
    serializer = JsonSerializer()
    return decode_state(encode_state_tree(state, serializer), serializer)


async def append_message(store: InMemoryStateStore[Any]) -> None:
    messages = await store.get("messages")
    await store.set("messages", [*messages, Message(text="new")])


async def edit_earlier_message(store: InMemoryStateStore[Any]) -> None:
    await store.set("messages.1.text", "edited")


async def set_state_model_copy(store: InMemoryStateStore[Any]) -> None:
    state = await store.get_state()
    await store.set_state(with_updates(state, title="renamed"))


async def edit_state_block(store: InMemoryStateStore[Any]) -> None:
    async with store.edit_state() as state:
        state.messages[2].tags.append("x")


async def set_fresh_validated_state(store: InMemoryStateStore[Any]) -> None:
    state = fresh_copy(await store.get_state())
    await store.set_state(with_updates(state, title="fresh"))


async def delete_dict_key(store: InMemoryStateStore[Any]) -> None:
    meta = await store.get("meta")
    await store.set("meta", {k: v for k, v in meta.items() if k != "a"})


async def insert_at_front(store: InMemoryStateStore[Any]) -> None:
    messages = await store.get("messages")
    await store.set("messages", [Message(text="first"), *messages])


MUTATIONS: list[Mutation] = [
    append_message,
    edit_earlier_message,
    set_state_model_copy,
    edit_state_block,
    set_fresh_validated_state,
    delete_dict_key,
    insert_at_front,
]


def assert_patch_round_trips(
    base: StateCheckpoint, current: StateCheckpoint
) -> list[dict[str, Any]]:
    patch = current.diff(base)
    patch = json.loads(json.dumps(patch))
    expected = json.loads(json.dumps(current.to_dict()))
    assert apply_state_patch(json.loads(json.dumps(base.to_dict())), patch) == expected
    return patch


@pytest.mark.parametrize("kind", ["dict", "typed"])
@pytest.mark.parametrize("mutation", MUTATIONS, ids=lambda m: m.__name__)
async def test_diff_round_trips_after_each_write(kind: str, mutation: Mutation) -> None:
    store = InMemoryStateStore(seed(kind))
    base = store.checkpoint()
    await mutation(store)
    patch = assert_patch_round_trips(base, store.checkpoint())
    assert patch


@pytest.mark.parametrize("kind", ["dict", "typed"])
async def test_diff_round_trips_across_a_sequence_of_writes(kind: str) -> None:
    store = InMemoryStateStore(seed(kind))
    first = previous = store.checkpoint()
    for mutation in MUTATIONS:
        await mutation(store)
        current = store.checkpoint()
        assert_patch_round_trips(previous, current)
        previous = current
    assert_patch_round_trips(first, previous)


@pytest.mark.parametrize("kind", ["dict", "typed"])
async def test_append_does_not_compare_untouched_prefix(kind: str) -> None:
    store = InMemoryStateStore(seed(kind))
    base = store.checkpoint()
    await append_message(store)
    EQ_CALLS.clear()
    patch = store.checkpoint().diff(base)
    assert EQ_CALLS == []
    assert [(op["op"], op["path"]) for op in patch] == [
        (
            "add",
            "/state_data/_data/messages/4"
            if kind == "dict"
            else "/state_data/value/messages/4",
        )
    ]


@pytest.mark.parametrize("kind", ["dict", "typed"])
async def test_fresh_equal_state_diffs_to_the_changed_field_only(kind: str) -> None:
    store = InMemoryStateStore(seed(kind))
    base = store.checkpoint()
    await set_fresh_validated_state(store)
    patch = store.checkpoint().diff(base)
    assert [op["op"] for op in patch] == ["replace"]
    assert patch[0]["value"] == "fresh"


async def test_unchanged_checkpoint_diffs_to_empty_patch() -> None:
    store = InMemoryStateStore(seed("typed"))
    assert store.checkpoint().diff(store.checkpoint()) == []


async def test_nested_model_values_in_dict_state_use_serialize_value_form() -> None:
    store = InMemoryStateStore(seed("dict"))
    base = store.checkpoint()
    await append_message(store)
    (op,) = store.checkpoint().diff(base)
    assert op["value"] == JsonSerializer().serialize_value(Message(text="new"))


def test_tree_payload_decodes_for_both_state_kinds() -> None:
    serializer = JsonSerializer()
    for kind in ("dict", "typed"):
        state = seed(kind)
        payload = StateCheckpoint(state, serializer).to_dict()
        restored = InMemoryStateStore.from_dict(payload, serializer)
        assert restored.checkpoint().to_dict() == payload


def test_apply_state_patch_leaves_input_unchanged() -> None:
    state = {"state_data": {"_data": {"xs": [1, 2]}}}
    patched = apply_state_patch(
        state,
        [
            {"op": "add", "path": "/state_data/_data/xs/2", "value": 3},
            {"op": "replace", "path": "/state_data/_data/xs/0", "value": 0},
        ],
    )
    assert patched == {"state_data": {"_data": {"xs": [0, 2, 3]}}}
    assert state == {"state_data": {"_data": {"xs": [1, 2]}}}


class NotesWorkflow(Workflow):
    @step
    async def write(self, ctx: Context, ev: StartEvent) -> StopEvent:
        notes = await ctx.store.get("notes", default=[])
        await ctx.store.set("notes", [*notes, len(notes)])
        return StopEvent(result=len(notes) + 1)


async def test_context_from_dict_accepts_checkpoint_state() -> None:
    wf = NotesWorkflow()
    handler = wf.run()
    await handler
    checkpoint = basic_runtime.state_checkpoint(handler.run_id)
    assert handler.ctx is not None
    snapshot = handler.ctx.to_dict()
    snapshot["state"] = json.loads(json.dumps(checkpoint.to_dict()))
    assert await wf.run(ctx=Context.from_dict(wf, snapshot)) == 2

    assert await wf.run(ctx=Context.from_dict(wf, handler.ctx.to_dict())) == 2


class Tick(Event):
    n: int


class PausingNotesWorkflow(Workflow):
    """Write one note per step, idempotently, and pause before the last step."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.gate = asyncio.Event()
        self.paused = asyncio.Event()

    @step
    async def first(self, ctx: Context, ev: StartEvent) -> Tick:
        await ctx.store.set("note_0", "first")
        return Tick(n=1)

    @step
    async def second(self, ctx: Context, ev: Tick) -> Tick | StopEvent:
        await ctx.store.set(f"note_{ev.n}", f"second-{ev.n}")
        if ev.n == 1:
            return Tick(n=2)
        self.paused.set()
        await self.gate.wait()
        return StopEvent(result=ev.n)


async def test_resume_from_state_rebuilt_from_patches() -> None:
    """Persist the journal plus state patches, then rebuild and resume from them."""
    wf = PausingNotesWorkflow()
    handler = wf.run()
    run_id = handler.run_id
    base = basic_runtime.state_checkpoint(run_id)
    stored_state = json.loads(json.dumps(base.to_dict()))
    patches: list[list[dict[str, Any]]] = []

    await asyncio.wait_for(wf.paused.wait(), timeout=5)
    cur = basic_runtime.state_checkpoint(run_id)
    patches.append(json.loads(json.dumps(cur.diff(base))))
    records = _journal_so_far(run_id)
    wf.gate.set()
    assert await handler == 2

    for patch in patches:
        stored_state = apply_state_patch(stored_state, patch)
    snapshot = basic_runtime.restore(wf, None, records).to_dict(include_state=False)
    snapshot["state"] = stored_state

    resumed = PausingNotesWorkflow()
    resumed.gate.set()
    ctx = Context.from_dict(resumed, json.loads(json.dumps(snapshot)))
    assert await resumed.run(ctx=ctx) == 2
    assert handler.ctx is not None
    for key in ("note_0", "note_1", "note_2"):
        assert await ctx.store.get(key) == await handler.ctx.store.get(key)


def _journal_so_far(run_id: str) -> list[JournalRecord]:
    """The records journaled so far, without waiting for the run to end."""
    queues = basic_runtime._queues[run_id]
    serializer = queues.serializer
    assert serializer is not None
    start = queues.init_state.journal_seq
    return [
        JournalRecord(seq=start + i, data=serializer.serialize(tick))
        for i, tick in enumerate(list(queues.ticks))
    ]


def test_state_checkpoint_rejects_unknown_run() -> None:
    with pytest.raises(RuntimeError, match="No active workflow"):
        basic_runtime.state_checkpoint("missing-run")
