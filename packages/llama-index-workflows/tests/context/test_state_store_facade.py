# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import DictState, InMemoryStateStore
from workflows.context.state_store_integration import (
    StateRecord,
    StateStoreFacade,
)


class TwoFieldState(BaseModel):
    a: int = 0
    b: int = 0


class SeedState(BaseModel):
    x: int = 0


class FakeDurableStorage:
    """Minimal durable StateStorage fake: counts saves, records copies."""

    def __init__(self, run_id: str = "run-1") -> None:
        self.run_id = run_id
        self.record: StateRecord | None = None
        self.save_count = 0
        self.copied_handles: list[str] = []

    async def load(self) -> StateRecord | None:
        return self.record.model_copy() if self.record is not None else None

    async def save(self, record: StateRecord) -> None:
        self.save_count += 1
        self.record = record.model_copy()

    def to_handle(self) -> dict[str, Any]:
        return {"store_type": "fake", "run_id": self.run_id}

    def parse_own_handle(self, payload: dict[str, Any]) -> str | None:
        if payload.get("store_type") == "fake":
            return payload["run_id"]
        return None

    async def copy_from_handle(self, handle: str) -> None:
        self.copied_handles.append(handle)


@pytest.fixture()
def serializer() -> JsonSerializer:
    return JsonSerializer()


def make_facade(
    storage: FakeDurableStorage, state_type: type[BaseModel] = DictState
) -> StateStoreFacade[Any]:
    return StateStoreFacade(storage, state_type, JsonSerializer())


def in_memory_seed_payload(serializer: BaseSerializer, **items: Any) -> dict[str, Any]:
    return InMemoryStateStore(DictState(**items)).to_dict(serializer)


# ============================================================================
# Locking: torn reads, reads inside edit_state, nested writers
# ============================================================================


@pytest.mark.asyncio
async def test_concurrent_get_state_never_observes_torn_edit() -> None:
    """A concurrent reader must see both-old or both-new, never a half edit."""
    store = InMemoryStateStore(TwoFieldState())
    mid_edit = asyncio.Event()
    finish_edit = asyncio.Event()

    async def editor() -> None:
        async with store.edit_state() as state:
            state.a = 1
            mid_edit.set()
            await finish_edit.wait()
            state.b = 1

    async def reader() -> tuple[int, int]:
        await mid_edit.wait()
        state = await store.get_state()
        return (state.a, state.b)

    edit_task = asyncio.create_task(editor())
    read_task = asyncio.create_task(reader())
    await asyncio.wait_for(mid_edit.wait(), timeout=2.0)
    # Give the reader a chance to run while the edit is mid-flight.
    await asyncio.sleep(0)
    finish_edit.set()
    observed = await asyncio.wait_for(read_task, timeout=2.0)
    await asyncio.wait_for(edit_task, timeout=2.0)

    assert observed in {(0, 0), (1, 1)}


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_get_inside_edit_state(durable: bool) -> None:
    store: Any = (
        make_facade(FakeDurableStorage())
        if durable
        else InMemoryStateStore(DictState())
    )
    await store.set("counter", 1)

    async def nested_read() -> Any:
        async with store.edit_state() as state:
            state["other"] = 2
            return await store.get("counter")

    assert await asyncio.wait_for(nested_read(), timeout=2.0) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_nested_writers_inside_edit_state_raise(durable: bool) -> None:
    store: Any = (
        make_facade(FakeDurableStorage())
        if durable
        else InMemoryStateStore(DictState())
    )

    async def run_nested_writers() -> None:
        async with store.edit_state() as state:
            state["touched"] = True
            with pytest.raises(RuntimeError, match="edit_state"):
                await store.set("foo", 1)
            with pytest.raises(RuntimeError, match="edit_state"):
                await store.set_state(DictState())
            with pytest.raises(RuntimeError, match="edit_state"):
                await store.clear()
            with pytest.raises(RuntimeError, match="edit_state"):
                async with store.edit_state():
                    pass

    await asyncio.wait_for(run_nested_writers(), timeout=2.0)
    assert await store.get("touched") is True


# ============================================================================
# Reads are pure: no default-row persistence from the read path
# ============================================================================


@pytest.mark.asyncio
async def test_read_on_empty_durable_storage_leaves_no_record() -> None:
    storage = FakeDurableStorage()
    facade = make_facade(storage)

    state = await facade.get_state()
    assert isinstance(state, DictState)
    assert await facade.get("missing", default=None) is None

    assert storage.save_count == 0
    assert storage.record is None


class FirstLoadGatedStorage(FakeDurableStorage):
    """Captures the first load's result, then blocks until released."""

    def __init__(self) -> None:
        super().__init__()
        self.first_load_started = asyncio.Event()
        self.release_first_load = asyncio.Event()
        self._loads = 0

    async def load(self) -> StateRecord | None:
        result = await super().load()
        self._loads += 1
        if self._loads == 1:
            self.first_load_started.set()
            await self.release_first_load.wait()
        return result


@pytest.mark.asyncio
async def test_read_racing_first_write_never_clobbers_the_write() -> None:
    """A read that observed empty storage must not persist a default over a
    write that committed while the read was in flight."""
    storage = FirstLoadGatedStorage()
    facade = make_facade(storage, SeedState)

    read_task = asyncio.create_task(facade.get_state())
    await asyncio.wait_for(storage.first_load_started.wait(), timeout=2.0)

    await asyncio.wait_for(facade.set_state(SeedState(x=1)), timeout=2.0)
    storage.release_first_load.set()
    await asyncio.wait_for(read_task, timeout=2.0)

    final = await facade.get_state()
    assert final.x == 1


# ============================================================================
# Seed lifecycle
# ============================================================================


@pytest.mark.asyncio
async def test_add_seed_foreign_durable_handle_raises_immediately(
    serializer: JsonSerializer,
) -> None:
    facade = make_facade(FakeDurableStorage())
    with pytest.raises(ValueError, match="sqlite"):
        facade.add_seed({"store_type": "sqlite", "run_id": "other"}, serializer)


@pytest.mark.asyncio
async def test_add_seed_empty_payload_raises(serializer: JsonSerializer) -> None:
    facade = make_facade(FakeDurableStorage())
    with pytest.raises(ValueError):
        facade.add_seed({}, serializer)


@pytest.mark.asyncio
async def test_add_seed_bad_in_memory_payload_raises_immediately(
    serializer: JsonSerializer,
) -> None:
    facade = make_facade(FakeDurableStorage())
    with pytest.raises(ValueError):
        facade.add_seed(
            {"store_type": "in_memory", "state_type": {"not": "a-string"}}, serializer
        )


@pytest.mark.asyncio
async def test_in_memory_store_add_seed_rejects_durable_payload(
    serializer: JsonSerializer,
) -> None:
    store = InMemoryStateStore(DictState())
    with pytest.raises(ValueError, match="fake"):
        store.add_seed({"store_type": "fake", "run_id": "run-1"}, serializer)


@pytest.mark.asyncio
async def test_seed_not_written_until_first_async_access(
    serializer: JsonSerializer,
) -> None:
    storage = FakeDurableStorage()
    facade = make_facade(storage)
    facade.add_seed(in_memory_seed_payload(serializer, counter=42), serializer)

    assert storage.save_count == 0

    assert await facade.get("counter") == 42
    assert storage.save_count == 1

    # Subsequent access does not re-materialize.
    assert await facade.get("counter") == 42
    assert storage.save_count == 1


class GatedSaveStorage(FakeDurableStorage):
    """Save blocks until released, exposing the seed-materialization window."""

    def __init__(self) -> None:
        super().__init__()
        self.save_started = asyncio.Event()
        self.release_save = asyncio.Event()

    async def save(self, record: StateRecord) -> None:
        self.save_started.set()
        await self.release_save.wait()
        await super().save(record)


@pytest.mark.asyncio
async def test_seed_materializes_exactly_once_under_concurrency(
    serializer: JsonSerializer,
) -> None:
    storage = GatedSaveStorage()
    facade = make_facade(storage)
    facade.add_seed(in_memory_seed_payload(serializer, counter=42), serializer)

    tasks = [asyncio.create_task(facade.get("counter")) for _ in range(5)]
    await asyncio.wait_for(storage.save_started.wait(), timeout=2.0)
    # Let the other readers reach ensure_seeded while the save is in flight.
    await asyncio.sleep(0)
    storage.release_save.set()

    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)
    assert results == [42] * 5
    assert storage.save_count == 1


@pytest.mark.asyncio
async def test_seed_own_handle_same_target_is_noop(
    serializer: JsonSerializer,
) -> None:
    storage = FakeDurableStorage(run_id="run-1")
    facade = make_facade(storage)
    facade.add_seed({"store_type": "fake", "run_id": "run-1"}, serializer)

    await facade.get_state()

    assert storage.copied_handles == []
    assert storage.save_count == 0


@pytest.mark.asyncio
async def test_seed_own_handle_other_target_copies_once(
    serializer: JsonSerializer,
) -> None:
    storage = FakeDurableStorage(run_id="run-1")
    facade = make_facade(storage)
    facade.add_seed({"store_type": "fake", "run_id": "run-2"}, serializer)

    await facade.get_state()
    await facade.get_state()

    assert storage.copied_handles == ["run-2"]


@pytest.mark.asyncio
async def test_seed_wins_over_existing_row(serializer: JsonSerializer) -> None:
    storage = FakeDurableStorage()
    facade = make_facade(storage)
    await facade.set("counter", 1)

    facade.add_seed(in_memory_seed_payload(serializer, counter=99), serializer)

    assert await facade.get("counter") == 99


# ============================================================================
# Handoff dispatch on storage durability
# ============================================================================


@pytest.mark.asyncio
async def test_serialize_for_handoff_durable_returns_handle(
    serializer: JsonSerializer,
) -> None:
    storage = FakeDurableStorage(run_id="run-7")
    facade = make_facade(storage)

    assert await facade.serialize_for_handoff(serializer) == {
        "store_type": "fake",
        "run_id": "run-7",
    }
    assert facade.to_dict(serializer) == {"store_type": "fake", "run_id": "run-7"}


@pytest.mark.asyncio
async def test_serialize_for_handoff_materializes_pending_seed(
    serializer: JsonSerializer,
) -> None:
    storage = FakeDurableStorage()
    facade = make_facade(storage)
    facade.add_seed(in_memory_seed_payload(serializer, counter=5), serializer)

    handle = await facade.serialize_for_handoff(serializer)

    assert handle == {"store_type": "fake", "run_id": "run-1"}
    assert storage.save_count == 1


@pytest.mark.asyncio
async def test_serialize_for_handoff_in_memory_returns_snapshot(
    serializer: JsonSerializer,
) -> None:
    store = InMemoryStateStore(DictState())
    await store.set("counter", 3)

    payload = await store.serialize_for_handoff(serializer)

    assert payload["store_type"] == "in_memory"
    restored = InMemoryStateStore.from_dict(payload, serializer)
    assert await restored.get("counter") == 3
