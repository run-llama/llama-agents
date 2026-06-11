# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Minimal unit tests for InMemoryStateStore.

Full state store protocol tests are in the integration test package
(llama-index-integration-tests/tests/test_state_store_matrix.py),
which tests InMemoryStateStore alongside SqlStateStore.

These tests provide fast feedback during development of the base package.
"""

from __future__ import annotations

import pytest
from workflows.context.state_store import (
    DictState,
    InMemoryStateStore,
    get_by_path,
    set_by_path,
)
from workflows.events import DictLikeModel, StopEvent


@pytest.mark.asyncio
async def test_in_memory_state_store_smoke() -> None:
    """Smoke test for basic InMemoryStateStore functionality."""
    store: InMemoryStateStore[DictState] = InMemoryStateStore(DictState())

    # Basic get/set
    await store.set("name", "test")
    assert await store.get("name") == "test"

    # Nested path
    await store.set("nested", {"key": "value"})
    assert await store.get("nested.key") == "value"

    # Default on missing
    assert await store.get("missing", default=None) is None

    # Clear
    await store.clear()
    assert await store.get("name", default=None) is None


@pytest.mark.asyncio
async def test_in_memory_edit_state() -> None:
    """Test edit_state context manager."""
    store: InMemoryStateStore[DictState] = InMemoryStateStore(DictState())

    async with store.edit_state() as state:
        state["counter"] = 1

    assert await store.get("counter") == 1


@pytest.mark.asyncio
async def test_state_keys_named_after_mapping_methods() -> None:
    """Keys colliding with DictLikeModel method names return stored values, not bound methods."""
    store: InMemoryStateStore[DictState] = InMemoryStateStore(DictState())

    for key, value in {
        "items": [1, 2, 3],
        "keys": "abc",
        "values": {"x": 1},
        "get": 42,
        "model_dump": "shadowed",
    }.items():
        await store.set(key, value)
        assert await store.get(key) == value


@pytest.mark.asyncio
async def test_missing_method_named_key_is_missing() -> None:
    """An unset key that collides with a method name behaves like any missing key."""
    store: InMemoryStateStore[DictState] = InMemoryStateStore(DictState())

    assert await store.get("items", default=None) is None
    with pytest.raises(ValueError):
        await store.get("keys")


@pytest.mark.asyncio
async def test_nested_set_through_method_named_segment() -> None:
    """Setting a nested path under a method-named key creates the intermediate dict."""
    store: InMemoryStateStore[DictState] = InMemoryStateStore(DictState())

    await store.set("items.nested", 1)
    assert await store.get("items.nested") == 1
    assert await store.get("items") == {"nested": 1}


@pytest.mark.asyncio
async def test_numeric_string_key_roundtrip() -> None:
    """Numeric string keys on DictState round-trip through set/get."""
    store: InMemoryStateStore[DictState] = InMemoryStateStore(DictState())

    await store.set("0", "zero")
    assert await store.get("0") == "zero"


def test_get_by_path_typed_dict_like_model() -> None:
    """Declared fields and properties on DictLikeModel subclasses stay reachable."""

    class TypedModel(DictLikeModel):
        foo: int = 7

    model = TypedModel()
    model["dynamic"] = "bar"
    assert get_by_path(model, "foo") == 7
    assert get_by_path(model, "dynamic") == "bar"

    # StopEvent.result is a @property backed by a private attr
    state = DictState()
    set_by_path(state, "ev", StopEvent(result=42))
    assert get_by_path(state, "ev.result") == 42


def test_set_by_path_typed_field_assigns_field() -> None:
    """set_by_path writes declared fields as fields, not shadow _data entries."""

    class TypedModel(DictLikeModel):
        foo: int = 7

    model = TypedModel()
    set_by_path(model, "foo", 9)
    assert model.foo == 9
    assert "foo" not in model._data
