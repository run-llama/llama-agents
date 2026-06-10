# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from llama_agents.client.protocol.serializable_events import EventEnvelopeWithMetadata
from llama_agents.server._pool import PoolProvider
from llama_agents.server._store.abstract_workflow_store import (
    HandlerQuery,
    PersistentHandler,
    Status,
)
from llama_agents.server._store.postgres_workflow_store import PostgresWorkflowStore
from server_test_fixtures import wait_for_passing  # type: ignore[import]
from workflows.events import Event, StopEvent


def _make_event() -> EventEnvelopeWithMetadata:
    class TestEvent(Event):
        key: str = "value"

    return EventEnvelopeWithMetadata.from_event(TestEvent())


def _make_stop_event() -> EventEnvelopeWithMetadata:
    return EventEnvelopeWithMetadata.from_event(StopEvent(result="done"))


def _make_handler(
    handler_id: str = "h1",
    workflow_name: str = "test_workflow",
    status: Status = "running",
    run_id: str = "run-1",
    started_at: datetime | None = None,
    updated_at: datetime | None = None,
    completed_at: datetime | None = None,
    idle_since: datetime | None = None,
    error: str | None = None,
) -> PersistentHandler:
    now = datetime.now(timezone.utc)
    return PersistentHandler(
        handler_id=handler_id,
        workflow_name=workflow_name,
        status=status,
        run_id=run_id,
        started_at=started_at or now,
        updated_at=updated_at or now,
        completed_at=completed_at,
        idle_since=idle_since,
        error=error,
    )


# ── Unit tests (no Postgres needed, test logic with mocks) ──────────


async def test_create_state_store_without_pool_raises() -> None:
    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")
    with pytest.raises(RuntimeError, match="pool not initialized"):
        store.create_state_store("run-1")


async def test_build_filters_empty_in_returns_none() -> None:
    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")
    assert store._build_filters(HandlerQuery(handler_id_in=[])) is None
    assert store._build_filters(HandlerQuery(run_id_in=[])) is None
    assert store._build_filters(HandlerQuery(status_in=[])) is None
    assert store._build_filters(HandlerQuery(workflow_name_in=[])) is None


async def test_build_filters_produces_correct_clauses() -> None:
    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")

    result = store._build_filters(HandlerQuery(handler_id_in=["h1", "h2"]))
    assert result is not None
    clauses, params = result
    assert len(clauses) == 1
    assert "handler_id IN" in clauses[0]
    assert params == ["h1", "h2"]

    result = store._build_filters(HandlerQuery(is_idle=True))
    assert result is not None
    clauses, params = result
    assert "idle_since IS NOT NULL" in clauses[0]
    assert params == []

    result = store._build_filters(HandlerQuery(is_idle=False))
    assert result is not None
    clauses, params = result
    assert "idle_since IS NULL" in clauses[0]


async def test_on_notify_wakes_condition() -> None:
    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")
    condition = store._get_or_create_condition("run-1")

    woken = False
    waiting = asyncio.Event()

    async def waiter() -> None:
        nonlocal woken
        async with condition:
            waiting.set()
            await condition.wait()
            woken = True

    task = asyncio.create_task(waiter())
    await waiting.wait()
    async with condition:
        pass

    # Simulate the NOTIFY callback
    store._on_notify(MagicMock(), 0, "wf_events", "run-1")

    await asyncio.wait_for(task, timeout=1.0)
    assert woken


async def test_close_without_start_is_safe() -> None:
    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")
    await store.close()  # Should not raise


async def test_borrowed_pool_not_closed_on_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When constructed with a borrowed provider, the borrowed pool is never closed."""
    fake_pool = MagicMock()
    fake_pool.close = MagicMock()  # would be awaited if called
    factory_calls = 0

    async def factory() -> Any:
        nonlocal factory_calls
        factory_calls += 1
        return fake_pool

    # _setup_listener acquires from the pool — short-circuit it for this unit test.
    async def noop_setup_listener(self: PostgresWorkflowStore) -> None:
        return None

    monkeypatch.setattr(PostgresWorkflowStore, "_setup_listener", noop_setup_listener)

    store = PostgresWorkflowStore(
        dsn="postgresql://localhost/test",
        pool=PoolProvider.borrowed(factory),
        auto_migrate=False,
    )

    await store.start()
    assert factory_calls == 1
    assert store._pool is fake_pool

    await store.close()
    fake_pool.close.assert_not_called()
    assert store._pool is None


async def test_listen_termination_callback_schedules_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the LISTEN conn drops, _on_listen_termination must schedule reconnect."""
    reconnect_called = asyncio.Event()

    async def fake_reconnect(self: PostgresWorkflowStore) -> None:
        reconnect_called.set()

    monkeypatch.setattr(PostgresWorkflowStore, "_reconnect_listener", fake_reconnect)

    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")
    store._on_listen_termination(MagicMock())

    await asyncio.wait_for(reconnect_called.wait(), timeout=1.0)


async def test_listen_termination_callback_noops_when_closing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The closing flag suppresses reconnect attempts during teardown."""
    called = False

    async def fake_reconnect(self: PostgresWorkflowStore) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(PostgresWorkflowStore, "_reconnect_listener", fake_reconnect)

    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")
    store._closing = True
    store._on_listen_termination(MagicMock())
    await asyncio.sleep(0.01)
    assert called is False


async def test_reconnect_listener_wakes_subscribers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a successful reconnect, all active subscribe_events conditions are notified."""

    async def fake_setup_listener(self: PostgresWorkflowStore) -> None:
        return None

    monkeypatch.setattr(PostgresWorkflowStore, "_setup_listener", fake_setup_listener)

    store = PostgresWorkflowStore(dsn="postgresql://localhost/test")
    # Pretend the pool exists so _reconnect_listener proceeds.
    store._pool = cast(Any, MagicMock())

    # Hold strong references to the conditions; the store keeps weak refs only.
    cond_a = store._get_or_create_condition("run-a")
    cond_b = store._get_or_create_condition("run-b")

    woken = {"a": False, "b": False}

    async def waiter(name: str, cond: asyncio.Condition) -> None:
        async with cond:
            await cond.wait()
            woken[name] = True

    task_a = asyncio.create_task(waiter("a", cond_a))
    task_b = asyncio.create_task(waiter("b", cond_b))
    await asyncio.sleep(0.01)

    await store._reconnect_listener()

    await asyncio.wait_for(asyncio.gather(task_a, task_b), timeout=1.0)
    assert woken == {"a": True, "b": True}


# ── Integration tests (require Docker) ──────────────────────────────


@pytest.mark.docker
async def test_integration_migrations_idempotent(postgres_dsn: str) -> None:
    store = PostgresWorkflowStore(dsn=postgres_dsn, schema="test_pg_store")
    try:
        await store.start()
        await store.run_migrations()
        await store.run_migrations()  # Should be idempotent
    finally:
        await store.close()


@pytest.mark.docker
async def test_integration_handler_crud(postgres_dsn: str) -> None:
    store = PostgresWorkflowStore(dsn=postgres_dsn, schema="test_pg_store")
    try:
        await store.start()
        await store.run_migrations()

        handler = _make_handler(handler_id="pg-h1", run_id="pg-run-1")
        await store.update(handler)

        results = await store.query(HandlerQuery(handler_id_in=["pg-h1"]))
        assert len(results) == 1
        assert results[0].handler_id == "pg-h1"

        count = await store.delete(HandlerQuery(handler_id_in=["pg-h1"]))
        assert count == 1

        results = await store.query(HandlerQuery(handler_id_in=["pg-h1"]))
        assert len(results) == 0
    finally:
        await store.close()


@pytest.mark.docker
async def test_integration_event_append_and_query(postgres_dsn: str) -> None:
    store = PostgresWorkflowStore(dsn=postgres_dsn, schema="test_pg_store")
    try:
        await store.start()
        await store.run_migrations()

        await store.append_event("pg-run-ev", _make_event())
        await store.append_event("pg-run-ev", _make_event())
        await store.append_event("pg-run-ev", _make_event())

        events = await store.query_events("pg-run-ev")
        assert len(events) == 3
        assert events[0].sequence == 0
        assert events[1].sequence == 1
        assert events[2].sequence == 2

        events = await store.query_events("pg-run-ev", after_sequence=0, limit=1)
        assert len(events) == 1
        assert events[0].sequence == 1
    finally:
        await store.close()


@pytest.mark.docker
async def test_integration_subscribe_events(postgres_dsn: str) -> None:
    store = PostgresWorkflowStore(
        dsn=postgres_dsn, schema="test_pg_store", poll_interval=0.05
    )
    try:
        await store.start()
        await store.run_migrations()

        run_id = "pg-run-sub"

        async def subscribe() -> list[object]:
            collected = []
            async for event in store.subscribe_events(run_id):
                collected.append(event)
            return collected

        subscribe_task = asyncio.create_task(subscribe())

        async def subscribed() -> None:
            assert run_id in store._conditions

        await wait_for_passing(subscribed, max_duration=2.0, interval=0.01)

        await store.append_event(run_id, _make_event())
        await store.append_event(run_id, _make_event())
        await store.append_event(run_id, _make_stop_event())

        collected = await asyncio.wait_for(subscribe_task, timeout=5.0)

        assert len(collected) == 3
    finally:
        await store.close()


@pytest.mark.docker
async def test_integration_tick_append_and_get(postgres_dsn: str) -> None:
    store = PostgresWorkflowStore(dsn=postgres_dsn, schema="test_pg_store")
    try:
        await store.start()
        await store.run_migrations()

        run_id = "pg-run-ticks"
        await store.append_tick(run_id, {"type": "TickSendEvent", "event": "a"})
        await store.append_tick(run_id, {"type": "TickSendEvent", "event": "b"})
        await store.append_tick(run_id, {"type": "TickSendEvent", "event": "c"})

        ticks = await store.get_ticks(run_id)
        assert len(ticks) == 3
        assert ticks[0].sequence == 0
        assert ticks[1].sequence == 1
        assert ticks[2].sequence == 2
        assert ticks[0].tick_data["event"] == "a"
        assert ticks[2].tick_data["event"] == "c"

        # Different run_id should be empty
        assert await store.get_ticks("other-run") == []
    finally:
        await store.close()


@pytest.mark.docker
async def test_integration_create_state_store_memoizes_per_run(
    postgres_dsn: str,
) -> None:
    store = PostgresWorkflowStore(dsn=postgres_dsn, schema="test_pg_store")
    try:
        await store.start()
        await store.run_migrations()

        first = store.create_state_store("pg-memo-run")
        second = store.create_state_store("pg-memo-run")
        assert first is second
        assert store.create_state_store("pg-memo-other") is not first

        await first.set("count", 0)

        async def increment(state_store: Any) -> None:
            for _ in range(5):
                async with state_store.edit_state() as state:
                    state["count"] = state["count"] + 1

        await asyncio.gather(increment(first), increment(second))
        assert await first.get("count") == 10
    finally:
        await store.close()
