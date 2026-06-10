# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Fake Agent Data API backend for testing AgentDataStore."""

from __future__ import annotations

import json
import uuid
from typing import Any

import httpx
import pytest
from llama_agents.server import AgentDataStore
from llama_agents.server._store.agent_data_client import AgentDataClient
from llama_agents.server._store.agent_data_state_store import AgentDataStateStore


class FakeAgentDataBackend:
    """In-memory backend that simulates the Agent Data API HTTP endpoints.

    Stores items keyed by (deployment_name, collection) and provides
    search/create/update/delete semantics matching the real API.
    """

    def __init__(self) -> None:
        # (deployment_name, collection) → list[{id, deployment_name, collection, data, created_at}]
        self._items: dict[tuple[str, str], list[dict[str, Any]]] = {}
        # Monotonic counter used to synthesize row-level created_at, matching
        # how the real backend auto-assigns a created_at column per row.
        self._create_counter: int = 0

    def _key(self, deployment_name: str, collection: str) -> tuple[str, str]:
        return (deployment_name, collection)

    def _get_items(self, deployment_name: str, collection: str) -> list[dict[str, Any]]:
        return self._items.setdefault(self._key(deployment_name, collection), [])

    def search(
        self,
        deployment_name: str,
        collection: str,
        filters: dict[str, Any] | None = None,
        page_size: int = 100,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        items = self._get_items(deployment_name, collection)
        if filters:
            matched = [item for item in items if self._matches(item["data"], filters)]
        else:
            matched = list(items)

        if order_by:
            parts = order_by.split()
            field = parts[0]
            reverse = len(parts) > 1 and parts[1].lower() == "desc"
            # Row-level fields (e.g. created_at) live on the item itself, not
            # in data. Fall back to data for user fields.
            matched.sort(
                key=lambda item: item.get(field, item["data"].get(field, 0)),
                reverse=reverse,
            )

        return matched[:page_size]

    @staticmethod
    def _matches(data: dict[str, Any], filters: dict[str, Any]) -> bool:
        for field, ops in filters.items():
            value = data.get(field)
            for op, expected in ops.items():
                if op == "eq" and value != expected:
                    return False
                if op == "includes" and value not in expected:
                    return False
                if op == "ne" and value == expected:
                    return False
                if op == "gt" and (value is None or value <= expected):
                    return False
                if op == "gte" and (value is None or value < expected):
                    return False
        return True

    def create(
        self, deployment_name: str, collection: str, data: dict[str, Any]
    ) -> dict[str, Any]:
        items = self._get_items(deployment_name, collection)
        self._create_counter += 1
        item = {
            "id": str(uuid.uuid4()),
            "deployment_name": deployment_name,
            "collection": collection,
            "data": data,
            "created_at": self._create_counter,
        }
        items.append(item)
        return item

    def update_item(self, item_id: str, data: dict[str, Any]) -> dict[str, Any]:
        for items_list in self._items.values():
            for item in items_list:
                if item["id"] == item_id:
                    item["data"] = data
                    return item
        raise ValueError(f"Item {item_id} not found")

    def delete_item(self, item_id: str) -> None:
        for items_list in self._items.values():
            for i, item in enumerate(items_list):
                if item["id"] == item_id:
                    items_list.pop(i)
                    return
        raise ValueError(f"Item {item_id} not found")

    def delete_many(
        self,
        deployment_name: str,
        collection: str,
        filters: dict[str, Any],
    ) -> int:
        items = self._get_items(deployment_name, collection)
        matched = [item for item in items if self._matches(item["data"], filters)]
        for item in matched:
            items.remove(item)
        return len(matched)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Route an httpx.Request to the appropriate handler."""
        path = request.url.path
        method = request.method

        if method == "POST" and path == "/api/v1/beta/agent-data/:search":
            body = json.loads(request.content)
            items = self.search(
                body["deployment_name"],
                body["collection"],
                body.get("filter"),
                body.get("page_size", 100),
                body.get("order_by"),
            )
            return httpx.Response(200, json={"items": items})

        if method == "POST" and path == "/api/v1/beta/agent-data/:delete":
            body = json.loads(request.content)
            count = self.delete_many(
                body["deployment_name"],
                body["collection"],
                body.get("filter", {}),
            )
            return httpx.Response(200, json={"deleted_count": count})

        if method == "POST" and path == "/api/v1/beta/agent-data":
            body = json.loads(request.content)
            item = self.create(
                body["deployment_name"], body["collection"], body["data"]
            )
            return httpx.Response(200, json=item)

        if method == "PUT" and path.startswith("/api/v1/beta/agent-data/"):
            item_id = path.split("/")[-1]
            body = json.loads(request.content)
            item = self.update_item(item_id, body["data"])
            return httpx.Response(200, json=item)

        if method == "DELETE" and path.startswith("/api/v1/beta/agent-data/"):
            item_id = path.split("/")[-1]
            self.delete_item(item_id)
            return httpx.Response(200, json={})

        return httpx.Response(404, json={"error": "not found"})


def _patch_client(
    client: AgentDataClient,
    backend: FakeAgentDataBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Patch an AgentDataClient's http_client to use the fake backend.

    Creates a single mock-transport client and makes http_client() return it,
    matching the shared-client pattern in AgentDataClient.
    """
    mock_http = httpx.AsyncClient(
        base_url=client._base_url,
        headers=client._headers(),
        params={"project_id": client._project_id},
        transport=httpx.MockTransport(backend.handle_request),
    )
    monkeypatch.setattr(client, "_shared_client", mock_http)


def create_agent_data_store(
    backend: FakeAgentDataBackend,
    monkeypatch: pytest.MonkeyPatch,
    collection: str = "handlers",
) -> AgentDataStore:
    """Create an AgentDataStore with httpx patched to use the fake backend."""
    store = AgentDataStore(
        base_url="https://fake-api.example.com",
        api_key="test-key",
        project_id="test-project",
        deployment_name="test-deploy",
        collection=collection,
    )
    _patch_client(store._client, backend, monkeypatch)
    return store


def create_agent_data_state_store(
    backend: FakeAgentDataBackend,
    monkeypatch: pytest.MonkeyPatch,
    run_id: str,
    state_type: type[Any] | None = None,
) -> AgentDataStateStore[Any]:
    """Create an AgentDataStateStore with httpx patched to use the fake backend."""
    client = AgentDataClient(
        base_url="https://fake-api.example.com",
        api_key="test-key",
        project_id="test-project",
        deployment_name="test-deploy",
    )
    _patch_client(client, backend, monkeypatch)
    store = AgentDataStateStore(
        client=client,
        run_id=run_id,
        state_type=state_type,
    )
    return store
