# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from workflow import MCP_URL, USER_AGENT, ParallelSearchWorkflow, WebRequest
from workflows.errors import WorkflowCancelledByUser


@pytest.fixture
def mcp_server(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {
        "requests": [],
        "calls": [],
        "clients": [],
        "error": False,
        "tools": True,
        "block": False,
        "started": asyncio.Event(),
    }
    real_client = httpx.AsyncClient

    async def respond(request: httpx.Request) -> httpx.Response:
        state["requests"].append(request)
        assert str(request.url) == MCP_URL
        assert request.headers["User-Agent"] == USER_AGENT
        assert "Authorization" not in request.headers
        if request.method == "GET":
            return httpx.Response(405)
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "notifications/initialized":
            return httpx.Response(202)
        if method == "initialize":
            result = {
                "protocolVersion": payload["params"]["protocolVersion"],
                "capabilities": {},
                "serverInfo": {"name": "test-mcp", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {"name": name, "inputSchema": {"type": "object"}}
                    for name in (["web_search", "web_fetch"] if state["tools"] else [])
                ]
            }
        elif method == "tools/call":
            state["calls"].append(payload["params"])
            state["started"].set()
            if state["block"]:
                await asyncio.Event().wait()
            result = {
                "isError": state["error"],
                "content": [
                    {
                        "type": "text",
                        "text": "MCP error"
                        if state["error"]
                        else "Source: https://example.org\nUseful excerpt",
                    }
                ],
            }
        else:
            raise AssertionError(method)
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result}
        )

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        client = real_client(transport=httpx.MockTransport(respond), **kwargs)
        state["clients"].append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event", "tool", "key", "expected"),
    [
        (
            WebRequest(query="workflow events"),
            "web_search",
            "search_queries",
            ["workflow events"],
        ),
        (
            WebRequest(url="https://example.org"),
            "web_fetch",
            "urls",
            ["https://example.org"],
        ),
    ],
)
async def test_workflow_dispatch(
    mcp_server: dict[str, Any],
    event: WebRequest,
    tool: str,
    key: str,
    expected: list[str],
) -> None:
    result = await ParallelSearchWorkflow(timeout=5).run(start_event=event)
    assert "https://example.org" in result
    assert "Useful excerpt" in result
    call = mcp_server["calls"][0]
    assert call["name"] == tool
    assert call["arguments"][key] == expected
    assert len(call["arguments"]["session_id"]) == 32
    methods = [
        json.loads(r.content)["method"]
        for r in mcp_server["requests"]
        if r.method == "POST"
    ]
    assert methods == [
        "initialize",
        "notifications/initialized",
        "tools/list",
        "tools/call",
    ]


@pytest.mark.asyncio
async def test_tool_error_is_not_success(mcp_server: dict[str, Any]) -> None:
    mcp_server["error"] = True
    with pytest.RaisesGroup(
        pytest.RaisesExc(RuntimeError, match="web_search failed: MCP error"),
        flatten_subgroups=True,
    ):
        await ParallelSearchWorkflow(timeout=5).run(
            start_event=WebRequest(query="events")
        )


@pytest.mark.asyncio
async def test_missing_tool(mcp_server: dict[str, Any]) -> None:
    mcp_server["tools"] = False
    with pytest.RaisesGroup(
        pytest.RaisesExc(RuntimeError, match="does not offer web_search"),
        flatten_subgroups=True,
    ):
        await ParallelSearchWorkflow(timeout=5).run(
            start_event=WebRequest(query="events")
        )
    assert not mcp_server["calls"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event",
    [
        WebRequest(),
        WebRequest(query="", url=""),
        WebRequest(query="events", url="https://example.org"),
    ],
)
async def test_invalid_input(mcp_server: dict[str, Any], event: WebRequest) -> None:
    with pytest.raises(Exception, match="exactly one nonempty"):
        await ParallelSearchWorkflow(timeout=5).run(start_event=event)
    assert not mcp_server["requests"]


@pytest.mark.asyncio
async def test_cancellation_closes_client(mcp_server: dict[str, Any]) -> None:
    mcp_server["block"] = True
    handler = ParallelSearchWorkflow(timeout=5).run(
        start_event=WebRequest(query="events")
    )
    await asyncio.wait_for(mcp_server["started"].wait(), timeout=2)
    await handler.cancel_run()
    with pytest.raises(WorkflowCancelledByUser):
        await handler
    assert all(client.is_closed for client in mcp_server["clients"])
