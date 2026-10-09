# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Call anonymous Parallel Search MCP tools from a workflow step."""

from __future__ import annotations

import argparse
import asyncio
import uuid

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import TextContent
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent

MCP_URL = "https://search.parallel.ai/mcp"
USER_AGENT = "llama-agents-parallel-search-example/1.0"


class WebRequest(StartEvent):
    query: str | None = None
    url: str | None = None


class ParallelSearchWorkflow(Workflow):
    @step
    async def retrieve(self, ev: WebRequest) -> StopEvent:
        if bool(ev.query) == bool(ev.url):
            raise ValueError("Provide exactly one nonempty query or URL")

        session_id = uuid.uuid4().hex
        if ev.url:
            tool = "web_fetch"
            arguments = {"urls": [ev.url], "session_id": session_id}
        else:
            tool = "web_search"
            arguments = {
                "objective": ev.query,
                "search_queries": [ev.query],
                "session_id": session_id,
            }

        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT}, timeout=60.0
        ) as client:
            async with streamable_http_client(MCP_URL, http_client=client) as (
                read,
                write,
                _,
            ):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    if tool not in {item.name for item in tools.tools}:
                        raise RuntimeError(f"MCP server does not offer {tool}")
                    result = await session.call_tool(tool, arguments)
                    text = "\n".join(
                        item.text
                        for item in result.content
                        if isinstance(item, TextContent)
                    )
                    if result.isError:
                        raise RuntimeError(f"{tool} failed: {text}")
                    if not text:
                        raise RuntimeError(f"{tool} returned no text content")
                    return StopEvent(result=text)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--query", help="Search objective and query")
    inputs.add_argument("--url", help="Fetch a page as markdown")
    args = parser.parse_args()
    result = await ParallelSearchWorkflow(timeout=90).run(
        start_event=WebRequest(query=args.query, url=args.url)
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
