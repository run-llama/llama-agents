# Web retrieval with Parallel Search MCP

This example calls [Parallel Search MCP](https://docs.parallel.ai/integrations/mcp/search-mcp)
from a workflow step using Streamable HTTP. Search returns source URLs and excerpts;
fetch returns page content as markdown. The anonymous endpoint is free for light
use at lower rate limits and needs no API key. The example does not load saved
credentials or environment API keys, and does not use an LLM.

From the repository root, install in a separate environment and run:

```bash
uv venv examples/parallel_search/.venv
uv pip install --python examples/parallel_search/.venv/bin/python -r examples/parallel_search/requirements.txt
examples/parallel_search/.venv/bin/python examples/parallel_search/workflow.py --query "LlamaIndex workflow events documentation"
examples/parallel_search/.venv/bin/python examples/parallel_search/workflow.py --url "https://docs.llamaindex.ai/en/stable/understanding/workflows/"
```

`ParallelSearchWorkflow.run(start_event=WebRequest(query=...))` performs a search.
Use `WebRequest(url=...)` instead to fetch a known URL, such as one returned by
search. Each run initializes an MCP session, discovers the requested tool, and
returns its text content. Tool errors raise exceptions; fetch responses can also
contain per-URL errors, which are retained in the returned content. This is a
retrieval workflow, with no agent reasoning loop or automatic retries.

Requests use a 60-second HTTP timeout and the CLI sets a 90-second workflow
timeout. Cancelling a workflow closes its MCP session and HTTP client.

To run the offline transport tests in the same environment:

```bash
uv pip install --python examples/parallel_search/.venv/bin/python pytest pytest-asyncio
examples/parallel_search/.venv/bin/python -m pytest -o addopts= examples/parallel_search/test_workflow.py
```
