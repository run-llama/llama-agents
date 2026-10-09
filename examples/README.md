# LlamaAgents Examples

A collection of runnable examples showing how to build, serve, and deploy agent workflows with `llama-index-workflows` and `llama-agents-*`.

New to the project? Start at the top of the list and work down — each step builds on the previous.

## Start here

1. **[`feature_walkthrough.ipynb`](feature_walkthrough.ipynb)** — The single best place to begin. A guided tour of workflows, steps, events, context, branching, loops, and streaming, all in one notebook.
2. **[`agent.ipynb`](agent.ipynb)** — Build a simple agent as a workflow. Covers tool calling and the agent loop pattern.
3. **[`document_processing.ipynb`](document_processing.ipynb)** — A realistic document pipeline: parsing, extraction, and orchestration.

## Serving workflows as an API

4. **[`server/`](server/)** — Wrap a workflow as a REST API with `WorkflowServer`, standalone or mounted inside an existing FastAPI app.
5. **[`client/`](client/)** — Call a running workflow server from Python with `WorkflowClient`, including streaming and human-in-the-loop.

## Durability and scale

6. **[`durable_workflows.ipynb`](durable_workflows.ipynb)** — Save and resume workflow runs using pluggable storage.
7. **[`dbos/`](dbos/)** — Production-grade durability with DBOS: crash recovery, multi-replica servers, and idle release.

## Deployment

8. **[`docker/`](docker/)** — Containerize a workflow server with Docker.
9. **[`k8s-otel/`](k8s-otel/)** — Deploy to Kubernetes with OpenTelemetry, Tilt, and a kind cluster.

## Observability and evaluation

10. **[`observability/`](observability/)** — Trace workflows with Arize Phoenix, Langfuse, and the built-in context logger.
11. **[`eval_driven_prompt_refinement.ipynb`](eval_driven_prompt_refinement.ipynb)** — Iterate on prompts using evaluation-driven feedback loops.

## Advanced patterns

- **[`parallel_search/`](parallel_search/)** — Search the web and fetch pages from a workflow using anonymous Parallel Search MCP.

- **[`streaming_internal_events.ipynb`](streaming_internal_events.ipynb)** — Stream intermediate events from nested workflow steps.
- **[`state_management_with_vector_databases.ipynb`](state_management_with_vector_databases.ipynb)** — Persist workflow state in a vector database.
- **[`document_agents/`](document_agents/)** — A finance triage agent built with document workflows.
- **[`visualization/`](visualization/)** — Visualize workflow graphs, including resource nodes.

---

For more on the library, see the [`llama-index-workflows` package README](../packages/llama-index-workflows/README.md) and the [project root README](../README.md).
