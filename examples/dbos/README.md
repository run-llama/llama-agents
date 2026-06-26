# DBOS Durability Examples

Examples of running durable workflows backed by [DBOS](https://github.com/dbos-inc/dbos-transact-py). DBOS gives you crash recovery, resumable runs, and multi-replica coordination with either SQLite (zero setup) or Postgres.

See [`packages/llama-agents-dbos/ARCHITECTURE.md`](../../packages/llama-agents-dbos/ARCHITECTURE.md) for the underlying model.

## Examples

| File | What it shows |
| --- | --- |
| [`server_quickstart.py`](server_quickstart.py) | The simplest durable `WorkflowServer` setup, using SQLite out of the box. **Start here.** |
| [`durable_workflow.py`](durable_workflow.py) | A looping counter workflow you can interrupt with Ctrl+C and resume with `--resume`. Demonstrates checkpointing without a server. |
| [`mlflow_trace_propagation.py`](mlflow_trace_propagation.py) | Verifies MLflow trace context propagates into a DBOS-backed workflow and manual spans parent under the workflow trace. |
| [`server_replicas.py`](server_replicas.py) | Two `WorkflowServer` replicas sharing a Postgres-backed event store. Start a run on replica A, stream events from replica B, interrupt, and resume. Requires Docker (uses [`docker-compose.yml`](docker-compose.yml)). |
| [`idle_release_demo.py`](idle_release_demo.py) | Shows how long-idle workflows are released from memory and automatically resumed when a new event arrives. |
| [`_replica.py`](_replica.py) | Single-replica server process used internally by `server_replicas.py`. |

## Running

```bash
# Quickest path — no database required
uv run examples/dbos/server_quickstart.py

# Multi-replica (needs Docker for Postgres)
docker compose -f examples/dbos/docker-compose.yml up -d
uv run examples/dbos/server_replicas.py
```
