# K8s + OpenTelemetry Example

A self-contained example deploying LlamaIndex Workflows on Kubernetes with distributed tracing via OpenTelemetry and Arize Phoenix.

## What's Inside

- **Counter workflow** — counts to 20 with 1s delays, emitting stream events
- **Greeter workflow** — human-in-the-loop (HITL) with idle release: asks for a name, waits, then greets
- **DBOS runtime** — durable execution across 2 replicas sharing Postgres
- **OpenTelemetry** — traces exported via OTLP to Phoenix
- **structlog** — structured logs with trace context (`run_id`, span tags)

## Architecture

```
┌──────────────┐     ┌──────────────┐
│  app-0       │     │  app-1       │
│  (replica)   │     │  (replica)   │
└──────┬───────┘     └──────┬───────┘
       │  OTLP gRPC         │
       ▼                    ▼
┌──────────────────────────────────┐
│         Phoenix (traces UI)      │
│         localhost:6006           │
└──────────────────────────────────┘
       │
       │  SQL
       ▼
┌──────────────────────────────────┐
│         Postgres                 │
│         (shared state)           │
└──────────────────────────────────┘
```

## Prerequisites

- Docker
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [Tilt](https://docs.tilt.dev/install.html)
- kubectl

## Quick Start

```bash
# Create a kind cluster (one-time setup)
kind create cluster --config examples/k8s-otel/kind-config.yaml

# Deploy
cd examples/k8s-otel
tilt up
```

Tilt opens a browser UI showing all resources. The Tiltfile is pinned to the `kind-llama-k8s-otel` context so it won't accidentally deploy elsewhere.

Ctrl-C stops Tilt but **leaves all resources running** — your Postgres data and Phoenix traces persist. Run `tilt up` again to reconnect.

To tear down app resources (Postgres data is preserved):

```bash
tilt down
```

To destroy the cluster entirely:

```bash
kind delete cluster --name llama-k8s-otel
```

## Manual Alternative

```bash
# Build from repo root
docker build -f examples/k8s-otel/Dockerfile -t k8s-otel-app .

# Deploy
kubectl apply -k examples/k8s-otel/k8s/

# Port-forward
kubectl port-forward -n llama-k8s-otel svc/app 8080:8080 &
kubectl port-forward -n llama-k8s-otel svc/phoenix 6006:6006 &
```

## Interacting with the App

### Counter workflow

```bash
# Start the counter without waiting (returns handler_id)
curl -s -X POST http://localhost:8080/api/workflows/counter/run-nowait \
  -H 'Content-Type: application/json' -d '{}'

# Check result (after ~20s)
curl -s http://localhost:8080/api/results/<handler_id>

# Or run synchronously (blocks until done)
curl -s -X POST http://localhost:8080/api/workflows/counter/run \
  -H 'Content-Type: application/json' -d '{}'
```

### Greeter workflow (HITL)

```bash
# Start — returns a handler_id
curl -s -X POST http://localhost:8080/api/workflows/greeter/run-nowait \
  -H 'Content-Type: application/json' -d '{}'
# {"handler_id": "abc123", ...}

# Send user input
curl -s -X POST http://localhost:8080/api/events/<handler_id> \
  -H 'Content-Type: application/json' \
  -d '{"event": {"type": "UserInput", "value": {"response": "Alice"}}}'
```

`GET /api/events/{handler_id}?after_sequence=-1` shows events emitted before the client connected.

```bash
# Get result
curl -s http://localhost:8080/api/results/<handler_id>
```

### View Traces

Open [http://localhost:6006](http://localhost:6006) in your browser to see traces in Phoenix.

## Notes

- **Postgres not ready on first deploy**: DBOS retries connections. App pods may restart once — that's normal.
- **Phoenix not ready**: The OTLP exporter silently drops spans if Phoenix isn't up yet. No app impact.
- **Idle release across replicas**: If a pod dies while a greeter workflow is idle-released, another replica picks it up via DBOS recovery. This is the intended distributed behavior.
- **Data persistence**: Postgres uses a PVC with `tilt.dev/down-policy: keep`, so data survives `tilt down`. Only deleting the kind cluster destroys data.
