---
"llama-agents-server": minor
---

Use the serializer from `Workflow(serializer=...)` or `WorkflowServer(serializer=...)` consistently for durable state and replay. The workflow setting takes precedence.

By default, the server restores only each workflow's declared event and state types. Workflows that store other context classes must list them in `JsonSerializer(allowed_types=[...])` or configure `JsonSerializer()` to retain import-based state restoration.

API events and stored results resolve only from the target workflow's public event registry. Register other public events with `add_workflow(..., additional_events=[...])`.
