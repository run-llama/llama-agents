---
"llama-index-workflows": minor
---

Add `Workflow(serializer=...)` and `Runtime(default_serializer=...)` to configure serialization across context state, durable ticks, and replay. `JsonSerializer(allowed_types=[...])` now accepts classes and resolves them from its registry instead of importing them by name.
