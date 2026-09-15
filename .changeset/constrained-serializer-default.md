---
"llama-index-workflows": minor
"llama-agents-server": minor
"llama-agents-dbos": minor
"llama-agents-appserver": minor
---

The default serializer for persisted workflow state now resolves only the workflow's declared event and state types.

Values in the context store whose classes are not declared by the workflow no longer restore by default; pass them via `JsonSerializer(allowed_types=[...])` on the workflow or server, or pass `JsonSerializer()` to keep the previous behavior.
