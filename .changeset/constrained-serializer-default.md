---
"llama-index-workflows": minor
"llama-agents-server": minor
"llama-agents-dbos": minor
"llama-agents-appserver": minor
---

The server's default serializer restores only the workflow's declared event and state types.

Values in the context store whose classes are not declared by the workflow require `JsonSerializer(allowed_types=[...])` on the workflow or server, or `JsonSerializer()` to keep import-based restoration.
