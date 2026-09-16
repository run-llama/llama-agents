---
"llama-agents-dbos": minor
---

Use the selected workflow serializer for initial state, DBOS state stores, idle release, persistence, and replay. Server-hosted workflows now restore declared types by default, so list other stored classes in `JsonSerializer(allowed_types=[...])` or configure `JsonSerializer()` to retain import lookup.
