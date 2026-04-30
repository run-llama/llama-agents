---
"llama-agents-dbos": minor
---

Add `max_recovery_attempts` to `DBOSRuntimeConfig`. When set, it is forwarded to the `@DBOS.workflow` decorator wrapping the runtime's control loop.
