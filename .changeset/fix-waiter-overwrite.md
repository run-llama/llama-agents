---
"llama-index-workflows": patch
---

Fix sequential `wait_for_event` calls of the same event type overwriting each other's resolved event.
