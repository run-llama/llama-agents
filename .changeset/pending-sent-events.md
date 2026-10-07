---
"llama-index-workflows": patch
---

Resumed runs no longer stall when a snapshot is taken after a step sends an event with ctx.send_event but before that event is processed
