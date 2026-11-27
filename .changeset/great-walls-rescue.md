---
"llama-index-workflows": patch
---

Restore journaled `ctx.wait_for_event` waiters with their requirements, so one answer no longer resolves every waiter of the same event type after a `BasicRuntime.restore()`
