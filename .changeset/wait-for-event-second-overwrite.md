---
"llama-index-workflows": patch
---

Fix `_resolve_waiters` overwriting a resolved waiter's event with a later one. A step that waits twice for the same event type with different `waiter_id`s no longer loses the first answer.
