---
"llama-index-workflows": patch
---

Keep the event that resolved a waiter when a later event of the same type arrives. A step that waits twice for the same event type with different `waiter_id`s used to get the second answer from both waits, because `_resolve_waiters` overwrote `resolved_event` on the already-resolved waiter and resumed the suspended work item a second time.
