---
"llama-index-workflows": patch
---
Fix `_resolve_waiters` overwriting an already-resolved waiter's `resolved_event`, so a step that waits twice for the same event type with distinct `waiter_id`s keeps each waiter's own answer (run-llama/llama_index#23861).
