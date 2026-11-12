---
"llama-index-workflows": patch
---

Preserve `wait_for_event` requirements across journal replay: `restore()` no longer drops the `has_requirements` flag, so a run resumed from the journal routes each answer to its own waiter instead of letting one event resolve every waiter of its type.
