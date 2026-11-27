---
"llama-index-workflows": patch
---
Validate a multi-slot join that is reachable only through a `@catch_error` handler. `StepFailedEvent` is a top-level entry event, so the events a handler and its downstream steps produce are top-level too; the stream-level check now seeds the top level with it, instead of rejecting such a join as "never produced at a common stream level".
