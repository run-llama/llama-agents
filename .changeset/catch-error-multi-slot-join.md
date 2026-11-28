---
"llama-index-workflows": patch
---

Validate multi-slot joins reachable only through a @catch_error handler. The top-level seeds now include StepFailedEvent, so events a handler produces (and everything downstream) are recognized as top-level instead of being reported as "produced at no level".
