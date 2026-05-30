---
"llama-index-workflows": patch
---

Warn when a child workflow declares `@catch_error` handlers: catch-error recovery only applies to the root workflow, so a handler on a nested child never runs and a child step failure fails the whole run.
