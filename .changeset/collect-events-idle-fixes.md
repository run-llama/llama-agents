---
"llama-index-workflows": patch
---

Fix idle checks racing buffered events and stale `ctx.collect_events()` firings.
