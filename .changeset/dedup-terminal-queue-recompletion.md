---
"llama-agents-server": patch
---
Deduplicate terminal-queue entries when a completed handler completes again, so eviction no longer removes a freshly completed handler.
