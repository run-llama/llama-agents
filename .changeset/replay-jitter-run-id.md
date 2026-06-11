---
"llama-index-workflows": patch
"llama-agents-dbos": patch
"llama-agents-server": patch
---

Seed retry jitter with the run id during snapshot tick replay so rebuilt snapshots match the live run, and consume old-format delayed-retry journal entries instead of duplicating them
