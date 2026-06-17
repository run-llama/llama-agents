---
"llama-agents-server": patch
---

Store workflow state as one record per (run_id, namespace) instead of a single bundled record per run. Adds an additive migration that introduces an unbounded namespace column defaulting to the root namespace, so existing rows are unchanged.
