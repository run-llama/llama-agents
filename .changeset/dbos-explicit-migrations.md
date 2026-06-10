---
"llama-agents-dbos": patch
---

DBOS postgres workflow store no longer auto-migrates on start; deployments with `run_migrations_on_launch=False` must run migrations explicitly.
