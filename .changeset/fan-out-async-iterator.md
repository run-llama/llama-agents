---
"llama-index-workflows": minor
---

A step returning AsyncIterator[E] fans out a batch that streams downstream as each event is yielded, so consumers overlap the producer. Replay re-runs the producer from the start and may re-emit events; manage idempotency with domain-id dedup or a resumable cursor checkpointed in ctx.store.
