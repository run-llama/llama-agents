---
"llama-index-workflows": minor
---

Fan-out/fan-in now lives in step signatures: a step returning list[E] or AsyncIterator[E] fans out a batch, a step taking list[E] joins it once the batch closes (no manual cardinality threading), returns honestly declare their emissions in the static graph, Collect(Take(n)) releases a join early on the nth arrival, and list[A | B] joins a flat heterogeneous batch.
