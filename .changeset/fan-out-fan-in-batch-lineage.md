---
"llama-index-workflows": minor
---

Fan-out/fan-in steps now compose automatically via batch lineage: a step returning list[E] or AsyncIterator[E] fans out a batch, and a step taking list[E] joins it once when the batch closes, with no manual cardinality threading.
