---
"llama-index-workflows": minor
---

Add the Collect selection algebra for batch fan-in. Annotate a list[E] join parameter with Collect(Take(n)) to release early on the nth arrival instead of waiting for the batch to close; bare list[E] stays Collect(All()). list[A | B] now collects a flat heterogeneous batch.
