---
"llama-index-workflows": minor
---

Add the Collect selection algebra for batch fan-in. Annotate a list[E] join parameter with Collect(Take(n)) or Collect(AtLeast(n)) to release early on the nth arrival instead of waiting for the batch to close; bare list[E] stays Collect(All()). list[A | B] now collects a flat heterogeneous batch. New ctx.replayed_events() accessor exposes prior-run emissions for user-side dedup.
