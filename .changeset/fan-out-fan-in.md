---
"llama-index-workflows": minor
---

Fan-out/fan-in now lives in step signatures: returning `list[E]` opens a finite collection stream and taking `list[E]` joins it on close. `Collect(Take(n))` releases a join early on the nth arrival, and `list[A | B]` joins a flat heterogeneous stream.
