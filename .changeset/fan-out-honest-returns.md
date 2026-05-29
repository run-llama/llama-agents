---
"llama-index-workflows": minor
---

Step return annotations now describe fan-out emissions for validation and graph representation: list[E], AsyncIterator[E], and AsyncGenerator[E, None] returns are parsed into the produced-event set, and the static representation exposes produced_by per event.
