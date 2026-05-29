---
"llama-index-workflows": minor
---

Step return annotations now describe fan-out emissions: list[E], AsyncIterator[E], and AsyncGenerator[E, None] returns are parsed into the produced-event set, the static representation exposes produced_by per event, and ctx.send_event rejects event types a step does not declare in its return annotation.
