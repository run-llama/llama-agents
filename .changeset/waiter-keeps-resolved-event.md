---
"llama-index-workflows": patch
---

Keep the event that resolved an already-resolved waiter, so a step that waits twice for the same event type no longer loses the first answer.
