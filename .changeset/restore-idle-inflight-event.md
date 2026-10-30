---
"llama-index-workflows": patch
---

Fix a context restored at an idle point losing an event that was already sent: the control loop now reduces a fetched external tick before publishing the idle event, so a snapshot taken at idle contains every event sent before it
