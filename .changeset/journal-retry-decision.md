---
"llama-index-workflows": patch
---

Record the retry decision inside the failure tick so replaying a run never re-invokes retry policy code, which could strand a delayed retry if the policy changed between the run and the replay
