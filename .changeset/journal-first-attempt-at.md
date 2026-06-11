---
"llama-index-workflows": patch
---

Record the original dispatch time inside the failure tick so elapsed-based retry stop conditions keep their budget across snapshot and resume instead of restarting it
