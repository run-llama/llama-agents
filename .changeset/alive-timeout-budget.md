---
"llama-index-workflows": minor
---

Workflow timeouts now measure accumulated alive time across resumes instead of resetting on each resume; long-suspended runs that repeatedly resume can now time out where they previously would not.
