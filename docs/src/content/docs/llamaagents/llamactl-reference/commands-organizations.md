---
title: organizations
sidebar:
  order: 107
---
List organizations available to the current profile.

## Usage

```bash
llamactl organizations get [-o text|json|yaml]
```

The current/default organization is marked in text output. On servers without organization support, text output prints a warning and structured output returns an empty list.
