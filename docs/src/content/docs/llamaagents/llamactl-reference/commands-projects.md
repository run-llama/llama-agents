---
title: projects
sidebar:
  order: 106
---
List and select projects for the current profile.

## Usage

```bash
llamactl projects [COMMAND] [options]
```

Commands:

- `get [PROJECT_ID] [--org ORG_ID] [-o text|json|yaml]`: List projects or show one project
- `use [PROJECT_ID] [--org ORG_ID]`: Set the active project for the current profile

`--project` on deployment commands still overrides the active profile project for one call.
