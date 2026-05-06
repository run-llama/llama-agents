---
title: environments
sidebar:
  order: 105
---
Manage environments (distinct control plane API URLs). Environments determine which profiles are shown and where auth/project actions apply.

## Usage

```bash
llamactl environments [COMMAND] [options]
```

Commands:

- `get [API_URL] [-o text|json|yaml]`: List environments or show one environment
- `add <API_URL>`: Probe the server and upsert the environment
- `use [API_URL]`: Select the current environment; prompts if omitted in interactive mode
- `delete [API_URL]`: Remove an environment and its associated profiles

Notes:

- Probing reads `requires_auth` and `min_llamactl_version` from the server version endpoint.
- Switching environment filters profiles shown by `llamactl auth get` and used by other commands.

## Commands

### Get

```bash
llamactl environments get [API_URL]
```

Shows a table of environments with API URL, whether auth is required, and the active environment. Pass `API_URL` to show one environment.

### Add

```bash
llamactl environments add <API_URL>
```

Probes the server at `<API_URL>` and stores discovered settings. Interactive mode can prompt for the URL.

### Use

```bash
llamactl environments use [API_URL]
```

Sets the current environment. If omitted in interactive mode, you’ll be prompted to select one.

### Delete

```bash
llamactl environments delete [API_URL]
```

Deletes an environment and all associated profiles. If the deleted environment was current, the current environment is reset to the default.

## See also

- Profiles and tokens: [`llamactl auth`](/python/llamaagents/llamactl-reference/commands-auth/)
- Projects: [`llamactl projects`](/python/llamaagents/llamactl-reference/commands-projects/)
- Getting started: [Introduction](/python/llamaagents/llamactl/getting-started/)
- Deployments: [`llamactl deployments`](/python/llamaagents/llamactl-reference/commands-deployments/)
