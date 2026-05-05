---
title: auth env
sidebar:
  order: 105
---
Manage environments (distinct control plane API URLs). Environments determine which profiles are shown and where auth/project actions apply.

## Usage

```bash
llamactl auth env [COMMAND] [options]
```

Commands:

- `list`: List known environments and mark the current one
- `add <API_URL>`: Probe the server and upsert the environment
- `switch [API_URL]`: Select the current environment
- `delete [API_URL]`: Remove an environment and its associated profiles

Notes:

- Probing reads `requires_auth` and `min_llamactl_version` from the server version endpoint.
- Switching environment filters profiles shown by `llamactl auth list` and used by other commands.

## Commands

### List

```bash
llamactl auth env list
```

Shows a table of environments with API URL, whether auth is required, and the current marker.

### Add

```bash
llamactl auth env add <API_URL>
```

Probes the server at `<API_URL>` and stores discovered settings.

### Switch

```bash
llamactl auth env switch [API_URL]
```

Sets the current environment. If `API_URL` is omitted in a TTY, choose from known environments. Scripts should pass `API_URL`.

### Delete

```bash
llamactl auth env delete [API_URL]
```

Deletes an environment and all associated profiles. If the deleted environment was current, the current environment is reset to the default.

## See also

- Profiles and tokens: [`llamactl auth`](/python/llamaagents/llamactl-reference/commands-auth/)
- Getting started: [Introduction](/python/llamaagents/llamactl/getting-started/)
- Deployments: [`llamactl deployments`](/python/llamaagents/llamactl-reference/commands-deployments/)
