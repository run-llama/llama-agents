---
title: auth
sidebar:
  order: 104
---
Authenticate and manage profiles for the current environment. Profiles store your control plane API URL, project, and optional API key.

## Usage

```bash
llamactl auth [COMMAND] [options]
```

Commands:

- `token [--project PROJECT] [--api-key KEY] [--interactive/--no-interactive]`: Create profile from API key; validates token and selects a project
- `login`: Login via web browser (OIDC device flow) and create a profile
- `list`: List login profiles in the current environment
- `switch [NAME] [--interactive/--no-interactive]`: Set currently logged in user/token
- `logout [NAME] [--interactive/--no-interactive]`: Delete a login and its local data
- `organizations [-o text|json|yaml|wide]`: List organizations available to the current profile
- `project [PROJECT_ID] [--org ORG_ID] [--interactive/--no-interactive]`: Change the active project for the current profile
- `inject [--env-file PATH]`: Write profile credentials to a `.env` file

Notes:

- Profiles are filtered by the current environment (`llamactl auth env switch`).
- Non-interactive `token` requires both `--api-key` and `--project`.

## Commands

### Token

```bash
llamactl auth token [--project PROJECT] [--api-key KEY] [--interactive/--no-interactive]
```

- Interactive: Prompts for API key (masked), validates it by listing projects, then lets you choose a project. Creates an auto‑named profile and sets it current.
- Non‑interactive: Requires both `--api-key` and `--project`.

Example:

```bash
llamactl auth token --api-key llx-... --project your-project-id --no-interactive
```

### Login

```bash
llamactl auth login
```

Login via your browser using the OIDC device flow, select a project, and create a login profile set as current.

### List

```bash
llamactl auth list
```

Shows a table of profiles for the current environment with name and active project. The current profile is marked with `*`.

### Switch

```bash
llamactl auth switch [NAME] [--interactive/--no-interactive]
```

Set the current profile. If `NAME` is omitted in interactive mode, you will be prompted to select one.

### Logout

```bash
llamactl auth logout [NAME] [--interactive/--no-interactive]
```

Delete a profile. If the deleted profile is current, the current selection is cleared.

### Organizations

```bash
llamactl auth organizations [-o text|json|yaml|wide]
```

List organizations available to the current profile. Text output marks the default organization.

Examples:

```bash
llamactl auth organizations
llamactl auth organizations -o json
```

### Project

```bash
llamactl auth project [PROJECT_ID] [--org ORG_ID] [--interactive/--no-interactive]
```

Change the active project for the current profile. In interactive mode, select from server projects. In environments that don't require auth, you can also enter a project ID.

- `--org ORG_ID`: Scope project lookup to an organization.

### Inject

```bash
llamactl auth inject [--env-file PATH] [--interactive/--no-interactive]
```

Write `LLAMA_CLOUD_API_KEY`, `LLAMA_CLOUD_BASE_URL`, and `LLAMA_AGENTS_PROJECT_ID` from the current profile into a `.env` file. Creates the file if it doesn't exist; overwrites existing values.

- `--env-file PATH`: Defaults to `.env` in the current directory.

## Environment Variables

`llamactl` can authenticate via environment variables instead of a stored profile. This is useful for CI, automated scripts, and environments where `llamactl auth login` isn't practical.

| Variable | Required | Default | Description |
|---|---|---|---|
| `LLAMA_CLOUD_API_KEY` | Yes | — | API key for authentication |
| `LLAMA_AGENTS_PROJECT_ID` | Yes (for project-scoped commands) | — | Project to operate on |
| `LLAMA_CLOUD_BASE_URL` | No | `https://api.cloud.llamaindex.ai` | Control plane API URL |
| `LLAMA_CLOUD_USE_PROFILE` | No | `false` | Set to `1` to ignore env vars and use a stored profile |

When `LLAMA_CLOUD_API_KEY` and `LLAMA_AGENTS_PROJECT_ID` are both set, `llamactl` uses them directly and skips profile lookup. If a stored profile also exists, a warning is printed to stderr; set `LLAMA_CLOUD_USE_PROFILE=1` to opt back into profile auth.

The `--project` flag always takes precedence over `LLAMA_AGENTS_PROJECT_ID`.

Example:

```bash
export LLAMA_CLOUD_API_KEY="llx-..."
export LLAMA_AGENTS_PROJECT_ID="your-project-id"
llamactl deployments get
```

Or generate a `.env` from your current profile with [`llamactl auth inject`](#inject).

## See also

- Environments: [`llamactl auth env`](/python/llamaagents/llamactl-reference/commands-auth-env/)
- Getting started: [Introduction](/python/llamaagents/llamactl/getting-started/)
- Deployments: [`llamactl deployments`](/python/llamaagents/llamactl-reference/commands-deployments/)
