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

- `token [--project PROJECT] [--api-key KEY]`: Create a profile from an API key; validates token and selects a project
- `login`: Log in via web browser (OIDC device flow) and create a profile
- `get [NAME] [-o text|json|yaml]`: List profiles or show one profile in the current environment
- `use [NAME]`: Set the current profile
- `logout [NAME]`: Delete a profile and its local credentials
- `inject [--env-file PATH]`: Write profile credentials to a `.env` file

Notes:

- Profiles are filtered by the current environment (`llamactl environments use`).
- `auth token` creates a profile without prompts when both `--api-key` and `--project` are supplied.
- Project selection moved to `llamactl projects use`.
- Organization listing moved to `llamactl organizations get`.

## Commands

### Token

```bash
llamactl auth token [--project PROJECT] [--api-key KEY]
```

Without flags, prompts for an API key, validates it by listing projects, then lets you choose a project. Creates an auto-named profile and sets it current.

With both `--api-key` and `--project`, creates the profile without prompts.

Example:

```bash
llamactl auth token --api-key llx-... --project your-project-id
```

### Login

```bash
llamactl auth login
```

Log in via your browser using the OIDC device flow, select a project, and create a login profile set as current.

### Get

```bash
llamactl auth get [NAME] [-o text|json|yaml]
```

Shows profiles for the current environment. Pass `NAME` to show one profile.

### Use

```bash
llamactl auth use [NAME]
```

Set the current profile. If `NAME` is omitted in a TTY, choose from the available profiles. Scripts should pass `NAME`.

### Logout

```bash
llamactl auth logout [NAME]
```

Delete a profile. If the deleted profile is current, the current selection is cleared.

### Inject

```bash
llamactl auth inject [--env-file PATH]
```

Write `LLAMA_CLOUD_API_KEY`, `LLAMA_CLOUD_BASE_URL`, and `LLAMA_AGENTS_PROJECT_ID` from the current profile into a `.env` file. Creates the file if it doesn't exist; overwrites existing values.

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

- Environments: [`llamactl environments`](/python/llamaagents/llamactl-reference/commands-environments/)
- Projects: [`llamactl projects`](/python/llamaagents/llamactl-reference/commands-projects/)
- Organizations: [`llamactl organizations`](/python/llamaagents/llamactl-reference/commands-organizations/)
- Getting started: [Introduction](/python/llamaagents/llamactl/getting-started/)
- Deployments: [`llamactl deployments`](/python/llamaagents/llamactl-reference/commands-deployments/)
