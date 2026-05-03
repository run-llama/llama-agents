---
title: serve
sidebar:
  order: 102
---
Serve your app locally for development and testing. Reads configuration from your project (e.g., `pyproject.toml` or `llama_agents.yaml`) and starts the Python API server, optionally proxying your UI in dev.

See also: [Deployment Config Reference](/python/llamaagents/llamactl/configuration-reference/) and [UI build and dev integration](/python/llamaagents/llamactl/ui-build/).

## Usage

```bash
llamactl serve [DEPLOYMENT_FILE] [options]
```

- `DEPLOYMENT_FILE` defaults to `.` (current directory). Provide a path to a specific deployment file or directory if needed.

## Options

- `--no-install`: Skip installing Python and JS dependencies
- `--no-reload`: Disable API server auto‑reload on code changes
- `--no-open-browser`: Do not open the browser automatically
- `--preview`: Build the UI to static files and serve them (production‑like)
- `--port <int>`: Port for the API server
- `--ui-port <int>`: Port for the UI proxy in dev

## Behavior

- Prepares the server environment (installs dependencies unless `--no-install`)
- In dev mode (default), proxies your UI dev server and reloads on change
- In preview mode, builds the UI to static files and serves them without a proxy

### Credential injection

If your app uses LlamaCloud (e.g., for LlamaParse or cloud persistence), `llamactl serve` automatically injects credentials into the child process environment. It checks for `LLAMA_CLOUD_API_KEY` in the environment first, then falls back to the active profile's API key. Credentials are also forwarded with `PUBLIC_`, `VITE_`, and `NEXT_PUBLIC_` prefixes so frontend frameworks can access them during local development.

See [`llamactl auth`](/python/llamaagents/llamactl-reference/commands-auth/) for details on environment variable and profile-based authentication.
