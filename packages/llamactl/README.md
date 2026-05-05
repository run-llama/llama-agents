# llamactl

`llamactl` is the CLI for developing LlamaAgents apps locally and managing their LlamaCloud deployments.

For the full guide, see the [LlamaAgents `llamactl` docs](https://developers.llamaindex.ai/python/llamaagents/llamactl/getting-started/).

## Installation

Install from PyPI:

```bash
pip install llamactl
```

Or run without installing:

```bash
uvx llamactl --help
```

## Quick Start

Create or select an auth profile:

```bash
llamactl auth login
```

If browser login is not available, use an API key:

```bash
llamactl auth token --api-key "$LLAMA_CLOUD_API_KEY" --project "$LLAMA_AGENTS_PROJECT_ID"
```

Scaffold and run an app:

```bash
llamactl init
cd my-app
llamactl serve
```

Create a cloud deployment:

```bash
llamactl deployments create
```

Inspect it and stream logs:

```bash
llamactl deployments get
llamactl deployments get NAME
llamactl deployments logs NAME --follow
```

For declarative deployments:

```bash
llamactl deployments template > deployment.yaml
llamactl deployments apply -f deployment.yaml
```

## Command Groups

- `llamactl auth`: log in, create API-key profiles, switch profiles, select projects, and list organizations.
- `llamactl auth env`: list, add, inspect, and switch LlamaCloud API environments.
- `llamactl deployments`: create, apply, edit, update, inspect, delete, roll back, and stream deployment logs.
- `llamactl init`: create a new LlamaAgents project from a starter template.
- `llamactl serve`: run the local app server and optional frontend dev server.
- `llamactl pkg`: generate container build files for self-hosted deployments.
- `llamactl completion`: generate or install shell completions.
- `llamactl agentcore`: run or export AgentCore apps.

## Configuration

`llamactl auth login` and `llamactl auth token` create local auth profiles. A profile stores the active API environment, project, and credential used by deployment commands.

For CI and other non-interactive environments, set env vars instead of using a profile:

```bash
export LLAMA_CLOUD_API_KEY="llx-..."
export LLAMA_AGENTS_PROJECT_ID="project-id"
```

`LLAMA_CLOUD_BASE_URL` can point the CLI at a non-default environment. When both `LLAMA_CLOUD_API_KEY` and `LLAMA_AGENTS_PROJECT_ID` are set, env var auth takes precedence over the stored profile for cloud commands. Many commands also accept `--project` to override the active project for that invocation.

## Shell Completion

Install completions for your current shell:

```bash
llamactl completion install
```

Or print a completion script:

```bash
llamactl completion generate zsh
```

## Requirements

- Python 3.12+
- `uv` for project dependency management
- `git` for cloud deployments

## License

MIT
