---
title: deployments
sidebar:
  order: 103
---
Deploy your app to LlamaCloud and manage existing deployments.

Deployment names are the stable ids shown in the `NAME` column. Commands use the active profile's project by default. Pass `--project PROJECT` on API-backed commands to override it for one command.

## Usage

```bash
llamactl deployments [COMMAND] [options]
```

Commands:

- `get [NAME]`: List deployments, or show one deployment
- `create`: Create a deployment from an editor or YAML file
- `edit [NAME]`: Edit a deployment in your editor, or update from a YAML file
- `apply -f FILE`: Create or update a deployment from YAML
- `template`: Print a YAML scaffold for a new deployment
- `delete NAME`: Delete a deployment
- `delete -f FILE`: Delete the deployment named in a YAML file
- `update NAME`: Redeploy at the latest commit
- `history NAME`: Show release history
- `rollback NAME`: Roll back to an earlier git SHA
- `logs NAME`: Fetch or stream deployment logs
- `configure-git-remote NAME`: Configure the push-mode git remote

Notes:

- `-o json` and `-o yaml` are for scripts. Status messages go to stderr; structured data goes to stdout.
- `-f -` reads YAML from stdin on commands that accept `-f`.
- `repo_url: ""` in apply YAML means push the current local working tree. Use `--no-push` when you want to save deployment config without pushing code.

## Commands

### Get

```bash
llamactl deployments get [NAME] [-o text|json|yaml|wide|template] [--project PROJECT]
```

With no `NAME`, lists deployments in the active project. With `NAME`, prints one deployment.

Output modes:

- `text`: Default table output
- `wide`: Table output with less common columns
- `json`: Machine-readable JSON
- `yaml`: Machine-readable YAML
- `template`: Apply-shaped YAML for one deployment only

Examples:

```bash
llamactl deployments get
llamactl deployments get invoice-agent -o yaml
llamactl deployments get invoice-agent -o template > deployment.yaml
```

### Create

```bash
llamactl deployments create [-f FILE] [--no-push] [--project PROJECT]
```

Without `-f`, opens `$EDITOR` with a commented YAML scaffold. Save and close the file to create the deployment.

With `-f FILE`, creates from YAML without opening an editor. Use `-f -` to read from stdin.

Flags:

- `-f, --filename FILE`: YAML file, or `-` for stdin
- `--no-push`: Skip pushing local code for push-mode deployments
- `--project PROJECT`: Override the active project

Examples:

```bash
llamactl deployments create
llamactl deployments create -f deployment.yaml
llamactl deployments create -f - < deployment.yaml
llamactl deployments create --no-push
```

### Edit

```bash
llamactl deployments edit [NAME] [-f FILE] [--no-push] [--project PROJECT]
```

Without `-f`, fetches the deployment, renders editable YAML, and opens `$EDITOR`. If `NAME` is omitted in an interactive terminal, `llamactl` asks you to pick a deployment.

With `-f FILE`, updates from YAML without opening an editor. If `NAME` is omitted, the YAML must include top-level `name`.

Flags:

- `-f, --filename FILE`: YAML file, or `-` for stdin
- `--no-push`: Skip pushing local code for push-mode deployments
- `--project PROJECT`: Override the active project

Examples:

```bash
llamactl deployments edit invoice-agent
llamactl deployments edit invoice-agent -f deployment.yaml
llamactl deployments edit -f deployment.yaml
```

### Apply

```bash
llamactl deployments apply -f FILE [--dry-run] [--no-push] [--annotate-on-error] [--project PROJECT]
```

Applies deployment YAML declaratively. If the top-level `name` exists, `apply` updates that deployment. If it does not exist, `apply` creates it. YAML produced by `deployments template` or `deployments get NAME -o template` is ready for this command.

`${VAR}` references are resolved from the process environment at apply time. Masked secret values from read output are ignored so round-tripping a deployment does not overwrite existing secrets with placeholders.

Flags:

- `-f, --filename FILE`: Required YAML file, or `-` for stdin
- `--dry-run`: Validate and print the resolved payload without changing the deployment
- `--no-push`: Skip pushing local code for push-mode deployments
- `--annotate-on-error`: Write validation errors back into the YAML as comments
- `--project PROJECT`: Override the active project

Examples:

```bash
llamactl deployments template > deployment.yaml
llamactl deployments apply -f deployment.yaml
llamactl deployments apply -f deployment.yaml --dry-run
llamactl deployments apply -f deployment.yaml --annotate-on-error
llamactl deployments get invoice-agent -o template | llamactl deployments apply -f -
```

### Template

```bash
llamactl deployments template
```

Prints a commented YAML scaffold for a new deployment. This command is offline and does not require auth. It uses local context when available, such as the current git remote, current branch, deployment config path, and secret names.

Example:

```bash
llamactl deployments template > deployment.yaml
```

### Delete

```bash
llamactl deployments delete NAME [--project PROJECT]
llamactl deployments delete -f FILE [--project PROJECT]
```

Deletes a deployment immediately. Pass `NAME` directly, or pass `-f FILE` to read the deployment name from YAML.

Flags:

- `-f, --filename FILE`: YAML file containing top-level `name`
- `--project PROJECT`: Override the active project

Examples:

```bash
llamactl deployments delete invoice-agent
llamactl deployments delete -f deployment.yaml
```

### Update

```bash
llamactl deployments update NAME [--git-ref REF] [--no-push] [--project PROJECT]
```

Redeploys using the latest commit for the deployment's configured git ref. Use `--git-ref` to switch to a branch, tag, or commit. For push-mode deployments, local code is pushed first unless `--no-push` is set.

Flags:

- `--git-ref REF`: Branch, tag, or commit SHA to deploy
- `--no-push`: Skip pushing local code for push-mode deployments
- `--project PROJECT`: Override the active project

Examples:

```bash
llamactl deployments update invoice-agent
llamactl deployments update invoice-agent --git-ref release-2026-05
llamactl deployments update invoice-agent --no-push
```

### History

```bash
llamactl deployments history NAME [-o text|json|yaml|wide] [--project PROJECT]
```

Shows release history for a deployment, newest first.

Examples:

```bash
llamactl deployments history invoice-agent
llamactl deployments history invoice-agent -o yaml
```

### Rollback

```bash
llamactl deployments rollback NAME [--git-sha SHA] [--project PROJECT]
```

Rolls a deployment back to a previous git SHA. In an interactive terminal, omit `--git-sha` to pick from release history. In non-interactive runs, pass `--git-sha`.

Flags:

- `--git-sha SHA`: Git SHA to roll back to
- `--project PROJECT`: Override the active project

Examples:

```bash
llamactl deployments history invoice-agent
llamactl deployments rollback invoice-agent --git-sha 3f2a1c9
llamactl deployments rollback invoice-agent
```

### Logs

```bash
llamactl deployments logs NAME [--follow] [--json] [--tail N] [--since-seconds N] [--include-init-containers] [--project PROJECT]
```

Fetches recent deployment logs and exits. Use `--follow` to keep streaming.

Flags:

- `--follow, -f`: Stream until interrupted
- `--json`: Emit one JSON log event per line
- `--tail N`: Number of log lines to fetch initially
- `--since-seconds N`: Only return logs newer than this many seconds
- `--include-init-containers`: Include init container logs
- `--project PROJECT`: Override the active project

Examples:

```bash
llamactl deployments logs invoice-agent
llamactl deployments logs invoice-agent --follow
llamactl deployments logs invoice-agent --tail 50 --since-seconds 3600
llamactl deployments logs invoice-agent --json
```

### Configure Git Remote

```bash
llamactl deployments configure-git-remote NAME [--project PROJECT]
```

Configures an authenticated git remote for a push-mode deployment. The remote is named `llamaagents-NAME`.

Examples:

```bash
llamactl deployments configure-git-remote invoice-agent
git push llamaagents-invoice-agent
```

## See also

- Getting started: [Introduction](/python/llamaagents/llamactl/getting-started/)
- Configure names, env, and UI: [Deployment Config Reference](/python/llamaagents/llamactl/configuration-reference/)
- Local dev server: [`llamactl serve`](/python/llamaagents/llamactl-reference/commands-serve/)
