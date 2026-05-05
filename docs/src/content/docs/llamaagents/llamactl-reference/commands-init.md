---
title: init
sidebar:
  order: 101
---
Create a new app from a starter template, or update an existing app to the latest template version.

## Usage

```bash
llamactl init [--template <id>] [--dir <path>] [--force]
llamactl init --update
```

## Templates

Run `llamactl init` without `--template` to open the template picker. The picker shows both UI templates and headless workflow templates.

Use `--template <id>` when you already know which template you want. Template IDs can change as templates are added or renamed, so use the picker or the [Agent Templates](/python/llamaagents/llamactl/agent-templates/) page for the current list.

## Options

- `--update`: Update the current app to the latest template version. Ignores other options.
- `--template <id>`: Template to use.
- `--dir <path>`: Directory to create the new app in. Defaults to the template name.
- `--force`: Overwrite the directory if it already exists.

## What it does

- Copies the selected template into the target directory using [`copier`](https://copier.readthedocs.io/en/stable/)
- Adds assistant docs: `AGENTS.md` and symlinks `CLAUDE.md`/`GEMINI.md`
- Initializes a Git repository if `git` is available
- Prints next steps to run locally and deploy

## Examples

Open the template picker:

```bash
llamactl init
```

Create from a known template:

```bash
llamactl init --template <template-id> --dir my-app
```

Overwrite an existing directory:

```bash
llamactl init --template <template-id> --dir ./my-app --force
```

Update an existing app to the latest template:

```bash
llamactl init --update
```

See also: [Getting Started guide](/python/llamaagents/llamactl/getting-started/).
