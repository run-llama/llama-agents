---
name: llamactl-qa
description: Plan and run a design-QA / "taste test" of llamactl changes against a real backend. Cooperatively builds a small matrix of cases worth eyeballing, runs them against a chosen backend (test environment by default, local kind+tilt for new API contract changes), and writes a design-review report to `thoughts/shared/qa/`. Use this for changes to `llamactl` commands, output formats, auth, or control-plane API contracts. For UI/template smoke tests, use `llamactl_browser_test` instead.
---

# llamactl CLI QA

A design QA, not an integration test. The goal is to put the actual rendered output in front of a human reviewer so they can judge whether the design is right — alignment, naming, key ordering, error wording, what's noisy, what's missing. The LLM's job is to set up well-chosen cases and call out what looks off; the human's job is to read the outputs and decide.

Two halves. First, a planning pass that turns "I changed X" into a small, sign-off-able matrix of cases worth running. Second, an execution pass that runs them and writes a report whose body is mostly verbatim output plus design-review notes.

Every row in the matrix is there because we agreed it covers a real risk or design question in the diff.

## Mode flags

Parse these from the user's invocation before doing anything else:

- `--auto`, `auto`, `fully auto`, `non-chatty`, or `-f` means run autonomously. Pick the backend, draft the matrix internally, execute it, write the report, and return only the report pointer plus any blocking issue. Do not ask for backend selection or matrix confirmation.
- In auto mode, prefer the least risky backend that still exercises the changed surface. Use offline rows for parse/render/local-file behavior, local kind+tilt for new control-plane contracts, and a test environment for read-only checks or safe writes. **Never use production for mutating QA.** Switch to a test environment first.
- Auto mode does not relax mutation rules. When creating deployments for QA (e.g., to test edit or delete flows), always create your own throwaway deployments rather than editing existing ones that might be important. Clean them up at the end of the run.

### What this is NOT

- **Not an automated integration test.** No `jq has(...)` shape assertions, no `python -c "assert ..."`. If a row reads as "the command works", drop it; if it reads as "let's see the output and judge it", keep it. Real integration tests are a separate task (authoring real pytest cases against the API), not this one.
- **Not exhaustive coverage.** 4–8 well-chosen rows. The point is taste, not a regression matrix.
- **Not pass/fail.** The report's job is "here's what the surface actually looks like", not "all green".

llamactl is churning right now (multi-slice rework: output modes, apply/delete, template/apply-loop). Treat this skill as a living reference. When a run teaches you something about llamactl that isn't in here, edit the skill before closing the task. See "Self-update" at the bottom.

## When to use

- Changes to `llamactl` commands (new flags, new output modes, command splits, behavior changes).
- Changes to control-plane API contracts that llamactl consumes.
- Changes to auth / profile / project resolution.

Skip when the change is fully covered by pytest, or when the failure mode is in the UI (use `llamactl_browser_test`).

## Pick a backend

1. **Test environment / test project.** Default for QA that creates or mutates deployments. Use a non-production environment or a dedicated test project so throwaway deployments don't pollute real workspaces. **Never run mutating QA commands against production.** When creating test deployments (to later edit/delete them as part of the QA), create them yourself rather than editing existing deployments that may be important. Pin every command with `--project <test-project-id>` so an active-profile slip can't write into the wrong project.
2. **Local kind + tilt.** When the change is a new API contract (new endpoint, new field, new behavior on the control plane) and you need to exercise it against a control plane that actually has the change. `uv run operator/dev.py up`. See `AGENTS.md` and `operator/AGENTS.md` for setup. Local mode runs with no auth: the `http://localhost:8011` env is preconfigured; switch to it with `llamactl auth env switch http://localhost:8011` (arg is the API URL, not a name) and a `default` profile is created automatically. No tokens, no `auth login`. Before drafting the matrix, list existing deployments with `kubectl get llamadeployments -A` — there's often something already in the cluster from prior work that you can target with `--project <id>`, saving the cost of a fresh `deployments create`.
3. **Offline only.** For changes that only affect local rendering (template output, help text, error formatting), no backend is needed at all. Run commands that don't hit the API (e.g., `deployments template`, `--help`, non-interactive error paths).

Ask the user which backend before drafting the matrix. The answer changes which rows are realistic. Skip this in auto mode and choose from the rules above.

## Test project scaffolds

Some QA needs a real on-disk project (anything that reads `cwd` — e.g., `deployments template`, `serve`, `apply -f` once it lands, anything that consults `pyproject.toml`'s `[tool.llamadeploy]` block, the `.env`, or git state). Use `llamactl init` into a temp dir rather than hand-rolling files or reusing your dev tree.

```bash
WORK=$(mktemp -d -t llamactl-qa.XXXXXX)
uv run llamactl init --no-interactive --template basic --dir "$WORK/app"
```

What you get:

- A scaffolded project with `pyproject.toml` ([tool.llamadeploy] block), `.env.template`, `src/`, `tests/`.
- An initial git commit (`init` runs `git init` + commit). `is_git_repo` is true; `git_ref` is `main`; no remote.
- No `.env` (only `.env.template`). Copy/edit if a row needs `available_secrets` populated: `cp "$WORK/app/.env.template" "$WORK/app/.env"` then append e.g. `OPENAI_API_KEY=test`.
- Available templates: `basic-ui`, `showcase`, `document-qa`, `extraction-review`, `classify-extract-sec`, `extract-reconcile-invoice`, `basic`, `document_parsing`, `human_in_the_loop`, `invoice_extraction`, `rag`, `web_scraping`. `basic` is fastest and has no required secrets — use it unless the row needs a template that lists required secrets.

For rows that need a non-git directory: `mktemp -d` and don't init. For rows that need a remote: `git -C "$WORK/app" remote add origin https://github.com/<you>/qa.git`. Don't push.

Run commands from inside the project: `(cd "$WORK/app" && llamactl deployments template)`. Don't `cd` in the parent shell — keeps rows independent. Capture full paths in the report so the reader can recreate.

Cleanup is `rm -rf "$WORK"` at the end of the run. Mention it in the report's footer.

If a row needs to **commit** changes (e.g., to test git-state-dependent behavior), do it inside the temp tree with a fresh `user.email` / `user.name` so the host's git config doesn't bleed in:

```bash
git -C "$WORK/app" -c user.email=qa@example.com -c user.name=qa commit -am "<msg>"
```

Don't try to make this temp project deployable to a real backend in the same run as offline QA. If a row needs a server-side deployment to edit or inspect, create a throwaway deployment yourself (e.g., `create -f` with a minimal YAML + `--no-push`) rather than editing an existing deployment that might be important. Clean it up with `deployments delete` at the end of the run.

## llamactl mental model

The bits that matter when designing a matrix.

**Scoping hierarchy**: environments → profiles → organizations → projects → deployments. An environment is a control-plane URL (e.g., `http://localhost:8011` for local). Each environment can have one or more profiles (authenticated identities). Each profile has an active organization, and each organization contains projects. Deployments live inside projects. `llamactl auth env list` shows environments. `llamactl auth list` shows profiles in the active environment. `llamactl auth env switch <url>` changes environment. `llamactl auth project [id]` changes the active project within the current profile.

A profile is `(env, oauth tokens, active org, active project)`. `--project <id>` overrides the profile's active project for one call. Before running any QA, check `llamactl auth env list` and `llamactl auth list` to confirm you're in the right environment and project — don't assume from a previous session. There are multiple environments with deployments available for testing; use a non-production one for mutating operations.

Read commands accept `-o text|json|yaml`. Text is human-facing; json/yaml are assertable. Prefer json for QA — exact-match assertions catch field-rename regressions text formatting hides.

Read commands (`deployments get/list/history/logs`, `auth list`, `auth env list`) do not need `--no-interactive` when their argument is supplied. The flag is a no-op there post-Slice-A.5 and slated to be removed in the parent plan's Phase 3. Don't pad commands with it. Write/select commands (`deployments delete`, `deployments edit`, `deployments rollback` without `--git-sha`) still branch on the interactive flag — pass `--no-interactive` for those if you don't want a prompt, or supply every required arg.

The command surface is moving. Run `--help` on any command you're testing before assuming flag names; don't go from memory.

## Cooperative planning

Don't run anything until the user has signed off on the matrix. Skip confirmation in auto mode.

1. Read the diff. `git diff <base>...HEAD --stat` and targeted reads on the command files. Identify the behavior surface: which subcommands, flags, output formats, endpoints changed.
2. List design questions, not features. One line each. The right question reads as "would the user be happy seeing this output?", not "does the command run". Examples: "Does the `spec:` / `status:` split actually feel right when you eyeball `get -o yaml`?" "Does the plain-table list output stay readable for a deployment whose repo URL is 80 chars?" "Does the no-secrets case render as cleanly as the with-secrets case?"
3. Map questions to a small matrix. One row per case. 4–8 rows is usually right; 12 means you're testing features.
4. Present the matrix inline. Wait for the user to keep, cut, or add rows. In auto mode, keep this matrix internal and run it.

Matrix row format. Note "Look for" replaces a pass-fail "Expect" — the reviewer is meant to read the output and judge against these prompts, not check a list of facts.

| # | Command | Backend | Look for | Covers |
|---|---|---|---|---|
| 1 | `deployments get my-app -o yaml --project <test>` | test-env | spec/status split reads cleanly; field ordering; null vs omitted; PAT presentation | new display model shape |
| 2 | `deployments get --project <test>` | test-env | column choices, alignment, behavior with long repo URLs | plain-table list mode |
| 3 | `deployments get my-app --project <other-test>` vs default | test-env | does the override actually retarget the project | `--project` override |

If a row's "Look for" reads as "the command works", drop it. If it reads as "I'd want to eyeball this", keep it.

## Execution

Run rows top to bottom. For each, the goal is to capture the actual output the human reviewer will judge.

Capture:

- The exact command (including inline env vars).
- Exit code and elapsed time (latency is part of UX).
- **Verbatim stdout and stderr.** Don't trim, don't summarize, don't paraphrase. The report's body is the output, not your verdict on it. Truncation only when output is genuinely huge (200+ line log dumps); keep head, tail, and a `... <N> lines elided ...` marker.
- Your design-review observations as one-line "Notes" — what a thoughtful reviewer would point at. "REPO column is 67 chars wide on a 100-char terminal — leaves no room for two repos to align." "`status.warning: null` reads cleanly; the omitted vs explicit-null asymmetry is visible." "Why are both `display_name` and `name` here when they're equal?"

What NOT to do:

- **No `jq has(...)` / `python -c "assert ..."` style assertions.** The report is for human judgment, not pass/fail. If you find yourself asserting a key exists, you're in the wrong skill — that's pytest territory.
- **Don't pre-judge.** Notes flag things to look at; the reader judges. "Suspicious: `personal_access_token` shown as `********` even when the underlying secret name is also `GITHUB_PAT`" is fine. "Wrong: PAT should be unmasked" is not — that's the reader's call.
- **Don't paper over surprises.** If a row produces a stack trace or an unexpected shape, capture it verbatim and call it out in Highlights. Re-running with different flags to make the symptom go away is forbidden; that's the QA finding.

For comparison rows (`--project` override, `list` vs `get`, text vs json, with-data vs empty), capture both calls in the same row, side by side. The reader should be able to eyeball the difference without scrolling.

Long-running commands (`serve`, `logs --follow`) go in the background with `run_in_background: true`. Stop them before moving on.

### Storing raw transcripts

Don't dump `.log` / `.out` / `.err` files into `thoughts/shared/qa/`. The report is the artifact. Embed verbatim outputs in the report with fenced blocks.

If a single output is genuinely too large to embed (200+ lines of logs, many-deployment dumps), put it in `thoughts/shared/qa/raw/<date>-<scope>/<row-id>.txt` and link it from the report row. Default to embedding; the subdir is the exception, not the rule.

## Report

The report is a design and interaction review surface, not a test result. The reader scrolls through it and forms their own opinion: are columns aligned, is naming consistent, does this command feel right, is the error message clear. Your job is to put the evidence in front of them and flag the design questions worth their attention.

Write to `thoughts/shared/qa/<date>-llamactl-<scope>.md`. Post a one-or-two-sentence pointer inline plus the file link. The user reads the file, not your inline summary.

Template:

````
# llamactl QA: <one-line scope>

Backend: <test environment | local kind | offline | other>
Profile: <name> (<env URL>)
Project: <test project id/name>
Branch: <branch>  Commit: <short sha>

## Design questions for the reviewer

<3–6 bullets. These are the things you want the reader's eyes on. Not "all rows passed". Examples:
- "Is `spec:` + `status:` the right split, or is the extra indent annoying?"
- "REPO column gets pushed off-screen when the URL isn't a github short. Do we want a `--wide` mode or terminal-aware wrapping?"
- "`auth list` text mode shows ACTIVE column but the indicator is `*` only — is that legible enough?"
- "When secrets are unset, the JSON spec block is just `{display_name, repo_url, deployment_file_path, suspended: false}`. Is `suspended: false` worth showing on a fresh deploy or is it noise?"

Lean open-ended. If a question has an obvious answer, it doesn't belong here.>

## Rows

### 1. <one-line description, e.g. "deployments get my-app -o yaml">

Command:
```
$ <exact command>
```

Output (exit=<n>, <elapsed>):
```
<verbatim stdout>
```
<if stderr non-empty:>
Stderr:
```
<verbatim stderr>
```

Notes:
- <one-line observation a thoughtful reviewer would make>
- <another, if there's something else worth flagging>

### 2. <comparison row>

Command A:
```
$ <command 1>
```

Output A (exit=<n>):
```
<output>
```

Command B:
```
$ <command 2>
```

Output B (exit=<n>):
```
<output>
```

Notes:
- <what the side-by-side reveals>

... same shape for remaining rows ...

## Followups

- <design questions that need a human answer>
- <regressions or design choices that should turn into issues / changes>
- <items the matrix didn't cover but the reader should know about>
````

Rules:

- **Show, don't tell.** "Output shows a spec block with editable fields" → no. Paste the YAML. The reader can see the shape.
- **Keep verbatim formatting.** Tables in fenced blocks preserve column rendering. Don't reformat or "clean up" output.
- **Comparison rows live in one section.** Two adjacent fenced blocks labeled clearly beat two separate rows.
- **Notes are observations, not verdicts.** Point at what's interesting; let the reader judge.
- **No celebration.** If everything looks reasonable, the report just looks reasonable. Don't add "all green" framing.

The inline chat reply is one or two sentences pointing at the file. Don't restate the report.

## Common gotchas

- Stale profile. `llamactl auth list` first, every session. A profile pointing at last week's env produces 401s that look like a code bug.
- Active project drift. A test that "works on my machine" can fail elsewhere because the active project differs. Pin every row with `--project`.
- Non-test project mutation. If you mutate without `--project`, the active profile decides where it lands. Always pin write commands.
- Wrong environment for mutations. `llamactl auth env list` shows which environment is active. Never run create/edit/delete against production — switch to a test environment first. The active environment persists across sessions, so always verify before running mutating commands.
- Editing existing deployments. Don't edit deployments you didn't create — they may be someone's real work. Create throwaway deployments for QA and delete them when done.
- TUI/prompt risk on write commands. `deployments delete/edit/rollback` and `create` will prompt when interactive. For QA, supply every required arg or pass `--no-interactive`. Read commands (`get`, `logs`, `history`, `auth list`, `auth env list`) don't need the flag.
- `tee` ate a row of a multi-line table once during a run. Use shell `>` redirect for QA captures, not `tee`, when you need every line preserved.
- `serve` vs tilt. `llamactl serve` runs the appserver locally with no kubernetes. Tilt runs the full cloud stack in kind. Different scopes; pick one.
- Don't leak IDs. Deployment and org IDs are fine in your terminal and in `thoughts/`, not fine in a public PR description. Strip or alias them in anything that might leave the repo.
- Worktree venv mismatch. When QAing a worktree branch, the parent shell's `VIRTUAL_ENV` often points at the main repo's `.venv` (the worktree-specific one shadows it). `uv run` then warns and ignores the worktree env, silently using a stale binary. Either `unset VIRTUAL_ENV` once at the top of your session, or invoke the worktree's binary directly (`.venv/bin/llamactl`) for QA captures. The worktree binary is the one with the diff under test.

## Self-update

llamactl's command surface is moving. When a run teaches you a fact this skill doesn't have, edit it.

Triggers for an edit:

- A flag name, command path, or output shape was different from what the skill implied.
- A failure mode showed up that isn't in "Common gotchas".
- A backend setup step (especially local tilt) needed something the skill didn't say.

What to add:

- New gotchas go to "Common gotchas" as one-line entries.
- New mental-model facts go to "llamactl mental model".
- Tilt or backend changes go to "Pick a backend".

Edit the file at `.agents/skills/llamactl-qa/SKILL.md` (the `.claude/skills/` entry is a symlink into here, maintained by `uv run dev sync-skills`). Keep the voice terse; don't pad. If you're unsure whether the fact is durable or just incidental to one run, leave it out. False additions are worse than missing ones.
