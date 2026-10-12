---
name: cv-comind-internal
description: Implement Curvine work tracked in CoMind on the internal repo (code.oppoer.me, main-next) with parent/child issues, commit KEY tagging, and Huoyan (cr push / MR) delivery. Use when the task is a CoMind issue KEY, internal MR, comind CLI workflow, or code.oppoer.me instead of GitHub issue/PR.
---

# cv-comind-internal

End-to-end delivery for **internal** Curvine development coordinated in **CoMind**, merged via **Huoyan** on `http://code.oppoer.me/80338435/curvine.git` (`main-next`).

For **GitHub** issues and `gh pr create`, use [cv-handle-issue](../cv-handle-issue/SKILL.md) and [cv-create-pr](../cv-create-pr/SKILL.md) instead.

## When to Use

- User or task references a **CoMind issue KEY** (e.g. `CV-12`) or `comind issue`
- Work targets **internal** remote / `main-next`, not community `main` on GitHub
- Delivery is **Huoyan review** (`cr push` or Merge Request), not GitHub PR

## Prerequisites

- `comind` CLI authenticated for the workspace
- Git remote `origin` → internal repo (oppo)
- Git author email **`@oppo.com`**
- **OneTeam ID** from parent issue or user when required by team policy; if missing, ask before commit
- Huoyan CLI `cr` configured (`cr -v`, `cr token …`) when using `cr push`

## Step 1: Load CoMind context

```bash
comind issue get <issue-id-or-key> --output json
comind issue comment list <id> --roots-only --summary --compact
```

If this is an implementation sub-issue, also read the **parent** issue for acceptance criteria and OneTeam ID.

Record:

| Field | Use |
| ----- | --- |
| `identifier` (KEY) | Every commit, branch name, MR title |
| Parent acceptance | Definition of done |
| OneTeam ID | Commit/MR body when provided |

## Step 2: Plan (no coding until approved)

Same discipline as [cv-handle-issue](../cv-handle-issue/SKILL.md): summarize type, scope, files, tests, size. Route **> ~500 lines** to [cv-tasks-breakdown](../cv-tasks-breakdown/SKILL.md) with CoMind sub-issues (`comind issue create --parent …`).

## Step 3: Checkout and branch

```bash
comind repo checkout http://code.oppoer.me/80338435/curvine.git --ref main-next
git fetch origin
git checkout -b feature/<KEY>-<short-slug>   # or agent/<agent>/<KEY>-<slug>
```

Do **not** push directly to `main-next` or `main`.

## Step 4: Implement

- Product code: `make format` before every commit; run relevant tests
- Docs/skills only: run `make format` if Rust touched; otherwise `npm run commitlint` when commit messages change

## Step 5: Commit format

Conventional Commits + **CoMind KEY** (required). Include OneTeam when known.

```text
<type>(<scope>): <english subject> (<KEY>)
```

Example:

```text
docs(skills): add cv-comind-internal for comind workflow (CV-3)
```

Optional footer: `OneTeam: <id>` when parent issue lists it.

## Step 6: Publish review (Huoyan)

**Feature branch → `main-next`** (typical):

1. Push branch (if remote branch missing, Huoyan may create it on first `cr push`)
2. Prefer **Merge Request** in Huoyan/GitLab UI: source = feature branch, target = `main-next`
3. MR title: `<KEY>: <type>(<module>): <summary>`

**Trunk-style / hotfix** (when team uses direct branch push):

```bash
cr push    # or cr push -a for a new ChangeId
```

Never use merge-pull; use **rebase merge** when merging to target branch (see team Huoyan docs).

Do **not** use `gh pr create` for internal-only delivery.

## Step 7: Register in CoMind

On the **implementation sub-issue**:

```markdown
## Huoyan / MR submitted
- CoMind: <KEY>
- Branch: …
- Review: <ChangeId or MR URL>
- Self-test: …
```

On the **parent issue**: one-line summary + link to sub-issue/review.

Optional: `comind issue metadata set <sub-id> mr_url "<url>"`

Query linked PRs when webhook is enabled:

```bash
comind issue pull-requests <id> --output json
```

## Step 8: Close out

After review is merged and landed on the target branch: comment merge SHA on parent issue; `comind issue status <sub-id> done --no-start` when acceptance is met. Do not close parent unless all criteria are done.

## Routing summary

| Track | Issue source | Delivery skill |
| ----- | ------------- | -------------- |
| Community | GitHub `#n` | cv-handle-issue → cv-create-pr (`gh`) |
| Internal | CoMind KEY | **cv-comind-internal** → Huoyan MR / `cr push` |

## Related

- Add/update cv skills → [cv-add-skills](../cv-add-skills/SKILL.md)
- GitHub PR body template (reference only for dual-track) → [cv-create-pr](../cv-create-pr/SKILL.md)
- Shared rules → `.agents/rules/workflow.md`
