---
name: cv-handle-issue
description: Fix a Curvine GitHub issue from analysis through plan approval to implementation, routing large features to cv-tasks-breakdown. Use when user asks to fix, resolve, or implement a specific issue URL or issue number.
---

# cv-handle-issue

End-to-end issue resolution with explicit user approval before coding.

## When to Use

- User provides an **issue URL or issue number** and asks to fix / implement / resolve it

## Prerequisites

- Valid issue reference (URL or `#<number>`)
- `gh auth status` OK

## Step 1: Read Issue

```bash
gh issue view <NUMBER> --json number,title,body,labels,state,url
```

Extract:

- Bug vs feature (labels, title prefix)
- Goal / Not Goal / Test Plan sections (if present)
- Acceptance criteria and reproduction steps

If issue is **closed** or scope is unclear, stop and ask user.

## Step 2: Analyze

Summarize for the user:

| Item | Content |
| ---- | ------- |
| Type | bug / feature |
| Problem | What is wrong or what is requested |
| Affected modules | From issue body + codebase exploration |
| Risks | Breaking changes, concurrency, compatibility |
| Test strategy | Existing tests + new cases needed |

Use deepwiki / codegraph as needed for CurvineIO/curvine.

## Step 3: Propose Plan (No Coding Yet)

Present a plan:

```markdown
## Proposed Approach

1. ...
2. ...

## Files to touch (estimate)
- `path/...` — ...

## Test plan
- ...

## Estimated size
~<N> lines / <M> files
```

### Size routing

| Estimate | Next step |
| -------- | --------- |
| ≤ ~500 lines | Continue with this skill after user approval |
| > ~500 lines or multi-module feature | Use [cv-tasks-breakdown](../cv-tasks-breakdown/SKILL.md); create sub-issues |

**Do not write code until the user explicitly approves the plan.**

Iterate on the plan until approval. For ambiguous requirements, ask clarifying questions.

## Step 4: Branch

After approval:

```bash
git fetch origin
git checkout main && git pull origin main
git checkout -b fix/<NUMBER>-<short-slug>   # or feat/...
```

## Step 5: Implement

- Minimal scope aligned with approved plan
- Follow `.cursor/rules/std-common/ai-workflow.mdc`
- Add/update tests per plan
- Run `make format` and relevant tests before commit

If using task breakdown, complete **one task / one commit** at a time.

## Step 6: Handoff to PR

When implementation and tests pass, use [cv-create-pr](../cv-create-pr/SKILL.md):

- PR title: `<type>(<module>): <description> (#<NUMBER>)`
- Fill all PR body sections including **Test verified**

## Rules

- **No coding without explicit user approval** on the plan
- Code change target: **≤ ~500 lines** per issue unless broken down via `cv-tasks-breakdown`
- Large features → sub-issues + task breakdown, not one monolithic PR
- End with the Wrap-up Summary section and its Chinese translation

## Wrap-up Summary (required)

Close every run with the block below, in the local chat, whether the run stopped at an approved plan or reached a pushed PR. GitHub-facing text stays English; this block is for the user, and it ends with its Chinese translation.

| Part | Content |
| ---- | ------- |
| Issue / PR | `#<n>` and URL, plus the PR number and the base and head SHAs when one was opened |
| Comments | `none` when no review comment was posted or addressed, otherwise id plus `path:line`, severity, and `posted` / `edited` / `skipped` |
| Problem solved | the root cause found in the code, and the behavior the fix restores |
| Approach | the approved plan, the files and crates changed, the tests added or updated, and the checks run |
| Reason | why this approach over the alternatives, and why the change stayed inside the approved scope |
| Open items | plan steps deferred to a follow-up issue, known gaps, and the next skill to run |

Then append the same summary in Chinese under a `### Summary (Chinese)` heading: translate the conclusions and reasons, keep issue/PR numbers, paths, SHAs, commands and quoted output verbatim, and do not add claims that are absent above.

## Related

- Break down large work → [cv-tasks-breakdown](../cv-tasks-breakdown/SKILL.md)
- File new issue → [cv-create-issue](../cv-create-issue/SKILL.md)
- Submit PR → [cv-create-pr](../cv-create-pr/SKILL.md)
- Review feedback → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
