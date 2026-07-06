---
name: cv-submit-pr-review
description: Perform a direct code review of a Curvine PR by reading the diff and changed files, analyzing correctness, safety, and design issues, and producing structured findings. Use when user asks to review a PR's code, do a code review, or check a PR before merge.
---

# cv-submit-pr-review

Review a Curvine PR's code content directly: locate PR → read diff → gather context → analyze → report findings → optionally post review.

## When to Use

- User asks to review / check / look at a PR's code
- User wants a code review before merge
- User provides a PR URL or number and asks for review
- User asks "is this PR OK?" or "review this change"

**Not for:** responding to existing reviewer comments → use [cv-address-pr-review](../cv-address-pr-review/SKILL.md).

## Review Scope

Focus **exclusively on code content**:

| Dimension | What to check |
| --------- | ------------- |
| Correctness | Logic errors, edge cases, off-by-one, None/null handling, error propagation |
| Safety | Concurrency races, panics / unwrap in hot paths, unsafe blocks, resource leaks, lock ordering |
| Design | Naming, module boundaries, abstractions, consistency with existing patterns |

**Ignore:** commit message quality, PR description wording, pure formatting nits (run `make format` separately).

## Step 1: Locate PR

```bash
# Current branch
gh pr view --json number,url,title,headRefName,baseRefName,additions,deletions,changedFiles

# By number or URL
gh pr view <number> --json number,url,title,headRefName,baseRefName,additions,deletions,changedFiles
```

If no PR exists on the current branch, ask the user for a PR number/URL or whether to review uncommitted local changes instead.

## Step 2: Collect Diff

```bash
PR=<number>

# Full diff with stats
gh pr diff ${PR}

# Per-file summary
gh pr view ${PR} --json files --jq '.files[] | "\(.path)\t+\(.additions)\t-\(.deletions)"'
```

For large PRs (> ~1000 lines), review file-by-file in logical batches rather than one giant diff.

## Step 3: Gather Context

For each changed file, read the **full file** (not just the diff hunk) so you understand surrounding code, types, and call sites:

1. Read the changed file in full
2. Find callers / definitions of changed symbols (`SearchSymbol`, `Grep`)
3. Check related module knowledge via `SearchMemory` for conventions and patterns

Context prevents false positives — a change that looks wrong in isolation may be correct given surrounding code.

## Step 4: Analyze (Per File)

Work through each changed file systematically. For every non-trivial change ask:

- **Correctness:** Does the logic do what it claims? Are edge cases handled? Are errors propagated, not swallowed?
- **Safety:** Can this panic? Is there a race? Are locks held correctly? Any `unsafe`? Any resource (file/conn/mutex) not released?
- **Design:** Does this fit existing patterns in the module? Is the abstraction at the right level? Are names clear?

Cross-reference against:
- Module knowledge cards (`SearchMemory`) for architecture and conventions
- Existing tests to verify the change is covered

## Step 5: Findings Report (Mandatory Before Any Action)

Output a table of all findings. Do **not** post to GitHub yet.

```markdown
| # | Severity | File:Line | Finding | Suggestion |
| - | -------- | --------- | ------- | ---------- |
| 1 | Critical | src/foo.rs:42 | Unwrap on Option that can be None when X | Use `ok_or_else` + error propagation |
| 2 | Major | src/bar.rs:88 | Lock held across await — potential deadlock | Move await outside lock scope |
| 3 | Minor | src/baz.rs:15 | Name `do_thing` is vague | Rename to `replicate_block` |
```

### Severity levels

| Level | Meaning |
| ----- | ------- |
| Critical | Bug, data loss, security hole, deadlock — must fix before merge |
| Major | Likely bug, missing error handling, design issue — should fix |
| Minor | Clarity, naming, minor consistency — nice to have |
| Nit | Style / preference — optional |

If **no issues found**, state that explicitly with a brief summary of what was checked.

## Step 6: User Decision

Present the findings table and ask the user how to proceed:

- **Fix** — address findings locally, then use [cv-create-pr](../cv-create-pr/SKILL.md) to update
- **Post review** — submit findings as a GitHub PR review (Step 7)
- **Discuss** — clarify specific findings before acting

Do **not** fix or post anything without explicit user approval.

## Step 7: Post Review to GitHub (Optional)

Only when the user asks to post. Submit as a PR review with inline + summary comments.

### Inline comments

```bash
gh api \
  -X POST \
  "repos/{owner}/{repo}/pulls/${PR}/comments" \
  -f body="<finding detail + suggestion>" \
  -f path="<file>" \
  -F line=<line> \
  -F side=RIGHT \
  -f commit_id="$(gh pr view ${PR} --json headRefOid --jq .headRefOid)"
```

Use `line` for single-line, or `start_line` + `line` for multi-line ranges (both on `side=RIGHT`).

### Review summary

```bash
gh pr review ${PR} \
  --request \
  --body "$(cat <<'EOF'
## Code Review

Reviewed <N> files, <M> lines changed.

### Summary
<overall assessment: is the change sound? what's the main risk?>

### Findings: <count>
- Critical: <n>
- Major: <n>
- Minor: <n>

See inline comments for details.
EOF
)"
```

Review states: `--request` (request changes), `--approve`, or `--comment` (neutral). Default to `--comment` unless the user specifies otherwise.

## Checklist

- [ ] PR located and diff collected
- [ ] Changed files read in full (not just hunks)
- [ ] Callers / definitions of changed symbols checked
- [ ] Module conventions verified (knowledge cards / existing patterns)
- [ ] Findings table produced with severity per item
- [ ] User decided on next action (fix / post / discuss)
- [ ] Review posted only after explicit approval

## Related

- Handle existing reviewer comments → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
- Fix findings and update PR → [cv-create-pr](../cv-create-pr/SKILL.md)
- Review during issue fix → [cv-handle-issue](../cv-handle-issue/SKILL.md)
