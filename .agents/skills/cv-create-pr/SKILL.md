---
name: cv-create-pr
description: Create or update a Curvine pull request with standardized title and body sections (Summary, Issue/Design, Changes table, Test verified, Dependencies). Use when user asks to create, submit, update, or push a PR.
---

# cv-create-pr

Create or update a PR with Curvine title and body conventions.

## When to Use

- User asks to create / submit / open a PR
- User asks to update PR title or body
- After `cv-handle-issue` or task commits are ready to publish

## Pre-flight

```bash
make format
git status
git diff
```

Only stage files changed in the current task/session. Confirm with user before commit.

## Sensitive data gate (mandatory)

Every push and every PR body must pass [cv-sensitive-review](../cv-sensitive-review/SKILL.md).
Run it **before** `git push` and **before** `gh pr create` / `gh pr edit`:

```bash
SCAN=.agents/skills/cv-sensitive-review/scripts/scan.sh
bash "$SCAN" --staged                      # what the commit adds
bash "$SCAN" --range origin/main...HEAD    # what the PR adds
bash "$SCAN" /tmp/pr_body.md               # the body you are about to publish
```

Exit `1` (S0/S1) blocks the PR: redact, or get an explicit user decision recorded in the
body. Exit `3` (S2) requires a decision too, but never a silent pass.
Write the body to `/tmp/pr_body.md` first and pass it with `--body-file`; ad-hoc
`--body` heredocs are not gate-able. Record the verdict in the PR body (section 6 below).

## PR Title Format

```
<type>(<module>): <description> (#<issueNumber>)
```

**Example:** `feat(fuse): add new compression algorithm (#123)`

**Supported types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`

Show title preview and get user approval.

## Commit Before PR

```bash
git add <confirmed-files>
git commit -m "$(cat <<'EOF'
<type>(<module>): <subject>

<optional body>
EOF
)"
git push -u origin HEAD
```

## PR Body Template

All PR bodies in **English**. Use this structure:

```markdown
# Summary

<1-3 sentences: what this PR does and why>

# Issue Describe / Design

Closes #<issueNumber> (or "Related to #<n>")

For **bug fixes**, include:
- Root cause
- Reproduction steps
- Fix approach

For **features**, include:
- Design decisions and trade-offs
- Link to parent issue / sub-issues if applicable

# Changes

| Module / File | Change | Impact on existing behavior |
| ------------- | ------ | --------------------------- |
| `path/to/file` | ... | None / behavior change: ... |

# Test verified

| Test case | Result | Notes |
| --------- | ------ | ----- |
| `cargo test -p ...` | PASS | |
| ... | | |

# Dependencies

- Related PRs: #<n> or None
- Related issues: #<n> or None
- Blocks / blocked by: or None

# Sensitive data review

- Scope: <diff range / payload files>
- Scanner: `bash .agents/skills/cv-sensitive-review/scripts/scan.sh --range origin/main...HEAD` -> PASS
- Manual pass: internal endpoints, topology, identifiers, customer data - none found
- Verdict: PASS (no S0/S1/S2 findings)
```

Do **not** include tool watermarks (e.g. "Made with Cursor").

## Create PR

Preview full title + body; wait for user approval.

```bash
gh pr create \
  --title "<type>(<module>): <description> (#<issueNumber>)" \
  --body-file /tmp/pr_body.md
```

Default: **draft** unless user says `ready`:

```bash
gh pr create --draft --title "..." --body "..."
```

## Update Existing PR

```bash
gh pr edit <number> \
  --title "<type>(<module>): <description> (#<issueNumber>)" \
  --body-file /tmp/pr_body.md
```

Or update current branch PR:

```bash
gh pr view --json number
gh pr edit <number> --title "..." --body "..."
```

## Checklist

- [ ] `make format` passed
- [ ] Sensitive data gate run on the diff **and** on `/tmp/pr_body.md` (PASS recorded)
- [ ] Tests run and recorded in **Test verified**
- [ ] Title matches format with issue number
- [ ] Changes table covers key modules
- [ ] User approved commit and PR content

## Related

- Review feedback → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
- Fix issue workflow → [cv-handle-issue](../cv-handle-issue/SKILL.md)
