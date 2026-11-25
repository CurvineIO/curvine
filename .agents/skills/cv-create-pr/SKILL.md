---
name: cv-create-pr
description: Create or update a Curvine pull request, validate DCO sign-offs, and diagnose or repair failed DCO checks. Use when user asks to create, submit, update, or push a PR or fix its DCO check.
---

# cv-create-pr

Create or update a PR with Curvine title and body conventions.

## When to Use

- User asks to create / submit / open a PR
- User asks to update PR title or body
- User asks to diagnose or repair a PR's DCO check
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
bash "$SCAN" --range "$BASE_SHA...HEAD"    # what the PR adds against its actual base
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

Sign off new commits with `git commit -s`. Before pushing, check every commit
introduced by the **actual PR base** (which may be another feature branch):

```bash
SCAN=.agents/skills/cv-sensitive-review/scripts/scan.sh
bash "$SCAN" /tmp/commit-message.md
git add <confirmed-files>
bash "$SCAN" --staged
git commit -s -F /tmp/commit-message.md
# Set BASE_SHA to the fetched target commit for this PR.
python3 .agents/skills/cv-create-pr/scripts/check-dco.py "$BASE_SHA" HEAD
```

Run the PR diff and body scans above before pushing. If the local check or
GitHub's `dco` job fails, follow [DCO checks and recovery](references/dco.md)
before publishing or updating the PR. Do not sign off on behalf of another
author or blindly rebase onto `main`.

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
- Scanner: `bash .agents/skills/cv-sensitive-review/scripts/scan.sh --range "$BASE_SHA...HEAD"` -> PASS
- Manual pass: internal endpoints, topology, identifiers, customer data - none found
- Verdict: PASS (no S0/S1/S2 findings)
```

Do **not** include tool watermarks (e.g. "Made with Cursor").

## Create PR

Preview full title + body; wait for user approval. After the DCO and sensitive
data checks pass, push the branch to the chosen head remote and open the PR.

```bash
git push -u "$HEAD_REMOTE" "HEAD:refs/heads/$HEAD_BRANCH"
gh pr create --draft --base "$BASE_BRANCH" --head "$PR_HEAD" \
  --title "<type>(<module>): <description> (#<issueNumber>)" \
  --body-file /tmp/pr_body.md
```

Specify `HEAD_REMOTE`, `HEAD_BRANCH`, and `BASE_BRANCH` from the intended PR.
Set `PR_HEAD` to the branch name, or `owner:branch` for a fork. Omit `--draft`
only if the user asks for a ready PR.

## Update Existing PR

After the checks pass, push the approved commits to the PR's head branch.
For a DCO-only history rewrite, use the explicit lease in the recovery guide.
To update the PR title or body:

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
- [ ] DCO verified per commit against the actual PR base; after a repair, confirm the GitHub `dco` check on the new head
- [ ] Sensitive data gate run on the diff **and** on `/tmp/pr_body.md` (PASS recorded)
- [ ] Tests run and recorded in **Test verified**
- [ ] Title matches format with issue number
- [ ] Changes table covers key modules
- [ ] User approved commit and PR content

## Related

- Review feedback → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
- Fix issue workflow → [cv-handle-issue](../cv-handle-issue/SKILL.md)
