# Curvine workflow rules

Referenced by `cv-*` skills.

## Sensitive data gate (applies to everything below)

No issue, PR, review comment, reply or commit is published before it passes
`cv-sensitive-review`. Run it on the payload **and** on the diff:

```bash
SCAN=.agents/skills/cv-sensitive-review/scripts/scan.sh
bash "$SCAN" /tmp/body.md                    # issue / PR / comment draft
bash "$SCAN" --staged                        # commit
bash "$SCAN" --range origin/main...HEAD      # PR diff
```

- Exit `1` (S0/S1: live credential, private key, internal endpoint, customer data) blocks publication.
- Exit `3` (S2: internal ticket ID, corporate email, PII-lite) requires an explicit decision, never a silent pass.
- Never echo the matched value anywhere: cite `path:line` + commit SHA.
- Deleting a committed secret does not revoke it - name the rotation owner before merging.

## Issue

- Follow `.github/ISSUE_TEMPLATE/` format
- Feature issues require **Goal**, **Not Goal**, **Test Plan** sections
- Sub-tasks: title `[SUB]`, body links `Parent: #<n>`

## Implementation

- `cv-handle-issue`: no coding until user approves the plan
- Large change (> ~500 lines): use `cv-tasks-breakdown` + sub-issues
- `make format` before every commit
- Each task must have test coverage

## PR title

```
<type>(<module>): <description> (#<issueNumber>)
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`

## PR body sections

1. **Summary**
2. **Issue Describe / Design** (root cause / repro / fix for bugs)
3. **Changes** (table: module, change, impact)
4. **Test verified** (table: case, result, notes)
5. **Dependencies** (related PRs / issues)

## Review

- Analyze every comment ID before coding
- Present accept/reject table; wait for user approval
- Reply in English; resolve fixed threads on GitHub

## CI

- Workflow changes: validate via `cv-run-workflow` on a feature branch
