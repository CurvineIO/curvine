# Curvine workflow rules

Referenced by `cv-*` skills.

## Issue tracks

| Track | Tracker | Skill |
| ----- | ------- | ----- |
| Community | GitHub issue | `cv-handle-issue`, `cv-create-issue` |
| Internal | CoMind parent + sub-issues | `cv-comind-internal` |

GitHub: follow `.github/ISSUE_TEMPLATE/`; features need **Goal**, **Not Goal**, **Test Plan**; sub-tasks title `[SUB]`, body `Parent: #<n>`.

Internal: parent issue holds acceptance criteria; implementation sub-issue holds branch/MR notes; commits include CoMind **KEY**; Huoyan MR or `cr push` to `main-next` (no direct push to main branches, rebase merge only).

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
