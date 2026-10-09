---
name: cv-create-pr
description: Create or update a Curvine pull request with standardized title and body sections (Summary, Issue/Design, Changes table, Test verified, Dependencies). Use when user asks to create, submit, update, or push a PR.
---

# cv-create-pr

Create or update a PR with Curvine title and body conventions.

## Language Rules

- **PR body (content):** write in **Chinese** — this is the communication medium with reviewers.
- **PR title:** keep in **English** (`<type>(<module>):` prefix stays English, machine-parseable).
- **Commit titles / messages:** always **English** (code-related text stays English).

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

## PR Title Format

```
<type>(<module>): <English description> (#<issueNumber>)
```

**Example:** `feat(fuse): add new compression algorithm (#123)`

**Supported types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`

Show title preview and get user approval.

## Commit Before PR

Commit title (subject) in **English** using the same format; body optional (English preferred).

```bash
git add <confirmed-files>
git commit -m "$(cat <<'EOF'
<type>(<module>): <English subject>

<optional body>
EOF
)"
git push -u origin HEAD
```

## PR Body Template

PR body in **Chinese** with bilingual section headings (e.g. `# 摘要 (Summary)`) so older search still matches. Use this structure:

```markdown
# 摘要 (Summary)

<1-3 sentences: what this PR does and why>

# 问题描述 / 设计 (Issue / Design)

Closes #<issueNumber> (or "Related to #<n>")

For **bug fixes**, include:
- Root cause
- Reproduction steps
- Fix approach

For **features**, include:
- Design decisions and trade-offs
- Link to parent issue / sub-issues if applicable

# 变更 (Changes)

| 模块 / 文件 | 变更内容 | 对现有行为的影响 |
| ------------- | ------ | --------------------------- |
| `path/to/file` | ... | None / behavior change: ... |

# 测试验证 (Test verified)

| 测试用例 | 结果 | 备注 |
| --------- | ------ | ----- |
| `cargo test -p ...` | 通过 | |
| ... | | |

# 依赖 (Dependencies)

- Related PRs: #<n> or None
- Related issues: #<n> or None
- Blocks / blocked by: or None
```

Do **not** include tool watermarks (e.g. "Made with Cursor").

## Create PR

Preview full title + body; wait for user approval.

```bash
gh pr create \
  --title "<type>(<module>): <English description> (#<issueNumber>)" \
  --body "$(cat <<'EOF'
# 摘要 (Summary)
...
EOF
)"
```

Default: **draft** unless user says `ready`:

```bash
gh pr create --draft --title "..." --body "..."
```

## Update Existing PR

```bash
gh pr edit <number> \
  --title "<type>(<module>): <English description> (#<issueNumber>)" \
  --body "$(cat <<'EOF'
...
EOF
)"
```

Or update current branch PR:

```bash
gh pr view --json number
gh pr edit <number> --title "..." --body "..."
```

## Checklist

- [ ] `make format` passed
- [ ] Tests run and recorded in **测试验证 (Test verified)**
- [ ] Title matches format with issue number
- [ ] Changes table covers key modules
- [ ] PR body in Chinese; commit/PR/issue titles in English
- [ ] User approved commit and PR content

## Related

- Review feedback → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
- Fix issue workflow → [cv-handle-issue](../cv-handle-issue/SKILL.md)
