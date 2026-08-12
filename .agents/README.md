# Curvine Agent Configuration

Cross-platform AI agent config hub. **`.agents/` is the single canonical source**; agent tools read it via symlinks.

## Layout

```text
.agents/
  README.md
  skills/             # project skills (shared)
  rules/              # cross-agent workflow rules
```

Only **`cv-*` skills** are tracked in git. Other skills (e.g. local `mermaid`) may exist under `.agents/skills/` but are gitignored.

## Agent skill paths

| Agent | Project path | Invocation |
| ----- | ------------ | ---------- |
| Claude Code | `.claude/skills/` → `.agents/skills/` | `/cv-create-pr` |
| Cursor | `.cursor/skills/` → `.agents/skills/` | auto-discover |
| Codex | `.agents/skills/` (native) | `/skills` |
| GitHub Copilot | `.github/skills/` → `.agents/skills/` | `/cv-create-pr` |

Edit versioned skills only under `.agents/skills/cv-*/`.

## Symlinks

```bash
ln -sfn ../.agents/skills .github/skills
rm -rf .cursor/skills && ln -sfn ../.agents/skills .cursor/skills
rm -rf .claude/skills && ln -sfn ../.agents/skills .claude/skills
```

## Versioned skills (`cv-` prefix)

| Skill | Stage |
| ----- | ----- |
| `cv-add-skills` | Meta — add/update cv skills |
| `cv-tasks-breakdown` | Plan → small tasks + sub-issues |
| `cv-create-issue` | File GitHub issue |
| `cv-handle-issue` | Fix issue (plan first, then code) |
| `cv-create-pr` | Create / update PR |
| `cv-submit-pr-review` | Review PR code content (find issues) |
| `cv-address-pr-review` | Handle review comments |
| `cv-run-workflow` | Dispatch GitHub Actions |
| `cv-csi-test` | CSI driver integration testing |

## Rules

- Cursor auto-rules: `.cursor/rules/` (`.mdc`)
- Shared rules: `.agents/rules/`

## AGENTS.md

Root `AGENTS.md` lists all versioned `cv-*` skills in `<available_skills>`. Use `cv-add-skills` when adding or updating skills.
