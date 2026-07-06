# Curvine cv-* Skill Structure

## Canonical layout

```text
.agents/skills/cv-<name>/
  SKILL.md              # required — entrypoint + YAML frontmatter
  references/           # optional — loaded on demand
    *.md
  scripts/              # optional — bash/python helpers
    *
  assets/               # optional — templates, not loaded into context
    *
```

## Cross-platform visibility

| Agent | Reads from |
| ----- | ---------- |
| Codex | `.agents/skills/` directly |
| Cursor | `.cursor/skills/` → symlink |
| Claude Code | `.claude/skills/` → symlink |
| GitHub Copilot | `.github/skills/` → symlink |

Single source of truth: **`.agents/skills/cv-*/`**

## Git tracking

`.gitignore`:

```gitignore
.agents/skills/*
!.agents/skills/cv-*/
```

- `cv-*` → committed
- anything else under `.agents/skills/` → local only (e.g. `mermaid/`)

## Naming

| Item | Convention | Example |
| ---- | ---------- | ------- |
| Directory | `cv-` + kebab-case | `cv-create-pr` |
| Frontmatter `name` | same as directory | `cv-create-pr` |
| Reference files | kebab-case.md | `workflow-catalog.md` |
| Scripts | snake_case or kebab | `test-suite.sh` |

## SKILL.md sections (recommended)

1. Title (`# cv-<name>`)
2. `## When to Use`
3. Workflow steps (`## Step N:` or `### Step N:`)
4. `## Related` — links to other cv skills
5. Optional `## Checklist` / `## Rules` / `## Do Not`

## Cross-linking

Use relative paths between cv skills:

```markdown
See [cv-create-pr](../cv-create-pr/SKILL.md)
```

## Workflow skill chain

```text
cv-tasks-breakdown → cv-create-issue (sub-issues)
cv-create-issue → cv-handle-issue
cv-handle-issue → cv-tasks-breakdown (large scope)
cv-handle-issue → cv-create-pr
cv-create-pr → cv-address-pr-review
cv-run-workflow → cv-create-pr (for workflow YAML changes)
```

New skills should declare where they fit in this chain.
