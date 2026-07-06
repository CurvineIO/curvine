---
name: cv-add-skills
description: Add or update Curvine cv-* project skills under .agents/skills/, following naming and layout conventions, and sync the AGENTS.md registry. Use when creating a new cv skill, updating an existing cv skill structure, or registering skills after changes.
---

# cv-add-skills

Canonical guide for adding or updating versioned `cv-*` skills in the Curvine repository.

## When to Use

- User asks to add / create a new Curvine project skill
- User asks to update structure or conventions of an existing `cv-*` skill
- A new `cv-*` skill was added but `AGENTS.md` registry is out of date
- Onboarding: understand how Curvine skills are organized

Read [references/structure.md](references/structure.md) and [references/registry.md](references/registry.md) before editing.

## Organization Overview

```text
.agents/                          # canonical agent config (git tracked)
  README.md
  skills/
    cv-<name>/                      # only cv-* skills are versioned
      SKILL.md                      # required entrypoint
      references/                   # optional deep docs
      scripts/                      # optional executables
      assets/                       # optional templates
  rules/
    workflow.md                     # shared workflow rules

AGENTS.md                           # skill registry (<available_skills>)
.gitignore                          # .agents/skills/* + !.agents/skills/cv-*/

# symlinks (not copies)
.github/skills  -> ../.agents/skills
.cursor/skills  -> ../.agents/skills
.claude/skills  -> ../.agents/skills
```

**Rules:**

| Rule | Detail |
| ---- | ------ |
| Prefix | All versioned skills **must** start with `cv-` |
| Location | Edit only `.agents/skills/cv-<name>/` |
| Language | `description` and body in **English** |
| Git | Only `cv-*` dirs are tracked; other local skills are gitignored |
| Registry | Every `cv-*` skill **must** appear in `AGENTS.md` |

## Add a New cv Skill

### Step 1: Validate name

- Format: `cv-<kebab-case>` (e.g. `cv-create-pr`, `cv-csi-test`)
- Must not collide with existing dirs under `.agents/skills/`
- Prefer verb-noun matching workflow stage (create / handle / review / run / add)

### Step 2: Create directory

```bash
SKILL="cv-<name>"
mkdir -p ".agents/skills/${SKILL}/references"
```

Copy template if helpful:

```bash
cp assets/skill-template/SKILL.md ".agents/skills/${SKILL}/SKILL.md"
```

### Step 3: Write SKILL.md

Required frontmatter:

```yaml
---
name: cv-<name>          # must match directory name
description: <capability in one sentence>. Use when <specific triggers>.
---
```

Body guidelines:

- `## When to Use` — bullet triggers
- Step-by-step workflow with runnable commands
- Link related cv skills: `[cv-create-pr](../cv-create-pr/SKILL.md)`
- Keep SKILL.md lean; move long docs to `references/`
- Align with `.agents/rules/workflow.md` where applicable

### Step 4: Register in AGENTS.md

Inside `<!-- SKILLS_TABLE_START -->` … `<!-- SKILLS_TABLE_END -->`, add:

```xml
<skill>
<name>cv-<name></name>
<description><same as SKILL.md frontmatter description></description>
<location>project</location>
</skill>
```

Keep entries sorted alphabetically by `<name>`.

### Step 5: Update .agents/README.md

Add one row to the versioned skills table.

### Step 6: Verify symlinks

```bash
ln -sfn ../.agents/skills .github/skills
ln -sfn ../.agents/skills .cursor/skills
ln -sfn ../.agents/skills .claude/skills
ls -la .github/skills .cursor/skills .claude/skills
```

### Step 7: Self-check

- [ ] Directory is `.agents/skills/cv-<name>/`
- [ ] `name` in frontmatter matches directory
- [ ] `description` is English, ≤ 1024 chars, includes "Use when"
- [ ] `AGENTS.md` entry added/updated
- [ ] `.agents/README.md` table updated
- [ ] No files written under symlink paths directly
- [ ] Skill not gitignored (`git check-ignore -v .agents/skills/cv-<name>/SKILL.md` → no match)

## Update an Existing cv Skill

1. Edit files under `.agents/skills/cv-<name>/` only
2. If `description` changes → sync `AGENTS.md` `<description>` block
3. If skill renamed → rename directory, update all cross-links, update registry, remove old registry entry
4. Do not change `cv-` prefix or move skill outside `.agents/skills/`

## Do Not

- Add non-`cv-` skills to git (they belong in user global skills or local gitignored dirs)
- Edit skills inside `.cursor/skills/` or `.claude/skills/` directly (symlinks)
- Skip `AGENTS.md` registration
- Use Chinese in `description` (body may reference Chinese project docs paths when needed)
- Create standalone setup scripts in the repo for skill management

## Current cv Skills (maintain this list when adding)

| Skill | Stage |
| ----- | ----- |
| `cv-add-skills` | Meta — add/update skills |
| `cv-create-issue` | Issue |
| `cv-create-pr` | PR |
| `cv-csi-test` | Testing |
| `cv-handle-issue` | Issue |
| `cv-submit-pr-review` | PR |
| `cv-address-pr-review` | PR |
| `cv-run-workflow` | CI |
| `cv-tasks-breakdown` | Planning |

## Related

- Workflow rules → `.agents/rules/workflow.md`
- Registry format detail → [references/registry.md](references/registry.md)
- Directory layout detail → [references/structure.md](references/structure.md)
