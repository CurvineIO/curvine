# AGENTS.md Registry

## Location

Repository root: `AGENTS.md`

Markers (do not remove):

```xml
<!-- SKILLS_TABLE_START -->
...
<!-- SKILLS_TABLE_END -->
```

## Entry template

```xml
<skill>
<name>cv-<name></name>
<description><English description matching SKILL.md frontmatter></description>
<location>project</location>
</skill>
```

## Usage block (do not narrow to block global skills)

The `<usage>` section clarifies:

- **Project workflow** → prefer `cv-*` in `<available_skills>`
- **User global skills** → still available via agent tool (e.g. `~/.cursor/skills/`)
- **Do not** invent unlisted project skills

## Maintenance checklist

When adding `cv-new-skill`:

1. Create `.agents/skills/cv-new-skill/SKILL.md`
2. Append `<skill>` block inside `<available_skills>`
3. Sort entries alphabetically by name
4. Verify `description` matches frontmatter exactly
5. Update `.agents/README.md` skills table
6. Update skill list in `cv-add-skills/SKILL.md` if maintained there

When updating description only:

1. Edit frontmatter in `SKILL.md`
2. Edit matching `<description>` in `AGENTS.md`
3. No directory rename needed

When renaming `cv-old` → `cv-new`:

1. `git mv .agents/skills/cv-old .agents/skills/cv-new`
2. Update `name:` in frontmatter
3. Replace registry entry (remove old, add new)
4. `rg 'cv-old' .agents/skills/` — fix cross-links

## Validation

```bash
# should NOT be ignored
git check-ignore -v .agents/skills/cv-add-skills/SKILL.md

# should be ignored (local-only skill example)
git check-ignore -v .agents/skills/mermaid/SKILL.md
```

## Invocation hint for agents

Agents read skills via:

```text
.agents/skills/<skill-name>/SKILL.md
```

Not via `openskills` unless user environment configures it.
