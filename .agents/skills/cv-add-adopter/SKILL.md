---
name: cv-add-adopter
description: Add or update an organization entry in the Curvine ADOPTERS.md file, including company/project name, logo, and use-case description. Use when a user says their company is using Curvine, asks to add their company logo to adopters, or wants to update an existing adopter entry.
---

# cv-add-adopter

Add an organization to [ADOPTERS.md](../../../ADOPTERS.md) — the public list of companies and projects using Curvine.

## When to Use

- User says their company/project is using Curvine and wants to be listed
- User asks to add their company logo to ADOPTERS.md or the website adopters wall
- User asks to update an existing adopter entry (use case, logo, link)
- A PR touches `ADOPTERS.md` or `docs/adopters/` and needs review guidance

## Entry Format

ADOPTERS.md has two tables:

1. **Production adopters** — `## Companies/Projects using Curvine in production`
2. **Evaluation / non-production users** — `## Companies/Projects evaluating or using Curvine` (append this section if it does not exist)

Each table has three columns:

```markdown
| Company / Project | Logo | Use Case |
| ----------------- | ---- | -------- |
| ExampleCorp | <img src="docs/adopters/examplecorp.svg" alt="ExampleCorp" height="40"> | ExampleCorp uses Curvine for ... |
```

## Workflow

### Step 1: Collect information from the user

Ask for (skip anything the user explicitly declines):

- Organization name (as they want it displayed)
- Production or evaluation — which table it belongs in
- Use case description (1–3 sentences: what they use Curvine for, which interfaces — FUSE, S3, CSI, SDK — and what it accelerates)
- Logo file (optional)
- Website URL (optional, used on the docs site)

### Step 2: Validate the logo

Logo requirements:

| Requirement | Detail |
| ----------- | ------ |
| Format | SVG preferred; transparent PNG acceptable (min 240px wide) |
| Location | `docs/adopters/<kebab-case-name>.<ext>` — e.g. `docs/adopters/oppo.svg` |
| Size | Render at `height="40"`; file < 100 KB |
| Name | kebab-case, lowercase, no spaces |
| Rights | User must be authorized to grant the project permission to display the logo (trademark/brand usage permission) |

If the user provides no logo, add the entry without the logo cell content (the docs site renders a text placeholder).

### Step 3: Edit ADOPTERS.md

- Append the row to the correct table, keeping existing row order
- Keep the use-case description factual and in English
- Do not modify other rows

### Step 4: Submit the PR

Follow [cv-create-pr](../cv-create-pr/SKILL.md):

- Title: `docs(adopters): add <name> to ADOPTERS.md`
- Body: include who requested it and confirmation that the submitter is authorized to display the logo
- One adopter per PR

## Review Checklist (for maintainers handling the PR)

- [ ] Logo path exists and file is SVG/transparent PNG, < 100 KB
- [ ] Logo rendered with `height="40"` so the table stays consistent
- [ ] Entry added to the correct table (production vs evaluation)
- [ ] Use-case description is factual, English, 1–3 sentences
- [ ] If only a logo with no use case: acceptable, use case may be omitted
- [ ] Logo file name is kebab-case and placed under `docs/adopters/`

## Related

- [cv-create-pr](../cv-create-pr/SKILL.md) — submit the change
- ADOPTERS.md is mirrored on the docs site (curvine-doc) "Who's Using Curvine" section; a maintainer syncs the docs-site data file after merging
