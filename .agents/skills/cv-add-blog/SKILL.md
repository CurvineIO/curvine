---
name: cv-add-blog
description: Publish a paired English and Chinese blog post with localized images to the CurvineIO/curvine-doc Docusaurus site, including frontmatter, cover, i18n directory, local build validation, and PR creation. Use when the user asks to submit an article to the Curvine blog, publish an EN/ZH blog post, or convert a Markdown article into a curvine-doc post.
---

# cv-add-blog

Publish one article as a paired English + Chinese blog post in [`CurvineIO/curvine-doc`](https://github.com/CurvineIO/curvine-doc) (Docusaurus 3.8).

## When to Use

- User asks to submit, publish, or post an article to the Curvine blog
- User provides a Markdown article and asks for EN + ZH versions on the docs site
- User asks to convert a PDF / report / production write-up into a blog post

## Target repo

| Item | Value |
| ---- | ----- |
| Site repo | `CurvineIO/curvine-doc` |
| Framework | Docusaurus 3.8 |
| EN post | `blog/YYYY-MM-DD-<slug>/index.md` |
| ZH post | `i18n/zh-cn/docusaurus-plugin-content-blog/YYYY-MM-DD-<slug>/index.md` |
| Images | Same directory as each `index.md` |
| Metadata | `blog/authors.yml`, `blog/tags.yml` |

## Post contract

Directory name carries the date and slug. Do not add a body H1; Docusaurus renders `title` as the page heading.

```yaml
---
title: "Concrete, number-led title"
description: "1-2 sentences with the headline result; used in cards and SEO."
authors: [david]
tags: [benchmark, bigdata]
image: ./cover.png
---
```

Body opening:

```markdown
![Cover](./cover.png)

> Results first: one-paragraph summary with the headline numbers.

<!-- truncate -->

## 1. First section
```

- Use `##` for top-level sections.
- Every image uses a path relative to the post: `![alt](./02-bottlenecks.png)`.
- `<!-- truncate -->` sits on its own line after the opening quote.

## Localization rules

- Both locales need full frontmatter and their **own copy** of every image.
- EN post: English prose and English-text figures. ZH post: Chinese prose and Chinese-text figures.
- Keep section structure, tables, numbers, and code blocks identical across locales.
- Never reuse an English-text figure in the Chinese post, or vice versa.
- Verify version numbers against the source in both posts, covers, and figure footers.

## Workflow

### 1. Prepare content and images

- Confirm facts and versions from the source document. If the source states a version, every cover and figure footer must match it.
- Prefer rebuilding diagrams as HTML/CSS at 1600x800 and rendering at 2x with headless Chrome. This keeps labels crisp and avoids garbled raster text.
- Cover: 16:9 (1792x1024). Keep text out of the artwork area; leave negative space for the headline.
- Use clean crops: no surrounding article text in a figure image.

### 2. Work in a clean worktree

```bash
git -C /path/to/curvine-doc fetch origin
git -C /path/to/curvine-doc worktree add -b blog/<slug> /path/to/curvine-doc-<slug> origin/main
cd /path/to/curvine-doc-<slug>
ln -s /path/to/curvine-doc/node_modules node_modules
```

Always `cd` into the new worktree before any other command; every later step uses paths relative to it. Check `git status` and `git worktree list` first. Never edit a dirty clone or an existing blog worktree.

### 3. Create the posts

```bash
mkdir -p blog/<date>-<slug>
mkdir -p i18n/zh-cn/docusaurus-plugin-content-blog/<date>-<slug>
```

- Strip the source H1 and subtitle; the frontmatter title replaces them.
- Add the cover image and opening quote, then `<!-- truncate -->`.
- Copy the EN figures and cover into the EN directory, the ZH figures and cover into the ZH directory.
- Name figures consistently (`cover.png`, `01-architecture.png`, ...).

### 4. Validate

```bash
npm run build
find build -path "*<slug>*" -name index.html
```

Docusaurus routes are date-based: `/blog/YYYY/MM/DD/<slug>/` and `/zh-cn/blog/YYYY/MM/DD/<slug>/`.
- Confirm both routes exist and the build reports no MDX or broken-link errors.
- Check that every referenced image exists and uses a relative path.
- For layout-sensitive changes, run `npm run serve` and open both routes.

### 5. Publish

- Stage only the two post directories (and `blog/tags.yml` if a tag was added).
- Push the branch and open a PR with [cv-create-pr](../cv-create-pr/SKILL.md).
- For `curvine-doc`, the PR title is `docs(blog): <description>`; no product issue number is required.
- Open as **draft** unless the user asks for ready.

## Frontmatter and metadata

- `authors`: use entries from `blog/authors.yml` (`barry`, `david`, `john`, `xuen`). The maintainer account `szbr9486` maps to `david`.
- `tags`: must exist in `blog/tags.yml`; add a new tag there in the same PR if needed.
- `image`: cover path relative to the post directory.
- Date: use today's date in the folder name; avoid future dates.

## Pitfalls

| Pitfall | Avoid |
| ------- | ----- |
| i18n frontmatter | Current posts include frontmatter in both locales; check the latest example instead of assuming it is inherited |
| Body H1 | Duplicates the Docusaurus title; start at `##` |
| Truncate placement | Keep `<!-- truncate -->` on its own line, outside the blockquote |
| MDX parsing | Keep bare `<...>` (for example `cv://<authority>`) inside backticks |
| Version drift | Cover, figures, and prose must state the same product version |
| Raster translation | Rebuild diagrams; do not paint translated text over an existing image |
| Cover collision | Keep headline text clear of the database / server artwork |
| Build artifacts | Never commit `build/`, `.docusaurus/`, or `node_modules/` |
| Dirty clone | Always work in a fresh worktree from `origin/main` |

## Checklist

- [ ] EN and ZH post directory names and dates match
- [ ] Frontmatter present in both locales; `image: ./cover.png`
- [ ] No body H1; `<!-- truncate -->` present
- [ ] Every image localized and present in the correct directory
- [ ] `npm run build` passes; both routes exist
- [ ] Product versions consistent across prose, cover, and figures
- [ ] `git status` shows only the intended post files
- [ ] PR title/body follow [cv-create-pr](../cv-create-pr/SKILL.md)

## Related

- [cv-create-pr](../cv-create-pr/SKILL.md) — PR title, body, and draft defaults
- [cv-add-skills](../cv-add-skills/SKILL.md) — skill layout and registry rules
- [cv-test-report](../cv-test-report/SKILL.md) — another external publishing workflow
