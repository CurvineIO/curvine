---
name: cv-create-issue
description: Create a GitHub issue for Curvine using repository templates, with extended Goal/Not Goal/Test Plan sections for features. Use when user asks to create an issue, report a serious bug found during testing, or when agent should suggest filing an issue for the current problem.
---

# cv-create-issue

Create structured GitHub issues that follow `.github/ISSUE_TEMPLATE/` and Curvine conventions.

## When to Use

- User explicitly asks to create / file / open an issue
- A **serious bug** is found during testing (data loss, crash, security, CI breakage)
- Agent detects an unresolved problem worth tracking — **ask user** whether to file an issue

## Step 1: Classify

| Type | Template | Label | Title prefix |
| ---- | -------- | ----- | ------------ |
| Bug | `bug_report.md` | `bug` | `[BUG]:` |
| Feature | `feature_request.md` | `enhancement` | `[FEATURE]` |

Search for duplicates first:

```bash
gh issue list --search "<keywords>" --state open --limit 10
```

## Step 2: Draft Body

### Bug (follow template)

Required sections from `.github/ISSUE_TEMPLATE/bug_report.md`:

- **Describe the bug**
- **To Reproduce** (numbered steps)
- **Expected behavior**
- **OS Version** (OS, Curvine version/branch, relevant config)
- **Additional context** (logs, screenshots)

### Feature (template + extensions)

Base sections from `.github/ISSUE_TEMPLATE/feature_request.md`:

- **Is your feature request related to a problem?**
- **Describe the solution you'd like**
- **Describe alternatives you've considered**
- **Additional context**

**Required extra sections for Feature issues:**

```markdown
# Goal

Design scope: list sub-requirements and the problem each solves.

| Sub-requirement | Problem solved |
| --------------- | -------------- |
| ... | ... |

# Not Goal

Modules, features, or boundaries this issue must **not** touch.

- ...

# Test Plan

| Test case | Test objective | Modules involved |
| --------- | -------------- | ---------------- |
| ... | ... | ... |
```

## Step 3: Confirm with User

Show title + full body preview. Wait for approval before `gh issue create`.

## Step 4: Create

```bash
# Bug
gh issue create \
  --title "[BUG]: <summary>" \
  --label bug \
  --body "$(cat <<'EOF'
...
EOF
)"

# Feature
gh issue create \
  --title "[FEATURE] <summary>" \
  --label enhancement \
  --body "$(cat <<'EOF'
...
EOF
)"
```

## Step 5: Output

Return issue number, URL, and suggested next step:

- Bug fix → [cv-handle-issue](../cv-handle-issue/SKILL.md)
- Large feature → [cv-tasks-breakdown](../cv-tasks-breakdown/SKILL.md)

## Rules

- Body in English unless user requests otherwise
- One issue per topic
- For sub-tasks of a parent issue, prefix title with `[SUB]` and link `Parent: #<n>` in body
- End with the Wrap-up Summary section and its Chinese translation

## Wrap-up Summary (required)

Close every run with the block below, in the local chat. The issue itself stays English; this block is for the user, and it ends with its Chinese translation.

| Part | Content |
| ---- | ------- |
| Issue / PR | `#<n>` and URL, the title, the labels, whether it closes, extends, or duplicates an existing issue, and the parent / sub-issue links |
| Comments | `none` when the run only created the issue, otherwise id plus `path:line`, severity, and `posted` / `edited` / `skipped` |
| Problem solved | what the issue reports or requests, and who is affected |
| Approach | the sections written (Goal / Not Goal / Test Plan for features), the evidence collected, and the reproduction steps |
| Reason for the shape | why a bug report or a feature request, why those acceptance criteria, and which scope was deliberately excluded |
| Open items | sub-issues still to file, decisions left to the user, and the next skill to run (fix, breakdown, or none) |

Then append the same summary in Chinese under a `### Summary (Chinese)` heading: translate the conclusions and reasons, keep issue numbers, paths, commands and quoted output verbatim, and do not add claims that are absent above.
