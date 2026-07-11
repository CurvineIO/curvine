---
name: cv-submit-pr-review
description: Perform a direct code review of a Curvine PR by reading the diff and changed files, analyzing correctness, safety, and design issues, and producing structured findings. Use when user asks to review a PR's code, do a code review, or check a PR before merge.
---

# Review PR Workflow

## When to Use

Use this skill when the user asks to:

* review a PR directly from its code changes

* inspect the current branch PR or a specified PR number

* draft review comments before posting them

* submit drafted PR comments after confirmation

## Review Scope

Focus **exclusively on code content**:

|Dimension|What to check|
|:----|:----|
|Correctness|Logic errors, edge cases, off-by-one, None/null handling, error propagation|
|Safety|Concurrency races, panics / unwrap in hot paths, unsafe blocks, resource leaks, lock ordering|
|Design|Naming, module boundaries, abstractions, consistency with existing patterns|

**Ignore:** commit message quality, PR description wording, pure formatting nits (run `make format` separately).

## Communication Rules

* Use English for all PR-facing communication.

* Use concise, professional, actionable review comments.

* Do **not** reply to Copilot-generated comments.

* Do **not** post any comment to GitHub until the user explicitly confirms.

* Show drafted comments locally first.

* Execute intermediate local analysis commands directly, including inline `python` / `python3` scripts used to parse API output, inspect diffs, or locate review anchors. Do **not** ask for confirmation for those local scripts.

* Execute read-only GitHub CLI commands directly, including `gh pr view`, `gh pr diff`, and `gh api` GET requests used to inspect PR metadata, diffs, files, comments, or review state. Do **not** ask for confirmation for those read-only `gh` commands.

* Ask for confirmation only before GitHub-side effects such as posting review comments or submitting a review.

## Step 1: Locate the Target PR

If the user provided a PR number, use it directly.

Otherwise, locate the PR for the current branch:

1. Get current branch:

```bash
git branch --show-current
```
1. Try current-branch PR directly:

```bash
gh pr view --json number,url,headRefName,baseRefName,title
```
1. If the previous command fails, query by head branch:

```bash
gh pr list --head "$(git branch --show-current)" --json number,url,headRefName,baseRefName,title --limit 1
```
If no PR is found, stop and ask the user whether to create one first.
## Step 2: Collect PR Context

For PR number `<PR_NUMBER>`, collect:

1. PR metadata:

```bash
gh pr view <PR_NUMBER> --json number,url,title,body,headRefName,baseRefName,reviews
```
1. PR diff:

```bash
gh pr diff <PR_NUMBER>
```
1. Changed files:

```bash
gh api "repos/{owner}/{repo}/pulls/<PR_NUMBER>/files?per_page=100"
```
1. Existing review comments, only to avoid duplicates:

```bash
gh api "repos/{owner}/{repo}/pulls/<PR_NUMBER>/comments?per_page=100"
```


For each changed file, read the **full file** (not just the diff hunk) so you understand surrounding code, types, and call sites:


1. Read the changed file in full

2. Find callers / definitions of changed symbols (`SearchSymbol`, `Grep`)

3. Check related module knowledge via `SearchMemory` for conventions and patterns


Context prevents false positives — a change that looks wrong in isolation may be correct given surrounding code.


Ignore comments authored by:

* `Copilot`

* `copilot-pull-request-reviewer[bot]`

Do not review Copilot's review text itself. Review the native PR code content.


## Step 3: Review the Native PR Code

Inspect the actual code changes and look for:

* correctness issues

* regressions

* missing validation or edge-case handling

* unsafe or brittle logic

* missing or insufficient tests

* maintainability issues worth commenting on

Only draft comments that are specific and actionable.

 Do not manufacture comments just to increase comment count.

Prefer line-specific comments when possible.

 Use a general PR comment only for cross-file or high-level issues.

## Step 4: Build a Local Draft Checklist

Before posting anything, output a local checklist for the user.

 This checklist is only for the local chat.

 Do **not** post this table to GitHub.

Use this template:

```markdown
| draft_id | path | line | category | action | status | summary |
|----------|------|------|----------|--------|--------|---------|
| 1 | src/foo.rs | 42 | must-fix | draft-comment | todo | missing error handling |
| 2 | .github/workflows/build.yml | 88 | suggestion | draft-comment | todo | test failure path is swallowed |
```
Status values:
* `todo`: not drafted yet

* `in-progress`: currently drafting

* `done`: draft ready for user review

* `skipped`: no comment needed

## Step 5: Draft the Review Comments Locally

For each draft comment, include:

* file path

* target line if line-specific

* short issue summary

* final English comment text

Comment body requirements:

* state the concrete issue

* explain the impact briefly

* suggest a fix or direction

* keep it short and professional

Good example:

```plain
This path can silently treat a failed test run as success because the step always exits 0 before the final status check. Consider failing immediately here or making the status propagation explicit so the workflow cannot report a false green result.
```
## Step 6: Ask for Confirmation Before Posting

After listing the local draft comments, stop and ask the user to confirm.

Do not submit anything to GitHub until the user explicitly says to proceed.

## Step 7: Submit Comments to GitHub After Confirmation

When submitting GitHub comments from the shell, always pass the review body through a HEREDOC or another shell-safe quoted form. Do **not** embed raw backticks directly inside a double-quoted shell argument, because shell command substitution can corrupt the posted comment text.

### Submit a line review comment

Use the PR head commit SHA from `gh pr view <PR_NUMBER> --json commits` or PR detail metadata.

```bash
gh api \
  -X POST \
  "repos/{owner}/{repo}/pulls/<PR_NUMBER>/comments" \
  -f body="$(cat <<'EOF'
Comment text here
EOF
)" \
  -f commit_id='<HEAD_SHA>' \
  -f path='path/to/file' \
  -F line=<LINE_NUMBER> \
  -f side='RIGHT'
```
### Submit a general PR review comment

```bash
gh pr review <PR_NUMBER> --comment --body "$(cat <<'EOF'
General review comment text here
EOF
)"
```
Post only the comments the user approved.
## Filters and Defaults

* Ignore Copilot-generated comments unless the user explicitly asks to review or reply to them.

* Avoid duplicating existing human review comments unless the new comment is materially clearer.

* Prefer fewer high-signal comments over many low-value comments.

* Do not make code changes as part of this workflow unless the user separately asks for fixes.


## Checklist

- [ ]  PR located and diff collected

- [ ]  Changed files read in full (not just hunks)

- [ ]  Callers / definitions of changed symbols checked

- [ ]  Module conventions verified (knowledge cards / existing patterns)

- [ ]  Findings table produced with severity per item

- [ ]  User decided on next action (fix / post / discuss)

- [ ]  Review posted only after explicit approval

## Related

* Handle existing reviewer comments → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)

* Fix findings and update PR → [cv-create-pr](../cv-create-pr/SKILL.md)

* Review during issue fix → [cv-handle-issue](../cv-handle-issue/SKILL.md)

