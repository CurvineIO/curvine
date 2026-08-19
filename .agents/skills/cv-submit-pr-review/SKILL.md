---
name: cv-submit-pr-review
description: Perform a direct code review of a Curvine PR against live base/head by mapping dirty crates, reading surrounding code, and checking correctness, safety, lifecycle, interface contracts, test strength, and performance on data I/O, RPC, and metadata paths. Use when user asks to review a PR's code, do a code review, or check a PR before merge.
---

# cv-submit-pr-review

This skill is **guidance, not a complete checklist**. Verify the PR's live base and exact head, map dirty crates from the changed-file list, then read the diff and enough surrounding code to understand the design. The file list identifies paths and layers; it does not replace semantic review. Re-establish the base and re-read the diff after a retarget or merge.

Prioritize correctness, lifecycle, safety, and broken required behavior over style. A short review with one substantiated blocker is better than a list of nits.

## When to Use

Use this skill when the user asks to:

* review a PR directly from its code changes
* inspect the current branch PR or a specified PR number
* draft review comments before posting them
* submit drafted PR comments after confirmation

## Sources of truth

Review against these, not against generic taste:

* [CONTRIBUTING.md](../../../CONTRIBUTING.md) — Rust style, tests, PR size
* [.agents/rules/workflow.md](../../rules/workflow.md) — issue / PR / review process
* Public crate contracts: `curvine-fs-api`, `curvine-ufs-api`, `curvine-storage-api`, `curvine-proto`
* Existing patterns in the dirty crates (callers, lock order, error types)
* CSI PRs: [cv-csi-test](../cv-csi-test/SKILL.md)

Treat disagreement with an existing design as a **design discussion**, not an automatic veto.

## Review Scope

Focus **exclusively on code content**:

| Dimension | What to check |
| :---- | :---- |
| Correctness | Logic errors, edge cases, off-by-one, None/null handling, error propagation |
| Safety | Concurrency races, panics / unwrap in hot paths, unsafe blocks, resource leaks, lock ordering |
| Lifecycle | Master/worker/fuse/CSI startup, heartbeat, failover, unmount, detach cleanup, cancellation during awaits |
| Contracts | Both sides of changed proto/RPC, fs-api, ufs-api, storage-api, JNI/Python SDK; errors, ownership, compatibility |
| Design | Naming, module boundaries, abstractions, consistency with existing patterns; challenge speculative generality |
| Tests | Assertions fail on the intended regression; real entry path (client/fuse/CSI/CLI) where the bug lives |
| Performance | Potential performance impact on critical paths: **data read/write** (block I/O patterns, extra buffer copies, sync/fsync frequency, read amplification), **message communication** (RPC payload size, serialization/deserialization overhead, extra network round trips, connection churn), and **metadata operations** (inode lookup cost, lock granularity and hold time, journal/rocksdb write amplification). Trace hot-path call chains to judge whether the change adds work per request/block/message. Any change likely to cause a significant performance regression **must** generate a comment with concrete improvement suggestions (e.g., batching, caching, avoiding copies, narrowing locks, async-ifying blocking calls). |

**Ignore:** commit message quality, PR description wording, pure formatting nits (run `make format` separately), and issues already enforced by a green gate (`cargo fmt`, clippy, PR title check, compile CI).

Detailed Curvine-specific probes: [references/manual-checks.md](references/manual-checks.md).

## Blocking requirements

1. **Critical-path performance.** A likely regression on data I/O, RPC, or metadata **must** get a comment with a concrete improvement direction.
2. **Docs match the code.** Public API, config defaults, proto/wire fields, errors, and user-visible behavior update the owning README/docs/comments in the same diff. Flag implementation narration and duplicated rationale.
3. **Required evidence exists.** The author ran relevant local checks for the dirty crates (`make format`, targeted `cargo test`), and CI covers compile/test. Review the semantic gaps neither can detect.
4. **Tests would catch the bug.** New behavior or a regression fix has an assertion that fails on the intended defect. Coverage is necessary but not evidence that the scenario is correct.
5. **Safety on hot paths.** `unwrap` / panic, `unsafe`, lock-order inversion, leaked FDs/block handles, and incomplete detach cleanup are blockers.
6. **Wire compatibility.** Breaking proto/RPC/SDK changes are called out with a migration path or an explicit compatibility decision.

## Communication Rules

* Use English for all PR-facing communication.
* Use concise, professional, actionable review comments.
* State the **defect, location, impact, and evidence**. Place a localized defect inline on the tightest relevant diff range; use a PR-level comment for cross-cutting architecture, scope, or review-wide synthesis.
* Separate **blockers** from **suggestions**. Omit issues already enforced by a green gate.
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

2. Try current-branch PR directly:

```bash
gh pr view --json number,url,headRefName,baseRefName,title
```

3. If the previous command fails, query by head branch:

```bash
gh pr list --head "$(git branch --show-current)" --json number,url,headRefName,baseRefName,title --limit 1
```

If no PR is found, stop and ask the user whether to create one first.

## Step 2: Collect PR Context

For PR number `<PR_NUMBER>`, collect the **live** base and head. Re-run this step after a retarget or merge; do not review a stale diff.

1. PR metadata (include SHAs):

```bash
gh pr view <PR_NUMBER> --json number,url,title,body,headRefName,baseRefName,headRefOid,baseRefOid,reviews
```

2. PR diff against that head:

```bash
gh pr diff <PR_NUMBER>
```

3. Changed files:

```bash
gh api "repos/{owner}/{repo}/pulls/<PR_NUMBER>/files?per_page=100"
```

4. Existing review comments, only to avoid duplicates:

```bash
gh api "repos/{owner}/{repo}/pulls/<PR_NUMBER>/comments?per_page=100"
```

5. Map **dirty crates / layers** from the file list before reading code. Group paths into:

| Layer | Typical paths |
| ----- | ------------- |
| Wire / public API | `crates/common/curvine-proto`, `*-api`, JNI/Python SDK |
| Server | `curvine-master`, `curvine-worker`, `curvine-raft`, `curvine-rocksdb` |
| Client / mount | `curvine-client`, `curvine-fuse`, CSI, CLI |
| Tests / docs / CI | `curvine-tests`, `.github/`, `docs/`, `.agents/` |

The map orients the review; it does not replace reading surrounding code.

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

Inspect the actual code changes using [references/manual-checks.md](references/manual-checks.md). At minimum look for:

* correctness issues and regressions
* lifecycle / concurrency defects (races, cancellation, incomplete cleanup)
* interface-contract mismatches (proto, fs-api, SDK both sides)
* performance regressions in critical paths — if the impact is significant, drafting a comment with improvement suggestions is mandatory
* missing validation or edge-case handling
* unsafe or brittle logic (`unwrap` on hot paths, `unsafe`, lock order)
* missing or weak tests (assertion would not fail on the intended regression; tests skip the real entry path)
* public-API expansion with a single internal caller
* speculative generality or unrelated scope
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
| draft_id | path | line | severity | action | status | summary |
|----------|------|------|----------|--------|--------|---------|
| 1 | src/foo.rs | 42 | blocker | draft-comment | todo | missing error handling on write path |
| 2 | .github/workflows/build.yml | 88 | suggestion | draft-comment | todo | test failure path is swallowed |
```

Severity values:

* `blocker` — correctness, safety, lifecycle, or broken required behavior; would withhold merge
* `must-fix` — clear defect that should land before merge
* `suggestion` — optional design / maintainability improvement

Status values:

* `todo`: not drafted yet
* `in-progress`: currently drafting
* `done`: draft ready for user review
* `skipped`: no comment needed (including green-gate / duplicate)

## Step 5: Draft the Review Comments Locally

For each draft comment, include:

* file path
* target line if line-specific
* severity
* short issue summary
* final English comment text

Comment body requirements:

* state the concrete defect
* give location (path / line / call chain)
* explain impact briefly
* cite evidence (surrounding code, missing test, both sides of an interface)
* suggest a fix or direction
* keep it short and professional

Good example:

```plain
This write path drops the RocksDB error and returns Ok, so a failed metadata persist
can be acknowledged to the client. The call chain is create -> inode::commit ->
rocks_store.put; a later restart will lose the inode. Propagate the error (or fail
the RPC) here so the client retries instead of treating the create as durable.
```

## Step 6: Ask for Confirmation Before Posting

After listing the local draft comments, stop and ask the user to confirm.

Do not submit anything to GitHub until the user explicitly says to proceed.

## Step 7: Submit Comments to GitHub After Confirmation

When submitting GitHub comments from the shell, always pass the review body through a HEREDOC or another shell-safe quoted form. Do **not** embed raw backticks directly inside a double-quoted shell argument, because shell command substitution can corrupt the posted comment text.

Use the **current** PR head SHA from Step 2 (`headRefOid`). If the PR moved since the draft, re-collect context and re-anchor comments.

### Submit a line review comment

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
* Omit issues already enforced by a green gate.
* Do not make code changes as part of this workflow unless the user separately asks for fixes.

## Checklist

- [ ] Live base/head verified (re-checked after retarget or merge)
- [ ] Dirty crates / layers mapped from the file list
- [ ] Changed files read in full (not just hunks)
- [ ] Callers / definitions of changed symbols checked
- [ ] Module conventions verified (sources of truth / existing patterns)
- [ ] Manual checks applied for contracts, lifecycle, consumer fit, test strength
- [ ] Performance impact assessed for data read/write, RPC, and metadata paths; significant regressions have mandatory comments with suggestions
- [ ] Findings table produced with severity (`blocker` / `must-fix` / `suggestion`)
- [ ] User decided on next action (fix / post / discuss)
- [ ] Review posted only after explicit approval

## Related

* Handle existing reviewer comments → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
* Fix findings and update PR → [cv-create-pr](../cv-create-pr/SKILL.md)
* Review during issue fix → [cv-handle-issue](../cv-handle-issue/SKILL.md)
* CSI-specific scenarios → [cv-csi-test](../cv-csi-test/SKILL.md)
