---
name: cv-submit-pr-review
description: Perform a direct code review of a Curvine PR against live base/head by reconstructing intent, reading tests first, mapping dirty crates, and checking correctness, safety, lifecycle, contracts, and performance on data I/O, RPC, and metadata paths. Use when user asks to review a PR's code, do a code review, or check a PR before merge.
---

# cv-submit-pr-review

This skill is **guidance, not a complete checklist**. Verify the PR's live base and exact head, map dirty crates from the changed-file list, then read the diff and enough surrounding code to understand the design. The file list identifies paths and layers; it does not replace semantic review. Re-establish the base and re-read the diff after a retarget or merge.

Prioritize correctness, lifecycle, safety, and broken required behavior over style. A short review with one substantiated blocker is better than a list of nits.

**Approval standard:** Approve a change when it definitely improves overall code health, even if it is not how you would have written it. Do not block on taste. If it improves the codebase and follows project conventions, approve.

**Honesty:** Do not rubber-stamp. Do not post LGTM or Approve without citing what was inspected (dirty crates, probes, tests). Do not soften a production bug into a suggestion. Sycophancy is a review failure.

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

When resolving disagreement, apply this order: **technical facts and data** override opinions; **CONTRIBUTING.md / workflow.md** override style preference; **existing patterns in the dirty crates** override a new approach that is not justified; personal taste is last. Treat disagreement with an existing design as a **design discussion**, not an automatic veto.

## Review Scope

Focus **exclusively on code content**:

| Dimension | What to check |
| :---- | :---- |
| Correctness | Logic errors, edge cases, off-by-one, None/null handling, error propagation |
| Safety | Concurrency races, panics / unwrap in hot paths, unsafe blocks, resource leaks, lock ordering |
| Lifecycle | Master/worker/fuse/CSI startup, heartbeat, failover, unmount, detach cleanup, cancellation during awaits |
| Contracts | Both sides of changed proto/RPC, fs-api, ufs-api, storage-api, JNI/Python SDK; errors, ownership, **backward compatibility** (function/struct parameters appended at the end; new proto fields must be `optional`/`repeated` and appended with new field numbers; never reorder, reuse, or retag existing fields) |
| Design | Naming, module boundaries, abstractions, consistency with existing patterns; challenge speculative generality; a refactor must **reduce** the concepts a reader holds, not relocate the same branches |
| Tests | Assertions fail on the intended regression; real entry path (client/fuse/CSI/CLI) where the bug lives |
| Performance | Potential performance impact on critical paths: **data read/write** (block I/O patterns, extra buffer copies, sync/fsync frequency, read amplification), **message communication** (RPC payload size, serialization/deserialization overhead, extra network round trips, connection churn), and **metadata operations** (inode lookup cost, lock granularity and hold time, journal/rocksdb write amplification). Trace hot-path call chains to judge whether the change adds work per request/block/message. Any change likely to cause a significant performance regression **must** generate a comment with concrete improvement suggestions (e.g., batching, caching, avoiding copies, narrowing locks, async-ifying blocking calls). |
| Dependencies | `Cargo.toml` / `Cargo.lock` additions and version bumps; changelog, lockfile diff, and whether the existing stack already solves the need |

**Ignore:** commit message quality, PR description wording, pure formatting nits (run `make format` separately), and issues already enforced by a green gate (`cargo fmt`, clippy, PR title check, compile CI). Ignore PR wording as a review axis; still **read** the PR body to reconstruct intent and the author's verification story.

Detailed Curvine-specific probes: [references/manual-checks.md](references/manual-checks.md).

### Structural remedies

When flagging structure, propose a named move — not only "this is complex":

* Replace a chain of conditionals with a typed model or an explicit dispatcher
* Collapse duplicate branches into a single clearer flow
* Separate orchestration from business logic
* Move feature-specific logic out of a shared `*-api` / proto helper into the crate that owns the concept
* Reuse the canonical helper instead of a near-duplicate
* Delete a pass-through wrapper that adds indirection without clarifying the API
* Extract a helper, or split a file that the change pushes well past a healthy size

Prefer the remedy that removes moving pieces over one that spreads the same complexity around.

## Blocking requirements

1. **Critical-path performance.** A likely regression on data I/O, RPC, or metadata **must** get a comment with a concrete, **quantified** improvement direction (extra copy per block, extra RPC RTT, longer lock hold), not "this could be slow".
2. **Docs match the code.** Public API, config defaults, proto/wire fields, errors, and user-visible behavior update the owning README/docs/comments in the same diff. Flag implementation narration and duplicated rationale.
3. **Required evidence exists.** Inspect the author's **verification story** in the PR body (what was run, which crates, any manual check). A green compile/test gate is not that story. The author ran relevant local checks for the dirty crates (`make format`, targeted `cargo test`), and CI covers compile/test. Review the semantic gaps neither can detect.
4. **Tests would catch the bug.** New behavior or a regression fix has an assertion that fails on the intended defect. Coverage is necessary but not evidence that the scenario is correct.
5. **Safety on hot paths.** `unwrap` / panic, `unsafe`, lock-order inversion, leaked FDs/block handles, and incomplete detach cleanup are blockers.
6. **Wire compatibility.** Breaking proto/RPC/SDK changes are called out with a migration path or an explicit compatibility decision.
7. **Backward-compatible evolution of functions and proto messages.** New parameters on Rust functions and public struct fields **must** be appended at the end and be defaultable. New proto fields **must** use the next unused (highest) field number and be `optional` (proto2) or `optional`/without `required`-style semantics (proto3), never `required`. Reordering, renumbering, reusing, or retagging existing proto fields, and inserting new `required` fields into an existing message, are blockers. See [references/manual-checks.md](references/manual-checks.md#backward-compatible-evolution).
8. **No deferred cleanup.** "I'll clean it up later" is not acceptable unless this PR files or links a follow-up issue with an owner. Wire-compat leftovers and incomplete detach cleanup must be fixed here.

## Communication Rules

* Use English for all PR-facing communication.
* Use concise, professional, actionable review comments.
* State the **defect, location, impact, and evidence**. Place a localized defect inline on the tightest relevant diff range; use a PR-level comment for cross-cutting architecture, scope, or review-wide synthesis.
* **Quantify impact** when possible (per request / block / RPC / lock hold), rather than "this might be slow".
* Prefix posted GitHub comment bodies so the author can tell required from optional:
  * `Critical:` — `blocker` (would withhold merge)
  * `Required:` — `must-fix` (clear defect before merge)
  * `Optional:` / `Consider:` — `suggestion`
  * Do **not** use `Nit:`. Formatting and style nits are out of scope.
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
| Manifest / lockfile | `Cargo.toml`, `Cargo.lock`, crate `Cargo.toml` files |

The map orients the review; it does not replace reading surrounding code.

**Reconstruct intent before reading production code.** From the PR title, body, and linked issue, write one sentence: what behavior should change, and why. If that sentence cannot be formed, stop and ask — do not review hunks against an invented spec.

**Inspect the verification story** in the same pass: which tests the author ran, which dirty crates they cover, and any manual check. A green CI job is not a substitute. Missing crate-level evidence for a behavior change is `must-fix`.

Ignore comments authored by:

* `Copilot`
* `copilot-pull-request-reviewer[bot]`

Do not review Copilot's review text itself. Review the native PR code content.

## Step 3: Review Tests First

Before walking production implementation, read **new and changed tests**:

* Do tests exist for the claimed behavior?
* Would the assertion fail on the intended defect (negative control)?
* Do they hit the real entry path (client / FUSE / CSI / CLI / worker), not only an internal helper?
* Are edge cases and error paths covered, or only the happy path?

Tests reveal claimed intent. Production code is then reviewed against that claim, not the other way around.

## Step 4: Review the Native PR Code

For each changed file, read the **full file** (not just the diff hunk) so you understand surrounding code, types, and call sites:

1. Read the changed file in full
2. Find callers / definitions of changed symbols (`SearchSymbol`, `Grep`)
3. Check related module knowledge via `SearchMemory` for conventions and patterns
4. After mapping callers, list **now-unused** symbols, proto fields, metrics, or compat shims. Put the list in the local draft and **ask before recommending deletion**.

Context prevents false positives — a change that looks wrong in isolation may be correct given surrounding code.

Inspect the actual code changes using [references/manual-checks.md](references/manual-checks.md). At minimum look for:

* correctness issues and regressions
* lifecycle / concurrency defects (races, cancellation, incomplete cleanup)
* interface-contract mismatches (proto, fs-api, SDK both sides)
* backward-compatibility violations: new function/struct/proto parameters not appended at the end; new proto fields marked `required` instead of `optional`; reordered/renumbered/reused field numbers
* performance regressions in critical paths — if the impact is significant, drafting a **quantified** comment with improvement suggestions is mandatory
* missing validation or edge-case handling
* unsafe or brittle logic (`unwrap` on hot paths, `unsafe`, lock order)
* missing or weak tests (assertion would not fail on the intended regression; tests skip the real entry path)
* public-API expansion with a single internal caller
* speculative generality or unrelated scope
* a refactor that relocates complexity instead of reducing it (same number of branches/modes a reader must hold)
* unjustified `Cargo.toml` / `Cargo.lock` additions or bulk version bumps; review the lockfile diff and changelog
* maintainability issues worth commenting on — with a named structural remedy when the issue is structural

Only draft comments that are specific and actionable.

Do not manufacture comments just to increase comment count.

Prefer line-specific comments when possible.

Use a general PR comment only for cross-file or high-level issues.

## Step 5: Build a Local Draft Checklist

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

After the findings table, record a **local verdict** (do not post this block to GitHub by itself unless the user asks):

```markdown
**Intent:** <one sentence>
**Verification story:** <what the author ran, or "missing">
**Dead code:** <list, or "none">
**Verdict:** Approve | Request changes | Comment
**Reason:** <one line citing crates/probes inspected>
```

Do not choose Approve without evidence of review. A findings table with zero comments is valid when the change is sound; still state what was inspected.

## Step 6: Draft the Review Comments Locally

For each draft comment, include:

* file path
* target line if line-specific
* severity
* short issue summary
* final English comment text, starting with `Critical:`, `Required:`, or `Optional:` / `Consider:`

Comment body requirements:

* start with the severity prefix above
* state the concrete defect
* give location (path / line / call chain)
* explain impact briefly and **quantify** it when possible
* cite evidence (surrounding code, missing test, both sides of an interface)
* suggest a fix, a named structural remedy, or a concrete direction
* keep it short and professional

Good example:

```plain
Required: This write path drops the RocksDB error and returns Ok, so a failed
metadata persist can be acknowledged to the client. The call chain is create ->
inode::commit -> rocks_store.put; a later restart will lose the inode. Propagate
the error (or fail the RPC) here so the client retries instead of treating the
create as durable.
```

Quantified performance example:

```plain
Required: write_block now copies the packet into a new Vec on every call. On the
64KiB packet path that is an extra memcpy per RPC. Take the buffer by ownership
or write in place to avoid the copy.
```

## Step 7: Ask for Confirmation Before Posting

After listing the local draft comments and the verdict, stop and ask the user to confirm.

Do not submit anything to GitHub until the user explicitly says to proceed.

## Step 8: Submit Comments to GitHub After Confirmation

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
* Do not accept "I'll clean it up later" without a tracked follow-up issue and owner.

## Checklist

- [ ] Live base/head verified (re-checked after retarget or merge)
- [ ] Intent reconstructed in one sentence from the PR body / linked issue
- [ ] Author verification story inspected (not only CI green)
- [ ] Dirty crates / layers mapped from the file list
- [ ] Tests reviewed before production implementation
- [ ] Changed files read in full (not just hunks)
- [ ] Callers / definitions of changed symbols checked
- [ ] Dead code listed; deletion not assumed
- [ ] Module conventions verified (sources of truth / existing patterns)
- [ ] Manual checks applied for contracts, lifecycle, consumer fit, test strength
- [ ] Backward compatibility verified: new function/struct/proto parameters appended at the end; new proto fields are `optional`/`repeated` with new field numbers; no reordered/renumbered/reused fields
- [ ] Cargo.toml / Cargo.lock reviewed when present
- [ ] Performance impact assessed for data read/write, RPC, and metadata paths; significant regressions have mandatory quantified comments with suggestions
- [ ] Structural findings include a named remedy
- [ ] Findings table produced with severity (`blocker` / `must-fix` / `suggestion`)
- [ ] Local verdict recorded (Approve / Request changes / Comment) with evidence
- [ ] User decided on next action (fix / post / discuss)
- [ ] Review posted only after explicit approval; posted comments use Critical / Required / Optional prefixes

## Related

* Handle existing reviewer comments → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
* Fix findings and update PR → [cv-create-pr](../cv-create-pr/SKILL.md)
* Review during issue fix → [cv-handle-issue](../cv-handle-issue/SKILL.md)
* CSI-specific scenarios → [cv-csi-test](../cv-csi-test/SKILL.md)
