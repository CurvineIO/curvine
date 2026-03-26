# P2P PR Split Execution Plan

> **For Claude/Codex:** This document is a strict execution guide for rebuilding the current `feature/p2p` branch into a sequence of reviewable PRs from `github/main`. Do not improvise the PR boundaries. Do not optimize the order. Do not merge behavior-changing P2P read-path code before the required foundation PRs are in place.

**Goal:** Split the current P2P branch into a sequence of independently mergeable PRs that preserve mainline behavior at every step and keep P2P fully gated until the first end-to-end slice is intentionally enabled.

**Architecture:** The split must be done as vertical capability slices, not by commit order and not by file ownership. The first slices add protocol/config/foundation only. The first PR that changes read behavior must still be guarded by `client.p2p.enable = false` by default and must preserve worker fallback.

**Tech Stack:** Rust, libp2p, protobuf/prost, Curvine client/master/server, shell-based verification scripts, minicluster integration tests.

---

## 1. Why This Plan Exists

The current `feature/p2p` branch is too large to review safely as one PR. The branch contains protocol changes, config changes, client read-path changes, runtime policy, data-plane redirect, cache backends, performance work, test harnesses, and rollout material all mixed together.

A naive split will fail for one of three reasons:

1. It will split by commit order and produce PRs that compile but have incoherent behavior.
2. It will split by file and produce PRs that do not build because `service.rs`, `block_reader.rs`, `fs_context.rs`, `proto`, and config changes are tightly coupled.
3. It will expose partial P2P behavior on mainline before fallback and policy mechanisms are present.

This plan avoids those failures by enforcing four rules:

1. Use `github/main` as the only valid split base.
2. Keep P2P default-off until the dedicated end-to-end slice lands.
3. Preserve worker fallback in every behavior-changing PR.
4. Keep verification proportional: no meaningless test spam, but every PR must prove its contract.

---

## 2. Hard Rules

These rules are mandatory.

1. **Base branch rule**
   - Every PR branch must be created from `github/main`, not local `main`.
   - Reason: local `main` is behind `github/main` and will contaminate the split.

2. **Default-off rule**
   - `client.p2p.enable` must remain `false` by default until the full feature is intentionally enabled by configuration.
   - No PR may change the default rollout posture.

3. **Fallback rule**
   - Every PR that touches read behavior must preserve direct worker read as the safe fallback path.
   - No PR may create a situation where `enable=true` can bypass worker fallback without explicit design intent.

4. **No fat cherry-pick rule**
   - Do not cherry-pick the original large commits as-is.
   - The original commits are too wide and will recreate the same review problem.
   - Rebuild each PR by selectively restoring files or hunks from `feature/p2p`.

5. **No standalone cleanup PR rule**
   - Do not create separate PRs for style-only changes, test trimming, or scope cleanup.
   - Absorb necessary cleanup into the owning PR.

6. **Verification rule**
   - Each PR must run only the smallest meaningful verification set for that slice.
   - Do not run noisy or irrelevant tests just to inflate confidence.

7. **Behavior continuity rule**
   - The first six PRs must not change mainline client read behavior.
   - The first behavior-changing PR must still be safe to merge because the feature remains opt-in.

---

## 3. Execution Model

### 3.0 Preflight

Before creating any PR worktree, verify the split sources exist locally and are current:

```bash
git fetch origin
git fetch github
git branch --list feature/p2p
git rev-parse github/main
```

If `feature/p2p` does not exist locally, create a local ref first:

```bash
git branch feature/p2p origin/feature/p2p
```

Do not run any extraction step until both `github/main` and `feature/p2p` resolve locally.

### 3.1 Branch Naming

Create exactly these PR branches:

1. `p2p-01-file-version-foundation`
2. `p2p-02-master-policy-protocol`
3. `p2p-03-p2p-config-surface`
4. `p2p-04-client-p2p-module-shell`
5. `p2p-05-fetched-cache-backend`
6. `p2p-06-service-lifecycle-shell`
7. `p2p-07-read-acceleration-baseline`
8. `p2p-08-runtime-policy-sync`
9. `p2p-09-data-plane-redirect`
10. `p2p-10-correctness-hardening`
11. `p2p-11-reader-and-listener-lifecycle-hardening`
12. `p2p-12-unified-boundary-hardening`
13. `p2p-13-data-plane-fast-path`
14. `p2p-14-adaptive-read-performance`
15. `p2p-15-runtime-service-hardening`
16. `p2p-16-metrics-label-telemetry`
17. `p2p-17-validation-harness-and-samples`

### 3.2 Recommended Workspace Setup

Use separate worktrees. Example:

```bash
git fetch github
git worktree add ../curvine-p2p-01 github/main
cd ../curvine-p2p-01
git switch -c p2p-01-file-version-foundation
```

Repeat per PR. Do not stack 14 branches in one dirty worktree.

### 3.3 Extraction Strategy

Preferred extraction order for each PR:

```bash
# from the PR worktree rooted at github/main
git restore --source feature/p2p -- path/to/file
# or, when only part of a file belongs in the PR
git restore -p --source feature/p2p -- path/to/file
```

Then manually prune any hunk that belongs to a later PR.

If the local source branch name is different, substitute it explicitly:

```bash
git restore --source origin/feature/p2p -- path/to/file
```

Do not use `git cherry-pick` on the original fat commits unless the commit maps 1:1 to the target PR, which is rare here.

---

## 4. Global Verification Matrix

Use the smallest meaningful gate for each PR.

### 4.1 Foundation PRs (`p2p-01` to `p2p-06`)

Run only:

```bash
cargo fmt --all
cargo test -p curvine-common --lib
cargo test -p curvine-client --lib
```

Add targeted tests only if the PR introduces new protocol/config behavior.

### 4.2 Behavior PRs (`p2p-07` to `p2p-13`)

Minimum gate:

```bash
cargo fmt --all
cargo test -p curvine-client --test p2p_e2e_test -- --nocapture
cargo test -p curvine-tests --test p2p_read_acceleration_test -- --nocapture
bash scripts/tests/p2p-production-gate.sh
```

If the PR does not yet include the relevant script or test, run the narrowest equivalent meaningful subset.

### 4.3 Tail PRs (`p2p-14` to `p2p-17`)

Use the smallest meaningful subset:

```bash
cargo fmt --all
cargo test -p curvine-client --lib --no-run
```

Then add only the targeted checks required by the tail slice:
- `p2p-14`: adaptive-path targeted tests
- `p2p-15`: runtime-service targeted tests + focused `p2p_e2e_test`
- `p2p-16`: metrics/label telemetry targeted tests
- `p2p-17`: proof-driver build, harness script syntax/forwarder checks, and `p2p_read_acceleration_test --no-run` or stronger

---

## 5. PR Sequence

## PR 1: `p2p-01-file-version-foundation`

**Purpose:** Establish shared file-version metadata required by the P2P read path without changing read behavior.

**Include:**
- `curvine-common/proto/common.proto`
- `curvine-common/src/state/file_status.rs`
- `curvine-common/src/utils/proto_utils.rs` only the `FileStatus`/`version_epoch` portion

**Exclude:**
- Any P2P-specific policy fields
- Any client read-path changes

**Behavior expectation:** No user-visible change.

**Required verification:**
```bash
cargo test -p curvine-common proto_roundtrip_keeps -- --nocapture
```

**Merge condition:** Safe on its own. No client/server behavior shift.

---

## PR 2: `p2p-02-master-policy-protocol`

**Purpose:** Add wire-level master policy structures and signing utilities.

**Include:**
- `curvine-common/proto/master.proto`
- `curvine-common/src/state/master_info.rs`
- `curvine-common/src/utils/common_utils.rs` only policy-signing helpers
- minimal `proto_utils.rs` support for master policy fields

**Exclude:**
- client sync logic
- master runtime application logic

**Behavior expectation:** Existing clients remain unaffected.

**Required verification:** targeted proto/signature tests only.

**Merge condition:** Protocol definitions compile and decode legacy payloads.

---

## PR 3: `p2p-03-p2p-config-surface`

**Purpose:** Add P2P config schema and validation while keeping runtime disabled by default.

**Include:**
- `curvine-common/src/conf/client_conf.rs`
- `curvine-common/src/conf/master_conf.rs`
- `curvine-common/src/conf/mod.rs`
- any minimal cargo/proto glue required for config compilation

**Exclude:**
- service wiring
- read-path interception

**Behavior expectation:** P2P remains unused unless explicitly enabled.

**Required verification:** config parsing and default-value tests.

**Merge condition:** Minimal config loads with `enable=false`; no runtime path change.

---

## PR 4: `p2p-04-client-p2p-module-shell`

**Purpose:** Introduce the client P2P module namespace and base types without read-path integration.

**Include:**
- `curvine-client/src/lib.rs`
- `curvine-client/src/p2p/mod.rs`
- `curvine-client/src/p2p/discovery.rs`
- `curvine-client/src/p2p/transfer.rs`
- `curvine-client/src/file/mod.rs` only exports needed by the shell

**Exclude:**
- `service.rs` behavior
- cache manager
- read-path hooks

**Behavior expectation:** Pure scaffolding.

**Required verification:** `cargo test -p curvine-client --lib`

**Merge condition:** Module compiles; runtime behavior unchanged.

---

## PR 5: `p2p-05-fetched-cache-backend`

**Purpose:** Add the fetched-cache backend as infrastructure before it is used by the main read path.

**Include:**
- `curvine-client/src/p2p/cache_manager.rs`
- `curvine-client/Cargo.toml` dependency additions required by this backend

**Exclude:**
- any `BlockReader` / `FsContext` integration
- data-plane logic

**Behavior expectation:** Backend exists but is dormant.

**Required verification:** backend unit tests only.

**Merge condition:** Standalone cache backend compiles and passes its local contract tests.

---

## PR 6: `p2p-06-service-lifecycle-shell`

**Purpose:** Introduce `P2pService` lifecycle wiring into the client, but do not intercept reads yet.

**Include:**
- early `service.rs` lifecycle skeleton only
- `fs_context.rs` service ownership/start/stop shell
- `curvine_filesystem.rs` creation/plumbing
- minimal metrics scaffolding needed to compile

**Exclude:**
- actual read acceleration
- policy sync behavior
- redirect/data-plane logic

**Behavior expectation:** With `enable=false`, no change. With `enable=true`, service may start but must not take over reads.

**Required verification:** targeted lifecycle tests only.

**Merge condition:** start/stop is clean and isolated from mainline reads.

---

## PR 7: `p2p-07-read-acceleration-baseline`

**Purpose:** First end-to-end P2P read path behind the feature flag, with worker fallback intact.

**Include:**
- baseline `BlockReader`/`FsReader` integration
- enough `service.rs` logic to serve chunk fetches
- minimal block/file-side plumbing required for a P2P read hit

**Primary files:**
- `curvine-client/src/block/block_reader.rs`
- `curvine-client/src/block/block_reader_remote.rs`
- `curvine-client/src/file/fs_reader_base.rs`
- `curvine-client/src/file/fs_reader_buffer.rs`
- `curvine-client/src/p2p/service.rs`

**Exclude:**
- runtime master policy sync
- data-plane redirect
- advanced perf optimizations

**Behavior expectation:**
- `enable=false`: identical to current mainline
- `enable=true`: P2P may serve reads, but worker fallback must remain correct

**Required verification:**
- minimal e2e read-acceleration proof
- no broad stress harness yet

**Merge condition:** This is the first PR that intentionally changes enabled behavior.

---

## PR 8: `p2p-08-runtime-policy-sync`

**Purpose:** Add master-driven runtime policy, signature verification, startup sync, periodic sync, and rollback to local policy.

**Include:**
- client sync logic in `fs_context.rs` / `service.rs`
- master handlers and filesystem support
- runtime allowlist application and version persistence

**Primary files:**
- `curvine-server/src/master/master_handler.rs`
- `curvine-server/src/master/fs/master_filesystem.rs`
- `curvine-client/src/file/fs_context.rs`
- `curvine-client/src/file/curvine_filesystem.rs`
- `curvine-client/src/p2p/service.rs`

**Exclude:**
- data-plane performance work

**Required verification:**
- startup sync applies immediately
- version `0` rollback restores local policy
- policy failure surfaces as failure, not silent success

---

## PR 9: `p2p-09-data-plane-redirect`

**Purpose:** Introduce redirect-to-data-plane architecture and ticketed follow-up fetches.

**Include:**
- control-plane redirect response
- data-plane listener/bind lifecycle
- request/response framing needed for redirected fetch
- block follow-up read path

**Primary files:**
- `curvine-client/src/p2p/service.rs`
- `curvine-client/src/p2p/transfer.rs` if framing types evolve

**Exclude:**
- correctness hardening that can be isolated later
- advanced perf tuning

**Required verification:**
- redirect only when first chunk is serviceable
- follow-up response never issues pointless ticket
- worker fallback remains available

**Execution note:** This branch is intermediate-only. It establishes redirect/ticket plumbing and some fetched-cache visibility needed to make the redirect path function, but it is not rollout-usable on its own. Deployment-safe redirect behavior does not exist until `p2p-11`, after `p2p-10` correctness fixes and `p2p-11` listener/reader lifecycle hardening.

---

## PR 10: `p2p-10-correctness-hardening`

**Purpose:** Fix correctness gaps discovered after redirect architecture exists.

**Include:**
- avoid redirecting empty peers
- serve published chunks before persist completes
- scope cached data-plane tickets correctly by request context
- refreshed provider advertisement via identify
- pending-fetched visibility and related correctness glue

**Primary file:**
- `curvine-client/src/p2p/service.rs`

**Required verification:**
- no stale ticket reuse across tenant/mtime
- no `response_not_found_or_empty` window after publish
- provider refresh works after republish

**Execution note:** This branch closes redirect correctness gaps, but it is still intermediate-only for rollout. Data-plane stop/restart correctness, transient `accept()` recovery, and reader/pool lifecycle safety land in `p2p-11`.

---

## PR 11: `p2p-11-reader-and-listener-lifecycle-hardening`

**Purpose:** Fix failure handling and lifecycle safety in reader paths and data-plane listener management.

**Include:**
- reader fallback correctness
- last-replica failure behavior
- remote reader cleanup and pool hygiene
- listener retry, shutdown, generation isolation
- `FsContext`/pool lifetime fixes directly needed by P2P lifecycle

**Primary files:**
- `curvine-client/src/block/block_reader.rs`
- `curvine-client/src/block/block_reader_remote.rs`
- `curvine-client/src/block/block_client_pool.rs`
- `curvine-client/src/file/fs_context.rs`
- `curvine-client/src/p2p/service.rs`

**Required verification:**
- broken remote readers do not leak back into pool
- stop/start listener is clean
- transient listener failure self-recovers

**Execution note:** This is the first redirect slice that is reasonable to describe as rollout-usable, and only when stacked on top of `p2p-10`.

---

## PR 12: `p2p-12-unified-boundary-hardening`

**Purpose:** Fix the one unified boundary issue that is both independently mergeable and relevant to mounted-path correctness: mount lookup must prefer the deepest matching mount.

**Include:**
- `mount_cache.rs`

**Exclude:**
- `cache_sync_writer.rs` changes that depend on later mount-write-type evolution
- `unified_filesystem.rs` route changes that depend on later mount model expansion
- unrelated unified refactors

**Required verification:**
- mounted path selection correctness

**Dependency note:** This PR is intentionally detached from the core P2P chain. It stacks on `p2p-11`, but `p2p-13` and later branches continue from `p2p-11`, not from `p2p-12`. Merge it independently when ready.

**Note:** During execution, broader unified changes were found to depend on non-P2P mount-model drift (`WriteType`, consistency semantics, and proto/display conversions). Those changes must not be force-fit into this PR.

---

## PR 13: `p2p-13-data-plane-fast-path`

**Purpose:** Land the 0ms-critical data-plane optimizations after the fetched-cache path is stable.

**Include:**
- avoid zeroing response buffers
- stream first data-plane chunk
- 0ms data-plane read-path shaping that does not change rollout posture

**Primary files:**
- `curvine-client/src/p2p/service.rs`
- `curvine-client/src/block/block_reader_remote.rs` if required by the first-chunk path

**Required verification:**
- production gate
- 0ms benchmark sanity
- redirected-read targeted tests

**Rule:** Do not mix adaptive bypass, metrics-label work, or test harness expansion into this PR.

---

## PR 14: `p2p-14-adaptive-read-performance`

**Purpose:** Land read-side optimization work that changes selection heuristics only after the 0ms data-plane path is already isolated.

**Include:**
- cached-hit read-flight bypass
- adaptive worker bypass
- adaptive-sample hygiene

**Primary files:**
- `curvine-client/src/block/block_reader.rs`
- `curvine-client/src/file/fs_context.rs`
- `curvine-client/src/p2p/service.rs` only if required for sample attribution

**Required verification:**
- production gate
- 0ms benchmark sanity
- targeted adaptive-behavior tests

**Rule:** Do not mix metrics-label work, validation harnesses, or rollout material into this PR.

---

## PR 15: `p2p-15-runtime-service-hardening`

**Purpose:** Finish the remaining runtime hardening in `service.rs` after adaptive read behavior has landed.

**Include:**
- data-plane tenant whitelist runtime update handling
- data-plane listener restart/stop generation safety
- cached ticket request-context scoping
- a compact `p2p_e2e_test` suite that locks the highest-value runtime regressions in service lifecycle, negative provider cache, and provider rediscovery

**Exclude:**
- metrics-label telemetry
- proof/stress harnesses
- config samples and rollout material

**Required verification:**
- targeted runtime-service tests
- compact core `p2p_e2e_test` suite

**Rule:** This PR may still change product code, but it must stay inside runtime service hardening only.

---

## PR 16: `p2p-16-metrics-label-telemetry`

**Purpose:** Add observability for read-source attribution, fallback reasons, and labeled P2P metrics without changing core protocol behavior.

**Include:**
- `curvine-client/src/client_metrics.rs`
- read-source / fallback telemetry plumbing in `block_reader.rs`
- tenant/job label extraction in `fs_reader_base.rs`
- metrics label policy setup and P2P snapshot syncing in `fs_context.rs`

**Exclude:**
- `curvine_filesystem.rs` API churn
- `fs_client.rs` cleanup or rename work
- product-path behavior changes unrelated to telemetry

**Required verification:**
- labeled read-source metrics tests
- labeled fallback metrics tests
- one cached-read path regression test
- `cargo test -p curvine-client --lib --no-run`

---

## PR 17: `p2p-17-validation-harness-and-samples`

**Purpose:** Bring in verification harnesses, sample configs, rollout material, and operational scripts after product logic is settled.

**Include:**
- `curvine-tests/tests/p2p_read_acceleration_test.rs`
- `curvine-cli/src/bin/p2p_proof_driver.rs`
- `scripts/tests/p2p-*.sh`
- `scripts/tests/master-forwarder.py`
- `etc/curvine-cluster-p2p-*.toml`

**Exclude:**
- unrelated historical script churn from other topics
- new product-path behavior

**Required verification:**
- `cargo build -p curvine-cli --bin p2p_proof_driver`
- `cargo test -p curvine-tests --test p2p_read_acceleration_test --no-run`
- `bash scripts/tests/test-master-forwarder.sh`
- shell syntax checks for the added scripts

**Rule:** This PR is the end of the split, not the beginning. Do not use it to smuggle more product changes.

---

## 6. Commits That Must Not Become Standalone PRs

Do **not** create separate PRs for these categories:

1. style-only cleanup
2. test trimming only
3. review-scope cleanup only
4. local experimental benchmark artifacts
5. docs-only drift caused by previous sessions

In the current branch, the following commits are examples of changes that should be absorbed, dropped, or rewritten into the owning PR rather than preserved as standalone PRs:

- `b3eb0d1`
- `e10c3ae`
- `4f950c7`
- `c51153f`
- `6da2486`

---

## 7. Extraction Checklist Per PR

For every PR branch, execute this checklist in order.

1. Create worktree from `github/main`.
2. Create the PR branch with the exact branch name from this plan.
3. Restore only the files/hunks that belong to that PR.
4. Delete any later-stage code that slipped in.
5. Run `cargo fmt --all`.
6. Run only the verification required by that PR.
7. Confirm `client.p2p.enable` default remains `false` unless the PR is only config/schema and still not behavior-changing.
8. Confirm worker fallback still exists for any read-path PR.
9. Confirm the diff contains no unrelated script churn.
10. Commit with a title matching the PR purpose.
11. Open PR against `github/main`.
12. After merge, rebase or recreate the next PR branch from fresh `github/main` plus merged PRs as needed.

---

## 8. Reviewer Guidance

Each PR description should explicitly answer these four questions:

1. What capability becomes possible after this PR?
2. What still remains disabled or incomplete after this PR?
3. Why is this PR safe to merge even if the rest of the split has not landed yet?
4. What exact tests prove that safety claim?

If the PR description cannot answer those four questions cleanly, the PR is too large or incorrectly scoped.

---

## 9. Stop Conditions

Stop and rescope if any of the following happens:

1. A PR requires more than one unrelated behavior change to build.
2. A PR cannot be explained without referencing more than two later PRs.
3. A PR forces `enable=true` behavior to be partially active before fallback/policy/correctness are ready.
4. A PR review discussion starts focusing on unrelated performance tuning instead of the PR's stated contract.

When a stop condition triggers, split the PR further or move code to a later PR. Do not push through with a messy boundary.

---

## 10. Final Recommendation

Use **17 PRs**, not 20.

That is the smallest number that keeps:
- protocol/config foundations understandable,
- the first enabled read-path PR reviewable,
- correctness hardening separate from optimization,
- validation/ops separate from product logic.

The extra tail split is intentional: runtime-service hardening, telemetry, and validation assets review more cleanly as separate concerns than they did in the original 14-PR draft. The detached `p2p-12` side PR is also intentional: it fixes a real mounted-path bug without forcing artificial dependency depth into the core P2P chain.
