# Manual review checks (Curvine)

Use these probes after mapping dirty crates. They cover defects that a green CI gate and a hunk-only diff will miss. Skip checks that do not apply to the dirty layer.

## Intent and interface contracts

Trace **both sides** of every changed interface:

* proto RPC request/response and error codes (`curvine-proto` ↔ master/worker/client)
* `curvine-fs-api` / `curvine-ufs-api` / `curvine-storage-api` trait changes
* JNI / Python SDK exports vs Rust implementation

Confirm the implementation matches the PR intent, including errors, cancellation, ownership, and cleanup. A default that changed on one side only is a contract bug.

## Lifecycle and concurrency

For async setup, heartbeats, callbacks, FUSE requests, CSI MountPod, or teardown:

* races before publishing state (inode visible before data durable, replica advertised before written)
* cancellation / timeout during awaits
* lock ordering and hold time (especially metadata + block maps)
* complete detach cleanup (FUSE OFD locks, block handles, RocksDB snapshots, connections)
* failover and restart: applied Raft index, worker heartbeat, client handshake/version fallback

## Consumer fit and API surface

Trace every current consumer of a changed public item. Flag:

* consumer-specific behavior leaking into a shared API crate
* the inverse: a new public method on `fs-api` / `ufs-api` / `storage-api` / proto whose only caller is one internal crate — prefer a private helper instead of expanding the public surface

## Scope, ownership, and necessity

Map each new abstraction, state machine, config option, defensive copy, and compatibility path to a **current** contract and production consumer. Challenge unrelated features and speculative generality ("we might need this later"). Keep the PR focused.

## Configuration and public choices

Ask what current-consumer evidence or prior art supports each new default, public operation, wire format, or imported external concept. Require an explicit choice or deferral when that evidence is absent. Silent default changes on heartbeat, replica, timeout, or cache policy are high-impact.

## Enforcement

Follow every denial path (ACL, quota, auth, path validation, set-id stripping) to the operation that executes it. Check alternate callers that can bypass a facade (direct master RPC, FUSE setattr bits, CLI, SDK).

## Borrowed vs owned state

Determine whether each retained value is borrowed or owned under the crate contract, then trace caches, metrics, listings, and query views to the authoritative source:

* buffers and slices that outlive the read
* block replica locations vs actual worker state
* inode views vs journal/RocksDB
* Raft applied index vs snapshot

Stale cache after the owner mutated is a correctness bug, not a nit.

## Bounds cover the final operation

Locate the owner of the complete emitted or retained result, including wrappers and metadata. Probe tiny, exact, and oversized limits: block size, packet size, path length (including multibyte), proto message size, replica count.

## Real entry path

Tests should exercise the shipped client, FUSE, CSI, CLI, or worker where the bug lives. A unit test of an internal helper does not catch a broken proto default, JNI export, or MountPod mount option. CSI changes: follow [cv-csi-test](../../cv-csi-test/SKILL.md).

## Test strength

Assertions must fail on the intended regression and verify external state (file contents, RPC error, metrics, logs, disposal) rather than restating the implementation. Coverage is not evidence that the scenario is correct. Prefer a negative control: a deliberately invalid case fails through the real path.

## Compatibility and versioning

Heartbeat / handshake version fields, proto evolution, Java/Python SDK parity, and legacy fallback belong in the same review as the server change. A server that understands a new field while old clients break (or the reverse) is a blocker unless the PR documents a rollout.

## Backward-compatible evolution

Curvine ships a versioned wire format (proto2) and versioned Rust APIs (`curvine-fs-api`, `curvine-ufs-api`, `curvine-storage-api`, JNI/Python SDK). Mixed-version rolling upgrades are the norm, so every change to an existing function signature, struct, or proto message **must** remain consumable by code built against the previous version. Treat the rules below as blockers, not suggestions.

### Function and struct parameters

* **Append-only positional parameters.** When adding a parameter to an existing public function or method, place it **last** in the parameter list. Inserting a parameter in the middle reorders every call site and breaks any external caller that passes arguments positionally.
* **Prefer a default / `Option` / builder.** A new trailing parameter should default to the previous behavior (e.g. `Option<T>` with `None`, a `Default`, or a builder method) so existing callers compiled against the old signature keep working without edits.
* **Do not change parameter order or types of existing parameters.** Rename + reorder is a breaking change even if the names match; re-typing `&str` to `String` (or vice versa) on a public trait method is breaking.
* **Trait methods.** Adding a method to a public trait without a default impl breaks all out-of-tree implementors. Provide a default body, or split a new trait.
* **Struct fields.** New public struct fields are appended at the end; do not insert, reorder, or retag existing fields. If the struct is constructed by external code via struct literal, prefer a builder or `..Default::default()` and document it.

### Proto messages (`curvine-proto`, `curvine-raft`)

* **New fields must be `optional` (proto2) or non-`required` (proto3).** Never add a new `required` field to an existing message. An old peer will ignore an unknown field (and will never populate it), while a new peer will reject a message from an old peer that omits the required field — either way rolling upgrades break. Use `optional` (proto2) or plain scalar / `optional` (proto3) so absence is valid.
* **Append new field numbers at the end.** Pick the next unused tag number for the message; do not fill gaps left by deleted fields (see "never reuse" below). Appending keeps old readers tolerant of new fields they do not recognize.
* **Never reuse, renumber, or retag a field.** A tag is a permanent contract. Renaming the field is fine; reusing its number for a different type/name is a wire break. Deleted fields must be reserved (`reserved 7;` / `reserved "old_name";`) and never reissued.
* **Never change a field's type or label.** Promoting `optional` to `required`, changing `int32` to `int64`, or `string` to `bytes` changes the wire encoding and breaks both sides.
* **Never change a field's default value** in a way that changes wire behavior: existing readers that omit the field still see the old default. If the semantic default must move, add a new field rather than editing the existing one.
* **Enums.** New enum values must be appended with new numbers; do not renumber or reuse existing values. For proto2 closed enums, recognize that old readers will map unknown values to the default — prefer adding a sentinel `UNKNOWN_* = 0` if not already present.
* **`map<K,V>` fields.** Treat a `map` like a `repeated` message pair: append at the end, never retag. Changing key or value type is breaking.
* **Oneofs.** Adding a new oneof field is fine; moving an existing field into or out of a oneof changes its wire semantics and is breaking.

### RPC and SDK parity

* **Both sides of every changed RPC.** A request/response change must update master, worker, client, JNI, and Python SDK in the same PR, or document a staged rollout. A server that accepts a new `optional` field while the old client still omits it is the safe direction; the reverse (server requires, client omits) is a blocker.
* **Heartbeat / handshake version.** If the PR bumps a heartbeat or handshake version field, verify the server still accepts the old version during the rollout window, and that the version is monotonic.
* **Java / Python SDK parity.** A new proto field surfaced to users must be exposed in both SDKs with the same name and semantics, or the SDK gap must be tracked.

### How to verify during review

1. Diff the `.proto` file and confirm every added line is `optional`/`repeated` with a tag greater than the previous max in that message.
2. Diff public Rust signatures in `*-api` crates and confirm new parameters are trailing and defaultable.
3. Grep for `reserved` near deleted fields; flag any tag that was previously used and is now reused.
4. Trace the changed RPC end-to-end and confirm both peers tolerate the field's absence.

## Author verification story

Read the PR body (and any **Test verified** table) before trusting CI:

* Which commands ran, and on which dirty crates?
* Is there a manual check for FUSE / CSI / failover when those layers changed?
* Does a green compile job stand in for a missing crate test? If yes, that gap is `must-fix`.

A passing gate proves the tree built. It does not prove the new scenario is correct.

## Dead code hygiene

After mapping callers of changed symbols:

1. List functions, modules, proto fields, metrics, and compat shims that are now unreachable
2. Put the list in the local draft
3. **Ask before recommending deletion** — do not silently delete, and do not leave confirmed dead code without a comment or a tracked follow-up

```text
DEAD CODE IDENTIFIED:
- format_legacy_path() in curvine-common — replaced by format_path()
- leftover metric `replica_legacy_sync` — no remaining writers
→ Safe to remove these?
```

## Cargo.toml and lockfile

When `Cargo.toml` or `Cargo.lock` changes, review them like production code:

1. Does the existing workspace already provide this capability?
2. Read the lockfile diff, not only the manifest; a single direct bump can pull many transitives
3. For a version bump, read the changelog; semver is a promise the crate may not have kept
4. Prefer one crate (or a small related group) per change so a break is revertible
5. Never accept a hand-edited lockfile. Commit it, review it, and let Cargo generate it
6. Flag bulk "bump deps" PRs that skip changelog review

A new dependency is a liability. Prefer the standard library and existing workspace crates.

## Complexity relocation

A refactor that moves the same branches into a new type or module without reducing the number of concepts a reader must hold is not simpler. Count modes, flags, and call-graph hops before and after. If the count is unchanged, ask for the version where a whole branch, mode, or layer disappears — or treat the reshuffle as optional noise.

When the finding is structural, name the remedy (dispatcher, collapse branches, move feature logic out of `*-api`, delete a pass-through wrapper). "This is complex" is not a review comment.
