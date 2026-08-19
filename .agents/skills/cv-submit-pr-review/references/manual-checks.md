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
