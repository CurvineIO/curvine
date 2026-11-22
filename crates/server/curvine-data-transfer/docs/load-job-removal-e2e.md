# Load-job removal: embedded-transfer end-to-end validation

Final acceptance record for issue
[#2209](https://github.com/CurvineIO/curvine/issues/2209) — removing the legacy
load-job engine and running every load / export / UFS write-back through the
transfer service, embedded in the master by default.

This page documents the T7b gate: full workspace regression, re-run of the E2E
suites after the client-surface and type renames, and a real-cluster smoke test
of the deployment path.

- Validated commit: `dcf3801ba` (T6c2 — rename transfer-internal task types)
- Toolchain: rustc 1.98.1 / cargo 1.98.1, Linux x86_64
- Result: **zero failures**. 1,283 unit/integration tests passed (Part 1) and
  36 E2E tests passed (Part 2). The smoke cluster (Part 3) started with the
  transfer service embedded in the master, with no standalone transfer process,
  and completed a byte-identical UFS → Curvine → UFS round trip.

## What the series changed

| Step | Change |
|------|--------|
| T1 | Transfer service runs embedded inside the master (no extra process, reuses master RPC/web/log settings) |
| T2–T4 | SDK, CLI, unified-fs and the journal `UfsLoader` route load/export through the transfer RPCs |
| T5a/T5b | Legacy load-job engine and job-master client deleted |
| T6a/T6b | Config, proto and launcher scripts cleaned up (`transfer.enabled` gone, `local-cluster.sh` starts transfer only when standalone) |
| T6c1/T6c2 | `LoadJob` client surface removed, transfer-internal types renamed |

Everything below is the evidence that the resulting tree still behaves like a
single system.

## Part 1 — Full regression

```
cargo build --workspace --exclude curvine-lancedb      # ok
```

| Package | Command | Result |
|---------|---------|--------|
| `curvine-config` | `cargo test -p curvine-config` | 206 passed, 0 failed |
| `curvine-proto` | `cargo test -p curvine-proto` | 49 passed, 0 failed |
| `curvine-fs-api` | `cargo test -p curvine-fs-api` | 26 passed, 0 failed |
| `curvine-model` | `cargo test -p curvine-model` | 77 passed, 0 failed |
| `curvine-sdk-core` | `cargo test -p curvine-sdk-core` | 15 passed, 0 failed |
| `curvine-unified-fs` | `cargo test -p curvine-unified-fs` | 18 passed, 0 failed |
| `curvine-transfer-client` | `cargo test -p curvine-transfer-client` | 3 passed, 0 failed |
| `curvine-master` | `cargo test -p curvine-master --lib` | 420 passed, 0 failed, 1 ignored (pre-existing) |
| `curvine-data-transfer` | `cargo test -p curvine-data-transfer` | 19 passed, 0 failed |
| `curvine-worker` | `cargo test -p curvine-worker` | 142 passed, 0 failed, 1 ignored (pre-existing) |
| `curvine-cli` | `cargo test -p curvine-cli` | 307 passed, 0 failed |
| `curvine-web` | `cargo test -p curvine-web` | 1 passed, 0 failed |
| `curvine-server` (tests), `curvine-client`, `curvine-libsdk-java` | `cargo check -p curvine-server --tests -p curvine-client -p curvine-libsdk-java` | compiles clean |

Grouped runs used for the table above:

```
cargo test -p curvine-config -p curvine-proto -p curvine-fs-api -p curvine-model
cargo test -p curvine-sdk-core -p curvine-unified-fs -p curvine-transfer-client
cargo test -p curvine-master --lib
cargo test -p curvine-data-transfer -p curvine-worker
cargo test -p curvine-cli -p curvine-web
cargo check -p curvine-server --tests -p curvine-client -p curvine-libsdk-java
```

Totals: **1,283 passed, 0 failed, 2 ignored**. The two ignored unit tests are
manual benchmarks unrelated to this series and were already `#[ignore]`d:
`master::meta::store::inode_store::tests::bench_create_tree_large` and
`worker::block::block_store::tests::measure_concurrent_file_rewrites`.

## Part 2 — E2E suites

All four suites run serially (`--test-threads=1`) because each builds a live
mini-cluster.

| Suite | Command | Result | Time |
|-------|---------|--------|------|
| `write_cache_test` | `cargo test -p curvine-tests --test write_cache_test -- --test-threads=1` | **12 passed**, 0 failed, 0 ignored | 105s |
| `fallback_read_test` | `cargo test -p curvine-tests --test fallback_read_test -- --test-threads=1` | **13 passed**, 0 failed, 0 ignored | 14s |
| `ufs_replay_failover_test` | `cargo test -p curvine-server --test ufs_replay_failover_test -- --test-threads=1 --nocapture` | **1 passed**, 0 failed | 15s |
| `journal_test` | `cargo test -p curvine-server --test journal_test -- --test-threads=1` | **10 passed**, 0 failed, **2 ignored** | 12s |

Total: **36 passed, 0 failed, 2 ignored**.

### Cache mode vs fs mode coverage

`write_cache_test` splits evenly between the two write types, both against the
embedded transfer:

| Cache mode (6) | fs mode (6) |
|----------------|-------------|
| `test_cache_mode` | `test_fs_mode` |
| `test_cache_mode_auto_caches_with_zero_ttl` | `test_fs_mode_free` |
| `test_cache_mode_free` | `test_fs_mode_delete_with_trash_enabled_purges_ufs` |
| `test_cache_mode_free_child_with_unified_disabled` | `test_fs_mode_ufs_write_append` |
| `test_cache_mode_free_with_trash_enabled` | `test_fs_mode_ufs_write_overwrite` |
| `test_cache_mode_recaches_after_lost_worker_cleanup` | `test_fs_mode_ufs_write_random` |

`fallback_read_test` (13 cases) covers both modes too: `TC-10..TC-16` exercise
fs-mode open/seek/read falling back to UFS when the worker holding the cache
disappears, and `TC-17..TC-21` do the same for cache mode, including the
shrunk/grown/unchanged UFS-object cases (`TC-18` expects an error rather than
serving stale Curvine bytes).

### Failover evidence

`ufs_replay_failover_test` starts 3 masters and 1 worker, kills the leader, and
asserts the surviving cluster drains the UFS write-back backlog through the
now-embedded transfer service on the new leader. It passed both the
leader-election and the backlog-drain assertions (`1 passed`), which is the
direct evidence that UFS replay no longer depends on the removed engine.

`journal_test` keeps 10 cases green: snapshot recovery, raft state sync between
two masters, and the `UfsLoader` mkdir/rename/skip-non-mount paths that now go
through the transfer. The two ignored cases
(`test_ufs_loader_complete_exports_and_release_writer_is_a_fallback`,
`test_ufs_loader_complete_file_rename_within_mount_exports_live_path`) need a
live master RPC + embedded transfer + worker, which the `JournalSystem`
in-process harness does not have. Their fs-mode export path is covered
end-to-end by `ufs_replay_failover_test` and
`write_cache_test::test_fs_mode`.

## Part 3 — Real-cluster smoke test

Deployment path exercised: `build/bin/local-cluster.sh` with the shipped
`etc/curvine-cluster.toml`, whose `[transfer]` section sets no `mode` and no
`endpoints` — i.e. the embedded default.

### Startup

```
build/bin/local-cluster.sh start
```

`local-cluster.sh` resolved `transfer_standalone()` to false and started only
`master` and `worker`; it never invoked `curvine-transfer.sh`. Status after
startup:

```
master: master is running (PID: 3453632)
worker: worker is running (PID: 3453854)
Cluster is fully operational.
```

### Embedded default verification

| Check | Result |
|-------|--------|
| `ps -eo args= \| grep -E "lib/curvine-transfer\|--service transfer"` | **0** processes — no standalone transfer |
| `local-cluster.sh status` | lists `master` and `worker` only, no `transfer` entry |
| `logs/transfer.pid` | absent (no standalone pid file) |
| `logs/transfer.log` | absent — the embedded service logs into `master.log` |
| `master.log` resolved config | `mode = "embedded"` |
| `master.log` startup | `configuring embedded curvine-transfer store_backend=memory report_endpoints=["localhost:<master-rpc>"]` → `embedded curvine-transfer started` → `embedded transfer scheduler started: local master is leader` |
| `GET /api/overview` on the master | `"master_state":"Active"`, `"live_workers":1` |
| `cv transfer list` | served by the master (empty table before any job) — proves transfer RPCs are multiplexed over the master RPC port |

### End-to-end load and export

A local-directory UFS backend was available, so the full round trip was run:
UFS file → `cv load` → Curvine blocks → `cv export` → UFS file.

```
cv mount file:///tmp/t7b_smoke_ufs /t7b_smoke_ufs --write-type cache_mode --access-mode read_write
cv load  file:///tmp/t7b_smoke_ufs/src.bin /t7b_smoke_ufs/loaded.bin -w
cv fs cat /t7b_smoke_ufs/loaded.bin > loaded_via_cat.bin
cv export /t7b_smoke_ufs/loaded.bin -w
```

Observations:

- `cv load` reported `State: Completed`, `4.0MB / 4.0MB`, `1 completed`.
- `cv fs --cache-only ls /t7b_smoke_ufs` listed `/t7b_smoke_ufs/loaded.bin`
  (4,194,304 bytes) — the object really is in the Curvine namespace, not only
  in UFS.
- `cv fs blocks /t7b_smoke_ufs/loaded.bin` showed one complete 4 MiB block on
  `localhost:<worker-rpc>` — written through the embedded transfer.
- `cv fs cat` returned the cached bytes and matched the source md5.
- `cv export` reported `State: Completed`, `4.0MB / 4.0MB`.
- `cmp` on both pairs was byte-identical:

```
aac4b844a5a9ba8d27e795fc34c32b8c  /tmp/t7b_smoke_ufs/src.bin
aac4b844a5a9ba8d27e795fc34c32b8c  /tmp/t7b_out/loaded_via_cat.bin   # load → read back through Curvine
aac4b844a5a9ba8d27e795fc34c32b8c  /tmp/t7b_smoke_ufs/loaded.bin     # export → back in UFS
```

- `cv transfer list` then showed both jobs on the embedded service:

```
| JOB ID      | KIND   | STATE     | PROGRESS             | SOURCE                        | TARGET                        |
|-------------|--------|-----------|----------------------|-------------------------------|-------------------------------|
| 1d0e33d6-…  | Export | Completed | 4.0MB / 4.0MB (100%) | /t7b_smoke_ufs/loaded.bin     | file:///tmp/…_ufs/loaded.bin  |
| f06248ad-…  | Load   | Completed | 4.0MB / 4.0MB (100%) | file:///tmp/…_ufs/src.bin     | /t7b_smoke_ufs/loaded.bin     |
```

`local-cluster.sh stop` then shut both services down gracefully, again touching
no transfer process.

### Smoke-test environment note

The machine already had another independent Curvine cluster bound to the default
ports (`8995/8996/8997/9010`), so the smoke run used a copy of
`etc/curvine-cluster.toml` with alternate ports and data directories
(`build/dist/conf/curvine-smoke.toml`, generated during the run and not
committed). Copying the file verbatim would also work on a host with the default
ports free. Nothing else in the config was changed: `[transfer]` still carries
no `mode` and no `endpoints`, so it is the same embedded-default path.

Two CLI behaviours worth knowing when reproducing by hand:

- `cv mount` requires the Curvine path to mirror the UFS path
  (`--check-path-consist`, on by default); a `file://` UFS root makes the
  authority comparison fail, so pass `--check-path-consist=false` or mirror the
  directory name.
- `cv load` needs an existing mount covering both the UFS source and the Curvine
  target (`find_mount` in the transfer cluster cache), so mount first, then load
  into a path under that mount.

## Known limitations

- `journal_test` keeps 2 `#[ignore]`d cases; both require a live master RPC +
  embedded transfer + worker that the in-process `JournalSystem` harness cannot
  provide. Equivalent coverage: `ufs_replay_failover_test` and
  `write_cache_test::test_fs_mode`.
- The smoke test used the local-directory (`file://`) UFS backend only. Object
  store backends (S3/OSS) are covered by the mini-cluster E2E suites, not by
  this manual smoke run.
- `curvine-lancedb` is excluded from the workspace build; it has no dependency
  on the transfer surface.

## Reproducing

```bash
cargo build --workspace --exclude curvine-lancedb

cargo test -p curvine-config -p curvine-proto -p curvine-fs-api -p curvine-model
cargo test -p curvine-sdk-core -p curvine-unified-fs -p curvine-transfer-client
cargo test -p curvine-master --lib
cargo test -p curvine-data-transfer -p curvine-worker
cargo test -p curvine-cli -p curvine-web
cargo check -p curvine-server --tests -p curvine-client -p curvine-libsdk-java

cargo test -p curvine-tests --test write_cache_test -- --test-threads=1
cargo test -p curvine-tests --test fallback_read_test -- --test-threads=1
cargo test -p curvine-server --test ufs_replay_failover_test -- --test-threads=1 --nocapture
cargo test -p curvine-server --test journal_test -- --test-threads=1
```
