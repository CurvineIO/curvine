# Repository Guidelines

## Architecture Overview
- Rust masters live in `curvine-server/`, shared protocol/state code in `curvine-common/` and `orpc/`, and SSD-backed workers under `curvine-server/src/worker/`.
- Masters boot a `JournalSystem` (`curvine-server/src/master/journal/journal_system.rs`) that restores snapshots, replays Raft logs, and serves a consistent `MasterFilesystem`.
- Workers expose `BlockStore` capacity and talk to masters through the blocking `MasterClient`.
- Clients build an `FsContext`, fetch metadata from masters, and stream data from workers via buffered readers or multi-replica writers (`curvine-client/src/file`).

## Master
- **Leading Master** — After election, `Master::start` launches RPC/Web services; `MasterHandler` serves metadata updates while `MasterActor` handles background work (`curvine-server/src/master/master_server.rs`, `curvine-server/src/master/master_handler.rs`).
- **Standby Master** — Followers keep the same journal view; `RoleMonitor` tracks leader changes so failover is instant (`curvine-common/src/raft/role_monitor.rs`).

## Raft
- Metadata writes append to a RocksDB-backed log; `JournalWriter` emits entries, `JournalLoader` replays them, and snapshot plumbing keeps recovery fast alongside TTL bucket upkeep (`curvine-server/src/master/journal`).

## Workers
- **Block storage** — `Worker::with_conf` derives host/IP, starts the RPC runtime, and instantiates `BlockActor`. Registration sends a `HeartbeatStatus::Start`, then full reports and periodic deltas keep `WorkerManager` current on capacity and block placement (`curvine-server/src/worker/worker_server.rs`, `curvine-server/src/worker/block/block_actor.rs`, `curvine-server/src/master/fs/worker_manager.rs`).

## Client
- **Read** — `FsClient::get_block_locations` resolves block maps; `FsReader` streams chunks in parallel with retry bookkeeping (`curvine-client/src/file/fs_client.rs`, `curvine-client/src/file/fs_reader.rs`).
- **Cacheing hit** — `UnifiedFileSystem::open` validates mount metadata and cached freshness; hits short-circuit to Curvine data and bump metrics (`curvine-client/src/unified/unified_filesystem.rs`).
- **Write** — `FsWriter`/`BlockWriter` pipeline data to replicas, then call `complete_file` so the master finalizes length and policy (`curvine-client/src/block/block_writer.rs`, `curvine-client/src/file/fs_client.rs`, `curvine-server/src/master/fs/master_filesystem.rs`).
- **Sync Cache** — Normal writes persist in Curvine tiers; lease checks on complete prevent conflicting commits (`curvine-server/src/master/fs/master_filesystem.rs`).
- **Async Cache** — On mounted UFS reads, `UnifiedFileSystem` can submit load jobs via `JobMasterClient::submit_load_job` to hydrate cache asynchronously (`curvine-client/src/rpc/job_master_client.rs`).

## Contributor Quick Guide
- **Structure** — Masters in `curvine-server/`, shared crates in `curvine-common/` and `orpc/`, clients in `curvine-client/`, integration tests in `curvine-tests/`.
- **Build/Test** — Run `make check-env`, `make build`, and `make cargo ARGS='test --workspace --release'`; `make format` runs `cargo fmt` + `cargo clippy --fix`.
- **Style** — Obey `rustfmt.toml` (100 cols, 4 spaces) and keep clippy clean; Go helpers must pass `go fmt`/`go vet`.
- **Testing** — Add targeted `cargo test -p <crate>` coverage, extend `curvine-tests/tests/`, and finish with `build/run-tests.sh`.
- **Collaboration** — Use Conventional Commits with issue IDs, include scope/tests in PRs, base deployment updates on `etc/`, and follow `SECURITY.md` for disclosures.
