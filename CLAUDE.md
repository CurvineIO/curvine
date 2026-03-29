# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and test commands

- Build everything: `make all`
- Build selected packages via the packaging script: `make build ARGS='-p server -p client'`
- Build core packages (`server`, `client`, `cli`): `make build ARGS='-p core'`
- Build with HDFS support: `make build-hdfs`
- Build the S3 gateway only: `make build ARGS='-p object'`
- Build debug artifacts: `make build ARGS='-d'`
- Create a release tarball after building: `make dist`
- Package an existing `build/dist` without rebuilding: `make dist-only`
- Run an arbitrary cargo command: `make cargo ARGS='test --verbose'`

### Rust quality checks

- Format via the repo hook script: `make format`
- Check formatting only: `cargo fmt --check`
- Run clippy with the same strictness as CI: `cargo clippy --all-targets -- --deny=warnings --allow clippy::uninlined-format-args`

### Tests

- Run all Rust tests: `cargo test --workspace`
- Run tests for one crate: `cargo test -p curvine-server`
- Run a single Rust test by name: `cargo test -p curvine-server <test_name> -- --nocapture`
- Run tests with nextest: `cargo nextest run --workspace`
- Run CI-style tests that skip UFS-dependent cases: `cargo nextest run --profile ci-no-ufs --workspace`
- Start the regression portal locally: `python3 curvine-tests/regression/build-server.py`
- Run the regression portal once for CI-style UT + coverage: `python3 curvine-tests/regression/build-server.py --run-once-ut-cov`

### Frontend (web UI)

- Web UI source lives in `curvine-web/webui`
- Install dependencies: `cd curvine-web/webui && npm install`
- Start dev server: `cd curvine-web/webui && npm run serve`
- Build frontend assets: `cd curvine-web/webui && npm run build`
- Lint frontend code: `cd curvine-web/webui && npm run lint`

### Local runtime

- Build distributable output into `build/dist`: `make all`
- Start master from `build/dist`: `build/dist/bin/curvine-master.sh start`
- Start worker from `build/dist`: `build/dist/bin/curvine-worker.sh start`
- Start FUSE from `build/dist`: `build/dist/bin/curvine-fuse.sh start`
- Cluster report: `build/dist/bin/cv report`

## High-level architecture

Curvine is a Rust workspace for a distributed cache/filesystem with separate metadata/control responsibilities on the master side and block/data responsibilities on the worker side.

### Main workspace crates

- `orpc`: internal async RPC/runtime/networking layer used by the rest of the system.
- `curvine-common`: shared config, error types, state models, filesystem abstractions, RocksDB helpers, and Raft/journal support.
- `curvine-server`: core server implementation for both master and worker roles.
- `curvine-client`: Rust client library and the main filesystem access layer.
- `curvine-ufs`: adapters for underlying storage backends, mainly via feature-gated OpenDAL integrations.
- `curvine-fuse`: FUSE process that exposes Curvine as a mounted filesystem.
- `curvine-web`: Axum-based HTTP/UI server used by master and worker web endpoints.
- `curvine-cli`: `cv` CLI for filesystem ops, reports, mounts, and load jobs.
- `curvine-s3-gateway`: S3-compatible gateway over Curvine.
- `curvine-libsdk`: `cdylib` bridge for Java/Python bindings.
- `curvine-tests`: regression harnesses, benchmarks, nextest profiles, and portal tooling.
- `curvine-csi`: separate Go CSI driver for Kubernetes deployments.

### Runtime roles and startup model

- `curvine-server/src/bin/curvine-server.rs` is the entrypoint for both server roles.
- The packaged `curvine-master.sh` and `curvine-worker.sh` scripts both wrap the same binary, selecting `--service master` or `--service worker`.
- `curvine-fuse` and `curvine-s3-gateway` are separate binaries.
- Cluster configuration is loaded through `ClusterConf` in `curvine-common/src/conf/cluster_conf.rs`; the example local config is `etc/curvine-cluster.toml`.

### Master side

The master owns metadata and coordination:

- Raft/journal-backed metadata startup happens in `curvine-server/src/master/master_server.rs`.
- The master initializes the journal system first, then starts RPC, web, the master actor loop, mount restore, job manager, and TTL scheduling.
- Master responsibilities include metadata filesystem state, mount table management, worker coordination, quotas, jobs, replication coordination, and journal-backed consistency.

### Worker side

The worker owns block storage and data-serving paths:

- Worker startup is in `curvine-server/src/worker/worker_server.rs`.
- The worker creates `BlockStore`, task management, replication support, RPC, web, and the block actor.
- Workers heartbeat to the master and serve block/data operations.
- On Linux, a worker can also start the S3 gateway inline when `worker.enable_s3_gateway = true`.

### Client and filesystem model

The most important client-side abstraction is `UnifiedFileSystem` in `curvine-client/src/unified/unified_filesystem.rs`.

- CLI, client code, and FUSE all build on this layer.
- Paths can resolve either to Curvine-native storage or to mounted UFS backends.
- Unified FS behavior is controlled by client config such as `enable_unified_fs`, `enable_rust_read_ufs`, and `mount_update_ttl`.
- Many operations first check whether a path is mounted and whether it is in cache mode before delegating to Curvine-native or UFS-backed logic.

### Web/UI layer

- `curvine-web/src/server/web_server.rs` provides the shared Axum web server used by server components.
- It serves service-specific API routers and falls back to static files in `curvine-web/webui`.
- The same crate is therefore both the HTTP API surface and the static web UI host.

### Testing structure

Testing is layered rather than centralized in one command:

- Standard crate/unit/integration tests run with `cargo test` or `cargo nextest`.
- `curvine-tests` contains the regression portal, nextest profiles, coverage helpers, and system-style test entrypoints.
- The `ci-no-ufs` nextest profile skips tests that require MinIO/UFS-backed environments.
- FUSE tests need a running cluster and Docker privileged mode with `/dev/fuse` when run in containers.

### Packaging and deployment surfaces

- `build/build.sh` assembles `build/dist` with binaries, configs, and test assets.
- `make dist` creates a tarball from `build/dist`.
- `curvine-docker/` contains compile, runtime, and Fluid image definitions.
- `curvine-csi/` contains the Go CSI driver and Kubernetes deployment assets.
- `curvine-s3-gateway` can be run standalone or co-started with a worker.

## Important repo-specific conventions

- `journal.journal_addrs` is important beyond Raft setup: it is also used for master discovery defaults and master hostname validation.
- `format_master` and `format_worker` in `etc/curvine-cluster.toml` are destructive local reset flags; do not enable them casually.
- The root `make` targets are packaging-oriented and run environment checks; direct `cargo` commands are often faster for iterative Rust development.
- The root `package.json` is only for commitlint tooling; the actual frontend app lives under `curvine-web/webui`.

## Good entry points for orientation

- `README.md`
- `Cargo.toml`
- `etc/curvine-cluster.toml`
- `curvine-common/src/conf/cluster_conf.rs`
- `curvine-server/src/bin/curvine-server.rs`
- `curvine-server/src/master/master_server.rs`
- `curvine-server/src/worker/worker_server.rs`
- `curvine-client/src/unified/unified_filesystem.rs`
- `curvine-cli/src/main.rs`
- `curvine-web/src/server/web_server.rs`
- `curvine-tests/README.md`
