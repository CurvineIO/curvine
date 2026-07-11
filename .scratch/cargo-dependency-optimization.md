# Cargo Dependency Optimization Plan

## Goal

Reduce Cargo compile time and `target` disk usage by optimizing dependency
declarations and feature selection, while preserving existing build behavior.

## Non-Goals

- Do not remove Opendal support.
- Do not change `workspace.default-members`.
- Do not change default crate features such as `curvine-client/default`.
- Do not change `Makefile`, `build/build.sh`, or CI build semantics unless a
  later task explicitly scopes and approves that behavior change.
- Do not optimize by excluding packages from the default build.

## Current Baseline

Measured in this workspace after `T1`:

| Area | Size |
| ---- | ---- |
| `target` | 1.5G |
| `target/release` | 1.5G |
| `target/release/deps` | 1.2G |
| `target/release/build` | 232M |

Rechecked after `T6`: the existing cached `target` directory is still 1.5G.
This is not an impact measurement because the workspace has not been rebuilt
from a fresh target after the dependency changes.

Largest build-script outputs currently observed:

| Path pattern | Size signal |
| ------------ | ----------- |
| `target/release/build/librocksdb-sys-*` | 24M, 25M, 35M, 35M |
| `target/release/build/curvine-common-*` | 13M, 13M |
| `target/release/build/raft-proto-*` | 14M |
| `target/release/build/ring-*` | 1.4M, 3.8M |
| `target/release/build/pyo3-*` | 1.6M, 2.1M, 2.1M |

Largest dependency artifacts currently observed:

| Artifact pattern | Size signal |
| ---------------- | ----------- |
| `liblibrocksdb_sys-*.rlib` | 19M |
| `curvine_server-*` | 19M |
| `curvine_cli-*` | 16M |
| `curvine_s3_gateway-*` | 15M |
| `libcurvine_libsdk.dylib` | 12M |
| `libcurvine_common-*.rlib` | 12M |
| `libopendal-*.rlib` | 11M |
| `libtokio-*.rlib` | 9.5M |
| `libbindgen-*.rlib` | 8.7M, 8.8M |

## Duplicate Dependency Families

The following duplicate versions were found by parsing `Cargo.lock`.
These are candidates for later investigation; not all are directly actionable.

High-impact families:

| Crate | Versions |
| ----- | -------- |
| `reqwest` | 0.11.27, 0.12.25 |
| `hyper` | 0.14.32, 1.7.0 |
| `http` | 0.2.12, 1.3.1 |
| `http-body` | 0.4.6, 1.0.1 |
| `hyper-rustls` | 0.24.2, 0.27.7 |
| `rustls` | 0.21.12, 0.23.35 |
| `tokio-rustls` | 0.24.1, 0.26.4 |
| `tower` | 0.4.13, 0.5.2 |
| `tower-http` | 0.5.2, 0.6.8 |
| `prost` | 0.11.9, 0.14.1 |
| `prost-build` | 0.11.9, 0.14.1 |
| `prost-derive` | 0.11.9, 0.14.1 |
| `prost-types` | 0.11.9, 0.14.1 |
| `syn` | 1.0.109, 2.0.117 |
| `dashmap` | 5.5.3, 6.1.0 |
| `rand` | 0.8.5, 0.9.2, 0.10.1 |
| `getrandom` | 0.2.16, 0.3.3, 0.4.2 |
| `thiserror` | 1.0.69, 2.0.16 |
| `indexmap` | 1.9.3, 2.14.0 |
| `lru` | 0.12.5, 0.16.1 |

Large transitive stacks:

- `curvine-cli` used `reqwest 0.11` before `T5`; it now depends on `reqwest
  0.12`, matching the stack already used by OpenDAL/ObjectStore/Lance.
- `opendal`, `object_store`, Lance, and LanceDB pull the `reqwest 0.12`,
  `hyper 1.x`, and `http 1.x` stack.
- Lance and LanceDB pull DataFusion and Arrow families. These remain required
  for LanceDB functionality and should stay feature-scoped.

## Direct Feature Candidates

These candidates are based on direct manifest declarations and source usage.
They must be verified one at a time.

| Candidate | Current signal | Status |
| --------- | -------------- | ------ |
| `moka` | Workspace enabled `sync` and `future`; source uses `moka::sync` and `moka::policy` | `future` removed in `c337093` |
| `prometheus` | `process` feature enabled; source usage found only metrics types and `proto::MetricType` | `process` removed in `c337093` |
| `uuid` | Workspace enabled `v4`, `fast-rng`, `macro-diagnostics`; source uses `Uuid::new_v4`, no `uuid!` macro found | `macro-diagnostics` removed in `c337093` |
| `tokio-util` | Workspace enabled `full`; source uses `codec` and re-exported `bytes` | `bytes` imports moved to direct crate and feature narrowed to `codec` in `a627075` |
| `tokio` | Workspace enables `full`; source uses runtime, macros, net, io, sync, time, signal, fs | Narrow only after exact feature matrix is verified |
| `serde_with` | No source usage found by static grep | Removed in `c337093` |
| `reqwest` | CLI used `0.11`; transitive deps use `0.12` | CLI manifest upgraded to `0.12` in `d01e8fc`; lock cleanup requires online Cargo |
| `opendal` | Required; service features are selected explicitly by Curvine features | Default OpenDAL crate features disabled in `76a9a2f`; Curvine default still enables `opendal-s3` |

## Phased Tasks

| Task | Purpose | Behavior impact |
| ---- | ------- | --------------- |
| T1 | Remove unused direct dependencies | None intended; completed in `f37b31b` |
| T2 | Record dependency audit baseline | None; completed in `3b86a0e` |
| T3 | Trim low-risk direct features | None intended; completed in `c337093` |
| T4 | Narrow `tokio-util` feature usage | None intended; completed in `a627075` |
| T5 | Align duplicate HTTP dependency stack where compatible | None intended; CLI manifest completed in `d01e8fc`; lock cleanup awaits online Cargo |
| T6 | Review heavy native/build dependency boundaries | None intended; OpenDAL default feature narrowing completed in `76a9a2f` |
| T7 | Record final impact and limitations | None; this update |

## Completed Commit Sequence

1. `f37b31b` - `build(cargo): remove unused direct dependencies`
2. `3b86a0e` - `docs(cargo): record dependency optimization baseline`
3. `c337093` - `build(cargo): trim low-risk dependency features`
4. `a627075` - `build(cargo): narrow tokio-util feature usage`
5. `d01e8fc` - `build(cargo): align cli reqwest with http stack`
6. `76a9a2f` - `build(cargo): avoid implicit opendal default features`

## Follow-up Checks

- Run `cargo check --workspace --locked` with network access or a complete local
  Cargo cache.
- Run `cargo tree -d --locked` after Cargo can update/read the full registry
  index. In particular, verify whether the old `reqwest 0.11`/`hyper 0.14`
  stack disappears from the resolved graph after lock cleanup.
- Run a clean build into a fresh target directory before comparing disk usage:
  `CARGO_TARGET_DIR=/tmp/curvine-target-after cargo build --workspace --locked`.
- Revisit `tokio/full` only after a crate-by-crate feature matrix is available;
  it is intentionally not changed in this phase.

## Verification Commands

Use these commands after each phase when available:

```bash
cargo +stable metadata --format-version 1 --no-deps
cargo +stable fmt --check
git diff --check
cargo check --workspace --locked
cargo tree -d --locked
cargo tree --workspace --edges features --locked
du -hd 2 target
```

Current local limitation: `cargo tree --offline` and focused `cargo check`
commands fail because the local cache/index is missing crates such as `symlink`
for `tracing-appender`. The sandbox also cannot connect to the local proxy at
`127.0.0.1:7890`, so online checks could not be completed here.
