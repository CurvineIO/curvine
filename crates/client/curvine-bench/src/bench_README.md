# Curvine Bench Developer Notes

This directory contains the core implementation behind `cv bench`. User-facing
usage belongs in the repository README; this document is for maintainers who
need to change benchmark behavior, add workloads, or inspect measurement
semantics.

## Module Layout

- `bench.rs` is the facade. It declares modules, re-exports public bench types,
  and owns the `CurvineBenchRunner` state shared by the internal modules.
- `config.rs` defines `BenchConfig`, `BenchPrefillConfig`, profile/mode/target
  enums, and default values.
- `plan.rs` maps `BenchConfig` to ordered `PlannedStep`s. This is the single
  place that defines how `basic`, `deep`, and explicit `--workload` compose.
- `runner.rs` owns the runner lifecycle: temp path creation, prepare/cleanup,
  executing the plan, and running timed mixed workloads.
- `suites.rs` defines the table-driven single-op suites: metadata and
  throughput.
- `ops.rs` executes individual operations and shared path-action loops.
- `seed.rs` prepares mixed workload seed data and implements `bench prefill`
  without fabricating a full `BenchConfig`.
- `paths.rs` owns path helpers, workset sharding, mode inference, and duration
  parsing.
- `report.rs` owns metrics aggregation, latency buckets, typed value/cost units,
  and JSON-compatible report structs.
- `workload.rs` parses workload specs, presets, weights, and operation
  classification.
- `backend.rs` adapts benchmark primitives to Curvine client mode or FUSE mode.

The CLI adapter lives under `curvine-cli/src/cmds/bench.rs`; its output-only
helpers live in `curvine-cli/src/cmds/bench/output.rs`.

## Execution Flow

The runner follows this high-level flow:

```text
BenchConfig
  -> BenchPlan::from_config()
  -> CurvineBenchRunner::run_once()
  -> PlannedStep execution
  -> BenchReport
```

`BenchPlan` keeps profile/workload composition out of the execution loop:

- `basic` runs `MetadataSuite` then `ThroughputSuite`.
- `deep` runs `basic` plus `mixed_metadata` and `mixed_throughput`.
- An explicit `--workload` replaces profile composition and runs only that
  workload.

## Suites And Workloads

The metadata suite is NNBench-style and intentionally stage-based:

1. Create zero-byte files.
2. Stat those files.
3. Open those files.
4. Rename those files to the `.renamed` suffix.
5. Delete the renamed files.

The throughput suite writes big files once, then reads them back.

Mixed workloads use weighted operation selection in a timed loop. Seed
preparation happens before the timed section so seed cost does not consume the
configured duration. When `working_set_files` is set, read/stat/open/list
operations select from the prefilled workset under `base_path`; write/rename/
delete style operations still use the benchmark temp path.

## Prefill And Worksets

`CurvineBenchRunner::prefill` uses `WorksetSeeder` directly. It should not build
a dummy `BenchConfig`; prefill has its own config and lifecycle.

Worksets are sharded as:

```text
<base>/part-000000/item-000000000000
<base>/part-000000/item-000000000001
...
```

The `files_per_dir` value is part of the path contract. A later bench run using
`--working-set-files` must use the same `--files-per-dir` value that was used
for prefill.

## Measurement Semantics

- `ValueUnit` and `CostUnit` are typed internally. Keep the output strings
  stable because CLI output and JSON consumers rely on them.
- Metadata-style operations report `ops/s` and `ms/op`.
- Big read/write throughput reports `MiB/s` and `s/file` in single-op suites.
- Mixed read/write throughput reports `MiB/s` and `ms/op`.
- `list_dir` currently counts one list call as one operation. It does not report
  entries per second.
- Percentile latency uses fixed buckets and is only meaningful with enough
  samples. The CLI prints a low-sample warning.

## Performance Notes

The benchmark should avoid measuring its own avoidable overhead:

- `CurvineBenchRunner` clones share `UnifiedFileSystem`; Curvine client internals
  use shared `Arc` contexts/connectors/block pools.
- Write buffers are shared with `Arc<[u8]>`.
- Read buffers are allocated once per worker and reused.
- `TimedLoop` uses shared atomics for mixed workloads; this is simple and
  deterministic, but can become visible in very high-QPS metadata microbenchmarks.
- Path string construction is still on the hot path for generated write/rename/
  delete operations. Profile before optimizing it.

## Maintenance Guidelines

- Add new profile composition in `plan.rs`.
- Add new single-op suite steps in `suites.rs`.
- Add new primitive operation execution in `ops.rs`.
- Add new workload parsing or classification in `workload.rs`.
- Add new report units or metric fields in `report.rs`, preserving existing JSON
  field names and unit strings unless intentionally changing the public output.
- Keep CLI output changes in the CLI output module rather than in the client
  bench core.

Before declaring a behavior change complete, run:

```bash
cargo fmt --check
cargo check -p curvine-client -p curvine-cli
cargo test -p curvine-client bench -- --nocapture
cargo test -p curvine-cli bench -- --nocapture
```

For changes touching real filesystem behavior, seed/prefill, or cleanup, also
run the example-cluster bench smoke using
`scripts/playbook/cluster/curvine_example_hosts.ini`.
