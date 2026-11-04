# ORPC versus reviewed gRPC head: matched LLVM comparison

Measured on 2026-11-02 to address [jlon's performance review](https://github.com/CurvineIO/curvine/pull/1957#pullrequestreview-5740437782).

**Result: the current default gRPC implementation fails the provisional performance gate at both network rates.** These are fresh ORPC-versus-gRPC measurements; the historical Cranelift comparison and gRPC-only checkpoint measurements are not used to evaluate this gate.

## Revisions and method

- ORPC: upstream `503ed7f87c2468a2012d621b647f473ed2ef9dab`, the upstream revision already merged into the reviewed candidate.
- gRPC: reviewed head `0eeb5054f92afe1457318525d109bc3e323b2312`.
- Separate detached worktrees, identical workload source; the only baseline addition is the copied benchmark example. No runtime source changes or compatibility shims.
- Both: Rust 1.98.1 / LLVM 22.1.8, optimized release builds, mold 2.40.4, line-table debug info, sccache. Builds finished before measurements; each transport's binaries were copied and hashed before the shared build cache was reused.
- Same VM: 16 available x86_64 vCPUs, AMD EPYC-Rome, Linux 7.0.0-38-generic. This is a software-shaped veth comparison.
- One master, one worker and one client; server and client run in separate network namespaces. `tc netem` applies 1 ms delay and the selected rate in each direction, with the unchanged default 1,000-packet queue limit.
- Four readers, 64 MiB file, 1 MiB reads, 16 warmup reads per reader, 1,024 measured reads per reader. Each run measures 4 GiB and 4,096 latencies. Twenty runs total: five per transport at each rate, 80 GiB measured and 81,920 latency samples.
- Short-circuit and client read-ahead disabled. gRPC's compiled default read window remains **1**; no opt-in pipelining or transport-specific tuning overrides.
- Order alternates ORPC/gRPC and gRPC/ORPC between pairs; the network-profile order alternates too. All planned runs completed. No trials were discarded.
- CPU is combined client and server-namespace process CPU from `/proc`, normalized by measured GiB. Stable PID/start-time checks include master, worker and supervisor. Throughput and CPU include file-wrap seeks; per-read latency excludes the seek call. Per-run P99 uses nearest rank; the tables use medians of per-run metrics.
- Every run had zero shaped-queue drops in both directions, three stable server-side processes, matching workload/binary hashes and successful process/namespace teardown. Server egress counters exceeded measured payload bytes, confirming data traversed the shaped link.

## Gate results

Ratios use unrounded medians. CPU percentages mean **gRPC CPU as a percentage of ORPC CPU**, not percentage increase.

| Profile | Metric | ORPC median | gRPC median | gRPC / ORPC | Target | Result |
| --- | --- | ---: | ---: | ---: | --- | --- |
| 1gbit | Read MB/s | 119.4 | 119.0 | 99.7% | >=85% | PASS |
| 1gbit | P99 ms | 75.55 | 85.68 | 113.4% | <=110% | FAIL |
| 1gbit | CPU s/GiB | 1.1125 | 3.3075 | 297.3% | <=115% | FAIL |
| 10gbit | Read MB/s | 1169.2 | 705.6 | 60.4% | >=85% | FAIL |
| 10gbit | P99 ms | 5.28 | 8.90 | 168.6% | <=110% | FAIL |
| 10gbit | CPU s/GiB | 0.9350 | 4.3650 | 466.8% | <=115% | FAIL |

The measured default transport does not satisfy the reviewer's CPU acceptance criterion. Approval still requires further optimization or an explicit maintainer decision accepting the measured cost, with tracked 25/100 GbE validation and a fallback decision. These warm-cache remote-read results do not establish write/cold-storage performance or the representative-hardware gate.

## Variation and memory

Each range is the minimum and maximum of five independent runs. RSS is sampled every 200 ms during timed reads; combined RSS sums master, worker and client and excludes kernel page cache and the supervisor. It is not proportional-set memory.

| Profile | Transport | Read MB/s median (range) | P99 ms median (range) | CPU s/GiB median (range) | Median sampled peak RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1gbit | orpc | 119.4 (119.4–119.4) | 75.55 (70.45–91.95) | 1.1125 (1.1025–1.1450) | 110.5 |
| 1gbit | grpc | 119.0 (118.5–119.1) | 85.68 (79.50–97.91) | 3.3075 (3.2500–3.4175) | 147.4 |
| 10gbit | orpc | 1169.2 (1150.4–1183.0) | 5.28 (4.30–6.18) | 0.9350 (0.9225–1.0075) | 107.5 |
| 10gbit | grpc | 705.6 (684.3–711.8) | 8.90 (8.49–9.38) | 4.3650 (4.1975–4.4250) | 153.6 |

## Per-run values

| Profile | Pair | Transport | Read MB/s | P99 ms | CPU s/GiB | Timed peak RSS MiB |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| 1gbit | 1 | orpc | 119.4 | 75.55 | 1.1450 | 101.7 |
| 1gbit | 1 | grpc | 118.9 | 85.68 | 3.4175 | 148.3 |
| 10gbit | 1 | orpc | 1173.9 | 4.74 | 0.9350 | 107.0 |
| 10gbit | 1 | grpc | 711.8 | 8.90 | 4.3650 | 154.6 |
| 10gbit | 2 | grpc | 684.3 | 9.36 | 4.3950 | 154.6 |
| 10gbit | 2 | orpc | 1183.0 | 4.30 | 0.9250 | 107.5 |
| 1gbit | 2 | grpc | 119.1 | 89.60 | 3.2975 | 145.5 |
| 1gbit | 2 | orpc | 119.4 | 70.45 | 1.1075 | 109.4 |
| 1gbit | 3 | orpc | 119.4 | 72.79 | 1.1350 | 112.8 |
| 1gbit | 3 | grpc | 118.5 | 85.25 | 3.2500 | 147.4 |
| 10gbit | 3 | orpc | 1169.2 | 5.28 | 1.0075 | 113.6 |
| 10gbit | 3 | grpc | 705.6 | 8.66 | 4.2625 | 153.1 |
| 10gbit | 4 | grpc | 685.0 | 9.38 | 4.4250 | 153.6 |
| 10gbit | 4 | orpc | 1165.4 | 5.55 | 0.9650 | 107.0 |
| 1gbit | 4 | grpc | 119.0 | 79.50 | 3.3075 | 147.5 |
| 1gbit | 4 | orpc | 119.4 | 91.95 | 1.1125 | 110.7 |
| 1gbit | 5 | orpc | 119.4 | 88.13 | 1.1025 | 110.5 |
| 1gbit | 5 | grpc | 119.0 | 97.91 | 3.3475 | 145.8 |
| 10gbit | 5 | orpc | 1150.4 | 6.18 | 0.9225 | 114.6 |
| 10gbit | 5 | grpc | 711.0 | 8.49 | 4.1975 | 152.0 |

## Evidence and reproduction

- [results.json](results.json): official comparator outputs, gate statuses, per-run summaries and raw archive digest.
- [provenance.json](provenance.json): toolchain/build flags, source revisions/status, workload digest, binary hashes, server-reported revisions and host details.
- [raw-measurements.json.gz](raw-measurements.json.gz): every result with full latency arrays, before/after server CPU snapshots, qdisc counters, RSS/host samples, planned order and the exact executed build/driver scripts. It contains no dataset files or server logs. Decode with Python `gzip`/`json`; scripts include this run's local paths, which must be adjusted when replaying elsewhere.
- [Harness instructions](../../README.md): namespace setup, measurement boundaries and comparator validation. Use the pinned revisions above and export `RPC_TC_READS=1024`, `RPC_TC_CONCURRENCY=4`, `RPC_TC_FILE_BYTES=67108864`, `RPC_TC_CHUNK_BYTES=1048576`, `RPC_TC_WARMUP_READS=16`, `DELAY=1ms`; set `RATE` to each profile. Leave `RPC_TC_READ_WINDOW` unset.

The matched build command in each worktree was:

```sh
cargo +1.98 build --locked --release \
  -p curvine-server --bin curvine-server \
  -p curvine-client-core --example rpc_tc_read
```

Both used `RUSTFLAGS='-Cdebuginfo=line-tables-only -Clink-arg=-fuse-ld=mold'`, `CXXFLAGS='-include cstdint -g1'`, `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`, `CARGO_PROFILE_RELEASE_STRIP=none`, `CARGO_INCREMENTAL=0`, six build jobs and the same warmed target cache. Preserve each transport's copied binaries before rebuilding the other worktree. No builds ran during timing.
