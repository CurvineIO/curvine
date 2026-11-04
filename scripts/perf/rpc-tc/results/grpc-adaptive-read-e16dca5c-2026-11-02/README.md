# Experimental auto-window checkpoint comparison

**The opt-in auto-window candidate fails the ORPC provisional gate in at least one profile.** Keep the compiled default window at 1; the experimental auto window is opt-in.

## Revisions and method

- One ORPC baseline, one previous gRPC checkpoint and one candidate; five independent runs per profile and label (30 runs, 120 GiB and 122,880 latency samples).
- Four readers; 64 MiB file, 1 MiB reads, 16 warmup reads and 1,024 timed reads per reader. Short-circuit and client read-ahead are disabled.
- Each direction is shaped to 1 ms plus the selected rate. All qdiscs report zero drops; server egress exceeds measured payload. Each trial has three stable server-side processes across the timed interval.
- CPU combines client and server-namespace process time per GiB. Per-run P99 uses nearest rank; group values are medians of per-run values. Throughput and CPU include file-wrap seeks; per-read latency excludes the seek call.
- Exact configured windows are read from each trial config and checked against the manifest. An unset window means the compiled default; it is not an inferred effective value. The window is recorded separately from the comparator workload.

| Label | Transport | Revision | Configured read window |
| --- | --- | --- | --- |
| orpc | orpc | `503ed7f87c2468a2012d621b647f473ed2ef9dab` | compiled default (not set in config) |
| previous | grpc | `0eeb5054f92afe1457318525d109bc3e323b2312` | compiled default (not set in config) |
| grpc | grpc | `e16dca5c374c1dd63fe9c7e5ad434ff9d21220fd` | 0 (experimental auto) |

## ORPC provisional gate

Ratios are candidate gRPC / ORPC medians. Decisions use unrounded values and the comparator thresholds: throughput ≥85%, P99 ≤110%, CPU ≤115%.

| Profile | Metric | ORPC median | Candidate median | Ratio | Target | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 1gbit | Throughput MB/s | 119.4 | 119.1 | 99.71% | ≥85% | PASS |
| 1gbit | P99 ms | 82.67 | 88.67 | 107.26% | ≤110% | PASS |
| 1gbit | CPU s/GiB | 1.1300 | 3.3825 | 299.34% | ≤115% | FAIL |
| 10gbit | Throughput MB/s | 1155.9 | 842.3 | 72.86% | ≥85% | FAIL |
| 10gbit | P99 ms | 5.96 | 8.45 | 141.85% | ≤110% | FAIL |
| 10gbit | CPU s/GiB | 0.9250 | 4.4250 | 478.38% | ≤115% | FAIL |

The tc comparison is provisional and does not establish performance on representative 25/100 GbE hardware.

## Candidate versus previous checkpoint

Ratios are candidate / previous medians; below 100% means a decrease. The comparator’s `provisional_pass` always applies the generic ORPC thresholds (≥85% throughput, ≤110% P99, ≤115% CPU), even for checkpoint comparisons.

This is an opt-in auto-window measurement. The worker retention criteria do not classify this experiment; the ORPC gate above governs whether a default change may be considered.

| Profile | Throughput ratio | P99 ratio | CPU ratio | Generic comparator pass |
| --- | ---: | ---: | ---: | --- |
| 1gbit | 100.04% | 101.51% | 99.71% | PASS |
| 10gbit | 120.70% | 94.92% | 104.12% | PASS |

## Variation and memory

Ranges show the minimum and maximum across five independent runs. Timed peak RSS is sampled, sums master, worker and client, and excludes page cache and the supervisor.

| Profile | Label | Throughput MB/s median (range) | P99 ms median (range) | CPU s/GiB median (range) | Timed peak RSS MiB median (range) |
| --- | --- | ---: | ---: | ---: | ---: |
| 1gbit | orpc | 119.4 (119.2–119.5) | 82.67 (73.68–116.97) | 1.1300 (1.1175–1.1425) | 113.2 (111.9–114.4) |
| 1gbit | previous | 119.0 (114.7–119.1) | 87.36 (79.48–95.82) | 3.3925 (3.3300–3.4225) | 147.0 (146.9–147.8) |
| 1gbit | grpc | 119.1 (119.0–119.1) | 88.67 (77.39–98.59) | 3.3825 (3.3625–3.4475) | 149.2 (146.4–150.8) |
| 10gbit | orpc | 1155.9 (1127.0–1160.3) | 5.96 (5.80–6.42) | 0.9250 (0.8800–0.9775) | 112.6 (108.8–116.7) |
| 10gbit | previous | 697.8 (686.0–716.1) | 8.90 (8.50–9.04) | 4.2500 (4.2425–4.3525) | 156.1 (153.4–157.1) |
| 10gbit | grpc | 842.3 (840.1–846.1) | 8.45 (8.34–8.83) | 4.4250 (4.3200–4.4650) | 190.0 (185.2–194.7) |

## Per-run measurements

| Profile | Iteration | Label | Window | Throughput MB/s | P99 ms | CPU s/GiB | Timed peak RSS MiB |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |
| 1gbit | 1 | orpc | compiled default (not set in config) | 119.2 | 90.14 | 1.1425 | 114.1 |
| 1gbit | 1 | previous | compiled default (not set in config) | 114.7 | 79.48 | 3.3875 | 146.9 |
| 1gbit | 1 | grpc | 0 (experimental auto) | 119.0 | 85.62 | 3.4475 | 150.2 |
| 10gbit | 1 | orpc | compiled default (not set in config) | 1144.0 | 6.42 | 0.9275 | 116.7 |
| 10gbit | 1 | previous | compiled default (not set in config) | 716.1 | 8.90 | 4.3075 | 153.4 |
| 10gbit | 1 | grpc | 0 (experimental auto) | 846.1 | 8.42 | 4.4250 | 189.8 |
| 10gbit | 2 | grpc | 0 (experimental auto) | 841.6 | 8.45 | 4.4375 | 192.4 |
| 10gbit | 2 | previous | compiled default (not set in config) | 697.8 | 9.01 | 4.3525 | 156.1 |
| 10gbit | 2 | orpc | compiled default (not set in config) | 1158.9 | 5.96 | 0.9250 | 116.2 |
| 1gbit | 2 | grpc | 0 (experimental auto) | 119.1 | 77.39 | 3.4375 | 150.8 |
| 1gbit | 2 | previous | compiled default (not set in config) | 119.1 | 86.29 | 3.3925 | 147.3 |
| 1gbit | 2 | orpc | compiled default (not set in config) | 119.4 | 82.67 | 1.1300 | 112.3 |
| 1gbit | 3 | orpc | compiled default (not set in config) | 119.4 | 116.97 | 1.1350 | 113.2 |
| 1gbit | 3 | previous | compiled default (not set in config) | 119.1 | 87.36 | 3.3950 | 147.8 |
| 1gbit | 3 | grpc | 0 (experimental auto) | 119.1 | 89.03 | 3.3825 | 147.6 |
| 10gbit | 3 | orpc | compiled default (not set in config) | 1127.0 | 6.41 | 0.9775 | 112.6 |
| 10gbit | 3 | previous | compiled default (not set in config) | 686.0 | 9.04 | 4.2425 | 154.2 |
| 10gbit | 3 | grpc | 0 (experimental auto) | 842.3 | 8.60 | 4.4025 | 190.0 |
| 10gbit | 4 | grpc | 0 (experimental auto) | 845.3 | 8.83 | 4.4650 | 194.7 |
| 10gbit | 4 | previous | compiled default (not set in config) | 696.6 | 8.50 | 4.2425 | 157.1 |
| 10gbit | 4 | orpc | compiled default (not set in config) | 1155.9 | 5.80 | 0.8975 | 111.8 |
| 1gbit | 4 | grpc | 0 (experimental auto) | 119.0 | 88.67 | 3.3650 | 146.4 |
| 1gbit | 4 | previous | compiled default (not set in config) | 119.0 | 92.72 | 3.3300 | 147.0 |
| 1gbit | 4 | orpc | compiled default (not set in config) | 119.4 | 79.74 | 1.1200 | 114.4 |
| 1gbit | 5 | orpc | compiled default (not set in config) | 119.5 | 73.68 | 1.1175 | 111.9 |
| 1gbit | 5 | previous | compiled default (not set in config) | 118.1 | 95.82 | 3.4225 | 147.0 |
| 1gbit | 5 | grpc | 0 (experimental auto) | 119.1 | 98.59 | 3.3625 | 149.2 |
| 10gbit | 5 | orpc | compiled default (not set in config) | 1160.3 | 5.92 | 0.8800 | 108.8 |
| 10gbit | 5 | previous | compiled default (not set in config) | 705.5 | 8.89 | 4.2500 | 156.5 |
| 10gbit | 5 | grpc | 0 (experimental auto) | 840.1 | 8.34 | 4.3200 | 185.2 |

## Evidence

- [results.json](results.json): every per-run value, median and range, recorded comparator outputs, independent gate checks and archive digest.
- [provenance.json](provenance.json): revisions, binary checksums, server versions, workload hash, configured windows and executed script hashes.
- [raw-measurements.json.gz](raw-measurements.json.gz): all result latency samples, before/after server CPU snapshots, qdisc counters, memory and host samples, run order, and exact executed driver and build scripts. The scripts contain local workspace paths. No raw configuration, dataset, logs, perf data or stack profiles are included.
- [Harness instructions](../../README.md): measurement boundaries and comparator method.

## Validation and retained configuration

The compiled default remains **1**. Automatic sizing is available only with explicit `read_pipeline_window = 0`; the matched ORPC gate failed, so this experiment does not justify promoting it to the default. The dedicated worker executor was measured and reverted before this checkpoint; see [its report](../grpc-read-executor-d778981c-2026-11-02/README.md). No tonic dependency patch or upgrade was made.

| Local release suite | Passed | Failed |
| --- | ---: | ---: |
| Client core unit tests (including 8 controller and 16 block-client cases) | 104 | 0 |
| Configuration unit tests | 143 | 0 |
| SDK core unit tests | 17 | 0 |
| Real-storage read pipeline integration | 1 | 0 |
| Real-storage batch writer integration | 2 | 0 |
| Total | 267 | 0 |

Strict all-target Clippy passes for client core, config and SDK core, and strict Clippy passes for the two integration targets above. Workspace formatting and shell syntax checks pass. A broader all-target `curvine-tests` lint run encountered an existing `useless_vec` warning at `fs_test.rs:144`; that unrelated file was left unchanged.

[Full PR CI on `94373b78`](https://github.com/CurvineIO/curvine/actions/runs/41508119884) passed, including compilation, Clippy, dependency boundaries, the new reader/SDK regressions, replication and mounted FUSE/POSIX smoke. Its runtime source is identical to measured `e16dca5c`; the intervening commit adds only the rejected-executor report.

[Jlon's code review](https://github.com/CurvineIO/curvine/pull/1957#pullrequestreview-5742030097) independently found no defect in the adaptive implementation and confirmed that the server-only frame-size suggestion cannot enlarge downloads with tonic 0.9. The required performance gate remains open. The explicit SDK `Default` implementation is retained so both Rust `Default()` and omitted configuration use one, while an explicit zero remains opt-in.
