# Worker checkpoint comparison

**The worker checkpoint does not meet the retention criteria across both profiles.** Do not retain the worker checkpoint on these measured criteria.

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
| grpc | grpc | `d778981c99ecb203e4e0ce03223d3f059c439de0` | compiled default (not set in config) |

## ORPC provisional gate

Ratios are candidate gRPC / ORPC medians. Decisions use unrounded values and the comparator thresholds: throughput ≥85%, P99 ≤110%, CPU ≤115%.

| Profile | Metric | ORPC median | Candidate median | Ratio | Target | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 1gbit | Throughput MB/s | 119.4 | 119.0 | 99.61% | ≥85% | PASS |
| 1gbit | P99 ms | 76.65 | 99.53 | 129.84% | ≤110% | FAIL |
| 1gbit | CPU s/GiB | 1.1425 | 3.4075 | 298.25% | ≤115% | FAIL |
| 10gbit | Throughput MB/s | 1169.6 | 713.8 | 61.03% | ≥85% | FAIL |
| 10gbit | P99 ms | 5.06 | 8.87 | 175.54% | ≤110% | FAIL |
| 10gbit | CPU s/GiB | 0.9625 | 4.3375 | 450.65% | ≤115% | FAIL |

The tc comparison is provisional and does not establish performance on representative 25/100 GbE hardware.

## Candidate versus previous checkpoint

Ratios are candidate / previous medians; below 100% means a decrease. The comparator’s `provisional_pass` always applies the generic ORPC thresholds (≥85% throughput, ≤110% P99, ≤115% CPU), even for checkpoint comparisons.

The separate worker retention decision requires throughput ≥95%, P99 ≤105% and CPU ≤95% in **both** profiles.

| Profile | Throughput ratio | P99 ratio | CPU ratio | Generic comparator pass | Worker retention |
| --- | ---: | ---: | ---: | --- | --- |
| 1gbit | 99.99% | 118.04% | 100.07% | FAIL | FAIL |
| 10gbit | 101.42% | 97.24% | 101.76% | PASS | FAIL |

## Variation and memory

Ranges show the minimum and maximum across five independent runs. Timed peak RSS is sampled, sums master, worker and client, and excludes page cache and the supervisor.

| Profile | Label | Throughput MB/s median (range) | P99 ms median (range) | CPU s/GiB median (range) | Timed peak RSS MiB median (range) |
| --- | --- | ---: | ---: | ---: | ---: |
| 1gbit | orpc | 119.4 (119.4–119.5) | 76.65 (66.82–83.47) | 1.1425 (1.1300–1.2000) | 113.1 (110.9–115.3) |
| 1gbit | previous | 119.0 (118.2–119.1) | 84.32 (74.37–104.70) | 3.4050 (3.3550–3.5100) | 148.9 (145.5–151.9) |
| 1gbit | grpc | 119.0 (118.9–119.0) | 99.53 (71.24–106.28) | 3.4075 (3.3275–3.4375) | 148.0 (145.7–149.9) |
| 10gbit | orpc | 1169.6 (1157.0–1178.0) | 5.06 (4.57–5.70) | 0.9625 (0.9475–1.0350) | 111.1 (107.4–115.6) |
| 10gbit | previous | 703.8 (690.0–714.4) | 9.13 (8.71–9.33) | 4.2625 (4.1550–4.4200) | 155.2 (152.7–155.6) |
| 10gbit | grpc | 713.8 (689.7–719.3) | 8.87 (8.45–9.29) | 4.3375 (4.1875–4.3550) | 155.0 (154.5–156.6) |

## Per-run measurements

| Profile | Iteration | Label | Window | Throughput MB/s | P99 ms | CPU s/GiB | Timed peak RSS MiB |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |
| 1gbit | 1 | orpc | compiled default (not set in config) | 119.4 | 66.82 | 1.2000 | 112.4 |
| 1gbit | 1 | previous | compiled default (not set in config) | 118.2 | 104.70 | 3.5100 | 145.5 |
| 1gbit | 1 | grpc | compiled default (not set in config) | 119.0 | 99.53 | 3.4075 | 148.0 |
| 10gbit | 1 | orpc | compiled default (not set in config) | 1157.0 | 5.70 | 0.9625 | 107.4 |
| 10gbit | 1 | previous | compiled default (not set in config) | 710.6 | 9.13 | 4.2975 | 154.2 |
| 10gbit | 1 | grpc | compiled default (not set in config) | 689.7 | 8.87 | 4.3475 | 154.8 |
| 10gbit | 2 | grpc | compiled default (not set in config) | 713.8 | 9.29 | 4.3550 | 155.2 |
| 10gbit | 2 | previous | compiled default (not set in config) | 703.0 | 8.86 | 4.2575 | 155.2 |
| 10gbit | 2 | orpc | compiled default (not set in config) | 1159.8 | 5.54 | 0.9700 | 114.4 |
| 1gbit | 2 | grpc | compiled default (not set in config) | 118.9 | 103.49 | 3.4300 | 147.3 |
| 1gbit | 2 | previous | compiled default (not set in config) | 118.3 | 90.03 | 3.4450 | 145.5 |
| 1gbit | 2 | orpc | compiled default (not set in config) | 119.4 | 76.65 | 1.1975 | 115.3 |
| 1gbit | 3 | orpc | compiled default (not set in config) | 119.5 | 83.47 | 1.1425 | 115.2 |
| 1gbit | 3 | previous | compiled default (not set in config) | 119.0 | 74.37 | 3.3550 | 151.9 |
| 1gbit | 3 | grpc | compiled default (not set in config) | 119.0 | 97.60 | 3.3625 | 148.4 |
| 10gbit | 3 | orpc | compiled default (not set in config) | 1178.0 | 4.75 | 0.9475 | 111.1 |
| 10gbit | 3 | previous | compiled default (not set in config) | 714.4 | 9.14 | 4.2625 | 155.3 |
| 10gbit | 3 | grpc | compiled default (not set in config) | 708.9 | 8.45 | 4.1875 | 155.0 |
| 10gbit | 4 | grpc | compiled default (not set in config) | 719.3 | 8.73 | 4.3375 | 154.5 |
| 10gbit | 4 | previous | compiled default (not set in config) | 703.8 | 8.71 | 4.1550 | 152.7 |
| 10gbit | 4 | orpc | compiled default (not set in config) | 1169.6 | 5.06 | 0.9600 | 115.6 |
| 1gbit | 4 | grpc | compiled default (not set in config) | 118.9 | 106.28 | 3.4375 | 145.7 |
| 1gbit | 4 | previous | compiled default (not set in config) | 119.0 | 84.32 | 3.3575 | 149.4 |
| 1gbit | 4 | orpc | compiled default (not set in config) | 119.4 | 76.11 | 1.1400 | 110.9 |
| 1gbit | 5 | orpc | compiled default (not set in config) | 119.4 | 81.52 | 1.1300 | 113.1 |
| 1gbit | 5 | previous | compiled default (not set in config) | 119.1 | 79.34 | 3.4050 | 148.9 |
| 1gbit | 5 | grpc | compiled default (not set in config) | 119.0 | 71.24 | 3.3275 | 149.9 |
| 10gbit | 5 | orpc | compiled default (not set in config) | 1175.1 | 4.57 | 1.0350 | 110.7 |
| 10gbit | 5 | previous | compiled default (not set in config) | 690.0 | 9.33 | 4.4200 | 155.6 |
| 10gbit | 5 | grpc | compiled default (not set in config) | 719.0 | 8.87 | 4.2625 | 156.6 |

The executor was reverted in `abe82490` after this experiment: CPU did not improve by 5%, and 1 Gbit/s P99 regressed beyond the retention limit. See the separate [pre-experiment CPU profile](profile-summary.md).

## Evidence

- [results.json](results.json): every per-run value, median and range, recorded comparator outputs, independent gate checks and archive digest.
- [provenance.json](provenance.json): revisions, binary checksums, server versions, workload hash, configured windows and executed script hashes.
- [raw-measurements.json.gz](raw-measurements.json.gz): all result latency samples, before/after server CPU snapshots, qdisc counters, memory and host samples, run order, and exact executed driver and build scripts. The scripts contain local workspace paths. No raw configuration, dataset, logs, perf data or stack profiles are included.
- [Harness instructions](../../README.md): measurement boundaries and comparator method.

## Executor validation before measurement

| Suite at `d778981c` | Passed | Failed |
| --- | ---: | ---: |
| `curvine-worker --lib read_executor` | 4 | 0 |
| `curvine-worker --lib grpc_data::tests` | 6 | 0 |

These release-mode checks cover bounded admission, FIFO ordering, prompt queued cancellation, active-read cleanup, shutdown drain/join and panic isolation. They validate the measured experiment; its production code was subsequently removed because the retention criteria failed.
