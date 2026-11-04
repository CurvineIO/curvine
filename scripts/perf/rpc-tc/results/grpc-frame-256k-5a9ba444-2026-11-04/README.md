# 256 KiB block response frames

This checkpoint changes only `GrpcBlockClient` to advertise a 256 KiB HTTP/2 receive-frame limit. Candidate `5a9ba44494216e89be151c561c8afa0844e1919d` is compared with its tonic 0.14.9 parent `887c2584a59f7207e4d14f0425c31d71650842ba` and upstream ORPC `c31606c9e08c0e30639409ab28db69a9158f758b`. The control binaries are exact snapshots from the preceding upgrade experiment, but every timing trial here is fresh. The read window stays at one.

**Decision: retain the 256 KiB limit.** Versus the upgrade-only control, CPU/GiB falls 24.2% at 1 Gbit/s and 42.2% at 10 Gbit/s. Throughput changes +0.3% / +29.8%, and P99 changes -3.2% / -18.5%. The 10 Gbit/s candidate reaches 998.7 MB/s at 2.1125 CPU s/GiB. Sampled combined peak RSS medians change from 130.2 to 131.9 MiB at 1 Gbit/s and from 134.3 to 132.1 MiB at 10 Gbit/s. All preregistered retention conditions pass.

**The migration gate remains open.** At 10 Gbit/s, throughput and P99 now pass the provisional ORPC thresholds, but CPU is 215.6% of ORPC against a 115% limit. At 1 Gbit/s, CPU is 171.5% and P99 is 115.9% of ORPC, so both fail their limits. These results do not imply maintainer acceptance or representative 25/100 GbE validation.

The real-storage read/seek integration, scoped strict Clippy and formatting passed before the candidate was committed. A separate 128 MiB read with the new client and tonic 0.9 server also passed. [Packet diagnostics](frame-diagnostics/README.md) verify the advertised limits and actual DATA frames; capture runs are excluded from timing medians. The [independent audit](audit.json) replayed all four comparisons, all 30 raw trials and both packet captures.

The two experiments have separate fresh controls. Do not add their percentage changes or compare absolute CPU numbers across experiments as a matched cumulative result. See the [upgrade-only report](../tonic-0149-887c2584-2026-11-04/README.md) for the preceding step.

Each profile has five independent 4 GiB runs per label. The controller used Linux network namespaces and tc with 1 ms delay in each direction. The configured read window was omitted; the previous and candidate gRPC builds declare a compiled default of one.

| Label | Transport | RPC tonic | Revision |
| --- | --- | --- | --- |
| orpc | orpc | n/a | `c31606c9e08c0e30639409ab28db69a9158f758b` |
| previous | grpc | 0.14.9 | `887c2584a59f7207e4d14f0425c31d71650842ba` |
| grpc | grpc | 0.14.9 | `5a9ba44494216e89be151c561c8afa0844e1919d` |

Ratios use medians of five runs; per-run P99 uses nearest rank. The production-retention and ORPC gates below are separate. The checkpoint comparator’s generic `provisional_pass` uses the ORPC thresholds and is not the production-retention decision.

| Profile | Comparison | Throughput ratio | P99 ratio | Total CPU ratio | Gate |
| --- | --- | ---: | ---: | ---: | --- |
| 1gbit | Candidate / previous | 100.33% | 96.76% | 75.76% | PASS |
| 1gbit | Candidate / ORPC | 99.97% | 115.89% | 171.54% | FAIL |
| 10gbit | Candidate / previous | 129.75% | 81.49% | 57.76% | PASS |
| 10gbit | Candidate / ORPC | 86.20% | 105.68% | 215.56% | FAIL |

Production retention requires throughput ≥95%, P99 ≤105%, and total CPU ≤95% of the previous gRPC control in both profiles. The provisional ORPC gate requires throughput ≥85%, P99 ≤110%, and total CPU ≤115% in both profiles.

Both-profile production retention: **PASS**. Both-profile ORPC gate: **FAIL**.

## Per-profile variation and CPU endpoints

CPU totals are medians of per-run sums; they need not equal the sum of the endpoint medians. Ranges show minimum–maximum across the five runs. Throughput is MB/s; CPU is seconds per GiB. Timed RSS is the sampled combined peak of master, worker, and client, excluding page cache and the supervisor.

| Profile | Label | Throughput median (range) | P99 ms median (range) | Client CPU median (range) | Server CPU median (range) | Total CPU median (range) | Timed RSS MiB median (range) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1gbit | orpc | 119.4 (119.3–119.4) | 72.47 (62.20–82.21) | 0.9550 (0.9000–0.9800) | 0.2750 (0.2725–0.2825) | 1.2300 (1.1775–1.2600) | 110.3 (109.4–117.1) |
| 1gbit | previous | 119.0 (118.0–119.1) | 86.80 (61.71–98.07) | 1.4250 (1.4050–1.4975) | 1.3600 (1.3275–1.4250) | 2.7850 (2.7325–2.9225) | 130.2 (129.8–130.6) |
| 1gbit | grpc | 119.3 (118.5–119.4) | 83.98 (70.42–101.84) | 1.0100 (0.9700–1.0250) | 1.1050 (1.0900–1.1825) | 2.1100 (2.0700–2.2075) | 131.9 (131.2–133.6) |
| 10gbit | orpc | 1158.6 (1137.6–1170.1) | 6.07 (5.24–6.44) | 0.7675 (0.7525–0.8375) | 0.2100 (0.2025–0.2125) | 0.9800 (0.9550–1.0475) | 110.5 (107.6–114.5) |
| 10gbit | previous | 769.7 (743.6–800.2) | 7.87 (7.27–8.39) | 1.6675 (1.5825–1.7375) | 1.9900 (1.9550–2.0225) | 3.6575 (3.5375–3.7350) | 134.3 (132.8–136.7) |
| 10gbit | grpc | 998.7 (979.6–1010.9) | 6.42 (5.88–6.74) | 0.9450 (0.9150–0.9700) | 1.1900 (1.1550–1.2075) | 2.1125 (2.0800–2.1775) | 132.1 (131.2–133.9) |

## Individual trials

| Profile | Iteration | Label | MB/s | P99 ms | Client CPU s/GiB | Server CPU s/GiB | Total CPU s/GiB | Timed RSS MiB |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1gbit | 1 | orpc | 119.4 | 66.92 | 0.9175 | 0.2725 | 1.1900 | 109.6 |
| 1gbit | 1 | previous | 118.8 | 98.07 | 1.4075 | 1.3375 | 2.7450 | 130.2 |
| 1gbit | 1 | grpc | 119.4 | 70.42 | 0.9700 | 1.1000 | 2.0700 | 131.2 |
| 10gbit | 1 | orpc | 1159.5 | 5.53 | 0.7675 | 0.2075 | 0.9750 | 108.3 |
| 10gbit | 1 | previous | 764.7 | 7.87 | 1.5825 | 1.9550 | 3.5375 | 136.7 |
| 10gbit | 1 | grpc | 1000.1 | 6.74 | 0.9450 | 1.1675 | 2.1125 | 131.8 |
| 10gbit | 2 | grpc | 984.6 | 6.42 | 0.9700 | 1.2075 | 2.1775 | 131.2 |
| 10gbit | 2 | previous | 800.2 | 7.27 | 1.6200 | 1.9550 | 3.5750 | 134.3 |
| 10gbit | 2 | orpc | 1170.1 | 5.24 | 0.7525 | 0.2025 | 0.9550 | 110.6 |
| 1gbit | 2 | grpc | 119.2 | 80.64 | 0.9725 | 1.1050 | 2.0775 | 133.6 |
| 1gbit | 2 | previous | 119.0 | 88.30 | 1.4425 | 1.3800 | 2.8225 | 130.2 |
| 1gbit | 2 | orpc | 119.4 | 82.21 | 0.9800 | 0.2725 | 1.2525 | 110.3 |
| 1gbit | 3 | orpc | 119.4 | 73.55 | 0.9550 | 0.2750 | 1.2300 | 109.4 |
| 1gbit | 3 | previous | 119.1 | 83.22 | 1.4250 | 1.3600 | 2.7850 | 129.8 |
| 1gbit | 3 | grpc | 118.5 | 101.84 | 1.0250 | 1.1825 | 2.2075 | 131.9 |
| 10gbit | 3 | orpc | 1137.6 | 6.44 | 0.8200 | 0.2125 | 1.0325 | 110.5 |
| 10gbit | 3 | previous | 769.7 | 7.91 | 1.7375 | 1.9975 | 3.7350 | 132.8 |
| 10gbit | 3 | grpc | 1010.9 | 5.88 | 0.9250 | 1.1550 | 2.0800 | 132.6 |
| 10gbit | 4 | grpc | 998.7 | 6.56 | 0.9150 | 1.1900 | 2.1050 | 132.1 |
| 10gbit | 4 | previous | 743.6 | 8.39 | 1.6900 | 2.0225 | 3.7125 | 133.6 |
| 10gbit | 4 | orpc | 1158.6 | 6.07 | 0.7675 | 0.2125 | 0.9800 | 107.6 |
| 1gbit | 4 | grpc | 119.3 | 83.98 | 1.0100 | 1.1525 | 2.1625 | 132.6 |
| 1gbit | 4 | previous | 118.0 | 61.71 | 1.4975 | 1.4250 | 2.9225 | 130.1 |
| 1gbit | 4 | orpc | 119.4 | 62.20 | 0.9775 | 0.2825 | 1.2600 | 114.3 |
| 1gbit | 5 | orpc | 119.3 | 72.47 | 0.9000 | 0.2775 | 1.1775 | 117.1 |
| 1gbit | 5 | previous | 119.1 | 86.80 | 1.4050 | 1.3275 | 2.7325 | 130.6 |
| 1gbit | 5 | grpc | 119.4 | 92.80 | 1.0200 | 1.0900 | 2.1100 | 131.9 |
| 10gbit | 5 | orpc | 1152.7 | 6.39 | 0.8375 | 0.2100 | 1.0475 | 114.5 |
| 10gbit | 5 | previous | 786.0 | 7.78 | 1.6675 | 1.9900 | 3.6575 | 134.9 |
| 10gbit | 5 | grpc | 979.6 | 6.34 | 0.9500 | 1.2050 | 2.1550 | 133.9 |

Five-run ranges and host samples help show variability, but they do not eliminate host noise. Linux tc is a provisional approximation and does not establish results on representative physical networks. Dependency-maintenance or security benefits of the upgrade must be evaluated separately from these performance gates.

[results.json](results.json) contains computed values and gate checks; [provenance.json](provenance.json) records build identities and hashes; [raw-measurements.json.gz](raw-measurements.json.gz) contains latency samples, process and qdisc records, host/RSS samples, criteria, and exact scripts. Scripts and build metadata can contain local workspace paths.
