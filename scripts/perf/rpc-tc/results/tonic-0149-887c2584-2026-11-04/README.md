# Tonic 0.14.9 upgrade

The upgrade-only checkpoint is `887c2584a59f7207e4d14f0425c31d71650842ba`, compared with tonic 0.9.2 at `6a6654cdfeb600e4fa3bd618ae9f131b4344c888` and upstream ORPC at `c31606c9e08c0e30639409ab28db69a9158f758b`. All include the same upstream changes. Tonic moves to 0.14.9, prost to 0.14.4, and Hyper to 1.12.0; Raft keeps its prost 0.11 storage types behind explicit wire adapters. This experiment keeps the transport settings and compiled read window at one.

**Decision: retain the upgrade.** CPU/GiB falls 21.4% at 1 Gbit/s and 14.9% at 10 Gbit/s versus fresh tonic 0.9 controls. Throughput changes -0.2% / +12.4%, and P99 changes -5.8% / -11.1%. Sampled combined peak RSS medians fall from 146.0 to 127.2 MiB and from 154.1 to 130.7 MiB respectively. All preregistered retention conditions pass. **The ORPC migration gate still fails**; these results do not provide maintainer acceptance or representative 25/100 GbE validation.

Validation covered 275 targeted test executions, including fragmented/coalesced protobuf Bytes ownership, Raft wire and persistence compatibility, plaintext and mTLS three-process recovery, storage-backed read/write paths and security. Strict all-target Clippy, formatting and both dependency guards pass. Two additional 128 MiB mixed-version checks passed (old client/new server and new client/old server); they are compatibility checks, excluded from performance medians. [validation-summary.json](validation-summary.json) records the counts. The [independent audit](audit.json) replayed all four comparisons from archived evidence.

Each profile has five independent 4 GiB runs per label. The controller used Linux network namespaces and tc with 1 ms delay in each direction. The configured read window was omitted; the previous and candidate gRPC builds declare a compiled default of one.

| Label | Transport | RPC tonic | Revision |
| --- | --- | --- | --- |
| orpc | orpc | n/a | `c31606c9e08c0e30639409ab28db69a9158f758b` |
| previous | grpc | 0.9.2 | `6a6654cdfeb600e4fa3bd618ae9f131b4344c888` |
| grpc | grpc | 0.14.9 | `887c2584a59f7207e4d14f0425c31d71650842ba` |

Ratios use medians of five runs; per-run P99 uses nearest rank. The production-retention and ORPC gates below are separate. The checkpoint comparator’s generic `provisional_pass` uses the ORPC thresholds and is not the production-retention decision.

| Profile | Comparison | Throughput ratio | P99 ratio | Total CPU ratio | Gate |
| --- | --- | ---: | ---: | ---: | --- |
| 1gbit | Candidate / previous | 99.81% | 94.15% | 78.62% | PASS |
| 1gbit | Candidate / ORPC | 99.53% | 112.93% | 246.51% | FAIL |
| 10gbit | Candidate / previous | 112.37% | 88.92% | 85.09% | PASS |
| 10gbit | Candidate / ORPC | 65.53% | 132.92% | 367.05% | FAIL |

Production retention requires throughput ≥95%, P99 ≤105%, and total CPU ≤95% of the previous gRPC control in both profiles. The provisional ORPC gate requires throughput ≥85%, P99 ≤110%, and total CPU ≤115% in both profiles.

Both-profile production retention: **PASS**. Both-profile ORPC gate: **FAIL**.

## Per-profile variation and CPU endpoints

Ranges show minimum–maximum across the five runs. Throughput is MB/s; CPU is seconds per GiB. Timed RSS is the sampled combined peak of master, worker, and client, excluding page cache and the supervisor.

| Profile | Label | Throughput median (range) | P99 ms median (range) | Client CPU median (range) | Server CPU median (range) | Total CPU median (range) | Timed RSS MiB median (range) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1gbit | orpc | 119.4 (118.4–119.4) | 75.63 (70.44–113.76) | 1.0050 (0.9700–1.0475) | 0.3050 (0.2800–0.3150) | 1.2900 (1.2525–1.3550) | 112.3 (110.1–118.0) |
| 1gbit | previous | 119.0 (118.9–119.1) | 90.72 (77.59–94.55) | 2.0050 (1.8375–2.0875) | 2.0000 (1.7925–2.0575) | 4.0450 (3.6300–4.1275) | 146.0 (145.2–149.4) |
| 1gbit | grpc | 118.8 (117.9–119.0) | 85.41 (72.32–92.18) | 1.5450 (1.4775–1.6300) | 1.6150 (1.4050–1.6400) | 3.1800 (2.8825–3.2450) | 127.2 (125.7–129.0) |
| 10gbit | orpc | 1140.5 (1123.9–1168.4) | 6.22 (5.09–6.65) | 0.8675 (0.7575–0.8900) | 0.2200 (0.2050–0.2325) | 1.1000 (0.9625–1.1100) | 111.6 (110.0–112.9) |
| 10gbit | previous | 665.1 (656.9–720.6) | 9.29 (8.71–9.70) | 2.1225 (1.9075–2.1450) | 2.6025 (2.3450–2.7025) | 4.7450 (4.2525–4.8475) | 154.1 (151.9–155.6) |
| 10gbit | grpc | 747.4 (733.2–771.1) | 8.26 (7.79–9.19) | 1.8300 (1.6750–1.9150) | 2.2075 (2.0125–2.3150) | 4.0375 (3.6875–4.2300) | 130.7 (130.3–134.9) |

## Individual trials

| Profile | Iteration | Label | MB/s | P99 ms | Client CPU s/GiB | Server CPU s/GiB | Total CPU s/GiB | Timed RSS MiB |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1gbit | 1 | orpc | 119.4 | 109.46 | 1.0475 | 0.3050 | 1.3525 | 114.7 |
| 1gbit | 1 | previous | 119.0 | 90.72 | 2.0700 | 2.0575 | 4.1275 | 145.2 |
| 1gbit | 1 | grpc | 118.8 | 72.59 | 1.6300 | 1.6150 | 3.2450 | 125.7 |
| 10gbit | 1 | orpc | 1123.9 | 6.17 | 0.8900 | 0.2200 | 1.1100 | 110.4 |
| 10gbit | 1 | previous | 665.1 | 9.29 | 2.1425 | 2.6025 | 4.7450 | 154.6 |
| 10gbit | 1 | grpc | 764.4 | 8.26 | 1.8300 | 2.2075 | 4.0375 | 130.5 |
| 10gbit | 2 | grpc | 734.4 | 8.38 | 1.7625 | 2.1225 | 3.8850 | 130.7 |
| 10gbit | 2 | previous | 676.5 | 9.52 | 2.1075 | 2.5025 | 4.6100 | 155.6 |
| 10gbit | 2 | orpc | 1143.1 | 6.31 | 0.8675 | 0.2325 | 1.1000 | 112.9 |
| 1gbit | 2 | grpc | 119.0 | 72.32 | 1.5850 | 1.6400 | 3.2250 | 127.2 |
| 1gbit | 2 | previous | 119.0 | 77.96 | 2.0875 | 2.0000 | 4.0875 | 145.9 |
| 1gbit | 2 | orpc | 118.5 | 113.76 | 1.0400 | 0.3150 | 1.3550 | 110.1 |
| 1gbit | 3 | orpc | 118.4 | 75.63 | 0.9700 | 0.3125 | 1.2825 | 118.0 |
| 1gbit | 3 | previous | 119.1 | 92.73 | 2.0050 | 2.0400 | 4.0450 | 147.0 |
| 1gbit | 3 | grpc | 119.0 | 92.18 | 1.5275 | 1.5525 | 3.0800 | 126.6 |
| 10gbit | 3 | orpc | 1140.5 | 6.22 | 0.8900 | 0.2100 | 1.1000 | 111.6 |
| 10gbit | 3 | previous | 656.9 | 9.70 | 2.1450 | 2.7025 | 4.8475 | 153.2 |
| 10gbit | 3 | grpc | 733.2 | 9.19 | 1.8700 | 2.2675 | 4.1375 | 134.9 |
| 10gbit | 4 | grpc | 747.4 | 8.13 | 1.9150 | 2.3150 | 4.2300 | 132.2 |
| 10gbit | 4 | previous | 661.7 | 9.20 | 2.1225 | 2.6525 | 4.7750 | 151.9 |
| 10gbit | 4 | orpc | 1168.4 | 5.09 | 0.8225 | 0.2200 | 1.0425 | 112.4 |
| 1gbit | 4 | grpc | 117.9 | 88.01 | 1.5450 | 1.6350 | 3.1800 | 127.2 |
| 1gbit | 4 | previous | 118.9 | 94.55 | 2.0000 | 1.9275 | 3.9275 | 146.0 |
| 1gbit | 4 | orpc | 119.4 | 70.44 | 0.9725 | 0.2800 | 1.2525 | 110.2 |
| 1gbit | 5 | orpc | 119.4 | 73.40 | 1.0050 | 0.2850 | 1.2900 | 112.3 |
| 1gbit | 5 | previous | 119.1 | 77.59 | 1.8375 | 1.7925 | 3.6300 | 149.4 |
| 1gbit | 5 | grpc | 118.4 | 85.41 | 1.4775 | 1.4050 | 2.8825 | 129.0 |
| 10gbit | 5 | orpc | 1130.9 | 6.65 | 0.7575 | 0.2050 | 0.9625 | 110.0 |
| 10gbit | 5 | previous | 720.6 | 8.71 | 1.9075 | 2.3450 | 4.2525 | 154.1 |
| 10gbit | 5 | grpc | 771.1 | 7.79 | 1.6750 | 2.0125 | 3.6875 | 130.3 |

Five-run ranges and host samples help show variability, but they do not eliminate host noise. Linux tc is a provisional approximation and does not establish results on representative physical networks. Dependency-maintenance or security benefits of the upgrade must be evaluated separately from these performance gates.

[results.json](results.json) contains computed values and gate checks; [provenance.json](provenance.json) records build identities and hashes; [raw-measurements.json.gz](raw-measurements.json.gz) contains latency samples, process and qdisc records, host/RSS samples, criteria, and exact scripts. Scripts and build metadata can contain local workspace paths.
