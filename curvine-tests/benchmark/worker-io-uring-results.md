# Worker io_uring benchmark results (#791)

**Keep the backend default-off.** In this two-pair Linux loopback comparison,
write throughput regressed by 50–66%. The standalone throughput read phase was
17–20% slower; mixed read throughput fell with the mixed workload's longer
wall time. All enabled trials recorded completed ring reads and writes, so
these are not fallback measurements.

## Reproduce

From the repository root, build both binaries with normal Curvine build
prerequisites. Use one build for both arms:

```sh
cargo build --locked -p curvine-server -p curvine-cli -j 4
python3 curvine-tests/benchmark/worker-io-uring-local.py \
  --server target/debug/curvine-server --cli target/debug/curvine-cli \
  --output target/worker-io-uring-791 --repetitions 2 \
  --files 64 --file-mib 4 --mixed-mib 512 --mixed-seconds 30
```

Use a fresh output directory. The [runner](worker-io-uring-local.py) saves the
exact `cv bench` or `cv bench prefill` arguments, cluster TOML, raw JSON, logs,
Prometheus snapshots, and worker CPU samples. See the
[operator guide](worker-io-uring.md) for rollout, rollback, and a manual
separate-client recipe.

- **Build:** working tree for `feat/791-worker-io-uring` on base `bac29dd0`,
  Rust `1.98.1`, Cargo debug profile. The measured server SHA-256 was
  `58e113a73a8a0d6d1da74f4cca11a8a387729bd6c6e3145447799cae37d8e2c9`; CLI
  SHA-256 was `31d36c5e7825faefb8fb7940121fbdeb5954b19d05e6f934fc02a3a160a74b2e`.
- **Host:** x86-64 KVM VM with 16 AMD EPYC-Rome vCPUs, about 15.6 GiB RAM,
  Ubuntu Linux `7.0.0-38-generic`, ext4 on `/dev/vda1`
  (`rw,relatime,discard,errors=remount-ro,commit=30`). The worker could create
  and submit to a ring (`kernel.io_uring_disabled=0`). No host policy changed.
- **Topology and config:** a fresh single master and single worker on localhost
  for every arm, one file-backed `[DISK]` directory and one replica. Both arms
  use 1 MiB block/read/write chunks, client `short_circuit = false`, worker
  `enable_send_file = false` and `enable_splice = false`, with worker I/O and
  blocking thread counts of 4 and 16. Only `worker.enable_io_uring` differs
  within a pair. Pair 1 runs off then on; pair 2 reverses the order.
- **Workloads:** `throughput` writes 64 × 4 MiB then reads them back;
  `mixed_throughput` pre-fills the same 256 MiB, then measures a 50:50
  read/write mix by operation weight (512 MiB combined limit or 30 s).
  At 16 threads the mixed workload completed 572 MiB per run because in-flight
  operations may overshoot. It completed 512 MiB at one thread. There was
  no cache eviction or forced writeback between these phases. This is a
  warm-page-cache recipe, not a claim of full residency or raw disk throughput.

## Throughput and latency

Throughput is the mean of the two trials, with the observed range in brackets.
Latency is p95/p99 in milliseconds, averaging the two CLI-reported whole-file
quantiles, **not** a pooled percentile or kernel I/O latency. Sample counts are
63–74 files per operation per run; the tails are noisy, especially at 16
threads. All 16 arms exited successfully with zero benchmark operation errors.

| Workload | Client threads | Operation | Off MiB/s [range] | On MiB/s [range] | Change | Off p95/p99 ms | On p95/p99 ms |
|---|---:|---|---:|---:|---:|---:|---:|
| Throughput | 1 | Write | 204.3 [144.4–264.1] | 92.8 [89.1–96.5] | −54.6% | 33.6/33.6 | 60.7/60.7 |
| Throughput | 1 | Read | 539.8 [491.8–587.9] | 431.9 [395.3–468.6] | −20.0% | 12.3/13.6 | 17.9/17.9 |
| Throughput | 16 | Write | 1277.7 [1159.3–1396.0] | 435.9 [435.8–436.1] | −65.9% | 84.5/88.0 | 183.9/183.9 |
| Throughput | 16 | Read | 1865.6 [1518.7–2212.4] | 1552.1 [1435.9–1668.2] | −16.8% | 66.1/66.1 | 69.3/69.3 |
| Mixed | 1 | Write | 150.7 [131.7–169.6] | 75.0 [71.8–78.1] | −50.3% | 26.2/30.4 | 62.4/62.4 |
| Mixed | 1 | Read | 146.0 [127.7–164.4] | 72.6 [69.6–75.7] | −50.3% | 12.3/12.3 | 15.1/15.1 |
| Mixed | 16 | Write | 900.8 [900.7–900.9] | 390.7 [385.3–396.1] | −56.6% | 61.7/61.7 | 177.4/177.4 |
| Mixed | 16 | Read | 840.0 [839.9–840.1] | 364.3 [359.3–369.3] | −56.6% | 45.4/45.4 | 43.0/43.0 |

Mixed read and write rates share the same workload timer. The lower mixed read
rate does not mean each read took proportionally longer: at 16 threads the
reported read tail was slightly lower, while write latency and the overall
runtime increased.

## Worker CPU and ring verification

CPU is user + system **process CPU seconds** from `/proc/<pid>/stat`, measured
across the whole benchmark CLI command. It excludes startup, separate mixed
prefill, and teardown, and does not include work charged to separate kernel
threads. Wall time and CPU seconds below are the averages of the two trials.

| Workload | Client threads | Off wall s | On wall s | Off CPU s (user + system) | On CPU s (user + system) |
|---|---:|---:|---:|---:|---:|
| Throughput | 1 | 1.922 | 3.424 | 1.060 (0.335 + 0.725) | 1.175 (0.570 + 0.605) |
| Throughput | 16 | 0.417 | 0.823 | 1.065 (0.355 + 0.710) | 1.665 (0.630 + 1.035) |
| Mixed | 1 | 1.795 | 3.524 | 0.960 (0.370 + 0.590) | 1.245 (0.595 + 0.650) |
| Mixed | 16 | 0.417 | 0.819 | 1.225 (0.420 + 0.805) | 1.875 (0.790 + 1.085) |

The `worker_io_uring_read_bytes` and `worker_io_uring_write_bytes` counters
were zero in every disabled arm and matched measured transfer bytes in every
enabled arm: **256/256 MiB** (read/write) for both throughput concurrency
levels, **252/260 MiB** for mixed at one thread, and **276/296 MiB** for mixed
at 16 threads. These deltas exclude startup probing and mixed prefill. They
count successful ring chunks, not committed or durable bytes.

This is an **end-to-end feature comparison**, not a syscall-only microbenchmark.
It includes the executor handoff and the ring writer's extra checkpoint syncs.
The results are consistent with that overhead but do not isolate its share.
The co-located debug setup, short runs, and VM filesystem do not establish
production throughput, cold-disk behavior, or production tail latency. Re-run
on representative hardware before considering wider rollout.
