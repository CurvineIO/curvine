# Worker io_uring follow-up: buffered Complete and owned async reads (#791)

**The write regression was mostly in the intermediate durable checkpoint.** After
publishing the completed private inode directly on eligible `Complete` requests,
the 16-client staged ring median rose from 519 to 2,221 MiB/s between runs.
The matched new `LocalFile` median was 2,764 MiB/s. Owned async reads recovered
some blocking-ring overhead but did not beat `LocalFile` in these workloads.
Keep the backend default-off.

## Reproduce

Run on a Linux host with working `io_uring` on a writable ext4 directory. Both
benchmarks use fresh loopback worker subprocesses and real block RPCs; the master
and `cv bench` are not in the measurement. Use the same optimized test binary
for all arms. The blocking staged `pwrite` backend and the read dispatch override
are test-only.

```sh
CURVINE_WRITE_BENCH_BLOCKS=512 CURVINE_WRITE_BENCH_REPS=3 \
  CURVINE_WRITE_BENCH_EXPLICIT_FLUSH=0 \
  cargo test --locked --release -p curvine-worker --lib -j 6 \
  worker::worker_server::write_benchmark::worker_write_ablation \
  -- --ignored --exact --nocapture

CURVINE_READ_BENCH_BLOCKS=256 CURVINE_READ_BENCH_REPS=3 \
  CURVINE_READ_BENCH_COLD=1 \
  cargo test --locked --release -p curvine-worker --lib -j 6 \
  worker::worker_server::write_benchmark::read_benchmark::worker_read_ablation \
  -- --ignored --exact --nocapture
```

For the explicit-Flush smoke run, set `CURVINE_WRITE_BENCH_EXPLICIT_FLUSH=1`,
`CURVINE_WRITE_BENCH_BLOCKS=32`, and `CURVINE_WRITE_BENCH_REPS=1`. The default data
root is the repository's `target/`; set `CURVINE_WRITE_BENCH_DIR` to use another
ext4 directory. Capture `WORKER_WRITE_BENCH` and `WORKER_READ_BENCH` JSON lines
for per-trial counters and timing. The [write harness](../../curvine-worker/src/worker/write_benchmark.rs)
and [read harness](../../curvine-worker/src/worker/read_benchmark.rs) verify the
expected backend, payload, and dispatch or completion mode.

Build: Rust 1.98.1, release profile, working tree on `bac29dd0`. The worker test
executable SHA-256 was `4e29bda9013ed2246a3519b3708b30855284163a5a418cc520a74c9f40c1b662`.
Host: Linux `7.0.0-38-generic`, 16 AMD EPYC-Rome vCPUs (KVM), 15.6 GiB RAM,
ext4 on `/dev/vda1` (`rw,relatime,discard,errors=remount-ro,commit=30`). Worker:
four I/O threads and 16 blocking threads; client: four I/O threads. Checksum
verification, short-circuit, splice, read-ahead, and sendfile are bypassed for
the measured paths. Process CPU is user plus system time, excluding the client
and work charged to separate kernel tasks.

## Writes: fresh blocks without an explicit flush

Each of the 18 trials wrote 512 × 1 MiB with one `Running` payload and `Complete`
per block and verified all finalized bytes outside the timing window. Values
are medians over three rotated trials. Complete RPC p95 is the median of the
three per-trial p95s. The older numbers are from the [checkpoint-on-Complete
experiment](worker-io-uring-write-ablation.md), not interleaved with these trials.

| Clients | Backend | Earlier MiB/s | Now MiB/s [min–max] | Complete RPC p95 ms | Worker CPU s |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `LocalFile` | 814 | 849 [339–1,001] | 0.24 | 0.50 |
| 1 | Staged `pwrite` | 164 | 868 [813–939] | 0.26 | 0.50 |
| 1 | Staged `io_uring` | 156 | 647 [624–647] | 0.31 | 0.67 |
| 16 | `LocalFile` | 2,984 | 2,764 [2,511–2,878] | 2.90 | 0.94 |
| 16 | Staged `pwrite` | 516 | 2,597 [2,555–2,790] | 2.83 | 0.96 |
| 16 | Staged `io_uring` | 519 | 2,221 [2,180–2,270] | 1.79 | 1.06 |

All staged no-flush runs used the buffered fast path for every block and took
zero durable staging checkpoints. In the six explicit-Flush smoke trials, both
staged arms took the expected 32 checkpoints per trial and used zero fast
completions. The ring arms verified real completed ring bytes in both workloads.

A successful buffered `Complete` is a logical publication, not a power-loss
persistence guarantee. The explicit `Flush` and prior-checkpoint behavior have
not been relaxed. At 16 clients the ring is still about 14% slower than staged
`pwrite` with the same buffered completion behavior.

## Reads: blocking ring versus owned async ring

The 36 trials used pre-seeded, synchronized file blocks. Warm sequential reads
retrieved 256 MiB in 128 KiB chunks. The cold-advised random workload read one
4 KiB page at a repeatable random offset per block (1 MiB total); `DONTNEED` was
used only for the trial files. Worker `/proc` I/O reported zero backing reads
for warm runs and roughly 1 MiB per cold-advised trial. This does not establish
that the VM's backing device had a cold cache. The short cold trials and the
10 ms process CPU accounting granularity limit CPU comparisons.

Throughput includes `Open`, `Complete`, and client byte verification. The table
uses medians over three trials, with p95/p99 for `Running` read RPCs only. The
ring arms completed the expected ring bytes and the async arm dispatched all
measured `Running` frames asynchronously. No other arm used async dispatch.

| Workload | Clients | Backend | MiB/s | Running p95/p99 µs | Worker CPU s |
| --- | ---: | --- | ---: | ---: | ---: |
| Warm sequential | 1 | `LocalFile` | 765 | 179 / 224 | 0.25 |
| Warm sequential | 1 | Blocking ring | 531 | 270 / 361 | 0.40 |
| Warm sequential | 1 | Async ring | 625 | 205 / 314 | 0.34 |
| Warm sequential | 16 | `LocalFile` | 3,826 | 646 / 1,210 | 0.33 |
| Warm sequential | 16 | Blocking ring | 2,851 | 1,028 / 1,463 | 0.45 |
| Warm sequential | 16 | Async ring | 2,850 | 976 / 1,569 | 0.34 |
| Cold-advised random | 1 | `LocalFile` | 12.77 | 225 / 281 | 0.04 |
| Cold-advised random | 1 | Blocking ring | 10.36 | 284 / 609 | 0.07 |
| Cold-advised random | 1 | Async ring | 9.79 | 289 / 434 | 0.06 |
| Cold-advised random | 16 | `LocalFile` | 67.76 | 583 / 1,094 | 0.06 |
| Cold-advised random | 16 | Blocking ring | 61.92 | 697 / 1,148 | 0.07 |
| Cold-advised random | 16 | Async ring | 52.75 | 813 / 1,414 | 0.06 |

These results justify removing the unnecessary checkpoint on buffered writes,
but not enabling the ring by default or changing the TCP runtime. In particular,
the one-client warm read improvement over the blocking ring did not translate
to a speedup over `LocalFile`.
