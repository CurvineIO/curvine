# Fresh staging-file prototype (#791)

**The prototype eliminates one seek and one EOF read per fresh block. The worker RPC benchmark did not establish a throughput improvement.** Keep this result separate from the [owned TCP buffer experiment](worker-io-uring-owned-writes.md): both ring builds here share the payload.

On its first nonempty write, a newly allocated staged session with no checkpoint can initialize its private scratch file without copying the canonical placeholder. The worker must hold the fresh allocation's exclusive lease; the writer checks that the pinned source is an empty regular file. A nonempty source, an overwrite of a visible prefix, and a write after a checkpoint still use the existing checked copy when forking the inode. No change to admission, completion, read visibility, or durable `Flush`.

## What changed on the syscall path?

On the VM, `strace -ff -yy -e trace=lseek,read` on a separate 64-block smoke run counted only rewinds to zero and reads returning EOF on canonical `staging/blk_*` descriptors. The baseline made 64 of each; the prototype made zero of each. All 64 blocks verified in each build. No traced run was used for timing. The avoided EOF read is not a payload read from the SSD.

## Matched release builds

The baseline was `f9d8fc7b`; the prototype used the same checkout, build settings, and Rust 1.98.1. The same two optimized Linux x86-64 executables ran on the [previous VM and local NVMe hosts](worker-io-uring-owned-writes.md#reproduce-on-linux-with-ext4-and-working-io_uring). This is the **worker TCP write** benchmark, not an end-to-end `cv bench` test.

Each build had one 32-block warmup. Six measured pairs alternated build order (baseline first in three, prototype first in three). Each invocation used the existing three-backend harness: 512 fresh 1 MiB blocks **per trial**, one payload and buffered `Complete` per block, at 1 and 16 clients. All six trials per invocation verified finalized bytes, completed ring bytes and publication mode; `io_uring` also verified that all 512 MiB used shared payloads. No changes to system cache or device settings. The table gives median MiB/s across six trials per cell; wins count paired trials, not individual RPCs.

| Backend | Clients | VM baseline → prototype | VM wins | NVMe baseline → prototype | NVMe wins |
|---|---:|---:|---:|---:|---:|
| `LocalFile` (unchanged) | 1 | 908.8 → 923.9 | 5/6 | 1,946.9 → 2,108.6 | 4/6 |
| `LocalFile` (unchanged) | 16 | 3,428.3 → 3,383.1 | 2/6 | 3,487.4 → 2,993.5 | 2/6 |
| Staged `pwrite` | 1 | 865.6 → 891.5 | 4/6 | 1,951.0 → 2,053.8 | 4/6 |
| Staged `pwrite` | 16 | 2,788.6 → 2,910.9 | 4/6 | 3,432.8 → 2,920.7 | 3/6 |
| Staged ring | 1 | 789.6 → 796.1 | 3/6 | 1,912.8 → 1,991.9 | 4/6 |
| Staged ring | 16 | 2,305.6 → 2,292.6 | 1/6 | 3,033.7 → 2,240.2 | 2/6 |

At 16 clients the ring median fell 0.6% on the VM and 26.2% on NVMe. The unchanged NVMe `LocalFile` also fell 14.2%, so the trials cannot attribute the full ring difference to the code; they also cannot rule out a regression. Median NVMe worker CPU seconds per 512 MiB for the ring were 0.245 → 0.225 (one client) and 0.515 → 0.510 (16 clients), with 10 ms CPU tick resolution. Do not promote this prototype as a performance win based on these results.

## Repeat the comparison

Save a copy of the release worker test executable from each build (`cargo test --locked --release -j 4 -p curvine-worker --lib --no-run` prints the path). From an existing writable ext4 directory, run each executable with the following environment and libtest arguments, alternating build order and saving stdout for each invocation:

```bash
CURVINE_WRITE_BENCH_DIR="$PWD" CURVINE_WRITE_BENCH_BLOCKS=512 \
  CURVINE_WRITE_BENCH_REPS=1 CURVINE_WRITE_BENCH_VERIFY=1 \
  CURVINE_WRITE_BENCH_EXPLICIT_FLUSH=0 CURVINE_WRITE_BENCH_COMPARE_COPY=0 \
  "$WORKER_TEST_BINARY" worker::worker_server::write_benchmark::worker_write_ablation \
  --ignored --exact --nocapture
```

SHA-256 of the measured executables: baseline `8ec39711846a8f33373e07cfdea5628738d65d1cb6f7547215799bcb7fc810c9`; prototype `83fd2226ce8bbe9e9794bbaeffc763ccce86e9e87ca2be2f3037aab04c863d97`.
