# Where 16-client staged writes spend time (#791)

**Look inside the buffered write submission/completion interval, not at permit acquisition or block publication.** On the local NVMe host, a 1 MiB ring write spent 3.04 ms between the first poll of its transfer and userspace observing completion. Enqueue to first poll averaged 9.9 µs; completion notification to the waiting writer averaged 8.5 µs. This does **not** mean the SSD took 3 ms: these are buffered writes and userspace timestamps.

## Experiment

Compared **staged `pwrite` vs staged `io_uring`**, not `LocalFile` vs `io_uring`. Both arms ran the same [fresh-staging prototype](worker-io-uring-fresh-staging.md) on top of `f9d8fc7b`, used the same staging, visibility and buffered publication code, and received the same TCP payloads. The shared ring submitted the received allocation; `pwrite` wrote directly from it on the blocking worker thread. No explicit `Flush`, checksums, splice, sendfile, or short circuit. This is a loopback worker benchmark, not `cv bench`, an `fsync` benchmark, or a physical-device latency test.

- One fresh worker per trial, 16 connections, 512 fresh 1 MiB blocks, one write and `Complete` per block. Four worker I/O threads, 16 blocking threads, four client I/O threads. Same release builds (Rust 1.98.1) on both hosts. Both used ext4: VM `/dev/vda1` (16 vCPUs, Linux `7.0.0-38-generic`); NVMe root `/dev/nvme0n1p2` (CT1000P3PSSD8, i5-13420H, 12 logical CPUs, Linux `7.2.15-76070215-generic`). No global cache drops or changes to kernel policy.
- Two builds: **control** compiled without the new clock probes or timestamp fields; **profile** compiled with `write-bench-profile` and armed after worker startup, before client readiness. The control retains the existing benchmark counters. One warmup pair per build (64 blocks). Four measured groups per host; each group ran both builds, each build ran one `pwrite`→ring pair and one ring→`pwrite` pair. Alternated which build ran first by group. No concurrent trials on a host.
- All 32 measured trials per host verified all finalized bytes outside the timing window. Per trial: 512 fast publications, zero checkpoints; exactly 512 MiB of ring and shared bytes in the ring arm, zero in `pwrite`. The [harness](../../curvine-worker/src/worker/write_benchmark.rs) requires exactly 512 samples per applicable stage and none in the control. The sum of publication samples matched the independent accumulated timer in every profiled trial.

## Throughput without the new probes

Median across eight trials per backend; brackets are the observed range. CPU is worker **process** user + system seconds per 512 MiB, not total host CPU. `Complete` p95 is the median of per-trial client RPC percentiles. Ring wins compare backends within each pair.

| Host | Staged `pwrite` MiB/s |   Staged ring MiB/s | Ring vs `pwrite` medians | Ring wins | CPU s, `pwrite` → ring | `Complete` p95 ms, `pwrite` → ring |
| ---- | --------------------: | ------------------: | -----------------------: | --------: | ---------------------: | ---------------------------------: |
| VM   |   2,866 [1,465–3,014] | 2,114 [1,322–2,278] |                   −26.2% |       0/8 |          0.960 → 0.880 |                      2.587 → 1.259 |
| NVMe |   3,614 [3,135–4,089] | 3,464 [2,460–3,690] |                    −4.1% |       2/8 |          0.625 → 0.505 |                      1.322 → 0.662 |

The NVMe ring used 19% less worker CPU but did not improve median throughput. The median **paired** rate change was −11.1% (range −23.3% to +10.2%); that statistic need not equal the −4.1% difference between the two medians. Don't extrapolate a fixed penalty from short runs on a shared root disk.

## Phase timings from the separate profile build

µs per block. Each column is the median of eight **per-trial** means or percentiles (nearest-rank p95); percentiles aren't pooled and columns aren't additive. The 16 clients' elapsed intervals overlap. `write_call` encloses the ring subphases; `publish` is a later RPC, not part of `write_call`.

| Backend and phase                           | NVMe mean | NVMe p50 | NVMe p95 | VM mean | VM p95 |
| ------------------------------------------- | --------: | -------: | -------: | ------: | -----: |
| `pwrite`: write callback                    |     1,261 |      414 |      770 |     548 |    800 |
| Ring: write callback                        |     3,063 |    3,099 |    5,485 |   5,203 |  7,062 |
| ↳ Acquire ring permit                       |      0.22 |     0.20 |     0.44 |    0.60 |   1.09 |
| ↳ Enqueue → first poll                      |       9.9 |      7.5 |     18.2 |    49.0 |  179.6 |
| ↳ First poll → transfer complete            |     3,042 |    3,077 |    5,436 |   5,053 |  6,966 |
| ↳ Reply ready → blocking caller receives it |       8.5 |      7.3 |     16.2 |    56.0 |  171.6 |
| `pwrite`: buffered publication              |       229 |      167 |      569 |     619 |  1,649 |
| Ring: buffered publication                  |        90 |       81 |      162 |     157 |    367 |

- `write_call` times the positional write callback only, excluding scratch-file preparation and pinning the read view. A few long NVMe `pwrite` calls raised its mean above its p95; the longest per trial ranged from 1.1 to 49.3 ms.
- [Executor probes](../../crates/adapters/curvine-storage-local/src/uring.rs) follow the same write across threads. Admission is the permit acquisition itself. Enqueue includes the channel handoff and waiting for the executor to first poll the I/O future. Transfer includes Compio submission/batching, buffered kernel work, and waiting for the executor to observe completion, including any short-write retries. Reply transit ends when the blocking caller receives success. Minor plumbing (e.g. channel setup and the byte observer) is outside these four intervals.
- For each trial, dividing total ring transfer time by total ring `write_call` time gives a median **99.3% on NVMe** (range 99.2–99.5%) and **97.9% on the VM** (96.3–98.5%). This is a share of the _write callback_, not of the whole benchmark. The same buffered finalization function was not slower in the ring arm; different arrival rates can change contention in shared code.
- A **separate, untimed-for-throughput** NVMe run used `strace -ff -qq --seccomp-bpf -T -e trace=io_uring_enter,io_uring_setup` on the control executable, one verified pair. Of 448 `io_uring_enter` calls requesting submissions, 386 requested one SQE, 57 requested two, and five requested three. Another 521 requested zero submissions with `min_complete=1`; this does not imply that every call slept. The 515 SQEs include startup/control operations, not just the 512 writes. This confirms some batching; the trace perturbs scheduling and cannot identify when each write's CQE became ready.

**Next experiment:** on a host with access to the io_uring submit/complete kernel tracepoints, correlate each write's kernel timestamps with userspace submission and completion observation. A long kernel interval points toward buffered-write or I/O-worker service; a CQE ready well before observation points toward executor wakeup or completion draining. The current measurements don't separate those cases. The ordinary benchmark accounts could not read the kernel tracepoint formats; no privileges or kernel settings were changed. Increasing the 64-operation admission cap or changing durability is not supported by this profile.

## Reproduce

Use the checkout containing these benchmark probes; they are not an operator setting. On Linux with working io_uring, from the repository root, set `CURVINE_WRITE_BENCH_DIR` to an **existing writable ext4 directory** with room for the 512 MiB trial and build artifacts as appropriate. A directory on an NVMe root partition is fine. The subshell keeps failure settings out of the interactive shell.

```bash
(
  set -euo pipefail
  : "${CURVINE_WRITE_BENCH_DIR:?set this to an existing ext4 directory}"
  root=$(realpath -- "$CURVINE_WRITE_BENCH_DIR")
  findmnt -T "$root" -o TARGET,SOURCE,FSTYPE
  run=$(mktemp -d "$root/write-stage-profile.XXXXXX")
  export CURVINE_WRITE_BENCH_DIR="$run" CURVINE_WRITE_BENCH_STAGE_PAIR=1
  export CURVINE_WRITE_BENCH_VERIFY=1 CURVINE_WRITE_BENCH_EXPLICIT_FLUSH=0 CURVINE_WRITE_BENCH_COMPARE_COPY=0
  unset CURVINE_WRITE_BENCH_CHILD_VARIANT
  bench=worker::worker_server::write_benchmark::worker_write_ablation
  build=(cargo test --locked --release -j 4 -p curvine-worker --lib)
  "${build[@]}" --no-run
  "${build[@]}" --features write-bench-profile --no-run
  echo "Worker profile logs: $run"
  run_pair() {
    local mode=$1 label=$2 blocks=$3 reps=$4 profile=0
    local -a flags=()
    if [[ $mode == profile ]]; then flags=(--features write-bench-profile); profile=1; fi
    CURVINE_WRITE_BENCH_PROFILE=$profile CURVINE_WRITE_BENCH_BLOCKS=$blocks CURVINE_WRITE_BENCH_REPS=$reps \
      "${build[@]}" "${flags[@]}" "$bench" -- --ignored --exact --nocapture >"$run/$label-$mode.log" 2>&1
  }
  for mode in control profile; do run_pair "$mode" warmup 64 1; done
  for group in 1 2 3 4; do
    if (( group % 2 )); then modes=(control profile); else modes=(profile control); fi
    for mode in "${modes[@]}"; do run_pair "$mode" "group-$group" 512 2; done
  done
  sed -n 's/^WORKER_WRITE_BENCH //p' "$run"/group-*.log > "$run/trials.jsonl"
  echo "Raw per-block nanosecond samples: $run/trials.jsonl (worker.stage_ns in profiled rows)"
)
```

For the reported runs, the two prebuilt executables were identical across hosts (SHA-256): control `2ecf22f9db05b7a9b37c54e3ef5a8f624a5357e8b5787f71447c0968fb541a70`; profile `9a751f6df4b66c8ef84c14ba9e2587bef418b4fbc8b920845e8913a4b761c164`. Raw logs, runner and `summary.json`: VM `target/791-write-stage-profile.BtzMom/`; NVMe `~/test-nvme/write-stage-profile.2hC8pb/`. A copy of the NVMe measured logs, summary and supplementary trace is in the VM run's `nvme/` directory.
