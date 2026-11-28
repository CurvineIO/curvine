# Does sharing the TCP write payload help the worker ring? (#791)

**Sharing the payload helped at one client on both the VM and the NVMe worker.** At 16 clients it reduced worker CPU time on both hosts, but median throughput rose only on the VM. Neither host showed a consistent end-to-end win over `LocalFile`. Keep `worker.enable_io_uring` off by default.

`WriteBlock/Running` arrives with a `BytesMut` on TCP. The synchronous worker handler freezes it without copying. A staged writer can then give Compio a `Bytes` handle to the same allocation. Batch writes can use the `Bytes` made from each decoded protobuf member; this does not eliminate protobuf decoding. Borrowed payloads and frames larger than the 1 MiB admission unit still copy one admitted chunk at a time. Writes still use the blocking pool, explicit offsets and the same staging, short-completion, acknowledgement and durability rules. This removes one userspace copy, not the kernel page-cache copy.

## Matched experiment

On each host, both arms used the same release worker test binary. Six pairs per concurrency alternated which arm ran first. Each trial wrote 512 fresh 1 MiB blocks via loopback TCP with no explicit `Flush`. Both arms used the real ring and buffered `Complete`. `copied_ring` is a test-only control: it gives the same executor a borrowed receive buffer instead of a shared one. Every trial verified 512 finalized blocks and 512 MiB of completed ring writes. The shared-buffer counter was 512 MiB in each shared trial and zero in each copied trial. A separate storage test checks the submitted buffer's address.

Medians of six trials; brackets are the throughput range. CPU is worker-process user + system seconds per 512 MiB, not host CPU. Latency is p95 of the `Complete` RPC only (median of per-trial percentiles).

| Host | Clients | Payload |     MiB/s [min–max] | Worker CPU s | Complete p95 ms |
| ---- | ------: | ------- | ------------------: | -----------: | --------------: |
| VM   |       1 | Copied  |       660 [447–681] |         0.66 |           0.282 |
| VM   |       1 | Shared  |       765 [675–792] |         0.56 |           0.269 |
| VM   |      16 | Copied  | 1,979 [1,878–2,194] |         1.07 |           1.433 |
| VM   |      16 | Shared  | 2,209 [1,953–2,249] |         0.86 |           1.217 |
| NVMe |       1 | Copied  | 1,843 [1,340–2,033] |        0.255 |           0.118 |
| NVMe |       1 | Shared  | 2,028 [1,908–2,132] |        0.235 |           0.116 |
| NVMe |      16 | Copied  | 3,284 [2,356–3,445] |         0.65 |           0.730 |
| NVMe |      16 | Shared  | 3,285 [3,190–3,571] |         0.51 |           0.680 |

On NVMe, sharing improved median throughput by 10% at one client. At 16 clients, the throughput medians differed by less than 0.1%, but worker CPU fell from 0.65 to 0.51 seconds per 512 MiB (22%). On both hosts, shared won 6/6 one-client throughput pairs and 4/6 at 16 clients.

On the VM, the throughput medians improved by 16% and 12% respectively. In separate three-repetition VM runs of the existing backend comparison, the old → new ring medians were 622 → 740 MiB/s (1 client) and 1,967 → 2,116 (16 clients). The staged `pwrite` control also moved, from 825 → 801 and 2,602 → 2,810 respectively. Use the interleaved copy control, not the change between binaries, to assess copying.

## End-to-end comparison

Same 64 × 4 MiB whole-file workload and eligibility settings as the [placement study](worker-io-uring-placement.md#end-to-end-cv-bench), with the prototype server and unchanged CLI. All 16 arms on each host verified, with zero operation errors. Each cell gives mean CLI rates from two pairs: one off/on and one on/off. This compares the entire enabled backend with the default, **not** just the write copy.

| Workload   | Clients | VM write MiB/s, off → on | NVMe write MiB/s, off → on | VM read MiB/s, off → on | NVMe read MiB/s, off → on |
| ---------- | ------: | -----------------------: | -------------------------: | ----------------------: | ------------------------: |
| Throughput |       1 |                290 → 331 |                  591 → 562 |               730 → 667 |               1,055 → 983 |
| Throughput |      16 |            1,931 → 1,663 |              2,344 → 2,388 |           2,322 → 2,313 |             3,277 → 2,936 |
| Mixed      |       1 |                212 → 218 |                  288 → 293 |               206 → 211 |                 280 → 284 |
| Mixed      |      16 |            1,232 → 1,186 |              1,272 → 1,336 |           1,149 → 1,105 |             1,186 → 1,246 |

At 16 clients on the VM, enabled throughput writes trailed the default by 14%; mixed writes trailed by 4%. On NVMe, enabled throughput writes averaged 2% above the default, but each backend won one pair. Do not attribute this apparent turnaround to the prototype: compared with the earlier NVMe run at `21948c9e`, ring writes barely moved (2,398 → 2,388 MiB/s) while `LocalFile` fell 15% (2,767 → 2,344 MiB/s). The interleaved worker test isolates the copy; these two short whole-file pairs do not.

## Reproduce on Linux with ext4 and working io_uring

Use a checkout containing this prototype. Set `CURVINE_WRITE_BENCH_DIR` to an **existing directory** on the desired filesystem (a directory on an NVMe root partition is fine). Keep the host otherwise idle. No formatting, mount changes or global cache drops are needed. Run from the repository root; keep any build environment used for the previous run. Run the benchmarks sequentially, not alongside builds or other workloads.

```bash
(
  set -euo pipefail
  : "${CURVINE_WRITE_BENCH_DIR:?set this to an existing ext4 directory}"
  findmnt -T "$CURVINE_WRITE_BENCH_DIR" -o TARGET,SOURCE,FSTYPE
  run=$(mktemp -d "$CURVINE_WRITE_BENCH_DIR/owned-writes.XXXXXX")
  export CURVINE_WRITE_BENCH_DIR="$run" CURVINE_WRITE_BENCH_BLOCKS=512
  export CURVINE_WRITE_BENCH_EXPLICIT_FLUSH=0 CURVINE_WRITE_BENCH_VERIFY=1
  bench=worker::worker_server::write_benchmark::worker_write_ablation
  echo "Worker logs: $run"
  CURVINE_WRITE_BENCH_COMPARE_COPY=1 CURVINE_WRITE_BENCH_REPS=6 \
    cargo test --locked --release -j 4 -p curvine-worker --lib "$bench" \
    -- --ignored --exact --nocapture 2>&1 | tee "$run/copy-pair.log"
  CURVINE_WRITE_BENCH_COMPARE_COPY=0 CURVINE_WRITE_BENCH_REPS=3 \
    cargo test --locked --release -j 4 -p curvine-worker --lib "$bench" \
    -- --ignored --exact --nocapture 2>&1 | tee "$run/backends.log"
)
```

For whole-file throughput use the `cv bench` runner below. Its worker data and logs are under the **checkout's `target/`**, not `CURVINE_WRITE_BENCH_DIR`; verify that directory is on the intended device as well. The runner creates its own fresh output directory and prints its path. Use the same client build profile on both hosts.

```bash
(
  set -euo pipefail
  mkdir -p target
  findmnt -T target -o TARGET,SOURCE,FSTYPE
  cargo build --locked --release -j 4 -p curvine-server --bin curvine-server
  cargo build --locked -j 4 -p curvine-cli
  python3 curvine-tests/benchmark/worker-io-uring-local.py \
    --server "${CARGO_TARGET_DIR:-target}/release/curvine-server" \
    --cli "${CARGO_TARGET_DIR:-target}/debug/curvine-cli" \
    --repetitions 2 --files 64 --file-mib 4 --mixed-mib 512 --mixed-seconds 30
)
```

VM environment: Linux `7.0.0-38-generic`, 16 KVM vCPUs, ext4 `/dev/vda1`, Rust 1.98.1. Raw artifacts on the VM: `target/791-owned-write-{before,after,pair}.log` and `target/791-owned-write-e2e/`.

NVMe environment: prototype `ca305c9e`, Linux `7.2.15-76070215-generic`, Intel Core i5-13420H (12 logical CPUs), 15 GiB RAM, Rust 1.98.1. The worker files were on the ext4 root partition of a local CT1000P3PSSD8 NVMe drive, not a dedicated raw device. Both hosts used four worker I/O threads, 16 blocking threads and four client I/O threads. To compare another host, retain the `WORKER_WRITE_BENCH` rows, end-to-end `trial.json` files, commit, kernel, and filesystem/device details.
