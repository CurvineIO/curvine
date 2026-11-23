# Does sharing the TCP write payload help the worker ring? (#791)

**On this VM, yes compared with copying the payload into another ring buffer; no consistent win over `LocalFile`.** Keep `worker.enable_io_uring` off by default. These are Linux VM results, not NVMe results.

`WriteBlock/Running` arrives with a `BytesMut` on TCP. The synchronous worker handler freezes it without copying. A staged writer can then give Compio a `Bytes` handle to the same allocation. Batch writes can use the `Bytes` made from each decoded protobuf member; this does not eliminate protobuf decoding. Borrowed payloads and frames larger than the 1 MiB admission unit still copy one admitted chunk at a time. Writes still use the blocking pool, explicit offsets and the same staging, short-completion, acknowledgement and durability rules. This removes one userspace copy, not the kernel page-cache copy.

## Matched experiment

One release worker test binary, six pairs per concurrency, alternating which arm runs first. Each trial wrote 512 fresh 1 MiB blocks via loopback TCP with no explicit `Flush`. Both arms used the real ring and buffered `Complete`. `copied_ring` is a test-only control: it gives the same executor a borrowed receive buffer instead of a shared one. Every trial verified 512 finalized blocks and 512 MiB of completed ring writes. The shared-buffer counter was 512 MiB in each shared trial and zero in each copied trial. A separate storage test checks the submitted buffer's address.

Medians of six trials; brackets are the throughput range. CPU is worker-process user + system seconds per 512 MiB, not host CPU. Latency is p95 of the `Complete` RPC only (median of per-trial percentiles).

| Clients | Payload | MiB/s [min–max] | Worker CPU s | Complete p95 ms |
|---:|---|---:|---:|---:|
| 1 | Copied | 660 [447–681] | 0.66 | 0.282 |
| 1 | Shared | 765 [675–792] | 0.56 | 0.269 |
| 16 | Copied | 1,979 [1,878–2,194] | 1.07 | 1.433 |
| 16 | Shared | 2,210 [1,953–2,249] | 0.86 | 1.217 |

The ratios of the throughput medians improved by 16% and 12% respectively. Shared won 6/6 one-client pairs but only 4/6 at 16 clients. In separate three-repetition runs of the existing backend comparison, the old → new ring medians were 622 → 740 MiB/s (1 client) and 1,967 → 2,116 (16 clients). The staged `pwrite` control also moved, from 825 → 801 and 2,602 → 2,810 respectively. Use the interleaved copy control, not the change between binaries, to assess copying.

## End-to-end comparison

Same 64 × 4 MiB whole-file workload and eligibility settings as the [placement study](worker-io-uring-placement.md#end-to-end-cv-bench), with the prototype server and unchanged CLI. All 16 arms verified. Means of two CLI rates per cell, one off/on pair and one on/off pair. This compares the entire enabled backend with the default, **not** just the write copy.

| Workload | Clients | Write MiB/s, off → on | Read MiB/s, off → on |
|---|---:|---:|---:|
| Throughput | 1 | 290 → 331 | 730 → 667 |
| Throughput | 16 | 1,931 → 1,664 | 2,322 → 2,313 |
| Mixed | 1 | 212 → 218 | 206 → 211 |
| Mixed | 16 | 1,232 → 1,186 | 1,149 → 1,106 |

At 16 clients, enabled throughput writes still trail the default by 14%; mixed writes trail by 4%. The two pairs and short workloads are not a production performance estimate.

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

VM environment: Linux `7.0.0-38-generic`, 16 KVM vCPUs, ext4 `/dev/vda1`, Rust 1.98.1. Four worker I/O threads, 16 blocking threads, four client I/O threads. Raw artifacts on the VM: `target/791-owned-write-{before,after,pair}.log` and `target/791-owned-write-e2e/`. To compare another host, retain the `WORKER_WRITE_BENCH` rows, end-to-end `trial.json` files, commit, kernel, and filesystem/device details. Earlier NVMe measurements at `21948c9e` predate the prototype and do not measure the effect of sharing buffers.
