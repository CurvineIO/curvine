# Worker io_uring benchmark results for #791

The [final read report](../../worker-io-uring-small-reads.md) covers the retained FUSE seek change and the matched SDK/FUSE backend comparison. The [write report](../../worker-io-uring-write-batching.md) distinguishes SDK batching gains from the worker backend comparison. Both use buffered loopback I/O on the ext4 VM. The [earlier NVMe matrix](../../worker-io-uring-nvme.md) used the preceding two-ring worker. The NVMe host was offline for the final client and worker changes.

| Measurements | Per-run data | Independent comparison |
| --- | --- | --- |
| Sequential and mixed writes and readback | [writes.csv](writes.csv) | `confirm` covers the eight-case matrix, `ablation` isolates the large-frame worker change, and `sdk-default-block` uses 128 MiB logical blocks. |
| Streaming reads and preceding writes | [reads.csv](reads.csv) | `seek-confirm` compares the retained FUSE implementation, incumbent and LocalFile at 4 and 128 KiB. |
| Verified 4 KiB random application reads | [random-reads.csv](random-reads.csv) | `seek-random` compares the final and incumbent clients on both backends. Each run checks all 8,192 returned pages. |

Group zero is the excluded warmup. Pair by experiment, group, client, workload, concurrency and operation. The reports give medians of within-group ratios. Absolute rates are per-arm medians. Worker CPU per GiB covers both phases of a streaming run and is repeated on its read and write rows. Streaming p95/p99 measure whole-file latency in milliseconds. Random-read p95/p99 measure individual pread calls in microseconds. For the CPU-affinity screen, the read depth was one. In the separate write pipeline prototype (`pipe`), the write depth is the number of outstanding RPCs. For gathered writes it is the maximum number of ready chunks in one RPC.

The read variants encode the client and the read profile. `inc` uses the incumbent FUSE client, `bound` bounded every FUSE read, and `seek` bounds reads after a seek. `d4` selects four outstanding reads; the CSV supplies the chunk size. The SDK binary is shared across these FUSE variants. In the write experiments, `new` is the final worker, `old` is the archived worker and `abl` is a matched rebuild with the earlier large-frame implementation. The backend column records the actual on/off arm.

Additional experiments cover [worker placement and FUSE transport](../../worker-io-uring-autoresearch.md), [worker and end-to-end comparisons](../../worker-io-uring-e2e-autoresearch.md), [write handoff profiling](../../worker-io-uring-write-profile.md) and [fresh staging](../../worker-io-uring-fresh-staging.md).

## Replaying the final backend comparison

The [replay archive](replay.tar.gz) contains the Python experiment drivers with their original relative layout. They import the repository's existing [cluster harness](../../worker-io-uring-local.py). Run from a clean checkout on a Linux host with Python 3.12+, a filesystem that supports io_uring, and enough free space for the dataset and build. Mounted cases require a working Curvine FUSE mount and passwordless sudo for its mount helper. SDK-only runs can use `sq64` as the cell list.

```sh
cargo build --locked --release -p curvine-server -p curvine-cli -p curvine-fuse
tar -xzf curvine-tests/benchmark/results/io-uring-791/replay.tar.gz
art=target/io-uring-window.sku4gY
mkdir -p "$art/bin"
ln -s "$(realpath target/release/curvine-server)" "$art/bin/server"
ln -s "$(realpath target/release/curvine-cli)" "$art/bin/cv"
ln -s "$(realpath target/release/curvine-fuse)" "$art/bin/fuse"

# Read matrix. Both arms use the final binaries. Only the backend changes within a pair.
python3 target/io-uring-affinity.QvHoCd/bench.py read-replay 4 \
  'rd4:1:128:4,rd4:0:128:4,r128d4:1:128:4,r128d4:0:128:4' \
  'fq1,fq16,sq64,fq64' --block 128 --files 64

# Same-binary eight-case write confirmation, including the batch-one controls.
python3 "$art/bench.py" write-replay 4 \
  'new:1:128:1,new:1:128:4,new:0:128:4,new:0:128:1' \
  'sq1,sq16,sm1,sm16,fq1,fq16,fm1,fm16' --block 4 --files 64
```

Each driver starts isolated services and creates a fresh results directory alongside the driver. Supply a new label for subsequent runs. Check the worker ring-byte deltas, errors and finalized block counts before comparing timing. For the historical random-read client ablation, the driver expects two FUSE executables named `fuse-incumbent` and `fuse-seek` alongside `seek-random.py`. Both are built with the same release flags. The incumbent predates the seek hint in `curvine-fuse/src/fs/fuse_reader.rs`.
