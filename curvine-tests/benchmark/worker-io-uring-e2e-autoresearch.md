# Worker io_uring end-to-end research on the ext4 VM

Five measured groups per case favored the new two-ring build. It gained **2.6% throughput** and used **4.9% less worker CPU per GiB** against the remeasured prior io_uring release, with higher median paired throughput in nine of 16 directions. It was **1.3% slower overall than LocalFile**, and ahead in seven directions. These aggregate percentages are geometric means of the per-direction or per-case median ratios. The eight-ring build gained 0.6% against the prior ring release and trailed the new two-ring build by 1.5% in their direct comparison.

The selected build uses cooperative completion flags with a compatible startup retry, skips read-buffer zero-fill, and retains up to sixteen idle 1 MiB response buffers. The completion bound and short-read loop advance the initialized prefix before exposing it. A pooled allocation becomes available after the last response slice drops. Fresh staged writers keep the descriptor from exclusive allocation, and readers pin a separate read-only descriptor.

## Final throughput matrix

Each throughput cell shows the median read and write rate in MiB/s. Percentage columns show the read and write median paired changes separately. The LocalFile control in this four-arm matrix uses the eight-ring release binary with the backend disabled. The local file path and its fixture are shared by both new binaries.

| Path and workload | LocalFile R / W | Prior ring R / W | Selected ring R / W | vs prior R / W | vs LocalFile R / W |
|---|---:|---:|---:|---:|---:|
| SDK mixed 1 | 345 / 331 | 304 / 292 | 309 / 296 | +1.6% / +1.6% | -7.1% / -7.1% |
| SDK mixed 16 | 1,365 / 1,341 | 1,343 / 1,319 | 1,482 / 1,455 | +9.7% / +9.7% | +11.2% / +11.2% |
| SDK sequential 1 | 916 / 514 | 876 / 464 | 958 / 468 | +8.9% / -4.8% | +3.3% / -8.2% |
| SDK sequential 16 | 2,701 / 2,397 | 2,722 / 2,250 | 2,929 / 2,142 | +9.8% / -2.2% | +10.7% / -10.2% |
| FUSE mixed 1 | 219 / 210 | 214 / 206 | 213 / 204 | -0.7% / -0.7% | -3.0% / -3.0% |
| FUSE mixed 16 | 783 / 769 | 802 / 788 | 803 / 789 | -1.5% / -1.5% | +2.6% / +2.6% |
| FUSE sequential 1 | 603 / 372 | 543 / 325 | 579 / 321 | +8.4% / +1.2% | +2.2% / -10.7% |
| FUSE sequential 16 | 2,152 / 1,212 | 2,068 / 1,142 | 2,168 / 1,120 | +4.9% / -0.8% | -5.6% / -6.2% |

At 16 SDK clients, mixed throughput beat the prior ring build in all five groups and worker CPU fell from 1,707 to 1,499 ms/GiB. SDK sequential read p95 fell from 27.2 to 25.2 ms at 16 clients, while write p95 rose from 48.4 to 52.2 ms. LocalFile led the selected ring on sequential writes in all four SDK and FUSE cases. Full per-cell p95, p99, CPU statistics and per-group ratios are in the results.

## Same-binary toggle

Two more matched groups used the byte-identical selected binary for both settings. All 32 arms verified their backend and byte routing. The selected ring trailed LocalFile by 5.0% in the median-pair aggregate and led four of 16 throughput directions. These runs show how much the VM comparison can vary across time windows.

| Path and workload | LocalFile R / W MiB/s | io_uring R / W MiB/s | Paired change R / W |
|---|---:|---:|---:|
| SDK mixed 1 | 337 / 323 | 250 / 240 | -25.9% / -25.9% |
| SDK mixed 16 | 1,502 / 1,475 | 1,477 / 1,451 | -1.4% / -1.4% |
| SDK sequential 1 | 876 / 500 | 882 / 496 | +0.9% / -0.8% |
| SDK sequential 16 | 2,835 / 2,328 | 2,946 / 2,197 | +4.6% / -5.5% |
| FUSE mixed 1 | 232 / 223 | 212 / 204 | -8.1% / -8.1% |
| FUSE mixed 16 | 810 / 795 | 788 / 774 | -2.7% / -2.7% |
| FUSE sequential 1 | 589 / 376 | 671 / 348 | +14.3% / -7.3% |
| FUSE sequential 16 | 2,210 / 1,142 | 2,110 / 1,173 | -4.0% / +2.7% |

## Environment and method

The measurement host was a 16-vCPU Linux 7.0.0-38-generic VM. The worker used ext4 on the guest's virtio `/dev/vda1` device. The NVMe test machine was unavailable.

Each arm used a fresh, isolated master and worker on loopback. Mounted runs used the standard kernel FUSE transport with direct I/O, eight I/O threads and eight blocking threads. All arms used the same release CLI and FUSE binary. Worker sendfile, splice, checksums, client short circuit and read-ahead were disabled across the matrix to exercise the worker buffered backend.

Each sequential case wrote and then read 64 files of 4 MiB. Each mixed case prefilled the same working set and ran a 50/50 read and write workload with a 384 MiB cap. The block size and read and write chunk sizes were 1 MiB. Each case ran at one and 16 clients. The final run used one excluded warmup and five measured groups per case. The four arms were fresh LocalFile, the archived two-ring release binary from the previous matrix, and two new release builds with different ring limits. The runner reserved at least 256 MiB beyond the estimated peak dataset and removed each private cluster after the arm.

The runner checked the requested and effective backend, startup and steady-state ring descriptor counts, and application operation counts and byte totals. For the strict ring arms the worker's ring counters had to account for every measured read and write byte. The LocalFile arm had zero ring bytes and descriptors. Mounted arms had to create a real FUSE mount and complete cleanup. Throughput is reported in MiB/s. Percentage changes are medians of the ratios within each matched group and can differ from a ratio of the displayed medians. The two mixed rates share a time window. File p95 and p99 differ from per-chunk I/O latency. Worker CPU is normalized to total measured GiB. It excludes the master, benchmark client and FUSE daemon.

## Research and screening

The prototypes drew on 3FS's use of stable read buffers and multiple worker queues, Ceph's submission strategies, and the cache- and contention-aware selection described for Lustre. Linux documents cooperative completion task work, deferred task work, submission polling and nonblocking cached reads. The ext4 buffered write implementation can take the kernel `io-wq` path, so adding user-space queues was tested independently of buffer reuse. [3FS read workers](https://github.com/deepseek-ai/3FS/blob/main/src/storage/aio/AioReadWorker.cc), [Ceph io_uring backend](https://github.com/ceph/ceph/blob/main/src/blk/kernel/io_uring.cc), [Lustre autoIO study](https://www.usenix.org/conference/fast24/presentation/qian), [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html), [preadv(2)](https://man7.org/linux/man-pages/man2/preadv.2.html), [Linux 7.0 ext4 file operations](https://github.com/torvalds/linux/blob/v7.0/fs/ext4/file.c)

| Screen | Verified arms | Findings |
|---|---:|---|
| Read-buffer initialization, staged reader descriptor sharing and staged `pwrite` | 96 | Skipping the read-buffer zero-fill gave 3.2% higher aggregate throughput than the prior ring build. Sharing the scratch descriptor would have allowed reading code to mutate a published snapshot. Staged `pwrite` isolated the write handoff cost. |
| Buffer reuse, cooperative and deferred task running, SQPOLL and forced async | 56 | Buffer reuse gave a 3.5% aggregate SDK gain against the build that skipped zero-fill without pooling. The cooperative task-running combination gave 7.7%. |
| `RWF_NOWAIT` and direct per-thread ring writes | 40 | `RWF_NOWAIT` split the throughput directions evenly with LocalFile in one exploratory matrix. A separate 4 MiB + 257 byte payload check took the cache path for all warm read bytes and used the ring for all read bytes after file-level cache eviction advice. Direct per-thread ring writes added ring descriptors and trailed LocalFile by 4.7% overall. |
| Worker allocator overrides | 24 | jemalloc gained 2.0% and mimalloc lost 2.7% on the aggregate SDK measures. Each improved three of eight throughput directions. |
| Two, four and eight cooperative rings | 20 | With 16 clients, eight rings reached 2,387 MiB/s on mounted sequential reads versus 2,098 for LocalFile. Mounted sequential writes were 1,228 versus 1,263 MiB/s. |
| Retaining the fresh allocation descriptor | 24 | Reusing the original descriptor gave a 12.0% SDK mixed gain at 16 clients and used 17.3% less worker CPU per GiB across two screening groups. The final builds also check permissions for restrictive creation modes. |

The research scores above are geometric means of per-direction median ratios against the named control. The test-only routing and allocator switches are recorded in the run manifests with their effective byte counts.

## Worker-only cross-checks

Two measured groups per case ran through the worker RPC path. These medians are separate from the SDK and mounted matrix. Cold-advised reads visited one random 4 KiB page per block after file-level eviction advice. Warm reads used 128 KiB chunks. Writes used 1 MiB blocks with content verification in both the buffered Complete and explicit Flush runs.

| Case | Clients | LocalFile MiB/s | Staged pwrite or blocking ring MiB/s | Selected ring MiB/s |
|---|---:|---:|---:|---:|
| Warm sequential read | 1 | 773 | 605 | 711 |
| Warm sequential read | 16 | 3,338 | 2,659 | 2,888 |
| Cold-advised random read | 1 | 11.75 | 10.07 | 10.56 |
| Cold-advised random read | 16 | 63.76 | 56.63 | 64.60 |
| Buffered Complete write | 1 | 884 | 889 | 770 |
| Buffered Complete write | 16 | 3,060 | 2,636 | 2,498 |
| Explicit Flush write | 1 | 206 | 169 | 159 |
| Explicit Flush write | 16 | 594 | 513 | 512 |

## Verification and artifacts

The release suite passed 162 storage and 175 worker tests. Eight targeted real-kernel tests covered short completion, sparse reads, mixed sendfile and buffered reads, staged visibility, and failed checkpoint recovery. The concurrent round trip also passed with one CPU available. Clippy passed with warnings denied and the write profiling feature enabled.

The selected and disabled modes both passed SHA-256 readback checks through the SDK and an actual FUSE mount. The SDK payload was 4 MiB + 257 bytes and was checked with warm pages and after per-file cache eviction advice. The mounted path also wrote a distinct payload that the SDK read back. The enabled worker accounted for every SDK payload byte in the ring counters in both cache conditions.

The run directory is `target/io-uring-e2e-research.2z270d`. It contains the selected release binary and SHA-256 manifest, prototype source snapshots, benchmark scripts, and test logs. `final-e2e/rows.jsonl` contains the 192 four-arm trials, including 32 warmup arms. `final-stats.json` has per-group throughput ratios, latency percentiles and worker CPU. `same-binary/rows.jsonl` and `same-binary-stats.json` record the standalone toggle comparison.
