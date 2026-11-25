# Remote file reads with io_uring

At 64 SDK clients and a 4 KiB remote read chunk, the final io_uring worker delivered 809 MiB/s versus 550 MiB/s on LocalFile, a 48.5% median paired gain. The combined write/read phases gained 34.8%. The new FUSE seek hint improved true random-read IOPS by 29.8% at 16 clients relative to the incumbent io_uring build. The 4 KiB, 64-client mounted streaming case regressed 9.1% against the incumbent in all four pairs. At the normal 128 KiB remote chunk, the 64-client SDK io_uring median was 3,473 MiB/s.

## Final FUSE implementation

The retained implementation limits SDK read-ahead when a FUSE read seeks to a different offset. Contiguous reads restore the end of the file as the prefetch limit. The earlier prototype bounded every read and lost 7.9% on the single-client 128 KiB sequential control. The final comparison below used freshly started clusters, rotated arms and one excluded warmup per configuration.

| Mounted random 4 KiB reads | Clients | Control → candidate IOPS | Paired change | Faster groups | p95 µs | p99 µs | Worker CPU-ms/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| FUSE change on io_uring | 16 | 27,183 → 36,048 | +29.8% | 5/5 | 989 → 736 | 1,422 → 1,035 | 39,360 → 16,000 |
| FUSE change on io_uring | 64 | 29,812 → 33,698 | +6.3% | 5/5 | 4,551 → 4,760 | 9,475 → 8,664 | 38,720 → 16,640 |
| FUSE change on LocalFile | 16 | 25,417 → 35,036 | +29.2% | 5/5 | 1,001 → 778 | 1,488 → 1,133 | 46,080 → 16,000 |
| FUSE change on LocalFile | 64 | 26,925 → 34,304 | +30.6% | 5/5 | 3,761 → 4,798 | 8,181 → 10,213 | 46,080 → 16,320 |
| Final io_uring vs final LocalFile | 16 | 35,036 → 36,048 | +2.2% | 5/5 | 778 → 736 | 1,133 → 1,035 | 16,000 → 16,000 |
| Final io_uring vs final LocalFile | 64 | 34,304 → 33,698 | +1.4% | 3/5 | 4,798 → 4,760 | 10,213 → 8,664 | 16,320 → 16,640 |

Each run verified the bytes returned by 8,192 deterministic random reads. The candidate issued 8,279 worker RPCs. Its 8,099 explicit seeks used the worker’s positional fallback. The remaining 180 requests accounted for exactly 737,280 ring bytes on io_uring. The LocalFile ring counters stayed at zero.

| Sequential reads | Remote chunk KiB | Client | Control → candidate MiB/s | Paired change | Faster pairs | Read p95 change | Read p99 change | Worker CPU/GiB change | Combined write/read rate change |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| Final io_uring vs final LocalFile | 4 | SDK ×64 | 550 → 809 | +48.5% | 4/4 | -32.6% | -32.6% | -35.6% | +34.8% |
| Final io_uring vs final LocalFile | 4 | FUSE ×1 | 64 → 83 | +24.7% | 3/4 | -21.5% | -20.8% | -9.6% | +19.3% |
| Final io_uring vs final LocalFile | 4 | FUSE ×16 | 440 → 476 | +10.8% | 4/4 | +3.5% | +3.5% | -19.7% | +9.2% |
| Final io_uring vs final LocalFile | 4 | FUSE ×64 | 456 → 458 | +3.2% | 3/4 | -3.4% | -3.1% | -17.9% | +1.6% |
| FUSE change on io_uring | 4 | FUSE ×1 | 82 → 83 | -1.3% | 2/4 | -1.5% | -0.5% | +1.3% | -2.5% |
| FUSE change on io_uring | 4 | FUSE ×16 | 490 → 476 | -1.3% | 2/4 | +10.9% | +10.9% | +3.5% | -0.9% |
| FUSE change on io_uring | 4 | FUSE ×64 | 507 → 458 | -9.1% | 0/4 | +10.1% | +10.2% | +5.5% | -5.3% |
| Final io_uring vs final LocalFile | 128 | SDK ×64 | 3,301 → 3,473 | +6.1% | 3/4 | -5.6% | -5.6% | +1.3% | -2.1% |
| Final io_uring vs final LocalFile | 128 | FUSE ×1 | 581 → 589 | +0.0% | 2/4 | -20.0% | -17.0% | -0.9% | +1.3% |
| Final io_uring vs final LocalFile | 128 | FUSE ×16 | 1,980 → 2,085 | +2.0% | 2/4 | -4.1% | -4.1% | +1.7% | -0.5% |
| Final io_uring vs final LocalFile | 128 | FUSE ×64 | 2,069 → 2,053 | -0.8% | 2/4 | +0.8% | +0.8% | +4.5% | -4.5% |
| FUSE change on io_uring | 128 | FUSE ×1 | 579 → 589 | +2.3% | 3/4 | -6.6% | -1.9% | +4.8% | +2.1% |
| FUSE change on io_uring | 128 | FUSE ×16 | 1,996 → 2,085 | +4.1% | 2/4 | -2.2% | -2.2% | -1.4% | +1.6% |
| FUSE change on io_uring | 128 | FUSE ×64 | 2,044 → 2,053 | -1.2% | 1/4 | +1.2% | +1.2% | +2.1% | +1.4% |


The SDK read pipeline allows up to four requests per connection in this experiment. Every sequential measurement used fresh 4 MiB files and warmed buffered readback on the Linux ext4 VM, with one worker, forced remote I/O and loopback networking. In the earlier 128-file, five-group check, SDK reads with 4 KiB remote chunks gained 36.6% at 16 clients and 34.1% at 64 against equally configured LocalFile. The final interleaved SDK and mounted FUSE comparison used 64 files, four measured groups, and the same worker and CLI binaries in each arm. The random application-read test used 64 files and five measured groups. Rates in the tables are medians. Changes are medians of paired ratios. The tail latencies refer to whole files for streaming and individual pread calls for random access.

Worker byte counters verified the backend for each streaming read and write. The random cases checked the exact split between seek-bearing and headerless RPCs. Payloads passed SDK/FUSE cross-read checks with io_uring enabled and disabled for both read sizes. Explicit Flush preserved the checkpoint SHA-256, Complete retained the file and Cancel discarded it. Four targeted FUSE reader tests passed. The NVMe peer was offline.

The 4 KiB scenario characterizes workloads with small remote fetches. The 128 KiB control gives the absolute streaming-bandwidth comparison. Both used read_pipeline_depth = 4. Default settings are preserved. [Worker read-path measurements](worker-io-uring-placement.md)

[Per-run read results and replay instructions](results/io-uring-791/README.md)
