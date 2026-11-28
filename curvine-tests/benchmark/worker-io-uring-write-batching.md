# Curvine remote write batching and io_uring

The bounded large-frame worker improved 4 MiB remote writes by **18.9% with one writer and 22.1% with 16** in a matched build comparison. The SDK batching change improved 128 KiB sequential writes on io_uring by **45.4% and 19.4%** in a separate four-round confirmation. These experiments use different frame sizes and references. The percentages cannot be added. The NVMe host was offline. The measurements use buffered writes on the ext4 VM.

At the default 128 MiB logical block size and 128 KiB SDK write chunk, four-chunk gathering improved the sequential SDK result in all four paired repetitions. One writer gained 75.0% and 16 writers gained 24.4%. The 16-writer median reached 3,027 MiB/s, with a +0.6% paired difference versus equally configured LocalFile. Across seven experiments, 303 measured cells and 88 warmups passed byte-count, backend-routing and error checks.

Use four ready chunks for the measured remote SDK scenario. Single-writer mounted FUSE writes regressed in confirmation, so the default remains one. Both backends support the SDK optimization.

## Independent end-to-end confirmation

| Workload | io_uring write MiB/s, batch 1 → 4 | Write change | Faster groups | Write p95 change | Read rate change | LocalFile write change, batch 1 → 4 | io_uring vs LocalFile writes, both batch 4 |
|---|---:|---:|---:|---:|---:|---:|---:|
| SDK sequential ×1 | 370 → 567 | +45.4% | 4/4 | -26.0% | +8.2% | +42.6% | -14.7% |
| SDK sequential ×16 | 2,245 → 2,679 | +19.4% | 3/4 | -5.1% | -3.2% | +7.4% | -5.3% |
| SDK mixed ×1 | 235 → 295 | +22.3% | 3/4 | +0.0% | +22.3% | +35.2% | -14.1% |
| SDK mixed ×16 | 1,317 → 1,358 | +7.5% | 3/4 | -5.1% | +7.5% | +4.4% | -0.6% |
| FUSE sequential ×1 | 427 → 354 | -16.1% | 0/4 | +7.1% | +4.9% | -15.2% | +1.3% |
| FUSE sequential ×16 | 1,357 → 1,341 | -1.8% | 2/4 | +0.0% | +5.6% | -2.6% | +2.1% |
| FUSE mixed ×1 | 233 → 220 | -4.6% | 1/4 | +4.6% | -4.6% | -0.8% | -6.8% |
| FUSE mixed ×16 | 664 → 727 | +17.1% | 3/4 | -39.3% | +17.1% | -12.2% | +1.5% |

SDK sequential ×1 had 2,048 → 576 median worker write requests per 256 MiB and 1,320 → 940 worker CPU-ms per GiB over the combined write/read interval.

SDK sequential ×16 had 2,048 → 576 median worker write requests per 256 MiB and 1,560 → 1,420 worker CPU-ms per GiB over the combined write/read interval.

Across the eight equally weighted workload cells, geometric means of the per-cell paired write ratios gave +9.7% for writes with batching and −4.8% for io_uring relative to LocalFile at batch four.

A mounted sequential run issued 512 worker writes per 256 MiB in both configurations. The two mixed FUSE cells also had equal request counts across configurations. The measured FUSE differences have no evidence of a reduction in RPC count. Mixed read and write rates share the elapsed time of their combined workload.

## Prototype screen

Two measured rounds per configuration at a 4 MiB logical block size. Changes are relative to one 128 KiB request at a time on the final worker. The pipelined sender was built separately.

| SDK remote writer | One writer MiB/s | Change | 16 writers MiB/s | Change |
|---|---:|---:|---:|---:|
| 128 KiB, one | 353 | +0.0% | 2,130 | +0.0% |
| 128 KiB, four outstanding RPCs | 460 | +31.4% | 2,279 | +6.9% |
| 128 KiB, gather two | 490 | +40.1% | 2,459 | +15.8% |
| 128 KiB, gather four | 574 | +66.4% | 2,852 | +34.1% |
| 512 KiB, one | 557 | +62.7% | 2,626 | +23.1% |
| 1 MiB, one | 646 | +87.7% | 2,471 | +16.0% |
| 4 MiB, one | 680 | +94.7% | 1,714 | -19.2% |
| 4 MiB, one, archived worker | 569 | +65.8% | 1,813 | -14.9% |

## Large writes and the original one-MiB matrix

| SDK write comparison | Clients | Reference → candidate MiB/s | Paired change | Faster groups |
|---|---:|---:|---:|---:|
| 4 MiB chunk, current vs archived worker | SDK sequential ×1 | 621 → 737 | +18.4% | 4/4 |
| 4 MiB chunk, current vs archived worker | SDK sequential ×16 | 1,879 → 1,958 | +2.0% | 3/4 |
| 4 MiB chunk, io_uring vs LocalFile | SDK sequential ×1 | 737 → 737 | +1.8% | 3/4 |
| 4 MiB chunk, io_uring vs LocalFile | SDK sequential ×16 | 2,120 → 1,958 | -8.4% | 0/4 |
| 1 MiB block and chunk, batch four vs one | SDK sequential ×1 | 439 → 451 | +3.4% | 3/3 |
| 1 MiB block and chunk, batch four vs one | SDK sequential ×16 | 2,242 → 2,474 | +9.0% | 3/3 |
| 1 MiB block and chunk, batch four vs one | SDK mixed ×16 | 1,491 → 1,410 | -2.9% | 0/3 |
| 1 MiB block and chunk, io_uring vs LocalFile | SDK sequential ×1 | 460 → 451 | -0.0% | 1/3 |
| 1 MiB block and chunk, io_uring vs LocalFile | SDK sequential ×16 | 2,344 → 2,474 | +5.5% | 2/3 |
| 1 MiB block and chunk, io_uring vs LocalFile | SDK mixed ×16 | 1,489 → 1,410 | -5.3% | 0/3 |
| 4 MiB block and 1 MiB chunk, batch four vs one | SDK sequential ×1 | 595 → 660 | +4.2% | 3/3 |
| 4 MiB block and 1 MiB chunk, batch four vs one | SDK sequential ×16 | 2,755 → 2,532 | -7.6% | 0/3 |
| Gathered 1 MiB chunks, current vs archived worker | SDK sequential ×1 | 590 → 660 | +11.6% | 3/3 |
| Gathered 1 MiB chunks, current vs archived worker | SDK sequential ×16 | 2,471 → 2,532 | -3.2% | 1/3 |
| Gathered 1 MiB chunks, io_uring vs LocalFile | SDK sequential ×1 | 698 → 660 | -5.4% | 0/3 |
| Gathered 1 MiB chunks, io_uring vs LocalFile | SDK sequential ×16 | 2,767 → 2,532 | -9.7% | 0/3 |

## SDK with the default 128 MiB logical block and 128 KiB chunk

The dataset still uses 4 MiB files. Remote I/O is forced for the backend comparison. Each configuration has four measured runs after warmup.

| SDK workload | Batch 1 → 4 MiB/s with io_uring | Paired change | Faster groups | io_uring vs LocalFile at batch four |
|---|---:|---:|---:|---:|
| SDK sequential ×1 | 333 → 576 | +75.0% | 4/4 | -15.0% |
| SDK sequential ×16 | 2,373 → 3,027 | +24.4% | 4/4 | +0.6% |
| SDK mixed ×16 | 1,345 → 1,497 | +11.1% | 4/4 | -0.9% |

## Controlled worker change

A second release build used the pre-experiment io_uring worker source and the final checkout for every other component. Both workers were built with the same Cargo command, toolchain and native flags. Both ran exactly the final client binary. Four measured repetitions followed one warmup. 128 KiB gathered chunks were included as a small-frame control.

| Input | Workload | Previous → current MiB/s | Paired change | Faster groups | Worker CPU-ms/GiB change |
|---|---:|---:|---:|---:|---:|
| 4 MiB ordinary write | SDK sequential ×1 | 581 → 691 | +18.9% | 3/4 | -13.1% |
| 4 MiB ordinary write | SDK sequential ×16 | 1,744 → 2,042 | +22.1% | 4/4 | -22.3% |
| Gathered 128 KiB chunks | SDK sequential ×1 | 555 → 543 | -6.6% | 1/4 | +1.1% |
| Gathered 128 KiB chunks | SDK sequential ×16 | 2,696 → 2,798 | +2.5% | 2/4 | -6.1% |

## Method

A fresh master and worker started for each configuration and repetition on a 16-vCPU Linux 7.0.0-38-generic VM with ext4 on /dev/vda1. SDK traffic used TCP loopback, one replica and buffered remote writes. The FUSE runs mounted the real Curvine filesystem. A sequential cell wrote 64 distinct 4 MiB files and then read those files. A mixed cell used a 384 MiB target and a prefilled working set, with already-started operations allowed to finish. The client count was 1 or 16. Each cell had a separate path. Rate units are MiB/s. The harness validated per-operation bytes, errors, completed logical block counts and backend routing. The first repetition of every experiment warmed the system and was excluded. Configurations rotated between repetitions and workload order was shuffled within each repetition. Reported changes are medians of paired ratios, which can differ from ratios of the displayed median rates. These timers measure buffered application throughput. They do not time a per-file fsync or physical NVMe I/O.

## Implementation and research

The SDK can gather two to four ready, in-memory chunks that fit within a single remote block and a 4 MiB cap. It sends the ordinary protocol header and payload with vectored TCP I/O and checks a single correlated acknowledgment. Each queued buffer retains its write-budget permit through the response. The default is one chunk. Blocks that cross the batching boundary, short-circuit writes, multiple replica writers and enabled bandwidth limiting retain their existing write paths.

The large-frame worker prototype reserves one admission unit per started MiB for shared payloads through 4 MiB. When capacity is available it can issue one contiguous positional write from the worker's incoming buffer. Under pressure it can submit bounded chunks. The existing global bound remains 64 units. This removes a worker-side copy for eligible frames. The TCP and page-cache paths still transfer data.

Apache HDFS tracks sent packets awaiting acknowledgment separately from packets ready to send. That design motivated testing overlap with four outstanding Curvine writes. Ceph's kernel block backend offered a comparison for separating submission and completion in the worker. The liburing man page recommends a non-vectored write for a contiguous disk buffer. In the gathered implementation, the discontiguous buffers are on the TCP sender and the worker receives a contiguous payload. [HDFS DataStreamer](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java) · [Ceph kernel io_uring backend](https://github.com/ceph/ceph/blob/main/src/blk/kernel/io_uring.cc) · [liburing writev manual](https://github.com/axboe/liburing/blob/master/man/io_uring_prep_writev.3)

[Per-run write results and replay instructions](results/io-uring-791/README.md)
