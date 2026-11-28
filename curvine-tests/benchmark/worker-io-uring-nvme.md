# Earlier two-ring worker E2E on NVMe

This run predates SDK write batching and the FUSE seek change. The final implementation was measured on the VM.

The selected two-ring release gained 6.1% aggregate throughput and used 7.0% less worker CPU per GiB compared with the prior ring release. It gained 2.0% against LocalFile in the five-group matrix. The two-group same-binary repeat put it 0.6% behind LocalFile, with higher throughput in nine of 16 directions.

| Comparison | NVMe throughput change | NVMe worker CPU/GiB change | VM throughput / CPU change |
|---|---:|---:|---:|
| Selected two rings vs prior ring | +6.1% | -7.0% | +2.6% / -4.9% |
| Selected two rings vs LocalFile | +2.0% | +2.3% | -1.3% / +4.4% |
| Selected two rings vs LocalFile, same binary | -0.6% | +4.3% | -5.0% / +11.9% |
| Eight rings vs selected two rings | -3.1% | +3.9% | See VM report |

The aggregate is the geometric mean of median matched-group ratios. CPU changes measure worker CPU per GiB across the eight workload cases. The absolute rates below are independent medians in MiB/s, shown as read / write.

## Five-group matrix

| Workload | LocalFile | Prior ring | Selected two rings | Eight rings | Selected vs LocalFile | Selected vs prior |
|---|---:|---:|---:|---:|---:|---:|
| SDK mixed 1 | 337 / 323 | 310 / 297 | 305 / 293 | 323 / 310 | -1.1% / -1.1% | +2.5% / +2.5% |
| SDK mixed 16 | 1,286 / 1,263 | 1,133 / 1,113 | 1,235 / 1,213 | 1,095 / 1,076 | +1.4% / +1.4% | +15.6% / +15.6% |
| SDK sequential 1 | 950 / 520 | 842 / 502 | 905 / 469 | 928 / 483 | -5.5% / -7.8% | +7.5% / -2.6% |
| SDK sequential 16 | 2,933 / 2,178 | 3,461 / 1,753 | 3,758 / 1,802 | 3,229 / 1,742 | +30.7% / -8.5% | +6.6% / +12.5% |
| FUSE mixed 1 | 242 / 232 | 231 / 221 | 254 / 244 | 234 / 225 | +4.6% / +4.6% | +7.6% / +7.6% |
| FUSE mixed 16 | 957 / 940 | 859 / 843 | 1,091 / 1,071 | 840 / 825 | +8.4% / +8.4% | +1.5% / +1.5% |
| FUSE sequential 1 | 735 / 376 | 687 / 367 | 748 / 350 | 703 / 351 | +8.8% / -7.1% | +7.9% / -3.6% |
| FUSE sequential 16 | 3,176 / 1,302 | 3,065 / 1,169 | 3,256 / 1,213 | 3,326 / 1,196 | +7.6% / -5.8% | +9.3% / +7.3% |

## Same-binary on/off repeat

Two additional groups per workload toggled only `worker.enable_io_uring` on the selected two-ring binary.

| Same binary | LocalFile | Selected two rings | Selected change |
|---|---:|---:|---:|
| SDK mixed 1 | 338 / 324 | 320 / 307 | -5.1% / -5.1% |
| SDK mixed 16 | 1,237 / 1,215 | 1,298 / 1,275 | +18.9% / +18.9% |
| SDK sequential 1 | 918 / 562 | 924 / 496 | +0.6% / -11.5% |
| SDK sequential 16 | 2,963 / 1,869 | 3,009 / 1,567 | +2.0% / -16.2% |
| FUSE mixed 1 | 245 / 235 | 227 / 217 | -6.9% / -6.9% |
| FUSE mixed 16 | 959 / 941 | 978 / 961 | +1.7% / +1.7% |
| FUSE sequential 1 | 733 / 371 | 737 / 355 | +0.5% / -4.3% |
| FUSE sequential 16 | 3,129 / 1,357 | 3,358 / 1,373 | +7.3% / +1.0% |

## File p95 and worker CPU

Each value is a median from the five-group matrix. The CPU column uses milliseconds of worker CPU per GiB of measured read and write traffic.

| Workload | LocalFile p95 read / write (ms) | Two-ring p95 read / write (ms) | Worker CPU LocalFile / two rings (ms/GiB) |
|---|---:|---:|---:|
| SDK mixed 1 | 8.0 / 16.4 | 7.2 / 16.4 | 1,067 / 1,147 |
| SDK mixed 16 | 46.1 / 50.1 | 48.5 / 56.4 | 1,614 / 1,753 |
| SDK sequential 1 | 7.3 / 13.4 | 5.6 / 14.4 | 1,080 / 1,180 |
| SDK sequential 16 | 29.7 / 63.6 | 22.4 / 65.5 | 1,600 / 1,800 |
| FUSE mixed 1 | 8.2 / 16.4 | 7.9 / 14.2 | 1,200 / 1,173 |
| FUSE mixed 16 | 44.7 / 59.5 | 43.6 / 63.9 | 1,637 / 1,453 |
| FUSE sequential 1 | 7.6 / 16.4 | 6.9 / 16.4 | 1,140 / 1,180 |
| FUSE sequential 16 | 23.7 / 83.4 | 24.7 / 96.5 | 1,660 / 1,800 |

## Host and verification

The NVMe host ran Linux 7.2.15-76070215-generic on an Intel Core i5-13420H with 12 logical CPUs. Trial data used ext4 on `/dev/nvme1n1p2`, backed by the CT1000P3PSSD8 NVMe drive. These are buffered E2E workloads, and sequential reads can take advantage of the page cache. The CPU and kernel also differ from the VM.

The SHA-256 manifest matched the original VM executables for both client transports and all three server builds. Each arm started an isolated master and worker. The SDK and actual mounted FUSE workloads ran sequential and 50/50 mixed traffic at one and 16 clients, with 64 files of 4 MiB, 1 MiB block and chunk sizes, and a 384 MiB mixed cap. FUSE used the same direct I/O and thread settings as the VM. A temporary privileged helper handled mount setup and teardown on this host.

All 192 main matrix arms passed, including 32 warmup arms. All 32 same-binary arms and eight smoke arms passed. The ring path accounted for every measured byte in enabled arms, and LocalFile reported zero ring bytes and descriptors. Both backend settings passed SDK SHA-256 readback with warm pages and after per-file cache eviction advice, plus mounted read and write verification. The private cluster directories and FUSE mounts were cleaned up.
