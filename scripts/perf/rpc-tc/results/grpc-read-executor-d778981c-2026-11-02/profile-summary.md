# Diagnostic profile before the read experiments

Profiled reviewed gRPC `0eeb5054` separately from acceptance measurements, using the immutable matched LLVM binaries from the previous report. Linux `tc` shaped each direction to 10 Gbit/s and 1 ms; four readers performed 4,096 measured 1 MiB reads each (16 GiB total), after warmup. Short-circuit and read-ahead were disabled; the compiled read window was one.

`perf record -e cpu-clock -F 99 --call-graph dwarf,16384 -p WORKER_PID,CLIENT_PID` attached after the timed-read start signal. It captured 6,246 samples with zero lost samples. The benchmark completed and its processes/namespaces were cleaned up. Profiling overhead means this run is diagnostic only and is excluded from every performance table and gate.

The following are self-sample percentages across both attached processes; they are not percentages of worker-only CPU. Client thread names `rpc-client` and `rpc_tc_read` belong to the same client process.

| Process/thread | Symbol | Self samples |
| --- | --- | ---: |
| worker | kernel `rep_movs_alternative` | 9.70% |
| worker | libc `__memmove_avx_unaligned_erms` | 8.42% |
| client runtime | libc `__memmove_avx_unaligned_erms` | 7.81% |
| worker | kernel `_raw_spin_unlock_irqrestore` | 4.18% |
| client main | libc `__memmove_avx_unaligned_erms` | 4.07% |
| client runtime | kernel `rep_movs_alternative` | 3.95% |
| client runtime | kernel `_raw_spin_unlock_irqrestore` | 2.96% |
| worker | kernel `kernel_init_pages` | 2.72% |
| client runtime | kernel `native_queued_spin_lock_slowpath` | 1.92% |
| worker | kernel `memset_orig` | 1.65% |
| worker | kernel `native_queued_spin_lock_slowpath` | 1.63% |
| worker | kernel `handle_softirqs` | 1.55% |
| client runtime | kernel `handle_softirqs` | 1.22% |
| client runtime | kernel `finish_task_switch.isra.0` | 1.04% |

The worker kernel-copy call paths include file reads into userspace and socket sends. Copies, allocation/page initialization and kernel synchronization are visible costs. This profile does not prove that scheduler handoffs or HTTP/2 framing are the primary bottleneck, nor that a dedicated executor will improve total CPU. The executor is evaluated as a separate measured experiment.

No tonic patch or dependency upgrade is included. In tonic 0.9, changing the server receive frame limit alone does not enlarge the frames sent to the client: the receiver advertises that setting, and the Endpoint builder has no corresponding setter. The server-only suggestion therefore cannot be claimed as a download optimization. Raw stack samples and process logs are excluded from the published artifacts.
