# Remaining gRPC CPU cost

## Implemented follow-ups

Both recommended experiments are complete and retained. Each used 30 fresh
Linux `tc` trials (five per variant/rate), identical LLVM release flags,
separate pinned worktrees, and the default read window of one. All 60 trials
had zero queue drops; their raw evidence and comparator results were
independently audited.

| Change | Profile | CPU/GiB change | Throughput change | P99 change |
| --- | --- | ---: | ---: | ---: |
| Tonic 0.14.9 / prost 0.14.4 upgrade | 1 Gbit/s | -21.4% | -0.2% | -5.8% |
| Tonic 0.14.9 / prost 0.14.4 upgrade | 10 Gbit/s | -14.9% | +12.4% | -11.1% |
| 256 KiB block response frames | 1 Gbit/s | -24.2% | +0.3% | -3.2% |
| 256 KiB block response frames | 10 Gbit/s | -42.2% | +29.8% | -18.5% |

Each row compares against that experiment's fresh preceding gRPC control;
the percentages are not additive or a matched cumulative comparison.
See the [upgrade report](results/tonic-0149-887c2584-2026-11-04/README.md)
and [frame-size report](results/grpc-frame-256k-5a9ba444-2026-11-04/README.md)
for absolute metrics, ranges, endpoint CPU, RSS and evidence. Separate packet
captures confirm actual 256 KiB DATA frames; captures are excluded from timing.

The final 10 Gbit/s checkpoint reaches 998.7 MB/s at 2.1125 CPU s/GiB. Its
throughput and P99 pass the provisional ORPC thresholds, but CPU is still
215.6% of ORPC against a 115% target. At 1 Gbit/s, CPU is 171.5% and P99 is
115.9% of ORPC, so both fail. Auto remains opt-in. Maintainer acceptance and
owned hardware-validation/fallback follow-up are still not supplied by these
results.

## Earlier source audit

The remainder preserves the investigation at `6a6654cd`, before the measured
upgrade above. Its tonic 0.9 observations and control numbers are historical.

Follow-up to [review 5747067746](https://github.com/CurvineIO/curvine/pull/1957#pullrequestreview-5747067746), audited on 2026-11-04. This is a source audit, not a new benchmark result. Existing reports describe their pinned revisions; merging newer upstream changes does not make those measurements current-head results.

## Acceptance remains open

The default read window stays at one; auto remains explicit opt-in. In the last adaptive experiment, the default gRPC control used 4.25 CPU s/GiB at 10 Gbit/s, while ORPC used 0.925. The 115% target is 1.06375 CPU s/GiB, requiring about a 75% reduction from that gRPC control. Neither a generic checkpoint pass nor improved pipelined throughput establishes this gate.

## Copies still present

These observations use the resolved tonic 0.9.2, prost 0.11.9 and bytes 1.12.1 sources:

- Worker `DataSlice::Buffer::to_bytes` freezes its `BytesMut`; the adapter does not copy the payload. See `crates/core/curvine-io/src/data_slice.rs`.
- Server prost encoding copies the payload into tonic's contiguous output buffer (`tonic/src/codec/prost.rs`, `codec/encode.rs`; `prost/src/encoding.rs`).
- Tonic receives HTTP/2 fragments into its own buffer with `extend_from_slice` (`tonic/src/codec/decode.rs`).
- Prost's Bytes decoding calls `copy_to_bytes`, but tonic 0.9's opaque `DecodeBuf` implements only `remaining`, `chunk` and `advance` (`tonic/src/codec/buffer.rs`). It therefore uses the default copying implementation from `bytes::Buf`. Merely selecting protobuf `Bytes` fields or an ordinary custom codec does not remove that copy.
- The benchmark's `Reader::read_full` copies data into the caller's output slice. Returning owned chunks through `Reader::async_read` serves a different API contract and cannot substitute for this workload.

## Newer tonic releases

The follow-up source check found relevant upstream improvements. The latest non-yanked stable release checked on 2026-11-04 is [tonic 0.14.9](https://github.com/grpc/grpc-rust/releases/tag/tonic-v0.14.9), published 2026-10-29. No dependency upgrade or new performance measurement is included in this audit.

| Release | Relevant change | Implication for this workload |
| --- | --- | --- |
| 0.9.2 (current) through 0.11 | `DecodeBuf` lacks an ownership-preserving `copy_to_bytes` override | Selecting protobuf `Bytes` still leaves an extra payload copy during decoding |
| 0.12.0 | [Delegates codec buffer operations to the underlying buffer](https://github.com/grpc/grpc-rust/pull/1695), including `DecodeBuf::copy_to_bytes` and `EncodeBuf::put` | Can remove that decode copy and improve encode reservation; does not eliminate receive accumulation, protobuf encoding, kernel or caller-buffer copies |
| 0.14.6 | Adds [`Endpoint::max_frame_size`](https://github.com/grpc/grpc-rust/releases/tag/tonic-v0.14.6) | Allows the client to advertise larger inbound HTTP/2 frames for block responses; actual frame sizing and CPU benefit must be measured |
| 0.14.9 | Contains both changes and subsequent fixes, including the 0.14.8 encoder unwind fix | Preferred released candidate for an upgrade experiment |

The exact [0.14.9 codec implementation](https://github.com/grpc/grpc-rust/blob/tonic-v0.14.9/tonic/src/codec/buffer.rs) retains the delegation. Its [receive path](https://github.com/grpc/grpc-rust/blob/tonic-v0.14.9/tonic/src/codec/decode.rs) still accumulates HTTP body data into a buffer. Therefore this is a specific copy reduction, not an end-to-end zero-copy transport.

Upgrading is more than changing the version string:

- Tonic 0.12 moves to the Hyper 1 / HTTP 1 generation. Tonic 0.14 separates protobuf integration into `tonic-prost` and `tonic-prost-build`, using prost 0.14. Both protocol build scripts, runtime dependencies, health/reflection and TLS features need coordinated updates. Preserve the lightweight API crate boundaries.
- The Raft build currently maps `.eraftpb` directly to `raft::eraftpb`. Raft 0.7 uses prost 0.11 types, so changing shared prost to 0.14 breaks that trait boundary. The upgrade needs an explicit compatible conversion or generation strategy; adding a second prost dependency alone does not fix embedded generated message types. Include persistent-log, snapshot and mixed-version wire tests in validation.
- Tonic 0.14.9 declares Rust 1.88 as its minimum; the repository pins Rust 1.98, so the checked repository toolchain satisfies it.

First benchmark an upgrade to 0.14.9 with current settings and read window one. Then benchmark client frame-size tuning as a separate change, retaining the upgrade-only binary as its control. Use fresh matched upstream ORPC and tonic 0.9 controls from the same upstream revision, identical LLVM release flags, Linux `tc` at 1/10 Gbit/s, five alternating trials per variant/profile and the unchanged `read_full` workload. Verify fragmented and multiple-message decoding, cancellation and buffer ownership before timing. Measure endpoint and combined CPU/GiB, throughput, P99, RSS and queue drops. Keep auto sizing opt-in throughout.

## Other candidate changes

| Candidate | Expected mechanism | Limitation |
| --- | --- | --- |
| Pre-reserve server encode capacity | A project-owned encoder can call `EncodeBuf::reserve(encoded_len)` before prost encoding to avoid repeated growth | Reassess after the newer tonic experiment: upstream buffer delegation may already address part of this; retains the full encode copy and reuse may limit the benefit |
| Bounded worker buffer pool | Reduce allocation/first-touch costs when buffers overlap | `LocalFile::read_full` already retains a buffer remainder, and `BytesMut::reserve` can reclaim it after ownership becomes unique; first prove allocations remain significant |

No CPU improvement is claimed for these candidates. The existing profile shows substantial copying and kernel work but does not attribute enough cost to any one candidate to predict gate closure. The rejected executor and server receive-frame setter are not proposed again; the newer client setter affects the receiving side of block reads.

An eventual experiment should compare matched upstream ORPC, the preceding gRPC checkpoint and the changed default-window implementation, using identical release flags and the existing `read_full` workload. Record endpoint CPU separately as well as combined CPU/GiB, throughput, P99, RSS, queue drops and allocation/copy profiles. Keep diagnostic profiling separate from acceptance timing. Retain no change solely because it improves throughput.

Until default-mode measurements pass, merge requires the reviewer's alternative: explicit maintainer acceptance with an owned hardware-validation/fallback follow-up. This audit does not supply that acceptance.
