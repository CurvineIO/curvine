// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Benchmark-style integration test: `InodeView` encoded with the new
//! `curvine-codec` vs the legacy bincode 1.3.3 encoding used in production
//! (the `SerdeUtils::serialize` path).
//!
//! For every sample shape the test first round-trips the value through both
//! codecs (correctness guard, aborts before any number is printed) and then
//! measures serialization/deserialization wall time and process CPU time,
//! reporting payload sizes and per-operation medians.
//!
//! Run with:
//! `cargo test --release -p curvine-master --test inode_codec_bench -- --nocapture`

use curvine_codec::{Codec, Decode, Reader, CURRENT_VERSION};
use curvine_master::master::meta::inode::{InodeDir, InodeFile, InodeView};
use curvine_master::master::meta::BlockMeta;
use curvine_model::{
    CreateFileOpts, FileAllocMode, FileAllocOpts, MkdirOpts, WorkerAddress, INTERNAL_CTIME_XATTR,
};
use std::hint::black_box;
use std::time::{Duration, Instant};

const ROUNDS: usize = 5;
/// Wall-time budget per round: long enough that `ROUNDS` rounds together
/// span several OS CPU-time accounting ticks (~15.6 ms on Windows), so the
/// aggregated CPU delta is meaningful even for sub-microsecond payloads.
const ROUND_MS: u64 = 40;
/// Operations between clock reads inside a round: keeps clock overhead
/// negligible (~25 ns / 64 ops) even for the smallest payloads.
const BATCH: usize = 64;
/// Untimed warmup operations before the first round.
const WARMUP: usize = 200;

const MB: i64 = 1024 * 1024;
const T0: i64 = 1_760_000_000_000;

#[test]
fn inode_view_codec_vs_bincode_bench() {
    let samples = build_samples();

    // Correctness guard before timing: every sample must round-trip through
    // both codecs. A failure aborts before any numbers are printed.
    for sample in &samples {
        assert_codec_round_trip(sample.name, &sample.view);
        assert_bincode_round_trip(sample.name, &sample.view);
    }

    let mut rows = Vec::with_capacity(samples.len());
    for sample in &samples {
        let view = &sample.view;
        let codec_bytes = Codec::encode_unversioned(view).expect("codec encode");
        let persisted_bytes = Codec::encode(CURRENT_VERSION, view).expect("codec persisted encode");
        let bincode_bytes = bincode::serialize(view).expect("bincode encode");

        let codec_encode = measure(|| {
            Codec::encode_unversioned(black_box(view))
                .expect("codec encode")
                .len()
        });
        let bincode_encode = measure(|| {
            bincode::serialize(black_box(view))
                .expect("bincode encode")
                .len()
        });
        let codec_decode = measure(|| {
            let decoded = decode_payload(black_box(codec_bytes.as_slice()));
            decoded.id() as usize
        });
        let bincode_decode = measure(|| {
            let decoded: InodeView =
                bincode::deserialize(black_box(bincode_bytes.as_slice())).expect("bincode decode");
            decoded.id() as usize
        });

        rows.push(Row {
            name: sample.name,
            codec_bytes: codec_bytes.len(),
            persisted_bytes: persisted_bytes.len(),
            bincode_bytes: bincode_bytes.len(),
            codec_encode,
            bincode_encode,
            codec_decode,
            bincode_decode,
        });
    }

    print_report(&rows);
}

// --- samples ---

struct Sample {
    name: &'static str,
    view: InodeView,
}

fn worker(id: u32) -> WorkerAddress {
    WorkerAddress {
        worker_id: id,
        hostname: format!("worker-{id}"),
        ip_addr: "127.0.0.1".to_string(),
        rpc_port: 8996,
        control_rpc_port: None,
        web_port: 8997,
    }
}

fn build_samples() -> Vec<Sample> {
    vec![
        Sample {
            name: "file-ufs-8blk",
            view: make_file_ufs_8blk(),
        },
        Sample {
            name: "file-writing-alloc",
            view: make_file_writing_alloc(),
        },
        Sample {
            name: "file-1024blk",
            view: make_file_1024blk(),
        },
        Sample {
            name: "file-512b",
            view: make_file_512b(),
        },
        Sample {
            name: "link-file",
            view: make_link_file(),
        },
        Sample {
            name: "dir-with-xattr",
            view: make_dir(),
        },
        Sample {
            name: "file-entry",
            view: make_file_entry(),
        },
    ]
}

/// Completed UFS-backed file: 8 blocks covering the three `BlockMeta` shapes
/// (committed / pre-assigned / allocated), two user xattrs, named owner.
fn make_file_ufs_8blk() -> InodeView {
    let mut opts = CreateFileOpts::with_create(false);
    opts.replicas = 2;
    opts.block_size = 4 * MB;
    opts.owner = "alice".to_string();
    opts.group = "analytics".to_string();
    opts.mode = 0o644;
    opts.sync_ufs_meta = true;
    opts.ufs_len = 8 * 4 * MB;
    opts.x_attr
        .insert("user.hive.table".to_string(), b"sales".to_vec());
    opts.x_attr.insert(
        "user.hive.partition".to_string(),
        b"dt=2026-10-16/country=CN".to_vec(),
    );

    let file_id = 2001;
    let mut file = InodeFile::with_opts(file_id, T0, opts);
    for seq in 0..8i64 {
        let block_id = file_id * 1_000 + seq;
        let block = match seq % 3 {
            // Committed block: locations and alloc opts already cleared.
            0 => BlockMeta::new(block_id, 4 * MB),
            // Pre-assigned: locations known, no alloc opts.
            1 => BlockMeta::with_pre(block_id, &[worker(1), worker(2)]),
            // Allocated in flight: alloc opts plus assigned locations.
            _ => {
                let mut block = BlockMeta::with_alloc(
                    block_id,
                    FileAllocOpts::with_alloc(4 * MB, FileAllocMode::DEFAULT),
                );
                assert!(block.assign_worker(&[worker(3)]));
                block
            }
        };
        file.add_block(block);
    }

    let mut view = InodeView::new_file("part-00000.parquet".to_string(), file);
    view.set_parent_id(1000);
    view
}

/// In-flight file: writer present (`file_write` = Some), 4 allocated blocks
/// with worker assignments, internal ctime xattr set.
fn make_file_writing_alloc() -> InodeView {
    let mut opts = CreateFileOpts::with_create(false);
    opts.replicas = 3;
    opts.block_size = 128 * MB;
    opts.client_name = "client-7f3a9d".to_string();
    opts.owner = "bob".to_string();
    opts.group = "default".to_string();
    opts.mode = 0o660;
    opts.x_attr.insert(
        INTERNAL_CTIME_XATTR.to_string(),
        (T0 + 1_000).to_le_bytes().to_vec(),
    );

    let file_id = 3001;
    let mut file = InodeFile::with_opts(file_id, T0 + 1_000, opts);
    for seq in 0..4i64 {
        let block_id = file_id * 1_000 + seq;
        let mut block = BlockMeta::with_alloc(
            block_id,
            FileAllocOpts {
                truncate: false,
                off: 0,
                len: 128 * MB,
                mode: FileAllocMode::ZERO_RANGE,
            },
        );
        assert!(block.assign_worker(&[worker(4), worker(5)]));
        file.add_block(block);
    }

    let mut view = InodeView::new_file("ongoing-upload.tmp".to_string(), file);
    view.set_parent_id(2002);
    view
}

/// Large completed file: 1024 blocks, the shape that dominates real meta DB
/// values (every 16th block still carries alloc opts).
fn make_file_1024blk() -> InodeView {
    const BLOCKS: i64 = 1024;
    let mut opts = CreateFileOpts::with_create(false);
    opts.replicas = 2;
    opts.block_size = 4 * MB;
    opts.sync_ufs_meta = true;
    opts.ufs_len = BLOCKS * 4 * MB;

    let file_id = 4001;
    let mut file = InodeFile::with_opts(file_id, T0 + 2_000, opts);
    for seq in 0..BLOCKS {
        let block_id = file_id * 1_000_000 + seq;
        let block = if seq % 16 == 0 {
            let mut block = BlockMeta::with_alloc(
                block_id,
                FileAllocOpts::with_alloc(4 * MB, FileAllocMode::DEFAULT),
            );
            assert!(block.assign_worker(&[worker(1)]));
            block
        } else {
            BlockMeta::new(block_id, 4 * MB)
        };
        file.add_block(block);
    }

    let mut view = InodeView::new_file("checkpoint-1024.blk".to_string(), file);
    view.set_parent_id(1000);
    view
}

/// Completed file with `blocks` committed blocks and no xattrs, so the codec
/// payload grows predictably (~18 B per block) with the block count.
fn make_file_blocks(file_id: i64, blocks: usize) -> InodeView {
    let mut opts = CreateFileOpts::with_create(false);
    opts.replicas = 2;
    opts.block_size = 4 * MB;
    opts.sync_ufs_meta = true;
    opts.ufs_len = (blocks as i64) * 4 * MB;
    let mut file = InodeFile::with_opts(file_id, T0, opts);
    for seq in 0..blocks as i64 {
        file.add_block(BlockMeta::new(file_id * 1_000_000 + seq, 4 * MB));
    }
    let mut view = InodeView::new_file("segment.bin".to_string(), file);
    view.set_parent_id(1000);
    view
}

/// Completed file tuned so the codec payload lands closest to 512 bytes —
/// roughly the size of a mid-sized inode value in the meta DB. The block count
/// is chosen by probing the encoder, so the reported record really is ~512 B.
fn make_file_512b() -> InodeView {
    const TARGET: usize = 512;
    let mut best = make_file_blocks(7001, 1);
    let mut best_diff = usize::MAX;
    for n in 1..=48 {
        let view = make_file_blocks(7001, n);
        let size = Codec::encode_unversioned(&view)
            .map(|b| b.len())
            .unwrap_or(usize::MAX);
        let diff = size.abs_diff(TARGET);
        if diff < best_diff {
            best_diff = diff;
            best = view;
        }
        if size > TARGET {
            break;
        }
    }
    best
}

/// Symlink: `target` = Some, `FileType::Link`.
fn make_link_file() -> InodeView {
    let file = InodeFile::with_link(
        6001,
        T0 + 3_000,
        "/ufs/warehouse/sales/part-00000.parquet",
        0o777,
        Some("dave".to_string()),
        Some("eng".to_string()),
    );
    let mut view = InodeView::new_file("latest.parquet".to_string(), file);
    view.set_parent_id(1000);
    view
}

/// Directory with xattrs (including the internal ctime marker) and ACL.
fn make_dir() -> InodeView {
    let mut opts = MkdirOpts::with_create(false);
    opts.mode = 0o755;
    opts.owner = "carol".to_string();
    opts.group = "eng".to_string();
    opts.x_attr.insert("user.tag".to_string(), b"hot".to_vec());
    opts.x_attr
        .insert("user.quota".to_string(), 1024u64.to_le_bytes().to_vec());
    opts.x_attr.insert(
        INTERNAL_CTIME_XATTR.to_string(),
        (T0 + 4_000).to_le_bytes().to_vec(),
    );

    let dir = InodeDir::with_opts(5001, T0 + 4_000, opts);
    let mut view = InodeView::new_dir("warehouse".to_string(), dir);
    view.set_parent_id(1000);
    view
}

/// Tiny entry shape: name + id only.
fn make_file_entry() -> InodeView {
    InodeView::new_entry("part-00000.parquet".to_string(), 2001)
}

// --- round-trip guards ---

fn decode_payload(bytes: &[u8]) -> InodeView {
    let mut reader = Reader::new(bytes, CURRENT_VERSION).expect("codec reader");
    InodeView::decode(&mut reader).expect("codec decode")
}

fn assert_codec_round_trip(label: &str, view: &InodeView) {
    let bytes = Codec::encode_unversioned(view).expect("codec encode");
    let decoded = decode_payload(&bytes);
    assert_view_eq(label, view, &decoded);

    let again = Codec::encode_unversioned(&decoded).expect("codec re-encode");
    // Maps inside features may iterate in a different order after a round
    // trip, so compare lengths instead of full bytes.
    assert_eq!(again.len(), bytes.len(), "{label}: codec re-encode size");
}

fn assert_bincode_round_trip(label: &str, view: &InodeView) {
    let bytes = bincode::serialize(view).expect("bincode encode");
    let decoded: InodeView = bincode::deserialize(&bytes).expect("bincode decode");
    assert_view_eq(label, view, &decoded);
}

fn assert_view_eq(label: &str, left: &InodeView, right: &InodeView) {
    assert_eq!(left.id(), right.id(), "{label}: id");
    assert_eq!(left.name(), right.name(), "{label}: name");
    assert_eq!(left.mtime(), right.mtime(), "{label}: mtime");
    assert_eq!(left.atime(), right.atime(), "{label}: atime");
    assert_eq!(left.child_len(), right.child_len(), "{label}: child_len");
    if !left.is_file_entry() {
        assert_eq!(
            left.x_attr().unwrap(),
            right.x_attr().unwrap(),
            "{label}: x_attr"
        );
        assert_eq!(
            left.nlink().unwrap(),
            right.nlink().unwrap(),
            "{label}: nlink"
        );
    }
    match (left, right) {
        (InodeView::File(a), InodeView::File(b)) => {
            assert_eq!(a.file.block_ids(), b.file.block_ids(), "{label}: blocks");
            assert_eq!(
                a.file.compute_len(),
                b.file.compute_len(),
                "{label}: file len"
            );
            assert_eq!(
                a.file.is_writing(),
                b.file.is_writing(),
                "{label}: writing state"
            );
        }
        (InodeView::Dir(_), InodeView::Dir(_)) => {}
        (InodeView::FileEntry(a), InodeView::FileEntry(b)) => {
            assert_eq!(a.id, b.id, "{label}: entry id");
        }
        _ => panic!("{label}: variant mismatch after round trip"),
    }
}

// --- measurement ---

struct Stat {
    wall_ns: f64,
    cpu_ns: Option<f64>,
}

fn measure<F: FnMut() -> usize>(mut op: F) -> Stat {
    // Warmup outside the timed rounds.
    for _ in 0..WARMUP {
        black_box(op());
    }

    // Wall time is reported per round (median); CPU time is aggregated over
    // all rounds because the OS updates process CPU counters in coarse ticks.
    let cpu_start = process_cpu_ns();
    let mut walls = Vec::with_capacity(ROUNDS);
    let mut total_iters = 0u64;
    let mut sink = 0usize;
    for _ in 0..ROUNDS {
        let start = Instant::now();
        let mut iters = 0u64;
        while start.elapsed() < Duration::from_millis(ROUND_MS) {
            for _ in 0..BATCH {
                sink = sink.wrapping_add(black_box(op()));
                iters += 1;
            }
        }
        let elapsed = start.elapsed();
        walls.push(elapsed.as_nanos() as f64 / iters as f64);
        total_iters += iters;
    }
    let cpu_end = process_cpu_ns();
    black_box(sink);

    Stat {
        wall_ns: median(&mut walls),
        cpu_ns: match (cpu_start, cpu_end) {
            (Some(a), Some(b)) => Some((b - a) as f64 / total_iters as f64),
            _ => None,
        },
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

/// Total process CPU time (kernel + user) in nanoseconds, or `None` when the
/// platform has no implementation.
#[cfg(windows)]
fn process_cpu_ns() -> Option<u64> {
    #[repr(C)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
        fn GetProcessTimes(
            process: *mut core::ffi::c_void,
            creation: *mut FileTime,
            exit: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
    }

    let mut creation = FileTime { low: 0, high: 0 };
    let mut exit = FileTime { low: 0, high: 0 };
    let mut kernel = FileTime { low: 0, high: 0 };
    let mut user = FileTime { low: 0, high: 0 };
    // SAFETY: the kernel writes only into the four out-parameters and the
    // pseudo handle returned by GetCurrentProcess is always valid.
    let ok = unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    };
    if ok == 0 {
        return None;
    }
    let kernel = ((kernel.high as u64) << 32) | kernel.low as u64;
    let user = ((user.high as u64) << 32) | user.low as u64;
    // FILETIME counts 100 ns ticks.
    Some((kernel + user) * 100)
}

#[cfg(not(windows))]
fn process_cpu_ns() -> Option<u64> {
    None
}

// --- report ---

struct Row {
    name: &'static str,
    codec_bytes: usize,
    persisted_bytes: usize,
    bincode_bytes: usize,
    codec_encode: Stat,
    bincode_encode: Stat,
    codec_decode: Stat,
    bincode_decode: Stat,
}

fn print_report(rows: &[Row]) {
    println!();
    println!("================================================================");
    println!(" InodeView codec vs bincode benchmark (curvine-master)");
    println!(" rounds: {ROUNDS} x {ROUND_MS}ms per op; wall = median of rounds");
    println!(" cpu = process CPU time aggregated over all rounds");
    println!(" codec payload = bare encode; +hdr adds the 1-byte persisted header");
    println!("================================================================");

    println!();
    println!("[1] payload size (bytes)");
    println!(
        "{:<22} {:>10} {:>10} {:>12} {:>9} {:>8}",
        "sample", "codec", "+hdr", "bincode", "delta", "ratio"
    );
    for row in rows {
        let delta =
            (row.codec_bytes as f64 - row.bincode_bytes as f64) / row.bincode_bytes as f64 * 100.0;
        let ratio = row.codec_bytes as f64 / row.bincode_bytes as f64;
        println!(
            "{:<22} {:>10} {:>10} {:>12} {:>8.1}% {:>7.2}x",
            row.name, row.codec_bytes, row.persisted_bytes, row.bincode_bytes, delta, ratio
        );
    }
    let codec_total: usize = rows.iter().map(|r| r.codec_bytes).sum();
    let bincode_total: usize = rows.iter().map(|r| r.bincode_bytes).sum();
    println!(
        "{:<22} {:>10} {:>10} {:>12} {:>8.1}% {:>7.2}x",
        "TOTAL",
        codec_total,
        codec_total + rows.len(),
        bincode_total,
        (codec_total as f64 - bincode_total as f64) / bincode_total as f64 * 100.0,
        codec_total as f64 / bincode_total as f64
    );

    print_timing_table("[2] encode (ns/op)", rows, |r| {
        (&r.codec_encode, &r.bincode_encode)
    });
    print_timing_table("[3] decode (ns/op)", rows, |r| {
        (&r.codec_decode, &r.bincode_decode)
    });
}

fn print_timing_table(title: &str, rows: &[Row], pick: fn(&Row) -> (&Stat, &Stat)) {
    println!();
    println!("{title}");
    println!(
        "{:<22} {:>12} {:>12} {:>14} {:>14} {:>10} {:>9}",
        "sample", "codec wall", "codec cpu", "bincode wall", "bincode cpu", "wall x", "cpu x"
    );
    for row in rows {
        let (codec, bincode) = pick(row);
        let wall_ratio = codec.wall_ns / bincode.wall_ns;
        let cpu_ratio = match (codec.cpu_ns, bincode.cpu_ns) {
            (Some(c), Some(b)) if b > 0.0 => format!("{:.2}x", c / b),
            _ => "n/a".to_string(),
        };
        println!(
            "{:<22} {:>12.0} {:>12} {:>14.0} {:>14} {:>9.2}x {:>9}",
            row.name,
            codec.wall_ns,
            fmt_ns(codec.cpu_ns),
            bincode.wall_ns,
            fmt_ns(bincode.cpu_ns),
            wall_ratio,
            cpu_ratio
        );
    }
}

fn fmt_ns(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{v:.0}"),
        None => "n/a".to_string(),
    }
}
