//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! dcache per-inode memory probe.
//!
//! Builds a synthetic dcache of N files spread over N/fanout directories twice:
//!
//! * `old`: the pre-optimisation layout — `HashMap<u64, Inode>` with the inode
//!   stored inline and a full backend `FileStatus` (path, name, ctime in
//!   `x_attr`, storage policy, ...) plus a `String` name in three places
//!   (inode, status, parent dentry key).
//! * `new`: the current layout — `Box<Inode>` in the table and `CachedStatus`
//!   (ctime inlined, user xattrs in a side map).
//!
//! Both variants use the real `Inode`/`CachedStatus`/`DirEntry`/`FileStatus`
//! types where they exist; only the `old` container mirrors the retired struct.
//!
//! `both` (the default) re-executes this binary once per variant, because the
//! allocator keeps freed pages mapped: measuring both in one process would
//! charge the second variant for the first one's peak.
//!
//! Usage (Linux; ~1 GB RSS for 1M inodes across the two runs):
//!
//! ```text
//! cargo run --release --example inode_mem_probe -p curvine-fuse -- \
//!     --count 1000000 --fanout 1000
//! ```
//!
//! Individual variant (useful with `perf stat` / `heaptrack`):
//!
//! ```text
//! cargo run --release --example inode_mem_probe -p curvine-fuse -- \
//!     --variant new --count 1000000
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::hint::black_box;
use std::mem::size_of;
use std::process::Command;
use std::time::Instant;

use curvine_fuse::fs::dcache::{CachedStatus, DirEntry, Inode, Lifecycle};
use curvine_model::{FileStatus, FileType, LocatedBlock, StoragePolicy, INTERNAL_CTIME_XATTR};
use curvine_runtime::common::FastHashMap;

const DEFAULT_COUNT: usize = 1_000_000;
const DEFAULT_FANOUT: usize = 1000;
const ROOT_INO: u64 = 1;

/// Backend status as the master returns it, including the internal ctime xattr
/// that the old dcache stored verbatim.
fn backend_status(name: &str, is_dir: bool) -> FileStatus {
    let mut x_attr = HashMap::new();
    x_attr.insert(
        INTERNAL_CTIME_XATTR.to_owned(),
        1_700_000_000_000_i64.to_le_bytes().to_vec(),
    );

    FileStatus {
        id: 42,
        // Real deployments carry a full namespace path in every status.
        path: format!("/warehouse/hive/warehouse/db.db/tbl/{name}"),
        name: name.to_owned(),
        is_dir,
        mtime: 1_700_000_000_000,
        atime: 1_700_000_000_000,
        children_num: 0,
        is_complete: !is_dir,
        len: 128 * 1024 * 1024,
        replicas: 1,
        block_size: 128 * 1024 * 1024,
        file_type: if is_dir {
            FileType::Dir
        } else {
            FileType::File
        },
        x_attr,
        storage_policy: StoragePolicy::default(),
        mode: if is_dir { 0o755 } else { 0o644 },
        owner: "root".to_owned(),
        group: "root".to_owned(),
        nlink: if is_dir { 2 } else { 1 },
        target: None,
    }
}

fn file_name(i: usize) -> String {
    format!("part-{:07}-c000.snappy.parquet", i)
}

// ---------------------------------------------------------------------------
// Old layout (pre 12cbf5657), mirrored field by field.
// ---------------------------------------------------------------------------

#[derive(Default)]
#[allow(dead_code)] // Mirrors the retired layout; fields exist only for `size_of`.
struct OldDirEntry {
    scan_complete: bool,
    children: BTreeMap<String, u64>,
    deleted_children: BTreeSet<String>,
}

#[derive(Default)]
#[allow(dead_code)] // Mirrors the retired layout; fields exist only for `size_of`.
struct OldInode {
    ino: u64,
    parent: u64,
    name: String,
    status: FileStatus,
    // `Box<Vec<_>>` is deliberate here: shrinking the inline struct is part of
    // what this probe measures, so the clippy suggestion does not apply.
    #[allow(clippy::box_collection)]
    locs: Option<Box<Vec<LocatedBlock>>>,
    lifecycle: Lifecycle,
    n_lookup: u64,
    ref_ctr: u64,
    last_access: u64,
    dir: Option<Box<OldDirEntry>>,
    mark_delete: bool,
    deferred_delete_path: Option<String>,
    deferred_delete_master_id: Option<i64>,
    master_id: Option<i64>,
}

type OldTree = FastHashMap<u64, OldInode>;
type NewTree = FastHashMap<u64, Box<Inode>>;

fn build_old(count: usize, fanout: usize) -> OldTree {
    let dir_count = count.div_ceil(fanout);
    let mut inodes: OldTree = FastHashMap::with_capacity(count + dir_count + 1);

    inodes.insert(
        ROOT_INO,
        OldInode {
            ino: ROOT_INO,
            name: "/".to_owned(),
            lifecycle: Lifecycle::Invalid,
            dir: Some(Box::new(OldDirEntry::default())),
            ..Default::default()
        },
    );

    // Directories first, so files can be linked into them.
    let mut dirs = Vec::with_capacity(dir_count);
    for d in 0..dir_count {
        let ino = ROOT_INO + 1 + d as u64;
        let name = format!("part-{:05}", d);
        let status = backend_status(&name, true);
        inodes.insert(
            ino,
            OldInode {
                ino,
                parent: ROOT_INO,
                // inode.name and the dentry key are separate allocations here.
                name: name.clone(),
                status,
                lifecycle: Lifecycle::Cached,
                n_lookup: 1,
                ref_ctr: 1,
                dir: Some(Box::new(OldDirEntry::default())),
                ..Default::default()
            },
        );
        inodes
            .get_mut(&ROOT_INO)
            .expect("root")
            .dir
            .as_mut()
            .expect("root dir")
            .children
            .insert(name, ino);
        dirs.push(ino);
    }

    for i in 0..count {
        let ino = ROOT_INO + 1 + dir_count as u64 + i as u64;
        let parent = dirs[i % dir_count];
        let name = file_name(i);
        // `name` is copied into status.name, inode.name and the dentry key.
        let status = backend_status(&name, false);
        inodes.insert(
            ino,
            OldInode {
                ino,
                parent,
                name: name.clone(),
                status,
                lifecycle: Lifecycle::Cached,
                n_lookup: 1,
                ref_ctr: 1,
                ..Default::default()
            },
        );
        inodes
            .get_mut(&parent)
            .expect("parent")
            .dir
            .as_mut()
            .expect("parent dir")
            .children
            .insert(name, ino);
    }

    inodes
}

fn build_new(count: usize, fanout: usize) -> NewTree {
    let dir_count = count.div_ceil(fanout);
    let mut inodes: NewTree = FastHashMap::with_capacity(count + dir_count + 1);
    inodes.insert(ROOT_INO, Box::new(Inode::new_root()));

    let mut dirs = Vec::with_capacity(dir_count);
    for d in 0..dir_count {
        let ino = ROOT_INO + 1 + d as u64;
        let name = format!("part-{:05}", d);
        let status = backend_status(&name, true);
        let key = Box::<str>::from(name.as_str());
        let inode = Inode::with_status(ino, ROOT_INO, key.clone(), status, 1);
        inodes.insert(ino, Box::new(inode));
        inodes
            .get_mut(&ROOT_INO)
            .expect("root")
            .dir
            .as_mut()
            .expect("root dir")
            .add_child(key, ino);
        dirs.push(ino);
    }

    for i in 0..count {
        let ino = ROOT_INO + 1 + dir_count as u64 + i as u64;
        let parent = dirs[i % dir_count];
        let name = file_name(i);
        let status = backend_status(&name, false);
        let key = Box::<str>::from(name.as_str());
        let inode = Inode::with_status(ino, parent, key.clone(), status, 1);
        inodes.insert(ino, Box::new(inode));
        inodes
            .get_mut(&parent)
            .expect("parent")
            .dir
            .as_mut()
            .expect("parent dir")
            .add_child(key, ino);
    }

    inodes
}

// ---------------------------------------------------------------------------
// RSS helpers (Linux /proc).
// ---------------------------------------------------------------------------

fn read_proc_status_kb(field: &str) -> Option<u64> {
    let text = fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(field) else {
            continue;
        };
        return rest.split_whitespace().next()?.parse().ok();
    }
    None
}

struct Rss {
    rss_kb: u64,
    hwm_kb: u64,
}

fn rss() -> Option<Rss> {
    Some(Rss {
        rss_kb: read_proc_status_kb("VmRSS:")?,
        // Peak is read before the structure is dropped, so it includes the
        // hash-table growth spikes that production also pays.
        hwm_kb: read_proc_status_kb("VmHWM:")?,
    })
}

fn main() {
    let args = Args::parse();

    if args.variant == "both" {
        run_both(&args);
        return;
    }

    let before = rss().expect("read /proc/self/status (this probe needs Linux)");
    let start = Instant::now();

    match args.variant.as_str() {
        "old" => {
            let tree = build_old(args.count, args.fanout);
            report("old", args.count, args.fanout, &before, start, tree.len());
            black_box(&tree);
            drop(tree);
        }
        "new" => {
            let tree = build_new(args.count, args.fanout);
            report("new", args.count, args.fanout, &before, start, tree.len());
            black_box(&tree);
            drop(tree);
        }
        other => {
            eprintln!("unknown --variant {other} (expected old|new|both)");
            std::process::exit(2);
        }
    }
}

fn report(label: &str, count: usize, fanout: usize, before: &Rss, start: Instant, entries: usize) {
    let after = rss().expect("read /proc/self/status");
    let delta = after.rss_kb.saturating_sub(before.rss_kb);
    let elapsed = start.elapsed();

    println!("---- variant={label} ----");
    println!(
        "count={count} fanout={fanout} entries={entries} elapsed={:.2}s allocator={}",
        elapsed.as_secs_f64(),
        curvine_alloc::allocator_type_name()
    );
    println!(
        "VmRSS before={} MB after={} MB delta={} MB",
        before.rss_kb / 1024,
        after.rss_kb / 1024,
        delta / 1024
    );
    println!("VmHWM (peak, incl. table growth) = {} MB", after.hwm_kb / 1024);
    println!(
        "per inode: {:.1} B (VmRSS delta / count)",
        (delta as f64 * 1024.0) / count as f64
    );
    println!();
}

fn run_both(args: &Args) {
    println!(
        "struct sizes: Inode={} CachedStatus={} FileStatus={} DirEntry={} Box<Inode>={} String={}",
        size_of::<Inode>(),
        size_of::<CachedStatus>(),
        size_of::<FileStatus>(),
        size_of::<DirEntry>(),
        size_of::<Box<Inode>>(),
        size_of::<String>(),
    );
    println!(
        "old mock struct sizes: OldInode={} OldDirEntry={}",
        size_of::<OldInode>(),
        size_of::<OldDirEntry>()
    );
    println!();

    let exe = std::env::current_exe().expect("current_exe");
    let mut per_inode = Vec::new();

    for variant in ["old", "new"] {
        let out = Command::new(&exe)
            .arg("--variant")
            .arg(variant)
            .arg("--count")
            .arg(args.count.to_string())
            .arg("--fanout")
            .arg(args.fanout.to_string())
            .output()
            .expect("spawn probe child");

        let stdout = String::from_utf8_lossy(&out.stdout);
        print!("{stdout}");
        eprint!("{}", String::from_utf8_lossy(&out.stderr));

        if !out.status.success() {
            eprintln!("variant {variant} failed: {}", out.status);
            std::process::exit(1);
        }

        match parse_per_inode(&stdout) {
            Some(bytes) => per_inode.push((variant, bytes)),
            None => eprintln!("variant {variant}: could not parse per-inode bytes"),
        }
    }

    if per_inode.len() == 2 {
        let old = per_inode[0].1;
        let new = per_inode[1].1;
        let saved = old - new;
        println!("---- summary ----");
        println!("old = {old:.1} B/inode, new = {new:.1} B/inode");
        println!(
            "saving = {saved:.1} B/inode = {:.1} MB at {} inodes",
            saved * args.count as f64 / (1024.0 * 1024.0),
            args.count
        );
    }
}

/// Extract the value from the `per inode: 123.4 B (...)` line.
fn parse_per_inode(stdout: &str) -> Option<f64> {
    stdout.lines().find_map(|line| {
        let rest = line.strip_prefix("per inode: ")?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

struct Args {
    variant: String,
    count: usize,
    fanout: usize,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            variant: "both".to_owned(),
            count: DEFAULT_COUNT,
            fanout: DEFAULT_FANOUT,
        };

        let mut it = std::env::args().skip(1);
        while let Some(flag) = it.next() {
            let mut value = || {
                it.next()
                    .unwrap_or_else(|| panic!("missing value for {flag}"))
            };
            match flag.as_str() {
                "--variant" => args.variant = value(),
                "--count" => args.count = value().parse().expect("--count must be a number"),
                "--fanout" => args.fanout = value().parse().expect("--fanout must be a number"),
                other => {
                    eprintln!("unknown argument {other}");
                    std::process::exit(2);
                }
            }
        }

        args.count = args.count.max(1);
        args.fanout = args.fanout.max(1);
        args
    }
}
