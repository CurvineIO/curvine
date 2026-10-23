// Measures the in-memory footprint of the master namespace tree.
//
// The production Packed layout is measured against the Map container back to
// back inside one (ignored) test so both RSS deltas share the same process and
// allocator warm-up:
//
//   Map    - `BTreeMap<String, Box<InodeView>>`; files compacted to
//            `FileEntry`. The name is retained twice: map key + inode name.
//   Packed - variable-length name+id records in 4 KB-bounded chunks plus a
//            side vector holding directory objects. Design:
//            docs/design/inode-children-packed-arena.md
//
// Scenario: 10,000,000 files with 100 files per directory, directory names
//           10 bytes and file names 20 bytes
//           => 100,000 directories + 1 root directory + 10,000,000 file entries.
//
// Memory is reported as process RSS (working set) delta, so allocator padding,
// Vec spare capacity and name buffers are all included.
//
// Layouts to measure are picked with CURVINE_MEM_TEST_LAYOUT=list|map|packed|both.
// The test defaults to Map then Packed, so the Packed baseline already carries
// Map's unreclaimed pages (the delta stays accurate, the baseline does not).
//
// Run (ignored by default because it allocates > 2 GiB):
//   cargo test -p curvine-master --release --test dir_tree_memory -- --ignored --nocapture

use curvine_config::ChildrenContainer;
use curvine_master::master::meta::inode::{
    InodeChildren, InodeDir, InodeFile, InodePtr, InodeView, NamedDir, NamedEntry, ROOT_INODE_ID,
    ROOT_INODE_NAME,
};
use curvine_sys::SysUtils;
use std::mem::size_of;
use std::time::{Duration, Instant};

const DIR_COUNT: usize = 100_000;
const FILES_PER_DIR: usize = 100;

const DIR_NAME_LEN: usize = 10;
const FILE_NAME_LEN: usize = 20;

const MIB: f64 = 1024.0 * 1024.0;

// Packed records mirror inodes_children.rs: 1-byte key len + 1-byte value len,
// then kind + id (+ dir slot).
const PACKED_FILE_HEADER: usize = 11;
const PACKED_DIR_HEADER: usize = 15;

fn label(container: ChildrenContainer) -> &'static str {
    match container {
        ChildrenContainer::List => "List",
        ChildrenContainer::Map => "Map",
        ChildrenContainer::Packed => "Packed",
    }
}

fn children(container: ChildrenContainer) -> InodeChildren {
    InodeChildren::new(container)
}

fn dir_name(i: usize) -> String {
    let name = format!("dir-{i:06}");
    debug_assert_eq!(name.len(), DIR_NAME_LEN);
    name
}

fn file_name(j: usize) -> String {
    let name = format!("file-{j:015}");
    debug_assert_eq!(name.len(), FILE_NAME_LEN);
    name
}

fn rss_mib() -> f64 {
    SysUtils::used_memory() as f64 / MIB
}

// Structural accounting from the tree itself. This is a lower bound of the heap
// payload: it counts boxed inodes, boxed named wrappers and name buffers, but
// not Vec spare capacity, chunk slack or allocator rounding (those only show up
// in RSS).
#[derive(Default)]
struct Accounting {
    dirs: usize,
    files: usize,
    name_bytes: usize,
    payload_bytes: usize,
}

fn account_tree(root: &InodeView, container: ChildrenContainer) -> Accounting {
    let mut acc = Accounting::default();
    let mut stack: Vec<InodePtr> = vec![InodePtr::from_ref(root)];

    // Per child slot the tree retains: one side-vector/Vec pointer slot, the
    // boxed InodeView, its boxed named payload and one copy of the name. Packed
    // additionally stores the name (and id) a second time in the record area;
    // Map keeps a second name copy as the BTreeMap key.
    let entry_overhead = size_of::<Box<InodeView>>() + size_of::<InodeView>();
    let packed = container == ChildrenContainer::Packed;
    let mapped = container == ChildrenContainer::Map;

    while let Some(node) = stack.pop() {
        let InodeView::Dir(dir) = node.as_ref() else {
            continue;
        };
        for child in dir.children_iter() {
            let is_dir = child.as_ref().is_dir();
            match child.as_ref() {
                InodeView::Dir(d) => {
                    acc.dirs += 1;
                    acc.name_bytes += d.name.len();
                    acc.payload_bytes += entry_overhead + size_of::<NamedDir>() + d.name.len();
                    if packed {
                        acc.payload_bytes += PACKED_DIR_HEADER + d.name.len();
                    } else {
                        acc.payload_bytes += size_of::<String>() + d.name.len();
                    }
                }
                InodeView::FileEntry(e) if packed => {
                    // Packed file edge: key/value lengths | name | kind + id.
                    acc.files += 1;
                    acc.name_bytes += e.name.len();
                    acc.payload_bytes += PACKED_FILE_HEADER + e.name.len();
                }
                InodeView::FileEntry(e) => {
                    // Files are compacted to FileEntry; the inode keeps one name
                    // copy and Map keeps another one as the key.
                    acc.files += 1;
                    acc.name_bytes += e.name.len();
                    acc.payload_bytes += entry_overhead + size_of::<NamedEntry>() + e.name.len();
                    if mapped {
                        acc.payload_bytes += size_of::<String>() + e.name.len();
                    }
                }
                InodeView::File(_) => unreachable!("files are compacted to FileEntry"),
            }
            if is_dir {
                stack.push(child);
            }
        }
    }

    acc
}

fn print_struct_sizes() {
    println!("struct sizes (bytes):");
    println!("  InodeView      = {}", size_of::<InodeView>());
    println!("  InodeDir       = {}", size_of::<InodeDir>());
    println!("  InodeFile      = {}", size_of::<InodeFile>());
    println!("  NamedDir       = {}", size_of::<NamedDir>());
    println!("  NamedEntry     = {}", size_of::<NamedEntry>());
    println!("  Box<InodeView> = {}", size_of::<Box<InodeView>>());
}

fn measure(container: ChildrenContainer, dir_count: usize) {
    let rss_start = rss_mib();

    // Phase 1: root + `dir_count` empty directories.
    let t = Instant::now();
    let mut root = InodeView::new_dir(
        ROOT_INODE_NAME.to_string(),
        InodeDir::with_children(ROOT_INODE_ID, 0, children(container)),
    );
    let mut next_id: i64 = ROOT_INODE_ID + 1;
    for i in 0..dir_count {
        let dir = InodeView::new_dir(
            dir_name(i),
            InodeDir::with_children(next_id, 0, children(container)),
        );
        next_id += 1;
        root.add_child(dir).unwrap();
    }
    let dir_time: Duration = t.elapsed();
    let rss_dirs = rss_mib();

    // Phase 2: `FILES_PER_DIR` files per directory. `add_child` returns the
    // owned full InodeView for files, which is dropped here just like the
    // master drops it after persisting to RocksDB.
    let t = Instant::now();
    for i in 0..dir_count {
        let dir = root
            .get_child(&dir_name(i))
            .expect("directory should exist");
        for j in 0..FILES_PER_DIR {
            let file = InodeView::new_file(file_name(j), InodeFile::new(next_id, 0));
            next_id += 1;
            dir.as_mut().add_child(file).unwrap();
        }
    }
    let file_time = t.elapsed();
    let rss_full = rss_mib();

    assert_eq!(dir_name(0).len(), DIR_NAME_LEN);
    assert_eq!(dir_name(dir_count - 1).len(), DIR_NAME_LEN);
    assert_eq!(file_name(0).len(), FILE_NAME_LEN);
    assert_eq!(file_name(FILES_PER_DIR - 1).len(), FILE_NAME_LEN);

    let acc = account_tree(&root, container);
    assert_eq!(acc.dirs, dir_count);
    assert_eq!(acc.files, dir_count * FILES_PER_DIR);
    assert_eq!(root.child_len(), dir_count);

    let files = acc.files as f64;
    let inodes = files + dir_count as f64 + 1.0;
    let dir_cost = rss_dirs - rss_start;
    let file_cost = rss_full - rss_dirs;
    let total_cost = rss_full - rss_start;

    println!(
        "\n=== namespace tree: {dir_count} dirs x {FILES_PER_DIR} files \
         (children = {}, dir names {DIR_NAME_LEN} B, file names {FILE_NAME_LEN} B) ===",
        label(container)
    );
    println!("RSS baseline         : {:8.1} MiB", rss_start);
    println!(
        "RSS root + dirs      : {:8.1} MiB  (+{:6.1} MiB, {:5.0} B/dir, {:?})",
        rss_dirs,
        dir_cost,
        dir_cost * MIB / dir_count as f64,
        dir_time
    );
    println!(
        "RSS + {} files       : {:8.1} MiB  (+{:6.1} MiB, {:5.0} B/file, {:?})",
        files as usize,
        rss_full,
        file_cost,
        file_cost * MIB / files,
        file_time
    );
    println!(
        "Total tree footprint : {:8.1} MiB  ({:.0} B/inode, {} inodes)",
        total_cost,
        total_cost * MIB / inodes,
        inodes as usize
    );
    println!(
        "Payload lower bound  : {:8.1} MiB  ({:.0} B/inode); names {} B, avg file name {} B",
        acc.payload_bytes as f64 / MIB,
        acc.payload_bytes as f64 / inodes,
        acc.name_bytes,
        FILE_NAME_LEN
    );
}

fn containers(default: &[ChildrenContainer]) -> Vec<ChildrenContainer> {
    match std::env::var("CURVINE_MEM_TEST_LAYOUT").ok().as_deref() {
        Some("list") => vec![ChildrenContainer::List],
        Some("map") => vec![ChildrenContainer::Map],
        Some("packed") => vec![ChildrenContainer::Packed],
        Some("both") => vec![ChildrenContainer::Map, ChildrenContainer::Packed],
        _ => default.to_vec(),
    }
}

#[test]
#[ignore = "allocates > 2 GiB; run explicitly with --ignored --nocapture"]
fn dir_tree_memory_10m_files() {
    print_struct_sizes();
    for container in containers(&[ChildrenContainer::Map, ChildrenContainer::Packed]) {
        measure(container, DIR_COUNT);
    }
}
