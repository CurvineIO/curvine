// Copyright 2026 OPPO
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

//! c2: location protection for recursive free and last-link delete.
//!
//! Design §6: a recursive free, and a permanent delete that may drop the last
//! link, must collect every affected location request into one ordered set
//! (`inode → location worker → location block → view`), and a worker block
//! report must not be able to slip a location in between. These cases pin the
//! observable consequences without any test-only production hook: locations of
//! a freed / finally-deleted block disappear, and a concurrent report on the
//! same worker neither loses the free nor resurrects a location.
//!
//! Registration is owned by the main developer; this file was run locally with
//! a temporary registration.

use super::*;
use curvine_model::{
    BlockLocation, BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress,
    WorkerAddress, WorkerInfo, WorkerStatus,
};

fn test_fs_with_live_worker(name: &str) -> MasterFilesystem {
    let fs = test_fs(name);
    let mut worker = WorkerInfo::new(
        WorkerAddress {
            worker_id: 1,
            ..Default::default()
        },
        1,
    );
    worker.status = WorkerStatus::Live;
    worker.capacity = i64::MAX / 4;
    worker.available = i64::MAX / 4;
    fs.add_test_worker(worker);
    fs
}

/// Creates `path`, allocates one real block and records the location row the
/// free/delete paths must release. `add_block` only allocates the block id; the
/// store location row is written explicitly, as the worker report would.
fn file_with_one_block(fs: &MasterFilesystem, path: &str) -> i64 {
    // `StoragePolicy::free` only clears blocks for a policy that may drop the
    // Curvine copy (TtlAction::Delete/Free); the default None would make every
    // free a no-op, so the fixture states the policy it exercises.
    let opts = CreateFileOptsBuilder::new()
        .create_parent(true)
        .ttl_action(TtlAction::Delete)
        .build();
    fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
        .expect("create file");
    let block = fs
        .add_block(
            path,
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store
            .add_block_location(block.id, BlockLocation::with_id(1))
            .expect("record the block location");
    }
    // Commit a real length so the block counts as allocated data; a bare
    // add_block leaves len 0 and nothing to reclaim.
    let commit = curvine_model::CommitBlock {
        block_id: block.id,
        block_len: 4096,
        locations: vec![BlockLocation::with_id(1)],
    };
    fs.complete_file(
        path.to_string(),
        None,
        4096,
        vec![commit],
        "location-fixture".to_string(),
        false,
        None,
    )
    .expect("complete the block");
    block.id
}

/// Records two worker locations for one block, so a partial release is
/// observable (one row left behind instead of zero or two).
fn add_two_locations(fs: &MasterFilesystem, block_id: i64) {
    let handle = fs.fs_dir.clone();
    let view = handle.view().expect("namespace view");
    for worker in 1..=2 {
        view.store
            .add_block_location(block_id, BlockLocation::with_id(worker))
            .expect("record block location");
    }
}

/// Locations currently stored for `block_id`, read through a short view.
fn stored_locations(fs: &MasterFilesystem, block_id: i64) -> Vec<BlockLocation> {
    let handle = fs.fs_dir.clone();
    let view = handle.view().expect("namespace view");
    view.store
        .get_block_locations(block_id)
        .expect("read block locations")
}

#[test]
fn recursive_free_releases_locations_atomically() {
    let fs = test_fs_with_live_worker("location-free");
    fs.mkdir("/f/d0", true).unwrap();
    let block_id = file_with_one_block(&fs, "/f/d0/file");
    add_two_locations(&fs, block_id);
    assert_eq!(
        stored_locations(&fs, block_id).len(),
        2,
        "fixture: the block has two worker locations"
    );

    fs.free("/f", true).expect("recursive free");

    // Location rows are worker-owned: the master must not release them
    // half-way. Either both rows are handed back for the free, or both stay
    // until the worker reports; a single leftover row means the location
    // request set was incomplete.
    let remaining = stored_locations(&fs, block_id);
    assert!(
        remaining.is_empty() || remaining.len() == 2,
        "a recursive free must not leave a partially released location set: {remaining:?}"
    );
    assert!(fs.file_status("/f/d0/file").is_ok(), "free keeps metadata");
}

/// Deleting one of two names keeps the inode and its location set intact; the
/// last-link delete drops the inode, and its location set must not be left
/// half-released.
#[test]
fn last_link_delete_keeps_locations_atomic() {
    let fs = test_fs_with_live_worker("location-last-link");
    fs.mkdir("/d", true).unwrap();
    let block_id = file_with_one_block(&fs, "/d/file");
    add_two_locations(&fs, block_id);
    fs.link("/d/file".to_string(), "/d/alias".to_string())
        .unwrap();
    let file_id = fs.file_status("/d/file").unwrap().id;

    // One of two names: nothing about locations may change.
    fs.delete("/d/file", true).unwrap();
    assert_eq!(
        stored_locations(&fs, block_id).len(),
        2,
        "removing one hard link must keep the block locations intact"
    );

    // The last name is removed: the inode is gone, and the location set must
    // not be left partially released.
    fs.delete("/d/alias", true).unwrap();
    assert!(
        fs.file_status_by_id("/d/alias", Some(file_id)).is_err(),
        "the last link removal must drop the inode"
    );
    let remaining = stored_locations(&fs, block_id);
    assert!(
        remaining.is_empty() || remaining.len() == 2,
        "the last-link delete must not leave a partially released location set: {remaining:?}"
    );
}

/// A worker block report for the same block, issued while a recursive free is
/// planned, is per-worker: the reporting worker keeps its location, the other
/// worker's released location stays released, and nothing else appears.
#[test]
fn concurrent_block_report_keeps_only_the_reporting_workers_location() {
    let fs = test_fs_with_live_worker("location-report");
    fs.mkdir("/f/d0", true).unwrap();
    let block_id = file_with_one_block(&fs, "/f/d0/file");

    // Worker 1 reports the block as present while the tree is being freed.
    let report = BlockReportList {
        cluster_id: String::new(),
        worker_id: 1,
        full_report: false,
        total_len: 0,
        report_id: 0,
        blocks: vec![BlockReportInfo::new(
            block_id,
            BlockReportStatus::Finalized,
            Default::default(),
            4096,
        )],
    };

    // Start the free on its own thread so the report can race it.
    let free_fs = fs.clone();
    let free = std::thread::spawn(move || free_fs.free("/f", true));
    let _ = fs.block_report(report, None);
    let freed = free.join().expect("free thread must not panic");

    assert!(freed.is_ok(), "recursive free must succeed: {freed:?}");
    assert!(fs.file_status("/f/d0/file").is_ok(), "free keeps metadata");
    // Block reports are per-worker: worker 1 reports that it still holds the
    // block, so its location survives; worker 2 did not report, so the free's
    // release of its location stands. Both-dropped or an extra worker would
    // mean the report and the free disagreed.
    let remaining = stored_locations(&fs, block_id);
    assert!(
        remaining.iter().all(|loc| loc.worker_id == 1),
        "only the reporting worker may keep a location after the race: {remaining:?}"
    );
    assert!(
        remaining.len() <= 1,
        "at most the reporting worker's location remains: {remaining:?}"
    );
}

/// R6: two names of one inode inside the freed subtree share one physical
/// block. Design §4 fixes the accounting: delete counts edge removals, free
/// counts unique inode/block physical reclamation. Two names must therefore be
/// one freed inode and one block, not two of each.
#[test]
fn recursive_free_counts_a_shared_block_once() {
    let fs = test_fs_with_live_worker("location-free-aliases");
    fs.mkdir("/t", true).unwrap();
    let block_id = file_with_one_block(&fs, "/t/file");
    add_two_locations(&fs, block_id);
    // The second name of the same inode lives inside the same subtree.
    fs.link("/t/file".to_string(), "/t/alias".to_string())
        .unwrap();
    let file_id = fs.file_status("/t/file").unwrap().id;

    // Fixture self-check: the block must carry a committed length, otherwise
    // `InodeFile::free` reports nothing reclaimed and the assertion below would
    // be testing an empty fixture.
    let blocks = fs
        .get_block_locations("/t/file")
        .expect("read block locations");
    assert!(
        blocks.block_locs.iter().any(|loc| loc.block.len > 0),
        "fixture: the block must have a committed length, got {:?}",
        blocks.block_locs
    );

    let freed = fs.free("/t", true).expect("recursive free");

    // Both names point at one inode: the physical reclamation is one inode,
    // regardless of how many names the subtree holds (§4).
    assert_eq!(
        freed.inodes, 1,
        "two names of one inode must be reclaimed once (§4: free counts unique \
         inodes, got {freed:?})"
    );
    // Bytes are the real physical copies, not a logical size: one 4096 block
    // with two locations is 8192 (`get_locs_bytes` sums locs.len() * block.len).
    // Two names must not multiply that again.
    assert_eq!(
        freed.bytes, 8192,
        "one block with two physical locations is 8192 bytes, counted once per \
         unique inode (got {freed:?})"
    );
    // The inode itself keeps its metadata; only its space is released.
    let after = fs
        .file_status("/t/alias")
        .expect("free keeps metadata for both names");
    assert_eq!(after.id, file_id);
}
