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
use std::sync::mpsc;
use std::time::Duration;

/// Bound for a handshake or an interleaved operation observed by this file.
const OBSERVE_WINDOW: Duration = Duration::from_secs(8);

fn test_fs_with_live_worker(name: &str) -> MasterFilesystem {
    test_fs_with_worker_session(name, "")
}

fn test_fs_with_two_workers(name: &str, session1: &str, session2: &str) -> MasterFilesystem {
    let fs = test_fs_with_worker_session(name, session1);
    let mut second = WorkerInfo::new(
        WorkerAddress {
            worker_id: 2,
            ..Default::default()
        },
        1,
    );
    second.status = WorkerStatus::Live;
    second.capacity = i64::MAX / 4;
    second.available = i64::MAX / 4;
    second.worker_session_id = session2.to_string();
    fs.add_test_worker(second);
    fs
}

fn test_fs_with_worker_session(name: &str, session: &str) -> MasterFilesystem {
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
    worker.worker_session_id = session.to_string();
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
    // replicas >= 2 keeps room for a second worker location: with the default
    // replica count of 1, `add_replication_location_if_needed` refuses a second
    // worker because the block is already fully replicated.
    let opts = CreateFileOptsBuilder::new()
        .create_parent(true)
        .ttl_action(TtlAction::Delete)
        .replicas(2)
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

/// Copy completion takes the same location protection as free. It reaches the
/// real public(crate) entry `commit_replication_location`, which validates the
/// worker session and inserts the location under the worker/block location
/// locks, without taking the file's inode lock.
///
/// Deterministic version: completion followed by a recursive free must leave a
/// whole location set, and a completion that arrives after the free must be
/// answered rather than silently resurrecting a live location for a file that
/// no longer references the block.
#[test]
fn replication_completion_and_free_keep_the_location_set_consistent() {
    let session = "replication-session-1";
    let fs = test_fs_with_worker_session("location-replication", session);
    fs.mkdir("/t", true).unwrap();
    let block_id = file_with_one_block(&fs, "/t/file");

    // Completion for the same worker adds nothing new but must be accepted,
    // proving the entry is reachable with a valid session.
    let added = fs
        .commit_replication_location(block_id, BlockLocation::with_id(1), session)
        .expect("replication completion must succeed with a matching session");
    let before_free = stored_locations(&fs, block_id);
    assert!(
        !before_free.is_empty(),
        "the block must still carry its location after completion (added={added})"
    );

    let freed = fs.free("/t", true).expect("recursive free");
    assert_eq!(freed.inodes, 1, "one inode must be reclaimed: {freed:?}");
    let after_free = stored_locations(&fs, block_id);
    assert!(
        after_free.is_empty() || after_free.len() == before_free.len(),
        "free must not leave a partially released set: {before_free:?} -> {after_free:?}"
    );

    // A late completion for the already-freed block is answered, not a panic;
    // it must not grow a live location set behind the free's back.
    let late = fs.commit_replication_location(block_id, BlockLocation::with_id(1), session);
    assert!(
        late.is_ok(),
        "a late completion must be answered rather than fail opaquely: {late:?}"
    );
    let after_late = stored_locations(&fs, block_id);
    assert!(
        after_late.len() <= after_free.len().max(1),
        "a late completion must not grow the freed block's location set: {after_free:?} -> {after_late:?}"
    );

    // A completion with a stale session must be rejected, not silently applied.
    let stale =
        fs.commit_replication_location(block_id, BlockLocation::with_id(1), "stale-session");
    assert!(
        stale.is_err(),
        "a completion for a changed worker session must be rejected, got {stale:?}"
    );
}

/// R7: a location appears after the sample but before the location locks are
/// taken. The post-lock coverage check must notice that the block now has a
/// worker/location the sample did not cover, which makes the operation restart
/// (<=3) or fall back.
///
/// The observable proof is a **second capture**: the retry re-enters the plan
/// path, so the plan pause hook fires again. If the coverage check were absent,
/// the free would proceed with the stale set and never re-plan - the second
/// pause would never arrive and this case would hang until its bound.
#[test]
fn location_added_after_sampling_forces_a_second_capture() {
    let fs = test_fs_with_live_worker("location-pause");
    fs.mkdir("/t/d0", true).unwrap();
    let block_id = file_with_one_block(&fs, "/t/d0/file");

    let (loc_reached_tx, loc_reached_rx) = mpsc::channel();
    let (loc_release_tx, loc_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_location_pause_hook(loc_reached_tx, loc_release_rx);

    let free_fs = fs.clone();
    let handle = std::thread::spawn(move || free_fs.free("/t", true));

    loc_reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the location sample must reach the pause point");

    // The block gains a second worker location after the sample.
    {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store
            .add_block_location(block_id, BlockLocation::with_id(2))
            .expect("add a late worker location");
    }

    // Arm the plan hook again: a correct coverage re-check restarts the
    // operation, which re-enters the plan path and fires this hook.
    let (plan_reached_tx, plan_reached_rx) = mpsc::channel();
    let (plan_release_tx, plan_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(plan_reached_tx, plan_release_rx);

    loc_release_tx.send(()).expect("release the free");

    let recaptured = plan_reached_rx.recv_timeout(OBSERVE_WINDOW);
    let _ = plan_release_tx.send(());
    let freed = handle
        .join()
        .expect("free thread must not panic")
        .expect("the free must finish");

    assert!(
        recaptured.is_ok(),
        "the location added after sampling must force a fresh capture; without \
         the post-lock coverage check the free would keep the stale set and \
         never re-plan"
    );
    assert_eq!(
        freed.inodes, 1,
        "the restarted free must reclaim exactly one inode: {freed:?}"
    );
}

/// R7: copy completion arrives inside the location sampling window. This uses
/// the real `commit_replication_location` entry (worker session validated,
/// worker/block location locks taken, durable row written), not a bare store
/// write, and the late worker must force a fresh capture exactly like a late
/// store row would.
#[test]
fn replication_completion_in_the_sampling_window_forces_a_second_capture() {
    let fs = test_fs_with_two_workers("location-replication-race", "session-1", "session-2");
    fs.mkdir("/t/d0", true).unwrap();
    let block_id = file_with_one_block(&fs, "/t/d0/file");

    let (loc_reached_tx, loc_reached_rx) = mpsc::channel();
    let (loc_release_tx, loc_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_location_pause_hook(loc_reached_tx, loc_release_rx);

    let free_fs = fs.clone();
    let handle = std::thread::spawn(move || free_fs.free("/t", true));

    loc_reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the location sample must reach the pause point");

    // Real replication completion for the second worker, after the sample.
    let added = fs
        .commit_replication_location(block_id, BlockLocation::with_id(2), "session-2")
        .expect("replication completion must succeed for a live session");

    let (plan_reached_tx, plan_reached_rx) = mpsc::channel();
    let (plan_release_tx, plan_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(plan_reached_tx, plan_release_rx);

    loc_release_tx.send(()).expect("release the free");

    let recaptured = plan_reached_rx.recv_timeout(OBSERVE_WINDOW);
    let _ = plan_release_tx.send(());
    let freed = handle
        .join()
        .expect("free thread must not panic")
        .expect("the free must finish");

    assert!(
        recaptured.is_ok(),
        "a replication completion that lands after the sample must force a fresh \
         capture (added={added}); without the coverage re-check the free would \
         keep the stale worker set"
    );
    assert_eq!(
        freed.inodes, 1,
        "the restarted free must reclaim exactly one inode: {freed:?}"
    );
    // The session was validated, so the late location is real state, not a mock.
    let locations = stored_locations(&fs, block_id);
    assert!(
        locations.iter().any(|loc| loc.worker_id == 2),
        "the replayed free must have seen the completed second-worker location: {locations:?}"
    );
}

/// R7: ID-only cache invalidation vs a recursive delete of the same inode.
/// Design ruling: an ID-level cache rewrite changes no directory edge or token,
/// so when it completes first the plan stays valid and no re-capture is owed.
/// The contract is freshness and no resurrection: the delete's body must read
/// the post-invalidation inode, not replay a stale DTO that still carries the
/// reclaimed block.
///
/// The invalidation is run to completion first (it takes the block location
/// locks itself); driving it synchronously inside the delete's pause window
/// would make the fixture wait on the very locks it is supposed to observe.
#[test]
fn id_only_invalidation_is_not_resurrected_by_a_later_delete() {
    let fs = test_fs_with_two_workers("location-id-only", "session-1", "session-2");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/file", true).unwrap();
    let block = fs
        .add_block(
            "/a/d0/file",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    let block_id = block.id;
    {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        for worker in 1..=2 {
            view.store
                .add_block_location(block_id, BlockLocation::with_id(worker))
                .expect("worker location");
        }
    }
    let commit = curvine_model::CommitBlock {
        block_id,
        block_len: 4096,
        locations: vec![BlockLocation::with_id(1), BlockLocation::with_id(2)],
    };
    fs.complete_file(
        "/a/d0/file".to_string(),
        None,
        4096,
        vec![commit],
        "location-fixture".to_string(),
        false,
        None,
    )
    .expect("complete the block");
    // `invalidate_lost_cache_files` only rewrites inodes in the Both storage
    // state with the Delete TTL action; a UFS mtime is what moves the policy
    // into Both.
    fs.set_attr(
        "/a/d0/file",
        curvine_model::SetAttrOptsBuilder::new()
            .ufs_mtime(1_000)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .expect("mark the file as a cache-mode replica");
    assert!(
        fs.file_status("/a/d0/file")
            .unwrap()
            .storage_policy
            .both_exists(),
        "fixture: the file must be in the Both storage state"
    );
    fs.link("/a/d0/file".to_string(), "/outside-keep".to_string())
        .expect("external survivor name");
    let file_id = fs.file_status("/a/d0/file").unwrap().id;

    // Step 1: real ID-level cleanup. Losing worker 1 leaves the block readable
    // by worker 2; losing worker 2 leaves it with no location, which is what
    // makes the by-id rewrite drop the block from the inode.
    fs.delete_locations(1)
        .expect("losing worker 1 must succeed");
    let cleaned = fs
        .delete_locations(2)
        .expect("the by-id cache cleanup must succeed");
    assert!(
        cleaned.removed_block_ids.contains(&block_id),
        "fixture: the cleanup must reclaim the block, got {:?}",
        cleaned.removed_block_ids
    );
    let blocks_after_cleanup = match {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store.get_inode(file_id, None).unwrap()
    } {
        Some(crate::master::meta::inode::InodeView::File(file)) => {
            file.blocks.iter().map(|b| b.id).collect::<Vec<_>>()
        }
        other => panic!("the inode must still be a file, got {other:?}"),
    };
    assert!(
        !blocks_after_cleanup.contains(&block_id),
        "the by-id rewrite must drop the unreadable cache block, got {blocks_after_cleanup:?}"
    );

    // Step 2: recursive delete of the subtree afterwards.
    fs.delete("/a", true)
        .expect("the recursive delete must finish");

    assert!(
        fs.file_status("/a").is_err(),
        "the planned tree must be gone"
    );
    let survivor = fs
        .file_status("/outside-keep")
        .expect("the external name must survive");
    assert_eq!(survivor.id, file_id, "the survivor keeps the same inode");
    let blocks_after_delete = match {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store.get_inode(file_id, None).unwrap()
    } {
        Some(crate::master::meta::inode::InodeView::File(file)) => {
            file.blocks.iter().map(|b| b.id).collect::<Vec<_>>()
        }
        None => Vec::new(),
        other => panic!("the inode must be a file or gone, got {other:?}"),
    };
    assert!(
        !blocks_after_delete.contains(&block_id),
        "the delete must not resurrect the cleaned block from a stale DTO, got \
         {blocks_after_delete:?}"
    );
    assert!(
        stored_locations(&fs, block_id).is_empty(),
        "no location row may remain for the reclaimed block"
    );
}

/// R7: the ID-level cleanup takes the same file inode write lock as the
/// recursive delete, so the two are mutually exclusive rather than racing on
/// the same inode. `delete_lost_worker_locations` acquires
/// `inode_locks.lock_many` (which includes the candidate files' inode write
/// locks) before it calls `invalidate_lost_cache_files`, so an invalidation
/// driven from a second thread has to wait for the delete that holds that lock.
///
/// Both operations are driven on their own threads; the delete's pause window
/// is the barrier, and neither thread waits on a hook the other must release.
#[test]
fn id_only_invalidation_excludes_with_the_delete_on_the_same_file_inode() {
    let fs = test_fs_with_two_workers("location-id-only-exclusive", "session-1", "session-2");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/file", true).unwrap();
    let block = fs
        .add_block(
            "/a/d0/file",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    let block_id = block.id;
    {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        for worker in 1..=2 {
            view.store
                .add_block_location(block_id, BlockLocation::with_id(worker))
                .expect("worker location");
        }
    }
    let commit = curvine_model::CommitBlock {
        block_id,
        block_len: 4096,
        locations: vec![BlockLocation::with_id(1), BlockLocation::with_id(2)],
    };
    fs.complete_file(
        "/a/d0/file".to_string(),
        None,
        4096,
        vec![commit],
        "location-fixture".to_string(),
        false,
        None,
    )
    .expect("complete the block");
    fs.set_attr(
        "/a/d0/file",
        curvine_model::SetAttrOptsBuilder::new()
            .ufs_mtime(1_000)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .expect("cache-mode replica");
    fs.link("/a/d0/file".to_string(), "/outside-keep".to_string())
        .expect("external survivor");
    let file_id = fs.file_status("/a/d0/file").unwrap().id;

    let (loc_reached_tx, loc_reached_rx) = mpsc::channel();
    let (loc_release_tx, loc_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_location_pause_hook(loc_reached_tx, loc_release_rx);

    let delete_fs = fs.clone();
    let delete_handle = std::thread::spawn(move || delete_fs.delete("/a", true));
    loc_reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the delete must reach its location sample");

    // The cleanup runs on its own thread: it needs the file inode write lock,
    // which the paused delete already holds.
    let idl_fs = fs.clone();
    let (idl_done_tx, idl_done_rx) = mpsc::channel();
    let idl_handle = std::thread::spawn(move || {
        let first = idl_fs.delete_locations(1);
        let second = idl_fs.delete_locations(2);
        let _ = idl_done_tx.send((first, second));
    });

    // Mutual exclusion: the cleanup cannot finish while the delete holds the
    // inode write lock.
    let early = idl_done_rx.recv_timeout(Duration::from_millis(500));
    assert!(
        early.is_err(),
        "the ID-level cleanup must block on the delete's file inode lock, but \
         it finished early"
    );

    loc_release_tx.send(()).expect("release the delete");
    delete_handle
        .join()
        .expect("delete thread must not panic")
        .expect("the recursive delete must finish");

    let (first, second) = idl_done_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the cleanup must complete once the delete releases the inode lock");
    let cleaned = second.expect("the by-id cleanup must succeed");
    assert!(
        first.is_ok(),
        "losing worker 1 must also succeed once the lock is free"
    );
    idl_handle.join().expect("cleanup thread must not panic");
    assert!(
        cleaned.removed_block_ids.contains(&block_id),
        "the serialized cleanup must still reclaim the block, got {:?}",
        cleaned.removed_block_ids
    );

    // Legal order (delete first, cleanup second) with no resurrection: the
    // surrogate name keeps the inode, and the reclaimed block is gone from it.
    assert!(fs.file_status("/a").is_err(), "the subtree must be gone");
    let survivor = fs
        .file_status("/outside-keep")
        .expect("the external name must survive");
    assert_eq!(survivor.id, file_id);
    match {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store.get_inode(file_id, None).unwrap()
    } {
        Some(crate::master::meta::inode::InodeView::File(file)) => assert!(
            file.blocks.iter().all(|b| b.id != block_id),
            "the cleanup must leave the reclaimed block out of the inode"
        ),
        other => panic!("the inode must still be a file, got {other:?}"),
    }
    assert!(
        stored_locations(&fs, block_id).is_empty(),
        "no location row may remain for the reclaimed block"
    );
}

/// R7: block report through the real public entry, racing a recursive
/// last-link delete. The report adds a location after the sampling point, so the
/// post-lock coverage check must restart the delete.
#[test]
fn block_report_racing_a_last_link_delete_forces_coverage() {
    let fs = test_fs_with_two_workers("location-report-race", "session-1", "session-2");
    fs.mkdir("/a/d0", true).unwrap();
    let block_id = file_with_one_block(&fs, "/a/d0/file");

    let (loc_reached_tx, loc_reached_rx) = mpsc::channel();
    let (loc_release_tx, loc_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_location_pause_hook(loc_reached_tx, loc_release_rx);

    let delete_fs = fs.clone();
    let handle = std::thread::spawn(move || delete_fs.delete("/a", true));
    loc_reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the location sample must reach the pause point");

    // Real worker block report for the second worker, after the sample.
    let report = BlockReportList {
        cluster_id: String::new(),
        worker_id: 2,
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
    let reported = fs
        .block_report(report, None)
        .expect("the real block report entry must succeed");

    let (plan2_tx, plan2_rx) = mpsc::channel();
    let (plan2_release_tx, plan2_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(plan2_tx, plan2_release_rx);
    loc_release_tx.send(()).expect("release the delete");

    let recaptured = plan2_rx.recv_timeout(OBSERVE_WINDOW);
    let _ = plan2_release_tx.send(());
    handle
        .join()
        .expect("delete thread must not panic")
        .expect("the delete must finish");

    assert!(
        recaptured.is_ok(),
        "a block report through the real entry that lands after sampling must \
         force a fresh capture (delete_blocks {:?})",
        reported.delete_blocks
    );
    assert!(
        fs.file_status("/a").is_err(),
        "the recursive delete must still complete"
    );
}
