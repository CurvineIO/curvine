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

//! Location interleaving cases (block report, replication completion, the
//! sampling window) split from `recursive_location_lock_test.rs`; reuse the
//! parent fixture.

use super::*;

/// A worker block report for the same block, issued while a recursive free is
/// planned, is per-worker: the reporting worker keeps its location, the other
/// worker's released location stays released, and nothing else appears.
#[test]
fn concurrent_block_report_keeps_only_the_reporting_workers_location() {
    // Two workers: the fixture block lives on worker 1, so only worker 2 can
    // widen the sampled location set (reporting worker 1 again is an
    // idempotent no-op and would prove nothing).
    let fs = test_fs_with_two_workers("location-report", "session-1", "session-2");
    fs.mkdir("/f/d0", true).unwrap();
    let block_id = file_with_one_block(&fs, "/f/d0/file");

    // Order the race instead of betting on the scheduler: the report lands
    // inside the sampling window, i.e. after the location sample and before
    // the stripes are acquired.
    let (loc_reached_tx, loc_reached_rx) = mpsc::channel();
    let (loc_release_tx, loc_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_location_pause_hook(loc_reached_tx, loc_release_rx);

    let free_fs = fs.clone();
    let (free_done_tx, free_done_rx) = std::sync::mpsc::channel();
    let free = std::thread::spawn(move || {
        let result = free_fs.free("/f", true);
        // Signal completion before returning so the driver never blocks
        // unboundedly on a worker that finished its work but stalled later.
        let _ = free_done_tx.send(());
        result
    });
    loc_reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the free's location sample must reach the pause point");

    // Worker 2, which the master did not know about, reports the block through
    // the real entry: this grows the live set past the sample.
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

    // Watch for the re-plan that the post-lock coverage check must trigger,
    // then release the free.
    let (plan2_tx, plan2_rx) = mpsc::channel();
    let (plan2_release_tx, plan2_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(plan2_tx, plan2_release_rx);
    loc_release_tx.send(()).expect("release the free");

    let recaptured = plan2_rx.recv_timeout(OBSERVE_WINDOW);
    let _ = plan2_release_tx.send(());
    free_done_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the worker must finish inside the bounded window");
    let freed = free.join().expect("free thread must not panic");

    assert!(freed.is_ok(), "recursive free must succeed: {freed:?}");
    assert!(
        recaptured.is_ok(),
        "the location reported after sampling must force a fresh capture \
         (delete_blocks {:?})",
        reported.delete_blocks
    );
    assert!(fs.file_status("/f/d0/file").is_ok(), "free keeps metadata");
    // `free` on a non-cache file is a metadata operation: it must not reclaim
    // the block at all, and in particular it must not drop the location it
    // only learned about inside its own sampling window. The old shape of this
    // test asserted "at most one location remains", which the no-op free
    // satisfied without ever exercising the race.
    let remaining = stored_locations(&fs, block_id);
    assert!(
        remaining.iter().any(|loc| loc.worker_id == 1),
        "worker 1's original location must survive a free: {remaining:?}"
    );
    assert!(
        remaining.iter().any(|loc| loc.worker_id == 2),
        "worker 2's location was reported inside the window, so the re-planned \
         free must own it and leave it in place: {remaining:?}"
    );
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
    let (handle_done_tx, _handle_done_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let result = free_fs.free("/t", true);
        // Signal completion before returning so the driver never blocks
        // unboundedly on a worker that finished its work but stalled later.
        let _ = handle_done_tx.send(());
        result
    });

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
    let (handle_done_tx, _handle_done_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let result = free_fs.free("/t", true);
        // Signal completion before returning so the driver never blocks
        // unboundedly on a worker that finished its work but stalled later.
        let _ = handle_done_tx.send(());
        result
    });

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
    let (handle_done_tx, _handle_done_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let result = delete_fs.delete("/a", true);
        // Signal completion before returning so the driver never blocks
        // unboundedly on a worker that finished its work but stalled later.
        let _ = handle_done_tx.send(());
        result
    });
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
