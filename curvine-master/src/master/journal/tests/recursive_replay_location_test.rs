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

//! c2 F2: replay of a recursive delete whose location set grows inside the
//! replay sampling window. Split out of `recursive_replay_lock_test.rs` to keep
//! each file to one job; reuses the parent fixture through `use super::*`.

use super::*;
use curvine_model::{
    BlockLocation, BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, CommitBlock,
    WorkerAddress, WorkerInfo, WorkerStatus,
};

/// R2/F2: a location that appears *inside* the replay sampling window must make
/// the post-lock coverage check re-plan. The driver stops the replay at the
/// checkpoint the production code exposes (sampling complete, worker/block
/// stripes not yet taken), grows the live location set through the real
/// `block_report` entry, and then watches for a SECOND checkpoint - that second
/// arrival is the re-capture, and it cannot happen from scheduling luck because
/// the first one is released only after the report returns.
#[test]
fn replay_location_grown_inside_the_sampling_window_forces_a_second_capture() -> FsResult<()> {
    fn install_worker(fs: &MasterFilesystem, worker_id: u32, session: &str) {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id,
                ..Default::default()
            },
            worker_id,
        );
        worker.status = WorkerStatus::Live;
        worker.capacity = i64::MAX / 4;
        worker.available = i64::MAX / 4;
        worker.worker_session_id = session.to_string();
        fs.add_test_worker(worker);
    }

    /// Creates `path` with one completed block whose only location is worker 1.
    fn file_with_one_block(fs: &MasterFilesystem, path: &str) -> FsResult<i64> {
        fs.create(path, true)?;
        let block = fs.add_block(
            path,
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )?;
        let block_id = block.id;
        {
            let handle = fs.fs_dir.clone();
            let view = handle.view()?;
            view.store
                .add_block_location(block_id, BlockLocation::with_id(1))?;
        }
        let commit = curvine_model::CommitBlock {
            block_id,
            block_len: 4096,
            locations: vec![BlockLocation::with_id(1)],
        };
        fs.complete_file(
            path.to_string(),
            None,
            4096,
            vec![commit],
            "replay-location-fixture".to_string(),
            false,
            None,
        )?;
        Ok(block_id)
    }

    let source = JournalSystem::from_conf(&test_conf("loc-window-source"))?;
    let target = JournalSystem::from_conf(&test_conf("loc-window-target"))?;
    let source_fs = source.fs();
    let target_fs = target.fs();

    // The target needs two live workers: the fixture block sits on worker 1, so
    // only worker 2 can widen the sampled set through a real report.
    install_worker(&target_fs, 1, "session-1");
    install_worker(&target_fs, 2, "session-2");

    // Source: the same one-block shape, then a real recursive delete entry.
    install_worker(&source_fs, 1, "session-1");
    source_fs.mkdir("/r/d0", true)?;
    file_with_one_block(&source_fs, "/r/d0/file")?;
    let entry = capture_one(&source_fs, || {
        source_fs.delete("/r", true).unwrap();
    });
    assert!(
        matches!(&entry, JournalEntry::Delete(delete) if delete.path == "/r"),
        "the captured entry must be the typed recursive delete"
    );

    // Target: the same shape with one location on worker 1.
    target_fs.mkdir("/r/d0", true)?;
    let block_id = file_with_one_block(&target_fs, "/r/d0/file")?;
    echo_entries(&target_fs);

    // Id captured before the window: the checkpoint assertion below reads the
    // store directly, because `file_status` would take the inode read lock that
    // the paused replay is holding (and would deadlock the driver).
    let file_id = {
        let handle = target_fs.fs_dir.clone();
        let view = handle.view()?;
        let inode = InodePath::resolve(view.root_node(), "/r/d0/file", &view.store)?;
        let id = inode.get_last_inode().map(|inode| inode.id()).unwrap_or(0);
        assert!(id > 0, "the fixture file must exist before the window");
        id
    };

    let writer = {
        let handle = target_fs.fs_dir.clone();
        let view = handle.view()?;
        view.journal_writer.clone()
    };

    let (first_tx, first_rx) = std::sync::mpsc::channel();
    let (first_release_tx, first_release_rx) = std::sync::mpsc::channel();
    writer.arm_location_pause_hook(first_tx, first_release_rx);
    let (second_tx, second_rx) = std::sync::mpsc::channel();
    let (second_release_tx, second_release_rx) = std::sync::mpsc::channel();

    // The driver: waits for the checkpoint, reports a location from worker 2
    // through the real entry, re-arms, and only then lets the replay continue.
    let driver_fs = target_fs.clone();
    let driver = std::thread::spawn(move || {
        first_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the replay must reach its location checkpoint");
        // Take-once: the first checkpoint is consumed, so re-arming here is the
        // only way a second capture could be observed at all.
        writer.arm_location_pause_hook(second_tx, second_release_rx);

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
        let reported = driver_fs
            .block_report(report, None)
            .expect("the real block report entry must accept worker 2's location");

        // The body has not run yet. Read the store directly (no inode lock) and
        // drop the view before touching the release channel, so nothing of ours
        // is held across the window.
        {
            let handle = driver_fs.fs_dir.clone();
            let view = handle.view().expect("namespace view");
            let still_present = view
                .store
                .get_inode(file_id, None)
                .expect("store read")
                .is_some();
            assert!(
                still_present,
                "the replay body must not have deleted anything before the \
                 checkpoint is released"
            );
        }

        first_release_tx
            .send(())
            .expect("the replay must still be waiting at its checkpoint");
        let recaptured = second_rx.recv_timeout(std::time::Duration::from_secs(30));
        let _ = second_release_tx.send(());
        (reported, recaptured.is_ok())
    });

    let loader = target.journal_loader();
    let replayed = loader.apply_entry(entry);
    let (reported, recaptured) = driver.join().expect("driver thread must not panic");

    replayed?;
    assert!(
        recaptured,
        "a location reported inside the sampling window must force a second \
         capture (delete_blocks {:?})",
        reported.delete_blocks
    );
    assert!(
        target_fs.file_status("/r").is_err(),
        "the replayed recursive delete must remove the subtree"
    );
    assert!(
        echo_entries(&target_fs).is_empty(),
        "replay must not echo journal entries"
    );

    // End state per the real `stage_decrement_nlink` path: the last link drops
    // the inode and its TTL, the gathered `DeleteResult` travels back to
    // `JournalLoader::delete` which discards it and therefore issues no worker
    // delete command - so BOTH location rows survive. Asserting an empty set
    // would be asserting a reclamation this path does not perform, and an empty
    // set would actually mean the replay silently skipped its work.
    let remaining = {
        let handle = target_fs.fs_dir.clone();
        let view = handle.view()?;
        view.get_block_locations(block_id)?
    };
    assert!(
        remaining.iter().any(|loc| loc.worker_id == 1),
        "worker 1's original row must survive the replay: {remaining:?}"
    );
    assert!(
        remaining.iter().any(|loc| loc.worker_id == 2),
        "worker 2's row was added inside the window; if it is missing the \
         injection never happened: {remaining:?}"
    );
    Ok(())
}
