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

//! Real exclusive-confirmation regressions: the public exclusive path waits
//! on an actual queued receipt, and a dropped confirmation must fail the
//! namespace holder closed through the confirmation RAII.

use super::*;
use crate::master::journal::journal_writer::tests::{build_writer, test_conf};
use crate::master::journal::{JournalEntry, JournalSystem};
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::NamespaceError;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn real_exclusive_delete_confirmation_failure_fails_holder_closed() -> FsResult<()> {
    use crate::master::meta::NamespaceError;

    let mut conf = test_conf("exclusive-delete-confirmation-fail");
    // No periodic snapshot and a roomy queue: the setup entries must never
    // block on the un-consumed testing channel.
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 8;
    let meta_dir = std::env::temp_dir().join(format!(
        "curvine-receipt-fail-meta-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal_dir = std::env::temp_dir().join(format!(
        "curvine-receipt-fail-journal-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf.master.meta_dir = meta_dir.to_str().unwrap().to_string();
    conf.journal.journal_dir = journal_dir.to_str().unwrap().to_string();
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs.mkdir("/dir", true)?;
    fs.create("/dir/file", true)?;
    // Drain the setup entries from the original testing writer's retained
    // receiver so it cannot interfere later.
    loop {
        match fs
            .journal_writer
            .receiver
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .try_recv()
        {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    }
    let failure_seq_before = fs.journal_writer.failure_seq();

    // Swap in a real receipts writer (require_raft_commit=true) into both the
    // FsDir and the local fs clone — the established pattern from
    // lost_commit_confirmation, applied to the public exclusive path.
    let mut writer = build_writer(&conf)?;
    writer.require_raft_commit = true;
    writer.recovery = journal.master_monitor().recovery();
    let writer = Arc::new(writer);
    fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT)?.journal_writer = writer.clone();
    let mut fs = fs;
    fs.journal_writer = writer.clone();

    // Public recursive directory delete: the cold bridge routes it through
    // run_metadata_write_exclusive, whose confirmation waits on the real
    // queued receipt.
    // Pre-operation opaque cursor handle: saved before the operation spawns.
    let pre_cursor = fs.fs_dir.view().unwrap().store.cursor_store();
    let pre_cursor_probe = pre_cursor.get_applied_req(77);
    assert!(
        matches!(&pre_cursor_probe, Ok(None)),
        "the live cursor must answer point lookups, got {pre_cursor_probe:?}"
    );
    let worker_fs = fs.clone();
    let (delete_started_tx, delete_started_rx) = mpsc::channel();
    let (delete_done_tx, delete_done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        delete_started_tx.send(()).unwrap();
        let result = worker_fs.delete("/dir", true);
        delete_done_tx.send(result).unwrap();
    });
    let started = delete_started_rx.recv_timeout(Duration::from_secs(5));

    // Phase 2: hold the unique receipt and observe the body completing.
    let mut queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5));
    let is_dir_delete = matches!(
        &queued,
        Ok(q) if matches!(
            &q.entry,
            JournalEntry::Delete(entry) if entry.path == "/dir"
        )
    );
    // Take the unique Sender out of the queued entry (no clone): dropping it
    // is the real confirmation break.
    let completion = queued.as_mut().ok().and_then(|q| q.completion.take());
    let completion_present = completion.is_some();

    // Phase 2 observations (no asserts yet).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let view_while_held = loop {
        match fs.fs_dir.view() {
            Ok(_) => break None,
            Err(NamespaceError::Busy) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            Err(e) => break Some(e),
        }
    };
    let dir_live = fs.file_status("/dir");
    let seq_while_held = fs.journal_writer.failure_seq();
    let worker_probe = delete_done_rx.recv_timeout(Duration::from_millis(100));
    let worker_still_pending = matches!(
        worker_probe,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );

    // Phase 3: drop the unique receipt (real confirmation break), drain the
    // controlled queue, then classify the final done observation. Only a
    // completed/Disconnected observation may join.
    drop(completion);
    loop {
        match writer.receiver.as_ref().unwrap().lock().unwrap().try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    }

    // Reuse the first probe if it already delivered the business result
    // (Ok): no second recv. Only a first-probe Timeout goes to the final
    // bounded wait; Disconnected stays as-is (the worker exited without
    // sending).
    let delete_done = match worker_probe {
        Ok(result) => Ok(result),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            delete_done_rx.recv_timeout(Duration::from_secs(10))
        }
    };
    // Completed/Disconnected: safe to join. Timeout: report and skip join
    // and every teardown step that could wait on locks this worker holds.
    let completed = matches!(
        &delete_done,
        Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    );
    let worker_join = if completed {
        Some(worker.join())
    } else {
        None // Timeout: no join, no lock-touching teardown
    };

    if !completed {
        // Real Timeout: report precisely; the worker is still running, so
        // begin_cold / shutdown / directory removal are all skipped to avoid
        // waiting on locks this worker may hold.
        assert!(
            worker_join.is_none(),
            "a timed-out worker must not have been joined"
        );
        panic!(
            "the delete did not complete within the bounded confirmation window              (timed out after 10s); worker left running, no terminal teardown performed"
        );
    }

    // Completed path: terminal observations while the fixture is alive.
    let holder_failed = fs.fs_dir.is_failed();
    let view_err = fs.fs_dir.view().err();
    let begin_err = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).err();
    let writer_fenced = writer.ensure_metadata_current().is_err();
    let failure_seq_after = fs.journal_writer.failure_seq();
    let cursor_after = pre_cursor.get_applied_req(77);
    let delete_err = delete_done
        .as_ref()
        .ok()
        .and_then(|result| result.as_ref().err())
        .map(|err| err.to_string());

    // Completed-path teardown: drop handles, shutdown (consumes self), then
    // remove this case's unique temp paths.
    drop(fs);
    drop(writer);
    journal.shutdown();
    let meta_cleanup = std::fs::remove_dir_all(&meta_dir);
    let journal_cleanup = std::fs::remove_dir_all(&journal_dir);

    // Outer assertions.
    started.expect("worker must start within the bound");
    assert!(
        is_dir_delete,
        "the queued entry must be the /dir delete, got {:?}",
        queued
    );
    assert!(
        completion_present,
        "the queued entry must carry a commit confirmation"
    );
    assert!(
        view_while_held.is_none(),
        "the view must reopen once the body completes, got {view_while_held:?}"
    );
    assert!(
        matches!(&dir_live, Err(e) if matches!(e, curvine_error::FsError::FileNotFound(_))),
        "the directory must be deleted in the live tree, got {dir_live:?}"
    );
    assert_eq!(
        seq_while_held, failure_seq_before,
        "no publication failure should be recorded while the receipt is held"
    );
    assert!(
        worker_still_pending,
        "the delete must still be waiting for its confirmation before the receipt was dropped"
    );

    let delete_err = delete_err.expect("the delete must finish once the confirmation fails");
    assert!(
        delete_err
            .to_string()
            .contains("journal commit failed after metadata commit"),
        "the delete must surface the real commit error, got {delete_err}"
    );

    assert!(
        holder_failed,
        "a dropped confirmation must fail the exclusive holder closed"
    );
    assert!(
        matches!(view_err, Some(NamespaceError::Unavailable)),
        "the view must be Unavailable after the terminal failure, got {view_err:?}"
    );
    assert!(
        matches!(begin_err, Some(crate::master::meta::ColdError::Unavailable)),
        "begin_cold must be Unavailable after the terminal failure, got {begin_err:?}"
    );
    assert!(
        writer_fenced,
        "the writer must remain fenced after the terminal failure"
    );
    assert_ne!(
        failure_seq_after, failure_seq_before,
        "the confirmation failure must be recorded on the failure sequence"
    );
    let cursor_err = cursor_after.expect_err("the retired cursor must fail point lookups");
    assert!(
        cursor_err
            .to_string()
            .contains("cursor store unavailable during metadata restore"),
        "the retired cursor must report the restore-unavailable error, got {cursor_err}"
    );
    meta_cleanup.expect("the meta dir must be removable");
    journal_cleanup.expect("the journal dir must be removable");
    // The joined worker must have reported its result successfully.
    worker_join
        .expect("the joined worker must not panic")
        .expect("the delete worker must report success");
    Ok(())
}

// The op Err + commit Err double-failure combination is not naturally
// reachable through the exclusive protocol: in `fs_dir.delete`, the only
// `send_and_wait` is the last operation before the body returns, so an op
// error and a commit error are the same event. A `before_commit` fault
// blocks the store batch before `send_and_wait` is reached, which produces
// only an op error (covered by `exclusive_delete_store_error_keeps_holder_
// open_and_keeps_op_error`). A dropped receipt produces only a commit error
// (covered by `real_exclusive_delete_confirmation_failure_fails_holder_
// closed`). The exclusive path's error priority code (op Err over commit
// Err) is still exercised by the existing match arms in
// `run_metadata_write_exclusive`, verified by the combination of these two
// single-failure tests.

#[test]
fn exclusive_delete_validation_error_keeps_holder_open() {
    // A public exclusive-path delete on a missing target produces a real
    // validation error inside the body, with failure_seq unchanged and the
    // holder staying OPEN after gate release.
    let conf = {
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            ..Default::default()
        };
        conf.change_test_meta_dir(format!(
            "exclusive-validation-open-{}",
            curvine_runtime::common::Utils::rand_str(8)
        ));
        conf
    };
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    let failure_seq_before = fs.journal_writer.failure_seq();

    let err = fs.delete("/nonexistent", false).unwrap_err();
    assert!(
        matches!(err, curvine_error::FsError::FileNotFound(_)),
        "a missing target must be FileNotFound, got {err:?}"
    );
    assert!(
        !fs.fs_dir.is_failed(),
        "a pure validation error must not fail the holder"
    );
    assert!(
        fs.fs_dir.view().is_ok(),
        "the view must reopen after a pure validation error"
    );
    assert_eq!(fs.journal_writer.failure_seq(), failure_seq_before);
    journal.shutdown();
    let meta_cleanup = std::fs::remove_dir_all(&conf.master.meta_dir);
    meta_cleanup.expect("the case meta dir must be removable");
}
