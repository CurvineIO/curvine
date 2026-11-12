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
    delete_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("delete worker started");

    // The queued receipt arrives with its completion still held: the delete
    // is applied to the live namespace but NOT yet confirmed. Every
    // observation is saved as a Result/bool — nothing asserts mid-flow.
    let queued = writer
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
    let completion = queued.as_ref().ok().and_then(|q| q.completion.clone());

    // While the receipt is held: wait (bounded) for the body to finish and
    // the cold owner to be released — the view reopens and the live edge is
    // already deleted, while the confirmation is still pending.
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

    // Drop the completion: the confirmation wait fails for real, the RAII
    // guard goes terminal before the exclusive gate opens.
    drop(completion);
    // Drain the controlled queue before waiting on done/join (single writer
    // Arc; this fixture's real entry was already received above).
    loop {
        match writer.receiver.as_ref().unwrap().lock().unwrap().try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    }

    let delete_done = delete_done_rx.recv_timeout(Duration::from_secs(10));
    // Only a completed observation may join; a Timeout must fail explicitly
    // without joining a still-running worker or touching locks it may hold.
    let worker_join = match &delete_done {
        Ok(_) => Some(worker.join()),
        Err(_) => None, // Timeout: do not join a still-running worker
    };

    // Collect terminal observations while the fixture is alive.
    let holder_failed = fs.fs_dir.is_failed();
    let view_err = fs.fs_dir.view().err();
    let begin_err = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).err();
    let writer_fenced = writer.ensure_metadata_current().is_err();
    let failure_seq_after = fs.journal_writer.failure_seq();
    let cursor_err = pre_cursor.get_applied_req(1);

    // Teardown: drop the filesystem handles and consume the journal system,
    // then remove this case's unique temp paths. remove_dir_all errors are
    // saved, not swallowed.
    drop(fs);
    journal.shutdown();
    let meta_cleanup = std::fs::remove_dir_all(&meta_dir);
    let journal_cleanup = std::fs::remove_dir_all(&journal_dir);

    // Outer assertions, after full teardown.
    let queued = queued.expect("the delete journal entry must be queued");
    assert!(
        is_dir_delete,
        "the queued entry must be the /dir delete, got {:?}",
        queued.entry
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
        matches!(
            worker_probe,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "the delete must still be waiting for its confirmation, got {worker_probe:?}"
    );

    let delete_result = delete_done.expect("the delete must finish once the confirmation fails");
    let delete_err = match delete_result {
        Ok(_) => panic!("the delete must fail on a dropped confirmation"),
        Err(err) => err,
    };

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
    assert!(
        delete_err
            .to_string()
            .contains("journal commit failed after metadata commit"),
        "the delete must surface the real commit error, got {delete_err}"
    );
    let cursor_err = cursor_err.expect_err("the retired cursor must fail point lookups");
    assert!(
        cursor_err
            .to_string()
            .contains("cursor store unavailable during metadata restore"),
        "the retired cursor must report the restore-unavailable error, got {cursor_err}"
    );
    meta_cleanup.expect("the meta dir must be removable");
    journal_cleanup.expect("the journal dir must be removable");
    Ok(())
}
