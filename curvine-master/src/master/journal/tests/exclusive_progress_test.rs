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

//! While an exclusive metadata write waits on its real queued receipt, a
//! real JournalLoader Scan must be able to advance the applied cursor, and a
//! second write RPC must stay blocked by the same exclusive gate. Dropping
//! the receipt must fail the holder closed.

use super::*;
use crate::master::journal::{JournalBatch, JournalEntry, JournalSystem};
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::NamespaceError;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_raft::raft::RoleState;
use raft::eraftpb::Entry;

use curvine_raft::utils::SerdeUtils;
use curvine_runtime::common::Utils;
use curvine_runtime::sync::channel::{AsyncChannel, CallChannel};

use std::sync::Arc;
use std::time::Duration;

const HANDSHAKE_WINDOW: Duration = Duration::from_secs(10);

/// One bounded observation of a single result. The early window is the probe;
/// the late window only runs when the probe timed out, and both observe the
/// same receiver, so a late arrival is still the same request rather than a
/// resend. `Disconnected` is a producer-side end, not a timeout.
enum Waited<T> {
    Delivered(T),
    Disconnected,
    Timeout,
}

fn wait_same_result<T>(
    rx: &std::sync::mpsc::Receiver<T>,
    early: Duration,
    late: Duration,
) -> Waited<T> {
    match rx.recv_timeout(early) {
        Ok(value) => Waited::Delivered(value),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Waited::Disconnected,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => match rx.recv_timeout(late) {
            Ok(value) => Waited::Delivered(value),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Waited::Disconnected,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Waited::Timeout,
        },
    }
}

struct ProgressFixture {
    conf: ClusterConf,
    journal: JournalSystem,
    fs: crate::master::fs::MasterFilesystem,
    loader: JournalLoader,
    receiver: Option<AsyncReceiver<ApplyMsg>>,
    runtime: tokio::runtime::Runtime,
    meta_dir: String,
    journal_dir: String,
}

impl ProgressFixture {
    fn new(name: &str) -> Self {
        Master::init_test_metrics();
        let mut conf = ClusterConf {
            testing: true,
            ..Default::default()
        };
        conf.change_test_meta_dir(format!("exclusive-progress-{name}-{}", Utils::rand_str(8)));
        conf.journal.snapshot_entries = 0;
        conf.journal.writer_channel_size = 8;
        let meta_dir = conf.master.meta_dir.clone();
        let journal_dir = conf.journal.journal_dir.clone();
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.master_monitor.set_role_for_test(RoleState::Leader);
        // Drain the setup entries from the original testing writer.
        fs.journal_writer.with_testing_receiver(|receiver| loop {
            match receiver.try_recv() {
                Ok(entry) => drop(entry),
                Err(_) => break,
            }
        });
        let mut loader = journal.journal_loader();
        // Drive the real apply worker through a channel this test owns.
        let (sender, receiver) = AsyncChannel::new(8).split();
        loader.sender = sender;
        loader.has_apply_worker = true;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        Self {
            conf,
            journal,
            fs,
            loader,
            receiver: Some(receiver),
            runtime,
            meta_dir,
            journal_dir,
        }
    }

    fn swap_in_receipts_writer(&mut self, conf: &ClusterConf) -> Arc<JournalWriter> {
        let rt = conf.journal.create_runtime();
        let client = curvine_raft::raft::RaftClient::from_conf(rt, &conf.journal);
        let mut writer = JournalWriter::new(
            true,
            client,
            &conf.journal,
            self.journal.master_monitor().recovery(),
        )
        .unwrap();
        writer.enable_testing_receipts(self.journal.master_monitor().recovery());
        let writer = Arc::new(writer);
        self.fs
            .fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .journal_writer = writer.clone();
        self.fs.journal_writer = writer.clone();
        writer
    }
}

fn committed_entry(op_entry: JournalEntry, req_id: i64) -> Entry {
    let mut batch = JournalBatch::new(1);
    batch.push(op_entry);
    Entry {
        index: 1,
        term: 1,
        data: batch.encode().unwrap(),
        context: SerdeUtils::serialize(&req_id).unwrap(),
        ..Default::default()
    }
}

#[test]
fn scan_advances_cursor_while_exclusive_delete_awaits_its_receipt() {
    let mut fixture = ProgressFixture::new("scan-progress");
    let conf = fixture.conf.clone();

    // Phase 1: setup with the original testing writer, then drain its
    // retained entries.
    let setup_fs = fixture.fs.clone();
    setup_fs.mkdir("/dir", true).unwrap();
    setup_fs.create("/dir/file", true).unwrap();
    setup_fs.create("/validation", false).unwrap();
    setup_fs
        .journal_writer
        .with_testing_receiver(|receiver| loop {
            match receiver.try_recv() {
                Ok(entry) => drop(entry),
                Err(_) => break,
            }
        });

    // Phase 2: install the receipts writer and align every participant
    // (FsDir body / final fs clone / loader) on the same Arc — verified with
    // a short view + Arc::ptr_eq, released immediately.
    let writer = fixture.swap_in_receipts_writer(&conf);
    fixture.loader.journal_writer = writer.clone();
    // Clone AFTER the swap: MasterFilesystem clones hold their own
    // journal_writer Arc, so a pre-swap clone would keep the old writer.
    let mfs = fixture.fs.clone();
    let gate = mfs.metadata_commit_gate();
    {
        let view = mfs.fs_dir.view().unwrap();
        assert!(
            Arc::ptr_eq(&view.journal_writer, &writer),
            "the FsDir must use the receipts writer"
        );
    }
    assert!(Arc::ptr_eq(&fixture.loader.journal_writer, &writer));
    assert!(Arc::ptr_eq(&mfs.journal_writer, &writer));
    let failure_seq_before = writer.failure_seq();

    // Phase 3: start the real apply worker on the aligned loader.
    let loader_for_worker = fixture.loader.clone();
    let receiver = match fixture.receiver.take() {
        Some(receiver) => receiver,
        None => panic!("the apply receiver must exist exactly once"),
    };
    let apply_worker = fixture.runtime.spawn(async move {
        loader_for_worker.run_apply(receiver).await;
    });

    // Pre-operation opaque cursor handle: saved before the operation spawns.
    let pre_cursor = mfs.fs_dir.view().unwrap().store.cursor_store();
    // Saved, not asserted here: an unexpected early result must not skip the
    // teardown. It is checked after cleanup.
    let pre_cursor_probe = pre_cursor.get_applied_req(77);

    // Real role promotion through the actual role_change path.
    eprintln!("progress: promoting to leader");
    fixture
        .runtime
        .block_on(fixture.loader.role_change(StateRole::Leader))
        .unwrap();
    eprintln!("progress: leader promoted");

    // Public recursive delete on its own OS thread.
    let delete_fs = mfs.clone();
    let (delete_started_tx, delete_started_rx) = std::sync::mpsc::channel();
    let (delete_done_tx, delete_done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        delete_started_tx.send(()).unwrap();
        let result = delete_fs.delete("/dir", true);
        delete_done_tx.send(result).unwrap();
    });
    delete_started_rx
        .recv_timeout(HANDSHAKE_WINDOW)
        .expect("delete worker started");
    eprintln!("progress: delete worker started");

    // The real queued receipt for the delete: typed Delete('/dir') with a
    // held completion. Entry arrival does not mean the body finished —
    // observe bounded progress first.
    let queued = writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(HANDSHAKE_WINDOW)
                .expect("missing delete journal entry")
        })
        .expect("the receipts receiver must exist");
    let is_dir_delete = matches!(
        &queued.entry,
        JournalEntry::Delete(entry) if entry.path == "/dir"
    );
    assert!(is_dir_delete, "the queued entry must be the /dir delete");
    let completion = queued.completion.expect("missing commit confirmation");

    // Bounded wait for the body to complete and the cold owner to reopen the
    // view; the live edge is already deleted while the confirmation is
    // pending.
    let deadline = std::time::Instant::now() + HANDSHAKE_WINDOW;
    let view_while_held = loop {
        match mfs.fs_dir.view() {
            Ok(_) => break None,
            Err(NamespaceError::Busy) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            Err(e) => break Some(e),
        }
    };
    assert!(
        view_while_held.is_none(),
        "the view must reopen once the body completes, got {view_while_held:?}"
    );
    assert!(
        matches!(mfs.file_status("/dir"), Err(e) if matches!(e, curvine_error::FsError::FileNotFound(_))),
        "the directory must be deleted in the live tree"
    );
    let seq_while_held = writer.failure_seq();
    assert_eq!(seq_while_held, failure_seq_before);
    let worker_probe = delete_done_rx.recv_timeout(Duration::from_millis(100));
    // Saved and reused: if the delete already reported here, the late wait
    // below must consume this result instead of waiting again.
    let worker_pending_at_body = matches!(
        worker_probe,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );

    // The same exclusive gate must still be held by the waiting delete.
    assert!(
        gate.has_exclusive_writer(),
        "the exclusive gate must stay closed across the confirmation wait"
    );
    // Arc identity: the loader's gate is the filesystem's gate.
    assert!(
        Arc::ptr_eq(
            &fixture.loader.metadata_commit_gate,
            &mfs.metadata_commit_gate()
        ),
        "the loader and the filesystem must share one gate"
    );

    // A second write RPC must stay blocked by the gate. `/validation` already
    // exists from setup, so this create targets a prepared path and simply
    // cannot proceed while the gate is closed. Started ack + bounded pending
    // is the evidence.
    let rpc_fs = mfs.clone();
    let (rpc_started_tx, rpc_started_rx) = std::sync::mpsc::channel::<()>();
    let (rpc_tx, rpc_rx) = std::sync::mpsc::channel();
    let rpc = std::thread::spawn(move || {
        rpc_started_tx.send(()).unwrap();
        rpc_tx.send(rpc_fs.create("/validation", false))
    });
    let rpc_started = rpc_started_rx.recv_timeout(HANDSHAKE_WINDOW);
    let rpc_pending = rpc_rx.recv_timeout(Duration::from_millis(200));
    // Saved and reused below, like the other probes: only a timed-out probe
    // starts another wait on this receiver. The "blocked by the gate" check
    // moves to the closing assertions so an early result cannot panic past
    // teardown.
    let rpc_blocked_at_body =
        matches!(rpc_pending, Err(std::sync::mpsc::RecvTimeoutError::Timeout));

    // Real Scan through the loader: register the delete as committed, run the
    // actual apply worker against it, and observe the cursor advance. The
    // receipt is still held — the applier must be able to proceed.
    let entry = committed_entry(queued.entry.clone(), 1);
    fixture
        .loader
        .log_store
        .append(std::slice::from_ref(&entry))
        .expect("append committed entry");
    fixture
        .loader
        .log_store
        .set_hard_state_commit(entry.index)
        .expect("advance committed index");

    let scan_loader = fixture.loader.clone();
    let scan_outcome = fixture.runtime.block_on(async {
        tokio::time::timeout(
            HANDSHAKE_WINDOW,
            scan_loader.apply(true, ApplyMsg::new_scan(AppliedIndex::default())),
        )
        .await
    });
    // One request, one result: a second scan would be a new request, not the
    // same receiver, so a timeout is reported instead of retried.
    let scan_result = match scan_outcome {
        Ok(result) => result,
        Err(_) => panic!("the real Scan must return within the bound"),
    };
    assert!(
        scan_result.is_ok(),
        "the real Scan must succeed while the receipt is held, got {scan_result:?}"
    );

    // The FSM applied watermark and the cursor store must both reflect the
    // committed entry — real confirmation, not a mocked ack.
    eprintln!("progress: scan done");
    let applied = fixture.loader.fsm_state_snapshot().unwrap().applied;
    assert_eq!(applied.index, 1, "the FSM applied index must advance");
    assert_eq!(applied.term, 1);
    let cursor_req = fixture
        .loader
        .cursor_store
        .get_applied_req(1)
        .expect("the cursor store must stay readable while the receipt is held");
    let cursor_req = cursor_req.expect("the applied request must be recorded");
    assert_eq!(cursor_req.index, 1);
    assert_eq!(cursor_req.term, 1);
    drop(scan_result);

    // Drop the receipt: the confirmation fails for real, the RAII guard goes
    // terminal before the exclusive gate opens.
    drop(completion);

    // Only a timed-out probe starts another wait on the same receiver.
    let delete_outcome = match worker_probe {
        Ok(value) => Waited::Delivered(value),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Waited::Disconnected,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            wait_same_result(&delete_done_rx, HANDSHAKE_WINDOW, HANDSHAKE_WINDOW)
        }
    };
    let delete_done = match delete_outcome {
        Waited::Delivered(result) => result,
        Waited::Disconnected => {
            // The producer already ended, so this join cannot block.
            worker.join().expect("the delete worker must not panic");
            panic!("the delete worker exited without reporting a result")
        }
        Waited::Timeout => panic!(
            "the delete did not report a result; skipping its join and the blocking teardown"
        ),
    };
    let delete_err = match delete_done {
        Ok(_) => panic!("the delete must fail on a dropped confirmation"),
        Err(err) => err,
    };
    worker.join().expect("delete worker must not panic");
    assert!(mfs.fs_dir.is_failed());
    assert!(
        matches!(mfs.fs_dir.view().err(), Some(NamespaceError::Unavailable)),
        "the view must be Unavailable after the terminal failure"
    );
    assert_ne!(writer.failure_seq(), failure_seq_before);
    let cursor_retired = pre_cursor.get_applied_req(77);
    let cursor_retired_err =
        cursor_retired.expect_err("the retired cursor must fail point lookups");
    assert!(
        cursor_retired_err
            .to_string()
            .contains("cursor store unavailable during metadata restore"),
        "the retired cursor must report the restore-unavailable error, got {cursor_retired_err}"
    );
    assert!(
        delete_err
            .to_string()
            .contains("journal commit failed after metadata commit"),
        "the delete must surface the real commit error, got {delete_err}"
    );

    // The blocked RPC resumes only to be fenced: the writer refuses new
    // admissions after the terminal failure.
    // Only a timed-out probe starts another wait on the same receiver.
    let rpc_outcome = match rpc_pending {
        Ok(value) => Waited::Delivered(value),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Waited::Disconnected,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            wait_same_result(&rpc_rx, HANDSHAKE_WINDOW, HANDSHAKE_WINDOW)
        }
    };
    let rpc_result = match rpc_outcome {
        Waited::Delivered(result) => result,
        Waited::Disconnected => {
            // The thread already ended, so this join cannot block.
            rpc.join().expect("the blocked RPC thread must not panic");
            panic!("the blocked RPC thread exited without reporting a result")
        }
        Waited::Timeout => panic!("the blocked RPC did not resume; skipping its join"),
    };
    let rpc_err = rpc_result
        .expect_err("the resumed RPC must be rejected by the fence, not run");
    assert!(
        rpc_err
            .to_string()
            .contains("journal writer is not accepting writes"),
        "the resumed RPC must be fenced by the writer, got {rpc_err}"
    );

    // Teardown: stop the real apply worker with a bounded shutdown, drain the
    // receipts, consume the journal system, remove this case's temp paths.
    let (shutdown_tx, shutdown_rx) = CallChannel::channel();
    fixture
        .runtime
        .block_on(async {
            fixture
                .loader
                .sender
                .send(ApplyMsg::Shutdown(shutdown_tx))
                .await
        })
        .unwrap();
    let shutdown_ack = fixture
        .runtime
        .block_on(async { tokio::time::timeout(HANDSHAKE_WINDOW, shutdown_rx.receive()).await });
    match shutdown_ack {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("the shutdown channel dropped without acknowledging: {error}"),
        Err(_) => {
            panic!("the shutdown was not acknowledged; skipping teardown that could block")
        }
    }
    writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });
    // A shutdown ack is not worker exit: join the real apply worker and the
    // blocked RPC thread before tearing the directories down.
    let worker_exit = fixture
        .runtime
        .block_on(async { tokio::time::timeout(HANDSHAKE_WINDOW, apply_worker).await });
    match worker_exit {
        Ok(result) => result.expect("the apply worker must not panic"),
        Err(_) => {
            panic!("the apply worker did not exit after shutdown; skipping blocking teardown")
        }
    }
    rpc.join().expect("the blocked RPC thread must not panic");
    // Release every independent handle this case still owns before the
    // journal system is shut down.
    drop(scan_loader);
    drop(mfs);
    drop(fixture.fs);
    drop(fixture.loader);
    drop(fixture.receiver);
    drop(fixture.runtime);
    fixture.journal.shutdown();
    let meta_cleanup = std::fs::remove_dir_all(&fixture.meta_dir);
    let journal_cleanup = std::fs::remove_dir_all(&fixture.journal_dir);
    meta_cleanup.expect("the meta dir must be removable");
    journal_cleanup.expect("the journal dir must be removable");
    // Observations saved before teardown are asserted only now.
    assert!(
        matches!(&pre_cursor_probe, Ok(None)),
        "the live cursor must answer point lookups, got {pre_cursor_probe:?}"
    );
    rpc_started.expect("the blocked RPC thread must have started");
    assert!(
        worker_pending_at_body,
        "the delete must still be waiting when the body completes"
    );
    assert!(
        rpc_blocked_at_body,
        "a second write RPC must stay blocked by the exclusive gate"
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn exclusive_delete_store_error_keeps_holder_open_and_keeps_op_error() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let mut fixture = ProgressFixture::new("excl-pub-fail");
    let conf = fixture.conf.clone();
    // A per-instance unique directory: the fault matcher below is keyed on
    // this exact name, so another case deleting "/dir" cannot consume it.
    let dir_name = format!("dir-{}", Utils::rand_str(8));
    let dir_path = format!("/{dir_name}");
    let setup_fs = fixture.fs.clone();
    setup_fs.mkdir(&dir_path, true).unwrap();
    setup_fs.create(format!("{dir_path}/file"), true).unwrap();
    setup_fs
        .journal_writer
        .with_testing_receiver(|receiver| loop {
            match receiver.try_recv() {
                Ok(entry) => drop(entry),
                Err(_) => break,
            }
        });

    // The setup clone is no longer needed: keeping it alive would hold the
    // store of an OPEN holder.
    drop(setup_fs);
    let writer = fixture.swap_in_receipts_writer(&conf);
    fixture.loader.journal_writer = writer.clone();
    let mfs = fixture.fs.clone();
    let failure_seq_before = writer.failure_seq();

    // Store-commit failure inside the exclusive body: the batch commit fails
    // before any publication, so the failure sequence does NOT move and the
    // holder stays OPEN (pure validation/store error → backfill).
    let runtime = curvine_fault::FaultRuntime::process();
    let rule_id = "excl-pub-fail-rule";
    runtime
        .configure(
            rule_id,
            curvine_fault::FaultRuleBuilder::named("master.metadata.delete.before_commit")
                .matches("child_name", dir_name.as_str())
                .unwrap()
                .times(1)
                .unwrap()
                .return_error("injected exclusive delete store failure")
                .unwrap(),
        )
        .unwrap();

    let delete_fs = mfs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let delete_path = dir_path.clone();
    let worker = std::thread::spawn(move || done_tx.send(delete_fs.delete(&delete_path, true)));

    let delete_outcome = wait_same_result(&done_rx, HANDSHAKE_WINDOW, HANDSHAKE_WINDOW);
    // Remove the rule before any branch below can fail, so a panic never
    // leaves the process-global fault configured.
    runtime.remove(rule_id).unwrap();
    let delete_result = match delete_outcome {
        Waited::Delivered(result) => result,
        Waited::Disconnected => {
            // The producer already ended, so this join cannot block.
            worker.join().expect("delete worker must not panic");
            panic!("the delete worker exited without reporting a result")
        }
        Waited::Timeout => {
            panic!("the delete did not report a result; skipping its join and blocking teardown")
        }
    };
    worker.join().expect("delete worker must not panic");

    // The op error keeps its original variant and message; the failure
    // sequence did NOT move, so the holder stays OPEN (normal backfill).
    let delete_err = delete_result.expect_err("the injected store failure must surface");
    assert!(
        matches!(&delete_err, curvine_error::FsError::Common(_)),
        "the injected store failure must keep its original Common variant, got {delete_err:?}"
    );
    assert!(
        delete_err
            .to_string()
            .contains("injected exclusive delete store failure"),
        "the original op error must be preserved, got {delete_err}"
    );
    assert!(
        !mfs.fs_dir.is_failed(),
        "a pre-publication store error must not fail the holder"
    );
    assert!(
        mfs.fs_dir.view().is_ok(),
        "the view must reopen after a pure store error"
    );
    assert_eq!(writer.failure_seq(), failure_seq_before);

    // Same closedown discipline as the other cases: release the borrowed
    // handles, shut the journal system down, then remove both trees with
    // their results kept.
    drop(mfs);
    let ProgressFixture {
        journal: fixture_journal,
        fs: fixture_fs,
        loader: fixture_loader,
        receiver: fixture_receiver,
        runtime: fixture_runtime,
        meta_dir,
        journal_dir,
        ..
    } = fixture;
    drop(fixture_fs);
    drop(fixture_loader);
    drop(fixture_receiver);
    drop(fixture_runtime);
    fixture_journal.shutdown();
    let meta_cleanup = std::fs::remove_dir_all(&meta_dir);
    let journal_cleanup = std::fs::remove_dir_all(&journal_dir);
    meta_cleanup.expect("the store-error meta tree must be removed");
    journal_cleanup.expect("the store-error journal tree must be removed");
}

#[test]
fn restore_intent_waits_for_the_exclusive_gate_then_reports_unavailable() -> FsResult<()> {
    use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
    use curvine_raft::raft::storage::AppStorage as _;
    use curvine_raft::raft::RaftResult;

    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 8;
    let meta_dir = std::env::temp_dir().join(format!(
        "curvine-exclusive-restore-meta-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal_dir = std::env::temp_dir().join(format!(
        "curvine-exclusive-restore-journal-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf.master.meta_dir = meta_dir.to_str().unwrap().to_string();
    conf.journal.journal_dir = journal_dir.to_str().unwrap().to_string();
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let mut fs = journal.fs();
    let mut loader = journal.journal_loader();
    fs.master_monitor.set_role_for_test(RoleState::Leader);

    // Setup with the original writer, then drain.
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.journal_writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });

    // Real non-empty snapshot before the exclusive operation.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let snapshot = runtime
        .block_on(loader.create_snapshot())
        .expect("a real checkpoint must be created");
    assert!(snapshot.files_data.is_some());

    // Install the receipts writer and align every participant: FsDir body,
    // local fs, and loader all share one Arc — verified with a short view +
    // Arc::ptr_eq before spawning the delete.
    let writer = {
        let rt = conf.journal.create_runtime();
        let client = curvine_raft::raft::RaftClient::from_conf(rt, &conf.journal);
        let mut writer = JournalWriter::new(
            true,
            client,
            &conf.journal,
            journal.master_monitor().recovery(),
        )
        .unwrap();
        writer.enable_testing_receipts(journal.master_monitor().recovery());
        let writer = Arc::new(writer);
        fs.fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .journal_writer = writer.clone();
        writer
    };
    fs.journal_writer = writer.clone();
    loader.journal_writer = writer.clone();
    {
        // Short view: verify all three writer Arcs are aligned.
        let view = fs.fs_dir.view().unwrap();
        assert!(
            Arc::ptr_eq(&view.journal_writer, &writer),
            "FsDir must use the receipts writer"
        );
    }
    assert!(Arc::ptr_eq(&fs.journal_writer, &writer));
    assert!(Arc::ptr_eq(&loader.journal_writer, &writer));

    // Phase 2: exclusive delete holds the gate and the unique receipt.
    let delete_fs = fs.clone();
    let (delete_started_tx, delete_started_rx) = std::sync::mpsc::channel();
    let (delete_done_tx, delete_done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        delete_started_tx.send(()).unwrap();
        let result = delete_fs.delete("/dir", true);
        delete_done_tx.send(result).unwrap();
    });
    delete_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("delete worker started");
    let queued = writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("missing delete journal entry")
        })
        .expect("the receipts receiver must exist");
    // The queued receipt must be the typed recursive Delete for /dir, not any
    // other entry that happens to arrive first.
    assert!(
        matches!(&queued.entry, JournalEntry::Delete(entry) if entry.path == "/dir"),
        "the queued entry must be the /dir delete, got {:?}",
        queued.entry
    );
    let completion = queued.completion.expect("missing commit confirmation");

    // Observe the completed body before starting the restore intent. Entry
    // arrival only proves an enqueue; the body must have published and
    // reopened the view, the delete must already be applied, the failure
    // sequence must not have moved, and the delete must still wait for this
    // receipt.
    let failure_seq_before_restore = writer.failure_seq();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut view_reopened = false;
    while std::time::Instant::now() < deadline {
        if fs.fs_dir.view().is_ok() {
            view_reopened = true;
            break;
        }
        std::thread::yield_now();
    }
    let live_status = fs.file_status("/dir");
    let delete_probe = delete_done_rx.recv_timeout(Duration::from_millis(100));
    // Saved and reused below; only a timed-out probe starts another wait.
    let delete_pending_at_body = matches!(
        delete_probe,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );
    assert!(
        view_reopened,
        "the body must complete and reopen the view before the restore intent starts"
    );
    assert!(
        matches!(&live_status, Err(error) if matches!(error, curvine_error::FsError::FileNotFound(_))),
        "the deleted directory must already be gone from the live tree, got {live_status:?}"
    );
    assert_eq!(
        writer.failure_seq(),
        failure_seq_before_restore,
        "a completed body must not bump the publication failure sequence"
    );
    assert!(
        matches!(
            delete_probe,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "the delete must still wait for its confirmation, got {delete_probe:?}"
    );

    // Restore intent: apply_snapshot must wait on the same exclusive gate.
    let restore_loader = loader.clone();
    let (restore_started_tx, restore_started_rx) = std::sync::mpsc::channel();
    let (restore_done_tx, restore_done_rx) = std::sync::mpsc::channel::<RaftResult<()>>();
    let snapshot_clone = snapshot.clone();
    let restore_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let restore_thread = std::thread::spawn(move || {
        restore_started_tx.send(()).unwrap();
        let result = restore_runtime.block_on(restore_loader.apply_snapshot(snapshot_clone));
        restore_done_tx.send(result).unwrap();
    });
    restore_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("restore thread started");
    let restore_pending = restore_done_rx.recv_timeout(Duration::from_millis(300));
    // Saved and reused below; the "still waiting" check moves to the end.
    let restore_pending_at_body = matches!(
        restore_pending,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );

    // Drop the receipt: the delete fails terminal, the gate opens, and the
    // restore proceeds into apply_snapshot0 — reporting the holder failed.
    eprintln!("restore: dropping completion");
    drop(completion);
    // Only a timed-out probe starts another wait on the same receiver.
    let delete_outcome = match delete_probe {
        Ok(value) => Waited::Delivered(value),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Waited::Disconnected,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => wait_same_result(
            &delete_done_rx,
            Duration::from_secs(10),
            Duration::from_secs(10),
        ),
    };
    let delete_done = match delete_outcome {
        Waited::Delivered(result) => result,
        Waited::Disconnected => {
            // The producer already ended, so this join cannot block.
            worker.join().expect("delete worker must not panic");
            panic!("the delete worker exited without reporting a result")
        }
        Waited::Timeout => {
            panic!("the delete did not report a result; skipping its join and blocking teardown")
        }
    };
    worker.join().expect("delete worker must not panic");
    let delete_err = match delete_done {
        Ok(_) => panic!("the delete must fail on a dropped confirmation"),
        Err(err) => err,
    };
    assert!(fs.fs_dir.is_failed());
    assert!(
        delete_err
            .to_string()
            .contains("journal commit failed after metadata commit"),
        "the delete must surface the real commit error, got {delete_err}"
    );

    let restore_outcome = match restore_pending {
        Ok(value) => Waited::Delivered(value),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Waited::Disconnected,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => wait_same_result(
            &restore_done_rx,
            Duration::from_secs(30),
            Duration::from_secs(30),
        ),
    };
    let restore_result = match restore_outcome {
        Waited::Delivered(result) => result,
        Waited::Disconnected => {
            // The producer already ended, so this join cannot block.
            restore_thread
                .join()
                .expect("restore thread must not panic");
            panic!("the restore thread exited without reporting a result")
        }
        Waited::Timeout => {
            panic!("the restore intent did not return; skipping its join and the blocking teardown")
        }
    };
    restore_thread
        .join()
        .expect("restore thread must not panic");

    let restore_err = match restore_result {
        Ok(_) => panic!("the restore must not succeed on a FAILED holder"),
        Err(err) => err,
    };
    assert!(
        restore_err
            .to_string()
            .contains("namespace permanently unavailable after failure"),
        "the restore must report the terminal namespace state, got {restore_err}"
    );

    drop(fs);
    drop(loader);
    // The case-owned runtime is an independent handle too: release it before
    // the journal system is shut down.
    drop(runtime);
    journal.shutdown();
    // Only the two independent roots this case created are removed. The
    // snapshot's files directory lives under meta_dir/checkpoint, so removing
    // meta_dir already covers it; deleting it separately would report a
    // NotFound caused by the parent removal, not by the restore.
    let meta_cleanup = std::fs::remove_dir_all(&meta_dir);
    let journal_cleanup = std::fs::remove_dir_all(&journal_dir);
    meta_cleanup.expect("the restore meta tree must be removed");
    journal_cleanup.expect("the restore journal tree must be removed");
    // The early probes must have observed real waiting, not an early result.
    assert!(
        delete_pending_at_body,
        "the delete must still be waiting when the body completes"
    );
    assert!(
        restore_pending_at_body,
        "the restore intent must wait on the exclusive gate"
    );
    Ok(())
}
