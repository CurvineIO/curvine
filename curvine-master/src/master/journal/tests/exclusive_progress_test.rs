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
use crate::master::fs::DeleteMode;
use crate::master::journal::{JournalBatch, JournalEntry, JournalSystem};
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::{InodeId, NamespaceError};
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
        context: SerdeUtils::serialize(&req_id).unwrap().into(),
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
    assert!(
        matches!(
            worker_probe,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "the delete must still be waiting for its confirmation, got {worker_probe:?}"
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

    // A second write RPC must stay blocked by the gate: create on a fresh
    // path fails nothing, it just cannot proceed. Started ack + bounded
    // pending is the evidence.
    let rpc_fs = mfs.clone();
    let (rpc_started_tx, rpc_started_rx) = std::sync::mpsc::channel::<()>();
    let (rpc_tx, rpc_rx) = std::sync::mpsc::channel();
    let rpc = std::thread::spawn(move || {
        rpc_started_tx.send(()).unwrap();
        rpc_tx.send(rpc_fs.create("/validation-2", false))
    });
    rpc_started_rx
        .recv_timeout(HANDSHAKE_WINDOW)
        .expect("rpc worker started");
    assert!(
        rpc_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "a second write RPC must stay blocked by the exclusive gate"
    );

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
    let scan_result = fixture.runtime.block_on(async {
        tokio::time::timeout(
            HANDSHAKE_WINDOW,
            scan_loader.apply(true, ApplyMsg::new_scan(AppliedIndex::default())),
        )
        .await
        .expect("real Scan must complete within the window")
        .expect("the real Scan must succeed while the receipt is held");
    });

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

    let delete_done = delete_done_rx
        .recv_timeout(HANDSHAKE_WINDOW)
        .expect("the delete must finish once the confirmation fails");
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
    assert!(
        delete_err
            .to_string()
            .contains("journal commit failed after metadata commit"),
        "the delete must surface the real commit error, got {delete_err}"
    );

    // The blocked RPC resumes only to be fenced: the writer refuses new
    // admissions after the terminal failure.
    let rpc_result = rpc_rx
        .recv_timeout(HANDSHAKE_WINDOW)
        .expect("the blocked RPC must resume once the gate opens");
    let rpc_err = rpc_result
        .err()
        .expect("the resumed RPC must be rejected by the fence, not run");
    assert!(
        rpc_err
            .to_string()
            .contains("journal writer is not accepting writes")
            || rpc_err.to_string().contains("JournalUnavailable"),
        "the resumed RPC must be fenced, got {rpc_err}"
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
    fixture
        .runtime
        .block_on(async {
            tokio::time::timeout(HANDSHAKE_WINDOW, shutdown_rx.receive())
                .await
                .expect("shutdown must be acknowledged")
        })
        .unwrap();
    writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });
    drop(mfs);
    fixture.journal.shutdown();
    let _ = std::fs::remove_dir_all(&fixture.meta_dir);
    let _ = std::fs::remove_dir_all(&fixture.journal_dir);
}

#[cfg(feature = "fault-injection")]
#[test]
fn exclusive_delete_store_error_keeps_holder_open_and_keeps_op_error() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let mut fixture = ProgressFixture::new("excl-pub-fail");
    let conf = fixture.conf.clone();
    let setup_fs = fixture.fs.clone();
    setup_fs.mkdir("/dir", true).unwrap();
    setup_fs.create("/dir/file", true).unwrap();
    setup_fs
        .journal_writer
        .with_testing_receiver(|receiver| loop {
            match receiver.try_recv() {
                Ok(entry) => drop(entry),
                Err(_) => break,
            }
        });

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
                .matches("child_name", "dir")
                .unwrap()
                .times(1)
                .unwrap()
                .return_error("injected exclusive delete store failure")
                .unwrap(),
        )
        .unwrap();

    let delete_fs = mfs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(delete_fs.delete("/dir", true)));

    let delete_result = done_rx.recv_timeout(HANDSHAKE_WINDOW);
    runtime.remove(rule_id).unwrap();
    let delete_result = delete_result.expect("the delete must finish within the window");
    worker.join().expect("delete worker must not panic");

    // The op error keeps its original variant and message; the failure
    // sequence did NOT move, so the holder stays OPEN (normal backfill).
    let delete_err = delete_result.expect_err("the injected store failure must surface");
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
    drop(fixture);
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
    let completion = queued.completion.expect("missing commit confirmation");

    eprintln!("restore: entry received, waiting for body");
    // The body completed (enqueue pushed a scoped waiter) and drop(cold)
    // ran, but the view may stay Busy while the scoped waiter waits on the
    // held receipt. The restore intent must be blocked by the gate in this
    // window — that's the gate-blocking proof. We do NOT wait for view
    // reopen here; the receipt drop below terminates the holder.

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
    assert!(
        matches!(
            restore_pending,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "the restore intent must wait on the exclusive gate, got {restore_pending:?}"
    );

    // Drop the receipt: the delete fails terminal, the gate opens, and the
    // restore proceeds into apply_snapshot0 — reporting the holder failed.
    eprintln!("restore: dropping completion");
    drop(completion);
    let delete_done = delete_done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the delete must finish once the confirmation fails");
    worker.join().expect("delete worker must not panic");
    let delete_err = match delete_done {
        Ok(_) => panic!("the delete must fail on a dropped confirmation"),
        Err(err) => err,
    };
    assert!(fs.fs_dir.is_failed());

    let restore_result = restore_done_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the restore intent must return once the gate opens");
    restore_thread
        .join()
        .expect("restore thread must not panic");

    let restore_err = match restore_result {
        Ok(_) => panic!("the restore must not succeed on a FAILED holder"),
        Err(err) => err,
    };
    assert!(
        restore_err.to_string().contains("unavailable")
            || restore_err.to_string().contains("failed"),
        "the restore must report the terminal holder, got {restore_err}"
    );

    drop(fs);
    drop(loader);
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&meta_dir);
    let _ = std::fs::remove_dir_all(&journal_dir);
    if let Some(files) = &snapshot.files_data {
        let _ = std::fs::remove_dir_all(&files.dir);
    }
    Ok(())
}
