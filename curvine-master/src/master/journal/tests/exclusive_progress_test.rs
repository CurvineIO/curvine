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

    // Setup files first (original testing writer; entries drained in the
    // fixture constructor).
    let fs = fixture.fs.clone();
    let gate = fs.metadata_commit_gate();
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.create("/validation", false).unwrap();

    // Install the receipts writer BEFORE any fs/loader clone, so the
    // exclusive scope, the FsDir body and the loader all share one Arc.
    let writer = fixture.swap_in_receipts_writer(&conf);
    fixture.loader.journal_writer = writer.clone();
    let failure_seq_before = writer.failure_seq();

    // Start the real apply worker AFTER the writer identity is aligned, so
    // the loader's apply path uses the same receipts writer.
    let loader_for_worker = fixture.loader.clone();
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
    let delete_fs = fs.clone();
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
        match fs.fs_dir.view() {
            Ok(_) => {
                eprintln!("progress: view reopened");
                break None;
            }
            Err(NamespaceError::Busy) if std::time::Instant::now() < deadline => {
                eprintln!("progress: view busy, cold still dropping");
                std::thread::yield_now();
            }
            Err(e) => {
                eprintln!("progress: view terminal: {e}");
                break Some(e);
            }
        }
    };
    assert!(
        view_while_held.is_none(),
        "the view must reopen once the body completes, got {view_while_held:?}"
    );
    assert!(
        matches!(fs.file_status("/dir"), Err(e) if matches!(e, curvine_error::FsError::FileNotFound(_))),
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
            &fs.metadata_commit_gate()
        ),
        "the loader and the filesystem must share one gate"
    );

    // A second write RPC must stay blocked by the gate: create on a fresh
    // path fails nothing, it just cannot proceed. Started ack + bounded
    // pending is the evidence.
    let rpc_fs = fs.clone();
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
    assert!(fs.fs_dir.is_failed());
    assert!(
        matches!(fs.fs_dir.view().err(), Some(NamespaceError::Unavailable)),
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
    drop(fs);
    fixture.journal.shutdown();
    let _ = std::fs::remove_dir_all(&fixture.meta_dir);
    let _ = std::fs::remove_dir_all(&fixture.journal_dir);
}
