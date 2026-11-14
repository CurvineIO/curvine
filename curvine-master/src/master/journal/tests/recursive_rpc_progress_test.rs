// Copyright 2026 OPPO
//
// Licensed under the Apache License, Version 2.0.
//
//! C2 §9: real apply-worker progress while a recursive RPC waits for its
//! confirmation.
//!
//! A recursive delete runs through the wired recursive coordination entry
//! (`run_recursive_metadata_write` via the public delete) with a real
//! receipts writer; its journal entry is committed by the test driver and
//! the real apply worker advances the FSM watermark, the applied-request
//! record and the cursor **while the RPC is still waiting for its
//! receipt** — proving §8.2's "FAILED is not a drain" progress guarantee
//! on the actual C2 path (not raw `apply_entry`, not an early ack).
//!
//! Owned by the main developer (C2 whole-review batch).

use super::*;
use crate::master::journal::{JournalBatch, JournalSystem};
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_error::FsResult;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_raft::raft::RoleState;
use curvine_raft::utils::SerdeUtils;
use curvine_runtime::common::Utils;
use raft::eraftpb::Entry;
use std::sync::Arc;
use std::time::Duration;

const HANDSHAKE_WINDOW: Duration = Duration::from_secs(10);

struct ProgressFixture {
    conf: ClusterConf,
    journal: JournalSystem,
    fs: MasterFilesystem,
    loader: JournalLoader,
    runtime: Arc<tokio::runtime::Runtime>,
    meta_dir: std::path::PathBuf,
    journal_dir: std::path::PathBuf,
}

impl ProgressFixture {
    fn new(name: &str) -> Self {
        Master::init_test_metrics();
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            ..Default::default()
        };
        conf.change_test_meta_dir(format!("rpc-progress-{}-{}", name, Utils::rand_str(8)));
        conf.journal.snapshot_entries = 0;
        conf.journal.writer_channel_size = 0;
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.master_monitor.set_role_for_test(RoleState::Leader);
        let loader = journal.journal_loader();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let meta_dir = std::path::PathBuf::from(conf.master.meta_dir.clone());
        let journal_dir = std::path::PathBuf::from(conf.journal.journal_dir.clone());
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
fn apply_worker_advances_fsm_req_cursor_while_recursive_rpc_waits_for_its_receipt() -> FsResult<()>
{
    let mut fixture = ProgressFixture::new("rpc-progress");
    let conf = fixture.conf.clone();

    // Phase 1: setup with the original testing writer, then drain the
    // retained setup entries so the receipt observed later is the
    // recursive delete's own.
    let setup_fs = fixture.fs.clone();
    setup_fs.mkdir("/tree", true).unwrap();
    setup_fs.create("/tree/leaf", false).unwrap();
    setup_fs
        .journal_writer
        .with_testing_receiver(|receiver| loop {
            match receiver.try_recv() {
                Ok(entry) => drop(entry),
                Err(_) => break,
            }
        });

    // Phase 2: install the real receipts writer (require_raft_commit=true)
    // aligned on FsDir and the filesystem; verified with a short view
    // before anything spawns.
    let writer = {
        let rt = conf.journal.create_runtime();
        let client = curvine_raft::raft::RaftClient::from_conf(rt, &conf.journal);
        let recovery = journal_master_recovery(&fixture);
        let mut writer = JournalWriter::new(true, client, &conf.journal, recovery.clone()).unwrap();
        writer.enable_testing_receipts(recovery);
        let writer = Arc::new(writer);
        fixture
            .fs
            .fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .journal_writer = writer.clone();
        fixture.fs.journal_writer = writer.clone();
        writer
    };
    let failure_seq_before = writer.failure_seq();

    // Pre-op opaque cursor handle: the point lookup must be healthy before
    // the operation.
    let pre_cursor = fixture.fs.fs_dir.view().unwrap().store.cursor_store();
    let pre_cursor_probe = pre_cursor.get_applied_req(77);
    assert!(
        matches!(&pre_cursor_probe, Ok(None)),
        "the live cursor must answer point lookups before the operation, got {pre_cursor_probe:?}"
    );

    // Phase 3/4: prove the C2 bounded subtree-lock path is taken (not the
    // C1 cold bridge) and run the real recursive delete. The plan_pause
    // hook fires after capture and BEFORE lock_many — a cold bridge would
    // never reach this point. The driver arms the hook before the worker
    // starts, waits for the reached signal with a bounded timeout, then
    // releases the pause so the coordination entry continues.
    let (pause_reached_tx, pause_reached_rx) = std::sync::mpsc::channel();
    let (pause_release_tx, pause_release_rx) = std::sync::mpsc::channel();
    fixture
        .fs
        .journal_writer
        .arm_plan_pause_hook(pause_reached_tx, pause_release_rx);

    // G6: arm the non-panic wait-arrival signal BEFORE the worker starts —
    // the next real non-empty JournalCommitScope::wait fires it. The driver
    // waits for this (instead of inferring wait entry from path deletion)
    // to prove the body returned and the RPC truly entered its confirmation
    // wait while the receipt is still unacked.
    let (wait_reached_tx, wait_reached_rx) = std::sync::mpsc::channel();
    fixture
        .fs
        .journal_writer
        .arm_wait_reached_hook(wait_reached_tx);

    let worker_fs = fixture.fs.clone();
    let worker = std::thread::spawn(move || worker_fs.delete("/tree", true));

    // The plan pause must fire within the window: this proves the C2
    // bounded subtree-lock path (not the C1 cold bridge) was taken.
    pause_reached_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the recursive delete must reach the plan pause (C2 path taken)");
    // Release the pause so the coordination entry continues to lock_many.
    pause_release_tx
        .send(())
        .expect("the plan pause must be releasable");

    // The body has published: the live tree no longer contains /tree
    // (this is the C2 locked-path completion signal, not a tree-still-
    // exists assertion).
    let live_deleted = (0..200)
        .find(|_| fixture.fs.file_status("/tree").is_err())
        .is_some();
    assert!(
        live_deleted,
        "the recursive delete must remove /tree in the live namespace"
    );
    assert!(
        fixture.fs.file_status("/tree").is_err(),
        "the live namespace must still serve reads while the confirmation waits"
    );
    assert_eq!(
        writer.failure_seq(),
        failure_seq_before,
        "the delete must still be waiting for its confirmation"
    );

    // G6: the REAL non-empty confirmation wait has been entered — this is
    // the actual wait event, not an inference from the path disappearing.
    wait_reached_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the recursive delete must enter the real non-empty commit wait");

    // Phase 5: real commit — the driver registers the delete as committed
    // and drives the REAL apply worker through the loader's public apply
    // entry. The FSM watermark, the applied-request record and the cursor
    // advance while the RPC is still waiting.
    // Take the queued entry but KEEP its completion alive: dropping the
    // completion would fail the worker's scope.wait (a real commit error)
    // before the Scan runs. The entry data is cloned for the committed
    // log append; the completion stays with `queued` until the test ends.
    let queued = fixture
        .fs
        .journal_writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(HANDSHAKE_WINDOW)
                .expect("missing the recursive delete journal entry")
        })
        .expect("the receipts receiver must exist");
    let op_entry = queued.entry.clone();
    let entry = committed_entry(op_entry, 1);
    let _keep_completion_alive = queued;
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

    eprintln!(
        "RPCPROG diag: before scan, holder failed={}, fatal_source={:?}",
        fixture.fs.fs_dir.is_failed(),
        journal_master_recovery(&fixture).fatal_source()
    );
    let scan_loader = fixture.loader.clone();
    fixture.runtime.block_on(async {
        tokio::time::timeout(
            HANDSHAKE_WINDOW,
            scan_loader.apply(true, ApplyMsg::new_scan(AppliedIndex::default())),
        )
        .await
        .expect("real Scan must complete within the window")
        .expect("the real Scan must succeed while the receipt is held");
    });

    // The FSM applied watermark and the request record both advanced while
    // the receipt is still held by the waiting RPC.
    eprintln!(
        "RPCPROG diag: holder failed={} view_err={:?}",
        fixture.fs.fs_dir.is_failed(),
        fixture.fs.fs_dir.view().err().map(|e| e.to_string())
    );
    let applied = fixture.loader.fsm_state_snapshot().unwrap().applied;
    assert_eq!(
        applied.index, 1,
        "the FSM applied index must advance while the RPC waits"
    );
    assert_eq!(applied.term, 1);
    let cursor_req = fixture
        .loader
        .cursor_store
        .get_applied_req(1)
        .expect("the cursor store must stay readable while the receipt is held")
        .expect("the applied request must be recorded");
    assert_eq!(cursor_req.index, 1);
    assert_eq!(cursor_req.term, 1);
    eprintln!(
        "rpc-progress: apply worker advanced FSM/req/cursor while the recursive RPC still waits"
    );

    // G6: real successful confirmation — send the ack (not drop).
    // The RPC worker's scope.wait resolves Ok, the coordination entry
    // returns Ok, and the holder stays usable.
    _keep_completion_alive
        .completion
        .as_ref()
        .expect("the receipt must carry a completion sender")
        .send(())
        .expect("the successful ack must be delivered");
    let worker_result = worker
        .join()
        .expect("the RPC worker must finish after the real ack");
    assert!(
        worker_result.is_ok(),
        "the recursive delete must succeed with the real confirmation: {:?}",
        worker_result.as_ref().err().map(|e| e.to_string())
    );
    // The holder must stay usable (no seq move, no retirement).
    assert_eq!(
        fixture.fs.journal_writer.failure_seq(),
        failure_seq_before,
        "the failure_seq must not move after the real ack"
    );
    assert!(
        !fixture.fs.fs_dir.is_failed(),
        "the holder must stay usable after the real ack"
    );
    assert!(
        fixture.fs.file_status("/tree").is_err(),
        "the tree must still be deleted after the successful confirmation"
    );
    let _ = pre_cursor;
    let _ = failure_seq_before;

    drop(fixture);
    Ok(())
}

fn journal_master_recovery(fixture: &ProgressFixture) -> crate::master::journal::RecoveryGate {
    fixture.journal.master_monitor().recovery()
}

// Keep the CallChannel import used by the fixture's runtime contract in
// scope for future phases of this file.
#[allow(unused_imports)]
use curvine_runtime::sync::channel::CallChannel;
