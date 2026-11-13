// Owned by deputy-2 (C2 §8.1–8.2 confirmation contract tests): shared
// fixtures only. Registration anchors (main developer) live in the
// master_filesystem.rs cfg(test) declaration block:
//   #[path = "tests/recursive_confirmation_support.rs"] mod recursive_confirmation_support;
//   #[path = "tests/recursive_confirmation_test.rs"] mod recursive_confirmation_test;
//   #[path = "tests/recursive_confirmation_lifecycle_test.rs"] mod recursive_confirmation_lifecycle_test;

use super::*;
use crate::master::journal::RecoveryGate;
use crate::master::meta::fs_dir_handle::NamespaceGuard;
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::{ColdError, NamespaceError};
use crate::master::Master;
use curvine_raft::raft::RaftClient;
use curvine_raft::raft::RoleState;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

pub(super) const WORKER_TIMEOUT: Duration = Duration::from_secs(20);
/// A waiter that has already called the blocking lock acquisition cannot
/// succeed while the failing body still holds the stripe; a short probe keeps
/// the claim bounded without pretending to observe the OS queue.
pub(super) const LOCK_HELD_PROBE: Duration = Duration::from_millis(200);

pub(super) fn test_conf(name: &str) -> ClusterConf {
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "{}-{}",
        name,
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf
}

pub(super) fn build_writer(conf: &ClusterConf) -> FsResult<JournalWriter> {
    Master::init_test_metrics();
    let rt = conf.journal.create_runtime();
    let client = RaftClient::from_conf(rt, &conf.journal);
    JournalWriter::new(true, client, &conf.journal, RecoveryGate::new())
}

/// Enabled journal with a truly unbounded testing queue: §8.3 admits the
/// recursive coordination entry, so every test below exercises the real
/// sharded path instead of the exclusive fallback.
pub(super) fn confirmation_env(name: &str) -> (ClusterConf, JournalSystem, MasterFilesystem) {
    let mut conf = test_conf(name);
    conf.journal.writer_channel_size = 0;
    conf.journal.snapshot_entries = 0;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    (conf, journal, fs)
}

/// Real receipts writer shared by the body's FsDir and the final fs clone.
/// Clones must be taken after this swap (MasterFilesystem clones carry their
/// own journal_writer Arc).
pub(super) fn swap_receipts_writer(
    fs: &mut MasterFilesystem,
    journal: &JournalSystem,
    conf: &ClusterConf,
) -> Arc<JournalWriter> {
    let mut writer = build_writer(conf).unwrap();
    writer.enable_testing_receipts(journal.master_monitor().recovery());
    let writer = Arc::new(writer);
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .journal_writer = writer.clone();
    fs.journal_writer = writer.clone();
    writer
}

pub(super) fn drain_receipts(writer: &Arc<JournalWriter>) {
    writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });
}

/// One real public admission — commit-gate slot, original epoch, commit
/// scope — wrapped around the sharded coordination entry. The exclusive cold
/// bridge must never run here: a Fallback means the test stopped reaching the
/// contract under test.
pub(super) fn run_admitted<R>(
    fs: &MasterFilesystem,
    path: &str,
    mode: InodeLockMode,
    need_locations: bool,
    body: impl FnOnce(&FsDir) -> FsResult<R>,
) -> FsResult<R> {
    fs.run_metadata_write_with_fallback(
        || {
            // One original epoch for the whole admission, captured before the
            // recursive entry takes any locks (R8 epoch contract).
            let epoch = fs.journal_writer.metadata_epoch()?;
            fs.run_recursive_metadata_write(epoch, path, mode, need_locations, body)
        },
        |_cold| panic!("the confirmation contract tests must stay on the sharded path"),
    )
}

/// Bounded result collection: a timeout fails the test instead of joining a
/// hung worker; a disconnected channel means the worker already ended, so the
/// join returns immediately (the caller does that after this returns).
pub(super) fn await_worker<T>(done: &Receiver<T>, what: &str) -> Result<T, RecvTimeoutError> {
    match done.recv_timeout(WORKER_TIMEOUT) {
        Ok(value) => Ok(value),
        Err(RecvTimeoutError::Timeout) => {
            panic!("{what} did not finish within {WORKER_TIMEOUT:?}; refusing an unbounded join")
        }
        Err(RecvTimeoutError::Disconnected) => Err(RecvTimeoutError::Disconnected),
    }
}

/// The expected rejection of a call whose return type differs per probe
/// (`NamespaceError` for views, `ColdError` for cold access).
pub(super) fn rejected<T, E>(result: Result<T, E>, context: &'static str) -> E {
    result.err().expect(context)
}

pub(super) fn assert_holder_terminal(
    fs: &MasterFilesystem,
    writer: &Arc<JournalWriter>,
    seq_before: u64,
) {
    assert!(fs.fs_dir.is_failed(), "the holder must be terminal");
    assert!(
        matches!(
            rejected(fs.fs_dir.view(), "the holder must reject new views"),
            NamespaceError::Unavailable
        ),
        "new views must be rejected after the holder went terminal"
    );
    assert!(
        matches!(
            rejected(
                fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT),
                "the terminal holder must reject begin_cold"
            ),
            ColdError::Unavailable
        ),
        "cold exclusive access must be rejected after the holder went terminal"
    );
    assert!(
        writer.ensure_metadata_current().is_err(),
        "journal writes must be fenced after the holder went terminal"
    );
    assert_ne!(
        writer.failure_seq(),
        seq_before,
        "a terminal confirmation must move the failure seq"
    );
}

/// A pre-failure view stays a legal, droppable reference (FAILED is not a
/// drain), while the terminated store rejects every new read.
pub(super) fn terminal_holder_alive_probe(pre_view: &NamespaceGuard<'_>) {
    let rejected = MasterFilesystem::resolve_path(pre_view, "/dir");
    assert!(
        rejected.is_err(),
        "reads through the terminal store must be rejected, not silently served"
    );
}

/// Fail the body with the original typed error when the leaf is missing:
/// `InodePath::resolve` returns a partial-chain Ok for missing leaves, so
/// the missing case is detected explicitly (§9: uncommitted store errors).
pub(super) fn typed_missing_leaf(fs_dir: &FsDir, path: &str) -> FsResult<()> {
    let inp = MasterFilesystem::resolve_path(fs_dir, path)?;
    if inp.get_last_inode().is_none() {
        return Err(FsError::file_not_found(path));
    }
    Ok(())
}

/// `ShardedOutcome` has no Debug impl, so `Result::unwrap_err` cannot be
/// used on `FsResult<ShardedOutcome<()>>`; extract the typed error directly.
pub(super) fn typed_op_error<R>(result: FsResult<R>, context: &'static str) -> FsError {
    match result {
        Err(op_err) => op_err,
        Ok(_) => panic!("{context}"),
    }
}

pub(super) fn unwind_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message.to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Case teardown: every case-owned view/cursor/writer/runtime reference is
/// dropped by the caller before this runs; remove failures are real and are
/// propagated instead of swallowed.
pub(super) fn remove_case_dirs(conf: &ClusterConf) {
    std::fs::remove_dir_all(&conf.master.meta_dir).expect("the case meta dir must be removable");
    std::fs::remove_dir_all(&conf.journal.journal_dir)
        .expect("the case journal dir must be removable");
}

pub(super) fn shutdown_case(journal: JournalSystem) {
    let ack = journal.shutdown();
    drop(ack);
}

/// A reader that reports before and after the blocking object-lock
/// acquisition. The driver proves the waiter could not acquire while the
/// failing body held the stripe, then reuses the post-lock observation.
pub(super) fn spawn_lock_waiter(
    fs: MasterFilesystem,
    inode_id: i64,
    queued: Sender<()>,
    acquired: Sender<(bool, Option<NamespaceError>)>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        queued
            .send(())
            .expect("the driver must observe the lock attempt");
        let _lock = fs.inode_locks.write_inode(inode_id);
        acquired
            .send((fs.fs_dir.is_failed(), fs.fs_dir.view().err()))
            .expect("the driver must observe the post-lock state");
    })
}

/// Arming the real publication-failure boundary: the
/// `master.metadata.recursive.publish_after_commit` fault point fires inside
/// `publish_metadata_impl` after the durable store commit and before the live
/// publication, so the body observes a genuine durable-but-unpublished error
/// through the real fatal entry (no setter). The point declares no matcher
/// keys, so the rule is one-shot (`times(1)`) and must only be armed by the
/// case that is about to publish its own recursive mutation.
/// Arming the real publication-failure boundary: the
/// `master.metadata.recursive.publish_after_commit` fault point fires inside
/// `unprotected_delete` after the store batch committed (durable) and before
/// the first live publish, so the body observes a genuine
/// durable-but-unpublished error through the real fatal entry (no setter).
/// The `child_name` context key matches the top name of the deleted subtree.
pub(super) fn arm_publication_failure(
    rule_id: &str,
    child_name: &str,
) -> &'static curvine_fault::FaultRuntime {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    let runtime = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.recursive.publish_after_commit")
        .matches("child_name", child_name)
        .unwrap()
        .return_error(format!("recursive publication failure: {child_name}"))
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    runtime
}

/// Removing the armed publication-failure rule so later cases keep running
/// with a healthy publication path.
pub(super) fn disarm_publication_failure(runtime: &curvine_fault::FaultRuntime, rule_id: &str) {
    runtime.remove(rule_id).unwrap();
}
