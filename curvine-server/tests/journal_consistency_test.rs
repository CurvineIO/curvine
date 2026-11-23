// Copyright 2026 OPPO.
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

//! Journal/raft consistency tests around the metadata rebuild rule.
//!
//! The local metadata RocksDB is a derived store, not a source of truth. A
//! node without a snapshot must discard it and rebuild from the journal; a
//! journal compacted below index 1 without a snapshot must fail closed. These
//! tests drive the same entry points the server uses:
//! - `JournalSystem` in testing mode keeps journal entries in memory, so a test
//!   can decide exactly which entries reach the raft log.
//! - `RaftNode::install_snapshot` is the function both startup and a leader
//!   step-down call, so the restart/demotion behavior here is production code.

use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_raft::conf::JournalConfExt;
use curvine_raft::proto::raft::AppliedIndex;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg, LogStorage, PeerStorage, RocksLogStorage};
use curvine_raft::raft::{RaftClient, RaftNode, RoleState};
use curvine_runtime::common::{SerdeUtils, Utils};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::master::journal::{JournalBatch, JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::Master;
use raft::eraftpb::{ConfState, Entry};
use raft::Storage;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

/// Master file counts live in a process-global metrics registry that every
/// `JournalSystem` resets. Serialize the cases that build more than one master.
static METRICS_SENSITIVE_TESTS: Mutex<()> = Mutex::new(());

fn serialize_metrics_sensitive_test() -> MutexGuard<'static, ()> {
    METRICS_SENSITIVE_TESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn test_conf(name: &str) -> ClusterConf {
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("{}-{}", name, Utils::rand_str(6)));
    conf
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = payload.downcast_ref::<String>() {
        return msg.clone();
    }
    if let Some(msg) = payload.downcast_ref::<&str>() {
        return (*msg).to_string();
    }
    String::new()
}

/// The previous incarnation's background tasks can hold the RocksDB LOCK for a
/// moment after `shutdown`, and `RocksStorageCore::new` unwraps that open, so a
/// reopen retries on both the error and the panic form (same as
/// `journal_test::reopen_journal_system`).
fn reopen(conf: &ClusterConf) -> CommonResult<JournalSystem> {
    for _ in 0..50 {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            JournalSystem::from_conf(conf)
        })) {
            Ok(Ok(js)) => return Ok(js),
            Ok(Err(e)) if e.to_string().contains("lock hold by current process") => {
                thread::sleep(Duration::from_millis(100));
            }
            Ok(Err(e)) => return Err(e.into()),
            Err(panic) if panic_message(&panic).contains("lock hold by current process") => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    JournalSystem::from_conf(conf).map_err(Into::into)
}

fn ledger_entry(index: u64, term: u64, journal: JournalEntry) -> CommonResult<Entry> {
    let mut batch = JournalBatch::new(index);
    batch.push(journal);
    Ok(Entry {
        index,
        term,
        data: SerdeUtils::serialize(&batch)?,
        ..Default::default()
    })
}

/// Runs the exact function startup and step-down use. A missing snapshot takes
/// the "discard the local store and replay the journal" path.
fn install_snapshot(conf: &ClusterConf, js: &JournalSystem, voters: Vec<u64>) -> CommonResult<u64> {
    let rt = conf.journal.create_runtime();
    let id = conf.journal.node_id()?;
    let client = RaftClient::from_conf(rt.clone(), &conf.journal);
    let storage = PeerStorage::new(
        rt.clone(),
        id,
        js.log_store(),
        js.journal_loader(),
        client,
        &conf.journal,
    );
    let applied = rt
        .block_on(RaftNode::<RocksLogStorage, JournalLoader>::install_snapshot(&storage, voters))?;
    Ok(applied)
}

/// A restart without a snapshot must discard the local metadata store, even
/// though the apply watermark is not persisted (`applied` starts at 0).
#[test]
fn restart_without_snapshot_discards_the_local_metadata_store() -> CommonResult<()> {
    let _serial = serialize_metrics_sensitive_test();
    Master::init_test_metrics();

    let mut conf = test_conf("rebuild-restart");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);

    fs.mkdir("/d", true)?;
    fs.create("/d/a", true)?;
    fs.link("/d/a", "/d/b")?;

    let journal_entries: Vec<JournalEntry> = fs.fs_dir.read().take_entries();
    assert_eq!(journal_entries.len(), 3, "mkdir + create + link");
    for (offset, journal) in journal_entries.into_iter().enumerate() {
        js.append_committed_entry_for_test(ledger_entry(offset as u64 + 1, 1, journal)?)?;
    }

    // Leader-first write whose proposal is lost: the handler already mutated
    // RocksDB, the entry never becomes durable. A rebuild must discard it.
    fs.create("/ghost", true)?;
    let lost: Vec<JournalEntry> = fs.fs_dir.read().take_entries();
    assert_eq!(lost.len(), 1);
    drop(lost);

    drop(fs);
    js.shutdown();

    conf.format_master = false;
    let js = reopen(&conf)?;

    // The committed log is durable; the apply watermark is not.
    assert_eq!(
        js.journal_loader().get_fsm_state().applied.index,
        0,
        "the apply watermark is reconstructed, not persisted"
    );
    assert_eq!(js.log_store().hard_state().commit, 3);

    assert_eq!(install_snapshot(&conf, &js, vec![1])?, 3);

    let fs = js.fs();
    assert!(fs.exists("/d/a")?, "the journal rebuilds committed state");
    assert!(fs.exists("/d/b")?);
    assert!(!fs.exists("/ghost")?, "the lost write must not be served");
    assert_eq!(fs.file_status("/d/a")?.nlink, 2);

    // The rebuild replaced the RocksDB contents, not just the in-memory tree.
    fs.restore_from_rocksdb()?;
    assert!(
        !fs.exists("/ghost")?,
        "the derived store must be discarded, not reloaded"
    );

    drop(fs);
    js.shutdown();
    Ok(())
}

/// A never-committed write must not survive a restart and must not be exported
/// by a snapshot taken afterwards.
#[test]
fn uncommitted_metadata_is_not_propagated_through_a_rebuilt_snapshot() -> CommonResult<()> {
    let _serial = serialize_metrics_sensitive_test();
    Master::init_test_metrics();

    let mut conf = test_conf("rebuild-ghost");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);

    fs.create("/real", true)?;
    let real_entry = fs
        .fs_dir
        .read()
        .take_entries()
        .pop()
        .expect("create must emit one journal entry");
    js.append_committed_entry_for_test(ledger_entry(1, 1, real_entry)?)?;

    fs.create("/ghost", true)?;
    let lost: Vec<JournalEntry> = fs.fs_dir.read().take_entries();
    assert_eq!(lost.len(), 1);
    drop(lost);

    drop(fs);
    js.shutdown();

    conf.format_master = false;
    let js = reopen(&conf)?;
    assert_eq!(install_snapshot(&conf, &js, vec![1])?, 1);

    let fs = js.fs();
    assert!(fs.exists("/real")?);
    assert!(!fs.exists("/ghost")?, "the lost write must not survive");
    fs.restore_from_rocksdb()?;
    assert!(
        !fs.exists("/ghost")?,
        "the rebuilt store must not retain the lost write"
    );

    // A snapshot taken now carries only the rebuilt state.
    let snapshot = AsyncRuntime::single().block_on(js.journal_loader().create_snapshot())?;
    assert_eq!(snapshot.fsm_state.applied.index, 1);

    let other = test_conf("rebuild-ghost-target");
    let target = JournalSystem::from_conf(&other)?;
    let target_fs = target.fs();
    AsyncRuntime::single().block_on(target.journal_loader().apply_snapshot(snapshot))?;

    assert!(target_fs.exists("/real")?);
    assert!(
        !target_fs.exists("/ghost")?,
        "a rebuilt snapshot must not export a never-committed write"
    );

    drop(fs);
    drop(target_fs);
    js.shutdown();
    target.shutdown();
    Ok(())
}

/// A snapshot is still the trusted base: demotion restores it and rolls back
/// the uncommitted write.
#[test]
fn step_down_reinstall_rolls_back_uncommitted_state_with_a_snapshot() -> CommonResult<()> {
    let _serial = serialize_metrics_sensitive_test();
    Master::init_test_metrics();

    let conf = test_conf("rebuild-step-down");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);

    fs.create("/committed", true)?;
    let entry = fs
        .fs_dir
        .read()
        .take_entries()
        .pop()
        .expect("create must emit one journal entry");
    js.append_committed_entry_for_test(ledger_entry(1, 1, entry)?)?;

    let rt = AsyncRuntime::single();
    rt.block_on(
        js.journal_loader()
            .apply(true, ApplyMsg::new_scan(AppliedIndex::default())),
    )?;
    assert_eq!(js.journal_loader().get_fsm_state().applied.index, 1);
    fs.master_monitor.set_term_for_test(1);

    let snapshot = rt.block_on(js.journal_loader().create_snapshot())?;
    js.log_store().create_snapshot(snapshot)?;

    fs.create("/ghost", true)?;
    let lost: Vec<JournalEntry> = fs.fs_dir.read().take_entries();
    assert_eq!(lost.len(), 1);
    drop(lost);

    assert_eq!(install_snapshot(&conf, &js, vec![1])?, 1);

    let fs = js.fs();
    assert!(fs.exists("/committed")?);
    assert!(
        !fs.exists("/ghost")?,
        "with a snapshot the reinstall removes the uncommitted write"
    );

    drop(fs);
    js.shutdown();
    Ok(())
}

/// A live member reinstalled without a snapshot must discard its local store
/// even when its apply watermark is already nonzero; the watermark only counts
/// locally applied entries, not trusted committed state.
#[test]
fn live_no_snapshot_reinstall_with_nonzero_watermark_discards_local_store() -> CommonResult<()> {
    let _serial = serialize_metrics_sensitive_test();
    Master::init_test_metrics();

    let conf = test_conf("rebuild-live-no-snapshot");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);

    fs.create("/committed", true)?;
    let entry = fs
        .fs_dir
        .read()
        .take_entries()
        .pop()
        .expect("create must emit one journal entry");
    js.append_committed_entry_for_test(ledger_entry(1, 1, entry)?)?;

    // Apply the committed entry so the watermark is nonzero without any
    // snapshot being registered.
    AsyncRuntime::single().block_on(
        js.journal_loader()
            .apply(true, ApplyMsg::new_scan(AppliedIndex::default())),
    )?;
    assert_eq!(js.journal_loader().get_fsm_state().applied.index, 1);
    fs.master_monitor.set_term_for_test(1);

    fs.create("/ghost", true)?;
    let lost: Vec<JournalEntry> = fs.fs_dir.read().take_entries();
    assert_eq!(lost.len(), 1);
    drop(lost);
    assert!(fs.exists("/ghost")?);

    // No snapshot is registered, so this takes the discard-and-replay branch
    // even though the live watermark is nonzero.
    assert_eq!(install_snapshot(&conf, &js, vec![1])?, 1);

    let fs = js.fs();
    assert!(fs.exists("/committed")?);
    assert!(
        !fs.exists("/ghost")?,
        "the uncommitted write must be discarded"
    );
    fs.restore_from_rocksdb()?;
    assert!(
        !fs.exists("/ghost")?,
        "the derived store must be discarded, not reloaded"
    );

    drop(fs);
    js.shutdown();
    Ok(())
}

/// P1: restart still takes the static `journal_addrs` as authoritative
/// membership and overwrites the membership persisted in the raft snapshot.
/// This is a separate defect from the metadata rebuild; the assertion records
/// the current behavior.
#[test]
fn startup_still_overwrites_persisted_membership_from_static_config() -> CommonResult<()> {
    let _serial = serialize_metrics_sensitive_test();
    Master::init_test_metrics();

    let conf = test_conf("rebuild-conf-state");
    let js = JournalSystem::from_conf(&conf)?;
    let id = conf.journal.node_id()?;

    let committed_voters = vec![id, 2, 3];
    js.log_store().set_conf_state(&ConfState {
        voters: committed_voters.clone(),
        ..Default::default()
    })?;

    let snapshot = AsyncRuntime::single().block_on(js.journal_loader().create_snapshot())?;
    js.log_store().create_snapshot(snapshot)?;

    let before = js.log_store().initial_state()?.conf_state.voters;
    assert_eq!(
        before, committed_voters,
        "the persisted membership is the committed one"
    );

    install_snapshot(&conf, &js, vec![id])?;

    let after = js.log_store().initial_state()?.conf_state.voters;
    println!("membership after restart: {after:?}");
    assert_eq!(
        after,
        vec![id],
        "the static config replaced the committed membership"
    );

    js.shutdown();
    Ok(())
}

/// A compacted journal without a snapshot cannot be rebuilt locally. The node
/// must fail closed instead of replaying a prefix or trusting the local DB.
#[test]
fn compacted_journal_without_snapshot_fails_closed() -> CommonResult<()> {
    let _serial = serialize_metrics_sensitive_test();
    Master::init_test_metrics();

    let mut conf = test_conf("rebuild-compacted");
    // Shrink the compaction retention window to (retain x entries) = 1.
    conf.journal.snapshot_entries = 1;
    conf.journal.retain_checkpoint_num = 1;

    let js = JournalSystem::from_conf(&conf)?;
    for index in 1..=5 {
        js.append_committed_entry_for_test(Entry {
            index,
            term: 1,
            ..Default::default()
        })?;
    }
    js.log_store().compact(3)?;
    assert!(
        js.log_store().first_index()? > 1,
        "setup must trim the journal prefix"
    );

    let err = install_snapshot(&conf, &js, vec![1])
        .expect_err("a compacted journal without a snapshot must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("compacted") && msg.contains("no snapshot"),
        "unexpected error: {msg}"
    );

    js.shutdown();
    Ok(())
}
