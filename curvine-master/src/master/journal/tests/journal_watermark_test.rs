// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

                                                              //
                                                              //     http://www.apache.org/licenses/LICENSE-2.0
                                                              //
                                                              // Unless required by applicable law or agreed to in writing, software
                                                              // distributed under the License is distributed on an "AS IS" BASIS,
                                                              // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
                                                              // See the License for the specific language governing permissions and
                                                              // limitations under the License.

use crate::master::journal::{
    CacheInvalidationEntry, JournalBatch, JournalEntry, JournalSystem, UfsAppliedEntry,
};
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_raft::utils::SerdeUtils;
use raft::eraftpb::Entry;

#[cfg(feature = "fault-injection")]
#[test]
fn cursor_write_failure_does_not_advance_applied() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "cursor-write-failure-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(loader.apply(true, ApplyMsg::new_entry(entry(1, &[]))))
        .unwrap();
    let faults = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.journal.cursor.before_write")
        .times(1)
        .unwrap()
        .return_error("injected cursor failure")
        .unwrap();
    faults.configure("cursor-write-failure", rule).unwrap();
    let result = runtime.block_on(loader.apply(true, ApplyMsg::new_entry(entry(2, &[]))));
    faults.remove("cursor-write-failure").unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected cursor failure"));
    assert_eq!(loader.get_fsm_state().applied.index, 1);
    assert!(journal.master_monitor().is_safe_mode());
    journal.shutdown();
}

#[cfg(feature = "fault-injection")]
#[test]
fn deferred_ufs_operations_reject_failed_restore() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "deferred-restore-failure-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();
    let faults = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.restore.after_store_detach")
        .matches("data_dir", conf.db_conf().data_dir.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected restore failure")
        .unwrap();
    faults.configure("deferred-restore-failure", rule).unwrap();
    let result = fs
        .fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0);
    faults.remove("deferred-restore-failure").unwrap();
    assert!(result.is_err());
    assert!(loader.deferred_ufs_records().is_err());
    assert!(loader.put_deferred_ufs(1, b"payload").is_err());
    assert!(loader.delete_deferred_ufs(1).is_err());
    journal.shutdown();
}

// Isolate the cross-column-family durability window independently of Raft
// networking. The test intentionally flushes only the cursor column family.
#[test]
fn cursor_flush_cannot_hide_a_committed_inode_after_restart() {
    Master::init_test_metrics();
    let child = std::env::var("CURVINE_CURSOR_CF_PROBE").ok();
    let base = child.clone().unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!(
                "curvine-cursor-cf-{}-{}",
                std::process::id(),
                curvine_runtime::common::Utils::rand_str(8)
            ))
            .to_str()
            .unwrap()
            .to_string()
    });
    let mut conf = ClusterConf {
        testing: true,
        format_master: false,
        ..Default::default()
    };
    conf.master.meta_dir = format!("{base}/meta");
    conf.journal.journal_dir = format!("{base}/journal");
    conf.journal.enable = true;
    if child.is_some() {
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.mkdir("/committed", false).unwrap();
        let mut batch = JournalBatch::new(1);
        for item in fs.fs_dir.view().unwrap().take_entries() {
            batch.push(item);
        }
        assert!(!batch.is_empty());
        journal
            .append_committed_entry_for_test(Entry {
                index: 1,
                term: 1,
                data: batch.encode().unwrap(),
                ..Default::default()
            })
            .unwrap();
        let loader = journal.journal_loader();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(loader.apply(
                true,
                ApplyMsg::new_entry(Entry {
                    index: 1,
                    term: 1,
                    ..Default::default()
                }),
            ))
            .unwrap();
        let dir = fs.fs_dir.view().unwrap();
        let db = dir.store_ref().store().db.db();
        let cf = db
            .cf_handle(crate::master::meta::store::RocksInodeStore::CF_COMMON)
            .unwrap();
        db.flush_cf(&cf).unwrap();
        std::process::exit(0);
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "master::journal::journal_loader::journal_watermark_test::cursor_flush_cannot_hide_a_committed_inode_after_restart"])
        .env("CURVINE_CURSOR_CF_PROBE", &base).status().unwrap();
    assert!(status.success());
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    // The no-snapshot rebuild resets the cursor; the committed state returns
    // with the log scan.
    let applied = loader.get_fsm_state().applied;
    assert_eq!(applied.index, 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(loader.apply(true, ApplyMsg::new_scan(applied)))
        .unwrap();
    assert_eq!(loader.get_fsm_state().applied.index, 1);
    assert!(
        journal.fs().exists("/committed").unwrap(),
        "committed inode rows survived the rebuild and replay"
    );
    drop(loader);
    journal.shutdown();
    std::fs::remove_dir_all(base).unwrap();
}

fn entry(index: u64, ids: &[u64]) -> Entry {
    let mut batch = JournalBatch::new(index);
    for &op_id in ids {
        batch.push(JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id,
            rpc_id: 0,
            inodes: vec![],
        }));
    }
    Entry {
        index,
        term: 1,
        data: batch.encode().unwrap(),
        ..Default::default()
    }
}

#[test]
fn corrupt_journal_batch_is_classified_as_metadata_damage() {
    use crate::master::journal::recovery::{classify_apply_error, ApplyDisposition};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "corrupt-journal-batch-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let corrupt = Entry {
        index: 1,
        term: 1,
        data: vec![0xff],
        ..Default::default()
    };
    let error = runtime
        .block_on(loader.apply0(false, &corrupt, false))
        .unwrap_err();
    let message = error.to_string();
    // Strip the test thread name, which itself contains "corrupt" and would
    // otherwise accidentally satisfy the string-based damage classifier.
    let message = &message[message.find("failed to decode journal batch").unwrap()..];
    assert_eq!(
        classify_apply_error(message),
        ApplyDisposition::EnterSafeMode
    );
    assert_eq!(loader.get_fsm_state().applied.index, 0);
    journal.shutdown();
}

#[test]
fn leader_duplicate_entry_does_not_rewind_metadata_cursor_when_ufs_lags() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "leader-duplicate-watermark-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for index in [1, 2, 2, 1] {
        runtime
            .block_on(loader.apply0(true, &entry(index, &[index]), false))
            .unwrap();
    }
    let state = loader.get_fsm_state();
    assert_eq!(state.ufs_applied.index, 0);
    assert_eq!(state.applied.index, 2);
    assert_eq!(state.applied.op_id, 2);
    let bytes = journal
        .fs()
        .fs_dir
        .view()
        .unwrap()
        .load_journal_applied()
        .unwrap()
        .unwrap();
    let persisted =
        <curvine_raft::proto::raft::AppliedIndex as prost::Message>::decode(bytes.as_slice())
            .unwrap();
    assert_eq!(persisted.index, 2);
    journal.shutdown();
}

#[test]
fn stale_concurrent_cursor_commit_fences_without_rewinding() {
    use curvine_raft::proto::raft::AppliedIndex;

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "concurrent-cursor-watermark-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let cursor = |index| AppliedIndex {
        index,
        term: 1,
        ..Default::default()
    };
    loader.set_applied(false, cursor(2), false).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut conflicting_entry = entry(2, &[]);
    conflicting_entry.term = 2;
    assert!(runtime
        .block_on(loader.apply0(false, &conflicting_entry, false))
        .is_err());
    let conflicting = AppliedIndex {
        term: 2,
        ..cursor(2)
    };
    assert!(loader.set_applied(false, conflicting, false).is_err());
    let result = loader.set_applied(false, cursor(1), false);
    assert!(result.is_err());
    assert_eq!(loader.get_fsm_state().applied.index, 2);
    let bytes = journal
        .fs()
        .fs_dir
        .view()
        .unwrap()
        .load_journal_applied()
        .unwrap()
        .unwrap();
    let stored = <AppliedIndex as prost::Message>::decode(bytes.as_slice()).unwrap();
    assert_eq!(stored.index, 2);
    assert_eq!(stored.term, 1);
    assert!(journal.master_monitor().is_safe_mode());
    journal.shutdown();
}

#[test]
fn delayed_ufs_markers_do_not_rewind_progress() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "ufs-marker-watermark-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let marker = |index, op_id, rpc_id| UfsAppliedEntry {
        index,
        op_id,
        rpc_id,
        term: 1,
    };
    loader.ufs_applied(marker(10, 100, 7)).unwrap();
    loader.ufs_applied(marker(9, 80, 8)).unwrap();
    loader.ufs_applied(marker(10, 90, 0)).unwrap();
    let state = loader.get_fsm_state();
    assert_eq!(
        (
            state.ufs_applied.index,
            state.ufs_applied.op_id,
            state.ufs_applied.rpc_id
        ),
        (10, 100, 7)
    );
    loader.ufs_applied(marker(11, 50, 9)).unwrap();
    let state = loader.get_fsm_state();
    assert_eq!(
        (
            state.ufs_applied.index,
            state.ufs_applied.op_id,
            state.ufs_applied.rpc_id
        ),
        (11, 100, 9)
    );
    journal.shutdown();
}

#[test]
fn ufs_op_id_never_goes_backwards_for_out_of_order_entries() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "ufs-op-watermark-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    journal
        .append_committed_entry_for_test(entry(1, &[100, 2]))
        .unwrap();
    journal
        .append_committed_entry_for_test(entry(2, &[50]))
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(loader.role_change(raft::StateRole::Leader))
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while loader.get_fsm_state().ufs_applied.index < 2 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let state = loader.get_fsm_state();
    assert_eq!(state.ufs_applied.index, 2);
    assert_eq!(state.ufs_applied.op_id, 100);
    journal.shutdown();
}

#[test]
fn applied_op_id_never_goes_backwards_for_out_of_order_entries() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "journal-op-watermark-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(loader.apply(true, ApplyMsg::new_entry(entry(1, &[100, 2]))))
        .unwrap();
    assert_eq!(loader.get_fsm_state().applied.op_id, 100);
    runtime
        .block_on(loader.apply(true, ApplyMsg::new_entry(entry(2, &[50]))))
        .unwrap();
    let state = loader.get_fsm_state();
    assert_eq!(state.applied.index, 2);
    assert_eq!(state.applied.op_id, 100);
    journal.shutdown();
}

// A duplicate proposal that reaches the applier after a restart or failover
// carries the same request ID in its context. The persisted applied-request
// record must make the applier skip the metadata replay while still advancing
// the cursor.
#[test]
fn duplicate_proposal_req_id_skips_metadata_replay() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "duplicate-req-applied-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut first = entry(1, &[1]);
    first.context = SerdeUtils::serialize(&7777i64).unwrap();
    let mut duplicate = entry(2, &[2]);
    duplicate.context = SerdeUtils::serialize(&7777i64).unwrap();

    runtime
        .block_on(loader.apply0(false, &first, false))
        .unwrap();
    assert_eq!(loader.get_fsm_state().applied.index, 1);

    runtime
        .block_on(loader.apply0(false, &duplicate, false))
        .unwrap();
    // The cursor advances past the duplicate, but the metadata did not replay:
    // the recorded request index still points at the first entry.
    assert_eq!(loader.get_fsm_state().applied.index, 2);
    let applied_req = journal
        .fs()
        .fs_dir
        .view()
        .unwrap()
        .store
        .store
        .get_applied_req(7777)
        .unwrap()
        .unwrap();
    assert_eq!(applied_req.index, 1);
    journal.shutdown();
}

// Single-node groups have no peer that could miss the newer record format, so
// leader promotion must enable persisted child counts and a step-down must
// return new records to the legacy format.
#[test]
fn record_format_negotiation_enables_on_leader_and_resets_on_step_down() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "record-format-negotiation-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();

    // No apply worker in testing: role_change runs the inline path.
    curvine_model::persist_children_count(false);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(loader.role_change(raft::StateRole::Leader))
        .unwrap();
    assert!(
        curvine_model::children_count_persisted(),
        "single-node leader must enable persisted child counts"
    );

    rt.block_on(loader.role_change(raft::StateRole::Follower))
        .unwrap();
    assert!(
        !curvine_model::children_count_persisted(),
        "step-down must return new records to the legacy format"
    );
    journal.shutdown();
}

// The Raft-layer constant answered by Ping and the model-layer gate constant
// must never diverge: a mismatch would either block enablement forever or
// enable it behind a replica that cannot read the records.
#[test]
fn record_format_constants_match_across_crates() {
    assert_eq!(
        curvine_raft::raft::RECORD_FORMAT_READABLE,
        curvine_model::RECORD_FORMAT_READABLE
    );
}
