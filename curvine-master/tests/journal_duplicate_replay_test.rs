// Copyright 2025 OPPO.
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

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::Master;
use curvine_runtime::common::Utils;

// A timed-out proposal can be committed before the RPC is retried. This test
// isolates reordered follower replay; it does not simulate the RPC timeout.
#[test]
#[ignore = "duplicate proposal replay lacks a durable idempotency boundary"]
fn delayed_duplicate_delete_does_not_remove_recreated_path() {
    Master::init_test_metrics();
    let mut source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    source_conf.change_test_meta_dir(format!("duplicate-source-{}", Utils::rand_str(8)));
    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("duplicate-target-{}", Utils::rand_str(8)));
    let source = JournalSystem::from_conf(&source_conf).unwrap();
    let target = JournalSystem::from_conf(&target_conf).unwrap();
    let fs = source.fs();
    fs.mkdir("/duplicate", false).unwrap();
    let mkdir = fs.fs_dir.view().unwrap().take_entries();
    fs.delete("/duplicate", false).unwrap();
    let delete = fs.fs_dir.view().unwrap().take_entries();
    fs.mkdir("/duplicate", false).unwrap();
    let recreate = fs.fs_dir.view().unwrap().take_entries();
    assert_eq!((mkdir.len(), delete.len(), recreate.len()), (1, 1, 1));
    let loader = target.journal_loader();
    loader.apply_entry(mkdir[0].clone()).unwrap();
    loader.apply_entry(delete[0].clone()).unwrap();
    loader.apply_entry(recreate[0].clone()).unwrap();
    loader.apply_entry(delete[0].clone()).unwrap();
    assert!(target.fs().exists("/duplicate").unwrap());
    source.shutdown();
    target.shutdown();
}

// ---------------------------------------------------------------------------
// Real Raft batch boundary.
//
// The ignored case above calls `apply_entry` directly, which bypasses the
// Raft Entry context, `JournalBatch` decode, and the durable applied-request
// check in `apply0`. This case drives the same public `AppStorage::apply`
// entry the applier uses, with real encoded batches and distinct request ids,
// so the persisted dedup boundary actually runs.
// ---------------------------------------------------------------------------

use curvine_master::master::journal::{JournalBatch, JournalEntry};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_raft::utils::SerdeUtils;
use raft::eraftpb::Entry;

/// Wraps one real journal entry into a decoded Raft batch entry.
fn raft_batch_entry(index: u64, req_id: i64, op: &JournalEntry) -> Entry {
    let mut batch = JournalBatch::new(index);
    batch.push(op.clone());
    Entry {
        index,
        term: 1,
        data: batch.encode().expect("encode journal batch"),
        context: SerdeUtils::serialize(&req_id)
            .expect("encode request context")
            .into(),
        ..Default::default()
    }
}

#[test]
fn raft_request_duplicate_delete_preserves_recreated_inode() {
    Master::init_test_metrics();
    let mut source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    source_conf.change_test_meta_dir(format!("duplicate-raft-source-{}", Utils::rand_str(8)));
    // Keep the testing writer's retained receiver and avoid the historical
    // queue/snapshot self-block while capturing real entries.
    source_conf.journal.enable = true;
    source_conf.journal.snapshot_entries = 0;
    source_conf.journal.writer_channel_size = 8;
    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("duplicate-raft-target-{}", Utils::rand_str(8)));
    let source_root = std::path::Path::new(&source_conf.master.meta_dir)
        .parent()
        .expect("source meta dir parent")
        .to_path_buf();
    let target_root = std::path::Path::new(&target_conf.master.meta_dir)
        .parent()
        .expect("target meta dir parent")
        .to_path_buf();

    let source = JournalSystem::from_conf(&source_conf).unwrap();
    let target = JournalSystem::from_conf(&target_conf).unwrap();
    let source_fs = source.fs();

    // Three genuine entries from a real public mkdir/delete/recreate sequence.
    let original_id = source_fs.mkdir("/duplicate", false).unwrap().id;
    let mkdir = source_fs.fs_dir.view().unwrap().take_entries();
    source_fs.delete("/duplicate", false).unwrap();
    let delete = source_fs.fs_dir.view().unwrap().take_entries();
    let recreated_id = source_fs.mkdir("/duplicate", false).unwrap().id;
    let recreate = source_fs.fs_dir.view().unwrap().take_entries();
    assert_eq!((mkdir.len(), delete.len(), recreate.len()), (1, 1, 1));
    assert!(matches!(mkdir[0], JournalEntry::Mkdir(_)));
    assert!(matches!(delete[0], JournalEntry::Delete(_)));
    assert!(matches!(recreate[0], JournalEntry::Mkdir(_)));
    assert_ne!(
        original_id, recreated_id,
        "the recreated path is a different inode"
    );

    let req_delete: i64 = 102;
    let first = raft_batch_entry(1, 101, &mkdir[0]);
    let second = raft_batch_entry(2, req_delete, &delete[0]);
    // The duplicate reuses this exact payload; only the index differs.
    let second_data = second.data.clone();
    let third = raft_batch_entry(3, 103, &recreate[0]);

    let loader = target.journal_loader();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let apply = |entry: Entry| {
        runtime
            .block_on(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    loader.apply(true, ApplyMsg::new_entry(entry)),
                )
                .await
            })
            .expect("applying a committed entry must finish within the bound")
    };
    let first_result = apply(first);
    let second_result = apply(second);
    let third_result = apply(third);

    let target_fs = target.fs();
    let after_recreate = target_fs.file_status("/duplicate");

    // Duplicate proposal: identical batch payload and identical request id.
    let duplicate = Entry {
        index: 4,
        term: 1,
        data: second_data,
        context: SerdeUtils::serialize(&req_delete)
            .expect("encode duplicate context")
            .into(),
        ..Default::default()
    };
    let duplicate_result = apply(duplicate);

    let after_duplicate = target_fs.file_status("/duplicate");
    let original_lookup = target_fs.file_status_by_id("/duplicate", Some(original_id));
    let echo = target_fs.fs_dir.view().unwrap().take_entries();

    // Everything is collected; tear down before asserting.
    source.shutdown();
    target.shutdown();
    let cleanup = (|| {
        let mut first_error = None;
        for root in [&source_root, &target_root] {
            if let Err(error) = std::fs::remove_dir_all(root) {
                first_error.get_or_insert(error);
            }
        }
        first_error
    })();

    assert!(first_result.is_ok(), "mkdir apply: {first_result:?}");
    assert!(second_result.is_ok(), "delete apply: {second_result:?}");
    assert!(third_result.is_ok(), "recreate apply: {third_result:?}");
    assert!(
        duplicate_result.is_ok(),
        "the duplicate request must be accepted and skipped: {duplicate_result:?}"
    );
    let after_recreate = after_recreate.expect("the recreated path must exist after replay");
    assert_eq!(
        after_recreate.id, recreated_id,
        "replay must recreate the same inode id the source produced"
    );
    let after_duplicate =
        after_duplicate.expect("the duplicate delete must not remove the recreated path");
    assert_eq!(
        after_duplicate.id, recreated_id,
        "the duplicate must not touch the recreated inode"
    );
    assert!(
        matches!(
            original_lookup.as_ref().map_err(|error| error.kind()),
            Err(curvine_error::ErrorKind::FileNotFound)
        ),
        "the original inode must stay gone, got: {original_lookup:?}"
    );
    assert!(
        echo.is_empty(),
        "replay must not echo journal entries: {echo:?}"
    );
    assert!(cleanup.is_none(), "cleanup failed: {cleanup:?}");
}
