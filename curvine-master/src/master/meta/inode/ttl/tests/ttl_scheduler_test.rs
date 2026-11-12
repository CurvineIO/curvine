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

//! TTL role-gating tests, moved out of the inline module so the scheduler file
//! keeps only its registration. The module is still named `tests`, so the case
//! paths are unchanged.

use super::*;
use crate::master::fs::MasterFilesystem;
use crate::master::journal::JournalSystem;
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::Master;
use curvine_config::{ClusterConf, JournalConf, MasterConf};
use curvine_error::ErrorKind;
use curvine_model::{CreateFileOptsBuilder, OpenFlags, SetAttrOpts, TtlAction, WorkerInfo};
use curvine_raft::raft::RoleState;
use curvine_runtime::common::{DurationUnit, Utils};

// Short bucket interval so expiration buckets become due within the test.
const TTL_BUCKET_INTERVAL_MS: u64 = 10;

fn ttl_test_fs(name: &str) -> MasterFilesystem {
    Master::init_test_metrics();
    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("ttl-scheduler-test/meta-{}", name)),
            ttl_bucket_interval: format!("{}ms", TTL_BUCKET_INTERVAL_MS),
            ttl_bucket_interval_unit: DurationUnit::new(TTL_BUCKET_INTERVAL_MS),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!("ttl-scheduler-test/journal-{}", name)),
            ..Default::default()
        },
        ..Default::default()
    };
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.add_test_worker(WorkerInfo::default());
    fs
}

fn build_ttl_manager(fs: &MasterFilesystem) -> (Arc<InodeTtlManager>, Arc<TtlBucketList>) {
    let bucket_list = {
        let fs_dir = fs.fs_dir.view().unwrap();
        fs_dir.get_ttl_bucket_list()
    };
    let manager = InodeTtlManager::new(fs.clone(), bucket_list.clone()).unwrap();
    (Arc::new(manager), bucket_list)
}

/// Creates one file with TTL explicitly disabled, then publishes its TTL
/// registration exactly once through `complete_file`. The old fixture enabled
/// TTL at create time and completed with an updated mtime, so a bucket-width
/// boundary could register the same inode twice.
fn create_expired_file(fs: &MasterFilesystem, path: &str, bucket_list: &TtlBucketList) {
    let opts = CreateFileOptsBuilder::new()
        .create_parent(true)
        .ttl_ms(0)
        .ttl_action(TtlAction::None)
        .build();
    fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
        .unwrap();
    assert_eq!(
        bucket_list.total_inodes(),
        0,
        "creating without TTL must not register an expiration bucket"
    );

    // `complete_file` applies the attributes first and then registers the TTL
    // entry once, so this is the single registration with a past expiry.
    let attr = SetAttrOpts {
        mtime: Some(1),
        ttl_ms: Some(1),
        ttl_action: Some(TtlAction::Delete),
        ..Default::default()
    };
    // `complete_file` returns block metadata only on some paths; `None` is a
    // normal result, not a failure. The TTL registration asserted next is the
    // business outcome this fixture needs.
    fs.complete_file(path, None, 0, vec![], "", false, Some(attr))
        .expect("complete must succeed");
    assert_eq!(
        bucket_list.total_inodes(),
        1,
        "complete must register exactly one TTL entry"
    );
}

// A standby must not execute a scheduled TTL pass; after promotion the next
// pass executes the cleanup that was skipped (CurvineIO/curvine#1753).
#[test]
fn standby_skips_scheduled_ttl_cleanup_and_resumes_after_promotion() {
    let fs = ttl_test_fs("standby-skip");
    let (manager, bucket_list) = build_ttl_manager(&fs);
    let checker = TtlHeartbeatChecker::new(manager, TtlHeartbeatConfig::default());

    create_expired_file(&fs, "/ttl/standby.log", &bucket_list);

    // fs_only_for_test builds the filesystem without a journal role; the
    // node counts as a standby until it is explicitly promoted.
    assert!(!checker.is_active());
    checker.run().unwrap();
    assert_eq!(
        checker.execution_count.get(),
        0,
        "a standby pass must not count as a TTL execution"
    );
    assert!(
        fs.file_status("/ttl/standby.log").is_ok(),
        "a non-active node must not delete the expired file"
    );
    assert_eq!(
        bucket_list.total_inodes(),
        1,
        "a skipped pass must not drain the bucket list"
    );

    // After promotion the next scheduled pass executes the cleanup.
    fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
    assert!(checker.is_active());
    checker.run().unwrap();
    assert_eq!(
        checker.execution_count.get(),
        1,
        "the promoted pass must count as one TTL execution"
    );
    assert!(
        fs.file_status("/ttl/standby.log").is_err(),
        "the active master must delete the expired file"
    );
    assert_eq!(bucket_list.total_inodes(), 0);
}

// A direct cleanup call on a non-active node must not delete anything and
// keeps the drained inodes indexed for a later pass (CurvineIO/curvine#1753).
#[test]
fn non_active_cleanup_requeues_inodes_until_promotion() {
    let fs = ttl_test_fs("requeue-skip");
    let (manager, bucket_list) = build_ttl_manager(&fs);

    create_expired_file(&fs, "/ttl/requeue.log", &bucket_list);

    assert!(!manager.is_active());
    assert_eq!(manager.cleanup().unwrap(), 1);
    assert!(
        fs.file_status("/ttl/requeue.log").is_ok(),
        "standby cleanup must not delete the file"
    );
    assert_eq!(
        bucket_list.total_inodes(),
        1,
        "skipped inodes must be requeued for a later pass"
    );

    // Once promoted, the requeued inode is cleaned by the next cleanup call.
    fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
    assert!(manager.is_active());
    assert_eq!(manager.cleanup().unwrap(), 1);
    assert!(
        fs.file_status("/ttl/requeue.log").is_err(),
        "the active master must delete the expired file"
    );
    assert_eq!(bucket_list.total_inodes(), 0);
}
