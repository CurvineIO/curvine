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

use crate::master::meta::inode::ttl::ttl_manager::InodeTtlManager;
use curvine_error::FsError;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::runtime::LoopTask;
use curvine_runtime::sync::AtomicCounter;
use log::{error, info};
use std::sync::Arc;
// TTL Scheduler Module
//
// This module provides the scheduling infrastructure for TTL operations.
// It implements a heartbeat-based scheduler that periodically triggers TTL cleanup.
//
// Key Features:
// - Heartbeat-based TTL cleanup scheduling
// - Integration with LoopTask framework
// - Configurable execution intervals and timeouts
// - Execution monitoring and statistics
// - Error handling and recovery
// - Task lifecycle management

#[derive(Clone)]
pub struct TtlHeartbeatChecker {
    ttl_manager: Arc<InodeTtlManager>,
    config: TtlHeartbeatConfig,
    execution_count: Arc<AtomicCounter>,
}

impl TtlHeartbeatChecker {
    pub fn new(ttl_manager: Arc<InodeTtlManager>, config: TtlHeartbeatConfig) -> Self {
        Self {
            ttl_manager,
            config,
            execution_count: Arc::new(AtomicCounter::new(0)),
        }
    }
    pub fn get_task_name(&self) -> &str {
        &self.config.task_name
    }
    pub fn get_timeout_ms(&self) -> u64 {
        self.config.timeout_ms
    }
    pub fn is_serving(&self) -> bool {
        self.ttl_manager.is_serving()
    }
}

#[derive(Debug, Clone)]
pub struct TtlHeartbeatConfig {
    pub task_name: String,
    pub timeout_ms: u64,
}

impl Default for TtlHeartbeatConfig {
    fn default() -> Self {
        Self {
            task_name: "ttl-heartbeat".to_string(),
            timeout_ms: 300_000, // 5 minutes timeout
        }
    }
}

impl LoopTask for TtlHeartbeatChecker {
    type Error = FsError;
    fn run(&self) -> Result<(), Self::Error> {
        if !self.is_serving() {
            info!(
                "skip inode ttl clean execution for task '{}': node is not serving writes",
                self.get_task_name()
            );
            return Ok(());
        }

        let spend = TimeSpent::new();
        let execution_id = self.execution_count.add_and_get(1);

        info!(
            "Starting inode ttl clean execution #{} for task '{}'",
            execution_id,
            self.get_task_name()
        );

        let result = self.ttl_manager.cleanup();
        let used_ms = spend.used_ms();
        if used_ms > self.get_timeout_ms() {
            error!(
                "Inode ttl execution #{} for task '{}' exceeded timeout of {}ms (actual: {}ms)",
                execution_id,
                self.get_task_name(),
                self.get_timeout_ms(),
                used_ms
            );
        }
        if let Err(e) = result {
            error!("TTL heartbeat execution failed: execution_id={}, task_name={}, duration={}ms, error={}",
                execution_id, self.get_task_name(), used_ms, e);
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        // TTL manager is always ready after creation, so we never terminate
        // unless the entire system is shutting down
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::fs::MasterFilesystem;
    use crate::master::journal::JournalSystem;
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::Master;
    use curvine_config::{ClusterConf, JournalConf, MasterConf};
    use curvine_model::{CreateFileOptsBuilder, OpenFlags, TtlAction, WorkerInfo};
    use curvine_raft::raft::RoleState;
    use curvine_runtime::common::{DurationUnit, Utils};
    use std::time::Duration;

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
            let fs_dir_lock = fs.fs_dir();
            let fs_dir = fs_dir_lock.read();
            fs_dir.get_ttl_bucket_list()
        };
        let manager = InodeTtlManager::new(fs.clone(), bucket_list.clone()).unwrap();
        (Arc::new(manager), bucket_list)
    }

    // Auto-created parents inherit a Delete TTL from the file (#2293), so these
    // tests create `/ttl` up front to keep only the file under test indexed.
    fn create_parent_dir(fs: &MasterFilesystem) {
        fs.mkdir("/ttl", true).unwrap();
    }

    fn create_expired_file(fs: &MasterFilesystem, path: &str) {
        create_parent_dir(fs);
        create_expired_file_with_implicit_parent(fs, path);
    }

    // Creates an expired file whose parent is auto-created by `create_parent`,
    // so the parent inherits the file's Delete TTL (#2293). `/ttl` is not
    // pre-created here on purpose.
    fn create_expired_file_with_implicit_parent(fs: &MasterFilesystem, path: &str) {
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();
        fs.complete_file(path, None, 0, vec![], "", false, None)
            .unwrap();
    }

    // A pass may visit an expired directory before its only child is unlinked,
    // so cascading the deletion up the tree takes another pass.
    fn drain_ttl_until<F>(manager: &InodeTtlManager, done: F)
    where
        F: Fn() -> bool,
    {
        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(TTL_BUCKET_INTERVAL_MS * 2));
            manager.cleanup().unwrap();
            if done() {
                return;
            }
        }
    }

    // A standby must not execute a scheduled TTL pass; after promotion the next
    // pass executes the cleanup that was skipped (CurvineIO/curvine#1753).
    #[test]
    fn standby_skips_scheduled_ttl_cleanup_and_resumes_after_promotion() {
        let fs = ttl_test_fs("standby-skip");
        let (manager, bucket_list) = build_ttl_manager(&fs);
        let checker = TtlHeartbeatChecker::new(manager, TtlHeartbeatConfig::default());

        create_expired_file(&fs, "/ttl/standby.log");
        std::thread::sleep(Duration::from_millis(100));

        // A follower counts as a standby until it is explicitly promoted.
        fs.master_monitor.journal_ctl.set_state(RoleState::Follower);
        assert!(!checker.is_serving());
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
        assert!(checker.is_serving());
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

    // `complete_file` advances mtime and re-indexes the file, so the same inode
    // reaches the bucket list twice with two expirations. It must move, not be
    // counted twice: a stale entry made `total_inodes()` read 2 for one file and
    // made one cleanup pass report 2 (#2290).
    #[test]
    fn complete_file_re_indexes_the_file_instead_of_duplicating_it() {
        let fs = ttl_test_fs("reindex-on-complete");
        let (manager, bucket_list) = build_ttl_manager(&fs);
        create_parent_dir(&fs);
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build();
        fs.create_with_opts(
            "/ttl/reindex.log",
            opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        // Longer than the bucket interval, so create and complete cannot land in
        // the same bucket: the second add has to replace the first one.
        std::thread::sleep(Duration::from_millis(30));
        fs.complete_file("/ttl/reindex.log", None, 0, vec![], "", false, None)
            .unwrap();

        assert_eq!(bucket_list.buckets_len(), 1, "one file must own one bucket");
        assert_eq!(
            bucket_list.total_inodes(),
            1,
            "complete_file must move the TTL entry instead of adding a second one"
        );

        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            manager.cleanup().unwrap(),
            1,
            "one cleanup pass must see the file once"
        );
    }

    // A direct cleanup call on a non-active node must not delete anything and
    // keeps the drained inodes indexed for a later pass (CurvineIO/curvine#1753).
    #[test]
    fn non_active_cleanup_requeues_inodes_until_promotion() {
        let fs = ttl_test_fs("requeue-skip");
        let (manager, bucket_list) = build_ttl_manager(&fs);

        create_expired_file(&fs, "/ttl/requeue.log");
        std::thread::sleep(Duration::from_millis(100));

        fs.master_monitor.journal_ctl.set_state(RoleState::Follower);
        assert!(!manager.is_serving());
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
        assert!(manager.is_serving());
        assert_eq!(manager.cleanup().unwrap(), 1);
        assert!(
            fs.file_status("/ttl/requeue.log").is_err(),
            "the active master must delete the expired file"
        );
        assert_eq!(bucket_list.total_inodes(), 0);
    }

    // Regression test for #2293: a load job creates the missing parent dirs, and
    // they inherit the file's Delete TTL. Once the file expires the TTL pass must
    // remove the now-empty directory instead of leaving it in the namespace
    // forever. The scheduler tests pre-create `/ttl`, so this is the only case
    // exercising the directory branch end to end.
    #[test]
    fn cleanup_removes_expired_file_and_its_implicit_parent() {
        let fs = ttl_test_fs("implicit-parent-cleanup");
        let (manager, bucket_list) = build_ttl_manager(&fs);

        create_expired_file_with_implicit_parent(&fs, "/implicit/file.log");
        assert!(
            fs.exists("/implicit").unwrap(),
            "create_parent must auto-create the parent directory"
        );
        assert_eq!(
            bucket_list.total_inodes(),
            2,
            "the file and its inherited-TTL parent must both be indexed"
        );

        std::thread::sleep(Duration::from_millis(100));
        drain_ttl_until(&manager, || !fs.exists("/implicit").unwrap());

        assert!(
            !fs.exists("/implicit/file.log").unwrap(),
            "the expired file must be deleted"
        );
        assert!(
            !fs.exists("/implicit").unwrap(),
            "the empty inherited-TTL directory must be deleted once its file is gone"
        );
        assert_eq!(bucket_list.total_inodes(), 0);
    }

    // The directory branch defers an expired directory that still has children,
    // so a file added between expiry and the TTL pass must survive: the
    // directory stays and remains queued for a later pass (#2293).
    #[test]
    fn expired_implicit_parent_with_a_live_child_is_deferred() {
        let fs = ttl_test_fs("implicit-parent-defer");
        let (manager, bucket_list) = build_ttl_manager(&fs);

        create_expired_file_with_implicit_parent(&fs, "/implicit/file.log");
        std::thread::sleep(Duration::from_millis(100));

        // A child without a TTL arrives after the parent expired but before the
        // cleanup pass.
        fs.create("/implicit/keep.log", false).unwrap();

        manager.cleanup().unwrap();

        assert!(
            !fs.exists("/implicit/file.log").unwrap(),
            "the expired file is still removed"
        );
        assert!(
            fs.exists("/implicit").unwrap(),
            "the parent must survive while it still has a live child"
        );
        assert!(fs.exists("/implicit/keep.log").unwrap());
        assert!(
            bucket_list.total_inodes() >= 1,
            "the surviving directory must stay queued for a later pass"
        );
    }
}
