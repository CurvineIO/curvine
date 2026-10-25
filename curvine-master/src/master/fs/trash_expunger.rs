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

use crate::master::fs::MasterFilesystem;
use crate::master::MasterMonitor;
use curvine_error::{FsError, FsResult};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::LoopTask;
use log::{info, warn};

// Bound on how many trash children are examined per namespace lock hold.
const TRASH_SCAN_BATCH: usize = 256;

/// Permanently removes recycle-bin entries whose retention has elapsed.
/// Only the active master runs the purge; standbys return without touching
/// the namespace so the journal keeps a single writer.
pub struct TrashExpunger {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    retention_ms: i64,
    scan_batch: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TrashExpungeResult {
    pub entries: u64,
    pub inodes: u64,
    pub bytes: u64,
}

impl TrashExpunger {
    pub fn new(fs: MasterFilesystem, monitor: MasterMonitor) -> Self {
        let retention_ms = fs.conf.trash_retention_ms().min(i64::MAX as u64) as i64;
        Self {
            fs,
            monitor,
            retention_ms,
            scan_batch: TRASH_SCAN_BATCH,
        }
    }

    #[cfg(test)]
    fn with_scan_batch(mut self, scan_batch: usize) -> Self {
        self.scan_batch = scan_batch;
        self
    }

    /// Purges entries deleted at or before `now_ms - retention`. Split from the
    /// scheduler loop so tests can move the clock without sleeping.
    pub fn expunge_expired(&self, now_ms: i64) -> FsResult<TrashExpungeResult> {
        let mut result = TrashExpungeResult::default();
        // "0" keeps entries until they are purged manually.
        if self.retention_ms <= 0 {
            return Ok(result);
        }

        let cutoff_ms = now_ms.saturating_sub(self.retention_ms);
        let mut start_after = None;
        loop {
            if !self.monitor.is_active() {
                break;
            }

            let page =
                self.fs
                    .scan_expired_trash(cutoff_ms, self.scan_batch, start_after.as_deref())?;
            for path in page.paths {
                match self.fs.delete_with_opts_if_active(&path, true, true) {
                    Ok(Some(deleted)) => {
                        result.entries += 1;
                        result.inodes += deleted.inodes;
                        result.bytes += deleted.bytes;
                    }
                    // Leadership moved elsewhere mid-sweep; a former leader
                    // must not mutate metadata or enqueue journal records.
                    Ok(None) => return Ok(result),
                    // The entry was restored or emptied after the scan.
                    Err(FsError::FileNotFound(_)) => {}
                    Err(e) => warn!("failed to expunge trash entry {}: {}", path, e),
                }
            }

            match page.next_after {
                Some(next) => start_after = Some(next),
                None => break,
            }
        }

        Ok(result)
    }
}

impl LoopTask for TrashExpunger {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        if !self.monitor.is_active() || self.retention_ms <= 0 {
            return Ok(());
        }

        let result = self.expunge_expired(LocalTime::mills() as i64)?;
        if result.entries > 0 {
            info!(
                "trash expunger purged {} expired entries ({} inodes, {} bytes)",
                result.entries, result.inodes, result.bytes
            );
        }
        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use crate::master::Master;
    use curvine_config::ClusterConf;
    use curvine_raft::raft::RoleState;
    use curvine_runtime::common::Utils;

    fn test_fs(name: &str, retention: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.trash_enabled = true;
        conf.master.trash_retention = retention.to_string();
        conf.master.init().unwrap();
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "trash-expunger/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "trash-expunger/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn trash_one(fs: &MasterFilesystem, path: &str) -> i64 {
        fs.create(path, true).unwrap();
        fs.delete_with_opts(path, false, false).unwrap();
        let entry = fs.list_trash().unwrap().remove(0);
        entry.delete_ms
    }

    #[test]
    fn expunges_only_entries_past_retention() {
        let fs = test_fs("expire", "1h");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        let expunger = TrashExpunger::new(fs.clone(), fs.master_monitor.clone());
        let delete_ms = trash_one(&fs, "/data/a.log");
        let retention_ms = fs.conf.trash_retention_ms() as i64;

        let kept = expunger
            .expunge_expired(delete_ms + retention_ms - 1)
            .unwrap();
        assert_eq!(kept, TrashExpungeResult::default());
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        let purged = expunger.expunge_expired(delete_ms + retention_ms).unwrap();
        assert_eq!(purged.entries, 1);
        assert_eq!(purged.inodes, 1);
        assert!(fs.list_trash().unwrap().is_empty());
        assert!(fs.get_block_locations("/data/a.log").is_err());
    }

    #[test]
    fn zero_retention_never_expunges() {
        let fs = test_fs("retention-zero", "0");
        let expunger = TrashExpunger::new(fs.clone(), fs.master_monitor.clone());
        trash_one(&fs, "/keep.log");

        let result = expunger.expunge_expired(i64::MAX).unwrap();
        assert_eq!(result, TrashExpungeResult::default());
        assert_eq!(fs.list_trash().unwrap().len(), 1);
    }

    #[test]
    fn run_expunges_only_on_the_active_master() {
        let fs = test_fs("leader-gate", "1ms");
        let expunger = TrashExpunger::new(fs.clone(), fs.master_monitor.clone());
        trash_one(&fs, "/data/b.log");
        std::thread::sleep(std::time::Duration::from_millis(10));

        expunger.run().unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        expunger.run().unwrap();
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn sweeps_all_entries_across_multiple_scan_pages() {
        let fs = test_fs("paged-sweep", "1ms");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        for id in 0..5 {
            trash_one(&fs, &format!("/data/{}.log", id));
        }
        let expunger = TrashExpunger::new(fs.clone(), fs.master_monitor.clone()).with_scan_batch(2);

        let result = expunger
            .expunge_expired(LocalTime::mills() as i64 + 10)
            .unwrap();

        assert_eq!(result.entries, 5);
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn demoted_master_stops_purging_mid_sweep() {
        let fs = test_fs("demote-sweep", "1ms");
        trash_one(&fs, "/data/a.log");
        trash_one(&fs, "/data/b.log");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);

        // Hold the namespace write lock so the sweep is in flight when the
        // role changes; the delete must observe the demotion at the mutation
        // boundary instead of purging the listing snapshot.
        let fs_dir = fs.fs_dir();
        let lock = fs_dir.write();
        let sweep_fs = fs.clone();
        let handle = std::thread::spawn(move || {
            let expunger = TrashExpunger::new(sweep_fs.clone(), sweep_fs.master_monitor.clone());
            expunger.expunge_expired(LocalTime::mills() as i64 + 10)
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        fs.master_monitor.journal_ctl.set_state(RoleState::Follower);
        drop(lock);

        let result = handle.join().unwrap().unwrap();
        assert_eq!(result, TrashExpungeResult::default());
        assert_eq!(fs.list_trash().unwrap().len(), 2);
    }

    #[test]
    fn stopped_master_terminates_expunger() {
        let fs = test_fs("stopped", "1h");
        let expunger = TrashExpunger::new(fs.clone(), fs.master_monitor.clone());

        assert!(!expunger.terminate());
        fs.master_monitor.journal_ctl.set_state(RoleState::Exit);
        assert!(expunger.terminate());
    }
}
