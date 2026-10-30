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

use crate::{ChoosingPolicy, RobinChoosingPolicy, VfsDir};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{HealthPolicy, HealthState, HealthTransition, StorageInfo, StorageType};
use curvine_runtime::common::ByteUnit;
use indexmap::map::Values;
use indexmap::IndexMap;
use log::{error, info, warn};
use std::ops::Index;
use std::sync::Arc;

/// Layout-independent request for physical capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRequest {
    pub storage_type: StorageType,
    pub bytes: i64,
}

impl StorageRequest {
    pub fn new(storage_type: StorageType, bytes: i64) -> CommonResult<Self> {
        if bytes < 0 {
            return err_box!("Storage allocation size cannot be negative: {}", bytes);
        }
        Ok(Self {
            storage_type,
            bytes,
        })
    }
}

// Directory list.
pub struct DirList {
    dirs: IndexMap<u32, Arc<VfsDir>>,
    chooser: ChoosingPolicy,
    health_policy: HealthPolicy,
}

impl DirList {
    pub fn new(list: Vec<VfsDir>) -> CommonResult<Self> {
        let mut dirs = IndexMap::new();
        for dir in list {
            if dirs.contains_key(&dir.id()) {
                return err_box!("Storage ID hash conflict for dir {}", dir.id());
            }
            dirs.insert(dir.id(), Arc::new(dir));
        }

        Ok(Self {
            dirs,
            chooser: ChoosingPolicy::Robin(RobinChoosingPolicy::new()),
            health_policy: HealthPolicy::default(),
        })
    }

    pub fn len(&self) -> usize {
        self.dirs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dir_iter(&self) -> Values<'_, u32, Arc<VfsDir>> {
        self.dirs.values()
    }

    pub fn get_dir(&self, dir_id: u32) -> Option<&Arc<VfsDir>> {
        self.dirs.get(&dir_id)
    }

    fn get_dir_check(&self, dir_id: u32) -> CommonResult<&VfsDir> {
        match self.get_dir(dir_id) {
            Some(dir) => Ok(dir),
            None => err_box!("No storage directory found: {}", dir_id),
        }
    }

    pub fn get_dir_index(&self, index: usize) -> Option<&Arc<VfsDir>> {
        self.dirs.get_index(index).map(|x| x.1)
    }

    // Get the total capacity of all storage directories.
    pub fn capacity(&self) -> i64 {
        let mut capacity = 0i64;
        for dir in self.dir_iter() {
            capacity += dir.capacity();
        }

        capacity
    }

    // Get the total available capacity of all storage
    pub fn available(&self) -> i64 {
        let mut capacity = 0i64;
        for dir in self.dir_iter() {
            capacity += dir.available();
        }

        capacity
    }

    // How much space is used for internal storage
    pub fn fs_used(&self) -> i64 {
        let mut used = 0i64;
        for dir in self.dir_iter() {
            used += dir.fs_used();
        }

        used
    }

    /// Select a physical directory without changing its capacity accounting.
    pub fn choose_dir(&mut self, request: StorageRequest) -> CommonResult<Arc<VfsDir>> {
        Ok(self
            .chooser
            .choose_dir(&self.dirs, request.storage_type, request.bytes)?
            .clone())
    }

    /// Update space usage when an existing block enters writing.
    pub fn update_write_space(
        &self,
        dir_id: u32,
        was_final: bool,
        old_bytes: i64,
        new_bytes: i64,
    ) -> CommonResult<()> {
        if new_bytes < 0 {
            return err_box!("Storage allocation size cannot be negative: {}", new_bytes);
        }
        let dir = self.get_dir_check(dir_id)?;
        let reclaimable = old_bytes.max(0);
        if new_bytes > dir.available().saturating_add(reclaimable) {
            return err_box!(
                "Not enough space in storage dir {}: need {}, available {} (including {} reusable bytes)",
                dir_id,
                new_bytes,
                dir.available(),
                reclaimable
            );
        }

        dir.release_space(was_final, old_bytes);
        dir.reserve_space(false, new_bytes);
        Ok(())
    }

    /// Update space usage when a writing block is finalized.
    pub fn update_final_space(
        &self,
        dir_id: u32,
        reserved_bytes: i64,
        final_bytes: i64,
    ) -> CommonResult<()> {
        if final_bytes < 0 {
            return err_box!(
                "Storage allocation size cannot be negative: {}",
                final_bytes
            );
        }
        let dir = self.get_dir_check(dir_id)?;
        dir.release_space(false, reserved_bytes);
        dir.reserve_space(true, final_bytes);
        Ok(())
    }

    /// Return capacity held by an aborted or removed block.
    pub fn release_space(&self, dir_id: u32, is_final: bool, bytes: i64) -> CommonResult<()> {
        self.get_dir_check(dir_id)?.release_space(is_final, bytes);
        Ok(())
    }

    /// Thresholds that decide when a directory becomes unusable and when it
    /// comes back. Shared by every checker.
    pub fn health_policy(&self) -> &HealthPolicy {
        &self.health_policy
    }

    pub fn with_health_policy(mut self, policy: HealthPolicy) -> Self {
        self.health_policy = policy;
        self
    }

    /// Probe every directory and advance its health state machine.
    ///
    /// Runs on the worker health-check loop, never on the heartbeat path: a
    /// directory is quarantined after `failures_to_unusable` consecutive
    /// failures and recovers automatically after `successes_to_recover`
    /// consecutive successes, so a transient error no longer disables a disk
    /// until the worker restarts. Each transition is logged once.
    pub fn probe_all(&self) -> Vec<ProbeReport> {
        let dirs: Vec<Arc<VfsDir>> = self.dir_iter().cloned().collect();
        probe_dirs(&dirs, &self.health_policy)
    }

    /// Per-directory state from the last probe. Pure memory: the heartbeat uses
    /// this so a hung mount cannot stall it.
    pub fn get_and_check_storages(&self, block_num: i64) -> Vec<StorageInfo> {
        self.dir_iter()
            .map(|dir| {
                let sample = dir.sample();
                let failed = dir.is_failed();
                StorageInfo {
                    dir_id: dir.id(),
                    storage_id: dir.version().storage_id.to_string(),
                    failed,
                    capacity: sample.capacity,
                    fs_used: sample.fs_used,
                    non_fs_used: sample.non_fs_used,
                    available: sample.available,
                    reserved_bytes: dir.reserved_bytes(),
                    storage_type: dir.storage_type(),
                    block_num,
                    dir_path: dir.path_str().to_string(),
                    health_state: dir.health_state(),
                    fault_kind: dir.fault_kind(),
                    unhealthy_since_ms: dir.unhealthy_since_ms(),
                }
            })
            .collect()
    }

    pub fn failed_count(&self) -> usize {
        self.dir_iter().filter(|dir| dir.is_failed()).count()
    }

    #[cfg(test)]
    fn reserve_with_str<S: AsRef<str>>(
        &mut self,
        stg_type: StorageType,
        size_str: S,
    ) -> CommonResult<Arc<VfsDir>> {
        let bytes = ByteUnit::from_str(size_str.as_ref())?.as_byte() as i64;
        let dir = self.choose_dir(StorageRequest::new(stg_type, bytes)?)?;
        dir.reserve_space(false, bytes);
        Ok(dir)
    }

    pub fn add_dir(&mut self, dir: VfsDir) {
        if !self.dirs.contains_key(&dir.id()) {
            info!(
                "Add new vfs dir, storage_type: {:?}, path: {}, device id: {}, capacity: {}, available: {}",
                dir.storage_type(),
                dir.path_str(),
                dir.device_id(),
                ByteUnit::byte_to_string(dir.capacity() as u64),
                ByteUnit::byte_to_string(dir.available() as u64),
            );
            self.dirs.insert(dir.id(), Arc::new(dir));
        } else {
            warn!("Dir {:?} already exists", dir)
        }
    }

    pub fn remove_dir(&mut self, dir: &VfsDir) -> CommonResult<()> {
        if self.dirs.swap_remove(&dir.id()).is_some() {
            info!(
                "Remove storage dir, [{:?}:{}]{:?}",
                dir.storage_type(),
                dir.capacity(),
                dir.base_path()
            );
        }

        Ok(())
    }
}

/// Outcome of probing one directory, for metrics and reporting.
#[derive(Debug, Clone)]
pub struct ProbeReport {
    pub dir_id: u32,
    pub dir_path: String,
    pub storage_type: StorageType,
    pub state: HealthState,
    pub ok: bool,
    pub elapsed: std::time::Duration,
    pub transition: Option<HealthTransition>,
}

/// Probe the given directories and advance their health state machines.
///
/// Free function so callers can collect the directories under a lock and run
/// the syscalls *without* holding it: a hung mount must never stall writers.
pub fn probe_dirs(dirs: &[Arc<VfsDir>], policy: &HealthPolicy) -> Vec<ProbeReport> {
    let now_ms = curvine_runtime::common::LocalTime::mills() as i64;
    dirs.iter()
        .map(|dir| {
            let started = std::time::Instant::now();
            let outcome = dir.probe();
            let elapsed = started.elapsed();
            let transition = dir.observe_health(&outcome, policy, now_ms);
            if let Some(t) = transition.as_ref() {
                log_health_transition(dir, t);
            }
            ProbeReport {
                dir_id: dir.id(),
                dir_path: dir.path_str().to_string(),
                storage_type: dir.storage_type(),
                state: dir.health_state(),
                ok: outcome.is_ok(),
                elapsed,
                transition,
            }
        })
        .collect()
}

fn log_health_transition(dir: &VfsDir, transition: &HealthTransition) {
    if transition.to == HealthState::Unusable {
        error!(
            "storage_health_transition dir_id={} path={} from={} to={} kind={} detail={}",
            dir.id(),
            dir.path_str(),
            transition.from,
            transition.to,
            transition.fault,
            transition.detail,
        );
    } else {
        info!(
            "storage_health_transition dir_id={} path={} from={} to={}",
            dir.id(),
            dir.path_str(),
            transition.from,
            transition.to,
        );
    }
}

impl Index<usize> for DirList {
    type Output = Arc<VfsDir>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.dirs[index]
    }
}

#[cfg(test)]
mod tests {
    use crate::{DirList, StorageRequest, VfsDir};
    use curvine_config::WorkerDataDir;
    use curvine_core_error::CommonResult;
    use curvine_model::StorageType;
    use curvine_model::{HealthPolicy, HealthState};
    use curvine_runtime::common::{ByteUnit, FileUtils};

    #[test]
    fn robin() -> CommonResult<()> {
        let id = "";

        FileUtils::delete_path("../testing/robin", true)?;
        let dirs = [
            WorkerDataDir::from_str("[MEM:10MB]../testing/robin/d1")?,
            WorkerDataDir::from_str("[SSD:100MB]../testing/robin/d2")?,
            WorkerDataDir::from_str("[SSD:100MB]../testing/robin/d3")?,
        ];

        let vfs_dir = vec![
            VfsDir::from_dir(id, dirs[0].clone())?,
            VfsDir::from_dir(id, dirs[1].clone())?,
            VfsDir::from_dir(id, dirs[2].clone())?,
        ];

        let mut list = DirList::new(vfs_dir)?;

        let mem = list.reserve_with_str(StorageType::Mem, "5MB")?;
        println!("choose mem: {:?}", mem.base_path());
        assert_eq!(dirs[0].path_str(), mem.path_str());

        println!("available {:?}", list.available());

        let ssd1 = list.reserve_with_str(StorageType::Mem, "90MB")?;
        println!("choose ssd1: {:?}", ssd1.base_path());
        assert_eq!(dirs[1].path_str(), ssd1.path_str());

        let ssd2 = list.reserve_with_str(StorageType::Mem, "90MB")?;
        println!("choose ssd2: {:?}", ssd2.base_path());
        assert_eq!(dirs[2].path_str(), ssd2.path_str());

        // Check capacity
        println!(
            "capacity = {}, available = {}",
            ByteUnit::byte_to_string(list.capacity() as u64),
            ByteUnit::byte_to_string(list.available() as u64)
        );
        // assert_eq!(list.capacity(), 21 * ByteUnit::GB as i64);
        // assert_eq!(list.available(), 2500 * ByteUnit::MB as i64);

        Ok(())
    }

    #[test]
    fn choosing_dir_does_not_change_capacity() -> CommonResult<()> {
        FileUtils::delete_path("../testing/dir-selection-capacity", true)?;
        let dir = VfsDir::from_dir(
            "",
            WorkerDataDir::from_str("[SSD:100MB]../testing/dir-selection-capacity")?,
        )?;
        let mut group = DirList::new(vec![dir])?;
        let before = group.available();

        let dir = group.choose_dir(StorageRequest::new(
            StorageType::Ssd,
            10 * ByteUnit::MB as i64,
        )?)?;
        assert_eq!(group.available(), before);
        dir.reserve_space(false, 10 * ByteUnit::MB as i64);
        assert_eq!(group.available(), before - 10 * ByteUnit::MB as i64);
        Ok(())
    }

    #[test]
    fn failing_dir_is_quarantined_and_recovers_automatically() -> CommonResult<()> {
        use curvine_model::ProbeOutcome;

        FileUtils::delete_path("../testing/dir-health-recovery", true)?;
        let dir = VfsDir::from_dir(
            "",
            WorkerDataDir::from_str("[SSD:100MB]../testing/dir-health-recovery")?,
        )?;
        let dir_id = dir.id();
        let mut group = DirList::new(vec![dir])?.with_health_policy(HealthPolicy::new(2, 2));
        let dir = group.get_dir(dir_id).unwrap().clone();
        assert_eq!(group.failed_count(), 0);

        let broken = ProbeOutcome::failed(curvine_model::FaultKind::IoError, "EIO");

        // First failure only marks the directory suspect: still allocatable.
        dir.observe_health(&broken, group.health_policy(), 1);
        assert_eq!(dir.health_state(), HealthState::Suspect);
        assert_eq!(group.failed_count(), 0);
        assert!(group
            .choose_dir(StorageRequest::new(StorageType::Ssd, 1)?)
            .is_ok());

        // Reaching the threshold quarantines it and stops allocation.
        dir.observe_health(&broken, group.health_policy(), 1);
        assert_eq!(group.failed_count(), 1);
        assert!(group
            .choose_dir(StorageRequest::new(StorageType::Ssd, 1)?)
            .is_err());

        // Recovery needs consecutive successes, then allocation resumes
        // without restarting the worker.
        let ok = ProbeOutcome::Ok;
        dir.observe_health(&ok, group.health_policy(), 2);
        assert_eq!(group.failed_count(), 1);
        dir.observe_health(&ok, group.health_policy(), 2);
        assert_eq!(dir.health_state(), HealthState::Healthy);
        assert_eq!(group.failed_count(), 0);
        assert!(group
            .choose_dir(StorageRequest::new(StorageType::Ssd, 1)?)
            .is_ok());
        Ok(())
    }

    #[test]
    fn probe_reports_missing_directory_as_typed_fault() -> CommonResult<()> {
        use curvine_model::FaultKind;

        FileUtils::delete_path("../testing/dir-probe-missing", true)?;
        let dir = VfsDir::from_dir(
            "",
            WorkerDataDir::from_str("[SSD:100MB]../testing/dir-probe-missing")?,
        )?;
        assert!(dir.probe().is_ok());

        FileUtils::delete_path("../testing/dir-probe-missing", true)?;
        let probe = dir.probe();
        let (kind, _) = probe.fault().expect("missing dir must be reported");
        assert_eq!(kind, FaultKind::PathMissing);
        Ok(())
    }

    #[test]
    fn exact_fit_and_empty_group_are_handled() -> CommonResult<()> {
        let mut empty = DirList::new(vec![])?;
        assert!(empty
            .choose_dir(StorageRequest::new(StorageType::Ssd, 1)?)
            .is_err());

        FileUtils::delete_path("../testing/capacity-exact-fit", true)?;
        let dir = VfsDir::from_dir(
            "",
            WorkerDataDir::from_str("[SSD:10MB]../testing/capacity-exact-fit")?,
        )?;
        let mut group = DirList::new(vec![dir])?;
        let available = group.available();
        let dir = group.choose_dir(StorageRequest::new(StorageType::Ssd, available)?)?;
        assert_eq!(group.available(), available);
        dir.reserve_space(false, available);
        assert_eq!(group.available(), 0);
        group.release_space(dir.id(), false, available)?;
        assert_eq!(group.available(), available);
        Ok(())
    }
}
