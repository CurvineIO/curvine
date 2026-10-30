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

use crate::{DirState, StorageVersion, DEFAULT_BLOCK_ALIGN};
use curvine_config::WorkerDataDir;
use curvine_core_error::CommonResult;
use curvine_io::LocalFile;
use curvine_model::{
    FaultKind, HealthPolicy, HealthState, HealthTracker, HealthTransition, ProbeOutcome,
    StorageType,
};
use curvine_runtime::common::{ByteUnit, FileUtils};
use curvine_runtime::sync::AtomicLong;
#[cfg(feature = "spdk")]
use curvine_storage_spdk::SpdkEnv;
use curvine_sys::FsStats;
use log::*;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
/// A physical worker storage directory.
///
/// This type owns medium identity, capacity accounting, health, and the
/// medium-specific device handle. It deliberately has no knowledge of how a
/// block layout names or arranges data inside the directory.
pub struct VfsDir {
    pub(crate) version: StorageVersion,
    pub(crate) stats: FsStats,
    pub(crate) storage_type: StorageType,
    pub(crate) conf_capacity: i64,
    pub(crate) reserved_bytes: i64,
    /// Free-space ratio floor for write admission. `0.0` disables. See
    /// `available()` guard and `raw_free_ratio()`.
    pub(crate) free_ratio: f64,
    pub(crate) final_bytes: AtomicLong,
    pub(crate) tmp_bytes: AtomicLong,
    pub(crate) state: Arc<DirState>,
    /// Health state machine for this directory. Driven by the worker probe
    /// loop, read by admission and the heartbeat snapshot.
    pub(crate) health: Arc<HealthTracker>,
    /// Capacity numbers sampled by the last probe. The heartbeat reports these
    /// so it never issues a filesystem syscall on a possibly hung mount.
    pub(crate) sample: Arc<SampleCache>,
}

/// Last sampled capacity of one directory (see `VfsDir::probe`).
pub struct SampleCache {
    capacity: AtomicLong,
    available: AtomicLong,
    fs_used: AtomicLong,
    non_fs_used: AtomicLong,
}

impl std::fmt::Debug for SampleCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SampleCache")
            .field("capacity", &self.capacity.get())
            .field("available", &self.available.get())
            .finish()
    }
}

impl Default for SampleCache {
    fn default() -> Self {
        Self {
            capacity: AtomicLong::new(0),
            available: AtomicLong::new(0),
            fs_used: AtomicLong::new(0),
            non_fs_used: AtomicLong::new(0),
        }
    }
}

/// One directory's capacity snapshot, safe to read from any thread.
#[derive(Debug, Clone, Copy, Default)]
pub struct DirSample {
    pub capacity: i64,
    pub available: i64,
    pub fs_used: i64,
    pub non_fs_used: i64,
}

impl VfsDir {
    pub fn new(
        version: StorageVersion,
        conf: WorkerDataDir,
        reserved_bytes: u64,
        free_ratio: f64,
    ) -> CommonResult<Self> {
        let stg_dir = if version.cluster_id.is_empty() {
            conf.path.clone()
        } else {
            format!("{}/{}", conf.path, version.cluster_id)
        };
        let stats = FsStats::new(&stg_dir);

        // SPDK: skip filesystem (no local dir, version file, or filesystem checks).
        // TODO(spdk): persist dir_id for SpdkDisk too; it's regenerated randomly every restart, breaking the dir_id-to-bdev binding.
        if conf.storage_type != StorageType::SpdkDisk {
            FileUtils::create_dir(stats.path(), true)?;
            stats.check_dir()?;
            // Save version
            let ver_file = PathBuf::from_str(&stg_dir)?.join("version");
            LocalFile::write_toml(ver_file.as_path(), &version)?;
        }

        // SPDK: resolve bdev name from global SpdkEnv (one bdev per data_dir)
        #[cfg(feature = "spdk")]
        let bdev_name: Option<(String, i64, i64)> = if conf.storage_type == StorageType::SpdkDisk {
            let env = SpdkEnv::global().ok_or_else(|| {
                curvine_core_error::err_msg!(
                    "StorageType::SpdkDisk dir '{}' requires SPDK environment, but it is not initialized",
                    conf.path
                )
            })?;
            let slot_count = env.bdev_slot_count();
            if slot_count == 0 {
                return curvine_core_error::err_box!(
                    "StorageType::SpdkDisk dir '{}' but no bdevs discovered from SPDK targets",
                    conf.path
                );
            }
            let idx = version.dir_id as usize % slot_count;
            let bdev_info = env.active_bdev_at_slot(idx)?;
            let name = bdev_info.name.clone();
            let bdev_cap = bdev_info.size_bytes as i64;
            let bdev_block_size = bdev_info.block_size as i64;
            info!(
                "SPDK dir '{}' (dir_id={}) mapped to bdev slot {} '{}' (capacity={})",
                conf.path, version.dir_id, idx, name, bdev_cap
            );
            Some((name, bdev_cap, bdev_block_size))
        } else {
            None
        };
        #[cfg(not(feature = "spdk"))]
        let bdev_name: Option<(String, i64, i64)> = if conf.storage_type == StorageType::SpdkDisk {
            return curvine_core_error::err_box!(
                "StorageType::SpdkDisk is not available. Compile with --features spdk"
            );
        } else {
            None
        };
        let (bdev_name_str, bdev_capacity, bdev_block_size) = match bdev_name {
            Some((name, cap, bs)) => (Some(name), cap, bs),
            None => (None, 0, DEFAULT_BLOCK_ALIGN),
        };

        let state = DirState {
            bdev_name: bdev_name_str,
            bdev_capacity,
            offset_alloc: super::DirState::new_offset_alloc(
                conf.storage_type,
                bdev_capacity,
                bdev_block_size,
            ),
        };
        let dir = Self {
            version,
            stats,
            storage_type: conf.storage_type,
            conf_capacity: conf.capacity as i64,
            reserved_bytes: reserved_bytes as i64,
            free_ratio,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state: Arc::new(state),
            health: Arc::new(HealthTracker::new()),
            sample: Arc::new(SampleCache::default()),
        };

        Ok(dir)
    }

    pub fn from_str<T: AsRef<str>>(id: T, conf: T) -> CommonResult<Self> {
        let dir = WorkerDataDir::from_str(conf.as_ref())?;
        let version = StorageVersion::with_cluster(id);
        Self::new(version, dir, 0, 0.0)
    }

    pub fn from_dir<T: AsRef<str>>(id: T, dir: WorkerDataDir) -> CommonResult<Self> {
        let version = StorageVersion::with_cluster(id);
        Self::new(version, dir, 0, 0.0)
    }

    pub fn id(&self) -> u32 {
        self.version.dir_id
    }

    pub fn version(&self) -> &StorageVersion {
        &self.version
    }

    pub fn capacity(&self) -> i64 {
        // SPDK: use bdev capacity (fs returns 0 for non-existent path).
        let physical_capacity = if self.storage_type == StorageType::SpdkDisk {
            self.state.bdev_capacity
        } else {
            self.stats.total_space() as i64
        };
        self.capacity_from_physical(physical_capacity)
    }

    fn capacity_from_physical(&self, physical_capacity: i64) -> i64 {
        if self.conf_capacity <= 0 {
            physical_capacity
        } else {
            self.conf_capacity.min(physical_capacity)
        }
    }

    pub fn available(&self) -> i64 {
        // `free_ratio` is a strict Curvine write-admission floor: report only
        // bytes that may be reserved without knowingly crossing the protected
        // physical-space boundary. `0.0` disables the floor.
        //
        // The filesystem path samples total/available only once per call. Its
        // raw physical headroom also subtracts current temporary reservations,
        // because those bytes may not have reached statvfs yet; this prevents
        // concurrent opens from reusing the same headroom. Once temporary data
        // is materialized this is conservative (it can double-count it until
        // finalize/abort), which intentionally favors protection over write
        // availability.
        if self.storage_type == StorageType::SpdkDisk {
            let capacity = self.capacity();
            let free = capacity.saturating_sub(self.fs_used()).max(0);
            let accounting_available = free.saturating_sub(self.reserved_bytes);
            let floor_available = free.saturating_sub(self.free_ratio_floor(capacity));
            return accounting_available.min(floor_available).max(0);
        }

        let disk_total = self.stats.total_space() as i64;
        let disk_available = self.stats.available_space() as i64;
        let capacity = self.capacity_from_physical(disk_total);
        let accounting_available = capacity
            .saturating_sub(self.fs_used())
            .saturating_sub(self.reserved_bytes);
        let floor_available = if self.free_ratio > 0.0 {
            disk_available
                .saturating_sub(self.free_ratio_floor(disk_total))
                .saturating_sub(self.tmp_bytes.get())
        } else {
            disk_available
        };

        accounting_available.min(floor_available).max(0)
    }

    fn free_ratio_floor(&self, total: i64) -> i64 {
        if total <= 0 || self.free_ratio <= 0.0 || !self.free_ratio.is_finite() {
            return 0;
        }

        ((total as f64 * self.free_ratio.min(1.0)).ceil() as i64).min(total)
    }

    fn calculate_free_ratio(free: i64, total: i64) -> f64 {
        if total <= 0 {
            return 0.0;
        }

        (free.max(0) as f64 / total as f64).min(1.0)
    }

    /// Raw free-space ratio of the underlying device, ignoring `reserved_bytes`
    /// and the `free_ratio` guard. This is what the guard compares against and
    /// what the `disk_free_ratio` metric reports.
    ///
    /// - Filesystem dirs: `available_space / total_space` (OS statvfs; reflects
    ///   other applications consuming the shared device).
    /// - SPDK raw-device dirs: `(capacity - used) / capacity`, where `capacity`
    ///   is `min(conf_capacity, bdev_capacity)` and `used` is curvine's own
    ///   `fs_used()` accounting.
    pub fn raw_free_ratio(&self) -> f64 {
        if self.storage_type == StorageType::SpdkDisk {
            // No OS statvfs; ratio = (bdev capacity - curvine used) / bdev capacity.
            let total = self.capacity();
            let free = total.saturating_sub(self.fs_used());
            Self::calculate_free_ratio(free, total)
        } else {
            // OS statvfs: reflects other applications consuming the shared device.
            let total = self.stats.total_space() as i64;
            let free = self.stats.available_space() as i64;
            Self::calculate_free_ratio(free, total)
        }
    }

    pub fn non_fs_used(&self) -> i64 {
        // SPDK: no filesystem overhead
        if self.storage_type == StorageType::SpdkDisk {
            return 0;
        }
        let v = self.stats.used_space() as i64 - self.fs_used();
        if v <= 0 {
            0
        } else {
            v
        }
    }

    pub fn fs_used(&self) -> i64 {
        self.final_bytes.get() + self.tmp_bytes.get()
    }

    pub fn reserved_bytes(&self) -> i64 {
        self.reserved_bytes
    }

    pub fn base_path(&self) -> &Path {
        self.stats.path()
    }

    pub fn path_str(&self) -> &str {
        self.base_path().to_str().unwrap_or("")
    }

    pub fn storage_type(&self) -> StorageType {
        self.storage_type
    }

    pub fn device_id(&self) -> u64 {
        self.stats.device_id()
    }

    // Allocate reserved space for writing blocks.
    pub(super) fn reserve_space(&self, is_final: bool, size: i64) {
        if size <= 0 {
            return;
        }

        if is_final {
            self.final_bytes.add_and_get(size);
        } else {
            self.tmp_bytes.add_and_get(size);
        }
    }

    // Free up space.
    pub(super) fn release_space(&self, is_final: bool, size: i64) {
        if size <= 0 {
            return;
        }
        if is_final {
            loop {
                let old_bytes = self.final_bytes.get();
                let mut new_bytes = old_bytes - size;
                if new_bytes < 0 {
                    warn!(
                        "final bytes become negative {}, reset to 0, dir {:?}",
                        new_bytes,
                        self.stats.path()
                    );
                    new_bytes = 0;
                }

                let res = self.final_bytes.compare_and_set(old_bytes, new_bytes);
                if res {
                    break;
                }
            }
        } else {
            loop {
                let old_bytes = self.tmp_bytes.get();
                let mut new_bytes = old_bytes - size;
                if new_bytes < 0 {
                    warn!(
                        "tmp bytes become negative {}, reset to 0, dir {:?}",
                        new_bytes,
                        self.stats.path()
                    );
                    new_bytes = 0;
                }

                let res = self.tmp_bytes.compare_and_set(old_bytes, new_bytes);
                if res {
                    break;
                }
            }
        }
    }

    pub fn check_dir(&self) -> CommonResult<()> {
        if self.storage_type == StorageType::SpdkDisk {
            return Ok(());
        }
        Ok(self.stats.check_dir()?)
    }

    pub(super) fn allocation_size(&self, requested_bytes: i64) -> Option<i64> {
        if requested_bytes < 0 {
            return None;
        }
        if self.storage_type == StorageType::SpdkDisk {
            self.state.offset_alloc.allocation_size(requested_bytes)
        } else {
            Some(requested_bytes)
        }
    }

    pub fn can_allocate(&self, stg_type: StorageType, block_size: i64) -> bool {
        (stg_type == StorageType::Disk || stg_type == self.storage_type)
            && !self.is_failed()
            && self
                .allocation_size(block_size)
                .is_some_and(|required| self.available() >= required)
    }

    /// Whether this directory may take new writes.
    pub fn is_failed(&self) -> bool {
        !self.health.state().is_usable()
    }

    pub fn health_state(&self) -> HealthState {
        self.health.state()
    }

    /// Feed one probe result into the state machine and return the transition,
    /// if the health state changed. The caller owns logging and reporting.
    pub fn observe_health(
        &self,
        outcome: &ProbeOutcome,
        policy: &HealthPolicy,
        now_ms: i64,
    ) -> Option<HealthTransition> {
        self.health.observe(outcome, policy, now_ms)
    }

    /// Capacity numbers from the last probe (no syscall).
    pub fn sample(&self) -> DirSample {
        DirSample {
            capacity: self.sample.capacity.get(),
            available: self.sample.available.get(),
            fs_used: self.sample.fs_used.get(),
            non_fs_used: self.sample.non_fs_used.get(),
        }
    }

    pub fn fault_kind(&self) -> curvine_model::FaultKind {
        self.health.fault()
    }

    pub fn unhealthy_since_ms(&self) -> i64 {
        self.health.since_ms()
    }

    /// Refresh the cached capacity sample. Called by the probe loop only.
    fn refresh_sample(&self) {
        self.sample.capacity.set(self.capacity());
        self.sample.available.set(self.available());
        self.sample.fs_used.set(self.fs_used());
        self.sample.non_fs_used.set(self.non_fs_used());
    }

    /// Run the storage health probe: identity (path/type/mode/device) plus a
    /// mount sanity check. Does not write to the directory.
    ///
    /// Returns a structured outcome so callers never have to match on message
    /// text: "disk wrong" and "disk full" stay distinguishable.
    pub fn probe(&self) -> ProbeOutcome {
        // SPDK raw devices have no filesystem to inspect.
        if self.storage_type == StorageType::SpdkDisk {
            self.refresh_sample();
            return ProbeOutcome::Ok;
        }

        let path = self.stats.path();
        let meta = match path.metadata() {
            Ok(meta) => meta,
            Err(e) => {
                return ProbeOutcome::failed(
                    FaultKind::from_io_error(&e),
                    format!("metadata {}: {}", path.display(), e),
                )
            }
        };

        if !meta.is_dir() {
            return ProbeOutcome::failed(
                FaultKind::NotDir,
                format!("{} is not a directory", path.display()),
            );
        }

        if meta.permissions().readonly() {
            return ProbeOutcome::failed(
                FaultKind::ReadOnly,
                format!("{} is read only", path.display()),
            );
        }

        if let Err(e) = self.stats.check_device() {
            return ProbeOutcome::failed(
                FaultKind::DeviceChanged,
                format!("{}: {}", path.display(), e),
            );
        }

        // A dead mount reports zero total space instead of failing stat().
        if self.stats.total_space() == 0 {
            return ProbeOutcome::failed(
                FaultKind::DeviceGone,
                format!("statvfs {} returned 0 total space", path.display()),
            );
        }

        self.refresh_sample();
        ProbeOutcome::Ok
    }
}

impl Debug for VfsDir {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VfsDir")
            .field("id", &self.id())
            .field("storage_type", &self.storage_type)
            .field(
                "capacity",
                &ByteUnit::byte_to_string(self.capacity() as u64),
            )
            .field(
                "available",
                &ByteUnit::byte_to_string(self.available() as u64),
            )
            .field(
                "final_bytes",
                &ByteUnit::byte_to_string(self.final_bytes.get() as u64),
            )
            .field(
                "tmp_bytes",
                &ByteUnit::byte_to_string(self.tmp_bytes.get() as u64),
            )
            .finish()
    }
}

#[cfg(all(test, feature = "spdk"))]
mod test {
    use super::*;
    use crate::vfs_dir::VfsDir;
    use crate::{BdevLayout, BlockLayout, FileLayout, StorageVersion, DEFAULT_BLOCK_ALIGN};
    use crate::{BlockMeta, BlockState};
    use curvine_config::WorkerDataDir;
    use curvine_core_error::CommonResult;
    use curvine_io::LocalFile;
    use curvine_model::{ExtendedBlock, StorageType};
    use curvine_runtime::common::{ByteUnit, FileUtils};
    use curvine_runtime::sync::AtomicLong;
    use curvine_sys::FsStats;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn spdk_state(_p: &str, cap: i64) -> Arc<DirState> {
        Arc::new(DirState {
            bdev_name: Some("nvme0".into()),
            bdev_capacity: cap,
            offset_alloc: DirState::new_offset_alloc(StorageType::SpdkDisk, cap, 4096),
        })
    }

    fn spdk_dir(p: &str, st: Arc<DirState>, conf: i64) -> VfsDir {
        VfsDir {
            version: StorageVersion::with_cluster("t"),
            stats: FsStats::new(p),
            storage_type: StorageType::SpdkDisk,
            conf_capacity: conf,
            reserved_bytes: 0,
            free_ratio: 0.0,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state: st,
            health: Arc::new(HealthTracker::new()),
            sample: Arc::new(SampleCache::default()),
        }
    }

    fn local_state(_p: &str) -> Arc<DirState> {
        Arc::new(DirState {
            bdev_name: None,
            bdev_capacity: 0,
            offset_alloc: DirState::new_offset_alloc(StorageType::Ssd, 0, DEFAULT_BLOCK_ALIGN),
        })
    }

    fn local_dir(p: &str, st: Arc<DirState>) -> VfsDir {
        VfsDir {
            version: StorageVersion::with_cluster("t"),
            stats: FsStats::new(p),
            storage_type: StorageType::Ssd,
            conf_capacity: 1 << 30,
            reserved_bytes: 0,
            free_ratio: 0.0,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state: st,
            health: Arc::new(HealthTracker::new()),
            sample: Arc::new(SampleCache::default()),
        }
    }

    #[test]
    fn dir() -> CommonResult<()> {
        let conf = "[SSD:100MB]../testing";
        let version = StorageVersion::with_cluster("vfs-test");
        let conf = WorkerDataDir::from_str(conf)?;
        let stg_dir = conf.storage_path("vfs-test");
        FileUtils::delete_path(stg_dir, true)?;

        let dir = VfsDir::new(version, conf, 0, 0.0)?;
        println!("dir.path_str() = {}", dir.path_str());
        assert_eq!(dir.available(), 100 * ByteUnit::MB as i64);

        // add tmp block
        let block = ExtendedBlock::with_size_str(1122, "10MB", StorageType::Mem)?;
        let layout = FileLayout;
        let tmp = layout.allocate(&dir, &block)?;
        dir.reserve_space(false, block.len);

        let tmp_file = FileLayout::block_path(&dir, &tmp)?;
        LocalFile::write_string(
            tmp_file.as_path(),
            "1".repeat(ByteUnit::MB as usize).as_str(),
            true,
        )?;
        println!(
            "tmp_file = {:?}, available = {}",
            tmp_file.as_path(),
            ByteUnit::byte_to_string(dir.available() as u64)
        );
        assert!(tmp_file.exists());
        assert_eq!(dir.available(), 90 * ByteUnit::MB as i64);

        // commit block
        // commit block (committed_len = 1MB, the actual data written)
        let final1 = layout.finalize(&dir, &tmp, ByteUnit::MB as i64)?;
        let file = FileLayout::block_path(&dir, &final1)?;
        dir.release_space(false, block.len);
        dir.reserve_space(true, final1.len);
        println!(
            "final_file = {:?}, available = {}",
            file,
            ByteUnit::byte_to_string(dir.available() as u64)
        );
        assert!(file.exists());
        assert_eq!(dir.available(), 99 * ByteUnit::MB as i64);

        Ok(())
    }
    // SPDK: finalize_block skips path.metadata()
    #[test]
    fn finalize_block_spdk() -> CommonResult<()> {
        let st = spdk_state("/spdk/final", 1 << 30);
        let dir = spdk_dir("/spdk/final", st, 1 << 30);
        let meta = BlockMeta {
            id: 1,
            len: 4096,
            state: BlockState::Writing,
            dir_id: dir.id(),
            storage_type: StorageType::SpdkDisk,
            actual_len: 4096,
            bdev_offset: 0,
        };
        let layout = BdevLayout::new(None);
        let r = layout.finalize(&dir, &meta, 2048)?;
        assert_eq!(r.len, 2048);
        Ok(())
    }
    // SPDK: check_dir skips path.metadata()
    #[test]
    fn check_dir_spdk() -> CommonResult<()> {
        let st = spdk_state("/spdk/check", 1 << 30);
        let dir = spdk_dir("/spdk/check", st, 1 << 30);
        assert!(dir.check_dir().is_ok() && !dir.is_failed());
        Ok(())
    }
    // SPDK: capacity/available use bdev size
    #[test]
    fn capacity_spdk() -> CommonResult<()> {
        let cap = 64 << 20;
        let st = spdk_state("/spdk/cap", cap);
        let dir = spdk_dir("/spdk/cap", st, 0);
        assert_eq!(dir.capacity(), cap);
        dir.reserve_space(false, 1 << 20);
        assert_eq!(dir.available(), cap - (1 << 20));
        Ok(())
    }
    // SPDK: conf_capacity caps bdev capacity
    #[test]
    fn capacity_spdk_capped() {
        let st = spdk_state("/spdk/cap2", 64 << 20);
        let dir = spdk_dir("/spdk/cap2", st, 32 << 20);
        assert_eq!(dir.capacity(), 32 << 20);
    }
    // Local: check_dir fails for non-existent path
    #[test]
    fn check_local_fails() {
        let st = local_state("/local/missing");
        let dir = local_dir("/local/missing", st);
        assert!(dir.check_dir().is_err());
    }
}
