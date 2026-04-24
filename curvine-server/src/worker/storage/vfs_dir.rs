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

use crate::worker::block::{BlockMeta, BlockState};
use crate::worker::storage::{
    DirState, StorageVersion, ACTIVE_DIR, DEFAULT_BLOCK_ALIGN, STAGING_DIR,
};
use curvine_common::conf::WorkerDataDir;
use curvine_common::state::{ExtendedBlock, StorageType};
use log::*;
use orpc::common::{ByteUnit, FileUtils};
#[cfg(feature = "spdk")]
use orpc::io::spdk_env::SpdkEnv;
use orpc::io::LocalFile;
use orpc::sync::AtomicLong;
use orpc::sys::FsStats;
use orpc::{try_err, CommonResult};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
pub struct VfsDir {
    pub(crate) version: StorageVersion,
    pub(crate) stats: FsStats,
    pub(crate) active_dir: PathBuf,
    pub(crate) staging_dir: PathBuf,
    pub(crate) storage_type: StorageType,
    pub(crate) conf_capacity: i64,
    pub(crate) reserved_bytes: i64,
    pub(crate) final_bytes: AtomicLong,
    pub(crate) tmp_bytes: AtomicLong,
    pub(crate) state: Arc<DirState>,
    pub(crate) check_failed: Arc<AtomicBool>,
}

impl VfsDir {
    pub fn new(
        version: StorageVersion,
        conf: WorkerDataDir,
        reserved_bytes: u64,
    ) -> CommonResult<Self> {
        let stg_dir = if version.cluster_id.is_empty() {
            conf.path.clone()
        } else {
            format!("{}/{}", conf.path, version.cluster_id)
        };
        let stats = FsStats::new(&stg_dir);

        let active_dir = stats.path().join(ACTIVE_DIR);
        let staging_dir = stats.path().join(STAGING_DIR);

        // SPDK: skip filesystem (no local dir, version file, or filesystem checks).
        if conf.storage_type != StorageType::SpdkDisk {
            FileUtils::create_dir(&active_dir, true)?;
            FileUtils::create_dir(&staging_dir, true)?;
            stats.check_dir()?;
            // Save version
            let ver_file = PathBuf::from_str(&stg_dir)?.join("version");
            LocalFile::write_toml(ver_file.as_path(), &version)?;
        }

        // SPDK: resolve bdev name from global SpdkEnv (one bdev per data_dir)
        #[cfg(feature = "spdk")]
        let bdev_name: Option<(String, i64)> = if conf.storage_type == StorageType::SpdkDisk {
            use orpc::io::spdk_env::SpdkEnv;
            let env = SpdkEnv::global().ok_or_else(|| {
                orpc::err_msg!(
                    "StorageType::SpdkDisk dir '{}' requires SPDK environment, but it is not initialized",
                    conf.path
                )
            })?;
            let names = env.bdev_names();
            if names.is_empty() {
                return orpc::err_box!(
                    "StorageType::SpdkDisk dir '{}' but no bdevs discovered from SPDK targets",
                    conf.path
                );
            }
            let idx = version.dir_id as usize % names.len();
            let name = names[idx].clone();
            let bdev_info = env.get_bdev(&name);
            let bdev_cap = bdev_info.map(|b| b.size_bytes as i64).unwrap_or(0);
            info!(
                "SPDK dir '{}' (dir_id={}) mapped to bdev '{}' (capacity={})",
                conf.path, version.dir_id, name, bdev_cap
            );
            Some((name, bdev_cap))
        } else {
            None
        };
        #[cfg(not(feature = "spdk"))]
        let bdev_name: Option<(String, i64)> = if conf.storage_type == StorageType::SpdkDisk {
            return orpc::err_box!(
                "StorageType::SpdkDisk is not available. Compile with --features spdk"
            );
        } else {
            None
        };
        let (bdev_name_str, bdev_capacity, bdev_block_size) = match bdev_name {
            Some((name, cap)) => {
                #[cfg(feature = "spdk")]
                let bs = SpdkEnv::global()
                    .and_then(|env: &orpc::io::spdk_env::SpdkEnv| env.get_bdev(&name))
                    .map(|b| b.block_size as i64)
                    .unwrap_or(DEFAULT_BLOCK_ALIGN);
                #[cfg(not(feature = "spdk"))]
                let bs = DEFAULT_BLOCK_ALIGN;
                (Some(name), cap, bs)
            }
            None => (None, 0, DEFAULT_BLOCK_ALIGN),
        };

        let state = DirState {
            dir_id: version.dir_id,
            base_path: stats.path().to_path_buf(),
            storage_type: conf.storage_type,
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
            active_dir,
            staging_dir,
            storage_type: conf.storage_type,
            conf_capacity: conf.capacity as i64,
            reserved_bytes: reserved_bytes as i64,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state: Arc::new(state),
            check_failed: Arc::new(AtomicBool::new(false)),
        };

        Ok(dir)
    }

    pub fn from_str<T: AsRef<str>>(id: T, conf: T) -> CommonResult<Self> {
        let dir = WorkerDataDir::from_str(conf.as_ref())?;
        let version = StorageVersion::with_cluster(id);
        Self::new(version, dir, 0)
    }

    pub fn from_dir<T: AsRef<str>>(id: T, dir: WorkerDataDir) -> CommonResult<Self> {
        let version = StorageVersion::with_cluster(id);
        Self::new(version, dir, 0)
    }

    pub fn id(&self) -> u32 {
        self.version.dir_id
    }

    pub fn version(&self) -> &StorageVersion {
        &self.version
    }

    pub fn capacity(&self) -> i64 {
        // SPDK: use bdev capacity (fs returns 0 for non-existent path)
        if self.storage_type == StorageType::SpdkDisk {
            let bdev_cap = self.state.bdev_capacity;
            return if self.conf_capacity <= 0 {
                bdev_cap
            } else {
                self.conf_capacity.min(bdev_cap)
            };
        }

        let disk_space = self.stats.total_space() as i64;

        if self.conf_capacity <= 0 {
            disk_space
        } else {
            self.conf_capacity.min(disk_space)
        }
    }

    pub fn available(&self) -> i64 {
        // SPDK: available = capacity - used - reserved (fs returns 0)
        if self.storage_type == StorageType::SpdkDisk {
            let capacity = self.capacity();
            let fs_used = self.fs_used();
            let reserved_bytes = self.reserved_bytes;
            return 0.max(capacity - fs_used - reserved_bytes);
        }

        let disk_available = self.stats.available_space() as i64;

        let capacity = self.capacity();
        let fs_used = self.fs_used();
        let reserved_bytes = self.reserved_bytes;
        let calculated_available = capacity - fs_used - reserved_bytes;

        0.max(calculated_available.min(disk_available))
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
    pub fn reserve_space(&self, is_final: bool, size: i64) {
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
    pub fn release_space(&self, is_final: bool, size: i64) {
        if size <= 0 {
            return;
        }
        if is_final {
            loop {
                let old_bytes = self.final_bytes.get();
                let mut new_bytes = old_bytes - size;
                if new_bytes < 0 {
                    warn!(
                        "tmp bytes become negative {}, reset to 0, dir {:?}",
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

    pub fn create_block(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let mut meta = BlockMeta::with_tmp(block, self);
        if self.storage_type == StorageType::SpdkDisk {
            // Allocate a non-overlapping offset range on the bdev for this block.
            let offset = self
                .state
                .offset_alloc
                .allocate(block.id, block.len)
                .map_err(|e| {
                    orpc::err_msg!(format!(
                        "Failed to allocate bdev offset for block {}: {}",
                        block.id, e
                    ))
                })?;
            meta.bdev_offset = offset;
        } else {
            let file = meta.get_block_path()?;
            let _ = try_err!(File::create(file));
        }
        Ok(meta)
    }

    pub fn finalize_block(&self, meta: &BlockMeta, committed_len: i64) -> CommonResult<BlockMeta> {
        if self.storage_type == StorageType::SpdkDisk {
            // SPDK: skip metadata stat (use in-memory BlockMeta)
            return Ok(BlockMeta::with_final_spdk(meta, committed_len));
        }
        let final_meta = BlockMeta::with_final(meta)?;
        Ok(final_meta)
    }

    // Scan all blocks in the directory
    pub fn scan_blocks(&self) -> CommonResult<Vec<BlockMeta>> {
        // SPDK bdevs don't store blocks on the filesystem — nothing to scan
        if self.storage_type == StorageType::SpdkDisk {
            // SPDK: loaded via scan_spdk_blocks() in VfsDataset
            return Ok(vec![]);
        }
        let active_dir = FileUtils::list_files(&self.active_dir, true)?;
        let staging_dir = FileUtils::list_files(&self.staging_dir, true)?;

        let mut vec = vec![];
        for file in active_dir {
            if let Ok(v) = BlockMeta::from_file(&file, BlockState::Finalized, self) {
                vec.push(v);
            }
        }

        for file in staging_dir {
            if let Ok(v) = BlockMeta::from_file(&file, BlockState::Recovering, self) {
                vec.push(v);
            }
        }

        Ok(vec)
    }

    /// Restore SPDK block metadata from RocksDB on restart.
    /// Called during dataset initialization for SPDK directories.
    /// - Loads all block records from RocksDB
    /// - Filters to this directory's dir_id
    /// - Restores the offset allocator state
    /// - Returns blocks as BlockMeta (non-finalized blocks become Recovering state)
    pub fn scan_spdk_blocks(&self, store: &super::SpdkMetaStore) -> CommonResult<Vec<BlockMeta>> {
        let all_records = store.scan_all()?;
        let all_records_len = all_records.len();
        // Filter to only records belonging to this directory.
        let records: Vec<_> = all_records
            .into_iter()
            .filter(|r| r.dir_id == self.id())
            .collect();

        // Restore the offset allocator from the saved entries.
        let alloc_entries: Vec<(i64, i64, i64)> = records
            .iter()
            .map(|r| (r.block_id, r.offset, r.size))
            .collect();
        self.state.offset_alloc.restore(&alloc_entries);

        // Reconstruct BlockMeta for each saved block.
        let mut blocks = Vec::with_capacity(records.len());
        for rec in &records {
            let state = if rec.finalized {
                BlockState::Finalized
            } else {
                // Non-finalized blocks found after restart are treated as
                // recovering — the write was interrupted.
                BlockState::Recovering
            };
            let block = BlockMeta {
                id: rec.block_id,
                len: rec.len,
                state,
                dir: self.state.clone(),
                actual_len: rec.len,
                bdev_offset: rec.offset,
            };
            blocks.push(block);
        }

        info!(
            "Restored {} SPDK blocks for dir {} from RocksDB (skipped {} from other dirs)",
            blocks.len(),
            self.id(),
            all_records_len - blocks.len(),
        );
        Ok(blocks)
    }

    pub fn check_dir(&self) -> CommonResult<()> {
        if self.storage_type == StorageType::SpdkDisk {
            return Ok(());
        }
        self.stats.check_dir()
    }

    pub fn can_allocate(&self, stg_type: StorageType, block_size: i64) -> bool {
        (stg_type == StorageType::Disk || stg_type == self.storage_type)
            && !self.is_failed()
            && self.available() > block_size
    }

    pub fn is_failed(&self) -> bool {
        self.check_failed.load(Ordering::SeqCst)
    }

    pub fn set_failed(&self) {
        self.check_failed.store(true, Ordering::SeqCst);
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
    use crate::worker::storage::vfs_dir::VfsDir;
    use crate::worker::storage::StorageVersion;
    use crate::worker::storage::DEFAULT_BLOCK_ALIGN;
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::{ExtendedBlock, StorageType};
    use orpc::common::{ByteUnit, FileUtils};
    use orpc::io::LocalFile;
    use orpc::sync::AtomicLong;
    use orpc::sys::FsStats;
    use orpc::CommonResult;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn spdk_state(p: &str, cap: i64) -> Arc<DirState> {
        Arc::new(DirState {
            dir_id: 1,
            base_path: PathBuf::from(p),
            storage_type: StorageType::SpdkDisk,
            bdev_name: Some("nvme0".into()),
            bdev_capacity: cap,
            offset_alloc: DirState::new_offset_alloc(StorageType::SpdkDisk, cap, 4096),
        })
    }

    fn spdk_dir(p: &str, st: Arc<DirState>, conf: i64) -> VfsDir {
        VfsDir {
            version: StorageVersion::with_cluster("t"),
            stats: FsStats::new(p),
            active_dir: PathBuf::from(p).join("a"),
            staging_dir: PathBuf::from(p).join("s"),
            storage_type: StorageType::SpdkDisk,
            conf_capacity: conf,
            reserved_bytes: 0,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state: st,
            check_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn local_state(p: &str) -> Arc<DirState> {
        Arc::new(DirState {
            dir_id: 1,
            base_path: PathBuf::from(p),
            storage_type: StorageType::Ssd,
            bdev_name: None,
            bdev_capacity: 0,
            offset_alloc: DirState::new_offset_alloc(StorageType::Ssd, 0, DEFAULT_BLOCK_ALIGN),
        })
    }

    fn local_dir(p: &str, st: Arc<DirState>) -> VfsDir {
        VfsDir {
            version: StorageVersion::with_cluster("t"),
            stats: FsStats::new(p),
            active_dir: PathBuf::from(p).join("a"),
            staging_dir: PathBuf::from(p).join("s"),
            storage_type: StorageType::Ssd,
            conf_capacity: 1 << 30,
            reserved_bytes: 0,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state: st,
            check_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn dir() -> CommonResult<()> {
        let conf = "[SSD:100MB]../testing";
        let version = StorageVersion::with_cluster("vfs-test");
        let conf = WorkerDataDir::from_str(conf)?;
        let stg_dir = conf.storage_path("vfs-test");
        FileUtils::delete_path(stg_dir, true)?;

        let dir = VfsDir::new(version, conf, 0)?;
        println!("dir.path_str() = {}", dir.path_str());
        assert_eq!(dir.available(), 100 * ByteUnit::MB as i64);

        // add tmp block
        let block = ExtendedBlock::with_size_str(1122, "10MB", StorageType::Mem)?;
        let tmp = dir.create_block(&block)?;
        dir.reserve_space(false, block.len);

        let tmp_file = tmp.get_block_path()?;
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
        let final1 = dir.finalize_block(&tmp, ByteUnit::MB as i64)?;
        let file = final1.get_block_path()?;
        dir.release_space(false, block.len);
        dir.reserve_space(false, final1.len);
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
        use crate::worker::block::{BlockMeta, BlockState};
        let st = spdk_state("/spdk/final", 1 << 30);
        let dir = spdk_dir("/spdk/final", st.clone(), 1 << 30);
        let meta = BlockMeta {
            id: 1,
            len: 4096,
            state: BlockState::Writing,
            dir: st,
            actual_len: 4096,
            bdev_offset: 0,
        };
        let r = dir.finalize_block(&meta, 2048)?;
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
