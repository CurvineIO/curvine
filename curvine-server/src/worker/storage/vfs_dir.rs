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
use crate::worker::storage::{DirState, StorageVersion, ACTIVE_DIR, STAGING_DIR};
use curvine_common::conf::WorkerDataDir;
use curvine_common::state::{ExtendedBlock, StorageType};
use log::*;
use orpc::common::{ByteUnit, FileUtils};
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

// Magic bytes to identify container files
// const CONTAINER_MAGIC_BYTES: &[u8] = b"CMB";

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

        // Initialize the directory.
        let active_dir = stats.path().join(ACTIVE_DIR);
        let staging_dir = stats.path().join(STAGING_DIR);
        FileUtils::create_dir(&active_dir, true)?;
        FileUtils::create_dir(&staging_dir, true)?;

        stats.check_dir()?;

        // Save version
        let ver_file = PathBuf::from_str(&stg_dir)?.join("version");
        LocalFile::write_toml(ver_file.as_path(), &version)?;

        let state = DirState {
            dir_id: version.dir_id,
            base_path: stats.path().to_path_buf(),
            storage_type: conf.storage_type,
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
        let disk_space = self.stats.total_space() as i64;

        if self.conf_capacity <= 0 {
            disk_space
        } else {
            self.conf_capacity.min(disk_space)
        }
    }

    pub fn available(&self) -> i64 {
        let disk_available = self.stats.available_space() as i64;

        let capacity = self.capacity();
        let fs_used = self.fs_used();
        let reserved_bytes = self.reserved_bytes;
        let calculated_available = capacity - fs_used - reserved_bytes;

        0.max(calculated_available.min(disk_available))
    }

    pub fn non_fs_used(&self) -> i64 {
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
        let meta = BlockMeta::with_tmp(block, self);
        let file = meta.get_block_path()?;
        let _ = try_err!(File::create(file));
        Ok(meta)
    }

    pub fn finalize_block(&self, meta: &BlockMeta) -> CommonResult<BlockMeta> {
        let final_meta = BlockMeta::with_final(meta)?;
        Ok(final_meta)
    }

    // Scan all blocks in the directory
    pub fn scan_blocks(&self) -> CommonResult<Vec<BlockMeta>> {
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

    pub fn check_dir(&self) -> CommonResult<()> {
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

#[cfg(test)]
mod test {
    use crate::worker::storage::vfs_dir::VfsDir;
    use crate::worker::storage::StorageVersion;
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::{ExtendedBlock, StorageType};
    use orpc::common::{ByteUnit, FileUtils};
    use orpc::io::LocalFile;
    use orpc::CommonResult;

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
        let final1 = dir.finalize_block(&tmp)?;
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
}
