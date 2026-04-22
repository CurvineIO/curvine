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
use crate::worker::storage::{Dataset, DirList, SpdkMetaStore, StorageVersion, VfsDir};
use curvine_common::conf::{ClusterConf, WorkerDataDir};
use curvine_common::state::{ExtendedBlock, StorageType};
use indexmap::map::Values;
use log::{info, warn};
use orpc::common::{ByteUnit, FileUtils, LocalTime, TimeSpent};
use orpc::{err_box, try_err, CommonResult};
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct VfsDataset {
    cluster_id: String,
    worker_id: u32,
    ctime: u64,
    dir_list: DirList,
    pub(crate) block_map: HashMap<i64, BlockMeta>,
    num_blocks_to_delete: AtomicUsize,
    /// RocksDB-backed store for SPDK block metadata. `None` when no SPDK
    /// directories are configured.
    spdk_meta: Option<Arc<SpdkMetaStore>>,
}

impl VfsDataset {
    fn new(cluster_id: &str, dir_list: DirList, spdk_meta: Option<Arc<SpdkMetaStore>>) -> Self {
        let worker_id = match dir_list.get_dir_index(0) {
            None => 0,
            Some(v) => v.version().worker_id,
        };

        let mut ds = Self {
            cluster_id: cluster_id.to_string(),
            worker_id,
            ctime: LocalTime::mills(),
            dir_list,
            block_map: HashMap::new(),
            num_blocks_to_delete: AtomicUsize::new(0),
            spdk_meta,
        };
        ds.initialize();
        ds
    }

    pub fn from_conf(cluster_id: &str, conf: &ClusterConf) -> CommonResult<Self> {
        let mut dir_list = DirList::new(vec![]);
        let dir_reserved = ByteUnit::from_str(&conf.worker.dir_reserved)?.as_byte();

        let mut worker_id: Option<u32> = None;
        let mut has_spdk = false;
        for s in &conf.worker.data_dir {
            let data_dir = WorkerDataDir::from_str(s)?;
            let storage_path = data_dir.storage_path(&conf.cluster_id);
            if conf.format_worker && FileUtils::exists(&storage_path) {
                FileUtils::delete_path(&storage_path, true)?;
                info!("Delete(format) data dir {}", storage_path);
            }

            if data_dir.storage_type == StorageType::Spdk {
                has_spdk = true;
            }

            let mut version = StorageVersion::read_version(&storage_path, &conf.cluster_id)?;
            match worker_id {
                None => {
                    worker_id = Some(version.worker_id);
                }

                Some(v) => version.worker_id = v,
            }

            let vfs_dir = VfsDir::new(version, data_dir, dir_reserved)?;
            dir_list.add_dir(vfs_dir);
        }

        if has_spdk {
            let mut seen: HashMap<String, u32> = HashMap::new();
            for dir in dir_list.dir_iter() {
                if dir.storage_type() == StorageType::Spdk {
                    if let Some(bdev) = dir.state.bdev_name.as_ref() {
                        if let Some(prev_id) = seen.insert(bdev.clone(), dir.id()) {
                            return orpc::err_box!(
                                "SPDK dirs {} and {} both map to bdev '{}' (dir_id collision).",
                                prev_id,
                                dir.id(),
                                bdev
                            );
                        }
                    }
                }
            }
        }
        // Open the shared RocksDB for SPDK metadata if any SPDK dirs exist.
        let spdk_meta = if has_spdk {
            // Use the first data_dir's storage path as the RocksDB location.
            let first_path = if let Some(s) = conf.worker.data_dir.first() {
                let dd = WorkerDataDir::from_str(s)?;
                dd.storage_path(&conf.cluster_id)
            } else {
                "spdk_meta".to_string()
            };
            let db_dir = format!("{}/spdk_meta", first_path);
            let store = SpdkMetaStore::open(&db_dir, conf.format_worker)?;
            Some(Arc::new(store))
        } else {
            None
        };

        Ok(Self::new(cluster_id, dir_list, spdk_meta))
    }

    // Initialize.
    // 1. Scan all blocks in the directory (filesystem or RocksDB for SPDK)
    // 2. Block is added to block_map.
    // 3. Update capacity usage.
    // TODO: pre-filter to avoid scan_all() per SPDK dir
    fn initialize(&mut self) {
        let spent = TimeSpent::new();
        for dir in self.dir_list.dir_iter() {
            let blocks = if dir.storage_type() == StorageType::Spdk {
                // SPDK dirs: restore from RocksDB
                match &self.spdk_meta {
                    Some(store) => dir.scan_spdk_blocks(store).unwrap(),
                    None => {
                        warn!("SPDK dir {} has no meta store — starting empty", dir.id());
                        vec![]
                    }
                }
            } else {
                dir.scan_blocks().unwrap()
            };
            for block in blocks {
                dir.reserve_space(true, block.len);
                self.block_map.insert(block.id, block);
            }
        }
        info!(
            "Dataset initialize, used {} ms, total block {}",
            spent.used_ms(),
            self.block_map.len()
        );
    }

    pub fn find_dir(&self, id: u32) -> CommonResult<&VfsDir> {
        match self.dir_list.get_dir(id) {
            None => {
                err_box!("No storage directory found: {:?}", id)
            }
            Some(v) => Ok(v),
        }
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_id
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn ctime(&self) -> u64 {
        self.ctime
    }

    pub fn dir_iter(&self) -> Values<'_, u32, VfsDir> {
        self.dir_list.dir_iter()
    }

    pub fn all_blocks(&self) -> Vec<BlockMeta> {
        let mut vec = vec![];
        for meta in self.block_map.values() {
            vec.push(meta.clone());
        }
        vec
    }
    /// Persist one SPDK block's metadata to RocksDB. No-op if no SPDK store.
    fn spdk_put(&self, meta: &BlockMeta) {
        if meta.storage_type() != StorageType::Spdk {
            return;
        }
        if let Some(ref store) = self.spdk_meta {
            let alloc_size = meta
                .dir
                .offset_alloc
                .get_entry(meta.id)
                .map_or(meta.actual_len, |(_, sz)| sz);
            if let Err(e) = store.put(
                meta.id,
                meta.dir_id(),
                meta.bdev_offset,
                alloc_size,
                meta.len,
                meta.is_final(),
            ) {
                warn!("SpdkMetaStore put failed for block {}: {}", meta.id, e);
            }
        }
    }

    /// Remove one SPDK block's metadata from RocksDB. No-op if no SPDK store.
    pub(crate) fn spdk_delete(&self, block_id: i64, storage_type: StorageType) {
        if storage_type != StorageType::Spdk {
            return;
        }
        if let Some(ref store) = self.spdk_meta {
            if let Err(e) = store.delete(block_id) {
                warn!("SpdkMetaStore delete failed for block {}: {}", block_id, e);
            }
        }
    }
}

impl Dataset for VfsDataset {
    fn capacity(&self) -> i64 {
        self.dir_list.capacity()
    }

    fn available(&self) -> i64 {
        self.dir_list.available()
    }

    fn fs_used(&self) -> i64 {
        self.dir_list.fs_used()
    }

    fn num_blocks(&self) -> usize {
        self.block_map.len()
    }

    fn num_blocks_to_delete(&self) -> usize {
        self.num_blocks_to_delete.load(Ordering::Relaxed)
    }

    fn increment_blocks_to_delete(&self) {
        self.num_blocks_to_delete.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_blocks_to_delete(&self) {
        self.num_blocks_to_delete.fetch_sub(1, Ordering::Relaxed);
    }

    fn get_block(&self, id: i64) -> Option<&BlockMeta> {
        self.block_map.get(&id)
    }

    fn open_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        match self.block_map.get(&block.id) {
            Some(meta) => {
                if meta.is_active() {
                    let dir = self.find_dir(meta.dir_id())?;
                    // Create a new block meta, where block.len represents the block size
                    let mut new_meta = BlockMeta::new(meta.id, block.len, dir);
                    new_meta.bdev_offset = meta.bdev_offset; // preserve allocated offset

                    dir.release_space(meta.is_final(), meta.actual_len);
                    dir.reserve_space(false, new_meta.len);
                    self.block_map.insert(new_meta.id(), new_meta.clone());
                    self.spdk_put(&new_meta);

                    Ok(new_meta)
                } else {
                    err_box!(
                        "Block {} is in recovering state and cannot be open in worker_id: {}",
                        block.id,
                        self.worker_id
                    )
                }
            }

            None => {
                let dir = self.dir_list.choose_dir(block)?;
                let meta = dir.create_block(block)?;

                self.block_map.insert(meta.id(), meta.clone());
                dir.reserve_space(false, block.len);
                self.spdk_put(&meta);

                Ok(meta)
            }
        }
    }

    fn finalize_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let meta = self.get_block_check(block.id)?;
        if meta.state() != &BlockState::Writing {
            return err_box!(
                "block {} status incorrect, expected {:?}, actual: {:?}",
                meta.id(),
                BlockState::Writing,
                meta.state()
            );
        }

        let dir = self.find_dir(meta.dir_id())?;
        let final_meta = dir.finalize_block(meta, block.len)?;
        if block.len != final_meta.len() {
            return err_box!(
                "Block {} length mismatch, expected: {}, actual: {}",
                meta.id(),
                block.len,
                final_meta.len()
            );
        }

        dir.release_space(false, meta.actual_len);
        dir.reserve_space(true, final_meta.actual_len);
        self.block_map.insert(final_meta.id(), final_meta.clone());
        self.spdk_put(&final_meta);

        Ok(final_meta)
    }

    fn abort_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        let meta = match self.block_map.remove(&block.id) {
            None => return err_box!("block {} not exists", block.id),
            Some(v) => v,
        };

        let dir_id = meta.dir_id();
        let is_spdk = meta.storage_type() == StorageType::Spdk;
        // SPDK: no filesystem file - nothing to delete.
        if !is_spdk {
            let file = meta.get_block_path()?;
            try_err!(fs::remove_file(file));
        }
        let dir = self.find_dir(dir_id)?;

        if is_spdk {
            self.spdk_delete(block.id, meta.storage_type());
            dir.state.offset_alloc.free(block.id);
        }
        dir.release_space(meta.is_final(), meta.actual_len);
        Ok(())
    }

    fn remove_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        self.abort_block(block)
    }
}

#[cfg(all(test, feature = "spdk"))]
mod test {
    use crate::worker::block::BlockState;
    use crate::worker::storage::{Dataset, VfsDataset};
    use curvine_common::conf::{ClusterConf, WorkerConf};
    use curvine_common::state::ExtendedBlock;
    use orpc::CommonResult;

    fn create_data_set(format: bool, dir: &str) -> VfsDataset {
        let conf = ClusterConf {
            format_worker: format,
            worker: WorkerConf {
                dir_reserved: "0".to_string(),
                data_dir: vec![
                    format!("[MEM:100B]../testing/dataset-{}/d1", dir),
                    format!("[SSD:200B]../testing/dataset-{}/d2", dir),
                    format!("[SSD:200B]../testing/dataset-{}/d3", dir),
                ],
                io_slow_threshold: "300ms".to_string(),
                ..WorkerConf::default()
            },
            ..Default::default()
        };
        VfsDataset::from_conf("test", &conf).unwrap()
    }

    #[test]
    fn sample() -> CommonResult<()> {
        let ds = create_data_set(true, "sample");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let meta = ds.open_block(&block)?;
        assert_eq!(ds.available(), 400);
        meta.write_test_data("50B")?;
        block.len = 50;
        ds.finalize_block(&block)?;
        assert_eq!(ds.available(), 450);
        Ok(())
    }
    #[test]
    fn append() -> CommonResult<()> {
        let ds = create_data_set(true, "append");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let meta = ds.open_block(&block)?;
        meta.write_test_data("50B")?;
        block.len = 50;
        ds.finalize_block(&block)?;
        block.len = 100;
        let meta2 = ds.open_block(&block)?;
        meta2.write_test_data("20B")?;
        ds.finalize_block(&block)?;
        assert_eq!(ds.available(), 430);
        Ok(())
    }
    #[test]
    fn initialize() -> CommonResult<()> {
        let mut ds = create_data_set(true, "init");
        for id in 1..12 {
            let block = ExtendedBlock::with_mem(id, &format!("{}B", id))?;
            ds.open_block(&block)?
                .write_test_data(&format!("{}B", id))?;
        }
        drop(ds);
        let ds = create_data_set(false, "init");
        assert_eq!(ds.block_map.len(), 11);
        Ok(())
    }
    #[test]
    fn abort_spdk() -> CommonResult<()> {
        use crate::worker::block::BlockMeta;
        use curvine_common::state::StorageType;
        use orpc::sync::AtomicLong;
        use orpc::sys::FsStats;
        use std::path::PathBuf;
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;
        let st = Arc::new(DirState {
            dir_id: 1,
            base_path: PathBuf::from("/tmp/spdk"),
            storage_type: StorageType::Spdk,
            bdev_name: Some("nvme0".into()),
            bdev_capacity: 1 << 30,
            offset_alloc: DirState::new_offset_alloc(StorageType::Spdk, 1 << 30, 4096),
        });
        let vfs = VfsDir {
            version: StorageVersion::with_cluster("t"),
            stats: FsStats::new("/tmp"),
            active_dir: PathBuf::from("/tmp/a"),
            staging_dir: PathBuf::from("/tmp/s"),
            storage_type: StorageType::Spdk,
            conf_capacity: 1 << 30,
            reserved_bytes: 0,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state: st.clone(),
            check_failed: Arc::new(AtomicBool::new(false)),
        };
        let mut ds = VfsDataset::new("t", DirList::new(vec![vfs]), None);
        let meta = BlockMeta {
            id: 1,
            len: 4096,
            state: BlockState::Writing,
            dir: st,
            actual_len: 4096,
            bdev_offset: 0,
        };
        ds.block_map.insert(1, meta);
        let ok = ds
            .abort_block(&ExtendedBlock::new(
                1,
                4096,
                StorageType::Spdk,
                FileType::File,
            )?)
            .is_ok();
        assert!(ok && ds.block_map.get(&1).is_none());
        Ok(())
    }
}
