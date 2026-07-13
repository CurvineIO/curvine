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
    BlockLayout, BlockLayoutKind, BlockLayouts, Dataset, DirList, SpdkMetaStore, StorageVersion,
    VfsDir, VfsMetaStore,
};
use curvine_common::conf::{ClusterConf, WorkerDataDir};
use curvine_common::state::{ExtendedBlock, StorageType};
use indexmap::map::Values;
use log::info;
use orpc::common::{ByteUnit, FileUtils, LocalTime, TimeSpent};
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct VfsDataset {
    cluster_id: String,
    worker_id: u32,
    ctime: u64,
    dir_list: DirList,
    meta: VfsMetaStore,
    layouts: BlockLayouts,
    num_blocks_to_delete: AtomicUsize,
}

impl VfsDataset {
    fn new(
        cluster_id: &str,
        dir_list: DirList,
        spdk_meta: Option<Arc<SpdkMetaStore>>,
    ) -> CommonResult<Self> {
        let worker_id = match dir_list.get_dir_index(0) {
            None => 0,
            Some(v) => v.version().worker_id,
        };

        let mut ds = Self {
            cluster_id: cluster_id.to_string(),
            worker_id,
            ctime: LocalTime::mills(),
            dir_list,
            meta: VfsMetaStore::new(spdk_meta.clone()),
            layouts: BlockLayouts::new(spdk_meta),
            num_blocks_to_delete: AtomicUsize::new(0),
        };
        ds.initialize()?;
        Ok(ds)
    }

    pub fn from_conf(cluster_id: &str, conf: &ClusterConf) -> CommonResult<Self> {
        let mut dir_list = DirList::new(vec![])?;
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

            if data_dir.storage_type == StorageType::SpdkDisk {
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
                if dir.storage_type() == StorageType::SpdkDisk {
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

        Self::new(cluster_id, dir_list, spdk_meta)
    }

    // Initialize.
    // 1. Scan all blocks in the directory (filesystem or RocksDB for SPDK)
    // 2. Block is added to meta store.
    // 3. Update capacity usage.
    // TODO: pre-filter to avoid scan_all() per SPDK dir
    fn initialize(&mut self) -> CommonResult<()> {
        let spent = TimeSpent::new();
        for dir in self.dir_list.dir_iter() {
            let layout = self.layouts.get(dir.storage_type());
            let blocks = layout.scan(dir)?;
            for block in blocks {
                dir.reserve_space(true, block.len);
                self.meta.put(block);
            }
        }
        info!(
            "Dataset initialize, used {} ms, total block {}",
            spent.used_ms(),
            self.meta.block_count()
        );
        Ok(())
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
        self.meta.all_blocks()
    }

    #[cfg(test)]
    /// Test helper: get the offset allocator for a given dir_id.
    pub fn offset_alloc_for_dir(
        &self,
        dir_id: u32,
    ) -> Option<&super::dir_state::BdevOffsetAllocator> {
        self.dir_list
            .dir_iter()
            .find(|d| d.id() == dir_id)
            .map(|d| &d.state.offset_alloc)
    }

    pub(crate) fn remove_block_state_by_id(
        &mut self,
        id: i64,
    ) -> CommonResult<(BlockMeta, BlockLayoutKind)> {
        let meta = match self.meta.remove(id) {
            None => return err_box!("Not found block {}", id),
            Some(meta) => meta,
        };

        let layout = self.layouts.get(meta.storage_type());
        if self.find_dir(meta.dir_id()).is_ok() {
            layout.release(&meta)?;
        }

        Ok((meta, layout.clone()))
    }

    pub(crate) fn release_block_space(&self, meta: &BlockMeta) -> CommonResult<()> {
        let dir = self.find_dir(meta.dir_id())?;
        dir.release_space(meta.is_final(), meta.actual_len);
        Ok(())
    }

    pub(crate) fn remove_block_by_id(&mut self, id: i64) -> CommonResult<BlockMeta> {
        let (meta, layout) = self.remove_block_state_by_id(id)?;
        layout.deallocate(&meta)?;
        self.release_block_space(&meta)?;
        Ok(meta)
    }

    pub fn layout_for(&self, meta: &BlockMeta) -> BlockLayoutKind {
        self.layouts.get(meta.storage_type()).clone()
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
        self.meta.block_count()
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
        self.meta.get(id)
    }

    fn open_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        match self.meta.get(block.id).cloned() {
            Some(meta) => {
                if meta.is_active() {
                    let dir = self.find_dir(meta.dir_id())?;
                    // Create a new block meta, where block.len represents the block size
                    let mut new_meta = BlockMeta::new(meta.id, block.len, dir);
                    new_meta.bdev_offset = meta.bdev_offset; // preserve allocated offset

                    dir.release_space(meta.is_final(), meta.actual_len);
                    dir.reserve_space(false, new_meta.len);
                    self.meta.put(new_meta.clone());

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
                let layout = self.layouts.get(dir.storage_type());
                let meta = layout.allocate(dir, block)?;

                self.meta.put(meta.clone());
                dir.reserve_space(false, block.len);

                Ok(meta)
            }
        }
    }

    fn finalize_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        // Keep the meta borrow scoped before mutating the meta store and dir accounting.
        let (dir_id, actual_len, final_meta) = {
            let meta = self.get_block_check(block.id)?;
            if meta.state() == &BlockState::Finalized {
                if meta.len() == block.len {
                    return Ok(meta.clone());
                }
                return err_box!(
                    "finalized block {} length mismatch, expected: {}, actual: {}",
                    meta.id(),
                    block.len,
                    meta.len()
                );
            }
            if meta.state() != &BlockState::Writing {
                return err_box!(
                    "block {} status incorrect, expected {:?}, actual: {:?}",
                    meta.id(),
                    BlockState::Writing,
                    meta.state()
                );
            }

            let layout = self.layouts.get(meta.storage_type());
            let final_meta = layout.finalize(meta, block.len)?;
            if block.len != final_meta.len() {
                return err_box!(
                    "Block {} length mismatch, expected: {}, actual: {}",
                    meta.id(),
                    block.len,
                    final_meta.len()
                );
            }

            (meta.dir_id(), meta.actual_len, final_meta)
        };

        let dir = self.find_dir(dir_id)?;
        dir.release_space(false, actual_len);
        dir.reserve_space(true, final_meta.actual_len);
        self.meta.put(final_meta.clone());

        Ok(final_meta)
    }

    fn abort_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        if self.meta.get(block.id).is_none() {
            return Ok(());
        }
        self.remove_block_by_id(block.id)?;
        Ok(())
    }

    fn remove_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        self.abort_block(block)
    }
}

#[cfg(test)]
mod test {
    use crate::worker::storage::{Dataset, DirList, DirState, StorageVersion, VfsDataset, VfsDir};
    use curvine_common::conf::{ClusterConf, WorkerConf};
    use curvine_common::state::{ExtendedBlock, FileType, StorageType};
    use orpc::sync::AtomicLong;
    use orpc::sys::FsStats;
    use orpc::CommonResult;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

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

    fn spdk_state(dir_id: u32) -> Arc<DirState> {
        Arc::new(DirState {
            dir_id,
            base_path: PathBuf::from("/tmp/spdk"),
            storage_type: StorageType::SpdkDisk,
            bdev_name: Some("nvme0".into()),
            bdev_capacity: 1 << 30,
            offset_alloc: DirState::new_offset_alloc(StorageType::SpdkDisk, 1 << 30, 4096),
        })
    }

    fn spdk_dir(state: Arc<DirState>) -> VfsDir {
        let dir_id = state.dir_id;
        let mut version = StorageVersion::with_cluster("t");
        version.dir_id = dir_id;
        VfsDir {
            version,
            stats: FsStats::new("/tmp"),
            active_dir: PathBuf::from("/tmp/a"),
            staging_dir: PathBuf::from("/tmp/s"),
            storage_type: StorageType::SpdkDisk,
            conf_capacity: 1 << 30,
            reserved_bytes: 0,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state,
            check_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn spdk_dataset() -> CommonResult<VfsDataset> {
        let st = spdk_state(1);
        VfsDataset::new("t", DirList::new(vec![spdk_dir(st)])?, None)
    }

    #[test]
    fn sample() -> CommonResult<()> {
        let mut ds = create_data_set(true, "sample");
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
        let mut ds = create_data_set(true, "append");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let meta = ds.open_block(&block)?;
        meta.write_test_data("50B")?;
        block.len = 50;
        ds.finalize_block(&block)?;
        block.len = 70;
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
        assert_eq!(ds.num_blocks(), 11);
        Ok(())
    }
    #[test]
    fn abort_spdk() -> CommonResult<()> {
        let mut ds = spdk_dataset()?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        ds.open_block(&block)?;
        let ok = ds.abort_block(&block).is_ok();
        assert!(ok && ds.get_block(1).is_none());
        Ok(())
    }
    #[test]
    fn spdk_create_abort_reuse_offset() -> CommonResult<()> {
        let mut ds = spdk_dataset()?;
        let block1 = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        let meta1 = ds.open_block(&block1)?;
        assert_eq!(meta1.bdev_offset, 0, "first block should get offset 0");
        assert_eq!(ds.offset_alloc_for_dir(1).unwrap().allocated_count(), 1);

        ds.abort_block(&block1)?;
        assert!(ds.get_block(1).is_none());
        assert_eq!(ds.offset_alloc_for_dir(1).unwrap().free_list_size(), 1);

        let block2 = ExtendedBlock::new(2, 4096, StorageType::SpdkDisk, FileType::File);
        let meta2 = ds.open_block(&block2)?;
        assert_eq!(meta2.bdev_offset, 0, "block 2 should reuse freed offset 0");
        assert_eq!(ds.offset_alloc_for_dir(1).unwrap().free_list_size(), 0);

        Ok(())
    }
    #[test]
    fn spdk_create_abort_interleaved() -> CommonResult<()> {
        let mut ds = spdk_dataset()?;
        let b1 = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        let b2 = ExtendedBlock::new(2, 4096, StorageType::SpdkDisk, FileType::File);
        let b3 = ExtendedBlock::new(3, 4096, StorageType::SpdkDisk, FileType::File);
        let m1 = ds.open_block(&b1)?;
        let m2 = ds.open_block(&b2)?;
        let m3 = ds.open_block(&b3)?;
        assert_eq!(m1.bdev_offset, 0);
        assert_eq!(m2.bdev_offset, 4096);
        assert_eq!(m3.bdev_offset, 8192);

        ds.abort_block(&b2)?;
        let alloc = ds.offset_alloc_for_dir(1).unwrap();
        assert_eq!(alloc.free_list_size(), 1);
        assert_eq!(alloc.free_list_entries()[0], (4096, 4096));

        ds.abort_block(&b1)?;
        let alloc = ds.offset_alloc_for_dir(1).unwrap();
        assert_eq!(alloc.free_list_size(), 1);
        assert_eq!(alloc.free_list_entries()[0], (0, 8192));

        let b4 = ExtendedBlock::new(4, 4096, StorageType::SpdkDisk, FileType::File);
        let m4 = ds.open_block(&b4)?;
        assert_eq!(m4.bdev_offset, 0);

        let b5 = ExtendedBlock::new(5, 4096, StorageType::SpdkDisk, FileType::File);
        let m5 = ds.open_block(&b5)?;
        assert_eq!(m5.bdev_offset, 4096);

        Ok(())
    }
    #[test]
    fn spdk_restore_free_list() -> CommonResult<()> {
        let st = spdk_state(1);
        let mut ds = VfsDataset::new("t", DirList::new(vec![spdk_dir(st.clone())])?, None)?;
        let b1 = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        let b2 = ExtendedBlock::new(2, 4096, StorageType::SpdkDisk, FileType::File);
        let b3 = ExtendedBlock::new(3, 4096, StorageType::SpdkDisk, FileType::File);
        ds.open_block(&b1)?;
        ds.open_block(&b2)?;
        ds.open_block(&b3)?;

        ds.abort_block(&b2)?;
        let snap = st.offset_alloc.snapshot();

        let st2 = spdk_state(1);
        st2.offset_alloc.restore(&snap);

        let entries = st2.offset_alloc.free_list_entries();
        assert_eq!(entries.len(), 1, "expected 1 free entry, got {:?}", entries);
        assert_eq!(entries[0], (4096, 4096));
        assert_eq!(st2.offset_alloc.free_list_bytes(), 4096);

        assert_eq!(st2.offset_alloc.allocate(4, 4096).unwrap(), 4096);
        Ok(())
    }
}
