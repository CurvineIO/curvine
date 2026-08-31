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

use crate::layout::FileFinalizePlan;
use crate::{
    BlockLayout, BlockLayoutKind, BlockLayouts, Dataset, DirFreeRatio, DirList, FileLayout,
    SpdkMetaStore, StorageRequest, StorageVersion, VfsDir, VfsMetaStore,
};
use crate::{BlockMeta, BlockState};
use curvine_config::{ClusterConf, WorkerDataDir};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{ExtendedBlock, StorageInfo, StorageType};
use curvine_runtime::common::{ByteUnit, FileUtils, LocalTime, TimeSpent};
use indexmap::map::Values;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct VfsDataset {
    cluster_id: String,
    worker_id: u32,
    ctime: u64,
    dir_list: DirList,
    meta: VfsMetaStore,
    committed_rewrites: HashMap<i64, BlockMeta>,
    layouts: BlockLayouts,
    num_blocks_to_delete: AtomicUsize,
}

pub struct RemovedBlockState {
    pub meta: BlockMeta,
    committed: Option<BlockMeta>,
    pub(crate) layout: BlockLayoutKind,
    pub(crate) dir: Arc<VfsDir>,
}

pub struct FileOpenReservation {
    pending: BlockMeta,
    committed: Option<BlockMeta>,
    dir: Arc<VfsDir>,
}

impl FileOpenReservation {
    pub fn prepare(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        match self.committed.as_ref() {
            Some(committed) => FileLayout.prepare_write(&self.dir, committed, block),
            None => FileLayout.allocate(&self.dir, block),
        }
    }
}

pub struct FileFinalizeReservation {
    writing: BlockMeta,
    pending: BlockMeta,
    dir: Arc<VfsDir>,
}

impl FileFinalizeReservation {
    pub fn prepare(&self, committed_len: i64) -> CommonResult<FileFinalizePlan> {
        FileLayout::prepare_finalize(&self.dir, &self.pending, committed_len)
    }
}

impl RemovedBlockState {
    pub fn deallocate(&self) -> CommonResult<()> {
        self.layout.deallocate(&self.dir, &self.meta)?;
        if let Some(committed) = self.committed.as_ref() {
            self.layout.deallocate(&self.dir, committed)?;
        }
        Ok(())
    }

    pub fn release(&self) {
        self.layout.release(&self.dir, &self.meta);
        if let Some(committed) = self.committed.as_ref() {
            self.layout.release(&self.dir, committed);
        }

        self.dir
            .release_space(self.meta.is_final(), self.meta.physical_bytes());
        if let Some(committed) = self.committed.as_ref() {
            self.dir
                .release_space(committed.is_final(), committed.physical_bytes());
        }
    }
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
            committed_rewrites: HashMap::new(),
            layouts: BlockLayouts::new(spdk_meta),
            num_blocks_to_delete: AtomicUsize::new(0),
        };
        ds.initialize()?;
        Ok(ds)
    }

    pub fn from_conf(cluster_id: &str, conf: &ClusterConf) -> CommonResult<Self> {
        let mut dir_list = DirList::new(vec![])?;
        let dir_reserved = ByteUnit::from_str(&conf.worker.dir_reserved)?.as_byte();
        let free_ratio = {
            let raw = conf.worker.free_ratio;
            if raw < 0.0 {
                warn!(
                    "worker.free_ratio={} is negative; clamping to 0.0 (disabled)",
                    raw
                );
                0.0
            } else if raw >= 1.0 {
                warn!("worker.free_ratio={} >= 1.0; clamping to 0.99", raw);
                0.99
            } else {
                raw
            }
        };

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

            let vfs_dir = VfsDir::new(version, data_dir, dir_reserved, free_ratio)?;
            dir_list.add_dir(vfs_dir);
        }

        if has_spdk {
            let mut seen: HashMap<String, u32> = HashMap::new();
            for dir in dir_list.dir_iter() {
                if dir.storage_type() == StorageType::SpdkDisk {
                    if let Some(bdev) = dir.state.bdev_name.as_ref() {
                        if let Some(prev_id) = seen.insert(bdev.clone(), dir.id()) {
                            return curvine_core_error::err_box!(
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
                let physical_bytes = block.physical_bytes();
                dir.reserve_space(block.is_final(), physical_bytes);
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
            None => err_box!("No storage directory found: {:?}", id),
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

    pub fn dir_iter(&self) -> Values<'_, u32, Arc<VfsDir>> {
        self.dir_list.dir_iter()
    }

    pub fn all_blocks(&self) -> Vec<BlockMeta> {
        self.meta
            .all_blocks()
            .into_iter()
            .filter_map(|meta| {
                self.committed_rewrites
                    .get(&meta.id())
                    .cloned()
                    .or_else(|| (meta.state() != &BlockState::Allocating).then_some(meta))
            })
            .collect()
    }

    pub fn get_readable_block(&self, id: i64) -> Option<&BlockMeta> {
        if let Some(committed) = self.committed_rewrites.get(&id) {
            return Some(committed);
        }
        let meta = self.meta.get(id)?;
        (meta.state() != &BlockState::Allocating).then_some(meta)
    }

    pub fn reserve_file_open(
        &mut self,
        block: &ExtendedBlock,
    ) -> CommonResult<Option<FileOpenReservation>> {
        if block.len < 0 {
            return err_box!("Invalid file block size: {}", block.len);
        }

        match self.meta.get(block.id).cloned() {
            Some(meta) => {
                if meta.storage_type() == StorageType::SpdkDisk || !meta.is_final() {
                    return Ok(None);
                }

                let dir = self
                    .dir_list
                    .get_dir(meta.dir_id())
                    .ok_or_else(|| {
                        curvine_core_error::err_msg!(format!(
                            "No storage directory found: {:?}",
                            meta.dir_id()
                        ))
                    })?
                    .clone();
                let required_bytes = meta.physical_bytes().max(block.len);
                if required_bytes > dir.available() {
                    return err_box!(
                        "Not enough space in storage dir {} for block {} rewrite: need {}, available {}",
                        meta.dir_id(),
                        meta.id(),
                        required_bytes,
                        dir.available()
                    );
                }

                let mut pending = BlockMeta::new(meta.id(), block.len, &dir);
                pending.state = BlockState::Allocating;
                pending.actual_len = required_bytes;
                dir.reserve_space(false, required_bytes);
                self.committed_rewrites.insert(meta.id(), meta.clone());
                self.meta.put(pending.clone());

                Ok(Some(FileOpenReservation {
                    pending,
                    committed: Some(meta),
                    dir,
                }))
            }
            None => {
                if block.storage_type == StorageType::SpdkDisk {
                    return Ok(None);
                }

                let dir = self
                    .dir_list
                    .choose_dir(StorageRequest::new(block.storage_type, block.len)?)?;
                let mut pending = BlockMeta::with_tmp(block, &dir);
                pending.state = BlockState::Allocating;
                dir.reserve_space(false, pending.physical_bytes());
                self.meta.put(pending.clone());

                Ok(Some(FileOpenReservation {
                    pending,
                    committed: None,
                    dir,
                }))
            }
        }
    }

    pub fn complete_file_open(
        &mut self,
        reservation: &FileOpenReservation,
        meta: BlockMeta,
    ) -> CommonResult<BlockMeta> {
        let pending = self.get_block_check(reservation.pending.id())?;
        if pending.state() != &BlockState::Allocating {
            return err_box!(
                "block {} open reservation lost while in state {:?}",
                pending.id(),
                pending.state()
            );
        }
        if meta.id() != pending.id() || meta.dir_id() != pending.dir_id() {
            return err_box!(
                "block {} open reservation returned different metadata",
                pending.id()
            );
        }

        self.meta.put(meta.clone());
        Ok(meta)
    }

    pub fn rollback_file_open(&mut self, reservation: &FileOpenReservation) -> CommonResult<()> {
        let pending = self.meta.remove(reservation.pending.id()).ok_or_else(|| {
            curvine_core_error::err_msg!(format!(
                "block {} open reservation missing during rollback",
                reservation.pending.id()
            ))
        })?;
        if pending.state() != &BlockState::Allocating {
            return err_box!(
                "block {} open reservation changed to {:?} before rollback",
                pending.id(),
                pending.state()
            );
        }

        reservation
            .dir
            .release_space(false, pending.physical_bytes());
        if let Some(committed) = reservation.committed.as_ref() {
            self.committed_rewrites.remove(&pending.id());
            self.meta.put(committed.clone());
        }
        Ok(())
    }

    pub fn reserve_file_finalize(
        &mut self,
        block: &ExtendedBlock,
    ) -> CommonResult<Option<FileFinalizeReservation>> {
        let writing = self.get_block_check(block.id)?.clone();
        if writing.storage_type() == StorageType::SpdkDisk
            || writing.state() != &BlockState::Writing
        {
            return Ok(None);
        }

        let dir = self
            .dir_list
            .get_dir(writing.dir_id())
            .ok_or_else(|| {
                curvine_core_error::err_msg!(format!(
                    "No storage directory found: {:?}",
                    writing.dir_id()
                ))
            })?
            .clone();
        let mut pending = writing.clone();
        pending.state = BlockState::Finalizing;
        self.meta.put(pending.clone());
        Ok(Some(FileFinalizeReservation {
            writing,
            pending,
            dir,
        }))
    }

    pub fn publish_file_finalize(
        &mut self,
        reservation: &FileFinalizeReservation,
        plan: FileFinalizePlan,
    ) -> CommonResult<BlockMeta> {
        let pending = self.get_block_check(reservation.pending.id())?.clone();
        if pending.state() != &BlockState::Finalizing {
            return err_box!(
                "block {} finalize reservation lost while in state {:?}",
                pending.id(),
                pending.state()
            );
        }
        let final_meta = plan.final_meta();
        if final_meta.id() != pending.id() || final_meta.dir_id() != pending.dir_id() {
            return err_box!(
                "block {} finalize plan returned different metadata",
                pending.id()
            );
        }

        // The plan already performed stat and directory preparation. Hold this
        // lock only while the rename becomes visible and metadata is published.
        let final_meta = FileLayout::publish_finalize(plan)?;
        if let Some(committed) = self.committed_rewrites.remove(&pending.id()) {
            reservation
                .dir
                .release_space(committed.is_final(), committed.physical_bytes());
        }
        reservation
            .dir
            .release_space(false, pending.physical_bytes());
        reservation
            .dir
            .reserve_space(true, final_meta.physical_bytes());
        self.meta.put(final_meta.clone());
        Ok(final_meta)
    }

    pub fn rollback_file_finalize(
        &mut self,
        reservation: &FileFinalizeReservation,
    ) -> CommonResult<()> {
        let pending = self.get_block_check(reservation.pending.id())?;
        if pending.state() != &BlockState::Finalizing {
            return err_box!(
                "block {} finalize reservation changed to {:?} before rollback",
                pending.id(),
                pending.state()
            );
        }
        self.meta.put(reservation.writing.clone());
        Ok(())
    }

    #[cfg(test)]
    fn write_test_data(&self, meta: &BlockMeta, size: &str) -> CommonResult<()> {
        let dir = self.find_dir(meta.dir_id())?;
        super::FileLayout::write_test_data(dir, meta, size)
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

    pub fn put_test_meta(&mut self, meta: BlockMeta) {
        self.meta.put(meta);
    }

    pub fn remove_block_state_by_id(&mut self, id: i64) -> CommonResult<RemovedBlockState> {
        let meta = match self.meta.get(id).cloned() {
            None => return err_box!("Not found block {}", id),
            Some(meta) => meta,
        };
        let layout = self.layouts.get(meta.storage_type()).clone();
        let dir = match self.dir_list.get_dir(meta.dir_id()) {
            None => return err_box!("No storage directory found: {:?}", meta.dir_id()),
            Some(dir) => dir.clone(),
        };
        let meta = self.meta.remove(id).expect("block metadata must exist");
        let committed = self.committed_rewrites.remove(&id);

        Ok(RemovedBlockState {
            meta,
            committed,
            layout,
            dir,
        })
    }

    pub fn restore_removed_block(&mut self, removed: RemovedBlockState) {
        let id = removed.meta.id();
        self.meta.put(removed.meta);
        if let Some(committed) = removed.committed {
            self.committed_rewrites.insert(id, committed);
        }
    }

    pub(crate) fn remove_block_by_id(&mut self, id: i64) -> CommonResult<BlockMeta> {
        let removed = self.remove_block_state_by_id(id)?;
        if let Err(e) = removed.deallocate() {
            self.restore_removed_block(removed);
            return Err(e);
        }
        removed.release();
        Ok(removed.meta)
    }

    pub fn layout_for(&self, meta: &BlockMeta) -> CommonResult<(BlockLayoutKind, Arc<VfsDir>)> {
        let dir = match self.dir_list.get_dir(meta.dir_id()) {
            None => return err_box!("No storage directory found: {:?}", meta.dir_id()),
            Some(dir) => dir.clone(),
        };
        Ok((self.layouts.get(meta.storage_type()).clone(), dir))
    }

    pub fn get_and_check_storages(&self) -> Vec<StorageInfo> {
        self.dir_list
            .get_and_check_storages(self.meta.block_count() as i64)
    }

    pub fn failed_storage_count(&self) -> usize {
        self.dir_list.failed_count()
    }

    pub fn storage_count(&self) -> usize {
        self.dir_list.len()
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

    fn dir_free_ratios(&self) -> Vec<DirFreeRatio> {
        self.dir_list
            .dir_iter()
            .map(|dir| DirFreeRatio {
                dir_id: dir.id(),
                dir_path: dir.path_str().to_string(),
                storage_type: dir.storage_type(),
                free_ratio: dir.raw_free_ratio(),
            })
            .collect()
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
                    let layout = self.layouts.get(meta.storage_type());
                    let preserves_committed =
                        meta.is_final() && layout.preserves_committed_on_write();
                    if preserves_committed {
                        let required_bytes = meta.physical_bytes().max(block.len);
                        if required_bytes > dir.available() {
                            return err_box!(
                                "Not enough space in storage dir {} for block {} rewrite: need {}, available {}",
                                meta.dir_id(),
                                meta.id(),
                                required_bytes,
                                dir.available()
                            );
                        }
                    }

                    let old_physical_bytes = meta.physical_bytes();
                    let new_meta = layout.prepare_write(dir, &meta, block)?;
                    let new_physical_bytes = new_meta.physical_bytes();

                    if preserves_committed {
                        dir.reserve_space(false, new_physical_bytes);
                        self.committed_rewrites.insert(meta.id(), meta);
                    } else {
                        self.dir_list.update_write_space(
                            meta.dir_id(),
                            meta.is_final(),
                            old_physical_bytes,
                            new_physical_bytes,
                        )?;
                    }
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
                let dir = self
                    .dir_list
                    .choose_dir(StorageRequest::new(block.storage_type, block.len)?)?;
                let layout = self.layouts.get(dir.storage_type());
                let meta = layout.allocate(&dir, block)?;

                dir.reserve_space(false, meta.physical_bytes());
                self.meta.put(meta.clone());

                Ok(meta)
            }
        }
    }

    fn finalize_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        // Keep the meta borrow scoped before mutating the meta store and dir accounting.
        let (dir_id, reserved_bytes, final_bytes, final_meta) = {
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

            let dir = self.find_dir(meta.dir_id())?;
            let layout = self.layouts.get(meta.storage_type());
            let reserved_bytes = meta.physical_bytes();
            let final_meta = layout.finalize(dir, meta, block.len)?;
            if block.len != final_meta.len() {
                return err_box!(
                    "Block {} length mismatch, expected: {}, actual: {}",
                    meta.id(),
                    block.len,
                    final_meta.len()
                );
            }

            let final_bytes = final_meta.physical_bytes();
            (meta.dir_id(), reserved_bytes, final_bytes, final_meta)
        };

        if let Some(committed) = self.committed_rewrites.remove(&block.id) {
            self.dir_list.release_space(
                committed.dir_id(),
                committed.is_final(),
                committed.physical_bytes(),
            )?;
        }
        self.dir_list
            .update_final_space(dir_id, reserved_bytes, final_bytes)?;
        self.meta.put(final_meta.clone());

        Ok(final_meta)
    }

    fn abort_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        let Some(meta) = self.meta.get(block.id).cloned() else {
            return Ok(());
        };

        if let Some(committed) = self.committed_rewrites.get(&block.id).cloned() {
            let (layout, dir) = self.layout_for(&meta)?;
            layout.deallocate(&dir, &meta)?;
            layout.release(&dir, &meta);
            dir.release_space(false, meta.physical_bytes());
            self.meta.remove(block.id);
            self.committed_rewrites.remove(&block.id);
            self.meta.put(committed);
            return Ok(());
        }

        self.remove_block_by_id(block.id)?;
        Ok(())
    }

    fn remove_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        if self.meta.get(block.id).is_none() {
            return Ok(());
        }
        self.remove_block_by_id(block.id)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        BlockLayout, Dataset, DirList, DirState, FileLayout, SpdkMetaStore, StorageVersion,
        VfsDataset, VfsDir,
    };
    use crate::{BlockMeta, BlockState};
    use curvine_config::{ClusterConf, WorkerConf};
    use curvine_core_error::CommonResult;
    use curvine_io::DataSlice;
    use curvine_model::{ExtendedBlock, FileType, StorageType};
    use curvine_runtime::common::FileUtils;
    use curvine_runtime::sync::AtomicLong;
    use curvine_sys::FsStats;
    use std::io::Write;
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

    fn spdk_state() -> Arc<DirState> {
        Arc::new(DirState {
            bdev_name: Some("nvme0".into()),
            bdev_capacity: 1 << 30,
            offset_alloc: DirState::new_offset_alloc(StorageType::SpdkDisk, 1 << 30, 4096),
        })
    }

    fn spdk_dir(dir_id: u32, state: Arc<DirState>) -> VfsDir {
        let mut version = StorageVersion::with_cluster("t");
        version.dir_id = dir_id;
        VfsDir {
            version,
            stats: FsStats::new("/tmp"),
            storage_type: StorageType::SpdkDisk,
            conf_capacity: 1 << 30,
            reserved_bytes: 0,
            free_ratio: 0.0,
            final_bytes: AtomicLong::new(0),
            tmp_bytes: AtomicLong::new(0),
            state,
            check_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn spdk_dataset() -> CommonResult<VfsDataset> {
        let st = spdk_state();
        VfsDataset::new("t", DirList::new(vec![spdk_dir(1, st)])?, None)
    }

    /// SPDK dir with controllable capacity, used bytes, and free_ratio, so the
    /// guard can be exercised deterministically without a real bdev.
    fn spdk_dir_with(dir_id: u32, free_ratio: f64, used_bytes: i64, capacity: i64) -> VfsDir {
        let mut version = StorageVersion::with_cluster("t");
        version.dir_id = dir_id;
        let state = Arc::new(DirState {
            bdev_name: Some("nvme0".into()),
            bdev_capacity: capacity,
            offset_alloc: DirState::new_offset_alloc(StorageType::SpdkDisk, capacity, 4096),
        });
        VfsDir {
            version,
            stats: FsStats::new("/tmp"),
            storage_type: StorageType::SpdkDisk,
            conf_capacity: capacity,
            reserved_bytes: 0,
            free_ratio,
            final_bytes: AtomicLong::new(used_bytes),
            tmp_bytes: AtomicLong::new(0),
            state,
            check_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn spdk_free_ratio_gate_blocks_when_below_threshold() -> CommonResult<()> {
        // 1 GiB device, 95% used -> free ratio 0.05 < 0.1 floor.
        let cap = 1 << 30;
        let used = cap - cap / 20;
        let dir = spdk_dir_with(1, 0.1, used, cap);

        assert!(dir.raw_free_ratio() < 0.1, "ratio should be below floor");
        assert_eq!(
            dir.available(),
            0,
            "available must be 0 when free ratio is below floor"
        );
        assert!(
            !dir.can_allocate(StorageType::SpdkDisk, 4096),
            "can_allocate must reject when gate is active"
        );
        Ok(())
    }

    #[test]
    fn spdk_free_ratio_disabled_by_default() -> CommonResult<()> {
        // Same 95% used, but free_ratio=0.0 disables the guard.
        let cap = 1 << 30;
        let used = cap - cap / 20;
        let dir = spdk_dir_with(1, 0.0, used, cap);

        assert_eq!(
            dir.available(),
            cap - used,
            "guard disabled -> normal accounting"
        );
        assert!(dir.can_allocate(StorageType::SpdkDisk, 4096));
        Ok(())
    }

    #[test]
    fn spdk_raw_free_ratio_value() -> CommonResult<()> {
        let cap = 1 << 30;
        let used = cap / 2; // 50% used
        let dir = spdk_dir_with(1, 0.0, used, cap);
        let r = dir.raw_free_ratio();
        assert!((r - 0.5).abs() < 1e-9, "expected 0.5, got {r}");
        Ok(())
    }

    #[test]
    fn dir_free_ratios_reports_per_dir() -> CommonResult<()> {
        let ds = spdk_dataset()?;
        let ratios = ds.dir_free_ratios();
        assert_eq!(ratios.len(), 1, "one spdk dir");
        assert_eq!(ratios[0].dir_id, 1);
        assert_eq!(ratios[0].storage_type, StorageType::SpdkDisk);
        assert!(
            ratios[0].free_ratio > 0.0 && ratios[0].free_ratio <= 1.0,
            "free_ratio {} out of range",
            ratios[0].free_ratio
        );
        Ok(())
    }

    #[test]
    fn fs_raw_free_ratio_sane_and_default_disabled() -> CommonResult<()> {
        use curvine_config::WorkerDataDir;
        FileUtils::delete_path("../testing/fs-free-ratio", true)?;
        let dir = VfsDir::from_dir(
            "",
            WorkerDataDir::from_str("[SSD:100MB]../testing/fs-free-ratio")?,
        )?;
        let r = dir.raw_free_ratio();
        assert!(r > 0.0 && r <= 1.0, "raw_free_ratio {r} must be in (0, 1]");
        // free_ratio defaults to 0.0 -> guard inactive, available positive.
        assert!(
            dir.available() > 0,
            "available should be positive on fresh dir"
        );
        Ok(())
    }

    #[test]
    fn sample() -> CommonResult<()> {
        let mut ds = create_data_set(true, "sample");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let meta = ds.open_block(&block)?;
        assert_eq!(ds.available(), 400);
        ds.write_test_data(&meta, "50B")?;
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
        ds.write_test_data(&meta, "40B")?;
        block.len = 40;
        ds.finalize_block(&block)?;
        block.len = 60;
        let meta2 = ds.open_block(&block)?;
        ds.write_test_data(&meta2, "20B")?;
        ds.finalize_block(&block)?;
        assert_eq!(ds.available(), 440);
        Ok(())
    }
    #[test]
    fn initialize() -> CommonResult<()> {
        let mut ds = create_data_set(true, "init");
        for id in 1..12 {
            let block = ExtendedBlock::with_mem(id, &format!("{}B", id))?;
            let meta = ds.open_block(&block)?;
            ds.write_test_data(&meta, &format!("{}B", id))?;
        }
        drop(ds);
        let ds = create_data_set(false, "init");
        assert_eq!(ds.num_blocks(), 11);
        assert_eq!(ds.all_blocks().len(), 11);
        assert!(ds.get_readable_block(1).is_some());
        Ok(())
    }
    #[test]
    fn abort_spdk() -> CommonResult<()> {
        let mut ds = spdk_dataset()?;
        let block = ExtendedBlock::new(1, 1, StorageType::SpdkDisk, FileType::File);
        let available = ds.available();
        ds.open_block(&block)?;
        assert_eq!(ds.available(), available - 4096);
        let ok = ds.abort_block(&block).is_ok();
        assert!(ok && ds.get_block(1).is_none());
        assert_eq!(ds.available(), available);
        Ok(())
    }
    #[test]
    fn spdk_prepare_write_preserves_extent_charge() -> CommonResult<()> {
        let mut ds = spdk_dataset()?;
        let mut block = ExtendedBlock::new(1, 1, StorageType::SpdkDisk, FileType::File);
        let available = ds.available();
        ds.open_block(&block)?;
        ds.finalize_block(&block)?;
        assert_eq!(ds.available(), available - 4096);

        block.len = 4097;
        assert!(ds.open_block(&block).is_err());
        assert_eq!(ds.available(), available - 4096);
        assert!(ds.get_block(block.id).unwrap().is_final());

        ds.abort_block(&block)?;
        assert_eq!(ds.available(), available);
        Ok(())
    }
    #[test]
    fn abort_and_failed_prepare_write_preserve_capacity() -> CommonResult<()> {
        let mut ds = create_data_set(true, "capacity-rollback");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let available = ds.available();
        ds.open_block(&block)?;
        assert_eq!(ds.available(), available - 100);
        ds.abort_block(&block)?;
        assert_eq!(ds.available(), available);

        let meta = ds.open_block(&block)?;
        ds.write_test_data(&meta, "50B")?;
        block.len = 50;
        ds.finalize_block(&block)?;
        let finalized_available = ds.available();

        block.len = 101;
        assert!(ds.open_block(&block).is_err());
        assert_eq!(ds.available(), finalized_available);
        assert!(ds.get_block(block.id).unwrap().is_final());
        Ok(())
    }
    #[test]
    fn failed_file_deallocate_keeps_block_and_capacity_reserved() -> CommonResult<()> {
        let mut ds = create_data_set(true, "deallocate-failure");
        let block = ExtendedBlock::with_mem(1, "100B")?;
        let available = ds.available();
        let meta = ds.open_block(&block)?;
        let dir = ds.find_dir(meta.dir_id())?;
        let block_path = FileLayout::block_path(dir, &meta)?;

        FileUtils::delete_path(&block_path, false)?;
        std::fs::create_dir(&block_path)?;
        std::fs::write(block_path.join("child"), b"data")?;

        assert!(ds.abort_block(&block).is_err());
        assert!(ds.get_block(block.id).is_some());
        assert_eq!(ds.available(), available - 100);

        FileUtils::delete_path(block_path, true)?;
        ds.abort_block(&block)?;
        assert!(ds.get_block(block.id).is_none());
        assert_eq!(ds.available(), available);
        Ok(())
    }
    #[test]
    fn failed_file_allocate_does_not_reserve_capacity() -> CommonResult<()> {
        let mut ds = create_data_set(true, "allocate-failure");
        let block = ExtendedBlock::with_mem(1, "100B")?;
        let available = ds.available();
        let dir = ds.dir_iter().next().unwrap().clone();
        let meta = BlockMeta::with_tmp(&block, &dir);
        let block_path = FileLayout::block_path(&dir, &meta)?;

        std::fs::create_dir(&block_path)?;

        assert!(ds.open_block(&block).is_err());
        assert!(ds.get_block(block.id).is_none());
        assert_eq!(ds.available(), available);

        FileUtils::delete_path(block_path, true)?;
        Ok(())
    }
    #[test]
    fn abort_rewrite_restores_committed_capacity() -> CommonResult<()> {
        let mut ds = create_data_set(true, "abort-rewrite-restores-capacity");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let meta = ds.open_block(&block)?;
        ds.write_test_data(&meta, "50B")?;
        block.len = 50;
        ds.finalize_block(&block)?;
        let finalized_available = ds.available();

        block.len = 20;
        ds.open_block(&block)?;
        assert_eq!(ds.available(), finalized_available - 50);
        ds.abort_block(&block)?;
        assert_eq!(ds.available(), finalized_available);
        assert!(ds.get_block(block.id).unwrap().is_final());
        Ok(())
    }
    #[test]
    fn abort_rewrite_preserves_finalized_file() -> CommonResult<()> {
        let mut ds = create_data_set(true, "abort-rewrite-preserves-finalized");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let meta = ds.open_block(&block)?;
        ds.write_test_data(&meta, "40B")?;
        block.len = 40;
        let finalized = ds.finalize_block(&block)?;
        let finalized_available = ds.available();
        let active_path = {
            let dir = ds.find_dir(finalized.dir_id())?;
            FileLayout::block_path(dir, &finalized)?
        };
        let committed_bytes = std::fs::read(&active_path)?;

        block.len = 60;
        let writing = ds.open_block(&block)?;
        let staging_path = {
            let dir = ds.find_dir(writing.dir_id())?;
            FileLayout::block_path(dir, &writing)?
        };
        assert!(active_path.exists());
        assert!(staging_path.exists());
        assert!(ds.get_block(block.id).unwrap().state() == &BlockState::Writing);
        assert!(ds.get_readable_block(block.id).unwrap().is_final());
        assert!(ds.all_blocks()[0].is_final());

        std::fs::write(&staging_path, b"partial rewrite")?;
        ds.abort_block(&block)?;

        let restored = ds.get_block(block.id).unwrap();
        assert!(restored.is_final());
        assert_eq!(restored.len(), finalized.len());
        assert_eq!(std::fs::read(active_path)?, committed_bytes);
        assert!(!staging_path.exists());
        assert_eq!(ds.available(), finalized_available);
        Ok(())
    }
    #[test]
    fn restart_during_rewrite_discards_staging_and_keeps_committed() -> CommonResult<()> {
        let dataset_name = "restart-during-rewrite";
        let mut ds = create_data_set(true, dataset_name);
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let writing = ds.open_block(&block)?;
        ds.write_test_data(&writing, "40B")?;
        block.len = 40;
        let finalized = ds.finalize_block(&block)?;
        let finalized_available = ds.available();
        let active_path = {
            let dir = ds.find_dir(finalized.dir_id())?;
            FileLayout::block_path(dir, &finalized)?
        };
        let committed_bytes = std::fs::read(&active_path)?;

        block.len = 60;
        let rewriting = ds.open_block(&block)?;
        let staging_path = {
            let dir = ds.find_dir(rewriting.dir_id())?;
            FileLayout::block_path(dir, &rewriting)?
        };
        std::fs::write(&staging_path, b"partial rewrite")?;
        assert!(active_path.exists());
        assert!(staging_path.exists());
        drop(ds);

        let restarted = create_data_set(false, dataset_name);
        let recovered = restarted.get_block(block.id).unwrap();
        assert!(recovered.is_final());
        assert_eq!(recovered.len(), finalized.len());
        assert_eq!(restarted.num_blocks(), 1);
        assert_eq!(restarted.available(), finalized_available);
        assert_eq!(std::fs::read(active_path)?, committed_bytes);
        assert!(!staging_path.exists());
        Ok(())
    }
    #[test]
    fn finalize_rewrite_publishes_staging_file() -> CommonResult<()> {
        let mut ds = create_data_set(true, "finalize-rewrite-publishes");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let writing = ds.open_block(&block)?;
        let initial_path = {
            let dir = ds.find_dir(writing.dir_id())?;
            FileLayout::block_path(dir, &writing)?
        };
        std::fs::write(&initial_path, b"old")?;
        block.len = 3;
        let finalized = ds.finalize_block(&block)?;
        let active_path = {
            let dir = ds.find_dir(finalized.dir_id())?;
            FileLayout::block_path(dir, &finalized)?
        };

        block.len = 6;
        let rewriting = ds.open_block(&block)?;
        let staging_path = {
            let dir = ds.find_dir(rewriting.dir_id())?;
            FileLayout::block_path(dir, &rewriting)?
        };
        let mut staging = std::fs::OpenOptions::new()
            .append(true)
            .open(&staging_path)?;
        staging.write_all(b"new")?;
        drop(staging);

        let published = ds.finalize_block(&block)?;
        assert!(published.is_final());
        assert_eq!(std::fs::read(active_path)?, b"oldnew");
        assert!(!staging_path.exists());
        assert!(ds.committed_rewrites.is_empty());
        Ok(())
    }

    #[test]
    fn finalize_rewrite_materializes_sparse_committed_length() -> CommonResult<()> {
        // Mirrors post-ResizeFile rewrite: master logical block len grows while
        // the worker still holds a short committed copy; finalize must extend
        // the staging file to the committed logical length.
        // Dataset Mem dir is only 100B and rewrite reserves max(old, new).
        let mut ds = create_data_set(true, "finalize-rewrite-sparse-len");
        let mut block = ExtendedBlock::with_mem(1, "50B")?;
        let writing = ds.open_block(&block)?;
        let initial_path = {
            let dir = ds.find_dir(writing.dir_id())?;
            FileLayout::block_path(dir, &writing)?
        };
        std::fs::write(&initial_path, b"B".repeat(20))?;
        block.len = 20;
        let finalized = ds.finalize_block(&block)?;
        let active_path = {
            let dir = ds.find_dir(finalized.dir_id())?;
            FileLayout::block_path(dir, &finalized)?
        };

        // Reopen for rewrite at a higher logical length (as after sparse resize),
        // write only into the middle, then commit the full logical length.
        block.len = 50;
        let rewriting = ds.open_block(&block)?;
        let staging_path = {
            let dir = ds.find_dir(rewriting.dir_id())?;
            FileLayout::block_path(dir, &rewriting)?
        };
        let mut staging = std::fs::OpenOptions::new()
            .write(true)
            .open(&staging_path)?;
        use std::io::{Seek, SeekFrom, Write};
        staging.seek(SeekFrom::Start(20))?;
        staging.write_all(b"D".repeat(10).as_slice())?;
        drop(staging);
        assert_eq!(std::fs::metadata(&staging_path)?.len(), 30);

        let published = ds.finalize_block(&block)?;
        assert!(published.is_final());
        assert_eq!(published.len(), 50);
        let bytes = std::fs::read(&active_path)?;
        assert_eq!(bytes.len(), 50);
        assert_eq!(&bytes[..20], b"B".repeat(20).as_slice());
        assert_eq!(&bytes[20..30], b"D".repeat(10).as_slice());
        assert!(!staging_path.exists());
        Ok(())
    }

    #[test]
    fn file_reader_uses_open_file_length_when_metadata_is_stale() -> CommonResult<()> {
        let mut ds = create_data_set(true, "reader-stale-physical-len");
        let mut block = ExtendedBlock::with_mem(1, "50B")?;
        let writing = ds.open_block(&block)?;
        ds.write_test_data(&writing, "20B")?;
        block.len = 20;
        let finalized = ds.finalize_block(&block)?;

        let mut stale = finalized.clone();
        stale.len = 50;
        let dir = ds.find_dir(stale.dir_id())?;
        let mut reader = FileLayout.open_reader(dir, &stale, 0, 50)?;
        let region = reader.read_region(false, 50)?;
        let bytes = match region {
            DataSlice::Buffer(bytes) => bytes,
            other => panic!("expected buffered sparse read, got {other:?}"),
        };

        assert_eq!(&bytes[..20], b"A".repeat(20).as_slice());
        assert!(bytes[20..].iter().all(|byte| *byte == 0));
        Ok(())
    }

    #[test]
    fn file_reader_clamps_physical_file_to_logical_length() -> CommonResult<()> {
        let mut ds = create_data_set(true, "reader-logical-length-boundary");
        let mut block = ExtendedBlock::with_mem(1, "50B")?;
        let writing = ds.open_block(&block)?;
        ds.write_test_data(&writing, "50B")?;
        block.len = 50;
        let finalized = ds.finalize_block(&block)?;

        let dir = ds.find_dir(finalized.dir_id())?;
        let mut reader = FileLayout.open_reader(dir, &finalized, 0, 20)?;
        let region = reader.read_region(false, 50)?;
        let bytes = match region {
            DataSlice::Buffer(bytes) => bytes,
            other => panic!("expected bounded buffered read, got {other:?}"),
        };

        assert_eq!(bytes.len(), 20);
        assert_eq!(&bytes[..], b"A".repeat(20).as_slice());
        Ok(())
    }

    #[test]
    fn remove_block_during_rewrite_deletes_active_and_staging_files() -> CommonResult<()> {
        let mut ds = create_data_set(true, "remove-during-rewrite");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let writing = ds.open_block(&block)?;
        ds.write_test_data(&writing, "40B")?;
        block.len = 40;
        let finalized = ds.finalize_block(&block)?;
        let active_path = {
            let dir = ds.find_dir(finalized.dir_id())?;
            FileLayout::block_path(dir, &finalized)?
        };

        block.len = 60;
        let rewriting = ds.open_block(&block)?;
        let staging_path = {
            let dir = ds.find_dir(rewriting.dir_id())?;
            FileLayout::block_path(dir, &rewriting)?
        };
        ds.remove_block(&block)?;

        assert!(ds.get_block(block.id).is_none());
        assert!(!active_path.exists());
        assert!(!staging_path.exists());
        assert!(ds.committed_rewrites.is_empty());
        Ok(())
    }
    #[test]
    fn spdk_initialize_restores_physical_capacity() -> CommonResult<()> {
        let path = "../testing/spdk_capacity_restore";
        let _ = std::fs::remove_dir_all(path);
        let store = Arc::new(SpdkMetaStore::open(path, true)?);
        store.put(1, 1, 0, 4096, 1, true)?;

        let dir = spdk_dir(1, spdk_state());
        let available = dir.available();
        let mut ds = VfsDataset::new("t", DirList::new(vec![dir])?, Some(store.clone()))?;

        let meta = ds.get_block(1).unwrap();
        assert_eq!(meta.len(), 1);
        assert_eq!(meta.physical_bytes(), 4096);
        assert_eq!(ds.available(), available - 4096);

        ds.abort_block(&ExtendedBlock::with_id(1))?;
        assert_eq!(ds.available(), available);
        drop(ds);
        drop(store);
        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }
    #[test]
    fn negative_file_prepare_write_keeps_metadata() -> CommonResult<()> {
        let mut ds = create_data_set(true, "negative-prepare-write");
        let mut block = ExtendedBlock::with_mem(1, "100B")?;
        let meta = ds.open_block(&block)?;
        ds.write_test_data(&meta, "50B")?;
        block.len = 50;
        let finalized = ds.finalize_block(&block)?;
        let available = ds.available();

        block.len = -1;
        assert!(ds.open_block(&block).is_err());

        let current = ds.get_block(block.id).unwrap();
        assert!(current.is_final());
        assert_eq!(current.len(), finalized.len());
        assert_eq!(current.physical_bytes(), finalized.physical_bytes());
        assert_eq!(ds.available(), available);
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
        let st = spdk_state();
        let mut ds = VfsDataset::new("t", DirList::new(vec![spdk_dir(1, st.clone())])?, None)?;
        let b1 = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        let b2 = ExtendedBlock::new(2, 4096, StorageType::SpdkDisk, FileType::File);
        let b3 = ExtendedBlock::new(3, 4096, StorageType::SpdkDisk, FileType::File);
        ds.open_block(&b1)?;
        ds.open_block(&b2)?;
        ds.open_block(&b3)?;

        ds.abort_block(&b2)?;
        let snap = st.offset_alloc.snapshot();

        let st2 = spdk_state();
        st2.offset_alloc.restore(&snap);

        let entries = st2.offset_alloc.free_list_entries();
        assert_eq!(entries.len(), 1, "expected 1 free entry, got {:?}", entries);
        assert_eq!(entries[0], (4096, 4096));
        assert_eq!(st2.offset_alloc.free_list_bytes(), 4096);

        assert_eq!(st2.offset_alloc.allocate(4, 4096).unwrap(), 4096);
        Ok(())
    }
}
