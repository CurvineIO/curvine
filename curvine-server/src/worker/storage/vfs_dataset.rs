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
use crate::worker::storage::{Dataset, DirList, StorageVersion, VfsDir};
use curvine_common::conf::{ClusterConf, WorkerDataDir};
use curvine_common::state::ExtendedBlock;
use indexmap::map::Values;
use log::info;
use orpc::common::{ByteUnit, FileUtils, LocalTime, TimeSpent};
use orpc::{err_box, try_err, CommonResult};
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct VfsDataset {
    cluster_id: String,
    worker_id: u32,
    ctime: u64,
    dir_list: DirList,
    pub(crate) block_map: HashMap<i64, BlockMeta>,
    num_blocks_to_delete: AtomicUsize,
}

impl VfsDataset {
    fn new(cluster_id: &str, dir_list: DirList) -> Self {
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
        };
        ds.initialize();
        ds
    }

    pub fn from_conf(cluster_id: &str, conf: &ClusterConf) -> CommonResult<Self> {
        let mut dir_list = DirList::new(vec![]);
        let dir_reserved = ByteUnit::from_str(&conf.worker.dir_reserved)?.as_byte();

        let mut worker_id: Option<u32> = None;
        for s in &conf.worker.data_dir {
            let data_dir = WorkerDataDir::from_str(s)?;
            let storage_path = data_dir.storage_path(&conf.cluster_id);
            if conf.format_worker && FileUtils::exists(&storage_path) {
                FileUtils::delete_path(&storage_path, true)?;
                info!("Delete(format) data dir {}", storage_path);
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

        Ok(Self::new(cluster_id, dir_list))
    }

    // Initialize.
    // 1. Scan all blocks in the directory
    // 2. Block is added to block_map.
    // 3. Update capacity usage.
    fn initialize(&mut self) {
        let spent = TimeSpent::new();
        for dir in self.dir_list.dir_iter() {
            let blocks = dir.scan_blocks().unwrap();
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
                    let new_meta = BlockMeta::new(meta.id, block.len, dir);

                    dir.release_space(meta.is_final(), meta.actual_len);
                    dir.reserve_space(false, new_meta.len);
                    self.block_map.insert(new_meta.id(), new_meta.clone());

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
        let final_meta = dir.finalize_block(meta)?;
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

        Ok(final_meta)
    }

    fn abort_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        let meta = match self.block_map.remove(&block.id) {
            None => return err_box!("block {} not exists", block.id),
            Some(v) => v,
        };

        let dir = self.find_dir(meta.dir_id())?;
        let file = meta.get_block_path()?;
        try_err!(fs::remove_file(file));
        dir.release_space(meta.is_final(), meta.actual_len);
        Ok(())
    }

    fn remove_block(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        self.abort_block(block)
    }
}

#[cfg(test)]
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
        let mut dataset = create_data_set(true, "sample");
        // println!("{:#?}", dataset.dir_list.dirs());

        let mut block = ExtendedBlock::with_mem(11226688, "100B")?;
        let tmp_meta = dataset.open_block(&block)?;
        assert_eq!(dataset.available(), 400);
        assert_eq!(
            dataset.get_block_check(block.id)?.state,
            BlockState::Writing
        );

        // commit block
        tmp_meta.write_test_data("50B")?;
        block.len = 50;
        let final_meta = dataset.finalize_block(&block)?;
        assert_eq!(dataset.available(), 450);
        assert!(final_meta.get_block_path()?.exists());
        assert_eq!(
            dataset.get_block_check(block.id)?.state,
            BlockState::Finalized
        );

        // abort block。
        let block = ExtendedBlock::with_mem(11226699, "100B")?;
        let tmp_meta1 = dataset.open_block(&block)?;
        assert_eq!(dataset.available(), 350);
        assert_eq!(dataset.available(), 350);
        tmp_meta1.write_test_data("50B")?;

        dataset.abort_block(&block)?;
        assert_eq!(dataset.available(), 450);
        assert!(!tmp_meta1.get_block_path()?.exists());

        Ok(())
    }

    #[test]
    fn append() -> CommonResult<()> {
        let mut dataset = create_data_set(true, "append");
        let mut block = ExtendedBlock::with_mem(11226688, "100B")?;

        let tmp_meta = dataset.open_block(&block)?;
        tmp_meta.write_test_data("50B")?;
        block.len = 50;
        dataset.finalize_block(&block)?;
        assert_eq!(
            dataset.get_block_check(block.id)?.state,
            BlockState::Finalized
        );

        // append
        block.len = 100;
        let new_meta = dataset.open_block(&block)?;
        assert_eq!(dataset.available(), 400);
        assert_eq!(
            dataset.get_block_check(block.id)?.state,
            BlockState::Writing
        );

        new_meta.write_test_data("20B")?;
        block.len = 70;
        dataset.finalize_block(&block)?;
        assert_eq!(dataset.available(), 430);
        assert_eq!(
            dataset.get_block_check(block.id)?.state,
            BlockState::Finalized
        );

        Ok(())
    }

    #[test]
    fn initialize() -> CommonResult<()> {
        let mut dataset = create_data_set(true, "initialize");
        for id in 1..12 {
            let size = format!("{}B", id);
            let block = ExtendedBlock::with_mem(id, &size)?;
            let tmp_meta = dataset.open_block(&block)?;
            tmp_meta.write_test_data(&size)?;
        }

        drop(dataset);

        let dataset = create_data_set(false, "initialize");
        println!("block_map {:?}", dataset.block_map);

        assert_eq!(11, dataset.block_map.len());
        for meta in dataset.block_map.values() {
            assert_eq!(meta.state, BlockState::Finalized);
            assert_eq!(meta.len, meta.id);
        }

        Ok(())
    }
}
