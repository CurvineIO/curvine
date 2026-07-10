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

use crate::worker::block::BlockMeta;
use crate::worker::storage::{BlockDataset, BlockLayout, Dataset};
use curvine_common::conf::ClusterConf;
use curvine_common::state::{ExtendedBlock, StorageInfo};
use log::error;
use orpc::CommonResult;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct BlockStore {
    state: Arc<RwLock<BlockDataset>>,
}

impl BlockStore {
    pub fn new(cluster_id: &str, conf: &ClusterConf) -> CommonResult<Self> {
        let dataset = BlockDataset::from_conf(cluster_id, conf)?;
        let block_store = BlockStore {
            state: Arc::new(RwLock::new(dataset)),
        };

        Ok(block_store)
    }

    pub(crate) fn write(&self) -> CommonResult<RwLockWriteGuard<'_, BlockDataset>> {
        match self.state.write() {
            Ok(state) => Ok(state),
            Err(e) => {
                log::error!("fatal block store write lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    pub(crate) fn read(&self) -> CommonResult<RwLockReadGuard<'_, BlockDataset>> {
        match self.state.read() {
            Ok(state) => Ok(state),
            Err(e) => {
                log::error!("fatal block store read lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    pub fn open_block(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        self.write()?.open_block(block)
    }

    pub fn finalize_block(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        self.write()?.finalize_block(block)
    }

    pub fn abort_block(&self, block: &ExtendedBlock) -> CommonResult<()> {
        self.write()?.abort_block(block)
    }

    pub fn get_block(&self, id: i64) -> CommonResult<BlockMeta> {
        let state = self.read()?;
        let b = state.get_block_check(id)?;
        Ok(b.clone())
    }

    pub fn worker_id(&self) -> CommonResult<u32> {
        let state = self.read()?;
        Ok(state.worker_id())
    }

    pub fn cluster_id(&self) -> CommonResult<String> {
        let state = self.read()?;
        Ok(state.cluster_id().to_string())
    }

    pub fn all_blocks(&self) -> CommonResult<Vec<BlockMeta>> {
        let state = self.read()?;
        Ok(state.all_blocks())
    }

    pub fn remove_block(&self, id: i64) -> CommonResult<()> {
        let mut state = self.write()?;
        let block = ExtendedBlock::with_id(id);
        state.remove_block(&block)
    }

    // Asynchronously delete block.
    pub fn async_remove_block(&self, id: i64) -> CommonResult<BlockMeta> {
        let (meta, layout) = {
            let mut state = self.write()?;
            let (meta, layout) = state.remove_block_state_by_id(id)?;
            state.decrement_blocks_to_delete();
            (meta, layout)
        };

        layout.deallocate(&meta)?;
        let state = self.read()?;
        state.release_block_space(&meta)?;
        Ok(meta)
    }

    // Get all storage information and check whether the storage directory is normal.
    // If the directory is not normal, the storage will be marked as failed.
    // This method is called by the heartbeat thread and returns all storage information, including failed storage.
    pub fn get_and_check_storages(&self) -> CommonResult<Vec<StorageInfo>> {
        let state = self.read()?;
        let mut vec = vec![];
        for item in state.dir_iter() {
            let failed = match item.check_dir() {
                Ok(_) => false,
                Err(e) => {
                    error!("check_dir {}: {}", item.id(), e);
                    item.set_failed();
                    true
                }
            };
            let info = StorageInfo {
                dir_id: item.id(),
                storage_id: item.version().storage_id.to_string(),
                failed,
                capacity: item.capacity(),
                fs_used: item.fs_used(),
                non_fs_used: item.non_fs_used(),
                available: item.available(),
                reserved_bytes: item.reserved_bytes(),
                storage_type: item.storage_type(),
                block_num: state.num_blocks() as i64,
                dir_path: item.path_str().to_string(),
            };
            vec.push(info);
        }

        Ok(vec)
    }
}
