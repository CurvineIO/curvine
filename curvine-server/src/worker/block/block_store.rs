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
use crate::worker::storage::{
    BlockDataset, BlockLayout, BlockReadContext, BlockWriteContext, Dataset,
};
use curvine_common::conf::ClusterConf;
use curvine_common::state::{ExtendedBlock, StorageInfo};
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

    pub fn open_writer(&self, meta: &BlockMeta, off: i64) -> CommonResult<BlockWriteContext> {
        let (layout, dir) = {
            let state = self.read()?;
            state.layout_for(meta)?
        };
        layout.open_writer(&dir, meta, off).map_err(Into::into)
    }

    pub fn open_reader(&self, meta: &BlockMeta, off: i64) -> CommonResult<BlockReadContext> {
        let (layout, dir) = {
            let state = self.read()?;
            state.layout_for(meta)?
        };
        layout.open_reader(&dir, meta, off).map_err(Into::into)
    }

    pub fn short_circuit(&self, meta: &BlockMeta) -> CommonResult<Option<String>> {
        let (layout, dir) = {
            let state = self.read()?;
            state.layout_for(meta)?
        };
        layout.short_circuit(&dir, meta)
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
        let removed = {
            let mut state = self.write()?;
            let remove_result = state.remove_block_state_by_id(id);
            // Heartbeat increments this counter before scheduling the task.
            // Consume it even when metadata removal reports an error.
            state.decrement_blocks_to_delete();
            remove_result
        }?;

        removed.layout.deallocate(&removed.dir, &removed.meta)?;
        removed.release_space();
        Ok(removed.meta)
    }

    // Get all storage information and check whether the storage directory is normal.
    // If the directory is not normal, the storage will be marked as failed.
    // This method is called by the heartbeat thread and returns all storage information, including failed storage.
    pub fn get_and_check_storages(&self) -> CommonResult<Vec<StorageInfo>> {
        let state = self.read()?;
        Ok(state.get_and_check_storages())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::storage::Dataset;
    use curvine_common::conf::WorkerConf;

    fn create_store(name: &str) -> CommonResult<BlockStore> {
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                dir_reserved: "0".to_string(),
                data_dir: vec![format!("[MEM:1KB]../testing/block-store-{name}")],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        BlockStore::new("test", &conf)
    }

    #[test]
    fn async_remove_missing_block_releases_delete_count() -> CommonResult<()> {
        let store = create_store("missing-block")?;
        store.read()?.increment_blocks_to_delete();

        assert!(store.async_remove_block(1).is_err());
        assert_eq!(store.read()?.num_blocks_to_delete(), 0);
        Ok(())
    }

    #[test]
    fn async_remove_invalid_dir_releases_delete_count() -> CommonResult<()> {
        let store = create_store("invalid-dir")?;
        {
            let mut state = store.write()?;
            let mut meta = BlockMeta::new(1, 100, state.dir_iter().next().unwrap());
            meta.dir_id = u32::MAX;
            state.put_test_meta(meta);
            state.increment_blocks_to_delete();
        }

        assert!(store.async_remove_block(1).is_err());
        let state = store.read()?;
        assert!(state.get_block(1).is_none());
        assert_eq!(state.num_blocks_to_delete(), 0);
        Ok(())
    }
}
