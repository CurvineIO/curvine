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
    BlockDataset, BlockLayout, BlockReadContext, BlockWriteContext, Dataset,
};
use crate::worker::Worker;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_model::{ExtendedBlock, StorageInfo};
use parking_lot::{Mutex, MutexGuard};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

const BLOCK_LOCK_STRIPES: usize = 256;

#[derive(Clone)]
pub struct BlockStore {
    state: Arc<RwLock<BlockDataset>>,
    block_locks: Arc<Vec<Mutex<()>>>,
}

impl BlockStore {
    pub fn new(cluster_id: &str, conf: &ClusterConf) -> CommonResult<Self> {
        let dataset = BlockDataset::from_conf(cluster_id, conf)?;
        let block_store = BlockStore {
            state: Arc::new(RwLock::new(dataset)),
            block_locks: Arc::new((0..BLOCK_LOCK_STRIPES).map(|_| Mutex::new(())).collect()),
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

    fn block_lock(&self, id: i64, operation: &str) -> MutexGuard<'_, ()> {
        let started = Instant::now();
        let index = (id as u64 as usize) % self.block_locks.len();
        let guard = self.block_locks[index].lock();
        self.observe_stripe_lock_wait(operation, started.elapsed());
        guard
    }

    fn with_dataset_write<T>(
        &self,
        operation: &str,
        action: impl FnOnce(&mut BlockDataset) -> CommonResult<T>,
    ) -> CommonResult<T> {
        let waiting = Instant::now();
        let mut state = self.write()?;
        let wait_elapsed = waiting.elapsed();
        let holding = Instant::now();
        let result = action(&mut state);
        let hold_elapsed = holding.elapsed();
        drop(state);

        self.observe_dataset_lock_wait(operation, wait_elapsed);
        self.observe_dataset_lock_hold(operation, hold_elapsed);
        result
    }

    fn observe_stripe_lock_wait(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .block_store_stripe_lock_wait_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    fn observe_dataset_lock_wait(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .block_dataset_write_lock_wait_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    fn observe_dataset_lock_hold(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .block_dataset_write_lock_hold_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    fn observe_file_layout_operation(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .file_layout_operation_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    pub fn open_block(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let _block_lock = self.block_lock(block.id, "open");
        let reservation =
            self.with_dataset_write("open", |state| state.reserve_file_open(block))?;

        let Some(reservation) = reservation else {
            return self.with_dataset_write("open", |state| state.open_block(block));
        };

        let started = Instant::now();
        let prepared = reservation.prepare(block);
        self.observe_file_layout_operation("open", started.elapsed());
        match prepared {
            Ok(meta) => self
                .with_dataset_write("open", |state| state.complete_file_open(&reservation, meta)),
            Err(error) => {
                if let Err(rollback) =
                    self.with_dataset_write("open", |state| state.rollback_file_open(&reservation))
                {
                    log::error!(
                        "failed to roll back block {} allocation after {}: {}",
                        block.id,
                        error,
                        rollback
                    );
                }
                Err(error)
            }
        }
    }

    pub fn finalize_block(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let _block_lock = self.block_lock(block.id, "finalize");
        let reservation =
            self.with_dataset_write("finalize", |state| state.reserve_file_finalize(block))?;

        let Some(reservation) = reservation else {
            return self.with_dataset_write("finalize", |state| state.finalize_block(block));
        };

        let started = Instant::now();
        let plan = reservation.prepare(block.len);
        self.observe_file_layout_operation("finalize", started.elapsed());
        let result = match plan {
            Ok(plan) => self.with_dataset_write("finalize", |state| {
                state.publish_file_finalize(&reservation, plan)
            }),
            Err(error) => Err(error),
        };
        match result {
            Ok(meta) => Ok(meta),
            Err(error) => {
                if let Err(rollback) = self.with_dataset_write("finalize", |state| {
                    state.rollback_file_finalize(&reservation)
                }) {
                    log::error!(
                        "failed to roll back block {} finalization after {}: {}",
                        block.id,
                        error,
                        rollback
                    );
                }
                Err(error)
            }
        }
    }

    pub fn abort_block(&self, block: &ExtendedBlock) -> CommonResult<()> {
        let _block_lock = self.block_lock(block.id, "abort");
        self.with_dataset_write("abort", |state| state.abort_block(block))
    }

    pub fn get_block(&self, id: i64) -> CommonResult<BlockMeta> {
        let state = self.read()?;
        let b = state
            .get_readable_block(id)
            .ok_or_else(|| curvine_core_error::err_msg!(format!("block {} not exists", id)))?;
        Ok(b.clone())
    }

    pub fn open_writer(&self, meta: &BlockMeta, off: i64) -> CommonResult<BlockWriteContext> {
        let (layout, dir) = {
            let state = self.read()?;
            state.layout_for(meta)?
        };
        layout.open_writer(&dir, meta, off).map_err(Into::into)
    }

    pub fn open_reader(
        &self,
        meta: &BlockMeta,
        off: i64,
        logical_len: i64,
    ) -> CommonResult<BlockReadContext> {
        let (layout, dir) = {
            let state = self.read()?;
            state.layout_for(meta)?
        };
        layout
            .open_reader(&dir, meta, off, logical_len)
            .map_err(Into::into)
    }

    /// Opens the currently readable generation while the dataset read lock is
    /// held, so a finalize publish cannot replace its path before the FD opens.
    /// `logical_len` may exceed the worker's physical bytes for sparse tails.
    pub fn open_reader_by_id(
        &self,
        id: i64,
        off: i64,
        logical_len: i64,
    ) -> CommonResult<(BlockMeta, BlockReadContext)> {
        self.open_reader_by_id_inner(id, off, Some(logical_len))
    }

    /// Opens the currently readable generation using its stored metadata length
    /// as the logical read boundary. This is intended for internal block copies
    /// that do not have a client-provided logical length.
    pub fn open_reader_by_id_at_stored_len(
        &self,
        id: i64,
        off: i64,
    ) -> CommonResult<(BlockMeta, BlockReadContext)> {
        self.open_reader_by_id_inner(id, off, None)
    }

    fn open_reader_by_id_inner(
        &self,
        id: i64,
        off: i64,
        logical_len: Option<i64>,
    ) -> CommonResult<(BlockMeta, BlockReadContext)> {
        let state = self.read()?;
        let meta = state
            .get_readable_block(id)
            .ok_or_else(|| curvine_core_error::err_msg!(format!("block {} not exists", id)))?
            .clone();
        let logical_len = logical_len.unwrap_or_else(|| meta.len());
        let (layout, dir) = state.layout_for(&meta)?;
        let reader = layout.open_reader(&dir, &meta, off, logical_len)?;
        Ok((meta, reader))
    }

    /// Returns a local path only while no finalize publication is in progress.
    /// A finalizing file can be atomically renamed before a co-located client
    /// opens the returned path, so callers must use `open_reader_by_id` then.
    pub fn short_circuit_by_id(&self, id: i64) -> CommonResult<Option<(BlockMeta, String, i64)>> {
        let state = self.read()?;
        if matches!(
            state.get_block(id).map(BlockMeta::state),
            Some(BlockState::Finalizing)
        ) {
            return Ok(None);
        }
        let meta = state
            .get_readable_block(id)
            .ok_or_else(|| curvine_core_error::err_msg!(format!("block {} not exists", id)))?
            .clone();
        let (layout, dir) = state.layout_for(&meta)?;
        let Some(path) = layout.short_circuit(&dir, &meta)? else {
            return Ok(None);
        };
        let physical_len = std::fs::metadata(&path)?.len() as i64;
        Ok(Some((meta, path, physical_len)))
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
        let _block_lock = self.block_lock(id, "remove");
        let block = ExtendedBlock::with_id(id);
        self.with_dataset_write("remove", |state| state.remove_block(&block))
    }

    // Asynchronously delete block.
    pub fn async_remove_block(&self, id: i64) -> CommonResult<Option<BlockMeta>> {
        let _block_lock = self.block_lock(id, "remove");
        let removed = self.with_dataset_write("remove", |state| {
            let remove_result = match state.get_block(id) {
                Some(_) => state.remove_block_state_by_id(id).map(Some),
                None => Ok(None),
            };
            // Heartbeat increments this counter before scheduling the task.
            // Consume it even when metadata removal reports an error.
            state.decrement_blocks_to_delete();
            remove_result
        })?;

        let Some(removed) = removed else {
            return Ok(None);
        };

        if let Err(e) = removed.deallocate() {
            self.with_dataset_write("restore", |state| {
                state.restore_removed_block(removed);
                Ok(())
            })?;
            return Err(e);
        }

        removed.release();
        Ok(Some(removed.meta))
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
    use crate::worker::block::BlockState;
    use crate::worker::storage::{Dataset, FileLayout};
    use curvine_config::WorkerConf;
    use curvine_io::DataSlice;
    use curvine_runtime::common::FileUtils;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Instant;

    fn create_store(name: &str) -> CommonResult<BlockStore> {
        create_store_with_capacity(name, "1KB")
    }

    fn create_store_with_capacity(name: &str, capacity: &str) -> CommonResult<BlockStore> {
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                dir_reserved: "0".to_string(),
                data_dir: vec![format!("[MEM:{capacity}]../testing/block-store-{name}")],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        BlockStore::new("test", &conf)
    }

    fn write_block(path: &str, bytes: usize) -> CommonResult<()> {
        const CHUNK_SIZE: usize = 1024 * 1024;

        let mut file = std::fs::File::create(path)?;
        let chunk = vec![0_u8; CHUNK_SIZE];
        let mut remaining = bytes;
        while remaining > 0 {
            let len = remaining.min(CHUNK_SIZE);
            file.write_all(&chunk[..len])?;
            remaining -= len;
        }
        file.sync_all()?;
        Ok(())
    }

    struct DeleteDenied {
        parent: PathBuf,
        permissions: std::fs::Permissions,
    }

    impl Drop for DeleteDenied {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.parent, self.permissions.clone());
        }
    }

    fn deny_block_delete(store: &BlockStore) -> CommonResult<DeleteDenied> {
        let mut block = ExtendedBlock::with_id(1);
        block.len = 100;
        let meta = finalize_block(store, &block)?;
        let path = store.short_circuit(&meta)?.unwrap();
        let parent = PathBuf::from(path).parent().unwrap().to_path_buf();
        let permissions = std::fs::metadata(&parent)?.permissions();
        let mut readonly = permissions.clone();
        readonly.set_mode(0o500);
        std::fs::set_permissions(&parent, readonly)?;
        Ok(DeleteDenied {
            parent,
            permissions,
        })
    }

    #[test]
    fn async_remove_missing_block_releases_delete_count() -> CommonResult<()> {
        let store = create_store("missing-block")?;
        store.read()?.increment_blocks_to_delete();

        assert!(store.async_remove_block(1)?.is_none());
        assert_eq!(store.read()?.num_blocks_to_delete(), 0);
        Ok(())
    }

    #[test]
    fn async_remove_invalid_dir_keeps_block_for_retry() -> CommonResult<()> {
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
        assert!(state.get_block(1).is_some());
        assert_eq!(state.num_blocks_to_delete(), 0);
        Ok(())
    }

    #[test]
    fn async_remove_deallocate_error_keeps_block_for_retry() -> CommonResult<()> {
        let store = create_store("deallocate-error")?;
        let _delete_denied = deny_block_delete(&store)?;
        store.read()?.increment_blocks_to_delete();

        let result = store.async_remove_block(1);
        assert!(result.is_err());
        let state = store.read()?;
        assert!(state.get_block(1).is_some());
        assert_eq!(state.num_blocks_to_delete(), 0);
        Ok(())
    }

    #[test]
    fn remove_deallocate_error_keeps_block_for_retry() -> CommonResult<()> {
        let store = create_store("remove-deallocate-error")?;
        let _delete_denied = deny_block_delete(&store)?;

        let result = store.remove_block(1);
        assert!(result.is_err());
        assert!(store.read()?.get_block(1).is_some());
        Ok(())
    }

    fn finalize_block(store: &BlockStore, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let writing = store.open_block(block)?;
        let path = store
            .short_circuit(&writing)?
            .expect("file layout must expose a local path");
        write_block(&path, block.len as usize)?;
        store.finalize_block(block)
    }

    fn read_bytes(reader: &mut BlockReadContext, len: i32) -> CommonResult<Vec<u8>> {
        let slice = reader.read_region(false, len)?;
        match slice {
            DataSlice::Bytes(bytes) => Ok(bytes.to_vec()),
            DataSlice::Buffer(bytes) => Ok(bytes.to_vec()),
            other => curvine_core_error::err_box!("unexpected test read slice: {:?}", other),
        }
    }

    #[test]
    fn failed_file_rewrite_restores_committed_block() -> CommonResult<()> {
        let store = create_store_with_capacity("rewrite-rollback", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let finalized = finalize_block(&store, &block)?;
        let available = store.read()?.available();
        let (_, dir) = store.read()?.layout_for(&finalized)?;
        let staging = BlockMeta::new(block.id, block.len, &dir);
        let staging_path = FileLayout::block_path(&dir, &staging)?;
        std::fs::create_dir(&staging_path)?;

        assert!(store.open_block(&block).is_err());
        let restored = store.get_block(block.id)?;
        assert!(restored.is_final());
        assert_eq!(restored.len(), finalized.len());
        assert_eq!(store.read()?.available(), available);

        FileUtils::delete_path(staging_path, true)?;
        Ok(())
    }

    #[test]
    fn file_rewrite_requires_capacity_for_committed_copy() -> CommonResult<()> {
        let store = create_store_with_capacity("rewrite-capacity", "1MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let finalized = finalize_block(&store, &block)?;
        let available = store.read()?.available();

        assert!(store.open_block(&block).is_err());
        assert_eq!(store.get_block(block.id)?.state(), &BlockState::Finalized);
        assert_eq!(store.get_block(block.id)?.len(), finalized.len());
        assert_eq!(store.read()?.available(), available);
        Ok(())
    }

    #[test]
    fn failed_file_finalize_restores_writing_state() -> CommonResult<()> {
        let store = create_store_with_capacity("finalize-rollback", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let writing = store.open_block(&block)?;
        let staging_path = store
            .short_circuit(&writing)?
            .expect("file layout must expose a local path");
        write_block(&staging_path, block.len as usize)?;

        let (_, dir) = store.read()?.layout_for(&writing)?;
        let mut final_meta = writing.clone();
        final_meta.state = BlockState::Finalized;
        let active_path = FileLayout::block_path(&dir, &final_meta)?;
        std::fs::create_dir(&active_path)?;

        assert!(store.finalize_block(&block).is_err());
        assert_eq!(store.get_block(block.id)?.state(), &BlockState::Writing);

        FileUtils::delete_path(&active_path, true)?;
        assert!(store.finalize_block(&block)?.is_final());
        Ok(())
    }

    #[test]
    fn finalizing_new_block_remains_reportable_and_readable() -> CommonResult<()> {
        let store = create_store_with_capacity("finalizing-report", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        store.open_block(&block)?;

        let reservation = store
            .write()?
            .reserve_file_finalize(&block)?
            .expect("file block finalize must reserve");
        let report = store.all_blocks()?;
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].state(), &BlockState::Finalizing);
        assert_eq!(store.get_block(block.id)?.state(), &BlockState::Finalizing);
        assert!(store.short_circuit_by_id(block.id)?.is_none());

        store.write()?.rollback_file_finalize(&reservation)?;
        Ok(())
    }

    #[test]
    fn short_circuit_reports_live_file_length() -> CommonResult<()> {
        let store = create_store_with_capacity("short-circuit-live-length", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "50B")?;
        let finalized = finalize_block(&store, &block)?;
        let path = store
            .short_circuit(&finalized)?
            .expect("file layout must expose a local path");
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(20)?;

        let (meta, _, physical_len) = store
            .short_circuit_by_id(block.id)?
            .expect("finalized file must support short-circuit reads");
        assert_eq!(meta.len(), 50);
        assert_eq!(physical_len, 20);
        Ok(())
    }

    #[test]
    fn reader_opened_before_publish_keeps_committed_generation() -> CommonResult<()> {
        let store = create_store_with_capacity("finalize-reader-generation", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "4B")?;
        let first = store.open_block(&block)?;
        let first_path = store
            .short_circuit(&first)?
            .expect("file layout must expose a local path");
        std::fs::write(first_path, b"old!")?;
        store.finalize_block(&block)?;

        let rewrite = store.open_block(&block)?;
        let rewrite_path = store
            .short_circuit(&rewrite)?
            .expect("file layout must expose a local path");
        std::fs::write(rewrite_path, b"new!")?;
        let reservation = store
            .write()?
            .reserve_file_finalize(&block)?
            .expect("rewrite finalize must reserve");
        let plan = reservation.prepare(block.len)?;

        let (_, mut committed_reader) = store.open_reader_by_id_at_stored_len(block.id, 0)?;
        store.write()?.publish_file_finalize(&reservation, plan)?;
        assert_eq!(read_bytes(&mut committed_reader, 4)?, b"old!");

        let (_, mut published_reader) = store.open_reader_by_id_at_stored_len(block.id, 0)?;
        assert_eq!(read_bytes(&mut published_reader, 4)?, b"new!");
        Ok(())
    }

    #[test]
    fn stored_len_reader_reads_finalized_block() -> CommonResult<()> {
        let store = create_store_with_capacity("stored-len-reader", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "4B")?;
        finalize_block(&store, &block)?;

        let (meta, mut reader) = store.open_reader_by_id_at_stored_len(block.id, 0)?;
        assert_eq!(meta.len(), block.len);
        assert_eq!(read_bytes(&mut reader, block.len as i32)?, vec![0; 4]);
        Ok(())
    }

    #[test]
    fn concurrent_open_of_same_block_keeps_single_writing_meta() -> CommonResult<()> {
        const WRITERS: usize = 4;

        let store = create_store_with_capacity("same-block-open", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let barrier = Arc::new(Barrier::new(WRITERS + 1));
        let mut workers = Vec::with_capacity(WRITERS);
        for _ in 0..WRITERS {
            let store = store.clone();
            let block = block.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                store.open_block(&block)
            }));
        }

        barrier.wait();
        for worker in workers {
            let meta = worker.join().expect("open worker panicked")?;
            assert_eq!(meta.id(), block.id);
            assert_eq!(meta.state(), &BlockState::Writing);
        }

        assert_eq!(store.all_blocks()?.len(), 1);
        store.abort_block(&block)?;
        assert!(store.all_blocks()?.is_empty());
        Ok(())
    }

    #[test]
    #[ignore = "manual benchmark: run with --ignored --nocapture"]
    fn measure_concurrent_file_rewrites() -> CommonResult<()> {
        const BLOCKS: i64 = 4;
        const BLOCK_BYTES: usize = 32 * 1024 * 1024;

        let store = create_store_with_capacity("concurrent-rewrites", "1GB")?;
        let blocks: Vec<ExtendedBlock> = (0..BLOCKS)
            .map(|id| ExtendedBlock::with_mem(id, "32MB"))
            .collect::<CommonResult<_>>()?;

        for block in &blocks {
            let meta = store.open_block(block)?;
            let path = store
                .short_circuit(&meta)?
                .expect("file layout must expose a local path");
            write_block(&path, BLOCK_BYTES)?;
            store.finalize_block(block)?;
        }

        let barrier = Arc::new(Barrier::new(BLOCKS as usize + 1));
        let mut workers = Vec::with_capacity(BLOCKS as usize);
        for block in blocks.clone() {
            let store = store.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                let started = Instant::now();
                let result = store.open_block(&block);
                let elapsed = started.elapsed();
                if result.is_ok() {
                    store.abort_block(&block)?;
                }
                Ok::<_, curvine_core_error::CommonError>(elapsed)
            }));
        }

        barrier.wait();
        let started = Instant::now();
        let elapsed: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("rewrite worker panicked"))
            .collect::<CommonResult<_>>()?;
        let wall_clock = started.elapsed();
        let max_latency = elapsed
            .into_iter()
            .max()
            .expect("benchmark has at least one block");

        eprintln!(
            "CONCURRENT_FILE_REWRITE_BENCH blocks={BLOCKS} bytes_per_block={BLOCK_BYTES} wall_clock_us={} max_open_us={}",
            wall_clock.as_micros(),
            max_latency.as_micros(),
        );
        Ok(())
    }
}
