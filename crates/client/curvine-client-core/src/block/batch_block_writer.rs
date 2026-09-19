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

use crate::block::batch_block_writer::BatchWriterAdapter::{BatchLocal, BatchRemote};
use crate::block::{BatchBlockWriterLocal, BatchBlockWriterRemote, CreateBlockContext};
use crate::file::FsContext;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{
    BlockLocation, CommitBlock, ExtendedBlock, LocatedBlock, StorageType, WorkerAddress,
};
use futures::future::try_join_all;
use futures::stream::{self, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const MAX_CONCURRENT_WORKER_GROUPS: usize = 16;

pub(super) fn validate_batch_contexts(
    blocks: &[ExtendedBlock],
    contexts: &[CreateBlockContext],
) -> FsResult<()> {
    if contexts.len() != blocks.len() {
        return err_box!(
            "batch block response count mismatch, expected {}, actual {}",
            blocks.len(),
            contexts.len()
        );
    }
    for (block, context) in blocks.iter().zip(contexts) {
        if context.id != block.id {
            return err_box!(
                "batch block response id mismatch, expected {}, actual {}",
                block.id,
                context.id
            );
        }
    }
    Ok(())
}

enum BatchWriterAdapter {
    BatchLocal(BatchBlockWriterLocal),
    BatchRemote(BatchBlockWriterRemote),
}

impl BatchWriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            BatchLocal(f) => f.worker_address(),
            BatchRemote(f) => f.worker_address(),
        }
    }

    fn actual_storage_type(&self, block_index: usize) -> StorageType {
        match self {
            BatchLocal(f) => f.actual_storage_type(block_index),
            BatchRemote(f) => f.actual_storage_type(block_index),
        }
    }

    async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        match self {
            BatchLocal(f) => f.write(files).await,
            BatchRemote(f) => f.write(files).await,
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        match self {
            BatchLocal(f) => f.flush().await,
            BatchRemote(f) => f.flush().await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            BatchLocal(f) => f.complete().await,
            BatchRemote(f) => f.complete().await,
        }
    }

    async fn cancel(&mut self) -> FsResult<()> {
        match self {
            BatchLocal(f) => f.cancel().await,
            BatchRemote(f) => f.cancel().await,
        }
    }

    // Create new WriterAdapter
    async fn new(
        fs_context: Arc<FsContext>,
        located_blocks: Vec<LocatedBlock>,
        worker_addr: &WorkerAddress,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        // SPDK bypasses kernel — no local path. Disable short-circuit if any block uses SPDK.
        // Use has_spdk from worker-reported actual storage type, not block.storage_type
        let has_spdk = located_blocks.iter().any(|lb| lb.has_spdk);
        let short_circuit =
            conf.short_circuit && fs_context.is_local_worker(worker_addr) && !has_spdk;

        let blocks: Vec<ExtendedBlock> = located_blocks.iter().map(|lb| lb.block.clone()).collect();
        let adapter = if short_circuit {
            match BatchBlockWriterLocal::new(
                fs_context.clone(),
                blocks.clone(),
                worker_addr.clone(),
                0,
            )
            .await
            {
                Ok(writer) => BatchLocal(writer),
                Err(FsError::NoLocalPath(_)) => {
                    // Server has no local path for batch blocks (e.g. SPDK bdev).
                    // BatchBlockWriterLocal already aborted the open; retry via remote.
                    let writer =
                        BatchBlockWriterRemote::new(&fs_context, blocks, worker_addr.clone(), 0)
                            .await?;
                    BatchRemote(writer)
                }
                Err(e) => return Err(e),
            }
        } else {
            let writer =
                BatchBlockWriterRemote::new(&fs_context, blocks, worker_addr.clone(), 0).await?;
            BatchRemote(writer)
        };

        Ok(adapter)
    }
}

struct WorkerGroupEntry {
    original_index: usize,
    location_index: usize,
}

struct WorkerGroup {
    writer: BatchWriterAdapter,
    entries: Vec<WorkerGroupEntry>,
}

struct PendingWorkerGroup {
    worker: WorkerAddress,
    located_blocks: Vec<LocatedBlock>,
    entries: Vec<WorkerGroupEntry>,
}

fn group_blocks_by_worker(located_blocks: &[LocatedBlock]) -> FsResult<Vec<PendingWorkerGroup>> {
    let mut groups = Vec::<PendingWorkerGroup>::new();
    let mut group_by_worker = HashMap::<u32, usize>::new();

    for (original_index, located_block) in located_blocks.iter().enumerate() {
        if located_block.locs.is_empty() {
            return err_box!(
                "There is no available worker for block {}",
                located_block.block.id
            );
        }

        let mut block_workers = HashSet::with_capacity(located_block.locs.len());
        for (location_index, worker) in located_block.locs.iter().enumerate() {
            if !block_workers.insert(worker.worker_id) {
                return err_box!(
                    "duplicate worker {} for block {}",
                    worker.worker_id,
                    located_block.block.id
                );
            }

            let group_index = match group_by_worker.get(&worker.worker_id) {
                Some(index) => *index,
                None => {
                    let index = groups.len();
                    group_by_worker.insert(worker.worker_id, index);
                    groups.push(PendingWorkerGroup {
                        worker: worker.clone(),
                        located_blocks: Vec::new(),
                        entries: Vec::new(),
                    });
                    index
                }
            };
            groups[group_index]
                .located_blocks
                .push(located_block.clone());
            groups[group_index].entries.push(WorkerGroupEntry {
                original_index,
                location_index,
            });
        }
    }

    Ok(groups)
}

pub struct BatchBlockWriter {
    groups: Vec<WorkerGroup>,
    fs_context: Arc<FsContext>,
    located_blocks: Vec<LocatedBlock>,
    file_lengths: Vec<i64>,
}
impl BatchBlockWriter {
    /// Create multiple BlockWriters for batch operations  
    pub async fn new(
        fs_context: Arc<FsContext>,
        located_blocks: Vec<LocatedBlock>,
    ) -> FsResult<Self> {
        if located_blocks.is_empty() {
            return err_box!("No blocks provided");
        }

        let pending_groups = group_blocks_by_worker(&located_blocks)?;
        let mut opened = stream::iter(pending_groups.into_iter().enumerate().map(
            |(group_index, group)| {
                let fs_context = fs_context.clone();
                async move {
                    let result =
                        BatchWriterAdapter::new(fs_context, group.located_blocks, &group.worker)
                            .await;
                    (group_index, group.entries, result)
                }
            },
        ))
        .buffer_unordered(MAX_CONCURRENT_WORKER_GROUPS)
        .collect::<Vec<_>>()
        .await;
        opened.sort_by_key(|(group_index, _, _)| *group_index);

        if let Some(error_index) = opened.iter().position(|(_, _, result)| result.is_err()) {
            let cancellations = opened.iter_mut().filter_map(|(_, _, result)| {
                result.as_mut().ok().map(|writer| async move {
                    let worker = writer.worker_address().clone();
                    (worker, writer.cancel().await)
                })
            });
            for (worker, result) in futures::future::join_all(cancellations).await {
                if let Err(cancel_error) = result {
                    log::warn!(
                        "failed to cancel batch group on worker {}: {}",
                        worker.worker_id,
                        cancel_error
                    );
                }
            }
            let error = match opened.swap_remove(error_index).2 {
                Ok(_) => unreachable!("batch group error index must contain an error"),
                Err(error) => error,
            };
            return Err(error);
        }

        let groups = opened
            .into_iter()
            .map(|(_, entries, writer)| WorkerGroup {
                writer: writer.expect("batch group error handled above"),
                entries,
            })
            .collect();
        let num_of_blocks = located_blocks.len();

        Ok(Self {
            groups,
            fs_context,
            located_blocks,
            file_lengths: Vec::with_capacity(num_of_blocks),
        })
    }

    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        if files.len() != self.located_blocks.len() {
            return err_box!(
                "batch file count mismatch, expected {}, actual {}",
                self.located_blocks.len(),
                files.len()
            );
        }

        self.file_lengths.clear();
        self.file_lengths
            .extend(files.iter().map(|(_, content)| content.len() as i64));
        let futures = self.groups.iter_mut().map(|group| async move {
            let group_files = group
                .entries
                .iter()
                .map(|entry| files[entry.original_index])
                .collect::<Vec<_>>();
            group
                .writer
                .write(&group_files)
                .await
                .map_err(|e| (group.writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let futures = self.groups.iter_mut().map(|group| async move {
            group
                .writer
                .flush()
                .await
                .map_err(|e| (group.writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }

    /// Complete all writers and return commit blocks  
    pub async fn complete(&mut self) -> FsResult<Vec<CommitBlock>> {
        let futures = self.groups.iter_mut().map(|group| async move {
            group
                .writer
                .complete()
                .await
                .map_err(|e| (group.writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }

        Ok(self.to_commit_blocks())
    }

    pub fn to_commit_blocks(&self) -> Vec<CommitBlock> {
        let mut locations = self
            .located_blocks
            .iter()
            .map(|block| vec![None; block.locs.len()])
            .collect::<Vec<_>>();
        for group in &self.groups {
            for (local_index, entry) in group.entries.iter().enumerate() {
                locations[entry.original_index][entry.location_index] = Some(BlockLocation::new(
                    group.writer.worker_address().worker_id,
                    group.writer.actual_storage_type(local_index),
                ));
            }
        }

        let mut commit_blocks = Vec::with_capacity(self.located_blocks.len());

        for (i, located_block) in self.located_blocks.iter().enumerate() {
            let mut commit_block = CommitBlock {
                block_id: located_block.block.id,
                block_len: located_block.block.len,
                locations: locations[i]
                    .iter()
                    .map(|location| {
                        location
                            .clone()
                            .expect("every allocated batch replica has a worker group")
                    })
                    .collect(),
            };

            if let Some(&length) = self.file_lengths.get(i) {
                commit_block.block_len = length;
            }

            commit_blocks.push(commit_block);
        }

        commit_blocks
    }
}

#[cfg(test)]
mod tests {
    use super::{group_blocks_by_worker, validate_batch_contexts, BatchBlockWriter};
    use crate::block::CreateBlockContext;
    use curvine_model::{CommitBlock, ExtendedBlock, LocatedBlock, StorageType, WorkerAddress};

    fn context(id: i64) -> CreateBlockContext {
        CreateBlockContext {
            id,
            off: 0,
            block_size: 1024,
            path: None,
            storage_type: StorageType::Disk,
        }
    }

    fn worker(worker_id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            ..Default::default()
        }
    }

    fn located_block(id: i64, worker_ids: impl IntoIterator<Item = u32>) -> LocatedBlock {
        LocatedBlock::new(
            ExtendedBlock::with_id(id),
            worker_ids.into_iter().map(worker).collect(),
        )
    }

    #[test]
    fn to_commit_blocks_keeps_vec_return_type() {
        let _: fn(&BatchBlockWriter) -> Vec<CommitBlock> = BatchBlockWriter::to_commit_blocks;
    }

    #[test]
    fn batch_context_validation_rejects_missing_response() {
        let blocks = vec![ExtendedBlock::with_id(1), ExtendedBlock::with_id(2)];

        let error = validate_batch_contexts(&blocks, &[context(1)]).unwrap_err();

        assert!(error.to_string().contains("response count mismatch"));
    }

    #[test]
    fn batch_context_validation_rejects_reordered_response() {
        let blocks = vec![ExtendedBlock::with_id(1), ExtendedBlock::with_id(2)];

        let error = validate_batch_contexts(&blocks, &[context(2), context(1)]).unwrap_err();

        assert!(error.to_string().contains("response id mismatch"));
    }

    #[test]
    fn grouping_common_path_keeps_one_group_per_replica() {
        const BLOCKS: usize = 1_000;
        const REPLICAS: usize = 3;
        let blocks = (0..BLOCKS)
            .map(|id| located_block(id as i64, [1, 2, 3]))
            .collect::<Vec<_>>();

        let groups = group_blocks_by_worker(&blocks).unwrap();

        assert_eq!(groups.len(), REPLICAS);
        assert!(groups.iter().all(|group| group.entries.len() == BLOCKS));
        assert_eq!(
            groups
                .iter()
                .map(|group| group.entries.len())
                .sum::<usize>(),
            BLOCKS * REPLICAS
        );
    }

    #[test]
    fn grouping_distributed_path_processes_each_assignment_once() {
        const BLOCKS: usize = 10_000;
        const WORKERS: usize = 32;
        let blocks = (0..BLOCKS)
            .map(|id| located_block(id as i64, [(id % WORKERS) as u32 + 1]))
            .collect::<Vec<_>>();

        let groups = group_blocks_by_worker(&blocks).unwrap();

        assert_eq!(groups.len(), WORKERS);
        assert_eq!(
            groups
                .iter()
                .map(|group| group.entries.len())
                .sum::<usize>(),
            BLOCKS
        );
        assert!(groups
            .iter()
            .all(|group| group.entries.len() <= BLOCKS.div_ceil(WORKERS)));
    }
}
