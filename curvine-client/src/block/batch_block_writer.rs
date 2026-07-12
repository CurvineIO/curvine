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
use crate::block::{BatchBlockWriterLocal, BatchBlockWriterRemote};
use crate::file::FsContext;
use curvine_common::fs::Path;
use curvine_common::state::{CommitBlock, ExtendedBlock, LocatedBlock, StorageType, WorkerAddress};
use curvine_common::FsResult;
use futures::future::join_all;
use orpc::err_box;
use std::sync::Arc;

enum BatchWriterAdapter {
    BatchLocal(BatchBlockWriterLocal),
    BatchRemote(BatchBlockWriterRemote),
}

struct BatchWriterGroup {
    writer: BatchWriterAdapter,
    indices: Vec<usize>,
}

async fn cancel_writer_groups(fs_context: &FsContext, groups: &mut [BatchWriterGroup]) {
    let futures = groups.iter_mut().map(|group| async move {
        let worker = group.writer.worker_address().clone();
        let cancels = vec![true; group.indices.len()];
        (worker, group.writer.complete(&cancels).await)
    });
    for (worker, result) in join_all(futures).await {
        if let Err(error) = result {
            fs_context.add_failed_worker(&worker);
            log::warn!(
                "failed to cancel batch blocks on worker {}: {}",
                worker,
                error
            );
        }
    }
}

impl BatchWriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            BatchLocal(f) => f.worker_address(),
            BatchRemote(f) => f.worker_address(),
        }
    }

    async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<Vec<bool>> {
        match self {
            BatchLocal(f) => f.write(files).await,
            BatchRemote(f) => f.write(files).await,
        }
    }

    /// Client-side flush is only needed for short-circuit local writers.
    /// Remote writers flush inside Worker `complete_block`, so this returns `None`.
    async fn flush_active(&mut self, active: &[bool]) -> FsResult<Option<Vec<bool>>> {
        match self {
            BatchLocal(f) => Ok(Some(f.flush_active(active).await?)),
            BatchRemote(_) => Ok(None),
        }
    }

    async fn complete(&mut self, cancels: &[bool]) -> FsResult<Vec<bool>> {
        match self {
            BatchLocal(f) => f.complete(cancels).await,
            BatchRemote(f) => f.complete(cancels).await,
        }
    }

    // Create new WriterAdapter
    async fn new(
        fs_context: Arc<FsContext>,
        located_blocks: &[LocatedBlock],
        worker_addr: &WorkerAddress,
    ) -> FsResult<(Self, Vec<bool>)> {
        let conf = &fs_context.conf.client;
        // SPDK bypasses kernel — no local path. Disable short-circuit if any block uses SPDK.
        let has_spdk = located_blocks
            .iter()
            .any(|lb| lb.block.storage_type == StorageType::SpdkDisk);
        let short_circuit =
            conf.short_circuit && fs_context.is_local_worker(worker_addr) && !has_spdk;

        let blocks: Vec<ExtendedBlock> = located_blocks.iter().map(|lb| lb.block.clone()).collect();
        if short_circuit {
            let (writer, results) =
                BatchBlockWriterLocal::new(fs_context, blocks, worker_addr.clone(), 0).await?;
            Ok((BatchLocal(writer), results))
        } else {
            let (writer, results) =
                BatchBlockWriterRemote::new(&fs_context, blocks, worker_addr.clone(), 0).await?;
            Ok((BatchRemote(writer), results))
        }
    }
}

pub struct BatchBlockWriter {
    groups: Vec<BatchWriterGroup>,
    fs_context: Arc<FsContext>,
    located_blocks: Vec<LocatedBlock>,
    item_success: Vec<bool>,
}

fn group_blocks_by_worker(
    located_blocks: &[LocatedBlock],
) -> (Vec<(WorkerAddress, Vec<usize>)>, Vec<bool>) {
    let mut item_success = vec![true; located_blocks.len()];
    let mut worker_groups: Vec<(WorkerAddress, Vec<usize>)> = Vec::new();
    for (index, located_block) in located_blocks.iter().enumerate() {
        if located_block.locs.is_empty() {
            item_success[index] = false;
        }
        for worker in &located_block.locs {
            if let Some((_address, indices)) = worker_groups
                .iter_mut()
                .find(|(address, _indices)| address == worker)
            {
                indices.push(index);
            } else {
                worker_groups.push((worker.clone(), vec![index]));
            }
        }
    }
    (worker_groups, item_success)
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

        let (worker_groups, mut item_success) = group_blocks_by_worker(&located_blocks);

        let mut groups = Vec::with_capacity(worker_groups.len());
        for (worker, indices) in worker_groups {
            let blocks = indices
                .iter()
                .map(|index| located_blocks[*index].clone())
                .collect::<Vec<_>>();
            match BatchWriterAdapter::new(fs_context.clone(), &blocks, &worker).await {
                Ok((writer, open_results)) => {
                    for (index, success) in indices.iter().zip(open_results.iter()) {
                        item_success[*index] &= *success;
                    }
                    groups.push(BatchWriterGroup { writer, indices });
                }
                Err(error) => {
                    fs_context.add_failed_worker(&worker);
                    log::warn!("batch open failed on worker {}: {}", worker, error);
                    for index in indices {
                        item_success[index] = false;
                    }
                }
            }
        }

        Ok(Self {
            groups,
            fs_context,
            located_blocks,
            item_success,
        })
    }

    fn merge_worker_results<FailedWorker>(
        results: &mut [bool],
        responses: Vec<(WorkerAddress, Vec<usize>, FsResult<Vec<bool>>)>,
        operation: &str,
        mut failed_worker: FailedWorker,
    ) where
        FailedWorker: FnMut(&WorkerAddress),
    {
        for (worker_addr, indices, response) in responses {
            let worker_results = match response {
                Ok(results) => results,
                Err(error) => {
                    failed_worker(&worker_addr);
                    log::warn!(
                        "batch {} failed on worker {}: {}",
                        operation,
                        worker_addr,
                        error
                    );
                    for index in indices {
                        results[index] = false;
                    }
                    continue;
                }
            };
            if worker_results.len() != indices.len() {
                failed_worker(&worker_addr);
                log::warn!(
                    "batch {} result count mismatch on worker {}, expected {}, actual {}",
                    operation,
                    worker_addr,
                    indices.len(),
                    worker_results.len()
                );
                for index in indices {
                    results[index] = false;
                }
                continue;
            }
            for (index, worker_success) in indices.iter().zip(worker_results.iter()) {
                results[*index] &= *worker_success;
            }
        }
    }

    async fn cancel_all(&mut self) {
        cancel_writer_groups(&self.fs_context, &mut self.groups).await;
        self.item_success.fill(false);
    }

    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        if files.len() != self.located_blocks.len() {
            let error = err_box!(
                "batch write count mismatch, files={}, blocks={}",
                files.len(),
                self.located_blocks.len()
            );
            self.cancel_all().await;
            return error;
        }
        for (located_block, (_, content)) in self.located_blocks.iter_mut().zip(files) {
            located_block.block.len = content.len() as i64;
        }
        let futures = self.groups.iter_mut().map(|group| async move {
            let worker_addr = group.writer.worker_address().clone();
            let indices = group.indices.clone();
            let worker_files = indices
                .iter()
                .map(|index| files[*index])
                .collect::<Vec<_>>();
            (
                worker_addr,
                indices,
                group.writer.write(&worker_files).await,
            )
        });
        let responses = join_all(futures).await;
        let mut results = self.item_success.clone();
        Self::merge_worker_results(&mut results, responses, "write", |worker| {
            self.fs_context.add_failed_worker(worker)
        });
        self.item_success = results.clone();
        Ok(())
    }

    /// Flush short-circuit local groups for still-active items, then AND results
    /// back onto the original file indices. Remote groups are skipped.
    async fn flush_active_local(&mut self) -> Vec<bool> {
        let active = self.item_success.clone();
        let futures = self.groups.iter_mut().map(|group| {
            let worker_active = group
                .indices
                .iter()
                .map(|index| active[*index])
                .collect::<Vec<_>>();
            async move {
                let worker_addr = group.writer.worker_address().clone();
                let indices = group.indices.clone();
                match group.writer.flush_active(&worker_active).await {
                    Ok(Some(results)) => Some((worker_addr, indices, Ok(results))),
                    Ok(None) => None,
                    Err(error) => Some((worker_addr, indices, Err(error))),
                }
            }
        });
        let responses = join_all(futures)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let mut results = self.item_success.clone();
        Self::merge_worker_results(&mut results, responses, "flush", |worker| {
            self.fs_context.add_failed_worker(worker)
        });
        results
    }

    pub async fn complete(&mut self) -> Vec<Option<CommitBlock>> {
        let flushed = self.flush_active_local().await;
        let cancels: Vec<bool> = flushed.iter().map(|success| !success).collect();
        let futures = self.groups.iter_mut().map(|group| {
            let worker_cancels = group
                .indices
                .iter()
                .map(|index| cancels[*index])
                .collect::<Vec<_>>();
            async move {
                let worker_addr = group.writer.worker_address().clone();
                let indices = group.indices.clone();
                (
                    worker_addr,
                    indices,
                    group.writer.complete(&worker_cancels).await,
                )
            }
        });
        let responses = join_all(futures).await;
        let mut results = flushed;
        Self::merge_worker_results(&mut results, responses, "commit", |worker| {
            self.fs_context.add_failed_worker(worker)
        });
        self.item_success = results.clone();
        self.located_blocks
            .iter()
            .zip(results)
            .map(|(block, success)| success.then(|| CommitBlock::from(block)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::FileType;

    fn worker(worker_id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            ..Default::default()
        }
    }

    fn block(id: i64, locs: Vec<WorkerAddress>) -> LocatedBlock {
        LocatedBlock {
            block: ExtendedBlock::new(id, 0, StorageType::Disk, FileType::File),
            locs,
        }
    }

    #[test]
    fn groups_each_file_only_on_its_assigned_workers() {
        let worker_1 = worker(1);
        let worker_2 = worker(2);
        let worker_3 = worker(3);
        let blocks = vec![
            block(1, vec![worker_1.clone(), worker_2.clone()]),
            block(2, vec![worker_2.clone()]),
            block(3, vec![worker_3.clone()]),
            block(4, vec![]),
        ];

        let (groups, initial_results) = group_blocks_by_worker(&blocks);

        assert_eq!(initial_results, vec![true, true, true, false]);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0], (worker_1, vec![0]));
        assert_eq!(groups[1], (worker_2, vec![0, 1]));
        assert_eq!(groups[2], (worker_3, vec![2]));
    }

    #[test]
    fn replica_results_are_anded_per_original_file() {
        let worker_1 = worker(1);
        let worker_2 = worker(2);
        let mut results = vec![true, true, true];
        let responses = vec![
            (worker_1, vec![0, 1], Ok(vec![true, false])),
            (worker_2, vec![0, 2], Ok(vec![false, true])),
        ];

        BatchBlockWriter::merge_worker_results(&mut results, responses, "test", |_| {});

        assert_eq!(results, vec![false, false, true]);
    }
}
