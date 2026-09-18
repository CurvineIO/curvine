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
use std::sync::Arc;

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

    // Create new WriterAdapter
    async fn new(
        fs_context: Arc<FsContext>,
        located_blocks: &[LocatedBlock],
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

pub struct BatchBlockWriter {
    inners: Vec<BatchWriterAdapter>,
    fs_context: Arc<FsContext>,
    located_blocks: Vec<LocatedBlock>,
    file_lengths: Vec<i64>,
}
impl BatchBlockWriter {
    /// Create multiple BlockWriters for batch operations  
    pub async fn new(
        fs_context: Arc<FsContext>,
        located_blocks: Vec<LocatedBlock>, // all blocks have the same worker information
    ) -> FsResult<Self> {
        if located_blocks.is_empty() {
            return err_box!("No blocks provided");
        }

        // Get the first block to extract worker information
        let first_locate = located_blocks.first().unwrap();

        if first_locate.locs.is_empty() {
            return err_box!("There is no available worker");
        }

        // Create adapters for each worker (same workers for all blocks)
        let mut inners = Vec::with_capacity(first_locate.locs.len());
        for addr in &first_locate.locs {
            // Create a batch adapter that can handle multiple blocks
            let adapter =
                BatchWriterAdapter::new(fs_context.clone(), &located_blocks, addr).await?;
            inners.push(adapter);
        }
        let num_of_blocks = located_blocks.len();

        Ok(Self {
            inners,
            fs_context,
            located_blocks,
            file_lengths: Vec::with_capacity(num_of_blocks),
        })
    }

    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        // Store individual file lengths
        for (_, content) in files {
            self.file_lengths.push(content.len() as i64);
        }
        // Write each file separately to all writers with index
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .write(files)
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .flush()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }

    /// Complete all writers and return commit blocks  
    pub async fn complete(&mut self) -> FsResult<Vec<CommitBlock>> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .complete()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }

        Ok(self.to_commit_blocks())
    }

    pub fn to_commit_blocks(&self) -> Vec<CommitBlock> {
        let mut commit_blocks = Vec::with_capacity(self.located_blocks.len());

        for (i, located_block) in self.located_blocks.iter().enumerate() {
            let locations = self
                .inners
                .iter()
                .map(|writer| {
                    BlockLocation::new(
                        writer.worker_address().worker_id,
                        writer.actual_storage_type(i),
                    )
                })
                .collect();
            let mut commit_block = CommitBlock {
                block_id: located_block.block.id,
                block_len: located_block.block.len,
                locations,
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
    use super::validate_batch_contexts;
    use crate::block::CreateBlockContext;
    use curvine_model::{ExtendedBlock, StorageType};

    fn context(id: i64) -> CreateBlockContext {
        CreateBlockContext {
            id,
            off: 0,
            block_size: 1024,
            path: None,
            storage_type: StorageType::Disk,
        }
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
}
