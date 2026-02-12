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

use crate::block::container_block_writer::ContainerWriterAdapter::{
    ContainerLocal, ContainerRemote,
};
use crate::block::{ContainerBlockWriterLocal, ContainerBlockWriterRemote};
use crate::file::FsContext;
use curvine_common::fs::Path;
use curvine_common::proto::SmallFileMetaProto;
use curvine_common::state::{
    CommitBlock, ContainerStatus, ExtendedBlock, LocatedBlock, WorkerAddress,
};
use curvine_common::FsResult;
use futures::future::try_join_all;
use std::sync::Arc;

enum ContainerWriterAdapter {
    ContainerLocal(ContainerBlockWriterLocal),
    ContainerRemote(ContainerBlockWriterRemote),
}

impl ContainerWriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            ContainerLocal(f) => f.worker_address(),
            ContainerRemote(f) => f.worker_address(),
        }
    }

    async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        match self {
            ContainerLocal(f) => f.write(files).await,
            ContainerRemote(f) => f.write(files).await,
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        match self {
            ContainerLocal(f) => f.flush().await,
            ContainerRemote(f) => f.flush().await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            ContainerLocal(f) => f.complete().await,
            ContainerRemote(f) => f.complete().await,
        }
    }

    // Create new WriterAdapter
    async fn new(
        fs_context: Arc<FsContext>,
        located_block: &LocatedBlock,
        worker_addr: &WorkerAddress,
        container_status: ContainerStatus,
        small_files_metadata: Vec<SmallFileMetaProto>,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        let short_circuit = conf.short_circuit && fs_context.is_local_worker(worker_addr);
        // choose the writer mode: remote or local
        let block: ExtendedBlock = located_block.block.clone();
        let adapter = if short_circuit {
            let writer = ContainerBlockWriterLocal::new(
                fs_context,
                block,
                worker_addr.clone(),
                0,
                container_status,
                small_files_metadata,
            )
            .await?;
            ContainerLocal(writer)
        } else {
            let writer = ContainerBlockWriterRemote::new(
                &fs_context,
                block,
                worker_addr.clone(),
                0,
                container_status,
                small_files_metadata,
            )
            .await?;
            ContainerRemote(writer)
        };

        Ok(adapter)
    }
}

pub struct ContainerBlockWriter {
    inners: Vec<ContainerWriterAdapter>,
    fs_context: Arc<FsContext>,
    located_block: LocatedBlock,
    file_lengths: Vec<i64>,
}
impl ContainerBlockWriter {
    /// Create multiple ContainerBlockWriter for the batch writing opertion
    pub async fn new(
        fs_context: Arc<FsContext>,
        container_status: ContainerStatus,
        located_block: LocatedBlock,
        small_files_metadata: Vec<SmallFileMetaProto>,
    ) -> FsResult<Self> {
        // Create adapters for each worker (same workers for all blocks)
        let mut inners = Vec::with_capacity(located_block.locs.len());
        for addr in &located_block.locs {
            // Create a batch adapter that can handle multiple blocks
            let adapter = ContainerWriterAdapter::new(
                fs_context.clone(),
                &located_block,
                addr,
                container_status.clone(),
                small_files_metadata.clone(),
            )
            .await?;
            inners.push(adapter);
        }

        Ok(Self {
            inners,
            fs_context,
            located_block,
            file_lengths: Vec::with_capacity(small_files_metadata.len()),
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
    pub async fn complete(&mut self) -> FsResult<CommitBlock> {
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

    pub fn to_commit_blocks(&self) -> CommitBlock {
        let mut commit_block = CommitBlock::from(&self.located_block);
        commit_block.block_len = self.file_lengths.iter().copied().sum::<i64>();
        commit_block
    }
}
