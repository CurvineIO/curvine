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

use crate::block::block_writer::WriterAdapter::{Local, Remote};
use crate::block::{BlockWriterLocal, BlockWriterRemote};
use crate::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::state::{BlockLocation, CommitBlock, LocatedBlock, WorkerAddress};
use curvine_common::FsResult;
use futures::future::try_join_all;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use std::sync::Arc;
use std::vec;

pub enum WriterAdapter {
    Local(BlockWriterLocal),
    Remote(BlockWriterRemote),
}

impl WriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            Local(f) => f.worker_address(),
            Remote(f) => f.worker_address(),
        }
    }

    async fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.write(buf).await,
            Remote(f) => f.write(buf).await,
        }
    }

    fn blocking_write(&mut self, rt: &Runtime, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.blocking_write(buf.clone()),
            Remote(f) => rt.block_on(f.write(buf.clone())),
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.flush().await,
            Remote(f) => f.flush().await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.complete().await,
            Remote(f) => f.complete().await,
        }
    }

    async fn cancel(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.cancel().await,
            Remote(f) => f.cancel().await,
        }
    }

    fn remaining(&self) -> i64 {
        match self {
            Local(f) => f.remaining(),
            Remote(f) => f.remaining(),
        }
    }

    fn pos(&self) -> i64 {
        match self {
            Local(f) => f.pos(),
            Remote(f) => f.pos(),
        }
    }

    // Add seek support for WriterAdapter
    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match self {
            Local(f) => f.seek(pos).await,
            Remote(f) => f.seek(pos).await,
        }
    }

    fn len(&self) -> i64 {
        match self {
            Local(f) => f.len(),
            Remote(f) => f.len(),
        }
    }

    // Create new WriterAdapter
    pub async fn new(
        fs_context: Arc<FsContext>,
        located_block: &LocatedBlock,
        worker_addr: &WorkerAddress,
        pipeline: bool,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        let short_circuit = conf.short_circuit && fs_context.is_local_worker(worker_addr);

        // todo: local write also could support pipeline write.
        let adapter = if short_circuit {
            let writer =
                BlockWriterLocal::new(fs_context, located_block.block.clone(), worker_addr.clone())
                    .await?;
            Local(writer)
        } else {
            let writer = BlockWriterRemote::new(
                &fs_context,
                located_block.block.clone(),
                worker_addr.clone(),
                pipeline,
                located_block.locs.clone(),
            )
            .await?;
            Remote(writer)
        };

        Ok(adapter)
    }
}

pub struct BlockWriter {
    inners: Vec<WriterAdapter>,
    locate: LocatedBlock,
    fs_context: Arc<FsContext>,
}

impl BlockWriter {
    pub async fn new(fs_context: Arc<FsContext>, locate: LocatedBlock) -> FsResult<Self> {
        let first_loc = locate
            .locs
            .first()
            .ok_or(FsError::common("There is no available worker"))?;
        let writers = if fs_context.cluster_conf().client.pipeline_write_enabled {
            vec![WriterAdapter::new(fs_context.clone(), &locate, first_loc, true).await?]
        } else {
            let mut inners = Vec::with_capacity(locate.locs.len());
            for addr in &locate.locs {
                let adapter = WriterAdapter::new(fs_context.clone(), &locate, addr, false).await?;
                inners.push(adapter);
            }
            inners
        };

        let writer = Self {
            inners: writers,
            locate,
            fs_context,
        };
        Ok(writer)
    }

    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let chunk = chunk.freeze();
        let mut futures = Vec::with_capacity(self.inners.len());
        for writer in self.inners.iter_mut() {
            let chunk_clone = chunk.clone();
            let task = async move {
                writer
                    .write(chunk_clone)
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            };
            futures.push(task);
        }

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }

    pub fn blocking_write(&mut self, rt: &Runtime, chunk: DataSlice) -> FsResult<()> {
        if self.inners.len() == 1 {
            if let Err(e) = self.inners[0].blocking_write(rt, chunk) {
                self.fs_context
                    .add_failed_worker(self.inners[0].worker_address());
                Err(e)
            } else {
                Ok(())
            }
        } else {
            rt.block_on(self.write(chunk))?;
            Ok(())
        }
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let mut futures = Vec::with_capacity(self.inners.len());
        for writer in self.inners.iter_mut() {
            let task = async move {
                writer
                    .flush()
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            };
            futures.push(task);
        }

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<CommitBlock> {
        let mut futures = Vec::with_capacity(self.inners.len());
        for writer in self.inners.iter_mut() {
            let task = async move {
                writer
                    .complete()
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            };
            futures.push(task);
        }

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(self.to_commit_block())
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        let mut futures = Vec::with_capacity(self.inners.len());
        for writer in self.inners.iter_mut() {
            let task = async move {
                writer
                    .cancel()
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            };
            futures.push(task);
        }

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }

    pub fn remaining(&self) -> i64 {
        self.inners[0].remaining()
    }

    pub fn has_remaining(&self) -> bool {
        self.inners[0].remaining() > 0
    }

    pub fn pos(&self) -> i64 {
        self.inners[0].pos()
    }

    pub fn len(&self) -> i64 {
        self.inners[0].len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Get block ID for cache management
    pub fn block_id(&self) -> i64 {
        self.locate.block.id
    }

    // Implement seek support for random writes
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(format!("Cannot seek to negative position: {pos}").into());
        }

        let mut futures = Vec::with_capacity(self.inners.len());
        for writer in self.inners.iter_mut() {
            let task = async move {
                writer
                    .seek(pos)
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            };
            futures.push(task);
        }

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }

    pub fn to_commit_block(&self) -> CommitBlock {
        let locs = self
            .locate
            .locs
            .iter()
            .map(|x| BlockLocation {
                worker_id: x.worker_id,
                storage_type: self.locate.block.storage_type,
            })
            .collect();

        CommitBlock {
            block_id: self.locate.block.id,
            block_len: self.pos(),
            locations: locs,
        }
    }
}
