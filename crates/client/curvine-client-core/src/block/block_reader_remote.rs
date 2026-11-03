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

use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_config::ClientConf;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_proto::DataHeaderProto;
use curvine_runtime::common::Utils;
use std::collections::VecDeque;

pub struct BlockReaderRemote {
    client: BlockClient,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    req_id: i64,
    seq_id: i32,
    header: Option<DataHeaderProto>,
    // Pipelined reads: sequence ids sent and not yet received, in send order.
    in_flight: VecDeque<i32>,
    // Requests sent before a seek or complete; their responses are drained
    // and dropped before the connection is used again.
    stale: VecDeque<i32>,
    // Offset the requests sent so far will have read up to.
    requested: i64,
    // Block offset the caller will read up to; requests beyond the next one
    // are not sent past it. Defaults to the block length.
    prefetch_end: i64,
    depth: usize,
    chunk_size: i64,
}

impl BlockReaderRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let client = fs_context.acquire_read(&worker_address).await?;
        let _ = client
            .open_block(
                &fs_context.conf.client,
                &block,
                off,
                len,
                req_id,
                seq_id,
                false,
            )
            .await?;

        Ok(Self::from_opened(
            &fs_context.conf.client,
            client,
            block,
            worker_address,
            off,
            len,
            req_id,
            seq_id,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_opened(
        conf: &ClientConf,
        client: BlockClient,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
        req_id: i64,
        seq_id: i32,
    ) -> Self {
        Self {
            client,
            block,
            worker_address,
            pos: off,
            len,
            req_id,
            seq_id,
            header: None,
            in_flight: VecDeque::new(),
            stale: VecDeque::new(),
            requested: off,
            prefetch_end: len,
            depth: conf.read_pipeline_depth.max(1),
            chunk_size: conf.read_chunk_size.max(1) as i64,
        }
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    /// Limit prefetching to `end`, the block offset the caller will read up
    /// to. Reads past it still succeed, one request at a time.
    pub fn set_prefetch_end(&mut self, end: i64) {
        self.prefetch_end = end.min(self.len);
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        // Responses already requested belong to the old position.
        self.stale.extend(self.in_flight.drain(..));
        self.requested = pos;
        self.pos = pos;
        self.header = Some(DataHeaderProto {
            offset: pos,
            flush: false,
            is_last: false,
        });
        Ok(self.pos)
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        self.drain_stale().await?;
        // Keep up to `depth` chunk requests in flight, not past what the caller
        // will read. Always keep one, in case the worker returned a short chunk.
        //
        // The queues are updated before a send and after a receive, so if this
        // future is dropped mid-await they still show the connection is busy.
        while self.in_flight.len() < self.depth
            && (self.requested < self.prefetch_end || self.in_flight.is_empty())
        {
            let seq_id = self.next_seq_id();
            let header = self.header.take();
            self.in_flight.push_back(seq_id);
            self.requested += self.chunk_size;
            self.client.send_read(self.req_id, seq_id, header).await?;
        }
        let Some(&seq_id) = self.in_flight.front() else {
            return err_box!("No read request in flight at pos {}", self.pos);
        };
        let chunk = self.client.receive_read(self.req_id, seq_id).await?;
        self.in_flight.pop_front();

        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    async fn drain_stale(&mut self) -> FsResult<()> {
        while let Some(&seq_id) = self.stale.front() {
            let chunk = self.client.receive_read(self.req_id, seq_id).await?;
            self.stale.pop_front();
            FsContext::get_metrics()
                .read_discarded_bytes
                .inc_by(chunk.len() as i64);
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        // The commit must not be answered ahead of reads still in flight.
        self.stale.extend(self.in_flight.drain(..));
        self.drain_stale().await?;
        let next_seq_id = self.next_seq_id();
        self.client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}

impl Drop for BlockReaderRemote {
    fn drop(&mut self) {
        // Responses still on the wire would be read by the connection's next
        // user, so the connection must not go back to the pool.
        if !self.in_flight.is_empty() || !self.stale.is_empty() {
            self.client.clear_pool();
        }
    }
}
