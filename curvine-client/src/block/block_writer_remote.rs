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

use crate::block::block_client::BlockClient;
use crate::file::FsContext;
use curvine_common::proto::DataHeaderProto;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::err_box;
use orpc::sys::DataSlice;

pub struct BlockWriterRemote {
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    client: BlockClient,
    pos: i64,
    seq_id: i32,
    req_id: i64,
    pending_header: Option<DataHeaderProto>,
    block_size: i64,
}

impl BlockWriterRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        pos: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;
        let block_size = fs_context.block_size();

        let client = fs_context.block_client(&worker_address).await?;
        let write_context = client
            .write_block(
                &block,
                pos,
                block_size,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                false,
            )
            .await?;

        if block_size != write_context.block_size {
            return err_box!(
                "Abnormal block size, expected length {}, actual length {}",
                block_size,
                write_context.block_size
            );
        }

        let writer = Self {
            block,
            client,
            pos,
            seq_id,
            req_id,
            worker_address,
            pending_header: None,
            block_size,
        };

        Ok(writer)
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    // Write data.
    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let len = chunk.len() as i64;
        let next_seq_id = self.next_seq_id();

        let header = self.pending_header.take();

        self.client
            .write_data(chunk, self.req_id, next_seq_id, header)
            .await?;

        self.pos += len;
        if self.pos > self.block.len {
            self.block.len = self.pos;
        }
        Ok(())
    }

    // refresh.
    pub async fn flush(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .write_flush(self.pos, self.req_id, next_seq_id)
            .await?;

        Ok(())
    }

    // Write complete
    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .write_commit(
                &self.block,
                self.pos,
                self.block_size,
                self.req_id,
                next_seq_id,
                false,
            )
            .await?;
        Ok(())
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .write_commit(
                &self.block,
                self.pos,
                self.block_size,
                self.req_id,
                next_seq_id,
                true,
            )
            .await
    }

    // Get the number of bytes left to writable in the current block.
    pub fn remaining(&self) -> i64 {
        self.block_size - self.pos
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub fn len(&self) -> i64 {
        self.block.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative position: {}", pos);
        } else if pos > self.block_size {
            return err_box!(
                "Seek position {} exceeds block capacity {}",
                pos,
                self.block_size
            );
        }

        // Set new position and pending header
        self.pos = pos;
        self.pending_header = Some(DataHeaderProto {
            offset: pos,
            flush: false,
            is_last: false,
        });

        Ok(())
    }
}
