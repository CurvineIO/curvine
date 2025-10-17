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

use crate::file::FsContext;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{DataSlice, RawPtr};
use orpc::try_option;
use std::sync::Arc;

pub struct BlockWriterLocal {
    rt: Arc<Runtime>,
    fs_context: Arc<FsContext>,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    file: RawPtr<LocalFile>,
    seq_id: i32,
    req_id: i64,
    len: i64,
    max_written_pos: i64,
}

impl BlockWriterLocal {
    pub async fn new(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        // Always start from append mode (block.len position)
        let pos = block.len;
        let len = fs_context.block_size();

        let client = fs_context.block_client(&worker_address).await?;
        let write_context = client
            .write_block(
                &block,
                pos,
                len,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                true,
            )
            .await?;

        let path = try_option!(write_context.path);
        let file = if pos > 0 {
            let mut file = LocalFile::with_append(path)?;
            file.seek(pos)?;
            file
        } else {
            LocalFile::with_write(path, false)?
        };

        let writer = Self {
            rt: fs_context.clone_runtime(),
            fs_context,
            block,
            worker_address,
            file: RawPtr::from_owned(file),
            seq_id,
            req_id,
            len,
            max_written_pos: pos,
        };

        Ok(writer)
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let file = self.file.clone();
        let res = self
            .rt
            .spawn_blocking(move || {
                file.as_mut().write_all(chunk.as_slice())?;
                Ok(())
            })
            .await?;

        let new_pos = self.pos();
        self.max_written_pos = self.max_written_pos.max(new_pos);
        res
    }

    // Block write data.
    pub fn blocking_write(&mut self, chunk: DataSlice) -> FsResult<()> {
        self.file.as_mut().write_all(chunk.as_slice())?;

        let new_pos = self.pos();
        self.max_written_pos = self.max_written_pos.max(new_pos);
        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let file = self.file.clone();
        self.rt
            .spawn_blocking(move || {
                file.as_mut().flush()?;
                Ok(())
            })
            .await?
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;
        let next_seq_id = self.next_seq_id();
        let client = self.fs_context.block_client(&self.worker_address).await?;
        client
            .write_commit(
                &self.block,
                self.pos(),
                self.len,
                self.req_id,
                next_seq_id,
                false,
            )
            .await
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        let client = self.fs_context.block_client(&self.worker_address).await?;
        client
            .write_commit(
                &self.block,
                self.pos(),
                self.len,
                self.req_id,
                next_seq_id,
                true,
            )
            .await
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos()
    }

    pub fn pos(&self) -> i64 {
        self.file.pos()
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub fn max_written_pos(&self) -> i64 {
        self.max_written_pos
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(format!("Cannot seek to negative position: {pos}").into());
        }

        if pos >= self.len {
            return Err(format!("Seek position {pos} exceeds block capacity {}", self.len).into());
        }

        // For local files, call seek directly
        let file = self.file.clone();
        self.rt
            .spawn_blocking(move || {
                file.as_mut().seek(pos)?;
                Ok(())
            })
            .await?
    }
}
