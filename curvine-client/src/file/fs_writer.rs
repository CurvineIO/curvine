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

use crate::file::{FsContext, FsWriterBase, FsWriterBuffer};
use bytes::BytesMut;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::{FileStatus, LastBlockStatus, LocatedBlock};
use curvine_common::FsResult;
use log::info;
use orpc::common::ByteUnit;
use orpc::sys::DataSlice;
use std::sync::Arc;

type Inner = FsWriterBuffer;

pub struct FsWriter {
    inner: Inner,
    buf: BytesMut,
    chunk_size: usize,
    pos: i64,
}

impl FsWriter {
    pub fn new(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let chunk_size = fs_context.write_chunk_size();
        let chunk_num = fs_context.write_chunk_num();
        let pos = status.len;
        info!(
            "Create writer, path={}, pos={}, block_size={}, chunk_size={}, chunk_number={}, replicas={}",
            &status.path,
            pos,
            ByteUnit::byte_to_string(status.block_size as u64),
            chunk_size,
            chunk_num,
            status.replicas
        );

        let writer = FsWriterBase::new(fs_context, path, status, last_block);
        let inner = FsWriterBuffer::new(writer, chunk_num);

        Self {
            inner,
            buf: BytesMut::with_capacity(chunk_size),
            chunk_size,
            pos,
        }
    }

    pub fn create(fs_context: Arc<FsContext>, path: Path, status: FileStatus) -> Self {
        Self::new(fs_context, path, status, None)
    }

    pub fn append(fs_context: Arc<FsContext>, path: Path, status: LastBlockStatus) -> Self {
        Self::new(fs_context, path, status.file_status, status.last_block)
    }
}

impl Writer for FsWriter {
    fn status(&self) -> &FileStatus {
        self.inner.status()
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        let len = chunk.len();
        self.inner.write(chunk).await?;
        Ok(len as i64)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        self.inner.flush().await
    }

    // Write is completed, perform the following operations
    // 1. Submit the last block.
    async fn complete(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        // The flush operation will be automatically called internally, so flush is not needed here.
        self.inner.complete().await
    }

    async fn cancel(&mut self) -> FsResult<()> {
        Ok(())
    }
    
    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(format!("Cannot seek to negative position: {}", pos).into());
        }
        
        // Flush current buffer
        self.flush_chunk().await?;
        
        // Delegate to inner writer to execute seek
        self.inner.seek(pos).await?;
        
        // Update current position
        self.pos = pos;
        Ok(())
    }
}

impl Drop for FsWriter {
    fn drop(&mut self) {
    }
}
