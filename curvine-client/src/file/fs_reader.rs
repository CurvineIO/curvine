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

use crate::file::{FsContext, FsReaderBuffer};
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileBlocks;
use curvine_common::FsResult;
use log::info;
use orpc::common::ByteUnit;
use orpc::err_box;
use orpc::sys::DataSlice;
use std::sync::Arc;

type Inner = FsReaderBuffer;

pub struct FsReader {
    inner: Inner,
    chunk: DataSlice,
    chunk_size: usize,
    pos: i64,
    len: i64,
}

impl FsReader {
    pub fn new(path: Path, fs_context: Arc<FsContext>, file_blocks: FileBlocks) -> FsResult<Self> {
        let chunk_size = fs_context.read_chunk_size();
        let len = file_blocks.status.len;
        let conf = &fs_context.conf.client;
        info!(
            "Create reader, path={}, len={}, blocks={}, chunk_size={}, chunk_number={}, read_parallel={}, slice_size={}, read_ahead={}-{}",
            &file_blocks.status.path,
            ByteUnit::byte_to_string(len as u64),
            file_blocks.block_locs.len(),
            chunk_size,
            conf.read_chunk_num,
            conf.read_parallel,
            conf.read_slice_size,
            conf.enable_read_ahead,
            conf.read_ahead_len
        );

        let inner = FsReaderBuffer::new(path, fs_context, file_blocks)?;
        let reader = Self {
            inner,
            chunk: DataSlice::Empty,
            chunk_size,
            pos: 0,
            len,
        };
        Ok(reader)
    }
}

impl Reader for FsReader {
    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn len(&self) -> i64 {
        self.len
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        self.inner.read().await
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative offset");
        } else if self.pos == pos {
            return Ok(());
        }

        let to_skip = pos - self.pos;
        if to_skip >= 0 && to_skip <= self.chunk.len() as i64 {
            self.chunk.advance(to_skip as usize);
        } else {
            self.chunk.clear();
            self.inner.seek(pos).await?;
        }

        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await
    }
}

impl Drop for FsReader {
    fn drop(&mut self) {
    }
}
