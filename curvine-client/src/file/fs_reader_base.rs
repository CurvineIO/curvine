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

use crate::block::BlockReader;
use crate::file::FsContext;
use curvine_common::fs::Path;
use curvine_common::state::{FileBlocks, SearchFileBlocks};
use curvine_common::FsResult;
use orpc::common::FastHashMap;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::{err_box, try_option_mut};
use std::mem;
use std::sync::Arc;

pub struct FsReaderBase {
    path: Path,
    fs_context: Arc<FsContext>,
    file_blocks: SearchFileBlocks,
    pos: i64,
    len: i64,

    // The block that is currently being read
    cur_reader: Option<BlockReader>,

    // All read blocks are reused; reduce the overhead of creating connections and improve the performance of random reads.
    close_reader_times: u32,
    close_reader_limit: u32,
    all_reader: FastHashMap<i64, BlockReader>,
}

impl FsReaderBase {
    pub fn new(path: Path, fs_context: Arc<FsContext>, file_blocks: FileBlocks) -> Self {
        let len = file_blocks.status.len;
        let close_reader_limit = fs_context.conf.client.close_reader_limit;
        Self {
            path,
            fs_context,
            file_blocks: SearchFileBlocks::new(file_blocks),
            pos: 0,
            len,
            cur_reader: None,
            close_reader_times: 0,
            close_reader_limit,
            all_reader: FastHashMap::default(),
        }
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
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

    pub fn path_str(&self) -> &str {
        self.path.path()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn fs_context(&self) -> &FsContext {
        &self.fs_context
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::empty());
        }

        let cur_reader = self.get_reader().await?;
        let chunk = cur_reader.read().await?;
        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    pub fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        if self.pos == self.len {
            return Ok(DataSlice::empty());
        }

        let cur_reader = rt.block_on(self.get_reader())?;
        let chunk = cur_reader.blocking_read(rt)?;
        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos == self.pos {
            return Ok(());
        } else if pos > self.len {
            return err_box!("seek position {} can not exceed file len {}", pos, self.len);
        }

        let (block_off, loc) = self.file_blocks.get_read_block(pos)?;
        if let Some(reader) = &mut self.cur_reader {
            // Check if the target position is in the current block
            if reader.block_id() == loc.block.id {
                // Within the same block, seek to the correct block offset
                reader.seek(block_off)?;
            } else {
                self.close_reader_times = self.close_reader_times.saturating_add(1);
                self.update_reader(None).await?;
            }
        }

        self.pos = pos;
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        if let Some(mut reader) = self.cur_reader.take() {
            reader.complete().await?;
        }
        // Clean all cached readers.
        for (_, mut reader) in self.all_reader.drain() {
            reader.complete().await?;
        }

        Ok(())
    }

    async fn update_reader(&mut self, cur: Option<BlockReader>) -> FsResult<()> {
        if let Some(mut old) = mem::replace(&mut self.cur_reader, cur) {
            if self.close_reader_times > self.close_reader_limit {
                self.all_reader.insert(old.block_id(), old);
            } else {
                old.complete().await?;
            }
        }

        Ok(())
    }

    async fn get_reader(&mut self) -> FsResult<&mut BlockReader> {
        match &self.cur_reader {
            Some(v) if v.has_remaining() => (),

            _ => {
                let (block_off, loc) = self.file_blocks.get_read_block(self.pos)?;
                let mut new_reader = match self.all_reader.remove(&loc.block.id) {
                    Some(v) => {
                        // Use the existing block reader
                        v
                    }

                    None => {
                        // Create a new block reader
                        BlockReader::new(self.fs_context.clone(), loc.clone(), block_off).await?
                    }
                };
                new_reader.seek(block_off)?;
                self.update_reader(Some(new_reader)).await?;
            }
        }

        Ok(try_option_mut!(self.cur_reader))
    }
}
