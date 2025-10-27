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

use crate::block::BlockWriter;
use crate::file::{FsClient, FsContext};
use curvine_common::fs::Path;
use curvine_common::state::{FileBlocks, FileStatus, WriteFileBlocks};
use curvine_common::FsResult;
use orpc::common::FastHashMap;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::{err_box, try_option_mut};
use std::mem;
use std::sync::Arc;

pub struct FsWriterBase {
    fs_context: Arc<FsContext>,
    fs_client: FsClient,
    path: Path,
    pos: i64,
    len: i64,
    file_blocks: WriteFileBlocks,
    cur_writer: Option<BlockWriter>,

    close_writer_times: u32,
    close_writer_limit: u32,
    all_writers: FastHashMap<i64, BlockWriter>,
}

impl FsWriterBase {
    pub fn new(fs_context: Arc<FsContext>, path: Path, status: FileBlocks, pos: i64) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let close_writer_limit = fs_context.conf.client.close_writer_limit;
        let len = status.len;
        let file_blocks = WriteFileBlocks::new(status);
        Self {
            fs_context,
            fs_client,
            pos,
            len,
            file_blocks,
            path,
            cur_writer: None,
            close_writer_times: 0,
            close_writer_limit,
            all_writers: FastHashMap::default(),
        }
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn status(&self) -> &FileStatus {
        &self.file_blocks.status
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

    pub async fn write(&mut self, mut chunk: DataSlice) -> FsResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        let mut remaining = chunk.len();
        while remaining > 0 {
            let cur_writer = self.get_writer().await?;
            let write_len = remaining.min(cur_writer.remaining() as usize);
            cur_writer.write(chunk.split_to(write_len)).await?;

            remaining -= write_len;
            self.pos += write_len as i64;
            if self.pos > self.len {
                self.len = self.pos;
            }
        }

        Ok(())
    }

    /// Block write.
    /// Explain why there is a separate blocking_write instead of rt.block_on(self.write)
    /// We hope to reduce thread switching for writing local files, and the logic of network writing and rt.block_on(self.write) is consistent.
    /// Local write will directly write to the file, without any thread switching.
    pub fn blocking_write(&mut self, rt: &Runtime, mut chunk: DataSlice) -> FsResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        let mut remaining = chunk.len();
        while remaining > 0 {
            let cur_writer = rt.block_on(self.get_writer())?;
            let write_len = remaining.min(cur_writer.remaining() as usize);

            // Write data request.
            cur_writer.blocking_write(rt, chunk.split_to(write_len))?;

            remaining -= write_len;
            self.pos += write_len as i64;
            if self.pos > self.len {
                self.len = self.pos;
            }
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        // Just flush the block writer in the current write state.
        match &mut self.cur_writer {
            None => Ok(()),
            Some(writer) => writer.flush().await,
        }
    }

    // Write is completed, perform the following operations
    // 1. Submit the last block.
    pub async fn complete(&mut self) -> FsResult<()> {
        if let Some(mut writer) = self.cur_writer.take() {
            let commit_block = writer.complete().await?;
            self.file_blocks.update_block(commit_block)?;
        };

        for (_, mut writer) in self.all_writers.drain() {
            let commit_block = writer.complete().await?;
            self.file_blocks.update_block(commit_block)?;
        }

        let last_commit = self.file_blocks.take_last_commit();
        self.fs_client
            .complete_file(&self.path, self.len, last_commit)
            .await?;
        Ok(())
    }

    async fn get_writer(&mut self) -> FsResult<&mut BlockWriter> {
        match &mut self.cur_writer {
            Some(v) if v.has_remaining() => (),

            _ => {
                let block = self.file_blocks.get_block(self.pos);
                match block {
                    // step1: If block already exists, seek operation exists, need to overwrite previous block.
                    // Multiple seek operations will automatically cache block writer, so need to check block writer cache.
                    Some((off, lb)) => {
                        let writer = match self.all_writers.remove(&lb.id) {
                            Some(v) => v,
                            None => {
                                BlockWriter::new(self.fs_context.clone(), lb.clone(), off).await?
                            }
                        };
                        self.update_writer(Some(writer)).await?;
                    }

                    None => {
                        // step2: Need to apply for a new block.
                        let commit_block = match self.cur_writer.take() {
                            Some(mut v) => Some(v.complete().await?),
                            _ => None,
                        };

                        // Apply for a new block
                        let lb = self
                            .fs_client
                            .add_block(&self.path, commit_block, &self.fs_context.client_addr)
                            .await?;

                        self.file_blocks.add_block(lb.clone())?;

                        let writer =
                            BlockWriter::new(self.fs_context.clone(), lb.clone(), 0).await?;
                        let _ = self.cur_writer.insert(writer);
                    }
                };
            }
        }

        Ok(try_option_mut!(self.cur_writer))
    }

    // Implement seek support for random writes
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative position: {}", pos);
        } else if pos == self.pos() {
            return Ok(());
        }

        let (block_off, seek_block) = self.file_blocks.get_block_check(pos)?;
        // Check if we have a current writer
        if let Some(writer) = &mut self.cur_writer {
            if writer.block_id() != seek_block.block.id {
                // If seek position is outside current block, clear current writer through update_writer
                // This ensures all writer caching logic is handled consistently in update_writer
                self.close_writer_times = self.close_writer_times.saturating_add(1);
                self.update_writer(None).await?;
            } else {
                writer.seek(block_off).await?;
            }
        }

        self.pos = pos;
        Ok(())
    }

    async fn update_writer(&mut self, cur: Option<BlockWriter>) -> FsResult<()> {
        if let Some(mut old) = mem::replace(&mut self.cur_writer, cur) {
            if self.close_writer_times > self.close_writer_limit {
                self.all_writers.insert(old.block_id(), old);
            } else {
                let commit_block = old.complete().await?;
                self.file_blocks.update_block(commit_block)?;
            }
        }

        Ok(())
    }
}
