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
use curvine_common::state::{FileStatus, FileBlocks, LocatedBlock, SearchFileBlocks};
use curvine_common::FsResult;
use orpc::common::FastHashMap;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::try_option_mut;
use std::mem;
use std::sync::Arc;

pub struct FsWriterBase {
    fs_context: Arc<FsContext>,
    fs_client: FsClient,
    path: Path,
    pos: i64,
    status: FileStatus,
    
    // 🔑 文件块信息，支持随机写
    file_blocks: Option<SearchFileBlocks>,
    
    // 块管理
    last_block: Option<LocatedBlock>,
    cur_writer: Option<BlockWriter>,
    
    // 🔑 写入器缓存，类似读取路径的实现
    close_writer_times: u32,
    close_writer_limit: u32,
    all_writers: FastHashMap<i64, BlockWriter>,
}

impl FsWriterBase {
    pub fn new(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let close_writer_limit = fs_context.conf.client.close_reader_limit; // 复用读取器的限制配置

        Self {
            fs_context,
            fs_client,
            pos: status.len,
            path,
            status,
            file_blocks: None, // 新文件，没有块信息
            last_block,
            cur_writer: None,
            close_writer_times: 0,
            close_writer_limit,
            all_writers: FastHashMap::default(),
        }
    }

    // 🔑 新增：支持随机写的构造函数，接收文件块信息
    pub fn with_blocks(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        file_blocks: FileBlocks,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let close_writer_limit = fs_context.conf.client.close_reader_limit;

        Self {
            fs_context,
            fs_client,
            pos: status.len,
            path,
            status,
            file_blocks: Some(SearchFileBlocks::new(file_blocks)), // 🔑 支持随机写
            last_block,
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
        &self.status
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
            // Write data request.
            cur_writer.write(chunk.split_to(write_len)).await?;

            remaining -= write_len;
            self.pos += write_len as i64;
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
    // 2. Clean up all cached writers.
    pub async fn complete(&mut self) -> FsResult<()> {
        let last_block = match self.cur_writer.take() {
            None => None,
            Some(mut writer) => {
                writer.complete().await?;
                Some(writer.to_commit_block())
            }
        };

        // 🔑 清理所有缓存的写入器
        for (_, mut writer) in self.all_writers.drain() {
            writer.complete().await?;
        }

        self.fs_client
            .complete_file(&self.path, self.pos, last_block)
            .await?;
        Ok(())
    }
    
    // 🔑 实现随机写 seek 支持
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(format!("Cannot seek to negative position: {pos}").into());
        }
        
        // 如果有当前writer，需要先检查seek位置是否在当前block范围内
        if let Some(writer) = &mut self.cur_writer {
            let block_start = self.pos - writer.pos();
            let block_end = block_start + writer.len();
            
            // 如果seek位置在当前block范围内，直接seek
            if pos >= block_start && pos < block_end {
                let block_offset = pos - block_start;
                writer.seek(block_offset).await?;
                self.pos = pos;
                return Ok(());
            }
        }
        
        // 如果seek位置不在当前block范围内，需要获取新的block
        // 这里简化处理：清空当前writer，更新位置
        // 实际使用时，get_writer会根据新位置获取正确的block
        self.cur_writer = None;
        self.pos = pos;
        
        Ok(())
    }

    // 🔑 类似读取路径的写入器更新逻辑
    async fn update_writer(&mut self, cur: Option<BlockWriter>) -> FsResult<()> {
        if let Some(mut old) = mem::replace(&mut self.cur_writer, cur) {
            if self.close_writer_times > self.close_writer_limit {
                // 缓存写入器以便重用
                let block_id = old.block_id();
                self.all_writers.insert(block_id, old);
            } else {
                // 完成并关闭写入器
                old.complete().await?;
            }
        }
        Ok(())
    }

    async fn get_writer(&mut self) -> FsResult<&mut BlockWriter> {
        match &self.cur_writer {
            Some(v) if v.has_remaining() => (),
            
            _ => {
                // 🔑 根据当前位置获取对应的块
                let (_block_off, lb) = if let Some(file_blocks) = &self.file_blocks {
                    // 尝试在现有文件块中查找
                    match file_blocks.get_write_block(self.pos) {
                        Ok((block_off, located_block)) => {
                            // 找到现有块，检查是否有缓存的写入器
                            let new_writer = match self.all_writers.remove(&located_block.block.id) {
                                Some(mut cached_writer) => {
                                    // 🔑 使用缓存的块写入器，但需要seek到正确位置
                                    cached_writer.seek(block_off).await?;
                                    cached_writer
                                }
                                None => {
                                    // 🔑 创建新的块写入器，直接传递block_off
                                    BlockWriter::with_offset(
                                        self.fs_context.clone(),
                                        located_block.clone(),
                                        Some(block_off),
                                    )
                                    .await?
                                }
                            };
                            
                            (block_off, new_writer)
                        }
                        Err(_) => {
                            // 位置超出现有文件范围，需要分配新块
                            self.allocate_new_block().await?
                        }
                    }
                } else {
                    // 新文件，直接分配新块
                    self.allocate_new_block().await?
                };
                
                // 更新当前写入器
                self.update_writer(Some(lb)).await?;
            }
        }

        Ok(try_option_mut!(self.cur_writer))
    }

    // 🔑 分配新块的助手方法
    async fn allocate_new_block(&mut self) -> FsResult<(i64, BlockWriter)> {
        let commit_block = if let Some(mut writer) = self.cur_writer.take() {
            writer.complete().await?;
            Some(writer.to_commit_block())
        } else {
            None
        };

        // 分配新块
        let lb = if let Some(lb) = self.last_block.take() {
            lb
        } else {
            self.fs_client
                .add_block(&self.path, commit_block, &self.fs_context.client_addr)
                .await?
        };

        let writer = BlockWriter::new(self.fs_context.clone(), lb).await?;
        Ok((0, writer)) // 新块的偏移量为 0
    }
}
