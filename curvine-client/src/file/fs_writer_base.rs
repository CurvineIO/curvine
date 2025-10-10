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
use curvine_common::state::{FileBlocks, FileStatus, LocatedBlock, SearchFileBlocks};
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
    file_blocks: Option<SearchFileBlocks>,

    // Block management
    last_block: Option<LocatedBlock>,
    cur_writer: Option<BlockWriter>,

    // Pending commit block from seek operations
    pending_commit_block: Option<curvine_common::state::CommitBlock>,

    // Writer cache similar to reader implementation
    close_writer_times: u32,
    close_writer_limit: u32,
    all_writers: FastHashMap<i64, BlockWriter>,

    // Track maximum written position for correct file length calculation
    max_written_pos: i64,

    // Track actual write ranges for each block (block_id -> (start_pos, end_pos))
    block_write_ranges: FastHashMap<i64, (i64, i64)>,
}

impl FsWriterBase {
    pub fn new(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let close_writer_limit = fs_context.conf.client.close_writer_limit;

        let initial_len = status.len;
        Self {
            fs_context,
            fs_client,
            pos: initial_len,
            path,
            status,
            file_blocks: None, // New file, no block information
            last_block,
            cur_writer: None,
            pending_commit_block: None,
            close_writer_times: 0,
            close_writer_limit,
            all_writers: FastHashMap::default(),
            max_written_pos: initial_len, // Initialize to current file length
            block_write_ranges: FastHashMap::default(),
        }
    }

    // Constructor with random write support, accepts file block information
    pub fn with_blocks(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        file_blocks: FileBlocks,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let close_writer_limit = fs_context.conf.client.close_writer_limit;

        let initial_len = status.len;
        Self {
            fs_context,
            fs_client,
            pos: initial_len,
            path,
            status,
            file_blocks: Some(SearchFileBlocks::new(file_blocks)), // Support random writes
            last_block,
            cur_writer: None,
            pending_commit_block: None,
            close_writer_times: 0,
            close_writer_limit,
            all_writers: FastHashMap::default(),
            max_written_pos: initial_len, // Initialize to current file length
            block_write_ranges: FastHashMap::default(),
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

    // Maintain file_blocks structure
    fn add_block_to_file_blocks(
        &mut self,
        block_id: i64,
        block_len: i64,
        locs: Vec<curvine_common::state::WorkerAddress>,
    ) {
        let new_located_block = LocatedBlock {
            block: curvine_common::state::ExtendedBlock {
                id: block_id,
                len: block_len,
                storage_type: curvine_common::state::StorageType::Disk,
                file_type: curvine_common::state::FileType::File,
            },
            locs,
        };

        if let Some(ref mut file_blocks) = self.file_blocks {
            // Add block to existing file_blocks
            let mut file_blocks_data = FileBlocks {
                status: file_blocks.status.clone(),
                block_locs: file_blocks.block_locs.clone(),
            };
            file_blocks_data.block_locs.push(new_located_block);

            // Update file_blocks
            *file_blocks = SearchFileBlocks::new(file_blocks_data);
        } else {
            // Create new file_blocks
            let file_blocks_data = FileBlocks {
                status: self.status.clone(),
                block_locs: vec![new_located_block],
            };

            self.file_blocks = Some(SearchFileBlocks::new(file_blocks_data));
        }
    }

    // Calculate correct file length (support random write and file holes)
    fn calculate_file_length(&self) -> i64 {
        if self.file_blocks.is_some() {
            // Existing file: calculate length based on file_blocks
            self.calculate_length_from_blocks()
        } else {
            // New file: calculate length based on write position
            self.calculate_length_from_position()
        }
    }

    // Calculate file length based on file_blocks
    fn calculate_length_from_blocks(&self) -> i64 {
        if let Some(file_blocks) = &self.file_blocks {
            let block_size = self.fs_context.block_size();
            let mut max_written_pos = 0i64;

            // Iterate through all blocks, calculate max position using actual write ranges
            for (i, block) in file_blocks.block_locs.iter().enumerate() {
                let block_start = i as i64 * block_size;
                let block_id = block.block.id;

                // Check actual write range of this block
                if let Some((_write_start, write_end)) = self.block_write_ranges.get(&block_id) {
                    // Use actual write end position
                    max_written_pos = max_written_pos.max(*write_end);
                } else if block.block.len > 0 {
                    // If no write range record but block has length, use original logic
                    let block_end = block_start + block.block.len;
                    max_written_pos = max_written_pos.max(block_end);
                }
            }

            // Consider max_written_pos (may include newly allocated blocks not in file_blocks)

            max_written_pos.max(self.max_written_pos)
        } else {
            // If no file_blocks, fall back to position-based calculation
            self.calculate_length_from_position()
        }
    }

    // Calculate file length based on write position (new file scenario)
    fn calculate_length_from_position(&self) -> i64 {
        // Use max_written_pos to ensure all written data is included

        self.max_written_pos
    }

    // Find and complete the actual last block
    async fn find_and_complete_last_block(
        &mut self,
    ) -> FsResult<Option<curvine_common::state::CommitBlock>> {
        // Collect all possible block writers and their position info
        let mut block_writers = Vec::new();

        // 1. Add current writer
        if let Some(writer) = self.cur_writer.take() {
            let block_id = writer.block_id();
            let block_pos = self.get_block_position(block_id);
            block_writers.push((block_pos, writer));
        }

        // 2. Add cached writers
        let cached_writers: Vec<_> = self.all_writers.drain().collect();
        for (block_id, writer) in cached_writers {
            let block_pos = self.get_block_position(block_id);
            block_writers.push((block_pos, writer));
        }

        if block_writers.is_empty() {
            return Ok(None);
        }

        // 3. Sort by position and find the last block
        block_writers.sort_by_key(|(pos, _)| *pos);

        let mut last_commit_block = None;

        // 4. Complete all writers, keep the last one as CommitBlock
        let total_writers = block_writers.len();
        for (i, (_block_pos, mut writer)) in block_writers.into_iter().enumerate() {
            let is_last = i == total_writers - 1;
            writer.complete().await?;

            if is_last {
                last_commit_block = Some(writer.to_commit_block());
            }
        }

        Ok(last_commit_block)
    }

    // Get block position in file (for sorting)
    fn get_block_position(&self, block_id: i64) -> i64 {
        if let Some(file_blocks) = &self.file_blocks {
            let block_size = self.fs_context.block_size();

            // Find block index in file_blocks
            for (i, located_block) in file_blocks.block_locs.iter().enumerate() {
                if located_block.block.id == block_id {
                    let position = i as i64 * block_size;
                    return position;
                }
            }
        }

        // If not found in file_blocks, use block_write_ranges
        if let Some((start_pos, _)) = self.block_write_ranges.get(&block_id) {
            return *start_pos;
        }

        // Default return 0 (first newly allocated block)
        0
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

            // Update maximum written position
            self.max_written_pos = self.max_written_pos.max(self.pos);

            // Record current block write range
            if let Some(writer) = &self.cur_writer {
                let block_id = writer.block_id();
                let write_start = self.pos - write_len as i64;
                let write_end = self.pos;

                // Update or insert block write range
                self.block_write_ranges
                    .entry(block_id)
                    .and_modify(|(start, end)| {
                        *start = (*start).min(write_start);
                        *end = (*end).max(write_end);
                    })
                    .or_insert((write_start, write_end));
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
        // Find the actual last block (sorted by file offset)
        let last_block = self.find_and_complete_last_block().await?;

        // Clean up all cached writers
        for (_, mut writer) in self.all_writers.drain() {
            writer.complete().await?;
        }

        // Calculate correct file length
        let file_length = self.calculate_file_length();

        self.fs_client
            .complete_file(&self.path, file_length, last_block)
            .await?;
        Ok(())
    }

    // Implement seek support for random writes
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(format!("Cannot seek to negative position: {pos}").into());
        }

        // Check if we have a current writer
        if let Some(writer) = &mut self.cur_writer {
            let block_start = self.pos - writer.pos();
            let block_end = block_start + writer.len();

            // If seek position is within current block range, seek directly
            if pos >= block_start && pos < block_end {
                let block_offset = pos - block_start;
                writer.seek(block_offset).await?;
                self.pos = pos;
                return Ok(());
            }

            // If seek position is outside current block, clear current writer through update_writer
            // This ensures all writer caching logic is handled consistently in update_writer
            self.close_writer_times = self.close_writer_times.saturating_add(1);
            self.update_writer(None).await?;
        }

        // Update position
        self.pos = pos;
        Ok(())
    }

    async fn update_writer(&mut self, cur: Option<BlockWriter>) -> FsResult<()> {
        if let Some(mut old) = mem::replace(&mut self.cur_writer, cur) {
            let block_id = old.block_id();
            let has_data = old.pos() > 0;
            let is_full = !old.has_remaining();

            if is_full {
                // Block is full, must complete it and save commit block for next allocation
                old.complete().await?;
                let commit_block = old.to_commit_block();
                self.pending_commit_block = Some(commit_block);
            } else if has_data && self.close_writer_times <= self.close_writer_limit {
                // Block has data but not full, cache for reuse
                self.all_writers.insert(block_id, old);
            } else {
                // Either no data or cache limit exceeded, complete and close
                old.complete().await?;
            }
        }

        Ok(())
    }

    async fn get_writer(&mut self) -> FsResult<&mut BlockWriter> {
        match &self.cur_writer {
            Some(v) if v.has_remaining() => {}

            _ => {
                if let Some(_v) = &self.cur_writer {}

                let (_block_off, lb) = if let Some(file_blocks) = &self.file_blocks {
                    // Try to find in existing file blocks
                    match file_blocks.get_write_block(self.pos) {
                        Ok((block_off, located_block)) => {
                            // Found existing block, check if there's a cached writer
                            let new_writer = match self.all_writers.remove(&located_block.block.id)
                            {
                                Some(mut cached_writer) => {
                                    // Use cached block writer, but need to seek to correct position
                                    cached_writer.seek(block_off).await?;
                                    cached_writer
                                }
                                None => {
                                    // Create new block writer, then seek to correct position
                                    let mut new_writer = BlockWriter::new(
                                        self.fs_context.clone(),
                                        located_block.clone(),
                                    )
                                    .await?;
                                    new_writer.seek(block_off).await?;
                                    new_writer
                                }
                            };

                            (block_off, new_writer)
                        }
                        Err(_) => {
                            // Position exceeds file range, need to allocate new block
                            self.allocate_new_block().await?
                        }
                    }
                } else {
                    // Even for new files, some blocks may have been allocated, check file_blocks
                    if let Some(file_blocks) = &self.file_blocks {
                        match file_blocks.get_write_block(self.pos) {
                            Ok((block_off, located_block)) => {
                                // Check cached writer
                                let new_writer =
                                    match self.all_writers.remove(&located_block.block.id) {
                                        Some(mut cached_writer) => {
                                            cached_writer.seek(block_off).await?;
                                            cached_writer
                                        }
                                        None => {
                                            let mut new_writer = BlockWriter::new(
                                                self.fs_context.clone(),
                                                located_block.clone(),
                                            )
                                            .await?;
                                            new_writer.seek(block_off).await?;
                                            new_writer
                                        }
                                    };

                                (block_off, new_writer)
                            }
                            Err(_) => self.allocate_new_block().await?,
                        }
                    } else {
                        // Truly new file, allocate first block
                        self.allocate_new_block().await?
                    }
                };

                // Update current writer
                self.update_writer(Some(lb)).await?;
            }
        }

        let writer = try_option_mut!(self.cur_writer);

        Ok(writer)
    }

    async fn allocate_new_block(&mut self) -> FsResult<(i64, BlockWriter)> {
        let commit_block = if let Some(mut writer) = self.cur_writer.take() {
            writer.complete().await?;
            let commit_block = writer.to_commit_block();

            Some(commit_block)
        } else {
            self.pending_commit_block.take()
        };

        // Allocate new block
        let lb = if let Some(lb) = self.last_block.take() {
            lb
        } else {
            let located_block = self
                .fs_client
                .add_block(&self.path, commit_block, &self.fs_context.client_addr)
                .await?;

            located_block
        };

        let writer = BlockWriter::new(self.fs_context.clone(), lb.clone()).await?;

        // Update file_blocks structure
        let block_size = self.fs_context.block_size();
        self.add_block_to_file_blocks(lb.block.id, block_size, lb.locs.clone());

        Ok((0, writer)) // New block offset is 0
    }
}
