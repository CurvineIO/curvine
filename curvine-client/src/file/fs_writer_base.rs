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
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{CommitBlock, FileAllocOpts, FileBlocks, FileStatus, WriteFileBlocks};
use curvine_common::FsResult;
use fxhash::FxHasher;
use linked_hash_map::LinkedHashMap;
use log::warn;
use orpc::common::FastHashSet;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::{err_box, try_option_mut};
use std::hash::BuildHasherDefault;
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

    cache_limit: usize,
    cache_writers: LinkedHashMap<i64, BlockWriter, BuildHasherDefault<FxHasher>>,
    /// Blocks whose contents must not be aborted during abnormal cleanup.
    ///
    /// This includes blocks already visible in the file when the writer was
    /// opened, blocks published by a successful flush, and blocks finalized on
    /// workers but still waiting for their commit metadata to reach the master.
    durable_blocks: FastHashSet<i64>,
}

impl FsWriterBase {
    pub fn new(fs_context: Arc<FsContext>, path: Path, status: FileBlocks, pos: i64) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let cache_limit = fs_context.conf.client.max_cache_block_handles;
        let len = status.len;
        let durable_blocks = FastHashSet::with_vec(
            status
                .block_locs
                .iter()
                .filter(|block| block.block.len > 0 && !block.locs.is_empty())
                .map(|block| block.block.id)
                .collect(),
        );
        let file_blocks = WriteFileBlocks::new(status);

        let cache_writers = LinkedHashMap::with_capacity_and_hasher(
            cache_limit,
            BuildHasherDefault::<FxHasher>::default(),
        );
        Self {
            fs_context,
            fs_client,
            pos,
            len,
            file_blocks,
            path,
            cur_writer: None,
            cache_limit,
            cache_writers,
            durable_blocks,
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

    pub fn file_blocks(&self) -> FileBlocks {
        FileBlocks::new(
            self.file_blocks.status.clone(),
            self.file_blocks.block_locs.clone(),
        )
    }

    pub async fn write(&mut self, mut chunk: DataSlice) -> FsResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        if self.pos > self.len {
            self.resize(FileAllocOpts::with_truncate(self.pos)).await?;
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

        if self.pos > self.len {
            rt.block_on(self.resize(FileAllocOpts::with_truncate(self.pos)))?;
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
        self.complete0(true).await?;
        Ok(())
    }

    // Write is completed, perform the following operations
    // 1. Submit the last block.
    pub async fn complete(&mut self) -> FsResult<()> {
        self.complete0(false).await?;
        Ok(())
    }

    fn add_durable_commit(&mut self, commit: CommitBlock) -> FsResult<()> {
        let block_id = commit.block_id;
        self.file_blocks.add_commit(commit)?;
        self.durable_blocks.insert(block_id);
        Ok(())
    }

    fn restore_commit_blocks(&mut self, commits: &[CommitBlock]) {
        for commit in commits {
            if let Err(e) = self.add_durable_commit(commit.clone()) {
                warn!(
                    "failed to restore pending commit for block {}: {}",
                    commit.block_id, e
                );
            }
        }
    }

    fn committed_len(&self) -> i64 {
        self.file_blocks
            .block_locs
            .iter()
            .map(|block| block.block.len)
            .sum()
    }

    /// Clean up every backend write session without invalidating durable data.
    ///
    /// A brand-new block that has never been flushed/finalized can be aborted.
    /// Once a block was published by flush, existed before this writer, or was
    /// finalized on a worker, aborting it would delete data the master may
    /// already reference. Such a block is finalized instead, and any pending
    /// commit metadata is submitted to the master with `only_flush=true`.
    /// Cleanup remains best effort: preserve the first error while attempting
    /// all remaining writers and the metadata submission.
    pub async fn cancel(&mut self) -> FsResult<()> {
        let mut first_error: Option<FsError> = None;
        let mut cleanup_commits = Vec::new();

        if let Some(mut writer) = self.cur_writer.take() {
            if self.durable_blocks.contains(&writer.block_id()) {
                match writer.complete().await {
                    Ok(commit) => cleanup_commits.push(commit),
                    Err(e) => first_error = Some(e),
                }
            } else if let Err(e) = writer.cancel().await {
                first_error = Some(e);
            }
        }

        for (_, writer) in self.cache_writers.iter_mut() {
            if self.durable_blocks.contains(&writer.block_id()) {
                match writer.complete().await {
                    Ok(commit) => cleanup_commits.push(commit),
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            } else if let Err(e) = writer.cancel().await {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        self.cache_writers.clear();

        for commit in cleanup_commits {
            if let Err(e) = self.add_durable_commit(commit) {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        let commit_blocks = self.file_blocks.take_commit_blocks();
        if !commit_blocks.is_empty() {
            let committed_len = self.committed_len();
            let result = self
                .fs_client
                .complete_file_by_id(
                    &self.path,
                    self.file_blocks.status.id,
                    committed_len,
                    commit_blocks.clone(),
                    true,
                )
                .await;
            if let Err(e) = result {
                // Retain the metadata in case the owner can retry cleanup. More
                // importantly, never compensate an ambiguous master response by
                // deleting blocks that may already have been published.
                self.restore_commit_blocks(&commit_blocks);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn complete0(&mut self, only_flush: bool) -> FsResult<Option<FileBlocks>> {
        if let Some(writer) = self.cur_writer.take() {
            self.cache_writers.insert(writer.block_id(), writer);
        };

        let mut writer_commits = Vec::with_capacity(self.cache_writers.len());
        for (_, writer) in self.cache_writers.iter_mut() {
            let commit_block = if only_flush {
                writer.flush().await?;
                writer.to_commit_block()
            } else {
                writer.complete().await?
            };

            writer_commits.push(commit_block);
        }

        for commit in writer_commits {
            self.file_blocks.add_commit(commit)?;
        }

        if !only_flush {
            self.cache_writers.clear();
        }

        let commit_blocks = self.file_blocks.take_commit_blocks();
        // From this point onward a request may have reached the master even if
        // its response is lost. Treat every submitted block as durable before
        // crossing that ambiguity boundary so later cleanup never aborts it.
        for commit in &commit_blocks {
            self.durable_blocks.insert(commit.block_id);
        }

        let result = self
            .fs_client
            .complete_file_by_id(
                &self.path,
                self.file_blocks.status.id,
                self.len,
                commit_blocks.clone(),
                only_flush,
            )
            .await;
        if result.is_err() {
            self.restore_commit_blocks(&commit_blocks);
        }
        result
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
                        let writer = match self.cache_writers.remove(&lb.id) {
                            Some(mut v) => {
                                // Writer from cache may have a different position, seek to correct offset
                                v.seek(off).await?;
                                v
                            }

                            None => {
                                let lb = if lb.should_assign() {
                                    let assign_lb = self
                                        .fs_client
                                        .assign_worker(&self.path, lb.block.clone())
                                        .await?;

                                    self.file_blocks.update_locate(&assign_lb)?;
                                    assign_lb
                                } else {
                                    lb
                                };
                                BlockWriter::new(
                                    self.fs_context.clone(),
                                    lb,
                                    off,
                                    self.file_blocks.status.block_size,
                                )
                                .await?
                            }
                        };

                        self.update_writer(Some(writer), true).await?;
                    }

                    None => {
                        self.update_writer(None, false).await?;

                        let commit_blocks = self.file_blocks.take_commit_blocks();
                        let last_block = self.file_blocks.last_block();
                        let add_result = self
                            .fs_client
                            .add_block_by_id(
                                &self.path,
                                self.file_blocks.status.id,
                                commit_blocks.clone(),
                                self.len,
                                last_block,
                            )
                            .await;
                        let lb = match add_result {
                            Ok(block) => block,
                            Err(e) => {
                                self.restore_commit_blocks(&commit_blocks);
                                return Err(e);
                            }
                        };
                        self.file_blocks.add_block(lb.clone())?;
                        let writer = BlockWriter::new(
                            self.fs_context.clone(),
                            lb.clone(),
                            0,
                            self.file_blocks.status.block_size,
                        )
                        .await?;

                        self.cur_writer.replace(writer);
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
        } else if pos > self.len {
            self.pos = pos;
            self.update_writer(None, true).await?;
            return Ok(());
        }

        let (block_off, seek_block) = self.file_blocks.get_block_check(pos)?;
        // Check if we have a current writer
        if let Some(writer) = &mut self.cur_writer {
            if writer.block_id() == seek_block.block.id {
                writer.seek(block_off).await?;
            } else {
                self.update_writer(None, true).await?;
            }
        }

        self.pos = pos;
        Ok(())
    }

    async fn update_writer(&mut self, cur: Option<BlockWriter>, cache: bool) -> FsResult<()> {
        let mut old = match mem::replace(&mut self.cur_writer, cur) {
            Some(v) => v,
            None => return Ok(()),
        };

        if cache && self.cache_limit > 0 {
            if self.cache_writers.len() >= self.cache_limit {
                if let Some((_, mut removed)) = self.cache_writers.pop_front() {
                    let commit_blocks = removed.complete().await?;
                    self.add_durable_commit(commit_blocks)?;
                }
            }
            self.cache_writers.insert(old.block_id(), old);
        } else {
            let commit_blocks = old.complete().await?;
            self.add_durable_commit(commit_blocks)?;
        }

        Ok(())
    }

    /// Resize the file to the specified length.
    ///
    /// This method coordinates the resize operation between client and master:
    /// 1. Submit all pending blocks before resize to ensure data consistency
    /// 2. Request master to resize the file metadata
    /// 3. Handle blocks that need reassignment due to resize
    /// 4. Update local writer state with new file blocks
    ///
    /// # Arguments
    /// * `opts` - File allocation options containing the target length and allocation mode
    ///
    /// # Returns
    /// * `FsResult<()>` - Success if resize completed, error otherwise
    ///
    /// # Note
    /// If a block with written data needs reassignment (has workers but new alloc_opts),
    /// it will be committed before reassignment. At most one such block exists.
    pub async fn resize(&mut self, opts: FileAllocOpts) -> FsResult<()> {
        opts.validate()?;
        let len = opts.len;

        // Step 1: Submit all blocks before resize
        if self.len > 0 {
            self.complete().await?;
        }

        // Step 2: Execute resize operation
        let file_blocks = self.fs_client.resize(&self.path, opts).await?;
        let mut file_blocks = WriteFileBlocks::new(file_blocks);
        let block_size = file_blocks.status.block_size;
        if file_blocks.len() != len {
            return err_box!(
                "Cannot resize file: {}, expect len {}, actual len {}",
                self.path,
                len,
                file_blocks.len()
            );
        }

        // Step 3: If a block with written data triggers reassignment, request worker to reassign the block.
        // At most one such block exists.
        for lb in &mut file_blocks.block_locs {
            if lb.should_resize() {
                let mut writer =
                    BlockWriter::new(self.fs_context.clone(), lb.clone(), 0, block_size).await?;
                let commit_block = writer.complete().await?;
                self.add_durable_commit(commit_block)?;
            }
        }

        // Step 4: Reset writer state
        self.pos = self.pos.min(len);
        self.len = len;
        self.file_blocks = file_blocks;
        self.durable_blocks = FastHashSet::with_vec(
            self.file_blocks
                .block_locs
                .iter()
                .filter(|block| block.block.len > 0 && !block.locs.is_empty())
                .map(|block| block.block.id)
                .collect(),
        );

        Ok(())
    }
}
