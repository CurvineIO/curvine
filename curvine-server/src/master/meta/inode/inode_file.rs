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

use crate::master::meta::feature::{AclFeature, FileFeature, WriteFeature};
use crate::master::meta::inode::{Inode, EMPTY_PARENT_ID};
use crate::master::meta::{BlockMeta, InodeId};
use curvine_common::state::{
    CommitBlock, CreateFileOpts, ExtendedBlock, FileAllocOpts, FileType, StoragePolicy,
};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::{err_box, CommonResult};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeFile {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) file_type: FileType,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,

    pub(crate) len: i64,
    pub(crate) block_size: i64,
    pub(crate) replicas: u16,

    pub(crate) storage_policy: StoragePolicy,

    pub(crate) features: FileFeature,

    pub(crate) blocks: Vec<BlockMeta>,

    // Number of hard links to this file
    pub(crate) nlink: u32,

    // Next sequence number for block ID generation (auto-increment)
    pub(crate) next_seq: u32,

    pub(crate) target: Option<String>,
}

impl InodeFile {
    pub fn new(id: i64, time: i64) -> Self {
        Self {
            id,
            file_type: FileType::File,
            mtime: time,
            atime: time,
            len: 0,
            block_size: 0,
            replicas: 0,

            storage_policy: Default::default(),
            features: FileFeature::new(),

            blocks: vec![],
            nlink: 1,
            next_seq: 0,
            target: None,
            parent_id: EMPTY_PARENT_ID,
        }
    }

    pub fn with_opts(id: i64, time: i64, opts: CreateFileOpts) -> InodeFile {
        let mut file = Self {
            id,
            file_type: opts.file_type,
            mtime: time,
            atime: time,
            len: 0,
            block_size: opts.block_size,
            replicas: opts.replicas,

            storage_policy: opts.storage_policy,
            features: FileFeature {
                x_attr: Default::default(),
                file_write: None,
                acl: AclFeature {
                    mode: opts.mode,
                    owner: opts.owner,
                    group: opts.group,
                },
            },

            blocks: vec![],
            nlink: 1,
            next_seq: 0,
            target: None,
            parent_id: EMPTY_PARENT_ID,
        };

        file.features.set_writing(opts.client_name);
        if !opts.x_attr.is_empty() {
            file.features.set_attrs(opts.x_attr);
        }

        file.features.set_mode(opts.mode);

        file
    }

    pub fn with_link(id: i64, time: i64, target: impl Into<String>, mode: u32) -> Self {
        Self {
            id,
            file_type: FileType::Link,
            mtime: time,
            atime: time,
            len: 0,
            block_size: 0,
            replicas: 0,

            storage_policy: Default::default(),
            features: FileFeature {
                x_attr: Default::default(),
                file_write: None,
                acl: AclFeature::with_mode(mode),
            },

            blocks: vec![],
            nlink: 1,
            next_seq: 0,
            target: Some(target.into()),
            parent_id: EMPTY_PARENT_ID,
        }
    }

    pub fn block_ids(&self) -> Vec<i64> {
        self.blocks.iter().map(|x| x.id).collect()
    }

    pub fn is_complete(&self) -> bool {
        self.features.file_write.is_none()
    }

    pub fn is_writing(&self) -> bool {
        self.features.file_write.is_some()
    }

    pub fn write_feature(&self) -> Option<&WriteFeature> {
        self.features.file_write.as_ref()
    }

    pub fn add_block(&mut self, id: BlockMeta) {
        self.blocks.push(id)
    }

    pub fn compute_len(&self) -> i64 {
        self.blocks.iter().map(|x| x.len).sum()
    }

    pub fn commit_len(&self, last: Option<&CommitBlock>) -> i64 {
        self.compute_len() + last.map(|x| x.block_len).unwrap_or(0)
    }

    fn calc_pos(&self, pos: i32) -> usize {
        if pos < 0 {
            (self.blocks.len() as i32 + pos) as usize
        } else {
            pos as usize
        }
    }

    pub fn get_block(&self, pos: i32) -> Option<&BlockMeta> {
        let pos = self.calc_pos(pos);
        if pos < self.blocks.len() {
            Some(&self.blocks[pos])
        } else {
            None
        }
    }

    pub fn get_block_mut(&mut self, pos: i32) -> Option<&mut BlockMeta> {
        let pos = self.calc_pos(pos);
        if pos < self.blocks.len() {
            Some(&mut self.blocks[pos])
        } else {
            None
        }
    }

    pub fn get_block_check(&self, pos: i32) -> CommonResult<&BlockMeta> {
        match self.get_block(pos) {
            None => err_box!("Not found block, pos = {}", pos),
            Some(v) => Ok(v),
        }
    }

    pub fn reopen(&mut self, client_name: impl AsRef<str>) -> Option<ExtendedBlock> {
        self.features.set_writing(client_name.as_ref().to_string());
        if let Some(last_block) = self.get_block_mut(-1) {
            let blk = ExtendedBlock {
                id: last_block.id,
                len: last_block.len,
                alloc_opts: last_block.alloc_opts.clone(),
                storage_type: self.storage_policy.storage_type,
                file_type: self.file_type,
            };
            Some(blk)
        } else {
            None
        }
    }

    /// Create a new block id
    /// It is composed of the inode id + block number of the file, starting from 1.
    /// inode id + serial number 0, is the file id.
    pub fn next_block_id(&mut self) -> CommonResult<i64> {
        let seq = self.next_seq as i64;
        self.next_seq += 1;
        InodeId::create_block_id(self.id, seq)
    }

    pub fn simple_string(&self) -> String {
        format!(
            "id={}, pid={}, len={}, nlink={}, blocks={:?}",
            self.id,
            self.parent_id,
            self.len,
            self.nlink,
            self.block_ids()
        )
    }

    // Decrement link count
    pub fn decrement_nlink(&mut self) -> u32 {
        if self.nlink > 0 {
            self.nlink -= 1;
        }
        self.nlink
    }

    // Get current link count
    pub fn nlink(&self) -> u32 {
        self.nlink
    }

    // Check if this is the last link
    pub fn is_last_link(&self) -> bool {
        self.nlink <= 1
    }

    /// Update file metadata for overwrite operation
    pub fn overwrite(&mut self, opts: CreateFileOpts, mtime: i64) {
        // Clear all blocks and reset file size
        self.blocks.clear();
        self.len = 0;

        // Update file metadata with new options
        self.replicas = opts.replicas;
        self.block_size = opts.block_size;
        self.storage_policy = opts.storage_policy;
        self.mtime = mtime;

        // Reset file writing state for new write operation
        self.features.set_writing(opts.client_name);
    }

    pub fn search_block_mut(&mut self, block_id: i64) -> Option<&mut BlockMeta> {
        let idx = self
            .blocks
            .binary_search_by_key(&block_id, |lb| lb.id)
            .ok()?;
        self.blocks.get_mut(idx)
    }

    pub fn search_block_mut_check(&mut self, block_id: i64) -> FsResult<&mut BlockMeta> {
        match self.search_block_mut(block_id) {
            Some(v) => Ok(v),
            None => err_box!("Not found block, block_id = {}", block_id),
        }
    }

    pub fn search_next_block(&self, previous: Option<i64>) -> Option<&BlockMeta> {
        match previous {
            None => self.blocks.first(),
            Some(previous) => {
                let idx = self
                    .blocks
                    .binary_search_by_key(&previous, |lb| lb.id)
                    .ok()?;
                self.blocks.get(idx + 1)
            }
        }
    }

    pub fn complete(
        &mut self,
        len: i64,
        commit_blocks: &[CommitBlock],
        client_name: impl AsRef<str>,
        only_flush: bool,
    ) -> FsResult<()> {
        for block in commit_blocks {
            let meta = self.search_block_mut_check(block.block_id)?;
            meta.commit(block);
        }
        self.len = self.len.max(len);
        let complete_len = self.compute_len();
        if complete_len != self.len {
            return err_box!(
                "Complete len is not equal to file len, complete_len = {}, file_len = {}",
                complete_len,
                self.len
            );
        }

        self.mtime = LocalTime::mills() as i64;
        if !only_flush {
            self.features.complete_write(client_name);
        }
        Ok(())
    }

    /// Search for block by file position
    /// Returns the block reference if found
    pub fn search_block_mut_by_pos(&mut self, file_pos: i64) -> Option<&mut BlockMeta> {
        if file_pos < 0 {
            return None;
        }

        let mut current = 0i64;
        for block in &mut self.blocks {
            let block_end = current + block.len;
            if file_pos >= current && file_pos < block_end {
                return Some(block);
            }
            current = block_end;
        }
        None
    }

    pub fn last_block_start_off(&self) -> i64 {
        if self.blocks.is_empty() {
            return 0;
        }
        self.blocks[..self.blocks.len() - 1]
            .iter()
            .map(|block| block.len)
            .sum()
    }

    /// Resize the file to the specified length.
    ///
    /// This method handles three cases:
    /// - If the new length equals the current length, no operation is needed
    /// - If the new length is smaller, truncate the file (remove excess blocks)
    /// - If the new length is larger, extend the file (allocate new blocks)
    ///
    /// # Arguments
    /// * `opts` - File allocation options containing the target length
    ///
    /// # Returns
    /// * `Vec<BlockMeta>` - Blocks that were removed during truncation (empty if extended or unchanged)
    pub fn resize(&mut self, opts: FileAllocOpts) -> FsResult<Vec<BlockMeta>> {
        if self.len == opts.len {
            Ok(vec![])
        } else if opts.len < self.len {
            let del_blocks = self.truncate(opts);
            Ok(del_blocks)
        } else {
            self.extend(opts)?;
            Ok(vec![])
        }
    }

    /// Extend the file to the specified length by allocating new blocks or extending existing ones.
    ///
    /// This method processes blocks from the last block's start position to the target length.
    /// For each block boundary:
    /// - If a block exists at the position, extend its length
    /// - If no block exists, create a new allocated block
    ///
    /// # Arguments
    /// * `opts` - File allocation options containing the target length
    fn extend(&mut self, opts: FileAllocOpts) -> FsResult<()> {
        let expect_len = opts.len;
        let block_size = self.block_size;
        let mut start = self.last_block_start_off();

        // Start from the last block's start position, process each block until expect_len
        while start < expect_len {
            let resize_len = block_size.min(expect_len - start);
            let block_opts = opts.clone_with_len(resize_len);

            if let Some(block) = self.search_block_mut_by_pos(start) {
                // Found existing block, extend its length
                block.len = resize_len;
                block.alloc_opts.replace(block_opts);
            } else {
                // No block found, create new block
                let new_block_id = self.next_block_id()?;
                self.add_block(BlockMeta::with_alloc(new_block_id, block_opts));
            }
            start += block_size;
        }

        self.len = expect_len;
        Ok(())
    }

    /// Truncate the file to the specified length by removing blocks beyond the target length.
    ///
    /// This method handles three cases:
    /// - If a block starts beyond the target length, remove it and all subsequent blocks
    /// - If a block spans the target length, truncate it and remove subsequent blocks
    /// - If a block ends before the target length, keep it unchanged
    ///
    /// # Arguments
    /// * `opts` - File allocation options containing the target length
    ///
    /// # Returns
    /// * `Vec<BlockMeta>` - Blocks that were removed during truncation
    fn truncate(&mut self, opts: FileAllocOpts) -> Vec<BlockMeta> {
        let expect_len = opts.len;
        let mut remove_start = None;

        let mut start = 0;
        for (idx, block) in self.blocks.iter_mut().enumerate() {
            let end = start + block.len;

            if start >= expect_len {
                // Current block's start position exceeds expect_len, delete all blocks starting from current block
                remove_start.replace(idx);
                break;
            } else if end > expect_len {
                // Current block spans expect_len, needs truncation
                let new_len = expect_len - start;
                if new_len > 0 {
                    // Truncate current block, keep first new_len bytes
                    block.len = new_len;
                    block.alloc_opts.replace(opts.clone_with_len(new_len));
                    remove_start.replace(idx + 1); // Delete subsequent blocks
                } else {
                    // new_len == 0, delete current block and subsequent blocks
                    remove_start.replace(idx);
                }
                break;
            }
            // end <= expect_len, keep current block, continue processing next

            start = end;
        }

        let mut del_blocks = vec![];
        if let Some(start_idx) = remove_start {
            // start_idx may equal blocks.len(), drain won't panic, just returns empty iterator
            for block in self.blocks.drain(start_idx..) {
                del_blocks.push(block);
            }
        }

        self.len = expect_len;
        del_blocks
    }
}

impl Inode for InodeFile {
    fn id(&self) -> i64 {
        self.id
    }

    fn parent_id(&self) -> i64 {
        self.parent_id
    }

    fn is_dir(&self) -> bool {
        false
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }

    fn atime(&self) -> i64 {
        self.atime
    }

    fn nlink(&self) -> u32 {
        self.nlink
    }
}

impl PartialEq for InodeFile {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
