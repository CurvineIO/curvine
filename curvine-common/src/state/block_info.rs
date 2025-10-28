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

use crate::proto::BlockWriteRequest;
use crate::state::{FileStatus, FileType, StorageType, WorkerAddress};
use crate::FsResult;
use orpc::common::ByteUnit;
use orpc::{err_box, try_option, CommonResult};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, Range};

// block location information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockLocation {
    pub worker_id: u32,
    pub storage_type: StorageType,
}

impl BlockLocation {
    pub fn new(id: u32, storage_type: StorageType) -> Self {
        Self {
            worker_id: id,
            storage_type,
        }
    }

    pub fn with_id(id: u32) -> Self {
        Self::new(id, StorageType::Disk)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitBlock {
    pub block_id: i64,
    pub block_len: i64,
    pub locations: Vec<BlockLocation>,
}

impl From<&LocatedBlock> for CommitBlock {
    fn from(located_block: &LocatedBlock) -> Self {
        let locations: Vec<BlockLocation> = located_block
            .locs
            .iter()
            .map(|x| BlockLocation::new(x.worker_id, located_block.storage_type))
            .collect();

        Self {
            block_id: located_block.id,
            block_len: located_block.block.len,
            locations,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExtendedBlock {
    pub id: i64,
    pub len: i64,
    pub storage_type: StorageType,
    pub file_type: FileType,
}

impl ExtendedBlock {
    pub fn new(id: i64, len: i64, storage_type: StorageType, file_type: FileType) -> Self {
        Self {
            id,
            len,
            storage_type,
            file_type,
        }
    }

    pub fn with_id(id: i64) -> Self {
        Self::new(id, 0, StorageType::Disk, FileType::File)
    }

    pub fn with_size_str(id: i64, size: &str, stg_type: StorageType) -> CommonResult<Self> {
        let bytes = ByteUnit::from_str(size)?.as_byte();
        Ok(Self::new(id, bytes as i64, stg_type, FileType::File))
    }

    pub fn with_mem(id: i64, size: &str) -> CommonResult<Self> {
        Self::with_size_str(id, size, StorageType::Mem)
    }

    pub fn with_ssd(id: i64, size: &str) -> CommonResult<Self> {
        Self::with_size_str(id, size, StorageType::Ssd)
    }

    pub fn from_req(req: &BlockWriteRequest) -> Self {
        Self::new(
            req.id,
            req.len,
            StorageType::from(req.storage_type),
            FileType::from(req.file_type),
        )
    }

    pub fn size_string(&self) -> String {
        ByteUnit::byte_to_string(self.len as u64)
    }

    pub fn is_stream(&self) -> bool {
        self.file_type != FileType::File && self.file_type != FileType::Dir
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocatedBlock {
    pub block: ExtendedBlock,
    pub locs: Vec<WorkerAddress>,
}

impl Deref for LocatedBlock {
    type Target = ExtendedBlock;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileBlocks {
    pub status: FileStatus,
    pub block_locs: Vec<LocatedBlock>,
}

impl FileBlocks {
    pub fn new(status: FileStatus, block_locs: Vec<LocatedBlock>) -> Self {
        Self { status, block_locs }
    }

    // According to the file reading location, get the block that needs to be read and the offset in the block.
    pub fn get_read_block(&self, file_pos: i64) -> FsResult<(i64, LocatedBlock)> {
        let mut start_pos = 0;
        for block in &self.block_locs {
            let end_pos = start_pos + block.block.len;
            if file_pos >= start_pos && file_pos < end_pos {
                let block_off = file_pos - start_pos;
                return Ok((block_off, block.clone()));
            }
            start_pos = end_pos;
        }

        err_box!("Not found block for pos {}", file_pos)
    }
}

impl Deref for FileBlocks {
    type Target = FileStatus;

    fn deref(&self) -> &Self::Target {
        &self.status
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SearchFileBlocks {
    pub status: FileStatus,
    pub block_locs: Vec<LocatedBlock>,
    search_off: Vec<Range<i64>>,
}

impl SearchFileBlocks {
    pub fn new(file_blocks: FileBlocks) -> Self {
        let mut search_off = Vec::new();
        let mut off = 0;
        for block in &file_blocks.block_locs {
            let start = off;
            let end = off + block.block.len;
            search_off.push(Range { start, end });

            off = end;
        }

        Self {
            status: file_blocks.status,
            block_locs: file_blocks.block_locs,
            search_off,
        }
    }

    pub fn get_read_block(&self, file_pos: i64) -> FsResult<(i64, LocatedBlock)> {
        let index = self.search_off.partition_point(|x| x.end <= file_pos);
        if let Some(lc) = self.block_locs.get(index) {
            let block_off = file_pos - self.search_off[index].start;
            Ok((block_off, lc.clone()))
        } else {
            err_box!("Not found block for pos {}", file_pos)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WriteFileBlocks {
    pub status: FileStatus,
    pub block_locs: Vec<LocatedBlock>,
    search_off: Vec<Range<i64>>,
    pub last_commit: Option<CommitBlock>,
}

impl WriteFileBlocks {
    pub fn new(file_blocks: FileBlocks) -> Self {
        let mut search_off = Vec::new();
        let mut off = 0;
        for _ in &file_blocks.block_locs {
            let start = off;
            off += file_blocks.status.block_size;
            search_off.push(Range { start, end: off });
        }

        // file commit needs to submit the last block
        let last_commit = file_blocks.block_locs.last().map(|x| x.into());

        Self {
            status: file_blocks.status,
            block_locs: file_blocks.block_locs,
            search_off,
            last_commit,
        }
    }

    pub fn get_block(&self, file_pos: i64) -> Option<(i64, LocatedBlock)> {
        let index = self.search_off.partition_point(|x| x.end <= file_pos);
        if let Some(lc) = self.block_locs.get(index) {
            let block_off = file_pos - self.search_off[index].start;
            Some((block_off, lc.clone()))
        } else {
            None
        }
    }

    pub fn get_block_check(&self, file_pos: i64) -> FsResult<(i64, LocatedBlock)> {
        match self.get_block(file_pos) {
            Some(lb) => Ok(lb),
            None => err_box!("Not found block for pos {}, blocks: {:#?}", file_pos, self),
        }
    }

    pub fn update_block(&mut self, commit: CommitBlock) -> FsResult<()> {
        if let Some(v) = self.block_locs.last_mut() {
            if v.id == commit.block_id {
                v.block.len = commit.block_len;
                let _ = self.last_commit.insert(commit);
                Ok(())
            } else {
                Ok(())
            }
        } else {
            err_box!("Not found last block")
        }
    }

    pub fn add_block(&mut self, mut lb: LocatedBlock) -> FsResult<()> {
        if let Some(v) = self.block_locs.last_mut() {
            if v.id == lb.id {
                v.block.len = self.status.block_size;

                let last_search = try_option!(self.search_off.last_mut());
                last_search.end = last_search.start + self.status.block_size;

                return Ok(());
            } else if v.id > lb.id {
                return err_box!(
                    "Cannot add block: existing block id {} does not match new block id {}",
                    v.id,
                    lb.id
                );
            }
        };

        lb.block.len = self.status.block_size;
        self.block_locs.push(lb);

        let start = self.search_off.last().map(|x| x.end).unwrap_or(0);
        let end = start + self.status.block_size;
        self.search_off.push(Range { start, end });

        Ok(())
    }

    pub fn take_last_commit(&mut self) -> Option<CommitBlock> {
        self.last_commit.take()
    }
}
