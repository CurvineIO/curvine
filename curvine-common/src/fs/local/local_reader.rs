//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::fs::{Path, Reader};
use crate::state::FileStatus;
use crate::utils::CommonUtils;
use crate::FsResult;
use orpc::io::LocalFile;
use orpc::sys::DataSlice;

pub struct LocalReader {
    path: Path,
    file: LocalFile,
    pos: i64,
    status: FileStatus,
    chunk: DataSlice,
    chunk_size: usize,
}

impl LocalReader {
    pub fn new(path: &Path, chunk_size: usize) -> FsResult<Self> {
        let file = LocalFile::with_read(path.path(), 0)?;
        let status = CommonUtils::file_to_status(path, &file)?;
        Ok(Self {
            path: path.clone(),
            file,
            pos: 0,
            status,
            chunk_size,
            chunk: DataSlice::Empty,
        })
    }
}

impl Reader for LocalReader {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> i64 {
        self.file.len()
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
        if !self.has_remaining() {
            return Ok(DataSlice::Empty);
        };
        let len = self.chunk_size.min(self.remaining() as usize);
        let chunk = self.file.read_full(None, len)?;
        Ok(DataSlice::bytes(chunk.freeze()))
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.file.seek(pos)?;
        self.pos = pos;
        self.chunk = DataSlice::Empty;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        Ok(())
    }
}
