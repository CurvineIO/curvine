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

use crate::error::FsError;
use crate::fs::{Path, Writer};
use crate::state::FileStatus;
use crate::utils::CommonUtils;
use crate::FsResult;
use orpc::common::FileUtils;
use orpc::err_ext;
use orpc::io::LocalFile;
use orpc::sys::DataSlice;
use prost::bytes::BytesMut;

pub struct LocalWriter {
    path: Path,
    file: LocalFile,
    pos: i64,
    status: FileStatus,
    chunk: BytesMut,
    chunk_size: usize,
}

impl LocalWriter {
    pub fn new(path: &Path, chunk_size: usize) -> FsResult<Self> {
        Self::open_write(path, true, chunk_size)
    }

    pub fn open_write(path: &Path, overwrite: bool, chunk_size: usize) -> FsResult<Self> {
        if !overwrite && FileUtils::exists(path.path()) {
            return err_ext!(FsError::file_exists(path.path()));
        }
        let file = LocalFile::with_write(path.path(), overwrite)?;
        let status = CommonUtils::file_to_status(path, &file)?;
        Ok(Self {
            path: path.clone(),
            file,
            pos: 0,
            status,
            chunk_size,
            chunk: BytesMut::with_capacity(chunk_size),
        })
    }

    pub fn open_append(path: &Path, chunk_size: usize) -> FsResult<Self> {
        let file = LocalFile::with_append(path.path())?;
        let status = CommonUtils::file_to_status(path, &file)?;
        Ok(Self {
            path: path.clone(),
            file,
            pos: status.len,
            status,
            chunk_size,
            chunk: BytesMut::with_capacity(chunk_size),
        })
    }
}

impl Writer for LocalWriter {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        let slice = chunk.as_slice();
        self.file.write_all(slice)?;
        Ok(slice.len() as i64)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        self.file.flush()?;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.flush().await
    }

    async fn cancel(&mut self) -> FsResult<()> {
        Ok(())
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.flush_chunk().await?;
        self.file.seek(pos)?;
        self.pos = pos;
        Ok(())
    }
}
