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

use crate::cosn::ffi::CosnFile;
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, Reader};
use curvine_io::DataSlice;
use curvine_model::FileStatus;

const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct CosnReader {
    /// Taken out around `spawn_blocking` so the raw handle can cross the thread
    /// boundary; restored before returning to the caller.
    file: Option<CosnFile>,
    path: Path,
    status: FileStatus,
    length: i64,
    pos: i64,
    chunk: DataSlice,
    chunk_size: usize,
}

impl CosnReader {
    pub(crate) fn new(file: CosnFile, path: Path, status: FileStatus) -> Self {
        let length = status.len;
        Self {
            file: Some(file),
            path,
            status,
            length,
            pos: 0,
            chunk: DataSlice::Empty,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    async fn with_file_blocking<T, F>(&mut self, f: F) -> FsResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&CosnFile) -> FsResult<T> + Send + 'static,
    {
        let file = self
            .file
            .take()
            .ok_or_else(|| FsError::common("COSN reader handle is missing"))?;
        let result = tokio::task::spawn_blocking(move || {
            let out = f(&file);
            (file, out)
        })
        .await
        .map_err(|e| FsError::common(format!("COSN read blocking task failed: {}", e)))?;
        self.file = Some(result.0);
        result.1
    }
}

impl Reader for CosnReader {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> i64 {
        self.length
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
        }

        let to_read = (self.remaining() as usize).min(self.chunk_size);
        let offset = self.pos;
        let path = self.path.full_path().to_owned();

        let bytes = self
            .with_file_blocking(move |file| {
                file.pread_loop(offset, to_read).map_err(|e| {
                    // Preserve the original error text; attach the path for context.
                    FsError::common(format!("{} ({})", e, path))
                })
            })
            .await?;

        if bytes.is_empty() {
            Ok(DataSlice::Empty)
        } else {
            Ok(DataSlice::Bytes(bytes::Bytes::from(bytes)))
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 || pos > self.length {
            return err_box!("Invalid seek position");
        }
        if pos == self.pos {
            return Ok(());
        }

        let to_skip = pos - self.pos;
        if to_skip >= 0 && to_skip <= self.chunk.len() as i64 {
            self.chunk.advance(to_skip as usize);
        } else {
            self.chunk.clear();
        }
        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.chunk = DataSlice::Empty;
        // Dropping CosnFile closes the stream via Drop.
        self.file.take();
        Ok(())
    }
}
