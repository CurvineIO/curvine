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

use crate::cosn::ffi::{cosn_error, CosnFile};
use bytes::BytesMut;
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, Writer};
use curvine_io::DataSlice;
use curvine_model::FileStatus;
use hdfs_sys::{hdfsFlush, hdfsHFlush, hdfsWrite};
use std::os::raw::c_void;

const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct CosnWriter {
    file: Option<CosnFile>,
    path: Path,
    status: FileStatus,
    pos: i64,
    chunk: BytesMut,
    chunk_size: usize,
}

impl CosnWriter {
    pub(crate) fn new(file: CosnFile, path: Path, status: FileStatus) -> Self {
        Self {
            file: Some(file),
            path,
            status,
            pos: 0,
            chunk: BytesMut::with_capacity(DEFAULT_CHUNK_SIZE),
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
            .ok_or_else(|| FsError::common("COSN writer handle is missing"))?;
        let result = tokio::task::spawn_blocking(move || {
            let out = f(&file);
            (file, out)
        })
        .await
        .map_err(|e| FsError::common(format!("COSN write blocking task failed: {}", e)))?;
        self.file = Some(result.0);
        result.1
    }
}

impl Writer for CosnWriter {
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
        let data = chunk.to_bytes();
        if data.is_empty() {
            return Ok(0);
        }
        let path = self.path.full_path().to_owned();
        let written = self
            .with_file_blocking(move |file| {
                let mut offset = 0usize;
                while offset < data.len() {
                    let n = unsafe {
                        hdfsWrite(
                            file.fs_raw(),
                            file.as_raw(),
                            data[offset..].as_ptr() as *const c_void,
                            (data.len() - offset) as i32,
                        )
                    };
                    if n < 0 {
                        return Err(cosn_error("write", &path));
                    }
                    if n == 0 {
                        return Err(FsError::common(format!(
                            "write returned 0 bytes on {}",
                            path
                        )));
                    }
                    offset += n as usize;
                }
                Ok(data.len() as i64)
            })
            .await?;
        Ok(written)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        let path = self.path.full_path().to_owned();
        self.with_file_blocking(move |file| {
            let rc = unsafe { hdfsHFlush(file.fs_raw(), file.as_raw()) };
            if rc != 0 {
                // Fall back to hdfsFlush if HFlush is unsupported.
                let rc2 = unsafe { hdfsFlush(file.fs_raw(), file.as_raw()) };
                if rc2 != 0 {
                    return Err(cosn_error("flush", &path));
                }
            }
            Ok(())
        })
        .await
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;
        // Dropping CosnFile closes the stream and finalizes the object.
        self.file.take();
        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        self.file.take();
        Ok(())
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if self.pos != pos {
            err_box!("not support random write")
        } else {
            Ok(())
        }
    }
}
