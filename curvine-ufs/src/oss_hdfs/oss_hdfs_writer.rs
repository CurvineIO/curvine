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

use bytes::BytesMut;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::sys::DataSlice;
use std::sync::{Arc, Mutex};

use crate::oss_hdfs::ffi::*;

// Extension methods for OSS-HDFS Writer
impl OssHdfsWriter {
    /// Get and validate the writer handle
    fn get_writer_handle(&self) -> FsResult<std::sync::MutexGuard<'_, Option<JindoWriterHandle>>> {
        let handle_opt = self
            .writer_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock writer handle: {}", e)))?;

        if handle_opt.is_none() {
            return Err(FsError::common("Writer handle is null"));
        }

        Ok(handle_opt)
    }

    /// Get current write position
    pub async fn tell(&self) -> FsResult<i64> {
        let handle_opt = self.get_writer_handle()?;
        let handle = handle_opt.as_ref().unwrap();

        let mut offset = 0i64;
        let status = unsafe { jindo_writer_tell(handle.0, &mut offset) };

        if status != JindoStatus::Ok {
            let err_msg = unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    std::ffi::CStr::from_ptr(err_ptr)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            return Err(FsError::common(format!("Failed to tell: {}", err_msg)));
        }

        Ok(offset)
    }
}

/// OSS-HDFS Writer implementation using JindoSDK C++ library via FFI
pub struct OssHdfsWriter {
    pub(crate) writer_handle: Arc<Mutex<Option<JindoWriterHandle>>>,
    pub(crate) path: Path,
    pub(crate) status: FileStatus,
    pub(crate) pos: i64,
    pub(crate) chunk_size: usize,
}

// Safety: JindoWriterHandle is a *mut c_void pointer from FFI.
// The handle is protected by Mutex, and all FFI calls are thread-safe.
// The underlying C++ library handles thread safety internally.
unsafe impl Send for OssHdfsWriter {}
unsafe impl Sync for OssHdfsWriter {}

impl Writer for OssHdfsWriter {
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
        // This is not used in our implementation
        // We write directly to JindoSDK in write_chunk
        // Return a mutable reference to a static empty buffer
        // Note: This is safe because we don't actually use the returned value
        // The actual writing is done in write_chunk
        use std::cell::RefCell;
        thread_local! {
            static EMPTY: RefCell<BytesMut> = RefCell::new(BytesMut::new());
        }
        EMPTY.with(|e| unsafe { &mut *e.as_ptr() })
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        let data = match chunk {
            DataSlice::Empty => return Ok(0),
            DataSlice::Bytes(bytes) => bytes,
            DataSlice::Buffer(buf) => buf.freeze(),
            DataSlice::IOSlice(_) | DataSlice::MemSlice(_) => {
                let slice = chunk.as_slice();
                bytes::Bytes::copy_from_slice(slice)
            }
        };

        let len = data.len() as i64;

        // Use a local scope to ensure MutexGuard is dropped before modifying self.pos
        let status = {
            let handle_opt = self.get_writer_handle()?;
            let handle = handle_opt.as_ref().unwrap();
            unsafe { jindo_writer_write(handle.0, data.as_ptr(), data.len()) }
        };

        if status != JindoStatus::Ok {
            let err_msg = unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    std::ffi::CStr::from_ptr(err_ptr)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            return Err(FsError::common(format!("Failed to write: {}", err_msg)));
        }

        self.pos += len;
        Ok(len)
    }

    async fn flush(&mut self) -> FsResult<()> {
        let handle_opt = self.get_writer_handle()?;
        let handle = handle_opt.as_ref().unwrap();

        let status = unsafe { jindo_writer_flush(handle.0) };

        if status != JindoStatus::Ok {
            let err_msg = unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    std::ffi::CStr::from_ptr(err_ptr)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            return Err(FsError::common(format!("Failed to flush: {}", err_msg)));
        }

        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        let mut handle_opt = self
            .writer_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock writer handle: {}", e)))?;

        if let Some(handle) = handle_opt.take() {
            let status = unsafe { jindo_writer_close(handle.0) };

            if status != JindoStatus::Ok {
                let err_msg = unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        std::ffi::CStr::from_ptr(err_ptr)
                            .to_string_lossy()
                            .into_owned()
                    }
                };
                // Still free the handle even if close failed
                unsafe { jindo_writer_free(handle.0) };
                return Err(FsError::common(format!(
                    "Failed to close writer: {}",
                    err_msg
                )));
            }
            
            // Free the handle after successful close
            unsafe { jindo_writer_free(handle.0) };
        }

        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        // JindoSDK doesn't have explicit cancel, but we can free the handle
        // Take the handle and set it to None to prevent Drop from freeing it again
        let mut handle_opt = self
            .writer_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock writer handle: {}", e)))?;

        if let Some(handle) = handle_opt.take() {
            unsafe {
                jindo_writer_free(handle.0);
            }
        }

        Ok(())
    }
}

impl Drop for OssHdfsWriter {
    fn drop(&mut self) {
        // Only free if handle hasn't been taken by cancel() or complete()
        let mut handle_opt = self.writer_handle.lock().ok();
        if let Some(ref mut handle_opt) = handle_opt {
            if let Some(handle) = handle_opt.take() {
                unsafe {
                    jindo_writer_free(handle.0);
                }
            }
        }
    }
}
