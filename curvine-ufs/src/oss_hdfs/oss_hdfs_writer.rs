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

use crate::oss_hdfs::callback_ctx::{I64CallbackCtx, StatusCallbackCtx};
use crate::oss_hdfs::ffi::*;
use bytes::BytesMut;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::sys::DataSlice;
use std::os::raw::c_void;

// Extension methods for OSS-HDFS Writer
impl OssHdfsWriter {
    /// Get and validate the writer handle.
    fn writer_handle(&self) -> FsResult<JindoWriterHandle> {
        let handle = self
            .writer_handle
            .as_ref()
            .ok_or_else(|| FsError::common("Writer handle is null"))?;

        if handle.is_null() {
            return Err(FsError::common("Writer handle pointer is null"));
        }

        Ok(handle.clone())
    }

    /// Get current write position
    pub async fn tell(&self) -> FsResult<i64> {
        let handle = self.writer_handle()?;

        self.tell_ctx.reset();
        extern "C" fn cb(
            status: JindoStatus,
            value: i64,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let ctx = unsafe { &*(userdata as *const I64CallbackCtx) };
            ctx.complete(status, value, err);
        }

        {
            let userdata = (&self.tell_ctx as *const I64CallbackCtx) as *mut c_void;
            let start_status =
                unsafe { jindo_writer_tell_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                return Err(FsError::common(format!(
                    "Failed to start tell: {}",
                    jindo_last_error()
                )));
            }
        }

        let (status, offset, err) = self.tell_ctx.wait().await?;
        if status != JindoStatus::Ok {
            let msg = err.unwrap_or_else(jindo_last_error);
            return Err(FsError::common(format!("Failed to tell: {}", msg)));
        }
        Ok(offset)
    }
}

/// OSS-HDFS Writer implementation using JindoSDK C++ library via FFI
pub struct OssHdfsWriter {
    pub(crate) writer_handle: Option<JindoWriterHandle>,
    pub(crate) path: Path,
    pub(crate) status: FileStatus,
    pub(crate) pos: i64,
    pub(crate) chunk_size: usize,
    pub(crate) chunk: BytesMut,
    // Reusable callback context for async write.
    pub(crate) write_ctx: I64CallbackCtx,
    // Reusable callback context for async tell.
    pub(crate) tell_ctx: I64CallbackCtx,
    // Reusable callback context for async operations returning just status (flush/close).
    pub(crate) status_ctx: StatusCallbackCtx,
}

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
        // Return a reference to the actual chunk buffer
        // This buffer is used by the Writer trait's default implementations
        // (flush_chunk, write, etc.) but we override write_chunk to write directly to JindoSDK
        &mut self.chunk
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

        // Ensure data is valid and get pointers before FFI call
        // This ensures data remains valid during the FFI call
        let data_ptr = data.as_ptr();
        let data_len = data.len();

        if data_ptr.is_null() || data_len == 0 {
            return Err(FsError::common("Invalid data pointer or length"));
        }

        // Keep `data` alive across the await (pointer must remain valid).
        self.write_ctx.reset();
        extern "C" fn cb(
            status: JindoStatus,
            value: i64,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            // `userdata` points to a long-lived `I64CallbackCtx` owned by `OssHdfsWriter`.
            let ctx = unsafe { &*(userdata as *const I64CallbackCtx) };
            ctx.complete(status, value, err);
        }

        let handle = self.writer_handle()?;
        {
            // IMPORTANT: keep raw pointer userdata scoped to this block so this Future remains Send.
            let userdata = (&self.write_ctx as *const I64CallbackCtx) as *mut c_void;
            let start_status = unsafe {
                jindo_writer_write_async(handle.as_raw(), data_ptr, data_len, Some(cb), userdata)
            };
            if start_status != JindoStatus::Ok {
                let err_msg = jindo_last_error();
                return Err(FsError::common(format!(
                    "Failed to start write: {}",
                    err_msg
                )));
            }
        }

        let (status, written, err) = self.write_ctx.wait().await?;
        if status != JindoStatus::Ok {
            let err_msg = err.unwrap_or_else(jindo_last_error);
            return Err(FsError::common(format!("Failed to write: {}", err_msg)));
        }
        if written != len {
            return Err(FsError::common(format!(
                "Short write: expected {}, got {}",
                len, written
            )));
        }

        self.pos += len;
        Ok(len)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        let handle = self.writer_handle()?;
        self.status_ctx.reset();
        extern "C" fn cb(
            status: JindoStatus,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let ctx = unsafe { &*(userdata as *const StatusCallbackCtx) };
            ctx.complete(status, err);
        }

        {
            let userdata = (&self.status_ctx as *const StatusCallbackCtx) as *mut c_void;
            let start_status =
                unsafe { jindo_writer_flush_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                let err_msg = jindo_last_error();
                return Err(FsError::common(format!(
                    "Failed to start flush: {}",
                    err_msg
                )));
            }
        }

        let (status, err) = self.status_ctx.wait().await?;
        if status != JindoStatus::Ok {
            let err_msg = err.unwrap_or_else(jindo_last_error);
            return Err(FsError::common(format!("Failed to flush: {}", err_msg)));
        }
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;
        let handle = self.writer_handle.take();

        if let Some(handle) = handle {
            self.status_ctx.reset();
            let userdata = (&self.status_ctx as *const StatusCallbackCtx) as *mut c_void;
            extern "C" fn cb(
                status: JindoStatus,
                err: *const std::os::raw::c_char,
                userdata: *mut c_void,
            ) {
                let ctx = unsafe { &*(userdata as *const StatusCallbackCtx) };
                ctx.complete(status, err);
            }

            let start_status =
                unsafe { jindo_writer_close_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    jindo_writer_free(handle.as_raw());
                }
                let err_msg = jindo_last_error();
                return Err(FsError::common(format!(
                    "Failed to start close writer: {}",
                    err_msg
                )));
            }

            let (status, err) = self.status_ctx.wait().await?;
            // Always free handle after close attempt.
            unsafe { jindo_writer_free(handle.as_raw()) };

            if status != JindoStatus::Ok {
                let err_msg = err.unwrap_or_else(jindo_last_error);
                return Err(FsError::common(format!(
                    "Failed to close writer: {}",
                    err_msg
                )));
            }
        }
        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        // JindoSDK doesn't have explicit cancel, but we can free the handle
        // Take the handle and set it to None to prevent Drop from freeing it again
        if let Some(handle) = self.writer_handle.take() {
            unsafe {
                jindo_writer_free(handle.as_raw());
            }
        }

        Ok(())
    }
}

impl Drop for OssHdfsWriter {
    fn drop(&mut self) {
        // Only free if handle hasn't been taken by cancel() or complete()
        if let Some(handle) = self.writer_handle.take() {
            unsafe {
                jindo_writer_free(handle.as_raw());
            }
        }
    }
}
