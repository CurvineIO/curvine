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
use crate::oss_hdfs::check_jindo_status;
use crate::oss_hdfs::ffi::*;
use bytes::BytesMut;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::sys::DataSlice;
use std::os::raw::c_void;
use std::sync::Arc;

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
            unsafe { I64CallbackCtx::complete_userdata(userdata, status, value, err) };
        }

        {
            let userdata = I64CallbackCtx::into_userdata(&self.tell_ctx);
            let start_status =
                unsafe { jindo_writer_tell_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe { I64CallbackCtx::drop_userdata(userdata) };
                check_jindo_status(start_status, "Failed to start tell", None)?;
            }
        }

        let (status, offset, err) = self.tell_ctx.wait().await?;
        if status != JindoStatus::Ok {
            check_jindo_status(status, "Failed to tell", err)?;
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
    pub(crate) write_ctx: Arc<I64CallbackCtx>,
    // Reusable callback context for async tell.
    pub(crate) tell_ctx: Arc<I64CallbackCtx>,
    // Reusable callback context for async operations returning just status (flush/close).
    pub(crate) status_ctx: Arc<StatusCallbackCtx>,
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
            unsafe { I64CallbackCtx::complete_userdata(userdata, status, value, err) };
        }

        let handle = self.writer_handle()?;
        {
            let userdata = I64CallbackCtx::into_userdata(&self.write_ctx);
            let start_status = unsafe {
                jindo_writer_write_async(handle.as_raw(), data_ptr, data_len, Some(cb), userdata)
            };
            if start_status != JindoStatus::Ok {
                unsafe { I64CallbackCtx::drop_userdata(userdata) };
                check_jindo_status(start_status, "Failed to start write", None)?;
            }
        }

        let (status, written, err) = self.write_ctx.wait().await?;
        if status != JindoStatus::Ok {
            check_jindo_status(status, "Failed to write", err)?;
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
            unsafe { StatusCallbackCtx::complete_userdata(userdata, status, err) };
        }

        {
            let userdata = StatusCallbackCtx::into_userdata(&self.status_ctx);
            let start_status =
                unsafe { jindo_writer_flush_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe { StatusCallbackCtx::drop_userdata(userdata) };
                check_jindo_status(start_status, "Failed to start flush", None)?;
            }
        }

        let (status, err) = self.status_ctx.wait().await?;
        if status != JindoStatus::Ok {
            check_jindo_status(status, "Failed to flush", err)?;
        }
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;
        let handle = self.writer_handle.take();

        if let Some(handle) = handle {
            self.status_ctx.reset();
            let userdata = StatusCallbackCtx::into_userdata(&self.status_ctx);
            extern "C" fn cb(
                status: JindoStatus,
                err: *const std::os::raw::c_char,
                userdata: *mut c_void,
            ) {
                unsafe { StatusCallbackCtx::complete_userdata(userdata, status, err) };
            }

            let start_status =
                unsafe { jindo_writer_close_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe { StatusCallbackCtx::drop_userdata(userdata) };
                unsafe {
                    jindo_writer_free(handle.as_raw());
                }
                check_jindo_status(start_status, "Failed to start close writer", None)?;
            }

            let (status, err) = self.status_ctx.wait().await?;
            // Always free handle after close attempt.
            unsafe { jindo_writer_free(handle.as_raw()) };

            if status != JindoStatus::Ok {
                check_jindo_status(status, "Failed to close writer", err)?;
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
