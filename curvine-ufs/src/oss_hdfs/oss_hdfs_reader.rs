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
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::sys::DataSlice;
use std::os::raw::c_void;

use crate::oss_hdfs::callback_ctx::{I64CallbackCtx, StatusCallbackCtx};
use crate::oss_hdfs::ffi::*;

// Extension methods for OSS-HDFS Reader
impl OssHdfsReader {
    /// Get and validate the reader handle.
    fn reader_handle(&self) -> FsResult<JindoReaderHandle> {
        let handle = self
            .reader_handle
            .as_ref()
            .ok_or_else(|| FsError::common("Reader handle is None"))?;

        if handle.is_null() {
            return Err(FsError::common("Reader handle pointer is null"));
        }

        Ok(handle.clone())
    }

    /// Random read at specific offset (does not update read position).
    pub async fn pread(&mut self, offset: i64, n: usize) -> FsResult<bytes::Bytes> {
        if offset < 0 || offset >= self.length {
            return Err(FsError::common("Invalid pread offset"));
        }

        let handle = self.reader_handle()?;

        if n == 0 {
            return Ok(bytes::Bytes::new());
        }

        // Similar to `read_chunk0`: reuse `self.buf` to avoid allocating a new buffer.
        self.buf.clear();
        self.buf.reserve(n);
        // SAFETY: We reserved enough capacity; the FFI will write up to `n` bytes.
        // We truncate to `actual_read` before exposing the bytes.
        unsafe {
            self.buf.set_len(n);
        }
        let mut buffer = self.buf.split_to(n);
        // Per-call ctx.
        let ctx = Box::new(I64CallbackCtx::default());
        ctx.reset();
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
            let userdata = (&*ctx as *const I64CallbackCtx) as *mut c_void;
            let start_status = unsafe {
                jindo_reader_pread_async(
                    handle.as_raw(),
                    offset,
                    n,
                    buffer.as_mut_ptr(),
                    Some(cb),
                    userdata,
                )
            };
            if start_status != JindoStatus::Ok {
                buffer.clear();
                let err_msg = jindo_last_error();
                return Err(FsError::common(format!(
                    "Failed to start pread: {}",
                    err_msg
                )));
            }
        }

        let (status, actual_read, err) = ctx.wait().await?;
        if status != JindoStatus::Ok {
            buffer.clear();
            let err_msg = err.unwrap_or_else(jindo_last_error);
            return Err(FsError::common(format!("Failed to pread: {}", err_msg)));
        }

        let actual_read = usize::try_from(actual_read.max(0)).unwrap_or(0);
        buffer.truncate(actual_read);
        Ok(buffer.freeze())
    }

    /// Get current read position
    pub async fn tell(&self) -> FsResult<i64> {
        let handle = self.reader_handle()?;

        let ctx = Box::new(I64CallbackCtx::default());
        ctx.reset();
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
            let userdata = (&*ctx as *const I64CallbackCtx) as *mut c_void;
            let start_status =
                unsafe { jindo_reader_tell_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                let err_msg = jindo_last_error();
                return Err(FsError::common(format!(
                    "Failed to start tell: {}",
                    err_msg
                )));
            }
        }

        let (status, offset, err) = ctx.wait().await?;
        if status != JindoStatus::Ok {
            let err_msg = err.unwrap_or_else(jindo_last_error);
            return Err(FsError::common(format!("Failed to tell: {}", err_msg)));
        }
        Ok(offset)
    }

    /// Get file length
    /// If FFI call fails (e.g., seek to end fails for newly written files),
    /// returns the cached length from when the reader was opened
    pub async fn get_file_length(&self) -> FsResult<i64> {
        let handle = self.reader_handle()?;

        let ctx = Box::new(I64CallbackCtx::default());
        ctx.reset();
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
            let userdata = (&*ctx as *const I64CallbackCtx) as *mut c_void;
            let start_status =
                unsafe { jindo_reader_get_file_length_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                return Ok(self.length);
            }
        }

        let (status, length, _err) = match ctx.wait().await {
            Ok(v) => v,
            Err(_) => return Ok(self.length),
        };
        if status != JindoStatus::Ok {
            return Ok(self.length);
        }
        Ok(length)
    }
}

/// OSS-HDFS Reader implementation using JindoSDK C++ library via FFI
pub struct OssHdfsReader {
    pub(crate) reader_handle: Option<JindoReaderHandle>,
    pub(crate) path: Path,
    pub(crate) length: i64,
    pub(crate) pos: i64,
    pub(crate) chunk_size: usize,
    pub(crate) status: FileStatus,
    pub(crate) chunk: DataSlice,
    /// Scratch buffer used by `read_chunk0()` to prepare writable memory for the FFI.
    ///
    /// NOTE: This is separate from `chunk` to avoid swapping `self.chunk` just to obtain a
    /// writable buffer. It also follows the common pattern:
    /// `reserve` -> `set_len` -> hand pointer to FFI -> `truncate`.
    pub(crate) buf: BytesMut,
    // Reusable callback contexts for &mut self operations.
    pub(crate) read_ctx: I64CallbackCtx,
    pub(crate) status_ctx: StatusCallbackCtx,
}

impl Reader for OssHdfsReader {
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
        // Return a reference to the actual chunk buffer
        // This buffer is used by the Reader trait's default implementations
        // (read_chunk, etc.) but we override read_chunk0 to read directly from JindoSDK
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
        // If file is empty or we've reached the end, return an empty buffer (keeps capacity reusable).
        if self.length == 0 || self.pos >= self.length {
            return Ok(DataSlice::empty());
        }

        // Only read up to remaining bytes.
        let remaining = (self.length - self.pos).max(0) as usize;
        let want = remaining.min(self.chunk_size());

        // Obtain a writable buffer from `self.buf` without touching `self.chunk`.
        self.buf.clear();
        self.buf.reserve(want);
        // SAFETY: We reserved enough capacity and will truncate to `actual_read` before exposing.
        // On error paths we clear the buffer before returning it.
        unsafe {
            self.buf.set_len(want);
        }
        let mut buffer = self.buf.split_to(want);

        {
            let handle = self.reader_handle()?;
            self.read_ctx.reset();
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
                let userdata = (&self.read_ctx as *const I64CallbackCtx) as *mut c_void;
                let start_status = unsafe {
                    jindo_reader_read_async(
                        handle.as_raw(),
                        want,
                        buffer.as_mut_ptr(),
                        Some(cb),
                        userdata,
                    )
                };
                if start_status != JindoStatus::Ok {
                    buffer.clear();
                    let err_msg = jindo_last_error();
                    return Err(FsError::common(format!(
                        "Failed to start read: {}",
                        err_msg
                    )));
                }
            }

            let (status, actual_read, err) = self.read_ctx.wait().await?;
            if status != JindoStatus::Ok {
                if self.length == 0 || self.pos >= self.length {
                    buffer.clear();
                    return Ok(DataSlice::Buffer(buffer));
                }
                let err_msg = err.unwrap_or_else(jindo_last_error);
                return Err(FsError::common(format!("Failed to read: {}", err_msg)));
            }

            let actual_read = usize::try_from(actual_read.max(0)).unwrap_or(0);
            if actual_read == 0 {
                buffer.clear();
                return Ok(DataSlice::Buffer(buffer));
            }
            buffer.truncate(actual_read);
        } // handle is dropped here, releasing the borrow

        // IMPORTANT:
        // Do NOT update `self.pos` here.
        //
        // The shared `Reader` trait implementation (see `curvine-common/src/fs/reader.rs`) updates
        // `pos` based on the length of the returned chunk. For example:
        // - `read()` calls `read_chunk()` and then updates `pos` by `chunk.len()` (line 78)
        // - `async_read()` calls `read_chunk()` and then updates `pos` by `chunk.len()` (line 93)
        //
        // If we update `pos` here in `read_chunk0()`, it will be double-counted (once here,
        // once by the caller), causing `pos` to advance too far. This leads to incorrect read
        // positions and breaks `seek()` behavior.

        Ok(DataSlice::Buffer(buffer))
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 || pos > self.length {
            return Err(FsError::common("Invalid seek position"));
        }

        // For empty files (length == 0), only pos 0 is valid and no FFI call is needed
        if self.length == 0 {
            if pos == 0 {
                self.pos = pos;
                return Ok(());
            } else {
                return Err(FsError::common("Invalid seek position for empty file"));
            }
        }

        {
            let handle = self.reader_handle()?;
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
                    unsafe { jindo_reader_seek_async(handle.as_raw(), pos, Some(cb), userdata) };
                if start_status != JindoStatus::Ok {
                    let err_msg = jindo_last_error();
                    return Err(FsError::common(format!(
                        "Failed to start seek: {}",
                        err_msg
                    )));
                }
            }

            let (status, err) = self.status_ctx.wait().await?;
            if status != JindoStatus::Ok {
                let err_msg = err.unwrap_or_else(jindo_last_error);
                return Err(FsError::common(format!("Failed to seek: {}", err_msg)));
            }
        } // handle is dropped here, releasing the borrow

        // Clear any buffered chunk data; otherwise, a backward seek could still read from the
        // previous forward-read buffer (which would return wrong data).
        self.chunk = DataSlice::empty();
        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        let handle = self.reader_handle.take();

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
                unsafe { jindo_reader_close_async(handle.as_raw(), Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    jindo_reader_free(handle.as_raw());
                }
                let err_msg = jindo_last_error();
                return Err(FsError::common(format!(
                    "Failed to start close reader: {}",
                    err_msg
                )));
            }

            let (status, err) = self.status_ctx.wait().await?;
            unsafe { jindo_reader_free(handle.as_raw()) };

            if status != JindoStatus::Ok {
                let err_msg = err.unwrap_or_else(jindo_last_error);
                return Err(FsError::common(format!(
                    "Failed to close reader: {}",
                    err_msg
                )));
            }
        }
        Ok(())
    }
}

impl Drop for OssHdfsReader {
    fn drop(&mut self) {
        // Only free if handle hasn't been taken by complete()
        if let Some(handle) = self.reader_handle.take() {
            unsafe {
                jindo_reader_free(handle.as_raw());
            }
        }
    }
}
