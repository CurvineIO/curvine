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
use std::ffi::CStr;
use std::os::raw::c_void;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::oss::ffi::*;

fn err_from_c(err: *const std::os::raw::c_char) -> Option<String> {
    if err.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(err) }.to_string_lossy().into_owned())
    }
}

// Extension methods for OSS-HDFS Writer
impl OssHdfsWriter {
    /// Get and validate the writer handle.
    ///
    /// IMPORTANT: returns a copied handle (does not hold a MutexGuard across `.await`).
    fn writer_handle(&self) -> FsResult<JindoWriterHandle> {
        let handle_opt = self
            .writer_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock writer handle: {}", e)))?;

        let handle = handle_opt
            .as_ref()
            .ok_or_else(|| FsError::common("Writer handle is null"))?;

        if handle.is_null() {
            return Err(FsError::common("Writer handle pointer is null"));
        }

        Ok(*handle)
    }

    /// Get current write position
    pub async fn tell(&self) -> FsResult<i64> {
        let handle = self.writer_handle()?;

        let (tx, rx) = oneshot::channel::<(JindoStatus, i64, Option<String>)>();
        extern "C" fn cb(
            status: JindoStatus,
            value: i64,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx =
                unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, i64, Option<String>)>) };
            let _ = tx.send((status, value, err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = unsafe { jindo_writer_tell_async(handle.0, Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, i64, Option<String>)>,
                    ))
                };
                return Err(FsError::common(format!(
                    "Failed to start tell: {}",
                    unsafe {
                        let err_ptr = jindo_get_last_error();
                        if err_ptr.is_null() {
                            "Unknown error".to_string()
                        } else {
                            CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                        }
                    }
                )));
            }
        }

        let (status, offset, err) = rx
            .await
            .map_err(|_| FsError::common("Async tell callback dropped"))?;
        if status != JindoStatus::Ok {
            let msg = err.unwrap_or_else(|| unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                }
            });
            return Err(FsError::common(format!("Failed to tell: {}", msg)));
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
    pub(crate) chunk: BytesMut,
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
        // Return a reference to the actual chunk buffer
        // This buffer is used by the Writer trait's default implementations
        // (flush_chunk, write, etc.) but we override write_chunk to write directly to JindoSDK
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        log::info!("write_chunk: {:?}", String::from_utf8_lossy(chunk.as_slice()));
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
        let (tx, rx) = oneshot::channel::<(JindoStatus, i64, Option<String>)>();
        extern "C" fn cb(
            status: JindoStatus,
            value: i64,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx =
                unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, i64, Option<String>)>) };
            let _ = tx.send((status, value, err_from_c(err)));
        }

        let handle = self.writer_handle()?;
        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status =
                unsafe { jindo_writer_write_async(handle.0, data_ptr, data_len, Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, i64, Option<String>)>,
                    ))
                };
                let err_msg = unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                };
                return Err(FsError::common(format!("Failed to start write: {}", err_msg)));
            }
        }

        let (status, written, err) = rx
            .await
            .map_err(|_| FsError::common("Async write callback dropped"))?;
        if status != JindoStatus::Ok {
            let err_msg = err.unwrap_or_else(|| unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                }
            });
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
        let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();
        extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
            let tx =
                unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
            let _ = tx.send((status, err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = unsafe { jindo_writer_flush_async(handle.0, Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>,
                    ))
                };
                let err_msg = unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                };
                return Err(FsError::common(format!("Failed to start flush: {}", err_msg)));
            }
        }

        let (status, err) = rx
            .await
            .map_err(|_| FsError::common("Async flush callback dropped"))?;
        if status != JindoStatus::Ok {
            let err_msg = err.unwrap_or_else(|| unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                }
            });
            return Err(FsError::common(format!("Failed to flush: {}", err_msg)));
        }
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;
        let handle = {
            let mut handle_opt = self
                .writer_handle
                .lock()
                .map_err(|e| FsError::common(format!("Failed to lock writer handle: {}", e)))?;
            handle_opt.take()
        };

        if let Some(handle) = handle {
            let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
                let tx =
                    unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
                let _ = tx.send((status, err_from_c(err)));
            }

            let start_status = unsafe { jindo_writer_close_async(handle.0, Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>));
                    jindo_writer_free(handle.0);
                }
                let err_msg = unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                };
                return Err(FsError::common(format!("Failed to start close writer: {}", err_msg)));
            }

            let (status, err) = rx
                .await
                .map_err(|_| FsError::common("Async close callback dropped"))?;
            // Always free handle after close attempt.
            unsafe { jindo_writer_free(handle.0) };

            if status != JindoStatus::Ok {
                let err_msg = err.unwrap_or_else(|| unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                });
                return Err(FsError::common(format!("Failed to close writer: {}", err_msg)));
            }
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
