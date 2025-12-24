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

use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader};
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

// Extension methods for OSS-HDFS Reader
impl OssHdfsReader {
    /// Get and validate the reader handle.
    ///
    /// IMPORTANT: returns a copied handle (does not hold a MutexGuard across `.await`).
    fn reader_handle(&self) -> FsResult<JindoReaderHandle> {
        let handle_opt = self
            .reader_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock reader handle: {}", e)))?;

        let handle = handle_opt
            .as_ref()
            .ok_or_else(|| FsError::common("Reader handle is None"))?;

        if handle.is_null() {
            return Err(FsError::common("Reader handle pointer is null"));
        }

        Ok(*handle)
    }

    /// Random read at specific offset (does not update read position)
    /// Suitable for high-concurrency random reads, e.g., parquet files
    pub async fn pread(&self, offset: i64, n: usize) -> FsResult<bytes::Bytes> {
        if offset < 0 || offset >= self.length {
            return Err(FsError::common("Invalid pread offset"));
        }

        let handle = self.reader_handle()?;

        let mut buffer = vec![0u8; n];
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
            let start_status = unsafe {
                jindo_reader_pread_async(handle.0, offset, n, buffer.as_mut_ptr(), Some(cb), userdata)
            };
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
                return Err(FsError::common(format!("Failed to start pread: {}", err_msg)));
            }
        }

        let (status, actual_read, err) = rx
            .await
            .map_err(|_| FsError::common("Async pread callback dropped"))?;
        if status != JindoStatus::Ok {
            let err_msg = err.unwrap_or_else(|| unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                }
            });
            return Err(FsError::common(format!("Failed to pread: {}", err_msg)));
        }

        let actual_read = usize::try_from(actual_read.max(0)).unwrap_or(0);
        buffer.truncate(actual_read);
        Ok(bytes::Bytes::from(buffer))
    }

    /// Get current read position
    pub async fn tell(&self) -> FsResult<i64> {
        let handle = self.reader_handle()?;

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
            let start_status = unsafe { jindo_reader_tell_async(handle.0, Some(cb), userdata) };
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
                return Err(FsError::common(format!("Failed to start tell: {}", err_msg)));
            }
        }

        let (status, offset, err) = rx
            .await
            .map_err(|_| FsError::common("Async tell callback dropped"))?;
        if status != JindoStatus::Ok {
            let err_msg = err.unwrap_or_else(|| unsafe {
                let err_ptr = jindo_get_last_error();
                if err_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                }
            });
            return Err(FsError::common(format!("Failed to tell: {}", err_msg)));
        }
        Ok(offset)
    }

    /// Get file length
    /// If FFI call fails (e.g., seek to end fails for newly written files),
    /// returns the cached length from when the reader was opened
    pub async fn get_file_length(&self) -> FsResult<i64> {
        let handle = self.reader_handle()?;

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
            let start_status = unsafe { jindo_reader_get_file_length_async(handle.0, Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, i64, Option<String>)>,
                    ))
                };
                return Ok(self.length);
            }
        }

        let (status, length, _err) = match rx.await {
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
    pub(crate) reader_handle: Arc<Mutex<Option<JindoReaderHandle>>>,
    pub(crate) path: Path,
    pub(crate) length: i64,
    pub(crate) pos: i64,
    pub(crate) chunk_size: usize,
    pub(crate) status: FileStatus,
    pub(crate) chunk: DataSlice,
}

// Safety: JindoReaderHandle is a *mut c_void pointer from FFI.
// The handle is protected by Mutex, and all FFI calls are thread-safe.
// The underlying C++ library handles thread safety internally.
unsafe impl Send for OssHdfsReader {}
unsafe impl Sync for OssHdfsReader {}

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
        // If file is empty or we've reached the end, return Empty
        if self.length == 0 || self.pos >= self.length {
            return Ok(DataSlice::Empty);
        }

        let chunk_size = self.chunk_size();
        let mut buffer = vec![0u8; chunk_size];

        {
            let handle = self.reader_handle()?;
            let (tx, rx) = oneshot::channel::<(JindoStatus, i64, Option<String>)>();
            extern "C" fn cb(
                status: JindoStatus,
                value: i64,
                err: *const std::os::raw::c_char,
                userdata: *mut c_void,
            ) {
                let tx = unsafe {
                    Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, i64, Option<String>)>)
                };
                let _ = tx.send((status, value, err_from_c(err)));
            }

            {
                let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
                let start_status = unsafe {
                    jindo_reader_read_async(handle.0, chunk_size, buffer.as_mut_ptr(), Some(cb), userdata)
                };
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
                    return Err(FsError::common(format!("Failed to start read: {}", err_msg)));
                }
            }

            let (status, actual_read, err) = rx
                .await
                .map_err(|_| FsError::common("Async read callback dropped"))?;
            if status != JindoStatus::Ok {
                if self.length == 0 || self.pos >= self.length {
                    return Ok(DataSlice::Empty);
                }
                let err_msg = err.unwrap_or_else(|| unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                });
                return Err(FsError::common(format!("Failed to read: {}", err_msg)));
            }

            let actual_read = usize::try_from(actual_read.max(0)).unwrap_or(0);
            if actual_read == 0 {
                return Ok(DataSlice::Empty);
            }
            buffer.truncate(actual_read);
        } // handle is dropped here, releasing the borrow

        // IMPORTANT:
        // Do NOT update `self.pos` here.
        //
        // The shared `Reader` trait implementation advances `pos` based on how many bytes are
        // actually consumed from the returned chunk (see `curvine-common/src/fs/reader.rs`).
        // If we update `pos` here, it will be double-counted, and `seek()` will appear to work
        // while reads still come from a stale buffer / wrong offsets.

        Ok(DataSlice::Bytes(bytes::Bytes::from(buffer)))
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
            let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();
            extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
                let tx =
                    unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
                let _ = tx.send((status, err_from_c(err)));
            }

            {
                let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
                let start_status = unsafe { jindo_reader_seek_async(handle.0, pos, Some(cb), userdata) };
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
                    return Err(FsError::common(format!("Failed to start seek: {}", err_msg)));
                }
            }

            let (status, err) = rx
                .await
                .map_err(|_| FsError::common("Async seek callback dropped"))?;
            if status != JindoStatus::Ok {
                let err_msg = err.unwrap_or_else(|| unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                });
                return Err(FsError::common(format!("Failed to seek: {}", err_msg)));
            }
        } // handle is dropped here, releasing the borrow

        // Clear any buffered chunk data; otherwise, a backward seek could still read from the
        // previous forward-read buffer (which would return wrong data).
        *self.chunk_mut() = DataSlice::empty();
        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        let handle = {
            let mut handle_opt = self
                .reader_handle
                .lock()
                .map_err(|e| FsError::common(format!("Failed to lock reader handle: {}", e)))?;
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

            let start_status = unsafe { jindo_reader_close_async(handle.0, Some(cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>));
                    jindo_reader_free(handle.0);
                }
                let err_msg = unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                };
                return Err(FsError::common(format!("Failed to start close reader: {}", err_msg)));
            }

            let (status, err) = rx
                .await
                .map_err(|_| FsError::common("Async close callback dropped"))?;
            unsafe { jindo_reader_free(handle.0) };

            if status != JindoStatus::Ok {
                let err_msg = err.unwrap_or_else(|| unsafe {
                    let err_ptr = jindo_get_last_error();
                    if err_ptr.is_null() {
                        "Unknown error".to_string()
                    } else {
                        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
                    }
                });
                return Err(FsError::common(format!("Failed to close reader: {}", err_msg)));
            }
        }
        Ok(())
    }
}

impl Drop for OssHdfsReader {
    fn drop(&mut self) {
        // Only free if handle hasn't been taken by complete()
        let mut handle_opt = self.reader_handle.lock().ok();
        if let Some(ref mut handle_opt) = handle_opt {
            if let Some(handle) = handle_opt.take() {
                unsafe {
                    jindo_reader_free(handle.0);
                }
            }
        }
    }
}
