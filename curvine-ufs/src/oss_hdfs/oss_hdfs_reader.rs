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
use std::sync::{Arc, Mutex};

use crate::oss_hdfs::ffi::*;

// Extension methods for OSS-HDFS Reader
impl OssHdfsReader {
    /// Get and validate the reader handle
    fn get_reader_handle(&self) -> FsResult<std::sync::MutexGuard<'_, JindoReaderHandle>> {
        let handle = self
            .reader_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock reader handle: {}", e)))?;

        if handle.is_null() {
            return Err(FsError::common("Reader handle is null"));
        }

        Ok(handle)
    }

    /// Random read at specific offset (does not update read position)
    /// Suitable for high-concurrency random reads, e.g., parquet files
    pub async fn pread(&self, offset: i64, n: usize) -> FsResult<bytes::Bytes> {
        if offset < 0 || offset >= self.length {
            return Err(FsError::common("Invalid pread offset"));
        }

        let handle = self.get_reader_handle()?;

        let mut buffer = vec![0u8; n];
        let mut actual_read = 0usize;

        let status = unsafe {
            jindo_reader_pread(handle.0, offset, n, buffer.as_mut_ptr(), &mut actual_read)
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
            return Err(FsError::common(format!("Failed to pread: {}", err_msg)));
        }

        buffer.truncate(actual_read);
        Ok(bytes::Bytes::from(buffer))
    }

    /// Get current read position
    pub async fn tell(&self) -> FsResult<i64> {
        let handle = self.get_reader_handle()?;

        let mut offset = 0i64;
        let status = unsafe { jindo_reader_tell(handle.0, &mut offset) };

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

    /// Get file length
    /// If FFI call fails (e.g., seek to end fails for newly written files),
    /// returns the cached length from when the reader was opened
    pub async fn get_file_length(&self) -> FsResult<i64> {
        let handle = self.get_reader_handle()?;

        let mut length = 0i64;
        let status = unsafe { jindo_reader_get_file_length(handle.0, &mut length) };

        if status != JindoStatus::Ok {
            // Fallback: return cached length from when reader was opened
            // This is more reliable for newly written files where seek may fail
            return Ok(self.length);
        }

        Ok(length)
    }
}

/// OSS-HDFS Reader implementation using JindoSDK C++ library via FFI
pub struct OssHdfsReader {
    pub(crate) reader_handle: Arc<Mutex<JindoReaderHandle>>,
    pub(crate) path: Path,
    pub(crate) length: i64,
    pub(crate) pos: i64,
    pub(crate) chunk_size: usize,
    pub(crate) status: FileStatus,
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
        // This is not used in our implementation
        // We manage chunks internally in read_chunk0
        // Return a mutable reference to a static empty slice
        // Note: This is safe because we don't actually use the returned value
        // The actual chunk is returned from read_chunk0
        use std::cell::RefCell;
        thread_local! {
            static EMPTY: RefCell<DataSlice> = RefCell::new(DataSlice::Empty);
        }
        EMPTY.with(|e| unsafe { &mut *e.as_ptr() })
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
        let mut actual_read = 0usize;

        {
            let handle = self.get_reader_handle()?;
            let status = unsafe {
                jindo_reader_read(handle.0, chunk_size, buffer.as_mut_ptr(), &mut actual_read)
            };

            if status != JindoStatus::Ok {
                // For empty files or EOF, actual_read might be 0, which is acceptable
                if self.length == 0 || self.pos >= self.length {
                    return Ok(DataSlice::Empty);
                }
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
                return Err(FsError::common(format!("Failed to read: {}", err_msg)));
            }
        } // handle is dropped here, releasing the borrow

        if actual_read == 0 {
            return Ok(DataSlice::Empty);
        }

        buffer.truncate(actual_read);
        self.pos += actual_read as i64;

        Ok(DataSlice::Bytes(bytes::Bytes::from(buffer)))
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 || pos > self.length {
            return Err(FsError::common("Invalid seek position"));
        }

        {
            let handle = self.get_reader_handle()?;
            let status = unsafe { jindo_reader_seek(handle.0, pos) };

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
                return Err(FsError::common(format!("Failed to seek: {}", err_msg)));
            }
        } // handle is dropped here, releasing the borrow

        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        let handle = self.get_reader_handle()?;
        unsafe {
            jindo_reader_close(handle.0);
        }
        Ok(())
    }
}

impl Drop for OssHdfsReader {
    fn drop(&mut self) {
        let handle = self.reader_handle.lock().ok();
        if let Some(handle) = handle {
            if !handle.is_null() {
                unsafe {
                    jindo_reader_free(handle.0);
                }
            }
        }
    }
}
