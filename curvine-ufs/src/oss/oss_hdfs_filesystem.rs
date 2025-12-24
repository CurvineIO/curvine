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
use curvine_common::conf::UfsConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{FileStatus, FileType, SetAttrOpts};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::error::ErrorExt;
use orpc::sys::DataSlice;
use std::collections::HashMap;
use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_void;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::conf::OssConf;
use crate::err_ufs;
use crate::oss::ffi::*;
use crate::oss::{OssHdfsReader, OssHdfsWriter, SCHEME};
use std::ffi::NulError;

// Helper to convert CString errors
fn cstring_err(e: NulError) -> FsError {
    FsError::common(format!("Invalid string (contains null byte): {}", e))
}

// Helper to set string configuration in JindoSDK
unsafe fn set_config_string(
    config_handle: *mut c_void,
    key: &str,
    value: &str,
) -> FsResult<()> {
    let key_cstr = CString::new(key).map_err(cstring_err)?;
    let value_cstr = CString::new(value).map_err(cstring_err)?;
    jindo_config_set_string(config_handle, key_cstr.as_ptr(), value_cstr.as_ptr());
    Ok(())
}

// Helper to set bool configuration in JindoSDK
unsafe fn set_config_bool(
    config_handle: *mut c_void,
    key: &str,
    value: bool,
) -> FsResult<()> {
    let key_cstr = CString::new(key).map_err(cstring_err)?;
    jindo_config_set_bool(config_handle, key_cstr.as_ptr(), value);
    Ok(())
}

// Helper to get last error from JindoSDK
fn get_last_error() -> String {
    unsafe {
        let err_ptr = jindo_get_last_error();
        if err_ptr.is_null() {
            String::from("Unknown error")
        } else {
            std::ffi::CStr::from_ptr(err_ptr)
                .to_string_lossy()
                .into_owned()
        }
    }
}

fn err_from_c(err: *const std::os::raw::c_char) -> Option<String> {
    if err.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(err) }.to_string_lossy().into_owned())
    }
}

/// OSS-HDFS file system implementation using JindoSDK C++ library via FFI
#[derive(Clone)]
pub struct OssHdfsFileSystem {
    inner: Arc<OssHdfsFileSystemInner>,
}

struct OssHdfsFileSystemInner {
    fs_handle: Mutex<JindoFileSystemHandle>,
    conf: UfsConf,
}

impl Drop for OssHdfsFileSystemInner {
    fn drop(&mut self) {
        // We are in the last Arc reference to this inner, so no other thread can
        // legitimately be using the handle anymore. Use get_mut() to avoid locking.
        let handle = match self.fs_handle.get_mut() {
            Ok(h) => h,
            Err(poisoned) => poisoned.into_inner(),
        };

        if !handle.is_null() {
            unsafe {
                jindo_filesystem_free(handle.0);
            }
            // Defensive: ensure any accidental future use crashes earlier / is caught.
            *handle = JindoFileSystemHandle(std::ptr::null_mut());
        }
    }
}

// Constants for FileStatus defaults
// Note: OSS-HDFS is an object storage system, not a traditional distributed filesystem.
// - replicas: Object storage handles redundancy at the storage layer, so we use 1 as a placeholder
// - block_size: Object storage stores data as objects, not blocks, so this value is informational only
const DEFAULT_BLOCK_SIZE: i64 = 4 * 1024 * 1024; // 4MB (matches opendal.rs default)
const DEFAULT_REPLICAS: i32 = 1; // Object storage redundancy is handled by the storage layer

impl OssHdfsFileSystem {
    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        // Validate scheme
        path
            .scheme()
            .ok_or_else(|| FsError::invalid_path(path.full_path(), "Missing scheme"))
            .and_then(|s| {
                if s == SCHEME {
                    Ok(s)
                } else {
                    Err(FsError::invalid_path(
                        path.full_path(),
                        format!("Expected scheme '{}', got '{}'", SCHEME, s),
                    ))
                }
            })?;

        let bucket = path
            .authority()
            .ok_or_else(|| FsError::invalid_path(path.full_path(), "URI missing bucket name"))?
            .to_string();

        // Convert HashMap to UfsConf for storage
        let ufs_conf = UfsConf::with_map(conf.clone());
        
        let oss_conf = OssConf::with_map(conf)
            .map_err(|e| FsError::from(e).ctx("Invalid OSS configuration"))?;

        // Create JindoSDK config
        let config_handle_ptr = unsafe { jindo_config_new() };
        if config_handle_ptr.is_null() {
            return err_ufs!("Failed to create JindoSDK config");
        }

        let config_handle = JindoConfigHandle(config_handle_ptr);
        // Set configuration parameters
        unsafe {
            set_config_string(config_handle.0, OssConf::ENDPOINT, &oss_conf.endpoint_url)?;
            set_config_string(config_handle.0, OssConf::ACCESS_KEY_ID, &oss_conf.access_key)?;
            set_config_string(config_handle.0, OssConf::ACCESS_KEY_SECRET, &oss_conf.secret_key)?;

            if let Some(region_name) = &oss_conf.region_name {
                set_config_string(config_handle.0, OssConf::REGION, region_name)?;
            }

            // Set OSS-HDFS specific flags
            set_config_bool(
                config_handle.0,
                OssConf::SECOND_LEVEL_DOMAIN_ENABLE,
                oss_conf.second_level_domain_enable,
            )?;
            set_config_bool(
                config_handle.0,
                OssConf::DATA_LAKE_STORAGE_ENABLE,
                oss_conf.data_lake_storage_enable,
            )?;
        }

        // Create filesystem
        let fs_handle_ptr = unsafe { jindo_filesystem_new() };
        if fs_handle_ptr.is_null() {
            unsafe { jindo_config_free(config_handle.0) };
            return err_ufs!("Failed to create JindoSDK filesystem");
        }

        let fs_handle = JindoFileSystemHandle(fs_handle_ptr);
        // Initialize filesystem
        // Get user from environment variables (similar to HDFS implementation)
        // Priority: HADOOP_USER_NAME -> USER -> default "root"
        let user = std::env::var("HADOOP_USER_NAME")
            .or_else(|_| std::env::var("USER"))
            .unwrap_or_else(|_| "root".to_string());
        
        let bucket_cstr = CString::new(format!("oss://{}/", bucket)).map_err(cstring_err)?;
        let user_cstr = CString::new(user.as_str()).map_err(cstring_err)?;

        let status = unsafe {
            jindo_filesystem_init(
                fs_handle.0,
                bucket_cstr.as_ptr(),
                user_cstr.as_ptr(),
                config_handle.0,
            )
        };

        unsafe { jindo_config_free(config_handle.0) };

        if status != JindoStatus::Ok {
            unsafe { jindo_filesystem_free(fs_handle.0) };
            return err_ufs!("Failed to initialize JindoSDK filesystem: {}", get_last_error());
        }

        Ok(Self {
            inner: Arc::new(OssHdfsFileSystemInner {
                fs_handle: Mutex::new(fs_handle),
                conf: ufs_conf,
            }),
        })
    }

    fn path_to_cstring(&self, path: &Path) -> FsResult<CString> {
        // IMPORTANT:
        // JindoSDK's OSS-HDFS C API validates the input as a URL/URI. In practice it rejects
        // both "/ufs-test/dir1" and "ufs-test/dir1" as "invalid path".
        //
        // The most reliable format is the full OSS URI, e.g. "oss://bucket/ufs-test/dir1".
        // `Path::full_path()` preserves scheme + authority + normalized path.
        //
        // (Note: `OssHdfsFileSystem` should only be used with `oss://...` paths, but we keep
        // a defensive fallback for non-URI paths.)
        let s = if path.scheme() == Some(SCHEME) {
            path.full_path()
        } else {
            path.path()
        };

        CString::new(s).map_err(cstring_err)
    }

    /// Execute an FFI operation with the filesystem handle locked.
    /// This ensures the lock is held for the entire duration of the FFI call.
    fn with_fs_handle<F, R>(&self, f: F) -> FsResult<R>
    where
        F: FnOnce(*mut c_void) -> R,
    {
        let handle = self
            .inner
            .fs_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock filesystem handle: {}", e)))?;
        
        // Validate handle before use
        if handle.is_null() {
            return Err(FsError::common("Filesystem handle is null"));
        }
        
        log::info!("with_fs_handle: {:?}", handle.0);
        Ok(f(handle.0))
    }

    fn check_status(status: JindoStatus, operation: &str) -> FsResult<()> {
        if status != JindoStatus::Ok {
            return err_ufs!("{}: {}", operation, get_last_error());
        }
        Ok(())
    }

    fn check_status_with_err(
        status: JindoStatus,
        operation: &str,
        err: Option<String>,
    ) -> FsResult<()> {
        if status != JindoStatus::Ok {
            if let Some(e) = err {
                return err_ufs!("{}: {}", operation, e);
            }
            return err_ufs!("{}: {}", operation, get_last_error());
        }
        Ok(())
    }

    fn new_file_status(path: &Path, is_dir: bool, len: i64, mtime: i64, is_complete: bool) -> FileStatus {
        FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir,
            mtime,
            is_complete,
            len,
            replicas: DEFAULT_REPLICAS,
            block_size: DEFAULT_BLOCK_SIZE,
            file_type: if is_dir { FileType::Dir } else { FileType::File },
            ..Default::default()
        }
    }

    /// Create FileStatus from JindoFileInfo with path and name
    fn file_status_from_info(path: String, name: String, info: &JindoFileInfo) -> FileStatus {
        // Convert C strings to Rust strings safely
        // Always check for null pointers before dereferencing
        let owner = if info.user.is_null() {
            String::new()
        } else {
            unsafe {
                // Safety: We've checked that info.user is not null
                // The pointer is valid until jindo_file_info_free is called
                match std::ffi::CStr::from_ptr(info.user).to_str() {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        // If conversion fails, use lossy conversion as fallback
                        std::ffi::CStr::from_ptr(info.user)
                            .to_string_lossy()
                            .into_owned()
                    }
                }
            }
        };

        let group = if info.group.is_null() {
            String::new()
        } else {
            unsafe {
                // Safety: We've checked that info.group is not null
                // The pointer is valid until jindo_file_info_free is called
                match std::ffi::CStr::from_ptr(info.group).to_str() {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        // If conversion fails, use lossy conversion as fallback
                        std::ffi::CStr::from_ptr(info.group)
                            .to_string_lossy()
                            .into_owned()
                    }
                }
            }
        };

        // Convert file type: 1=dir, 2=file, 3=symlink, 4=mount
        let file_type = match info.type_ {
            1 => FileType::Dir,
            2 => FileType::File,
            3 => FileType::Link,
            _ => FileType::File, // Default to File for unknown types
        };

        FileStatus {
            path,
            name,
            is_dir: info.type_ == 1,
            mtime: info.mtime,
            atime: info.atime,
            is_complete: true,
            len: info.length,
            replicas: DEFAULT_REPLICAS,
            block_size: DEFAULT_BLOCK_SIZE,
            file_type,
            mode: (info.perm as u16) as u32, // Convert i16 to u32 (safe: file permissions are non-negative)
            owner,
            group,
            ..Default::default()
        }
    }

    pub fn conf(&self) -> &UfsConf {
        &self.inner.conf
    }
}

impl FileSystem<OssHdfsWriter, OssHdfsReader> for OssHdfsFileSystem {
    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let path_cstr = self.path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();

        extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
            // Safety: userdata is a Box<oneshot::Sender<...>> allocated by Rust.
            let tx = unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
            let _ = tx.send((status, err_from_c(err)));
        }

        {
            // IMPORTANT: keep raw pointer userdata scoped to this block so this Future remains Send.
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_mkdir_async(
                    fs_handle,
                    path_cstr.as_ptr(),
                    create_parent,
                    Some(cb),
                    userdata,
                )
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>))
                };
                Self::check_status(start_status, "Failed to start async mkdir")?;
            }
        }

        let (status, err) = rx
            .await
            .map_err(|_| FsError::common("Async mkdir callback dropped"))?;
        Self::check_status_with_err(status, "Failed to create directory", err)?;

        Ok(true)
    }

    async fn create(&self, path: &Path, _overwrite: bool) -> FsResult<OssHdfsWriter> {
        let path_cstr = self.path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel::<(JindoStatus, JindoWriterHandle, Option<String>)>();

        extern "C" fn cb(
            status: JindoStatus,
            writer: *mut c_void,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx = unsafe {
                Box::from_raw(
                    userdata as *mut oneshot::Sender<(JindoStatus, JindoWriterHandle, Option<String>)>,
                )
            };
            let _ = tx.send((status, JindoWriterHandle(writer), err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_open_writer_async(fs_handle, path_cstr.as_ptr(), Some(cb), userdata)
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, JindoWriterHandle, Option<String>)>,
                    ))
                };
                Self::check_status(start_status, "Failed to start async open_writer")?;
            }
        }

        let (status, writer_handle, err) = rx
            .await
            .map_err(|_| FsError::common("Async open_writer callback dropped"))?;
        Self::check_status_with_err(status, "Failed to create writer", err)?;
        if writer_handle.is_null() {
            return Err(FsError::common("Writer handle is null after creation"));
        }

        let current_time = LocalTime::mills() as i64;
        let status = Self::new_file_status(path, false, 0, current_time, false);

        Ok(OssHdfsWriter {
            writer_handle: Arc::new(Mutex::new(Some(writer_handle))),
            path: path.clone(),
            status,
            pos: 0,
            chunk_size: 8 * 1024 * 1024, // 8MB
            chunk: BytesMut::new(),
        })
    }

    async fn append(&self, path: &Path) -> FsResult<OssHdfsWriter> {
        let path_cstr = self.path_to_cstring(path)?;

        // Open writer in append mode (creates file if not exists, appends if exists)
        let (tx, rx) = oneshot::channel::<(JindoStatus, JindoWriterHandle, Option<String>)>();

        extern "C" fn cb(
            status: JindoStatus,
            writer: *mut c_void,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx = unsafe {
                Box::from_raw(
                    userdata as *mut oneshot::Sender<(JindoStatus, JindoWriterHandle, Option<String>)>,
                )
            };
            let _ = tx.send((status, JindoWriterHandle(writer), err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_open_writer_append_async(
                    fs_handle,
                    path_cstr.as_ptr(),
                    Some(cb),
                    userdata,
                )
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, JindoWriterHandle, Option<String>)>,
                    ))
                };
                Self::check_status(start_status, "Failed to start async open_writer_append")?;
            }
        }

        let (status, writer_handle, err) = rx
            .await
            .map_err(|_| FsError::common("Async open_writer_append callback dropped"))?;
        Self::check_status_with_err(status, "Failed to open writer for append", err)?;
        if writer_handle.is_null() {
            return Err(FsError::common("Writer handle is null after append creation"));
        }

        // Get current position (file length if file exists, 0 if new file)
        let (tx, rx) = oneshot::channel::<(JindoStatus, i64, Option<String>)>();
        extern "C" fn tell_cb(
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
            let start_status = unsafe { jindo_writer_tell_async(writer_handle.0, Some(tell_cb), userdata) };
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, i64, Option<String>)>));
                    jindo_writer_free(writer_handle.0);
                }
                Self::check_status(start_status, "Failed to start async writer tell")?;
            }
        }

        let (status, current_pos, err) = rx
            .await
            .map_err(|_| FsError::common("Async writer tell callback dropped"))?;
        if status != JindoStatus::Ok {
            unsafe { jindo_writer_free(writer_handle.0) };
            Self::check_status_with_err(status, "Failed to get writer position for append", err)?;
        }

        let current_time = LocalTime::mills() as i64;
        let mut file_status = Self::new_file_status(path, false, current_pos, current_time, false);
        // Update length to current position
        file_status.len = current_pos;

        Ok(OssHdfsWriter {
            writer_handle: Arc::new(Mutex::new(Some(writer_handle))),
            path: path.clone(),
            status: file_status,
            pos: current_pos,
            chunk_size: 8 * 1024 * 1024, // 8MB
            chunk: BytesMut::new(),
        })
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let path_cstr = self.path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel::<(JindoStatus, bool, Option<String>)>();

        extern "C" fn cb(
            status: JindoStatus,
            value: bool,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx =
                unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, bool, Option<String>)>) };
            let _ = tx.send((status, value, err_from_c(err)));
        }

        {
            // IMPORTANT: keep raw pointer userdata scoped to this block so this Future remains Send.
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_exists_async(fs_handle, path_cstr.as_ptr(), Some(cb), userdata)
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, bool, Option<String>)>,
                    ))
                };
                Self::check_status(start_status, "Failed to start async exists")?;
            }
        }

        let (status, exists, err) = rx
            .await
            .map_err(|_| FsError::common("Async exists callback dropped"))?;
        Self::check_status_with_err(status, "Failed to check existence", err)?;

        Ok(exists)
    }

    async fn open(&self, path: &Path) -> FsResult<OssHdfsReader> {
        let path_cstr = self.path_to_cstring(path)?;

        let (tx, rx) = oneshot::channel::<(JindoStatus, JindoReaderHandle, Option<String>)>();

        extern "C" fn cb(
            status: JindoStatus,
            reader: *mut c_void,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx =
                unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, JindoReaderHandle, Option<String>)>) };
            let _ = tx.send((status, JindoReaderHandle(reader), err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_open_reader_async(fs_handle, path_cstr.as_ptr(), Some(cb), userdata)
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, JindoReaderHandle, Option<String>)>,
                    ))
                };
                Self::check_status(start_status, "Failed to start async open_reader")?;
            }
        }

        let (open_status, reader_handle, err) = rx
            .await
            .map_err(|_| FsError::common("Async open_reader callback dropped"))?;
        Self::check_status_with_err(open_status, "Failed to open reader", err)?;
        if reader_handle.is_null() {
            return err_ufs!("Failed to open reader: reader handle is null");
        }

        // Get file status - if this fails, we need to free the reader handle
        let file_status = match self.get_status(path).await {
            Ok(status) => status,
            Err(e) => {
                // Clean up reader handle on error
                unsafe { jindo_reader_free(reader_handle.0) };
                return Err(e);
            }
        };

        Ok(OssHdfsReader {
            reader_handle: Arc::new(Mutex::new(Some(reader_handle))),
            path: path.clone(),
            length: file_status.len,
            pos: 0,
            chunk_size: 8 * 1024 * 1024, // 8MB
            status: file_status,
            chunk: DataSlice::Empty,
        })
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let src_cstr = self.path_to_cstring(src)?;
        let dst_cstr = self.path_to_cstring(dst)?;
        let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();

        extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
            let tx = unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
            let _ = tx.send((status, err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_rename_async(
                    fs_handle,
                    src_cstr.as_ptr(),
                    dst_cstr.as_ptr(),
                    Some(cb),
                    userdata,
                )
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>))
                };
                Self::check_status(start_status, "Failed to start async rename")?;
            }
        }

        let (status, err) = rx
            .await
            .map_err(|_| FsError::common("Async rename callback dropped"))?;
        Self::check_status_with_err(status, "Failed to rename", err)?;

        Ok(true)
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let path_cstr = self.path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();

        extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
            let tx = unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
            let _ = tx.send((status, err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_remove_async(fs_handle, path_cstr.as_ptr(), recursive, Some(cb), userdata)
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>))
                };
                Self::check_status(start_status, "Failed to start async delete")?;
            }
        }

        let (status, err) = rx
            .await
            .map_err(|_| FsError::common("Async delete callback dropped"))?;
        Self::check_status_with_err(status, "Failed to delete", err)?;

        Ok(())
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let path_cstr = self.path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel::<(JindoStatus, usize, Option<String>)>();

        extern "C" fn cb(
            status: JindoStatus,
            info: *mut JindoFileInfo,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx =
                unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, usize, Option<String>)>) };
            let _ = tx.send((status, info as usize, err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_get_file_info_async(fs_handle, path_cstr.as_ptr(), Some(cb), userdata)
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, usize, Option<String>)>))
                };
                Self::check_status(start_status, "Failed to start async get_file_info")?;
            }
        }

        let (status, info_addr, err) = rx
            .await
            .map_err(|_| FsError::common("Async get_file_info callback dropped"))?;
        let info_ptr = info_addr as *mut JindoFileInfo;

        if status != JindoStatus::Ok {
            // Ensure we don't leak any partial allocation.
            if !info_ptr.is_null() {
                unsafe {
                    jindo_file_info_free(info_ptr);
                    jindo_free(info_ptr as *mut c_void);
                }
            }
            return if status == JindoStatus::FileNotFound {
                Err(FsError::common("File not found"))
            } else {
                Self::check_status_with_err(status, "Failed to get file info", err)?;
                unreachable!("check_status_with_err returns Err on non-OK status");
            };
        }

        if info_ptr.is_null() {
            return err_ufs!("Failed to get file info: null result");
        }

        let file_status = unsafe {
            Self::file_status_from_info(path.full_path().to_owned(), path.name().to_owned(), &*info_ptr)
        };

        unsafe {
            jindo_file_info_free(info_ptr);
            jindo_free(info_ptr as *mut c_void);
        }

        Ok(file_status)
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let path_cstr = self.path_to_cstring(path)?;
        let (tx, rx) = oneshot::channel::<(JindoStatus, usize, Option<String>)>();

        extern "C" fn cb(
            status: JindoStatus,
            result: *mut JindoListResult,
            err: *const std::os::raw::c_char,
            userdata: *mut c_void,
        ) {
            let tx = unsafe {
                Box::from_raw(
                    userdata as *mut oneshot::Sender<(JindoStatus, usize, Option<String>)>,
                )
            };
            let _ = tx.send((status, result as usize, err_from_c(err)));
        }

        {
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;
            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_list_dir_async(fs_handle, path_cstr.as_ptr(), false, Some(cb), userdata)
            })?;
            if start_status != JindoStatus::Ok {
                unsafe {
                    drop(Box::from_raw(
                        userdata as *mut oneshot::Sender<(JindoStatus, usize, Option<String>)>,
                    ))
                };
                Self::check_status(start_status, "Failed to start async list_dir")?;
            }
        }

        let (status, list_addr, err) = rx
            .await
            .map_err(|_| FsError::common("Async list_dir callback dropped"))?;
        let list_ptr = list_addr as *mut JindoListResult;
        if status != JindoStatus::Ok {
            if !list_ptr.is_null() {
                unsafe {
                    jindo_list_result_free(list_ptr);
                    jindo_free(list_ptr as *mut c_void);
                }
            }
            Self::check_status_with_err(status, "Failed to list directory", err)?;
        }

        let mut file_statuses = Vec::new();
        if list_ptr.is_null() {
            return Ok(file_statuses);
        }

        unsafe {
            if (*list_ptr).file_infos.is_null() || (*list_ptr).count == 0 {
                jindo_list_result_free(list_ptr);
                jindo_free(list_ptr as *mut c_void);
                return Ok(file_statuses);
            }

            let file_infos_slice =
                std::slice::from_raw_parts((*list_ptr).file_infos, (*list_ptr).count);

            for info in file_infos_slice {
                let entry_path = if info.path.is_null() {
                    path.full_path().to_owned()
                } else {
                    match std::ffi::CStr::from_ptr(info.path).to_str() {
                        Ok(s) => s.to_string(),
                        Err(_) => std::ffi::CStr::from_ptr(info.path).to_string_lossy().into_owned(),
                    }
                };

                let trimmed_path = entry_path.trim_end_matches('/');
                let file_name = trimmed_path
                    .rfind('/')
                    .map(|i| &trimmed_path[i + 1..])
                    .unwrap_or(trimmed_path)
                    .to_owned();

                file_statuses.push(Self::file_status_from_info(entry_path, file_name, info));
            }

            jindo_list_result_free(list_ptr);
            jindo_free(list_ptr as *mut c_void);
        }

        Ok(file_statuses)
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        let path_cstr = self.path_to_cstring(path)?;

        // Handle permission (mode in SetAttrOpts)
        if let Some(mode) = opts.mode {
            let perm = mode as i16; // Convert u32 mode to i16 permission
            let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;

            extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
                let tx = unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
                let _ = tx.send((status, err_from_c(err)));
            }

            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_set_permission_async(
                    fs_handle,
                    path_cstr.as_ptr(),
                    perm,
                    Some(cb),
                    userdata,
                )
            })?;
            if start_status != JindoStatus::Ok {
                unsafe { drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>)) };
                Self::check_status(start_status, "Failed to start async set_permission")?;
            }

            let (status, err) = rx
                .await
                .map_err(|_| FsError::common("Async set_permission callback dropped"))?;
            Self::check_status_with_err(status, "Failed to set permission", err)?;
        }

        // Handle owner
        if opts.owner.is_some() || opts.group.is_some() {
            let user_cstr = CString::new(opts.owner.as_deref().unwrap_or("")).map_err(cstring_err)?;
            let group_cstr = CString::new(opts.group.as_deref().unwrap_or("")).map_err(cstring_err)?;
            let (tx, rx) = oneshot::channel::<(JindoStatus, Option<String>)>();
            let userdata = Box::into_raw(Box::new(tx)) as *mut c_void;

            extern "C" fn cb(status: JindoStatus, err: *const std::os::raw::c_char, userdata: *mut c_void) {
                let tx = unsafe { Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>) };
                let _ = tx.send((status, err_from_c(err)));
            }

            let start_status = self.with_fs_handle(|fs_handle| unsafe {
                jindo_filesystem_set_owner_async(
                    fs_handle,
                    path_cstr.as_ptr(),
                    user_cstr.as_ptr(),
                    group_cstr.as_ptr(),
                    Some(cb),
                    userdata,
                )
            })?;
            if start_status != JindoStatus::Ok {
                unsafe { drop(Box::from_raw(userdata as *mut oneshot::Sender<(JindoStatus, Option<String>)>)) };
                Self::check_status(start_status, "Failed to start async set_owner")?;
            }

            let (status, err) = rx
                .await
                .map_err(|_| FsError::common("Async set_owner callback dropped"))?;
            Self::check_status_with_err(status, "Failed to set owner", err)?;
        }

        Ok(())
    }
}