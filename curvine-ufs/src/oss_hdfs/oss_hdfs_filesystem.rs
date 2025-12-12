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

use curvine_common::conf::UfsConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{FileStatus, FileType, SetAttrOpts};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::error::ErrorExt;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_void;
use std::sync::{Arc, Mutex};

use crate::conf::OssConf;
use crate::err_ufs;
use crate::oss_hdfs::ffi::*;
use crate::oss_hdfs::{OssHdfsReader, OssHdfsWriter, SCHEME};
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

/// OSS-HDFS file system implementation using JindoSDK C++ library via FFI
#[derive(Clone)]
pub struct OssHdfsFileSystem {
    fs_handle: Arc<Mutex<JindoFileSystemHandle>>,
    conf: Arc<UfsConf>,
    bucket: String,
}

// Safety: JindoFileSystemHandle is a *mut c_void pointer from FFI.
// The handle is protected by Mutex, and all FFI calls are thread-safe.
// The underlying C++ library handles thread safety internally.
unsafe impl Send for OssHdfsFileSystem {}
unsafe impl Sync for OssHdfsFileSystem {}

// Constants for FileStatus defaults
// Note: OSS-HDFS is an object storage system, not a traditional distributed filesystem.
// - replicas: Object storage handles redundancy at the storage layer, so we use 1 as a placeholder
// - block_size: Object storage stores data as objects, not blocks, so this value is informational only
const DEFAULT_BLOCK_SIZE: i64 = 4 * 1024 * 1024; // 4MB (matches opendal.rs default)
const DEFAULT_REPLICAS: i32 = 1; // Object storage redundancy is handled by the storage layer

impl OssHdfsFileSystem {
    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        // Validate scheme
        let scheme = path
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
            fs_handle: Arc::new(Mutex::new(fs_handle)),
            conf: Arc::new(ufs_conf),
            bucket,
        })
    }

    fn path_to_cstring(&self, path: &Path) -> FsResult<CString> {
        let object_path = if path.is_root() {
            "/".to_string()
        } else {
            format!("oss://{}{}", self.bucket, path.path())
        };
        CString::new(object_path).map_err(cstring_err)
    }

    /// Execute an FFI operation with the filesystem handle locked.
    /// This ensures the lock is held for the entire duration of the FFI call.
    fn with_fs_handle<F, R>(&self, f: F) -> FsResult<R>
    where
        F: FnOnce(*mut c_void) -> R,
    {
        let handle = self
            .fs_handle
            .lock()
            .map_err(|e| FsError::common(format!("Failed to lock filesystem handle: {}", e)))?;
        Ok(f(handle.0))
    }

    fn check_status(status: JindoStatus, operation: &str) -> FsResult<()> {
        if status != JindoStatus::Ok {
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
        let owner = if info.user.is_null() {
            String::new()
        } else {
            unsafe {
                std::ffi::CStr::from_ptr(info.user)
                    .to_string_lossy()
                    .into_owned()
            }
        };

        let group = if info.group.is_null() {
            String::new()
        } else {
            unsafe {
                std::ffi::CStr::from_ptr(info.group)
                    .to_string_lossy()
                    .into_owned()
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
        &self.conf
    }
}

impl Drop for OssHdfsFileSystem {
    fn drop(&mut self) {
        let handle = self.fs_handle.lock().ok();
        if let Some(handle) = handle {
            if !handle.is_null() {
                unsafe {
                    jindo_filesystem_free(handle.0);
                }
            }
        }
    }
}

impl FileSystem<OssHdfsWriter, OssHdfsReader> for OssHdfsFileSystem {
    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let path_cstr = self.path_to_cstring(path)?;

        let status = self.with_fs_handle(|fs_handle| {
            unsafe { jindo_filesystem_mkdir(fs_handle, path_cstr.as_ptr(), create_parent) }
        })?;
        Self::check_status(status, "Failed to create directory")?;

        Ok(true)
    }

    async fn create(&self, path: &Path, _overwrite: bool) -> FsResult<OssHdfsWriter> {
        let path_cstr = self.path_to_cstring(path)?;

        let writer_handle = {
            let mut writer_ptr: *mut c_void = std::ptr::null_mut();
            let status = self.with_fs_handle(|fs_handle| {
                unsafe { jindo_filesystem_open_writer(fs_handle, path_cstr.as_ptr(), &mut writer_ptr) }
            })?;
            Self::check_status(status, "Failed to create writer")?;
            JindoWriterHandle(writer_ptr)
        };

        let current_time = LocalTime::mills() as i64;
        let status = Self::new_file_status(path, false, 0, current_time, false);

        Ok(OssHdfsWriter {
            writer_handle: Arc::new(Mutex::new(Some(writer_handle))),
            path: path.clone(),
            status,
            pos: 0,
            chunk_size: 8 * 1024 * 1024, // 8MB
        })
    }

    async fn append(&self, path: &Path) -> FsResult<OssHdfsWriter> {
        let path_cstr = self.path_to_cstring(path)?;

        // Open writer in append mode (creates file if not exists, appends if exists)
        let writer_handle = {
            let mut writer_ptr: *mut c_void = std::ptr::null_mut();
            let status = self.with_fs_handle(|fs_handle| {
                unsafe { jindo_filesystem_open_writer_append(fs_handle, path_cstr.as_ptr(), &mut writer_ptr) }
            })?;
            Self::check_status(status, "Failed to open writer for append")?;
            JindoWriterHandle(writer_ptr)
        };

        // Get current position (file length if file exists, 0 if new file)
        let mut current_pos = 0i64;
        let status = unsafe { jindo_writer_tell(writer_handle.0, &mut current_pos) };
        if status != JindoStatus::Ok {
            unsafe { jindo_writer_free(writer_handle.0) };
            Self::check_status(status, "Failed to get writer position for append")?;
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
        })
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let path_cstr = self.path_to_cstring(path)?;

        let mut exists = false;
        let status = self.with_fs_handle(|fs_handle| {
            unsafe { jindo_filesystem_exists(fs_handle, path_cstr.as_ptr(), &mut exists) }
        })?;
        Self::check_status(status, "Failed to check existence")?;

        Ok(exists)
    }

    async fn open(&self, path: &Path) -> FsResult<OssHdfsReader> {
        let path_cstr = self.path_to_cstring(path)?;

        let reader_handle = {
            let mut reader_ptr: *mut c_void = std::ptr::null_mut();
            let open_status = self.with_fs_handle(|fs_handle| {
                unsafe { jindo_filesystem_open_reader(fs_handle, path_cstr.as_ptr(), &mut reader_ptr) }
            })?;
            Self::check_status(open_status, "Failed to open reader")?;
            JindoReaderHandle(reader_ptr)
        };

        let file_status = self.get_status(path).await.map_err(|e| {
            unsafe { jindo_reader_free(reader_handle.0) };
            e
        })?;

        Ok(OssHdfsReader {
            reader_handle: Arc::new(Mutex::new(reader_handle)),
            path: path.clone(),
            length: file_status.len,
            pos: 0,
            chunk_size: 8 * 1024 * 1024, // 8MB
            status: file_status,
        })
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let src_cstr = self.path_to_cstring(src)?;
        let dst_cstr = self.path_to_cstring(dst)?;

        let status = self.with_fs_handle(|fs_handle| {
            unsafe { jindo_filesystem_rename(fs_handle, src_cstr.as_ptr(), dst_cstr.as_ptr()) }
        })?;
        Self::check_status(status, "Failed to rename")?;

        Ok(true)
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let path_cstr = self.path_to_cstring(path)?;

        let status = self.with_fs_handle(|fs_handle| {
            unsafe { jindo_filesystem_remove(fs_handle, path_cstr.as_ptr(), recursive) }
        })?;
        Self::check_status(status, "Failed to delete")?;

        Ok(())
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let path_cstr = self.path_to_cstring(path)?;

        // Initialize file_info with zeroed memory (all fields null/zero)
        let mut file_info: JindoFileInfo = unsafe { std::mem::zeroed() };

        let status = self.with_fs_handle(|fs_handle| {
            unsafe { jindo_filesystem_get_file_info(fs_handle, path_cstr.as_ptr(), &mut file_info) }
        })?;

        if status != JindoStatus::Ok {
            unsafe { jindo_file_info_free(&mut file_info) };
            return if status == JindoStatus::FileNotFound {
                Err(FsError::common("File not found"))
            } else {
                err_ufs!("Failed to get file info: {}", get_last_error())
            };
        }

        let file_status = Self::file_status_from_info(
            path.full_path().to_owned(),
            path.name().to_owned(),
            &file_info,
        );

        unsafe { jindo_file_info_free(&mut file_info) };

        Ok(file_status)
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let path_cstr = self.path_to_cstring(path)?;

        let mut list_result = JindoListResult {
            file_infos: std::ptr::null_mut(),
            count: 0,
        };

        let status = self.with_fs_handle(|fs_handle| {
            unsafe { jindo_filesystem_list_dir(fs_handle, path_cstr.as_ptr(), false, &mut list_result) }
        })?;

        if status != JindoStatus::Ok {
            unsafe { jindo_list_result_free(&mut list_result) };
            Self::check_status(status, "Failed to list directory")?;
        }

        let mut file_statuses = Vec::new();
        unsafe {
            // Convert raw pointer array to slice for safer and clearer indexing
            let file_infos_slice = std::slice::from_raw_parts(list_result.file_infos, list_result.count);
            
            for info in file_infos_slice {
                let entry_path = if info.path.is_null() {
                    path.full_path().to_owned()
                } else {
                    std::ffi::CStr::from_ptr(info.path)
                        .to_string_lossy()
                        .into_owned()
                };

                // Extract filename from path: get the last component after the last '/'
                // First trim trailing '/' to handle directory paths correctly
                let trimmed_path = entry_path.trim_end_matches('/');
                let file_name = trimmed_path
                    .rfind('/')
                    .map(|i| &trimmed_path[i + 1..])
                    .unwrap_or(trimmed_path)
                    .to_owned();

                file_statuses.push(Self::file_status_from_info(
                    entry_path,  // Move ownership instead of cloning
                    file_name,
                    info,
                ));
            }
            jindo_list_result_free(&mut list_result);
        }

        Ok(file_statuses)
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        let path_cstr = self.path_to_cstring(path)?;

        // Handle permission (mode in SetAttrOpts)
        if let Some(mode) = opts.mode {
            let perm = mode as i16; // Convert u32 mode to i16 permission
            let status = self.with_fs_handle(|fs_handle| {
                unsafe { jindo_filesystem_set_permission(fs_handle, path_cstr.as_ptr(), perm) }
            })?;
            Self::check_status(status, "Failed to set permission")?;
        }

        // Handle owner
        if opts.owner.is_some() || opts.group.is_some() {
            let user_cstr = CString::new(opts.owner.as_deref().unwrap_or("")).map_err(cstring_err)?;
            let group_cstr = CString::new(opts.group.as_deref().unwrap_or("")).map_err(cstring_err)?;

            let status = self.with_fs_handle(|fs_handle| {
                unsafe {
                    jindo_filesystem_set_owner(
                        fs_handle,
                        path_cstr.as_ptr(),
                        user_cstr.as_ptr(),
                        group_cstr.as_ptr(),
                    )
                }
            })?;
            Self::check_status(status, "Failed to set owner")?;
        }

        Ok(())
    }
}