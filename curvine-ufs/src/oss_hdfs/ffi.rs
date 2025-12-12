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

//! FFI bindings for JindoSDK C++ library

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

// Opaque handle types
// We use newtype wrappers instead of type aliases to allow implementing Send/Sync
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct JindoFileSystemHandle(pub *mut c_void);
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct JindoWriterHandle(pub *mut c_void);
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct JindoReaderHandle(pub *mut c_void);
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct JindoConfigHandle(pub *mut c_void);

// Safety: These handles are opaque pointers to C++ objects.
// The underlying JindoSDK C++ library is thread-safe and handles
// all synchronization internally. We mark these as Send/Sync to allow
// them to be used across thread boundaries when properly synchronized
// (e.g., wrapped in Arc<Mutex<>>).
unsafe impl Send for JindoFileSystemHandle {}
unsafe impl Sync for JindoFileSystemHandle {}
unsafe impl Send for JindoWriterHandle {}
unsafe impl Sync for JindoWriterHandle {}
unsafe impl Send for JindoReaderHandle {}
unsafe impl Sync for JindoReaderHandle {}
unsafe impl Send for JindoConfigHandle {}
unsafe impl Sync for JindoConfigHandle {}

impl JindoFileSystemHandle {
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

impl JindoWriterHandle {
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

impl JindoReaderHandle {
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

impl JindoConfigHandle {
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

// Status codes
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JindoStatus {
    Ok = 0,
    Error = 1,
    FileNotFound = 2,
    IoError = 3,
}

// File info structure
#[repr(C)]
pub struct JindoFileInfo {
    pub path: *mut c_char,
    pub user: *mut c_char,
    pub group: *mut c_char,
    #[allow(non_snake_case)]
    pub type_: i8, // 1=dir, 2=file, 3=symlink, 4=mount (C struct uses 'type' but Rust keyword)
    pub perm: i16,
    pub length: i64,
    pub mtime: i64,
    pub atime: i64,
}

// List result structure
#[repr(C)]
pub struct JindoListResult {
    pub file_infos: *mut JindoFileInfo,
    pub count: usize,
}

// Content summary structure
#[repr(C)]
pub struct JindoContentSummary {
    pub file_count: i64,
    pub dir_count: i64,
    pub file_size: i64,
}

// External function declarations
#[link(name = "jindosdk_ffi")]
extern "C" {
    // Config functions
    pub fn jindo_config_new() -> *mut c_void;
    pub fn jindo_config_set_string(
        config: *mut c_void,
        key: *const c_char,
        value: *const c_char,
    );
    pub fn jindo_config_set_bool(config: *mut c_void, key: *const c_char, value: bool);
    pub fn jindo_config_free(config: *mut c_void);

    // FileSystem functions
    pub fn jindo_filesystem_new() -> *mut c_void;
    pub fn jindo_filesystem_init(
        fs: *mut c_void,
        bucket: *const c_char,
        user: *const c_char,
        config: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_free(fs: *mut c_void);

    // Directory operations
    pub fn jindo_filesystem_mkdir(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
    ) -> JindoStatus;
    pub fn jindo_filesystem_rename(
        fs: *mut c_void,
        oldpath: *const c_char,
        newpath: *const c_char,
    ) -> JindoStatus;
    pub fn jindo_filesystem_remove(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
    ) -> JindoStatus;
    pub fn jindo_filesystem_exists(
        fs: *mut c_void,
        path: *const c_char,
        exists: *mut bool,
    ) -> JindoStatus;

    // File info operations
    pub fn jindo_filesystem_get_file_info(
        fs: *mut c_void,
        path: *const c_char,
        info: *mut JindoFileInfo,
    ) -> JindoStatus;
    pub fn jindo_file_info_free(info: *mut JindoFileInfo);

    pub fn jindo_filesystem_list_dir(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
        result: *mut JindoListResult,
    ) -> JindoStatus;
    pub fn jindo_list_result_free(result: *mut JindoListResult);

    pub fn jindo_filesystem_get_content_summary(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
        summary: *mut JindoContentSummary,
    ) -> JindoStatus;

    // OSS-HDFS specific operations
    pub fn jindo_filesystem_set_permission(
        fs: *mut c_void,
        path: *const c_char,
        perm: i16,
    ) -> JindoStatus;
    pub fn jindo_filesystem_set_owner(
        fs: *mut c_void,
        path: *const c_char,
        user: *const c_char,
        group: *const c_char,
    ) -> JindoStatus;

    // Writer functions
    pub fn jindo_filesystem_open_writer(
        fs: *mut c_void,
        path: *const c_char,
        writer: *mut *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_open_writer_append(
        fs: *mut c_void,
        path: *const c_char,
        writer: *mut *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_writer_write(
        writer: *mut c_void,
        data: *const u8,
        len: usize,
    ) -> JindoStatus;
    pub fn jindo_writer_flush(writer: *mut c_void) -> JindoStatus;
    pub fn jindo_writer_tell(writer: *mut c_void, offset: *mut i64) -> JindoStatus;
    pub fn jindo_writer_close(writer: *mut c_void) -> JindoStatus;
    pub fn jindo_writer_free(writer: *mut c_void);

    // Reader functions
    pub fn jindo_filesystem_open_reader(
        fs: *mut c_void,
        path: *const c_char,
        reader: *mut *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_read(
        reader: *mut c_void,
        n: usize,
        scratch: *mut u8,
        actual_read: *mut usize,
    ) -> JindoStatus;
    pub fn jindo_reader_pread(
        reader: *mut c_void,
        offset: i64,
        n: usize,
        scratch: *mut u8,
        actual_read: *mut usize,
    ) -> JindoStatus;
    pub fn jindo_reader_seek(reader: *mut c_void, offset: i64) -> JindoStatus;
    pub fn jindo_reader_tell(reader: *mut c_void, offset: *mut i64) -> JindoStatus;
    pub fn jindo_reader_get_file_length(reader: *mut c_void, length: *mut i64)
        -> JindoStatus;
    pub fn jindo_reader_close(reader: *mut c_void) -> JindoStatus;
    pub fn jindo_reader_free(reader: *mut c_void);

    // Error handling
    pub fn jindo_get_last_error() -> *const c_char;
}