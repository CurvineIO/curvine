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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

// Status codes
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JindoStatus {
    Ok = 0,
    #[allow(dead_code)]
    Error = 1,
    FileNotFound = 2,
    #[allow(dead_code)]
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
#[allow(dead_code)]
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

    // =========================
    // Async directory/meta APIs
    // =========================
    pub fn jindo_filesystem_mkdir_async(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_rename_async(
        fs: *mut c_void,
        oldpath: *const c_char,
        newpath: *const c_char,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_remove_async(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_exists_async(
        fs: *mut c_void,
        path: *const c_char,
        cb: Option<
            extern "C" fn(status: JindoStatus, value: bool, err: *const c_char, userdata: *mut c_void),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;

    pub fn jindo_file_info_free(info: *mut JindoFileInfo);

    pub fn jindo_filesystem_get_file_info_async(
        fs: *mut c_void,
        path: *const c_char,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                info: *mut JindoFileInfo,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;

    pub fn jindo_list_result_free(result: *mut JindoListResult);

    pub fn jindo_filesystem_list_dir_async(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                result: *mut JindoListResult,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;

    #[allow(dead_code)]
    pub fn jindo_filesystem_get_content_summary_async(
        fs: *mut c_void,
        path: *const c_char,
        recursive: bool,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                summary: *const JindoContentSummary,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;

    pub fn jindo_filesystem_set_permission_async(
        fs: *mut c_void,
        path: *const c_char,
        perm: i16,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_set_owner_async(
        fs: *mut c_void,
        path: *const c_char,
        user: *const c_char,
        group: *const c_char,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;

    pub fn jindo_writer_free(writer: *mut c_void);

    pub fn jindo_reader_free(reader: *mut c_void);

    // =========================
    // Async writer/reader APIs
    // =========================
    pub fn jindo_filesystem_open_writer_async(
        fs: *mut c_void,
        path: *const c_char,
        cb: Option<extern "C" fn(status: JindoStatus, writer: *mut c_void, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_open_writer_append_async(
        fs: *mut c_void,
        path: *const c_char,
        cb: Option<extern "C" fn(status: JindoStatus, writer: *mut c_void, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_open_reader_async(
        fs: *mut c_void,
        path: *const c_char,
        cb: Option<extern "C" fn(status: JindoStatus, reader: *mut c_void, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;

    pub fn jindo_writer_write_async(
        writer: *mut c_void,
        data: *const u8,
        len: usize,
        cb: Option<extern "C" fn(status: JindoStatus, value: i64, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_writer_flush_async(
        writer: *mut c_void,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_writer_tell_async(
        writer: *mut c_void,
        cb: Option<extern "C" fn(status: JindoStatus, value: i64, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_writer_close_async(
        writer: *mut c_void,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;

    pub fn jindo_reader_read_async(
        reader: *mut c_void,
        n: usize,
        scratch: *mut u8,
        cb: Option<extern "C" fn(status: JindoStatus, value: i64, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_pread_async(
        reader: *mut c_void,
        offset: i64,
        n: usize,
        scratch: *mut u8,
        cb: Option<extern "C" fn(status: JindoStatus, value: i64, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_seek_async(
        reader: *mut c_void,
        offset: i64,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_tell_async(
        reader: *mut c_void,
        cb: Option<extern "C" fn(status: JindoStatus, value: i64, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_get_file_length_async(
        reader: *mut c_void,
        cb: Option<extern "C" fn(status: JindoStatus, value: i64, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_close_async(
        reader: *mut c_void,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;

    // Error handling
    pub fn jindo_get_last_error() -> *const c_char;

    // Generic free for heap memory allocated by the C++ shim (malloc).
    pub fn jindo_free(p: *mut c_void);
}