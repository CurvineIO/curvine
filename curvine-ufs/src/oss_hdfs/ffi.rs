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

use orpc::sys::RawPtr;
use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

/// Fetch the last error message reported by the JindoSDK C++ shim.
///
/// This is a thin convenience helper around `jindo_get_last_error()` to avoid
/// repeating unsafe string conversions at every call site.
#[inline]
pub(crate) fn jindo_last_error() -> String {
    unsafe {
        let err_ptr = jindo_get_last_error();
        if err_ptr.is_null() {
            String::from("Unknown error")
        } else {
            CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
        }
    }
}

// Opaque handle types
// We wrap raw pointers with `RawPtr` for FFI ergonomics and to make Send/Sync explicit.
// NOTE: ownership of these pointers is managed by the JindoSDK C++ shim (freed via `jindo_*_free`),
// so we always create `RawPtr` with `owned = false`.
#[derive(Clone, Debug)]
pub struct JindoFileSystemHandle(pub RawPtr<c_void>);
#[derive(Clone, Debug)]
pub struct JindoWriterHandle(pub RawPtr<c_void>);
#[derive(Clone, Debug)]
pub struct JindoReaderHandle(pub RawPtr<c_void>);
#[derive(Clone, Debug)]
pub struct JindoConfigHandle(pub RawPtr<c_void>);

// NOTE:
// These handle newtypes inherit auto-traits (`Send`/`Sync`) from `RawPtr<c_void>`.
// We intentionally do not add explicit `unsafe impl Send/Sync` here to avoid
// duplicating unsafe promises in multiple places.

impl JindoFileSystemHandle {
    #[inline]
    pub fn from_raw(p: *mut c_void) -> Self {
        Self(RawPtr::from_raw(p as *const c_void))
    }

    #[inline]
    pub fn null() -> Self {
        Self::from_raw(std::ptr::null_mut())
    }

    pub fn is_null(&self) -> bool {
        self.as_raw().is_null()
    }

    #[inline]
    pub fn as_raw(&self) -> *mut c_void {
        self.0.as_mut_ptr()
    }
}

impl JindoWriterHandle {
    #[inline]
    pub fn from_raw(p: *mut c_void) -> Self {
        Self(RawPtr::from_raw(p as *const c_void))
    }

    pub fn is_null(&self) -> bool {
        self.as_raw().is_null()
    }

    #[inline]
    pub fn as_raw(&self) -> *mut c_void {
        self.0.as_mut_ptr()
    }
}

impl JindoReaderHandle {
    #[inline]
    pub fn from_raw(p: *mut c_void) -> Self {
        Self(RawPtr::from_raw(p as *const c_void))
    }

    pub fn is_null(&self) -> bool {
        self.as_raw().is_null()
    }

    #[inline]
    pub fn as_raw(&self) -> *mut c_void {
        self.0.as_mut_ptr()
    }
}

impl JindoConfigHandle {
    #[inline]
    pub fn from_raw(p: *mut c_void) -> Self {
        Self(RawPtr::from_raw(p as *const c_void))
    }

    #[inline]
    pub fn as_raw(&self) -> *mut c_void {
        self.0.as_mut_ptr()
    }
}

// Status codes
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JindoStatus {
    Ok = 0,
    // Kept for ABI completeness with the C++ shim even if currently unused by Rust call sites.
    #[allow(dead_code)]
    Error = 1,
    FileNotFound = 2,
    // Kept for ABI completeness with the C++ shim even if currently unused by Rust call sites.
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
// Kept for ABI completeness with the C++ shim even if currently unused by Rust call sites.
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
    pub fn jindo_config_set_string(config: *mut c_void, key: *const c_char, value: *const c_char);
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
            extern "C" fn(
                status: JindoStatus,
                value: bool,
                err: *const c_char,
                userdata: *mut c_void,
            ),
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
    // Kept for ABI completeness with the C++ shim even if currently unused by Rust call sites.
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
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                writer: *mut c_void,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_open_writer_append_async(
        fs: *mut c_void,
        path: *const c_char,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                writer: *mut c_void,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_filesystem_open_reader_async(
        fs: *mut c_void,
        path: *const c_char,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                reader: *mut c_void,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;

    pub fn jindo_writer_write_async(
        writer: *mut c_void,
        data: *const u8,
        len: usize,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                value: i64,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_writer_flush_async(
        writer: *mut c_void,
        cb: Option<extern "C" fn(status: JindoStatus, err: *const c_char, userdata: *mut c_void)>,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_writer_tell_async(
        writer: *mut c_void,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                value: i64,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
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
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                value: i64,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_pread_async(
        reader: *mut c_void,
        offset: i64,
        n: usize,
        scratch: *mut u8,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                value: i64,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
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
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                value: i64,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
        userdata: *mut c_void,
    ) -> JindoStatus;
    pub fn jindo_reader_get_file_length_async(
        reader: *mut c_void,
        cb: Option<
            extern "C" fn(
                status: JindoStatus,
                value: i64,
                err: *const c_char,
                userdata: *mut c_void,
            ),
        >,
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
