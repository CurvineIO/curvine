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

#![allow(clippy::not_unsafe_ptr_arg_deref, unused)]

use crate::io::IOResult;
use crate::sys::{CChar, CStr, CString};
use std::ffi::OsStr;

pub struct FFIUtils;

impl FFIUtils {
    pub fn ptr_to_string(ptr: *const CChar) -> IOResult<String> {
        if ptr.is_null() {
            return Ok("".to_string());
        }

        let str = unsafe { CStr::from_ptr(ptr).to_str()? };
        Ok(str.to_string())
    }

    // iAfter into_raw, Rust will not recycle memory and needs to be manually released by itself
    pub fn string_to_ptr(str: impl AsRef<str>) -> IOResult<*mut CChar> {
        let string = CString::new(str.as_ref())?;
        Ok(string.into_raw())
    }

    // Convert a bare pointer array to rust slice
    pub fn ptr_to_slice<'a, T>(buf: *mut T, len: usize) -> &'a mut [T] {
        let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
        slice
    }

    // Convert the rust instance to a naked pointer and return this naked pointer to another language.
    // Note: rust will not be recycled in the instance, and it is necessary to call free_native_handle to release it manually.
    pub fn into_raw_ptr<T>(s: T) -> i64 {
        Box::into_raw(Box::new(s)) as i64
    }

    /// Frees a raw pointer allocated by Rust's allocator.
    ///
    /// # Safety
    ///
    /// This function is unsafe because improper use may lead to:
    /// - Double-free: If the pointer has already been freed.
    /// - Use-after-free: If the pointer is used after calling this function.
    /// - Invalid pointer: If the pointer was not allocated by Rust's allocator.
    ///
    /// ## Caller's Responsibility
    /// - The pointer must be allocated by Rust's global allocator (e.g., `Box::into_raw`).
    /// - The pointer must not be used after this call.
    /// - This function must not be called more than once for the same pointer.
    pub unsafe fn free_raw_ptr<T>(h: *mut T) {
        drop(Box::from_raw(h));
    }

    pub fn get_os_str(source: &[u8]) -> &OsStr {
        #[cfg(target_os = "linux")]
        {
            use std::ffi::OsStr;
            use std::os::unix::ffi::OsStrExt;
            OsStr::from_bytes(source)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let str = std::str::from_utf8(source).unwrap();
            OsStr::new(str)
        }
    }

    pub fn get_os_bytes(source: &str) -> &[u8] {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::ffi::OsStrExt;
            let source = Self::get_os_str(source.as_bytes());
            source.as_bytes()
        }

        #[cfg(not(target_os = "linux"))]
        {
            source.as_bytes()
        }
    }

    // Create a CString, and the failure triggers the panic; failure is a very small probability event, and you don’t worry about causing unknown situations.
    pub fn new_cs_string<T: Into<Vec<u8>>>(str: T) -> CString {
        CString::new(str).expect("error create CString")
    }
}
