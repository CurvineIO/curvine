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

pub type SysResult<T> = std::io::Result<T>;

// `Other` is reserved for opaque failures; pass an explicit `ErrorKind` whenever the
// failure has a category callers can branch on (unsupported platform, bad argument, EOF).
macro_rules! sys_error {
    ($kind:path, $message:literal) => {
        Err(::std::io::Error::new($kind, $message))
    };
    ($kind:path, $format:literal, $($arg:expr),+ $(,)?) => {
        Err(::std::io::Error::new($kind, format!($format, $($arg),+)))
    };
    ($message:expr) => {
        Err(::std::io::Error::other($message))
    };
    ($format:expr, $($arg:expr),+ $(,)?) => {
        Err(::std::io::Error::other(format!($format, $($arg),+)))
    };
}

// Fully qualified paths keep the expansion independent of what the call site imports.
macro_rules! sys_call {
    ($expression:expr) => {{
        let result = $expression;
        if result as $crate::sys::CInt <= $crate::sys::ERRNO_SENTINEL {
            Err(::std::io::Error::last_os_error())
        } else {
            Ok(result as $crate::sys::CInt)
        }
    }};
}

/// Build a new iovec that skips the first `offset` bytes of `iov`, so a partially
/// completed vectored transfer can be resumed with the remaining bytes only.
pub fn skip_iov_bytes<'a>(
    iov: &'a [std::io::IoSlice<'a>],
    mut offset: usize,
) -> Vec<std::io::IoSlice<'a>> {
    let mut result = Vec::with_capacity(iov.len());
    for slice in iov {
        if offset == 0 {
            result.push(std::io::IoSlice::new(&slice[..]));
        } else if slice.len() <= offset {
            offset -= slice.len();
        } else {
            result.push(std::io::IoSlice::new(&slice[offset..]));
            offset = 0;
        }
    }
    result
}

mod sys_libc;
pub use self::sys_libc::*;

mod cache_manager;
pub use self::cache_manager::*;

mod data_slice;
pub use self::data_slice::DataSlice;

pub mod pipe;

mod fs_stats;
pub use self::fs_stats::FsStats;

mod raw_ptr;
pub use self::raw_ptr::RawPtr;

mod ffi_utils;
pub use self::ffi_utils::FFIUtils;

mod raw_vec;
pub use self::raw_vec::RawVec;

mod raw_io_slice;
pub use self::raw_io_slice::RawIOSlice;

mod sys_utils;
pub use self::sys_utils::SysUtils;

mod signal;
pub use self::signal::*;

pub type CInt = std::ffi::c_int;

pub type CLong = std::ffi::c_longlong;

pub type CSize = libc::size_t;

pub type CChar = std::ffi::c_char;

pub type CVoid = std::ffi::c_void;

pub type CString = std::ffi::CString;

pub type CStr = std::ffi::CStr;

// The native I/O type of the operating system.
pub type RawIO = i32;

pub const ERRNO_SENTINEL: i32 = -1;

// Offset type, default is i32, in 64, the system is i64
// When converting the offset to a bare pointer, if the type is biased, it will cause the offset to be not read correctly, resulting in a strange exception.
// Define these types and report an error in advance when a type mismatch occurs.
pub type OffType = libc::off_t;
pub type SizeType = libc::size_t;

#[cfg(not(target_os = "linux"))]
pub const PIPE_BUF: usize = 4096;

#[cfg(target_os = "linux")]
pub const PIPE_BUF: usize = libc::PIPE_BUF;

pub const ST_BLOCK_SIZE: u64 = 512;
