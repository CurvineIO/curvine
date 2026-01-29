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

#![allow(clippy::unnecessary_cast)]

use crate::raw::fuse_abi::{fuse_in_header, fuse_out_header};
use once_cell::sync::Lazy;

pub mod fs;
pub mod macros;
pub mod raw;
pub mod session;
pub mod web_server;

mod fuse_error;

pub use self::fuse_error::FuseError;

mod fuse_utils;
pub use self::fuse_utils::FuseUtils;

pub type FuseResult<T> = Result<T, FuseError>;

// fuse3 session type
pub type RawSession = *mut core::ffi::c_void;

pub const FUSE_DEVICE_NAME: &str = "/dev/fuse";

pub const FUSE_NAME: &str = "curvine-fuse";

pub const FUSE_IN_HEADER_LEN: usize = size_of::<fuse_in_header>();

pub const FUSE_SUCCESS: i32 = 0;

pub const FUSE_OUT_HEADER_LEN: usize = size_of::<fuse_out_header>();

pub const FILE_HANDLE_READ_BIT: u64 = 1 << 63;

pub const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;

pub const FUSE_ROOT_ID: u64 = 1;

// curvine root node id.
pub const FS_ROOT_ID: i64 = 1000;

pub const FUSE_PATH_SEPARATOR: &str = "/";

pub const FUSE_BLOCK_SIZE: u64 = 65536;

pub const FUSE_KERNEL_VERSION: u32 = 7;

pub const FUSE_KERNEL_MINOR_VERSION: u32 = 31;

pub const FUSE_MAX_MAX_PAGES: usize = 256;

pub const FUSE_BUFFER_HEADER_SIZE: usize = 0x1000; // 4096

pub const FUSE_DEFAULT_PAGE_SIZE: usize = 4096;

pub const FUSE_MAX_PAGES: u32 = 1 << 22;

pub const FUSE_BIG_WRITES: u32 = 1 << 5;

pub const FUSE_ASYNC_READ: u32 = 1 << 0;

pub const FUSE_SPLICE_WRITE: u32 = 1 << 7;

pub const FUSE_SPLICE_MOVE: u32 = 1 << 8;

pub const FUSE_SPLICE_READ: u32 = 1 << 9;

pub const FUSE_ASYNC_DIO: u32 = 1 << 15;

pub const FUSE_DO_READDIRPLUS: u32 = 1 << 13;

pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14;

pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16;

pub const FUSE_POSIX_ACL: u32 = 1 << 20;

pub const FUSE_DO_RENAME2: u32 = 1 << 11;

pub const FUSE_MAX_NAME_LENGTH: usize = 255;

pub const FUSE_UNKNOWN_INODES: u64 = 0xffffffff;

pub const FUSE_MAX_BACKGROUND: u16 = 16;

pub const FUSE_CURRENT_DIR: &str = ".";

pub const FUSE_PARENT_DIR: &str = "..";

pub const FUSE_S_ISUID: u32 = 0x800;

pub const FUSE_S_ISGID: u32 = 0x400;

// Default file permission code
pub const FUSE_DEFAULT_MODE: u32 = 0o777;

pub const FUSE_DEFAULT_UMASK: u32 = 0o022;

pub const FUSE_UNKNOWN_INO: u64 = 0xffffffff;

pub const FUSE_FOPEN_DIRECT_IO: u32 = 1 << 0;

pub const FUSE_FOPEN_KEEP_CACHE: u32 = 1 << 1;

pub const FUSE_FOPEN_NONSEEKABLE: u32 = 1 << 2;

pub const FUSE_FOPEN_CACHE_DIR: u32 = 1 << 3;

pub const FUSE_FOPEN_STREAM: u32 = 1 << 4;

pub const FUSE_FOPEN_NOFLUSH: u32 = 1 << 5;

pub const FUSE_FOPEN_PARALLEL_DIRECT_WRITES: i32 = 1 << 6;

// FUSE setattr valid bit flags (aligned with linux/fs/fuse definitions)
pub const FATTR_MODE: u32 = 1 << 0;

pub const FATTR_UID: u32 = 1 << 1;

pub const FATTR_GID: u32 = 1 << 2;

pub const FATTR_SIZE: u32 = 1 << 3;

pub const FATTR_ATIME: u32 = 1 << 4;

pub const FATTR_MTIME: u32 = 1 << 5;

pub const FATTR_ATIME_NOW: u32 = 1 << 7;

pub const FATTR_MTIME_NOW: u32 = 1 << 8;

// The minimum version of the clone fd feature can be used.
pub const FUSE_CLONE_FD_MIN_VERSION: f32 = 4.2f32;

pub const FUSE_NOTIFY_UNIQUE: u64 = 0;

pub const STATE_FILE_MAGIC: &[u8; 4] = b"cvfs";

pub const STATE_FILE_VERSION: u64 = 1;

pub static UNIX_KERNEL_VERSION: Lazy<f32> = Lazy::new(FuseUtils::get_kernel_version);
