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

#![allow(unused_variables)]

use crate::*;
use curvine_common::state::{FileStatus, FileType};
use orpc::io::IOResult;
use orpc::sys;
use orpc::sys::{FFIUtils, RawIO};
use std::process::Command;
use std::slice;
use tokio_util::bytes::BytesMut;

pub struct FuseUtils;

impl FuseUtils {
    pub fn struct_as_bytes<T>(dst: &T) -> &[u8] {
        let len = size_of::<T>();
        let ptr = dst as *const T as *const u8;
        unsafe { slice::from_raw_parts(ptr, len) }
    }

    pub fn struct_as_buf<T>(dst: &T) -> BytesMut {
        let bytes = Self::struct_as_bytes(dst);
        BytesMut::from(bytes)
    }

    pub fn get_kernel_version() -> f32 {
        let output = Command::new("uname")
            .arg("-r")
            .output()
            .expect("Failed to execute 'uname -r'");

        // Convert output to string and remove the line break at the end
        let kernel_ver = String::from_utf8_lossy(&output.stdout).trim().to_string();

        let parts: Vec<&str> = kernel_ver.split('.').collect();
        if parts.len() < 2 {
            1f32
        } else {
            format!("{}.{}", parts[0], parts[1]).parse::<f32>().unwrap()
        }
    }

    pub fn get_mode(perm: u32, typ: FileType) -> u32 {
        match typ {
            FileType::Dir => perm | (libc::S_IFDIR as u32),

            #[cfg(target_os = "linux")]
            FileType::Link => FUSE_DEFAULT_MODE | (libc::S_IFLNK as u32),

            _ => perm | (libc::S_IFREG as u32),
        }
    }

    pub fn s_isreg(mode: u32) -> bool {
        #[cfg(target_os = "linux")]
        {
            ((mode) & libc::S_IFMT) == libc::S_IFREG
        }

        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    // Determine whether it is the running file type.
    // Currently, the file system only supports: files and directories.
    pub fn is_allow_file_type(mode: u32) -> bool {
        let file_type = mode & libc::S_IFMT as u32;

        file_type == libc::S_IFREG as u32 || file_type == libc::S_IFDIR as u32
    }

    pub fn has_truncate(flags: u32) -> bool {
        ((flags as i32) & libc::O_TRUNC) != 0
    }

    pub fn has_append(flags: u32) -> bool {
        ((flags as i32) & libc::O_APPEND) != 0
    }

    pub fn has_create(flags: u32) -> bool {
        ((flags as i32) & libc::O_CREAT) != 0
    }

    // Determine whether it is the running file type.
    // Currently, the file system only supports: files and directories.
    pub fn is_dir(mode: u32) -> bool {
        let file_type = mode & libc::S_IFMT as u32;
        file_type == libc::S_IFDIR as u32
    }

    pub fn aligned_sub_buf(buf: &mut [u8], alignment: usize) -> &mut [u8] {
        let off = alignment - (buf.as_ptr() as usize) % alignment;
        if off == alignment {
            buf
        } else {
            &mut buf[off..]
        }
    }

    // In fuse 2, this default size is 132kb (128 + 4)
    pub fn get_fuse_buf_size() -> usize {
        let page_size = sys::get_pagesize().unwrap_or(FUSE_DEFAULT_PAGE_SIZE);
        FUSE_MAX_MAX_PAGES * page_size + FUSE_IN_HEADER_LEN
    }

    fn fuse_dev_ioc_clone() -> u64 {
        #[cfg(not(target_os = "linux"))]
        {
            panic!("unsupported operation")
        }

        #[cfg(target_os = "linux")]
        {
            use nix;
            // request_code_read! macro is equivalent to linux _IOR macro
            nix::request_code_read!(229, 0, 4)
        }
    }

    // Device ioctls
    // #define FUSE_DEV_IOC_MAGIC 229
    // #define FUSE_DEV_IOC_CLONE _IOR(FUSE_DEV_IOC_MAGIC, 0, uint32_t)
    // fd clone is a new feature added to the Linux kernel 4.2 or above version, and its main function is to reduce competition for fd locks.
    pub fn fuse_clone_fd(source_fd: RawIO) -> IOResult<RawIO> {
        let path = FFIUtils::new_cs_string(FUSE_DEVICE_NAME);
        let clone_fd = sys::open(&path, libc::O_RDWR)?;

        let request = Self::fuse_dev_ioc_clone();
        let mut master_fd = source_fd;
        sys::ioctl(
            clone_fd,
            request,
            &mut master_fd as *mut _ as *mut libc::c_void,
        )?;

        Ok(clone_fd)
    }

    pub fn fuse_st_size(status: &FileStatus) -> u64 {
        match status.file_type {
            FileType::Link => status.target.as_ref().map(|x| x.len()).unwrap_or(0) as u64,
            FileType::Dir => FUSE_DEFAULT_PAGE_SIZE as u64,
            _ => status.len as u64,
        }
    }
}
