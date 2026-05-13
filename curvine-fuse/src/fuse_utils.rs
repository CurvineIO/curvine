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

use crate::fs::operator::{Create, MkDir};
use crate::raw::fuse_abi::{fuse_attr, fuse_entry_out, fuse_setattr_in};
use crate::*;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::FuseConf;
use curvine_common::fs::Path;
use curvine_common::state::{
    CreateFileOpts, CreateFileOptsBuilder, FileStatus, FileType, MkdirOpts, MkdirOptsBuilder,
    SetAttrOpts,
};
use orpc::common::LocalTime;
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

    pub fn is_dot(&self, name: &str) -> bool {
        name == FUSE_PARENT_DIR || name == FUSE_CURRENT_DIR
    }

    pub fn create_opts(op: &Create<'_>, fs: &UnifiedFileSystem) -> CreateFileOpts {
        let mut builder = CreateFileOptsBuilder::with_conf(&fs.conf().client);
        if op.arg.mode != 0 {
            builder = builder.acl(
                op.header.uid,
                op.header.gid,
                op.arg.mode & 0o7777 & !op.arg.umask,
            )
        }

        builder.build()
    }

    pub fn check_xattr(name: &str, read: bool) -> FuseResult<()> {
        // Handle system extended attributes FIRST, before any path resolution
        // This avoids unnecessary operations and provides fastest response
        // Kernel may still query these even if FUSE_POSIX_ACL is disabled in init response
        // Kernel requested POSIX_ACL support (kernel_requested_POSIX_ACL: 1048576)
        // but we disabled it in our response, yet kernel still queries ACL attributes
        match name {
            "security.capability"
            | "security.selinux"
            | "system.posix_acl_access"
            | "system.posix_acl_default" => {
                if read {
                    err_fuse!(libc::ENODATA, "get_xattr {}", name)
                } else {
                    err_fuse!(libc::EOPNOTSUPP, "set_xattr {}", name)
                }
            }
            _ => Ok(()),
        }
    }

    pub fn fill_open_flags(conf: &FuseConf, v: u32) -> u32 {
        let mut flags = v;
        if conf.direct_io {
            flags |= FUSE_FOPEN_DIRECT_IO;
        } else {
            flags |= FUSE_FOPEN_KEEP_CACHE;
        }
        if conf.cache_readdir {
            flags |= FUSE_FOPEN_CACHE_DIR
        }
        if conf.non_seekable {
            flags |= FUSE_FOPEN_NONSEEKABLE
        }

        flags
    }

    pub fn open_opts(fs: &UnifiedFileSystem) -> CreateFileOpts {
        CreateFileOptsBuilder::with_conf(&fs.conf().client).build()
    }

    pub fn status_to_attr(conf: &FuseConf, status: &FileStatus) -> FuseResult<fuse_attr> {
        let blocks = ((status.len + 511) / 512) as u64;

        let mtime_sec = (status.mtime.max(0) / 1000) as u64;
        let mtime_nsec = ((status.mtime.max(0) % 1000) * 1_000_000) as u32;

        let atime_sec = (status.atime.max(0) / 1000) as u64;
        let atime_nsec = ((status.atime.max(0) % 1000) * 1_000_000) as u32;

        let ctime_sec = mtime_sec;
        let ctime_nsec = mtime_nsec;

        let uid = if status.owner.is_empty() {
            conf.uid
        } else if let Ok(numeric_uid) = status.owner.parse::<u32>() {
            numeric_uid
        } else {
            match sys::get_uid_by_name(&status.owner) {
                Some(uid) => uid,
                None => conf.uid,
            }
        };

        let gid = if status.group.is_empty() {
            conf.gid
        } else if let Ok(numeric_gid) = status.group.parse::<u32>() {
            numeric_gid
        } else {
            match sys::get_gid_by_name(&status.group) {
                Some(gid) => gid,
                None => conf.gid,
            }
        };

        let mode = if status.mode != 0 {
            FuseUtils::get_mode(status.mode, status.file_type)
        } else {
            FuseUtils::get_mode(FUSE_DEFAULT_MODE & !conf.umask, status.file_type)
        };
        let size = FuseUtils::fuse_st_size(status);

        // For links, nlink should be greater than 1
        // Now we use the actual nlink from FileStatus
        let nlink = status.nlink;

        Ok(fuse_attr {
            ino: status.id as u64,
            size,
            blocks,
            atime: atime_sec,
            mtime: mtime_sec,
            ctime: ctime_sec,
            atimensec: atime_nsec,
            mtimensec: mtime_nsec,
            ctimensec: ctime_nsec,
            mode,
            nlink,
            uid,
            gid,
            rdev: 0,
            blksize: FUSE_BLOCK_SIZE as u32,
            padding: 0,
        })
    }

    pub fn mkdir_opts(op: &MkDir<'_>, fs: &UnifiedFileSystem) -> MkdirOpts {
        let mut builder = MkdirOptsBuilder::with_conf(&fs.conf().client);
        if op.arg.mode != 0 {
            builder = builder.acl(
                op.header.uid,
                op.header.gid,
                op.arg.mode & 0o7777 & !op.arg.umask,
            )
        }

        builder.build()
    }

    pub fn create_entry_out(conf: &FuseConf, attr: fuse_attr) -> fuse_entry_out {
        fuse_entry_out {
            nodeid: attr.ino,
            generation: 0,
            entry_valid: conf.entry_ttl.as_secs(),
            attr_valid: conf.attr_ttl.as_secs(),
            entry_valid_nsec: conf.entry_ttl.subsec_nanos(),
            attr_valid_nsec: conf.attr_ttl.subsec_nanos(),
            attr,
        }
    }

    pub fn new_dot_status(name: &str) -> FileStatus {
        FileStatus::with_name(FUSE_UNKNOWN_INO as i64, name.to_string(), true)
    }

    pub fn fuse_setattr_to_opts(setattr: &fuse_setattr_in) -> FuseResult<SetAttrOpts> {
        // Only set fields when the corresponding valid flag is present
        let owner = if (setattr.valid & FATTR_UID) != 0 {
            match sys::get_username_by_uid(setattr.uid) {
                Some(username) => Some(username),
                None => Some(setattr.uid.to_string()),
            }
        } else {
            None
        };

        let group = if (setattr.valid & FATTR_GID) != 0 {
            match sys::get_groupname_by_gid(setattr.gid) {
                Some(groupname) => Some(groupname),
                None => Some(setattr.gid.to_string()),
            }
        } else {
            None
        };

        // Strip file type bits; keep only permission and special bits
        let mode = if (setattr.valid & FATTR_MODE) != 0 {
            Some(setattr.mode & 0o7777)
        } else {
            None
        };

        // Handle time modifications
        let mut atime = None;
        let mut mtime = None;

        if (setattr.valid & FATTR_ATIME) != 0 {
            atime = Some((setattr.atime * 1000) as i64);
        } else if (setattr.valid & FATTR_ATIME_NOW) != 0 {
            atime = Some(LocalTime::mills() as i64);
        }

        if (setattr.valid & FATTR_MTIME) != 0 {
            mtime = Some((setattr.mtime * 1000) as i64);
        } else if (setattr.valid & FATTR_MTIME_NOW) != 0 {
            mtime = Some(LocalTime::mills() as i64);
        }

        Ok(SetAttrOpts {
            owner,
            group,
            mode,
            atime,
            mtime,
            ..Default::default()
        })
    }

    pub fn file_opts_to_status(path: &Path, opts: &CreateFileOpts) -> FileStatus {
        let time = LocalTime::mills() as i64;
        FileStatus {
            path: path.clone_uri(),
            name: path.name().to_owned(),
            is_dir: false,
            file_type: FileType::File,
            atime: time,
            mtime: time,
            mode: opts.mode.to_owned(),
            owner: opts.owner.to_owned(),
            group: opts.group.to_owned(),
            nlink: 1,
            ..Default::default()
        }
    }
}
