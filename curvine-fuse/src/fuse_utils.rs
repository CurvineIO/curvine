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

use crate::fs::operator::{Create, MkDir, MkNod};
use crate::raw::fuse_abi::{
    fuse_attr, fuse_entry_out, fuse_ioctl_iovec, fuse_ioctl_out, fuse_setattr_in, FUSE_IOCTL_RETRY,
};
use crate::*;
use bytes::BytesMut;
use curvine_client::unified::UnifiedFileSystem;
use curvine_config::FuseConf;
use curvine_fs_api::Path;
use curvine_io::IOResult;
use curvine_model::{
    CreateFileOpts, CreateFileOptsBuilder, FileStatus, FileType, MkdirOpts, MkdirOptsBuilder,
    SetAttrOpts, FS_APPEND_FL, FS_IMMUTABLE_FL, IFLAGS_XATTR, MKNOD_RDEV_XATTR,
};
use curvine_runtime::common::LocalTime;
use curvine_sys as sys;
use curvine_sys::{FFIUtils, RawIO};
use std::collections::HashMap;
use std::process::Command;
use std::slice;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XattrOp {
    Get,
    Set,
    Remove,
}

pub struct FuseUtils;

impl FuseUtils {
    /// Reinterpret a value as its raw bytes for writing to the FUSE device.
    ///
    /// SAFETY / usage contract: `T` MUST be a FUSE C-ABI (`#[repr(C)]`) struct.
    /// `slice::from_raw_parts` reads all `size_of::<T>()` bytes verbatim,
    /// *including padding* — so the caller must ensure `T` has no uninitialized
    /// padding (all current FUSE ABI structs zero their `padding` fields, e.g.
    /// `fuse_attr` in `status_to_attr`). Passing a non-`repr(C)` type, or one
    /// with pointers / uninit fields, risks ABI mismatch and info leakage.
    /// Kept crate-private so only FUSE ABI structs reach it.
    pub(crate) fn struct_as_bytes<T>(dst: &T) -> &[u8] {
        let len = size_of::<T>();
        let ptr = dst as *const T as *const u8;
        unsafe { slice::from_raw_parts(ptr, len) }
    }

    /// Owned-buffer variant of [`Self::struct_as_bytes`]; same C-ABI contract applies.
    pub(crate) fn struct_as_buf<T>(dst: &T) -> BytesMut {
        let bytes = Self::struct_as_bytes(dst);
        BytesMut::from(bytes)
    }

    pub fn get_kernel_version() -> (u32, u32) {
        let Ok(output) = Command::new("uname").arg("-r").output() else {
            return (0, 0);
        };
        if !output.status.success() {
            return (0, 0);
        }

        let kernel_release = String::from_utf8_lossy(&output.stdout);
        Self::parse_kernel_version(kernel_release.trim()).unwrap_or((0, 0))
    }

    fn parse_kernel_version(kernel_release: &str) -> Option<(u32, u32)> {
        let mut parts = kernel_release.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        Some((major, minor))
    }

    // Per-type default permission bits for synthetic entries. File / Stream /
    // Agg / Object all count as regular files.
    fn default_mode(typ: FileType) -> u32 {
        match typ {
            FileType::Dir => FUSE_DEFAULT_DIR_MODE,
            FileType::Link => FUSE_DEFAULT_SYMLINK_MODE,
            _ => FUSE_DEFAULT_FILE_MODE,
        }
    }

    // Decide reported permission bits, including symlink and synthetic `.`/`..` defaults.
    pub fn effective_perm(status: &FileStatus, umask: u32) -> u32 {
        if status.file_type == FileType::Link {
            return FUSE_DEFAULT_SYMLINK_MODE;
        }

        if status.id == FUSE_UNKNOWN_INO as i64 && FuseUtils.is_dot(&status.name) {
            Self::default_mode(status.file_type) & !umask
        } else {
            status.mode & 0o7777
        }
    }

    pub fn get_mode(perm: u32, typ: FileType) -> u32 {
        let perm = perm & 0o7777;
        match typ {
            FileType::Dir => perm | (libc::S_IFDIR as u32),
            #[cfg(target_os = "linux")]
            FileType::Link => perm | (libc::S_IFLNK as u32),
            #[cfg(target_os = "linux")]
            FileType::Fifo => perm | (libc::S_IFIFO as u32),
            #[cfg(target_os = "linux")]
            FileType::Char => perm | (libc::S_IFCHR as u32),
            #[cfg(target_os = "linux")]
            FileType::Block => perm | (libc::S_IFBLK as u32),
            #[cfg(target_os = "linux")]
            FileType::Socket => perm | (libc::S_IFSOCK as u32),
            _ => perm | (libc::S_IFREG as u32),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn special_file_type_from_mode(mode: u32) -> Option<FileType> {
        match mode & libc::S_IFMT as u32 {
            t if t == libc::S_IFIFO as u32 => Some(FileType::Fifo),
            t if t == libc::S_IFCHR as u32 => Some(FileType::Char),
            t if t == libc::S_IFBLK as u32 => Some(FileType::Block),
            t if t == libc::S_IFSOCK as u32 => Some(FileType::Socket),
            _ => None,
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn special_file_type_from_mode(_mode: u32) -> Option<FileType> {
        None
    }

    pub fn rdev_from_status(status: &FileStatus) -> u32 {
        status
            .x_attr
            .get(MKNOD_RDEV_XATTR)
            .and_then(|bytes| {
                let arr: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
                Some(u32::from_le_bytes(arr))
            })
            .unwrap_or(0)
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

    // The file system supports regular files and directories only.
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

    pub fn is_dir(mode: u32) -> bool {
        let file_type = mode & libc::S_IFMT as u32;
        file_type == libc::S_IFDIR as u32
    }

    // In fuse 2, this default size is 132kb (128 + 4)
    pub fn get_fuse_buf_size() -> usize {
        let page_size = sys::get_pagesize().unwrap_or(FUSE_DEFAULT_PAGE_SIZE);
        FUSE_MAX_MAX_PAGES * page_size + FUSE_BUFFER_HEADER_SIZE
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

    // FUSE_DEV_IOC_CLONE clones a FUSE fd to reduce fd-lock contention.
    pub fn fuse_clone_fd(source_fd: RawIO) -> IOResult<RawIO> {
        let path = FFIUtils::new_cs_string(FUSE_DEVICE_NAME);
        let clone_fd = sys::open(&path, libc::O_RDWR)?;
        Self::clone_fd_ioctl(clone_fd, source_fd)
    }

    // Close `clone_fd` on ioctl failure so fallback dup does not leak it.
    pub(crate) fn clone_fd_ioctl(clone_fd: RawIO, source_fd: RawIO) -> IOResult<RawIO> {
        let request = Self::fuse_dev_ioc_clone();
        let mut master_fd = source_fd;
        if let Err(e) = sys::ioctl(
            clone_fd,
            request,
            &mut master_fd as *mut _ as *mut libc::c_void,
        ) {
            if let Err(ce) = sys::close(clone_fd) {
                log::warn!("fuse_clone_fd: close leaked fd {} failed: {}", clone_fd, ce);
            }
            return Err(e.into());
        }

        Ok(clone_fd)
    }

    pub fn fuse_st_size(status: &FileStatus) -> FuseResult<u64> {
        let size = match status.file_type {
            FileType::Link => status.target.as_ref().map(|x| x.len()).unwrap_or(0) as u64,
            FileType::Dir => FUSE_DEFAULT_PAGE_SIZE as u64,
            FileType::Fifo | FileType::Char | FileType::Block | FileType::Socket => 0,
            // Reject negative backend lengths before converting regular-file size.
            _ => {
                if status.len < 0 {
                    return err_fuse!(
                        libc::EINVAL,
                        "negative file length {} for ino {}",
                        status.len,
                        status.id
                    );
                }
                status.len as u64
            }
        };
        Ok(size)
    }

    pub fn is_dot(&self, name: &str) -> bool {
        name == FUSE_PARENT_DIR || name == FUSE_CURRENT_DIR
    }

    pub fn create_opts(op: &Create<'_>, fs: &UnifiedFileSystem) -> CreateFileOpts {
        CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .acl(
                op.header.uid,
                op.header.gid,
                op.arg.mode & 0o7777 & !op.arg.umask,
            )
            .build()
    }

    /// Sticky-directory hard-link rule: caller must own the source file or the
    /// destination directory (Linux vfs_link / may_link semantics).
    pub fn check_sticky_hardlink(
        check_permission: bool,
        caller_uid: u32,
        dest_dir_uid: u32,
        source_file_uid: u32,
        dest_dir_mode: u32,
    ) -> FuseResult<()> {
        if !check_permission || caller_uid == 0 {
            return Ok(());
        }
        if dest_dir_mode & libc::S_ISVTX as u32 == 0 {
            return Ok(());
        }
        if caller_uid == dest_dir_uid || caller_uid == source_file_uid {
            return Ok(());
        }
        err_fuse!(
            libc::EPERM,
            "sticky directory hard link denied for uid {}",
            caller_uid
        )
    }

    pub fn check_xattr(name: &str, op: XattrOp) -> FuseResult<()> {
        if name.contains('\0') {
            return match op {
                XattrOp::Get => err_fuse!(libc::ENODATA, "get_xattr {}", name),
                XattrOp::Set => err_fuse!(libc::EOPNOTSUPP, "set_xattr {}", name),
                XattrOp::Remove => err_fuse!(libc::EOPNOTSUPP, "remove_xattr {}", name),
            };
        }

        // Handle system xattrs before path resolution; kernels may query them even without POSIX ACL.
        match name {
            MKNOD_RDEV_XATTR
            | IFLAGS_XATTR
            | "security.capability"
            | "security.selinux"
            | "system.posix_acl_access"
            | "system.posix_acl_default" => match op {
                XattrOp::Get => err_fuse!(libc::ENODATA, "get_xattr {}", name),
                XattrOp::Set => err_fuse!(libc::EOPNOTSUPP, "set_xattr {}", name),
                XattrOp::Remove => {
                    err_fuse!(libc::EOPNOTSUPP, "remove_xattr {}", name)
                }
            },
            _ => Ok(()),
        }
    }

    /// Linux `FS_IOC_GETFLAGS` on aarch64/x86_64.
    pub const FS_IOC_GETFLAGS: u32 = 0x8008_6601;

    /// Linux `FS_IOC_SETFLAGS` on aarch64/x86_64.
    pub const FS_IOC_SETFLAGS: u32 = 0x4008_6602;

    const FS_IOCTL_MUTABLE_FLAGS: u32 = FS_IMMUTABLE_FL | FS_APPEND_FL;

    pub fn user_xattr_supported(file_type: FileType) -> bool {
        matches!(file_type, FileType::File | FileType::Dir | FileType::Link)
    }

    pub fn check_user_xattr_namespace(file_type: FileType, name: &str) -> FuseResult<()> {
        if name.starts_with("user.") && !Self::user_xattr_supported(file_type) {
            return err_fuse!(libc::EPERM, "user xattr not permitted on {:?}", file_type);
        }
        Ok(())
    }

    pub fn file_flags_from_status(status: &FileStatus) -> u32 {
        status
            .x_attr
            .get(IFLAGS_XATTR)
            .and_then(|bytes| {
                let arr: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
                Some(u32::from_le_bytes(arr))
            })
            .unwrap_or(0)
    }

    pub fn file_has_immutable_or_append(status: &FileStatus) -> bool {
        let flags = Self::file_flags_from_status(status);
        flags & Self::FS_IOCTL_MUTABLE_FLAGS != 0
    }

    pub fn set_attr_for_file_flags(_status: &FileStatus, new_flags: u32) -> SetAttrOpts {
        let mut add_x_attr = HashMap::new();
        add_x_attr.insert(
            IFLAGS_XATTR.to_string(),
            Self::normalize_ioctl_file_flags(new_flags)
                .to_le_bytes()
                .to_vec(),
        );
        SetAttrOpts {
            add_x_attr,
            ..Default::default()
        }
    }

    pub fn normalize_ioctl_file_flags(flags: u32) -> u32 {
        flags & Self::FS_IOCTL_MUTABLE_FLAGS
    }

    pub fn ioctl_flag_bytes() -> usize {
        std::mem::size_of::<libc::c_long>()
    }

    pub fn decode_ioctl_file_flags(data: &[u8]) -> FuseResult<u32> {
        // Prefer native long width, but accept 4-byte payloads used by
        // FS_IOC32_* and by LTP helpers that store flags in an `int`.
        if data.len() >= 8 {
            Ok(i64::from_ne_bytes(data[..8].try_into().unwrap()) as u32)
        } else if data.len() >= 4 {
            Ok(u32::from_ne_bytes(data[..4].try_into().unwrap()))
        } else {
            err_fuse!(libc::EINVAL, "ioctl in buffer too small")
        }
    }

    pub fn append_ioctl_file_flags(buf: &mut BytesMut, flags: u32, out_size: u32) {
        if out_size == 0 {
            return;
        }
        let nbytes = Self::ioctl_flag_bytes();
        let want = out_size as usize;
        let encoded = (flags as libc::c_long).to_ne_bytes();
        let copy = want.min(nbytes).min(encoded.len());
        buf.extend_from_slice(&encoded[..copy]);
        if want > copy {
            buf.resize(buf.len() + (want - copy), 0);
        }
    }

    /// Build a `FUSE_IOCTL_RETRY` reply that asks the kernel to transfer
    /// `in_len`/`out_len` bytes at the userspace pointer `arg_ptr`.
    pub fn build_ioctl_retry(arg_ptr: u64, in_len: u64, out_len: u64) -> BytesMut {
        let in_iovs = u32::from(in_len > 0);
        let out_iovs = u32::from(out_len > 0);
        let out = fuse_ioctl_out {
            result: 0,
            flags: FUSE_IOCTL_RETRY,
            in_iovs,
            out_iovs,
        };
        let mut buf = BytesMut::from(Self::struct_as_bytes(&out));
        if in_len > 0 {
            let iov = fuse_ioctl_iovec {
                base: arg_ptr,
                len: in_len,
            };
            buf.extend_from_slice(Self::struct_as_bytes(&iov));
        }
        if out_len > 0 {
            let iov = fuse_ioctl_iovec {
                base: arg_ptr,
                len: out_len,
            };
            buf.extend_from_slice(Self::struct_as_bytes(&iov));
        }
        buf
    }

    pub fn file_open_flags(conf: &FuseConf, keep_cache: bool) -> u32 {
        let mut flags = 0;
        if conf.direct_io {
            flags |= FUSE_FOPEN_DIRECT_IO;
        } else if keep_cache {
            flags |= FUSE_FOPEN_KEEP_CACHE;
        } else if conf.open_direct_on_stale {
            flags |= FUSE_FOPEN_DIRECT_IO;
        }
        if conf.non_seekable {
            flags |= FUSE_FOPEN_NONSEEKABLE;
        }

        flags
    }

    pub fn dir_open_flags(conf: &FuseConf) -> u32 {
        let mut flags = Self::file_open_flags(conf, true);
        if conf.cache_readdir {
            flags |= FUSE_FOPEN_CACHE_DIR;
        }

        flags
    }

    pub fn open_opts(fs: &UnifiedFileSystem) -> CreateFileOpts {
        CreateFileOptsBuilder::with_conf(&fs.conf().client).build()
    }

    pub fn status_to_attr(conf: &FuseConf, status: &FileStatus) -> FuseResult<fuse_attr> {
        // Derive blocks from reported size so size and blocks stay consistent.
        let size = FuseUtils::fuse_st_size(status)?;
        let blocks = size.div_ceil(512);

        let mtime_sec = (status.mtime.max(0) / 1000) as u64;
        let mtime_nsec = ((status.mtime.max(0) % 1000) * 1_000_000) as u32;

        let atime_sec = (status.atime.max(0) / 1000) as u64;
        let atime_nsec = ((status.atime.max(0) % 1000) * 1_000_000) as u32;

        // Legacy/object-store statuses may not provide ctime yet. Falling back to mtime
        // preserves the previous behavior without hiding a real independent ctime.
        let ctime = status.ctime();
        let ctime_sec = (ctime.max(0) / 1000) as u64;
        let ctime_nsec = ((ctime.max(0) % 1000) * 1_000_000) as u32;

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

        let perm = FuseUtils::effective_perm(status, conf.umask);
        let mode = FuseUtils::get_mode(perm, status.file_type);
        let rdev = FuseUtils::rdev_from_status(status);

        // A regular file/dir has at least one link; some backends (UFS/object
        // store) do not populate nlink, leaving it 0. Report at least 1.
        let nlink = if status.nlink == 0 { 1 } else { status.nlink };

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
            rdev,
            blksize: FUSE_BLOCK_SIZE as u32,
            padding: 0,
        })
    }

    pub fn mkdir_opts(op: &MkDir<'_>, fs: &UnifiedFileSystem) -> MkdirOpts {
        MkdirOptsBuilder::with_conf(&fs.conf().client)
            .acl(
                op.header.uid,
                op.header.gid,
                op.arg.mode & 0o7777 & !op.arg.umask,
            )
            .build()
    }

    pub fn mknod_opts(
        op: &MkNod<'_>,
        fs: &UnifiedFileSystem,
        file_type: FileType,
    ) -> CreateFileOpts {
        let mode = op.arg.mode & 0o7777 & !op.arg.umask;
        CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .file_type(file_type)
            .acl(op.header.uid, op.header.gid, mode)
            .x_attr(
                MKNOD_RDEV_XATTR.to_string(),
                op.arg.rdev.to_le_bytes().to_vec(),
            )
            .build()
    }

    /// Kernel dentry/attribute cache lifetimes for FUSE replies.
    ///
    /// Returns the configured `entry_ttl`/`attr_ttl` regardless of
    /// `check_permission`, trading cache freshness for performance: inside the
    /// TTL the kernel reuses cached dentries/attrs, so a parent mode change
    /// (e.g. chmod removing search) is invisible until the cache expires, and
    /// path traversal may skip the userspace X_OK check where POSIX requires
    /// EACCES (LTP lstat02/stat03/readlink03).
    ///
    /// For strict/POSIX semantics (full LTP compliance), mount with
    /// `entry_timeout_ms = 0` and `attr_timeout_ms = 0` so every lookup/getattr
    /// revalidates against the metadata service.
    pub fn kernel_cache_timeouts(conf: &FuseConf) -> (u64, u32, u64, u32) {
        (
            conf.entry_ttl.as_secs(),
            conf.entry_ttl.subsec_nanos(),
            conf.attr_ttl.as_secs(),
            conf.attr_ttl.subsec_nanos(),
        )
    }

    pub fn create_entry_out(conf: &FuseConf, attr: fuse_attr) -> fuse_entry_out {
        let (entry_valid, entry_valid_nsec, attr_valid, attr_valid_nsec) =
            Self::kernel_cache_timeouts(conf);
        fuse_entry_out {
            nodeid: attr.ino,
            generation: 0,
            entry_valid,
            attr_valid,
            entry_valid_nsec,
            attr_valid_nsec,
            attr,
        }
    }

    pub fn new_dot_status(name: &str) -> FileStatus {
        FileStatus::with_name(FUSE_UNKNOWN_INO as i64, name.to_string(), true)
    }

    /// Supplementary groups for the FUSE requester, resolved from `/proc/<pid>/status`.
    ///
    /// `fuse_in_header` exposes only the effective gid; chmod(2) setgid handling and
    /// chown gid changes must also consider supplementary groups of the caller pid.
    pub fn caller_supplementary_groups(pid: u32) -> Vec<u32> {
        if pid == 0 {
            return Vec::new();
        }

        let status_path = format!("/proc/{pid}/status");
        let Ok(content) = std::fs::read_to_string(status_path) else {
            return Vec::new();
        };

        content
            .lines()
            .find_map(|line| line.strip_prefix("Groups:"))
            .map(|groups| {
                groups
                    .split_whitespace()
                    .filter_map(|gid| gid.parse::<u32>().ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether the caller belongs to `file_gid` via effective or supplementary groups.
    pub fn caller_in_file_group(effective_gid: u32, file_gid: u32, pid: u32) -> bool {
        if effective_gid == file_gid {
            return true;
        }

        Self::caller_supplementary_groups(pid).contains(&file_gid)
    }

    /// Apply Linux chmod/fchmod security rules for special mode bits. For non-root
    /// callers, setgid is cleared when the caller is not in the inode's group
    /// (see chmod(2) and LTP chmod05/fchmod05).
    pub fn normalize_chmod_mode(mode: u32, caller_uid: u32, in_file_group: bool) -> u32 {
        let mut mode = mode & 0o7777;
        if caller_uid == 0 {
            return mode;
        }

        if !in_file_group {
            mode &= !(libc::S_ISGID as u32);
        }
        mode
    }

    pub fn fuse_setattr_to_opts(setattr: &fuse_setattr_in) -> FuseResult<SetAttrOpts> {
        // FATTR_SIZE is intentionally handled by CurvineFileSystem::set_attr because resizing
        // requires the file handle and cache invalidation managed by the caller.

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

        let mut atime = None;
        let mut mtime = None;

        if (setattr.valid & FATTR_ATIME) != 0 {
            atime = Some(Self::timestamp_to_millis(
                "atime",
                setattr.atime,
                setattr.atimensec,
            )?);
        } else if (setattr.valid & FATTR_ATIME_NOW) != 0 {
            atime = Some(LocalTime::mills() as i64);
        }

        if (setattr.valid & FATTR_MTIME) != 0 {
            mtime = Some(Self::timestamp_to_millis(
                "mtime",
                setattr.mtime,
                setattr.mtimensec,
            )?);
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

    fn timestamp_to_millis(field: &str, seconds: u64, nanoseconds: u32) -> FuseResult<i64> {
        if nanoseconds >= 1_000_000_000 {
            return err_fuse!(
                libc::EINVAL,
                "{} nanoseconds out of range: {}",
                field,
                nanoseconds
            );
        }

        // FUSE represents seconds as u64, including negative Unix timestamps encoded in two's
        // complement. Reinterpret it as i64 before doing checked arithmetic.
        let seconds = seconds as i64;
        match seconds
            .checked_mul(1000)
            .and_then(|millis| millis.checked_add(i64::from(nanoseconds / 1_000_000)))
        {
            Some(millis) => Ok(millis),
            None => err_fuse!(libc::EINVAL, "{} timestamp out of range", field),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::fuse_abi::{
        fuse_ioctl_iovec, fuse_ioctl_out, fuse_setattr_in, FUSE_IOCTL_RETRY,
    };
    use curvine_model::INTERNAL_CTIME_XATTR;

    #[test]
    fn protected_xattr_errors_match_operation() {
        for name in [
            "security.capability",
            "security.selinux",
            "system.posix_acl_access",
            "system.posix_acl_default",
        ] {
            assert_eq!(
                FuseUtils::check_xattr(name, XattrOp::Get)
                    .unwrap_err()
                    .errno(),
                libc::ENODATA
            );
            assert_eq!(
                FuseUtils::check_xattr(name, XattrOp::Set)
                    .unwrap_err()
                    .errno(),
                libc::EOPNOTSUPP
            );
            assert_eq!(
                FuseUtils::check_xattr(name, XattrOp::Remove)
                    .unwrap_err()
                    .errno(),
                libc::EOPNOTSUPP
            );
        }
    }

    #[test]
    fn user_xattr_is_allowed_for_every_operation() {
        for op in [XattrOp::Get, XattrOp::Set, XattrOp::Remove] {
            FuseUtils::check_xattr("user.curvine", op).unwrap();
        }
    }

    #[test]
    fn mknod_rdev_xattr_is_internal_only() {
        assert_eq!(
            FuseUtils::check_xattr(MKNOD_RDEV_XATTR, XattrOp::Get)
                .unwrap_err()
                .errno(),
            libc::ENODATA
        );
        assert_eq!(
            FuseUtils::check_xattr(MKNOD_RDEV_XATTR, XattrOp::Set)
                .unwrap_err()
                .errno(),
            libc::EOPNOTSUPP
        );
        assert_eq!(
            FuseUtils::check_xattr(MKNOD_RDEV_XATTR, XattrOp::Remove)
                .unwrap_err()
                .errno(),
            libc::EOPNOTSUPP
        );
    }

    #[test]
    fn nul_xattr_names_are_internal_only() {
        assert_eq!(
            FuseUtils::check_xattr(INTERNAL_CTIME_XATTR, XattrOp::Get)
                .unwrap_err()
                .errno(),
            libc::ENODATA
        );
        for op in [XattrOp::Set, XattrOp::Remove] {
            assert_eq!(
                FuseUtils::check_xattr(INTERNAL_CTIME_XATTR, op)
                    .unwrap_err()
                    .errno(),
                libc::EOPNOTSUPP
            );
        }
    }

    #[test]
    fn kernel_cache_timeouts_ignore_check_permission() {
        let conf = FuseConf::default();
        assert!(conf.check_permission);

        assert_eq!(
            FuseUtils::kernel_cache_timeouts(&conf),
            (
                conf.entry_ttl.as_secs(),
                conf.entry_ttl.subsec_nanos(),
                conf.attr_ttl.as_secs(),
                conf.attr_ttl.subsec_nanos(),
            )
        );
    }

    #[test]
    fn file_open_flags_are_built_only_from_response_flags() {
        let mut conf = FuseConf::default();

        assert_eq!(
            FuseUtils::file_open_flags(&conf, true),
            FUSE_FOPEN_KEEP_CACHE
        );
        assert_eq!(FuseUtils::file_open_flags(&conf, false), 0);

        conf.open_direct_on_stale = true;
        assert_eq!(
            FuseUtils::file_open_flags(&conf, false),
            FUSE_FOPEN_DIRECT_IO
        );

        conf.non_seekable = true;
        assert_eq!(
            FuseUtils::file_open_flags(&conf, false),
            FUSE_FOPEN_DIRECT_IO | FUSE_FOPEN_NONSEEKABLE
        );

        conf.direct_io = true;
        assert_eq!(
            FuseUtils::file_open_flags(&conf, true),
            FUSE_FOPEN_DIRECT_IO | FUSE_FOPEN_NONSEEKABLE
        );

        let request_only_flags = (libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND) as u32;
        assert_eq!(
            FuseUtils::file_open_flags(&conf, true) & request_only_flags,
            0
        );
    }

    #[test]
    fn cache_dir_flag_is_limited_to_directory_responses() {
        let mut conf = FuseConf {
            cache_readdir: true,
            ..Default::default()
        };

        assert_eq!(
            FuseUtils::file_open_flags(&conf, true),
            FUSE_FOPEN_KEEP_CACHE
        );
        assert_eq!(
            FuseUtils::dir_open_flags(&conf),
            FUSE_FOPEN_KEEP_CACHE | FUSE_FOPEN_CACHE_DIR
        );

        conf.direct_io = true;
        conf.non_seekable = true;
        assert_eq!(
            FuseUtils::dir_open_flags(&conf),
            FUSE_FOPEN_DIRECT_IO | FUSE_FOPEN_NONSEEKABLE | FUSE_FOPEN_CACHE_DIR
        );
    }

    #[test]
    fn caller_in_file_group_matches_effective_gid() {
        assert!(FuseUtils::caller_in_file_group(100, 100, 0));
        assert!(!FuseUtils::caller_in_file_group(100, 200, 0));
    }

    #[test]
    fn caller_in_file_group_considers_supplementary_groups() {
        let pid = std::process::id();
        let groups = FuseUtils::caller_supplementary_groups(pid);
        if let Some(&gid) = groups.first() {
            assert!(FuseUtils::caller_in_file_group(0, gid, pid));
        }
    }

    #[test]
    fn normalize_chmod_mode_strips_setgid_for_non_group_member() {
        let mode = FuseUtils::normalize_chmod_mode(0o3777, 1000, false);
        assert_eq!(mode, 0o1777);
    }

    #[test]
    fn normalize_chmod_mode_preserves_setgid_for_group_member() {
        let mode = FuseUtils::normalize_chmod_mode(0o3777, 1000, true);
        assert_eq!(mode, 0o3777);
    }

    #[test]
    fn normalize_chmod_mode_preserves_setuid_for_non_root() {
        let mode = FuseUtils::normalize_chmod_mode(0o6777, 1000, true);
        assert_eq!(mode, 0o6777);
    }

    #[test]
    fn normalize_chmod_mode_allows_special_bits_for_root() {
        let mode = FuseUtils::normalize_chmod_mode(0o4777, 0, false);
        assert_eq!(mode, 0o4777);
    }

    #[test]
    fn setattr_preserves_millisecond_from_nsec() {
        let setattr = fuse_setattr_in {
            valid: FATTR_ATIME | FATTR_MTIME,
            atime: 1_700_000_000,
            mtime: 1_800_000_000,
            atimensec: 123_456_789,
            mtimensec: 987_654_321,
            ..Default::default()
        };

        let opts = FuseUtils::fuse_setattr_to_opts(&setattr).unwrap();

        assert_eq!(opts.atime, Some(1_700_000_000_123));
        assert_eq!(opts.mtime, Some(1_800_000_000_987));
    }

    #[test]
    fn setattr_rejects_out_of_range_nsec() {
        for setattr in [
            fuse_setattr_in {
                valid: FATTR_ATIME,
                atimensec: 1_000_000_000,
                ..Default::default()
            },
            fuse_setattr_in {
                valid: FATTR_MTIME,
                mtimensec: 1_000_000_000,
                ..Default::default()
            },
        ] {
            let err = FuseUtils::fuse_setattr_to_opts(&setattr).unwrap_err();

            assert_eq!(err.errno(), libc::EINVAL);
        }
    }

    #[test]
    fn kernel_version_6_10_compares_greater_than_6_9() {
        let version = FuseUtils::parse_kernel_version("6.10.3").unwrap();

        assert!(version > (6, 9));
    }

    #[test]
    fn kernel_version_4_10_enables_clone_fd() {
        let version = FuseUtils::parse_kernel_version("4.10.0").unwrap();

        assert!(version >= FUSE_CLONE_FD_MIN_VERSION);
    }

    #[test]
    fn kernel_version_parse_failure_disables_clone_fd() {
        let version = FuseUtils::parse_kernel_version("unexpected").unwrap_or((0, 0));

        assert!(version < FUSE_CLONE_FD_MIN_VERSION);
    }

    // Regression: failed clone ioctl must close the handed-in fd.
    #[cfg(target_os = "linux")]
    #[test]
    fn fuse_clone_fd_closes_fd_on_ioctl_error() {
        // Non-zero pipe size: pipe2 sets F_SETPIPE_SZ, which rejects 0 on some
        // kernels; a page size is rounded up to the kernel minimum and is safe.
        let [r, w] = sys::pipe2(4096).expect("pipe2");

        let res = FuseUtils::clone_fd_ioctl(r, w);
        // The bogus ioctl on a pipe fd must fail (ENOTTY/EINVAL), and the error
        // is propagated unchanged.
        assert!(res.is_err(), "ioctl on a pipe fd should fail");

        // Error path should close `r`; F_GETFD should now fail with EBADF.
        assert!(
            sys::fcntl_get(r).is_err(),
            "clone_fd should have been closed on ioctl error"
        );

        // `r` is already closed inside clone_fd_ioctl; only close the write end.
        let _ = sys::close(w);
    }

    fn file_status(file_type: FileType, len: i64, mode: u32) -> FileStatus {
        FileStatus {
            file_type,
            len,
            mode,
            ..Default::default()
        }
    }

    #[test]
    fn status_to_attr_rejects_negative_len() {
        let conf = FuseConf::default();

        // A regular file with a negative len is rejected (size comes from len).
        let err =
            FuseUtils::status_to_attr(&conf, &file_status(FileType::File, -1, 0o644)).unwrap_err();
        assert_eq!(err.errno(), libc::EINVAL);

        // Dir / Link sizes do not come from len, so a negative len is harmless.
        assert!(FuseUtils::status_to_attr(&conf, &file_status(FileType::Dir, -1, 0o755)).is_ok());
        let mut link = file_status(FileType::Link, -1, 0);
        link.target = Some("x".to_string());
        assert!(FuseUtils::status_to_attr(&conf, &link).is_ok());
    }

    #[test]
    fn status_to_attr_preserves_independent_ctime() {
        let conf = FuseConf::default();
        let mut status = file_status(FileType::File, 0, 0o644);
        status.mtime = 1_000;
        status.x_attr.insert(
            INTERNAL_CTIME_XATTR.to_string(),
            2_500_i64.to_le_bytes().to_vec(),
        );

        let attr = FuseUtils::status_to_attr(&conf, &status).unwrap();
        assert_eq!((attr.mtime, attr.mtimensec), (1, 0));
        assert_eq!((attr.ctime, attr.ctimensec), (2, 500_000_000));
    }

    #[test]
    fn blocks_derived_from_fuse_size() {
        let conf = FuseConf::default();

        // Directory: size = 4096 ⇒ blocks = 8 (old code used raw len 0 ⇒ 0).
        let dir = FuseUtils::status_to_attr(&conf, &file_status(FileType::Dir, 0, 0o755)).unwrap();
        assert_eq!(dir.size, 4096);
        assert_eq!(dir.blocks, 8);

        // Regular file: blocks = ceil(size / 512).
        let f =
            FuseUtils::status_to_attr(&conf, &file_status(FileType::File, 1000, 0o644)).unwrap();
        assert_eq!(f.blocks, 2);
        let empty =
            FuseUtils::status_to_attr(&conf, &file_status(FileType::File, 0, 0o644)).unwrap();
        assert_eq!(empty.blocks, 0);
    }

    #[test]
    fn real_mode_zero_is_preserved() {
        let conf = FuseConf::default(); // umask = 0o22

        // POSIX mode 0000 is valid and must not be confused with a missing mode.
        let f = FuseUtils::status_to_attr(&conf, &file_status(FileType::File, 0, 0)).unwrap();
        assert_eq!(f.mode & 0o7777, 0);
        assert_eq!(f.mode & libc::S_IFMT as u32, libc::S_IFREG as u32);

        // Directories may also legitimately have mode 0000.
        let d = FuseUtils::status_to_attr(&conf, &file_status(FileType::Dir, 0, 0)).unwrap();
        assert_eq!(d.mode & 0o7777, 0);
        assert_eq!(d.mode & libc::S_IFMT as u32, libc::S_IFDIR as u32);
    }

    #[test]
    fn synthetic_dot_uses_default_mode() {
        let conf = FuseConf::default(); // umask = 0o22

        // Synthetic dot entries have no persisted ACL and still need usable
        // directory permissions.
        let mut dot = file_status(FileType::Dir, 0, 0);
        dot.id = FUSE_UNKNOWN_INO as i64;
        dot.name = ".".to_string();
        let dot_attr = FuseUtils::status_to_attr(&conf, &dot).unwrap();
        assert_eq!(dot_attr.mode & 0o7777, 0o755);
    }

    #[test]
    fn get_mode_masks_permission_bits() {
        // A stray file-type bit in `perm` must be stripped before the correct
        // type bit is applied.
        let mode = FuseUtils::get_mode(libc::S_IFDIR as u32 | 0o755, FileType::File);
        assert_eq!(mode & 0o7777, 0o755);
        assert_eq!(mode & libc::S_IFMT as u32, libc::S_IFREG as u32);
    }

    #[test]
    fn default_file_mode_is_not_executable() {
        // The regular-file default must not carry execute bits.
        assert_eq!(FUSE_DEFAULT_FILE_MODE & 0o111, 0);

        // An explicit mode 0000 remains non-executable and is preserved.
        let conf = FuseConf::default();
        let f = FuseUtils::status_to_attr(&conf, &file_status(FileType::File, 0, 0)).unwrap();
        assert_eq!(f.mode & 0o7777, 0);
    }

    #[test]
    fn status_to_attr_nlink_defaults_to_one() {
        let conf = FuseConf::default();

        // nlink == 0 (e.g. UFS/object store that does not set it) becomes 1.
        let mut zero = file_status(FileType::File, 0, 0o644);
        zero.nlink = 0;
        assert_eq!(FuseUtils::status_to_attr(&conf, &zero).unwrap().nlink, 1);

        // A real nlink is preserved.
        let mut three = file_status(FileType::File, 0, 0o644);
        three.nlink = 3;
        assert_eq!(FuseUtils::status_to_attr(&conf, &three).unwrap().nlink, 3);
    }

    #[test]
    fn check_sticky_hardlink_denies_cross_owner_link() {
        let err = FuseUtils::check_sticky_hardlink(true, 1000, 0, 0, 0o1777).unwrap_err();
        assert_eq!(err.errno(), libc::EPERM);

        assert!(FuseUtils::check_sticky_hardlink(true, 0, 0, 1000, 0o1777).is_ok());
        assert!(FuseUtils::check_sticky_hardlink(true, 1000, 1000, 0, 0o1777).is_ok());
        assert!(FuseUtils::check_sticky_hardlink(true, 1000, 0, 1000, 0o1777).is_ok());
        assert!(FuseUtils::check_sticky_hardlink(true, 1000, 0, 0, 0o755).is_ok());
    }

    #[test]
    fn special_file_type_from_mode_maps_chr_blk_fifo_sock() {
        assert_eq!(
            FuseUtils::special_file_type_from_mode(0o20777),
            Some(FileType::Char)
        );
        assert_eq!(
            FuseUtils::special_file_type_from_mode(0o60777),
            Some(FileType::Block)
        );
        assert_eq!(
            FuseUtils::special_file_type_from_mode(0o10777),
            Some(FileType::Fifo)
        );
        assert_eq!(
            FuseUtils::special_file_type_from_mode(0o140777),
            Some(FileType::Socket)
        );
        assert!(FuseUtils::special_file_type_from_mode(0o100644).is_none());
    }

    #[test]
    fn user_xattr_namespace_rejects_special_nodes() {
        let err = FuseUtils::check_user_xattr_namespace(FileType::Fifo, "user.test").unwrap_err();
        assert_eq!(err.errno(), libc::EPERM);
        FuseUtils::check_user_xattr_namespace(FileType::File, "user.test").unwrap();
    }

    #[test]
    fn ioctl_file_flags_round_trip_native_long() {
        use bytes::BytesMut;

        let flags = FS_IMMUTABLE_FL | FS_APPEND_FL;
        let mut buf = BytesMut::new();
        FuseUtils::append_ioctl_file_flags(&mut buf, flags, FuseUtils::ioctl_flag_bytes() as u32);
        assert_eq!(buf.len(), FuseUtils::ioctl_flag_bytes());
        assert_eq!(FuseUtils::decode_ioctl_file_flags(&buf).unwrap(), flags);
    }

    #[test]
    fn ioctl_file_flags_decode_accepts_four_byte_int_payload() {
        let flags = FS_IMMUTABLE_FL | FS_APPEND_FL;
        let bytes = flags.to_ne_bytes();
        assert_eq!(FuseUtils::decode_ioctl_file_flags(&bytes).unwrap(), flags);
    }

    #[test]
    fn ioctl_retry_reply_includes_out_iovec_for_getflags() {
        let arg_ptr = 0xdead_beef_u64;
        let nbytes = FuseUtils::ioctl_flag_bytes() as u64;
        let buf = FuseUtils::build_ioctl_retry(arg_ptr, 0, nbytes);
        let out_size = std::mem::size_of::<fuse_ioctl_out>();
        let iov_size = std::mem::size_of::<fuse_ioctl_iovec>();
        assert_eq!(buf.len(), out_size + iov_size);

        let out: fuse_ioctl_out = unsafe { std::ptr::read(buf.as_ptr() as *const fuse_ioctl_out) };
        assert_eq!(out.result, 0);
        assert_eq!(out.flags, FUSE_IOCTL_RETRY);
        assert_eq!(out.in_iovs, 0);
        assert_eq!(out.out_iovs, 1);

        let iov: fuse_ioctl_iovec =
            unsafe { std::ptr::read(buf[out_size..].as_ptr() as *const fuse_ioctl_iovec) };
        assert_eq!(iov.base, arg_ptr);
        assert_eq!(iov.len, nbytes);
    }

    #[test]
    fn ioctl_retry_reply_includes_in_iovec_for_setflags() {
        let arg_ptr = 0x1234_5678_u64;
        let nbytes = FuseUtils::ioctl_flag_bytes() as u64;
        let buf = FuseUtils::build_ioctl_retry(arg_ptr, nbytes, 0);
        let out_size = std::mem::size_of::<fuse_ioctl_out>();
        let out: fuse_ioctl_out = unsafe { std::ptr::read(buf.as_ptr() as *const fuse_ioctl_out) };
        assert_eq!(out.flags, FUSE_IOCTL_RETRY);
        assert_eq!(out.in_iovs, 1);
        assert_eq!(out.out_iovs, 0);
        let iov: fuse_ioctl_iovec =
            unsafe { std::ptr::read(buf[out_size..].as_ptr() as *const fuse_ioctl_iovec) };
        assert_eq!(iov.base, arg_ptr);
        assert_eq!(iov.len, nbytes);
    }

    #[test]
    fn file_flags_round_trip_through_internal_xattr() {
        let mut status = file_status(FileType::File, 0, 0o644);
        let opts = FuseUtils::set_attr_for_file_flags(&status, FS_IMMUTABLE_FL | FS_APPEND_FL);
        status.x_attr.extend(opts.add_x_attr);
        assert_eq!(
            FuseUtils::file_flags_from_status(&status),
            FS_IMMUTABLE_FL | FS_APPEND_FL
        );
        assert!(FuseUtils::file_has_immutable_or_append(&status));
    }

    #[test]
    fn status_to_attr_reports_special_node_mode_and_rdev() {
        let conf = FuseConf::default();
        let mut chr = file_status(FileType::Char, 0, 0o777);
        chr.x_attr
            .insert(MKNOD_RDEV_XATTR.to_string(), 42u32.to_le_bytes().to_vec());

        let attr = FuseUtils::status_to_attr(&conf, &chr).unwrap();
        assert_eq!(attr.mode & libc::S_IFMT as u32, libc::S_IFCHR as u32);
        assert_eq!(attr.mode & 0o7777, 0o777);
        assert_eq!(attr.rdev, 42);
        assert_eq!(attr.size, 0);
    }
}
