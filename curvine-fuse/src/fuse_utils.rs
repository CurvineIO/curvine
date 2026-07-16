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
use bytes::BytesMut;
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

    /// Owned-buffer variant of [`struct_as_bytes`]; same C-ABI contract applies.
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

    // Per-type default permission bits, used only as a fallback when a status
    // carries no mode (mode == 0). File / Stream / Agg / Object all count as
    // regular files.
    fn default_mode(typ: FileType) -> u32 {
        match typ {
            FileType::Dir => FUSE_DEFAULT_DIR_MODE,
            FileType::Link => FUSE_DEFAULT_SYMLINK_MODE,
            _ => FUSE_DEFAULT_FILE_MODE,
        }
    }

    // Decide the permission bits to report for a status.
    //
    // - Symlinks always report the default (the kernel ignores symlink perm bits).
    // - mode == 0 means the status carries no mode (synthetic / default-constructed
    //   status); fall back to a per-type default masked by umask, matching what a
    //   fresh create would have produced.
    // - Otherwise report the stored mode verbatim (only stray/high bits stripped).
    //   umask is NOT re-applied: it is a create-time mask, and getattr must report
    //   the mode as stored.
    //
    // NOTE (trade-off): mode == 0 is treated as "unset" and gets a default, which
    // also overrides the literal meaning of an explicit `chmod 000`. This does not
    // grant access in practice: curvine's own `check_access_permissions` uses the
    // raw `status.mode`, so with the default `check_permission = true` a 000 file is
    // still denied. The literal 000 is only lost if `check_permission` is disabled
    // and access is left to the kernel's `default_permissions`.
    pub fn effective_perm(mode: u32, typ: FileType, umask: u32) -> u32 {
        if typ == FileType::Link {
            return FUSE_DEFAULT_SYMLINK_MODE;
        }
        if mode == 0 {
            Self::default_mode(typ) & !umask
        } else {
            mode & 0o7777
        }
    }

    pub fn get_mode(perm: u32, typ: FileType) -> u32 {
        // Strip any file-type / stray high bits before OR-ing the correct type bit.
        let perm = perm & 0o7777;
        match typ {
            FileType::Dir => perm | (libc::S_IFDIR as u32),

            #[cfg(target_os = "linux")]
            FileType::Link => perm | (libc::S_IFLNK as u32),

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

    // Device ioctls
    // #define FUSE_DEV_IOC_MAGIC 229
    // #define FUSE_DEV_IOC_CLONE _IOR(FUSE_DEV_IOC_MAGIC, 0, uint32_t)
    // fd clone is a new feature added to the Linux kernel 4.2 or above version, and its main function is to reduce competition for fd locks.
    pub fn fuse_clone_fd(source_fd: RawIO) -> IOResult<RawIO> {
        let path = FFIUtils::new_cs_string(FUSE_DEVICE_NAME);
        let clone_fd = sys::open(&path, libc::O_RDWR)?;
        Self::clone_fd_ioctl(clone_fd, source_fd)
    }

    // Issue the FUSE_DEV_IOC_CLONE ioctl on an already-opened `clone_fd`.
    // On failure, close `clone_fd` before propagating so it does not leak:
    // the caller (fuse_mnt.rs) falls back to dup(self.fd) and never sees this
    // fd again, so nothing else would ever close it.
    //
    // NOTE: close(2) releases the fd even when it returns an error (EINTR/EIO),
    // so we log-and-ignore the close error rather than retry — retrying would
    // risk a double-close of a since-reused fd. The original ioctl error is
    // returned unchanged because it carries the more useful diagnostic.
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
            return Err(e);
        }

        Ok(clone_fd)
    }

    pub fn fuse_st_size(status: &FileStatus) -> FuseResult<u64> {
        let size = match status.file_type {
            FileType::Link => status.target.as_ref().map(|x| x.len()).unwrap_or(0) as u64,
            FileType::Dir => FUSE_DEFAULT_PAGE_SIZE as u64,
            // Regular files (File / Stream / Agg / Object) take their size from
            // `status.len`. Defend the FUSE boundary against abnormal backend
            // data: a negative len would cast to a huge u64, so reject it.
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
        // Derive size first (rejects negative len for regular files), then blocks
        // from the reported size so the two stay consistent (a directory reports
        // size = 4096 ⇒ blocks = 8, not 0). `div_ceil` on a u64 cannot overflow.
        let size = FuseUtils::fuse_st_size(status)?;
        let blocks = size.div_ceil(512);

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

        let perm = FuseUtils::effective_perm(status.mode, status.file_type, conf.umask);
        let mode = FuseUtils::get_mode(perm, status.file_type);

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
            rdev: 0,
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
        // FATTR_SIZE is intentionally handled by CurvineFileSystem::set_attr because resizing
        // requires the file handle and cache invalidation managed by the caller.

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
    use crate::raw::fuse_abi::fuse_setattr_in;

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

    // Regression for the clone-fd leak: when the FUSE_DEV_IOC_CLONE ioctl fails,
    // `clone_fd_ioctl` must close the fd it was handed before returning the error,
    // otherwise it leaks (the caller falls back to dup and never sees this fd).
    //
    // We drive this without touching /dev/fuse: a plain pipe fd does not accept
    // the FUSE clone ioctl, so `sys::ioctl` fails deterministically and the
    // close path is exercised. `fcntl(F_GETFD)` on the now-closed fd returns
    // EBADF, confirming the close happened. Linux-only: pipe2/ioctl/fcntl_get
    // are not available on other targets.
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

        // `r` should have been closed by the error path: F_GETFD now returns EBADF.
        // (Primary guarantee is the propagated error above; this is a best-effort
        // check — in theory another thread could reuse the fd number, but not in
        // this single-threaded test.)
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
    fn mode_zero_uses_default_mode() {
        let conf = FuseConf::default(); // umask = 0o22

        // Regular file with mode == 0 falls back to 0o666 & !0o22 = 0o644.
        let f = FuseUtils::status_to_attr(&conf, &file_status(FileType::File, 0, 0)).unwrap();
        assert_eq!(f.mode & 0o7777, 0o644);
        assert_eq!(f.mode & libc::S_IFMT as u32, libc::S_IFREG as u32);

        // Directory with mode == 0 falls back to 0o777 & !0o22 = 0o755.
        let d = FuseUtils::status_to_attr(&conf, &file_status(FileType::Dir, 0, 0)).unwrap();
        assert_eq!(d.mode & 0o7777, 0o755);
        assert_eq!(d.mode & libc::S_IFMT as u32, libc::S_IFDIR as u32);

        // Regression: a synthetic dot dir (id == FUSE_UNKNOWN_INO, mode == 0) still
        // reports 0o755, matching the old dedicated dot branch.
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

        // A regular file with mode == 0 is reported non-executable.
        let conf = FuseConf::default();
        let f = FuseUtils::status_to_attr(&conf, &file_status(FileType::File, 0, 0)).unwrap();
        assert_eq!(f.mode & 0o111, 0);
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
}
