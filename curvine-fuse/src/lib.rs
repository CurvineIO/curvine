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
#![recursion_limit = "256"]

use crate::raw::fuse_abi::{fuse_in_header, fuse_out_header};
use curvine_alloc as _;
use once_cell::sync::Lazy;

pub mod cli;
pub mod fs;
// Crate-internal only: `err_fuse!` is `pub(crate)`, so `mod`, not `pub mod`.
mod macros;
pub mod raw;
pub mod session;
pub mod web_server;

// Re-export the crate-internal `err_fuse!` macro at the crate root so existing
// `use crate::err_fuse;` imports resolve. Not `#[macro_export]`ed — see macros.rs.
pub(crate) use macros::err_fuse;

mod fuse_error;
pub use self::fuse_error::FuseError;

pub mod fuse_metrics;
pub use self::fuse_metrics::FuseMetrics;

mod fuse_utils;
pub use self::fuse_utils::{FuseUtils, XattrOp};

pub type FuseResult<T> = Result<T, FuseError>;

pub const FUSE_DEVICE_NAME: &str = "/dev/fuse";

pub const FUSE_IN_HEADER_LEN: usize = size_of::<fuse_in_header>();

pub const FUSE_SUCCESS: i32 = 0;

pub const FUSE_OUT_HEADER_LEN: usize = size_of::<fuse_out_header>();

pub(crate) const FILE_HANDLE_READ_BIT: u64 = 1 << 63;

pub(crate) const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;

pub const FUSE_ROOT_ID: u64 = 1;

pub const FUSE_PATH_SEPARATOR: &str = "/";

pub const FUSE_BLOCK_SIZE: u64 = 65536;

pub const FUSE_KERNEL_VERSION: u32 = 7;

pub const FUSE_KERNEL_MINOR_VERSION: u32 = 31;

/// Upper bound on `max_pages`, matching the kernel's internal limit.
pub const FUSE_MAX_MAX_PAGES: usize = 256;

pub const FUSE_BUFFER_HEADER_SIZE: usize = 0x1000;

pub const FUSE_DEFAULT_PAGE_SIZE: usize = 4096;

pub const FUSE_PATH_MAX_DEPTH: usize = 4096;

/// FUSE init capability bit (uapi `fuse.h`: `FUSE_MAX_PAGES = (1 << 22)`): negotiates that
/// `fuse_init_out.max_pages` carries the per-request page count.
pub const FUSE_MAX_PAGES: u32 = 1 << 22;

/// Kernel sends the extended 64-byte `fuse_init_in` request introduced in ABI 7.36.
/// Curvine negotiates ABI 7.31 and does not advertise this bit in the reply, but it must
/// consume the extended request sent by newer kernels before negotiating down.
pub const FUSE_INIT_EXT: u32 = 1 << 30;

pub const FUSE_BIG_WRITES: u32 = 1 << 5;

pub const FUSE_ASYNC_READ: u32 = 1 << 0;

pub const FUSE_SPLICE_WRITE: u32 = 1 << 7;

pub const FUSE_SPLICE_MOVE: u32 = 1 << 8;

pub const FUSE_SPLICE_READ: u32 = 1 << 9;

pub const FUSE_ASYNC_DIO: u32 = 1 << 15;

pub const FUSE_DO_READDIRPLUS: u32 = 1 << 13;

pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14;

/// FUSE init capability bit for remote POSIX (fcntl) locking.
pub const FUSE_POSIX_LOCKS: u32 = 1 << 1;

/// FUSE init capability bit for remote BSD (flock) locking.
pub const FUSE_FLOCK_LOCKS: u32 = 1 << 10;

/// FUSE init capability bit: the daemon passes `mode` through unchanged on
/// create/mkdir/mknod (kernel skips umask masking).
pub const FUSE_DONT_MASK: u32 = 1 << 6;

/// FUSE init capability bit: skip open notifications to the daemon.
pub const FUSE_NO_OPEN_SUPPORT: u32 = 1 << 17;

/// FUSE init capability bit: parallel directory operations with independent
/// inode locks.
pub const FUSE_PARALLEL_DIROPS: u32 = 1 << 18;

/// FUSE init capability bit: the daemon handles suid/sgid clearing on write.
pub const FUSE_HANDLE_KILLPRIV: u32 = 1 << 19;

/// FUSE init capability bit: kernel aborts the connection on daemon errors.
pub const FUSE_ABORT_ERROR: u32 = 1 << 21;

/// FUSE init capability bit: kernel may cache symlink targets.
pub const FUSE_CACHE_SYMLINKS: u32 = 1 << 23;

/// FUSE init capability bit: skip opendir notifications to the daemon.
pub const FUSE_NO_OPENDIR_SUPPORT: u32 = 1 << 24;

/// FUSE init capability bit: kernel does not auto-invalidate page cache on
/// write; the daemon sends explicit invalidation.
pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 1 << 25;

/// FUSE init capability bit: enhanced killpriv semantics (ABI 7.37).
pub const FUSE_HANDLE_KILLPRIV_V2: u32 = 1 << 28;

/// FUSE init capability bit: daemon accepts the extended 16-byte
/// `fuse_setxattr_in` request layout.
pub const FUSE_SETXATTR_EXT: u32 = 1 << 29;

pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16;

pub const FUSE_POSIX_ACL: u32 = 1 << 20;

/// FUSE init capability bit for ioctl on directories; not related to RENAME2.
pub const FUSE_HAS_IOCTL_DIR: u32 = 1 << 11;

/// Kernel automatically invalidates the page cache on open when mtime or size
/// has changed (CAP_AUTO_INVAL_DATA, available since Linux 2.6.35).
pub const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12;

/// Kernel exportfs support: enables `name_to_handle_at` / `open_by_handle_at` via
/// `LOOKUP(nodeid, ".")` / `LOOKUP(nodeid, "..")` reconstruction.
///
/// Advertised in `SUPPORTED_INIT_FLAGS` so that `name_to_handle_at` /
/// `open_by_handle_at` remain usable after `drop_caches`. The kernel's
/// `fuse_get_parent` / `fuse_get_dentry` short-circuit with `-ESTALE` when
/// `fc->export_support` is unset, and `NodeState::fs_lookup` now handles root
/// `.`/`..` correctly.
pub const FUSE_EXPORT_SUPPORT: u32 = 1 << 4;

/// Kernel init capability bit: the daemon handles O_TRUNC atomically inside
/// `open`, so the kernel skips the follow-up SETATTR(size=0). Curvine's `open`
/// does NOT truncate, so this bit must never be advertised -- otherwise
/// O_TRUNC is silently lost when a writer for the inode already exists (the
/// shared writer ignores the second open's flags).
pub const FUSE_ATOMIC_O_TRUNC: u32 = 1 << 3;

/// Init capabilities the daemon actually implements, negotiated as an explicit
/// allowlist: `init` advertises `SUPPORTED_INIT_FLAGS & op.arg.flags` (only what
/// BOTH the daemon supports and the kernel offered), instead of blindly echoing
/// every kernel-offered flag.
///
/// Deliberately EXCLUDED (never advertised): FUSE_ATOMIC_O_TRUNC (open does not
/// truncate), FUSE_POSIX_ACL (no ACL handling), FUSE_HAS_IOCTL_DIR (no ioctl),
/// FUSE_SETXATTR_EXT (the decoder currently accepts only the 8-byte compatible
/// request layout).
pub const SUPPORTED_INIT_FLAGS: u32 = FUSE_ASYNC_READ
    | FUSE_BIG_WRITES
    | FUSE_ASYNC_DIO
    | FUSE_AUTO_INVAL_DATA
    | FUSE_READDIRPLUS_AUTO
    | FUSE_DO_READDIRPLUS
    | FUSE_POSIX_LOCKS
    | FUSE_FLOCK_LOCKS
    | FUSE_MAX_PAGES
    | FUSE_EXPORT_SUPPORT
    | FUSE_SPLICE_MOVE
    | FUSE_SPLICE_WRITE
    | FUSE_SPLICE_READ
    | FUSE_DONT_MASK
    | FUSE_PARALLEL_DIROPS
    | FUSE_HANDLE_KILLPRIV
    | FUSE_ABORT_ERROR
    | FUSE_CACHE_SYMLINKS
    | FUSE_NO_OPENDIR_SUPPORT
    | FUSE_EXPLICIT_INVAL_DATA
    | FUSE_HANDLE_KILLPRIV_V2;

/// Human-readable FUSE init-capability names; unknown bits are kept as hex.
pub fn fuse_init_flag_names(flags: u32) -> Vec<String> {
    const KNOWN: &[(u32, &str)] = &[
        (FUSE_ASYNC_READ, "ASYNC_READ"),
        (FUSE_DONT_MASK, "DONT_MASK"),
        (FUSE_POSIX_LOCKS, "POSIX_LOCKS"),
        (FUSE_ATOMIC_O_TRUNC, "ATOMIC_O_TRUNC"),
        (FUSE_EXPORT_SUPPORT, "EXPORT_SUPPORT"),
        (FUSE_BIG_WRITES, "BIG_WRITES"),
        (FUSE_SPLICE_WRITE, "SPLICE_WRITE"),
        (FUSE_SPLICE_MOVE, "SPLICE_MOVE"),
        (FUSE_SPLICE_READ, "SPLICE_READ"),
        (FUSE_FLOCK_LOCKS, "FLOCK_LOCKS"),
        (FUSE_HAS_IOCTL_DIR, "HAS_IOCTL_DIR"),
        (FUSE_AUTO_INVAL_DATA, "AUTO_INVAL_DATA"),
        (FUSE_DO_READDIRPLUS, "DO_READDIRPLUS"),
        (FUSE_READDIRPLUS_AUTO, "READDIRPLUS_AUTO"),
        (FUSE_ASYNC_DIO, "ASYNC_DIO"),
        (FUSE_WRITEBACK_CACHE, "WRITEBACK_CACHE"),
        (FUSE_POSIX_ACL, "POSIX_ACL"),
        (FUSE_MAX_PAGES, "MAX_PAGES"),
        (FUSE_NO_OPEN_SUPPORT, "NO_OPEN_SUPPORT"),
        (FUSE_PARALLEL_DIROPS, "PARALLEL_DIROPS"),
        (FUSE_HANDLE_KILLPRIV, "HANDLE_KILLPRIV"),
        (FUSE_ABORT_ERROR, "ABORT_ERROR"),
        (FUSE_CACHE_SYMLINKS, "CACHE_SYMLINKS"),
        (FUSE_NO_OPENDIR_SUPPORT, "NO_OPENDIR_SUPPORT"),
        (FUSE_EXPLICIT_INVAL_DATA, "EXPLICIT_INVAL_DATA"),
        (FUSE_HANDLE_KILLPRIV_V2, "HANDLE_KILLPRIV_V2"),
        (FUSE_SETXATTR_EXT, "SETXATTR_EXT"),
        (FUSE_INIT_EXT, "INIT_EXT"),
    ];
    let mut names: Vec<String> = KNOWN
        .iter()
        .filter(|(bit, _)| flags & bit != 0)
        .map(|(_, name)| name.to_string())
        .collect();
    let known_mask: u32 = KNOWN.iter().fold(0, |acc, (bit, _)| acc | bit);
    let unknown = flags & !known_mask;
    if unknown != 0 {
        names.push(format!("0x{:x}", unknown));
    }
    names
}

/// Minimum FUSE ABI the daemon accepts and advertises.
pub const FUSE_MIN_ABI: (u32, u32) = (FUSE_KERNEL_VERSION, FUSE_KERNEL_MINOR_VERSION);

pub const FUSE_MAX_NAME_LENGTH: usize = 255;

/// Placeholder for the statfs `files`/`ffree` counts (total/free inodes) when the count is unknown
/// — Curvine's distributed backend keeps no global inode statistics.
pub const FUSE_UNKNOWN_INODES: u64 = 0xffffffff;

pub const FUSE_CURRENT_DIR: &str = ".";

pub const FUSE_PARENT_DIR: &str = "..";

pub const FUSE_S_ISUID: u32 = 0x800;

pub const FUSE_S_ISGID: u32 = 0x400;

// Default permission bits for synthetic entries; persisted modes are not overridden.
pub const FUSE_DEFAULT_FILE_MODE: u32 = 0o666; // regular files: rw, no exec
pub const FUSE_DEFAULT_DIR_MODE: u32 = 0o777; // dirs: rwx (exec needed to traverse)
pub const FUSE_DEFAULT_SYMLINK_MODE: u32 = 0o777; // symlink perm bits are ignored by the kernel

/// Non-zero sentinel inode for synthetic dirents, reserved from real allocation.
pub const FUSE_UNKNOWN_INO: u64 = 0xffffffff;

pub const FUSE_FOPEN_DIRECT_IO: u32 = 1 << 0;

pub const FUSE_FOPEN_KEEP_CACHE: u32 = 1 << 1;

pub const FUSE_FOPEN_NONSEEKABLE: u32 = 1 << 2;

pub const FUSE_FOPEN_CACHE_DIR: u32 = 1 << 3;

// FUSE FOPEN response flags kept for ABI completeness (kernel `fuse.h`), not yet
// negotiated by this daemon; crate-internal until a feature wires them up.
#[allow(dead_code)]
pub(crate) const FUSE_FOPEN_STREAM: u32 = 1 << 4;

#[allow(dead_code)]
pub(crate) const FUSE_FOPEN_NOFLUSH: u32 = 1 << 5;

#[allow(dead_code)]
pub(crate) const FUSE_FOPEN_PARALLEL_DIRECT_WRITES: u32 = 1 << 6;

// FUSE setattr valid bit flags (aligned with linux/fs/fuse definitions)
pub const FATTR_MODE: u32 = 1 << 0;

pub const FATTR_UID: u32 = 1 << 1;

pub const FATTR_GID: u32 = 1 << 2;

pub const FATTR_SIZE: u32 = 1 << 3;

pub const FATTR_ATIME: u32 = 1 << 4;

pub const FATTR_MTIME: u32 = 1 << 5;

pub const FATTR_FH: u32 = 1 << 6;

pub const FATTR_ATIME_NOW: u32 = 1 << 7;

pub const FATTR_MTIME_NOW: u32 = 1 << 8;

// Minimum kernel version that supports the clone-fd feature.
pub const FUSE_CLONE_FD_MIN_VERSION: (u32, u32) = (4, 2);

pub const FUSE_NOTIFY_UNIQUE: u64 = 0;

pub const STATE_FILE_MAGIC: &[u8; 4] = b"cvfs";

pub const STATE_FILE_VERSION: u64 = 1;

pub static UNIX_KERNEL_VERSION: Lazy<(u32, u32)> = Lazy::new(FuseUtils::get_kernel_version);
