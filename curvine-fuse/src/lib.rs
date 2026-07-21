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
use once_cell::sync::Lazy;

pub mod cli;
pub mod fs;
// Crate-internal only: `macros` exposes no public API (its sole item, the
// `err_fuse!` macro, is `pub(crate)`), so the module is `mod`, not `pub mod`.
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

/// Upper bound on `max_pages` (the number of pages per request), matching the
/// kernel's internal `FUSE_MAX_MAX_PAGES = 256` (`fs/fuse/fuse_i.h` on v5.4–v5.10;
/// newer kernels moved it to the `fuse_max_pages_limit` sysctl). NOTE: this is a
/// page-count LIMIT, distinct from the `FUSE_MAX_PAGES` init capability bit below.
pub const FUSE_MAX_MAX_PAGES: usize = 256;

pub const FUSE_BUFFER_HEADER_SIZE: usize = 0x1000; // 4096

pub const FUSE_DEFAULT_PAGE_SIZE: usize = 4096;

pub const FUSE_PATH_MAX_DEPTH: usize = 4096;

/// FUSE init capability bit (uapi `fuse.h`: `FUSE_MAX_PAGES = (1 << 22)`):
/// negotiates that `fuse_init_out.max_pages` carries the per-request page count.
/// NOTE: this is a capability BIT, distinct from the `FUSE_MAX_MAX_PAGES` page
/// limit above — the near-identical names are both kernel-official.
pub const FUSE_MAX_PAGES: u32 = 1 << 22;

pub const FUSE_BIG_WRITES: u32 = 1 << 5;

pub const FUSE_ASYNC_READ: u32 = 1 << 0;

pub const FUSE_SPLICE_WRITE: u32 = 1 << 7;

pub const FUSE_SPLICE_MOVE: u32 = 1 << 8;

pub const FUSE_SPLICE_READ: u32 = 1 << 9;

pub const FUSE_ASYNC_DIO: u32 = 1 << 15;

pub const FUSE_DO_READDIRPLUS: u32 = 1 << 13;

pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14;

/// FUSE init capability bit (uapi `fuse.h`: `FUSE_POSIX_LOCKS = (1 << 1)`):
/// remote POSIX (fcntl) file locking. Implemented via `get_lk`/`set_lk`/
/// `set_lkw`, which route to the distributed backend's lock service.
pub const FUSE_POSIX_LOCKS: u32 = 1 << 1;

/// FUSE init capability bit (uapi `fuse.h`: `FUSE_FLOCK_LOCKS = (1 << 10)`):
/// remote BSD (flock) file locking. Implemented via the same lock path;
/// `release`/`flush` drop flock/posix locks through `LockFlags::Flock`/`Plock`.
pub const FUSE_FLOCK_LOCKS: u32 = 1 << 10;

pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16;

pub const FUSE_POSIX_ACL: u32 = 1 << 20;

/// FUSE init capability bit `1 << 11` (uapi `fuse.h`): the kernel supports
/// ioctl on directories. NOTE: RENAME2 is an *opcode* (`FUSE_RENAME2 = 45`),
/// not an init flag — bit 11 is `FUSE_HAS_IOCTL_DIR`. Not yet negotiated by
/// this daemon.
pub const FUSE_HAS_IOCTL_DIR: u32 = 1 << 11;

/// Kernel automatically invalidates the page cache on open when mtime or size
/// has changed (CAP_AUTO_INVAL_DATA, available since Linux 2.6.35).
pub const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12;

/// Kernel exportfs support: enables `name_to_handle_at` / `open_by_handle_at` via
/// `LOOKUP(nodeid, ".")` / `LOOKUP(nodeid, "..")` reconstruction.
pub const FUSE_EXPORT_SUPPORT: u32 = 1 << 4;

/// Kernel init capability bit: the daemon handles O_TRUNC atomically inside
/// `open`, so the kernel skips the follow-up SETATTR(size=0). Curvine's `open`
/// does NOT truncate, so this bit must never be advertised -- otherwise
/// O_TRUNC is silently lost when a writer for the inode already exists (the
/// shared writer ignores the second open's flags). See issue #1122.
///
/// Value `1 << 3` per:
///   - linux/fuse.h:339        `#define FUSE_ATOMIC_O_TRUNC     (1 << 3)`
///   - fuse3/fuse_common.h:158 `#define FUSE_CAP_ATOMIC_O_TRUNC (1 << 3)`
pub const FUSE_ATOMIC_O_TRUNC: u32 = 1 << 3;

/// Init capabilities the daemon actually implements, negotiated as an explicit
/// allowlist: `init` advertises `SUPPORTED_INIT_FLAGS & op.arg.flags` (only what
/// BOTH the daemon supports and the kernel offered), instead of blindly echoing
/// every kernel-offered flag. Each bit here maps to a real implementation:
/// ASYNC_READ/ASYNC_DIO (async read + direct-io pipeline), BIG_WRITES (writer
/// handles >4KiB writes), AUTO_INVAL_DATA (kernel auto-invalidates page cache on
/// open), EXPORT_SUPPORT (`.`/`..` lookup reconstruction), READDIRPLUS_AUTO +
/// DO_READDIRPLUS (`read_dir_plus`), POSIX_LOCKS + FLOCK_LOCKS
/// (`get_lk`/`set_lk`/`set_lkw`), MAX_PAGES (negotiated only if the kernel offers it).
///
/// Deliberately EXCLUDED (never advertised): FUSE_ATOMIC_O_TRUNC (open does not
/// truncate, #1122), FUSE_POSIX_ACL (no ACL handling), FUSE_HAS_IOCTL_DIR (no
/// ioctl), and any unknown/future kernel bit. FUSE_WRITEBACK_CACHE and the
/// FUSE_SPLICE_* bits are config-gated daemon-requested caps handled separately
/// (see `negotiate_out_flags`), not part of this kernel-masked allowlist.
pub const SUPPORTED_INIT_FLAGS: u32 = FUSE_ASYNC_READ
    | FUSE_BIG_WRITES
    | FUSE_ASYNC_DIO
    | FUSE_AUTO_INVAL_DATA
    | FUSE_EXPORT_SUPPORT
    | FUSE_READDIRPLUS_AUTO
    | FUSE_DO_READDIRPLUS
    | FUSE_POSIX_LOCKS
    | FUSE_FLOCK_LOCKS
    | FUSE_MAX_PAGES;

/// Human-readable names of the FUSE init-capability bits set in `flags`, for
/// logging the negotiated capability set. Known bits are named; any leftover
/// unknown bits are appended as a single `0x…` hex token so nothing is hidden.
pub fn fuse_init_flag_names(flags: u32) -> Vec<String> {
    const KNOWN: &[(u32, &str)] = &[
        (FUSE_ASYNC_READ, "ASYNC_READ"),
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

/// Minimum FUSE ABI (major, minor) the daemon accepts. curvine only implements
/// the 7.31 struct layout / semantics, so it rejects older kernels and, on
/// negotiation, advertises exactly this version rather than echoing the kernel.
pub const FUSE_MIN_ABI: (u32, u32) = (FUSE_KERNEL_VERSION, FUSE_KERNEL_MINOR_VERSION);

pub const FUSE_MAX_NAME_LENGTH: usize = 255;

/// Placeholder for the statfs `files`/`ffree` counts (total/free inodes) when the
/// count is unknown — Curvine's distributed backend keeps no global inode
/// statistics. The kernel passes this value through verbatim (it is not a
/// kernel-recognized "unknown" sentinel), so `df -i` reports it as a large count.
/// Distinct from `FUSE_UNKNOWN_INO` (an inode-number sentinel) despite the equal value.
pub const FUSE_UNKNOWN_INODES: u64 = 0xffffffff;

pub const FUSE_CURRENT_DIR: &str = ".";

pub const FUSE_PARENT_DIR: &str = "..";

pub const FUSE_S_ISUID: u32 = 0x800;

pub const FUSE_S_ISGID: u32 = 0x400;

// Per-type default permission bits for synthetic entries such as `.` and `..`.
// Real backends (master ACL / UFS) set a mode, so these are not overrides for
// persisted permission mode 0000.
pub const FUSE_DEFAULT_FILE_MODE: u32 = 0o666; // regular files: rw, no exec
pub const FUSE_DEFAULT_DIR_MODE: u32 = 0o777; // dirs: rwx (exec needed to traverse)
pub const FUSE_DEFAULT_SYMLINK_MODE: u32 = 0o777; // symlink perm bits are ignored by the kernel

/// Sentinel "invalid inode number": used for synthetic `.`/`..` dirents and
/// reserved by `DirTree::next_id` so it is never handed out as a real inode.
/// A non-zero sentinel is deliberate — `d_ino == 0` can be filtered by filldir.
/// Distinct from `FUSE_UNKNOWN_INODES` (a statfs count) despite the equal value.
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

pub const FATTR_ATIME_NOW: u32 = 1 << 7;

pub const FATTR_MTIME_NOW: u32 = 1 << 8;

// The minimum version of the clone fd feature can be used.
pub const FUSE_CLONE_FD_MIN_VERSION: (u32, u32) = (4, 2);

pub const FUSE_NOTIFY_UNIQUE: u64 = 0;

pub const STATE_FILE_MAGIC: &[u8; 4] = b"cvfs";

pub const STATE_FILE_VERSION: u64 = 1;

pub static UNIX_KERNEL_VERSION: Lazy<(u32, u32)> = Lazy::new(FuseUtils::get_kernel_version);
