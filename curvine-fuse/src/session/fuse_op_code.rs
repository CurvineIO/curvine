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

use num_enum::{FromPrimitive, IntoPrimitive};

#[repr(u32)]
#[derive(Debug, FromPrimitive, IntoPrimitive, Copy, Clone, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum FuseOpCode {
    #[num_enum(default)]
    NOT_SUPPORTED = 0,

    FUSE_LOOKUP = 1,
    FUSE_FORGET = 2, // no reply
    FUSE_GETATTR = 3,
    FUSE_SETATTR = 4,
    FUSE_READLINK = 5,
    FUSE_SYMLINK = 6,
    FUSE_MKNOD = 8,
    FUSE_MKDIR = 9,
    FUSE_UNLINK = 10,
    FUSE_RMDIR = 11,
    FUSE_RENAME = 12,
    FUSE_LINK = 13,
    FUSE_OPEN = 14,
    FUSE_READ = 15,
    FUSE_WRITE = 16,
    FUSE_STATFS = 17,
    FUSE_RELEASE = 18,
    FUSE_FSYNC = 20,
    FUSE_SETXATTR = 21,
    FUSE_GETXATTR = 22,
    FUSE_LISTXATTR = 23,
    FUSE_REMOVEXATTR = 24,
    FUSE_FLUSH = 25,
    FUSE_INIT = 26,
    FUSE_OPENDIR = 27,
    FUSE_READDIR = 28,
    FUSE_RELEASEDIR = 29,
    FUSE_FSYNCDIR = 30,
    FUSE_GETLK = 31,
    FUSE_SETLK = 32,
    FUSE_SETLKW = 33,
    FUSE_ACCESS = 34,
    FUSE_CREATE = 35,
    FUSE_INTERRUPT = 36,
    FUSE_BMAP = 37,
    FUSE_DESTROY = 38,
    FUSE_IOCTL = 39,
    FUSE_POLL = 40,
    FUSE_NOTIFY_REPLY = 41,
    FUSE_BATCH_FORGET = 42,
    FUSE_FALLOCATE = 43,
    FUSE_READDIRPLUS = 44,
    FUSE_RENAME2 = 45,
    FUSE_LSEEK = 46,

    CUSE_INIT = 4096,
}

impl FuseOpCode {
    /// Returns a stable, low-cardinality `&'static str` name for this opcode,
    /// suitable for use as a metric label. Zero-allocation: the metrics hot
    /// path must never `format!("{:?}", op)`.
    ///
    /// Names are short CamelCase (e.g. `Lookup`, `GetAttr`, `Read`) matching
    /// the `opcode` label convention in the FUSE metrics design.
    // Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
    #[allow(dead_code)]
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FuseOpCode::NOT_SUPPORTED => "NotSupported",
            FuseOpCode::FUSE_LOOKUP => "Lookup",
            FuseOpCode::FUSE_FORGET => "Forget",
            FuseOpCode::FUSE_GETATTR => "GetAttr",
            FuseOpCode::FUSE_SETATTR => "SetAttr",
            FuseOpCode::FUSE_READLINK => "Readlink",
            FuseOpCode::FUSE_SYMLINK => "Symlink",
            FuseOpCode::FUSE_MKNOD => "MkNod",
            FuseOpCode::FUSE_MKDIR => "Mkdir",
            FuseOpCode::FUSE_UNLINK => "Unlink",
            FuseOpCode::FUSE_RMDIR => "RmDir",
            FuseOpCode::FUSE_RENAME => "Rename",
            FuseOpCode::FUSE_LINK => "Link",
            FuseOpCode::FUSE_OPEN => "Open",
            FuseOpCode::FUSE_READ => "Read",
            FuseOpCode::FUSE_WRITE => "Write",
            FuseOpCode::FUSE_STATFS => "StatFs",
            FuseOpCode::FUSE_RELEASE => "Release",
            FuseOpCode::FUSE_FSYNC => "Fsync",
            FuseOpCode::FUSE_SETXATTR => "SetXAttr",
            FuseOpCode::FUSE_GETXATTR => "GetXAttr",
            FuseOpCode::FUSE_LISTXATTR => "ListXAttr",
            FuseOpCode::FUSE_REMOVEXATTR => "RemoveXAttr",
            FuseOpCode::FUSE_FLUSH => "Flush",
            FuseOpCode::FUSE_INIT => "Init",
            FuseOpCode::FUSE_OPENDIR => "OpenDir",
            FuseOpCode::FUSE_READDIR => "ReadDir",
            FuseOpCode::FUSE_RELEASEDIR => "ReleaseDir",
            FuseOpCode::FUSE_FSYNCDIR => "FsyncDir",
            FuseOpCode::FUSE_GETLK => "GetLk",
            FuseOpCode::FUSE_SETLK => "SetLk",
            FuseOpCode::FUSE_SETLKW => "SetLkW",
            FuseOpCode::FUSE_ACCESS => "Access",
            FuseOpCode::FUSE_CREATE => "Create",
            FuseOpCode::FUSE_INTERRUPT => "Interrupt",
            FuseOpCode::FUSE_BMAP => "BMap",
            FuseOpCode::FUSE_DESTROY => "Destroy",
            FuseOpCode::FUSE_IOCTL => "Ioctl",
            FuseOpCode::FUSE_POLL => "Poll",
            FuseOpCode::FUSE_NOTIFY_REPLY => "NotifyReply",
            FuseOpCode::FUSE_BATCH_FORGET => "BatchForget",
            FuseOpCode::FUSE_FALLOCATE => "FAllocate",
            FuseOpCode::FUSE_READDIRPLUS => "ReadDirPlus",
            FuseOpCode::FUSE_RENAME2 => "Rename2",
            FuseOpCode::FUSE_LSEEK => "Lseek",
            FuseOpCode::CUSE_INIT => "CuseInit",
        }
    }
}

/// The dispatch status of an opcode: an opcode-level record of whether an
/// opcode is handled, and — for the unhandled ones — whether that is deliberate
/// (`Unsupported`) or protocol-internal (`Internal`), with the rationale.
///
/// The compile-time guarantee that a *parsed* operator actually has a dispatch
/// arm lives in the exhaustive (no-`_`) matches in `dispatch_meta` and
/// `send_stream_dispatch`: adding a `FuseOperator` variant with no arm fails to
/// compile there. This matrix complements that with the opcode-level intent
/// (why BMAP/POLL/etc. are ENOSYS, which opcodes are protocol-internal); its own
/// exhaustive match likewise forces a newly-added `FuseOpCode` to be classified.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum DispatchStatus {
    /// Has a real `dispatch_meta`/`send_stream_dispatch` arm.
    Handled,
    /// Dispatched via `send_none` (no reply to the kernel).
    NoReply,
    /// Intentionally falls through the dispatch wildcard to ENOSYS.
    Unsupported,
    /// Not dispatched to a `FileSystem` op here (protocol/enum-internal).
    Internal,
}

#[cfg(test)]
impl FuseOpCode {
    pub(crate) fn expected_dispatch(&self) -> DispatchStatus {
        use DispatchStatus::*;
        match self {
            // No-reply ops (send_none).
            FuseOpCode::FUSE_FORGET
            | FuseOpCode::FUSE_BATCH_FORGET
            | FuseOpCode::FUSE_INTERRUPT => NoReply,

            // Intentionally unsupported (ENOSYS via wildcard):
            //   - FUSE_BMAP:  logical->physical block map, only for block-backed
            //     (fuseblk) filesystems; meaningless for a distributed fs.
            //   - FUSE_POLL:  readiness for special/char files; regular files are
            //     always ready, so the VFS never needs it here.
            //   - FUSE_IOCTL: device/fs-specific ioctls; no passthrough (and
            //     FUSE_HAS_IOCTL_DIR is deliberately not negotiated in init).
            //   - FUSE_LSEEK: SEEK_HOLE/SEEK_DATA only; ENOSYS is a safe VFS
            //     fallback (kernel treats the file as hole-less).
            FuseOpCode::FUSE_BMAP
            | FuseOpCode::FUSE_POLL
            | FuseOpCode::FUSE_IOCTL
            | FuseOpCode::FUSE_LSEEK => Unsupported,

            // Not dispatched as a FileSystem op: NOT_SUPPORTED is the num_enum
            // default for unknown raw opcodes; CUSE_INIT is a CUSE handshake, not
            // a filesystem path; NOTIFY_REPLY is a daemon->kernel notify channel.
            // These have no `parse_operator` arm, so they never reach the
            // dispatcher during normal operation. If one ever did, it would fall
            // through the dispatch wildcard like `Unsupported` (NOT_SUPPORTED as
            // `unknown_opcode`, the others as `unimplemented_opcode`); `Internal`
            // records that we intentionally do not route them, not that reaching
            // dispatch is impossible.
            FuseOpCode::NOT_SUPPORTED | FuseOpCode::CUSE_INIT | FuseOpCode::FUSE_NOTIFY_REPLY => {
                Internal
            }

            // Everything else has a real dispatch arm.
            FuseOpCode::FUSE_LOOKUP
            | FuseOpCode::FUSE_GETATTR
            | FuseOpCode::FUSE_SETATTR
            | FuseOpCode::FUSE_READLINK
            | FuseOpCode::FUSE_SYMLINK
            | FuseOpCode::FUSE_MKNOD
            | FuseOpCode::FUSE_MKDIR
            | FuseOpCode::FUSE_UNLINK
            | FuseOpCode::FUSE_RMDIR
            | FuseOpCode::FUSE_RENAME
            | FuseOpCode::FUSE_LINK
            | FuseOpCode::FUSE_OPEN
            | FuseOpCode::FUSE_READ
            | FuseOpCode::FUSE_WRITE
            | FuseOpCode::FUSE_STATFS
            | FuseOpCode::FUSE_RELEASE
            | FuseOpCode::FUSE_FSYNC
            | FuseOpCode::FUSE_SETXATTR
            | FuseOpCode::FUSE_GETXATTR
            | FuseOpCode::FUSE_LISTXATTR
            | FuseOpCode::FUSE_REMOVEXATTR
            | FuseOpCode::FUSE_FLUSH
            | FuseOpCode::FUSE_INIT
            | FuseOpCode::FUSE_OPENDIR
            | FuseOpCode::FUSE_READDIR
            | FuseOpCode::FUSE_RELEASEDIR
            | FuseOpCode::FUSE_FSYNCDIR
            | FuseOpCode::FUSE_GETLK
            | FuseOpCode::FUSE_SETLK
            | FuseOpCode::FUSE_SETLKW
            | FuseOpCode::FUSE_ACCESS
            | FuseOpCode::FUSE_CREATE
            | FuseOpCode::FUSE_DESTROY
            | FuseOpCode::FUSE_FALLOCATE
            | FuseOpCode::FUSE_READDIRPLUS
            | FuseOpCode::FUSE_RENAME2 => Handled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FuseOpCode;

    #[test]
    fn as_str_matches_the_full_label_table() {
        // Full (variant, expected-label) table covering every enum variant.
        // Any accidental relabeling must update this table (and the design
        // doc's `opcode` label convention) together, so a Prometheus series
        // name can never drift silently.
        let table = [
            (FuseOpCode::NOT_SUPPORTED, "NotSupported"),
            (FuseOpCode::FUSE_LOOKUP, "Lookup"),
            (FuseOpCode::FUSE_FORGET, "Forget"),
            (FuseOpCode::FUSE_GETATTR, "GetAttr"),
            (FuseOpCode::FUSE_SETATTR, "SetAttr"),
            (FuseOpCode::FUSE_READLINK, "Readlink"),
            (FuseOpCode::FUSE_SYMLINK, "Symlink"),
            (FuseOpCode::FUSE_MKNOD, "MkNod"),
            (FuseOpCode::FUSE_MKDIR, "Mkdir"),
            (FuseOpCode::FUSE_UNLINK, "Unlink"),
            (FuseOpCode::FUSE_RMDIR, "RmDir"),
            (FuseOpCode::FUSE_RENAME, "Rename"),
            (FuseOpCode::FUSE_LINK, "Link"),
            (FuseOpCode::FUSE_OPEN, "Open"),
            (FuseOpCode::FUSE_READ, "Read"),
            (FuseOpCode::FUSE_WRITE, "Write"),
            (FuseOpCode::FUSE_STATFS, "StatFs"),
            (FuseOpCode::FUSE_RELEASE, "Release"),
            (FuseOpCode::FUSE_FSYNC, "Fsync"),
            (FuseOpCode::FUSE_SETXATTR, "SetXAttr"),
            (FuseOpCode::FUSE_GETXATTR, "GetXAttr"),
            (FuseOpCode::FUSE_LISTXATTR, "ListXAttr"),
            (FuseOpCode::FUSE_REMOVEXATTR, "RemoveXAttr"),
            (FuseOpCode::FUSE_FLUSH, "Flush"),
            (FuseOpCode::FUSE_INIT, "Init"),
            (FuseOpCode::FUSE_OPENDIR, "OpenDir"),
            (FuseOpCode::FUSE_READDIR, "ReadDir"),
            (FuseOpCode::FUSE_RELEASEDIR, "ReleaseDir"),
            (FuseOpCode::FUSE_FSYNCDIR, "FsyncDir"),
            (FuseOpCode::FUSE_GETLK, "GetLk"),
            (FuseOpCode::FUSE_SETLK, "SetLk"),
            (FuseOpCode::FUSE_SETLKW, "SetLkW"),
            (FuseOpCode::FUSE_ACCESS, "Access"),
            (FuseOpCode::FUSE_CREATE, "Create"),
            (FuseOpCode::FUSE_INTERRUPT, "Interrupt"),
            (FuseOpCode::FUSE_BMAP, "BMap"),
            (FuseOpCode::FUSE_DESTROY, "Destroy"),
            (FuseOpCode::FUSE_IOCTL, "Ioctl"),
            (FuseOpCode::FUSE_POLL, "Poll"),
            (FuseOpCode::FUSE_NOTIFY_REPLY, "NotifyReply"),
            (FuseOpCode::FUSE_BATCH_FORGET, "BatchForget"),
            (FuseOpCode::FUSE_FALLOCATE, "FAllocate"),
            (FuseOpCode::FUSE_READDIRPLUS, "ReadDirPlus"),
            (FuseOpCode::FUSE_RENAME2, "Rename2"),
            (FuseOpCode::FUSE_LSEEK, "Lseek"),
            (FuseOpCode::CUSE_INIT, "CuseInit"),
        ];
        for (op, expected) in table {
            assert_eq!(op.as_str(), expected, "label mismatch for {:?}", op);
        }
    }

    #[test]
    fn expected_dispatch_classifies_every_opcode() {
        use super::DispatchStatus::*;
        // Full (variant, expected-status) table. `expected_dispatch` is an
        // exhaustive match, so adding a FuseOpCode variant already fails to
        // compile until classified there; this table additionally pins the
        // intended status so a reclassification is a conscious edit.
        let table = [
            (FuseOpCode::NOT_SUPPORTED, Internal),
            (FuseOpCode::FUSE_LOOKUP, Handled),
            (FuseOpCode::FUSE_FORGET, NoReply),
            (FuseOpCode::FUSE_GETATTR, Handled),
            (FuseOpCode::FUSE_SETATTR, Handled),
            (FuseOpCode::FUSE_READLINK, Handled),
            (FuseOpCode::FUSE_SYMLINK, Handled),
            (FuseOpCode::FUSE_MKNOD, Handled),
            (FuseOpCode::FUSE_MKDIR, Handled),
            (FuseOpCode::FUSE_UNLINK, Handled),
            (FuseOpCode::FUSE_RMDIR, Handled),
            (FuseOpCode::FUSE_RENAME, Handled),
            (FuseOpCode::FUSE_LINK, Handled),
            (FuseOpCode::FUSE_OPEN, Handled),
            (FuseOpCode::FUSE_READ, Handled),
            (FuseOpCode::FUSE_WRITE, Handled),
            (FuseOpCode::FUSE_STATFS, Handled),
            (FuseOpCode::FUSE_RELEASE, Handled),
            (FuseOpCode::FUSE_FSYNC, Handled),
            (FuseOpCode::FUSE_SETXATTR, Handled),
            (FuseOpCode::FUSE_GETXATTR, Handled),
            (FuseOpCode::FUSE_LISTXATTR, Handled),
            (FuseOpCode::FUSE_REMOVEXATTR, Handled),
            (FuseOpCode::FUSE_FLUSH, Handled),
            (FuseOpCode::FUSE_INIT, Handled),
            (FuseOpCode::FUSE_OPENDIR, Handled),
            (FuseOpCode::FUSE_READDIR, Handled),
            (FuseOpCode::FUSE_RELEASEDIR, Handled),
            (FuseOpCode::FUSE_FSYNCDIR, Handled),
            (FuseOpCode::FUSE_GETLK, Handled),
            (FuseOpCode::FUSE_SETLK, Handled),
            (FuseOpCode::FUSE_SETLKW, Handled),
            (FuseOpCode::FUSE_ACCESS, Handled),
            (FuseOpCode::FUSE_CREATE, Handled),
            (FuseOpCode::FUSE_INTERRUPT, NoReply),
            (FuseOpCode::FUSE_BMAP, Unsupported),
            (FuseOpCode::FUSE_DESTROY, Handled),
            (FuseOpCode::FUSE_IOCTL, Unsupported),
            (FuseOpCode::FUSE_POLL, Unsupported),
            (FuseOpCode::FUSE_NOTIFY_REPLY, Internal),
            (FuseOpCode::FUSE_BATCH_FORGET, NoReply),
            (FuseOpCode::FUSE_FALLOCATE, Handled),
            (FuseOpCode::FUSE_READDIRPLUS, Handled),
            (FuseOpCode::FUSE_RENAME2, Handled),
            (FuseOpCode::FUSE_LSEEK, Unsupported),
            (FuseOpCode::CUSE_INIT, Internal),
        ];
        for (op, expected) in table {
            assert_eq!(
                op.expected_dispatch(),
                expected,
                "dispatch status mismatch for {:?}",
                op
            );
        }
    }
}
