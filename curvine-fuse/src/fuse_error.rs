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

use curvine_common::error::FsError;
use orpc::io::IOError;
use orpc::CommonError;
use std::fmt;
use std::fmt::Debug;
use tokio::time::error::Elapsed;

#[derive(Debug)]
pub struct FuseError {
    pub(crate) errno: i32,
    pub(crate) error: CommonError,
}

impl FuseError {
    // crate-private: the public safe constructor is `from_errno_msg`, which
    // normalizes the errno. Keeping `new` internal prevents external/future code
    // from bypassing normalization and building an illegal (non-positive) errno.
    pub(crate) fn new(errno: i32, error: CommonError) -> Self {
        // Internal invariant: FuseError always holds a positive POSIX errno.
        // `ResponseData::create` negates it (`-error`) when encoding the reply, so
        // a negative/zero errno here would produce an illegal FUSE reply frame.
        // Surfaced loudly in debug; release callers should route arbitrary integers
        // through `err_fuse!` / `from_errno_msg`, which normalize the value.
        debug_assert!(
            errno > 0,
            "FuseError errno must be a positive POSIX errno, got {}",
            errno
        );
        Self { errno, error }
    }

    /// Build a `FuseError` from an arbitrary integer errno, normalizing it to a
    /// positive POSIX errno via [`normalize_errno`]. This is the construction
    /// path used by `err_fuse!`; it also lets callers that need a `FuseError`
    /// value (not a `Result`) build one directly — e.g. before passing it to
    /// `send_rep_tagged` — without the `let _: FuseResult<_> = err_fuse!(...)`
    /// detour.
    pub fn from_errno_msg<E: TryInto<i32>>(errno: E, error: CommonError) -> Self {
        Self::new(normalize_errno(errno), error)
    }

    /// The POSIX errno this error maps to. Used by the metrics finish path to
    /// stash the errno label source.
    pub(crate) fn errno(&self) -> i32 {
        self.errno
    }
}

/// Normalize an arbitrary integer errno into a positive POSIX errno.
///
/// Uses `TryInto<i32>` for a genuine range check (no `as` truncation/wrapping):
/// a value that does not fit in `i32`, or is non-positive, falls back to `EIO`.
/// This keeps `FuseError`'s "positive errno" invariant and prevents an illegal
/// FUSE reply frame (the reply path negates the errno). Release-safe — never
/// panics — but a non-positive / out-of-range input trips a `debug_assert` so a
/// caller bug is surfaced in debug/test builds rather than silently coerced.
pub(crate) fn normalize_errno<E: TryInto<i32>>(errno: E) -> i32 {
    match errno.try_into() {
        Ok(e) if e > 0 => e,
        _ => {
            debug_assert!(
                false,
                "err_fuse! errno is non-positive or out of i32 range; falling back to EIO"
            );
            libc::EIO
        }
    }
}

impl From<String> for FuseError {
    fn from(value: String) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

impl std::error::Error for FuseError {}

impl fmt::Display for FuseError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "errno {}: {}", self.errno, self.error)
    }
}

impl From<&str> for FuseError {
    fn from(value: &str) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

impl From<CommonError> for FuseError {
    fn from(value: CommonError) -> Self {
        Self::new(libc::EIO, value)
    }
}

impl From<IOError> for FuseError {
    fn from(value: IOError) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

impl From<FsError> for FuseError {
    fn from(value: FsError) -> Self {
        // Map well-known FsError kinds directly to POSIX errno
        let mapped = match &value {
            FsError::FileAlreadyExists(_) => Some(libc::EEXIST),
            FsError::FileNotFound(_) => Some(libc::ENOENT),
            FsError::DirNotEmpty(_) => Some(libc::ENOTEMPTY),
            FsError::IsADirectory(_) => Some(libc::EISDIR),
            FsError::ParentNotDir(_) => Some(libc::ENOTDIR),
            FsError::NotADirectory(_) => Some(libc::ENOTDIR),
            FsError::InvalidPath(_) => Some(libc::EINVAL),
            FsError::InvalidFileSize(_) => Some(libc::EFBIG),
            FsError::InvalidArgument(_) => Some(libc::EINVAL),
            FsError::DiskOutOfSpace(_) => Some(libc::ENOSPC),
            FsError::Timeout(_) => Some(libc::ETIMEDOUT),
            FsError::Unsupported(_) => Some(libc::ENOSYS),
            FsError::InProgress(_) => Some(libc::EBUSY),
            FsError::UnsupportedUfsRead(_) => Some(libc::EOPNOTSUPP),
            FsError::ReadOnly(_) => Some(libc::EROFS),
            _ => None,
        };

        if let Some(errno) = mapped {
            return Self::new(errno, value.into());
        }

        // Fallback: infer from message content for UFS/common errors (e.g., opendal PermissionDenied)
        let msg = value.to_string().to_lowercase();
        if msg.contains("permission denied") || msg.contains("os error 13") {
            return Self::new(libc::EACCES, value.into());
        }
        if msg.contains("not implemented") || msg.contains("unsupported") {
            return Self::new(libc::ENOSYS, value.into());
        }

        // Default to EIO
        Self::new(libc::EIO, value.into())
    }
}

impl From<Elapsed> for FuseError {
    fn from(value: Elapsed) -> Self {
        // A tokio timeout is a timeout, same as FsError::Timeout -> ETIMEDOUT;
        // keep both conversion paths consistent instead of degrading to EIO.
        Self::new(libc::ETIMEDOUT, value.into())
    }
}

impl From<std::io::Error> for FuseError {
    fn from(value: std::io::Error) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

/// Maps an errno integer to a stable, low-cardinality symbolic `&'static str`
/// label, suitable for the `errno` label on error metrics. Zero-allocation: the
/// metrics hot path must never format the raw integer or a libc-locale string.
///
/// The table is the closed set of errnos actually produced in the FUSE layer
/// (verified against `libc::E*` usage across `curvine-fuse/src/` plus the
/// `FsError -> errno` mapping above). Any errno outside the table collapses to
/// `"OTHER"`; if a new errno starts being produced, add it here and to the
/// design doc's label table together.
// Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
#[allow(dead_code)]
pub(crate) fn errno_label(errno: i32) -> &'static str {
    match errno {
        libc::ENOENT => "ENOENT",
        libc::EIO => "EIO",
        libc::EINTR => "EINTR",
        libc::ENOSYS => "ENOSYS",
        libc::EAGAIN => "EAGAIN",
        libc::EACCES => "EACCES",
        libc::EBADF => "EBADF",
        libc::EINVAL => "EINVAL",
        libc::EPERM => "EPERM",
        libc::EOPNOTSUPP => "EOPNOTSUPP",
        libc::EEXIST => "EEXIST",
        libc::ENOTDIR => "ENOTDIR",
        libc::EISDIR => "EISDIR",
        libc::ENOTEMPTY => "ENOTEMPTY",
        libc::ETIMEDOUT => "ETIMEDOUT",
        libc::ENOSPC => "ENOSPC",
        libc::ETXTBSY => "ETXTBSY",
        libc::EPROTO => "EPROTO",
        libc::ERANGE => "ERANGE",
        libc::ENODATA => "ENODATA",
        libc::ENOMEM => "ENOMEM",
        libc::EBUSY => "EBUSY",
        libc::ENAMETOOLONG => "ENAMETOOLONG",
        libc::EFBIG => "EFBIG",
        libc::EROFS => "EROFS",
        _ => "OTHER",
    }
}

/// Low-cardinality lowercase label for a splice/receive errno, used by
/// `receive_errors_total{errno}`. Deliberately separate from [`errno_label`]:
/// receive errors occur before a request is decoded and the design fixes this
/// to the small lowercase set the receiver loop actually matches on, rather
/// than the full uppercase POSIX table.
pub(crate) fn splice_errno_label(errno: i32) -> &'static str {
    match errno {
        libc::ENOENT => "enoent",
        libc::EINTR => "eintr",
        libc::EAGAIN => "eagain",
        libc::ENODEV => "enodev",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::{errno_label, splice_errno_label, FuseError};
    use curvine_common::error::FsError;

    // Display prints the errno NUMBER first, then the message ("errno 2: ...").
    #[test]
    fn display_prints_errno_before_message() {
        let err = FuseError::new(libc::ENOENT, "no such file".into());
        let s = format!("{}", err);
        assert!(
            s.starts_with(&format!("errno {}: ", libc::ENOENT)),
            "expected errno number first, got: {}",
            s
        );
        assert!(s.contains("no such file"));
    }

    // A tokio timeout maps to ETIMEDOUT, consistent with FsError::Timeout — not EIO.
    #[tokio::test]
    async fn elapsed_maps_to_etimedout() {
        let elapsed = tokio::time::timeout(
            std::time::Duration::from_millis(0),
            std::future::pending::<()>(),
        )
        .await
        .expect_err("zero-duration timeout must elapse");
        let err: FuseError = elapsed.into();
        assert_eq!(err.errno, libc::ETIMEDOUT);
        assert_eq!(errno_label(err.errno), "ETIMEDOUT");
    }

    #[test]
    fn invalid_file_size_maps_to_efbig() {
        let err: FuseError = FsError::file_too_large(1 << 60).into();
        assert_eq!(err.errno, libc::EFBIG);
        assert_eq!(errno_label(err.errno), "EFBIG");
    }

    #[test]
    fn read_only_maps_to_erofs() {
        let err: FuseError = FsError::read_only("/mnt/ro").into();
        assert_eq!(err.errno, libc::EROFS);
        assert_eq!(errno_label(err.errno), "EROFS");
    }

    #[test]
    fn errno_label_maps_the_closed_set() {
        // Full (errno, expected-label) table. Any accidental relabeling must
        // update this table and the design doc's errno Label rules together.
        let table = [
            (libc::ENOENT, "ENOENT"),
            (libc::EIO, "EIO"),
            (libc::EINTR, "EINTR"),
            (libc::ENOSYS, "ENOSYS"),
            (libc::EAGAIN, "EAGAIN"),
            (libc::EACCES, "EACCES"),
            (libc::EBADF, "EBADF"),
            (libc::EINVAL, "EINVAL"),
            (libc::EPERM, "EPERM"),
            (libc::EOPNOTSUPP, "EOPNOTSUPP"),
            (libc::EEXIST, "EEXIST"),
            (libc::ENOTDIR, "ENOTDIR"),
            (libc::EISDIR, "EISDIR"),
            (libc::ENOTEMPTY, "ENOTEMPTY"),
            (libc::ETIMEDOUT, "ETIMEDOUT"),
            (libc::ENOSPC, "ENOSPC"),
            (libc::ETXTBSY, "ETXTBSY"),
            (libc::EPROTO, "EPROTO"),
            (libc::ERANGE, "ERANGE"),
            (libc::ENODATA, "ENODATA"),
            (libc::ENOMEM, "ENOMEM"),
            (libc::EBUSY, "EBUSY"),
            (libc::ENAMETOOLONG, "ENAMETOOLONG"),
            (libc::EFBIG, "EFBIG"),
            (libc::EROFS, "EROFS"),
        ];
        for (e, expected) in table {
            assert_eq!(errno_label(e), expected, "label mismatch for errno {}", e);
        }
    }

    #[test]
    fn unmapped_errno_falls_back_to_other() {
        // An errno not in the table (e.g. ELOOP) collapses to OTHER.
        assert_eq!(errno_label(libc::ELOOP), "OTHER");
        assert_eq!(errno_label(0), "OTHER");
        assert_eq!(errno_label(99999), "OTHER");
    }

    #[test]
    fn splice_errno_label_maps_the_lowercase_set() {
        // The 5 lowercase values the receiver loop actually matches on.
        assert_eq!(splice_errno_label(libc::ENOENT), "enoent");
        assert_eq!(splice_errno_label(libc::EINTR), "eintr");
        assert_eq!(splice_errno_label(libc::EAGAIN), "eagain");
        assert_eq!(splice_errno_label(libc::ENODEV), "enodev");
        // Anything else (incl. 0 / unknown) collapses to lowercase "other".
        assert_eq!(splice_errno_label(libc::EIO), "other");
        assert_eq!(splice_errno_label(0), "other");
        assert_eq!(splice_errno_label(99999), "other");
    }
}
