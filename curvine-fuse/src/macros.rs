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

// Create fuse error.
//
// Crate-internal macro: NOT `#[macro_export]`ed, because its expansion
// references crate-private helpers (`crate::fuse_error::normalize_errno` /
// `errno_label`) and `orpc::err_msg!`, which are not reachable from an external
// crate — exporting it would advertise an unusable public macro. It is made
// importable within the crate via `pub(crate) use err_fuse;` below (re-exported
// at the crate root in lib.rs) so existing `use crate::err_fuse;` imports keep
// working. All call sites are inside curvine-fuse.
//
// The errno expression is normalized to a positive POSIX errno by
// `FuseError::from_errno_msg` (via `normalize_errno`), so a negative / zero /
// out-of-range input can never produce an illegal FUSE reply frame. Errno
// normalization and construction live in `fuse_error.rs` — this macro is only
// the `Err(...)` sugar plus the default message.
macro_rules! err_fuse {
    ($errno:expr) => ({
        // Single-arg form: synthesize a default message with the symbolic errno
        // label (e.g. "err_fuse ENOSYS") using the normalized errno.
        let errno = $crate::fuse_error::normalize_errno($errno);
        let msg = orpc::err_msg!("err_fuse {}", $crate::fuse_error::errno_label(errno));
        Err($crate::FuseError::from_errno_msg(errno, msg.into()))
    });

    ($errno:expr, $msg:expr) => ({
        let msg = orpc::err_msg!("{}", $msg);
        Err($crate::FuseError::from_errno_msg($errno, msg.into()))
    });

    ($errno:expr, $f:tt, $($arg:expr),+) => ({
        let msg = orpc::err_msg!($f, $($arg),+);
        Err($crate::FuseError::from_errno_msg($errno, msg.into()))
    });
}

// Make the crate-internal macro importable via a path (`use crate::err_fuse;`)
// without `#[macro_export]`. Re-exported at the crate root in lib.rs.
pub(crate) use err_fuse;

#[cfg(test)]
mod test {
    use crate::FuseResult;

    // A positive libc errno is preserved unchanged, and the single-arg default
    // message carries the symbolic errno label (not the raw number).
    #[test]
    fn err_fuse_positive_errno_preserved() {
        let err: FuseResult<u32> = err_fuse!(libc::ENOENT);
        let e = err.unwrap_err();
        assert_eq!(e.errno, libc::ENOENT);
        assert!(
            e.error.to_string().contains("ENOENT"),
            "single-arg default message should contain the errno label, got: {}",
            e.error
        );
    }

    // The format arm interpolates its arguments into the message.
    #[test]
    fn err_fuse_formats_message() {
        let err: FuseResult<u32> = err_fuse!(libc::EINVAL, "bad {}", 5);
        let e = err.unwrap_err();
        assert_eq!(e.errno, libc::EINVAL);
        assert!(
            e.error.to_string().contains("bad 5"),
            "message should contain formatted args, got: {}",
            e.error
        );
    }

    // Illegal errno (out of i32 range / zero / negative) is normalized by
    // `normalize_errno`: it trips a `debug_assert` in debug/test builds (exposing
    // the caller bug) and falls back to EIO in release. This mirrors the crate's
    // double-reply guard pattern (debug panics, release is safe). The release
    // fallback is asserted in the `#[cfg(not(debug_assertions))]` variants below.

    // Debug builds: an out-of-range errno panics via the debug_assert.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "out of i32 range")]
    fn err_fuse_oversized_errno_panics_in_debug() {
        let _: FuseResult<u32> = err_fuse!(u64::MAX);
    }

    // Debug builds: a zero errno panics via the debug_assert.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "out of i32 range")]
    fn err_fuse_zero_errno_panics_in_debug() {
        let _: FuseResult<u32> = err_fuse!(0_usize);
    }

    // Debug builds: a negative errno panics via the debug_assert (the direct
    // illegal-FUSE-frame case: it would otherwise encode as +errno).
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "out of i32 range")]
    fn err_fuse_negative_errno_panics_in_debug() {
        let _: FuseResult<u32> = err_fuse!(-libc::ENOENT);
    }

    // Release builds: illegal errno falls back to EIO instead of encoding an
    // illegal frame or truncating via `as`.
    #[cfg(not(debug_assertions))]
    #[test]
    fn err_fuse_illegal_errno_falls_back_to_eio() {
        assert_eq!(
            (err_fuse!(u64::MAX) as FuseResult<u32>).unwrap_err().errno,
            libc::EIO
        );
        assert_eq!(
            (err_fuse!(0_usize) as FuseResult<u32>).unwrap_err().errno,
            libc::EIO
        );
        assert_eq!(
            (err_fuse!(-libc::ENOENT) as FuseResult<u32>)
                .unwrap_err()
                .errno,
            libc::EIO
        );
    }
}
