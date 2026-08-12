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

// Crate-internal fuse error macro. Not `#[macro_export]`ed: its expansion
// references crate-private helpers unreachable from an external crate.
macro_rules! err_fuse {
    ($errno:expr) => ({
        // Single-arg form: synthesize a default message with the symbolic errno
        // label (e.g. "err_fuse ENOSYS").
        let errno = $crate::fuse_error::normalize_errno($errno);
        let msg = curvine_core_error::err_msg!("err_fuse {}", $crate::fuse_error::errno_label(errno));
        Err($crate::FuseError::from_errno_msg(errno, msg.into()))
    });

    ($errno:expr, $msg:expr) => ({
        let msg = curvine_core_error::err_msg!("{}", $msg);
        Err($crate::FuseError::from_errno_msg($errno, msg.into()))
    });

    ($errno:expr, $f:tt, $($arg:expr),+) => ({
        let msg = curvine_core_error::err_msg!($f, $($arg),+);
        Err($crate::FuseError::from_errno_msg($errno, msg.into()))
    });
}

// Make the macro importable via `use crate::err_fuse;` without `#[macro_export]`.
pub(crate) use err_fuse;

#[cfg(test)]
mod test {
    use crate::FuseResult;

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

    // Illegal errno normalizes to EIO in release and trips debug assertions in tests.
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
