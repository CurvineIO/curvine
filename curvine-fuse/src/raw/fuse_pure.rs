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

//! Linux-only pure FUSE mount/unmount implementation.
//!
//! This module talks to the kernel directly via `libc::mount` / `libc::umount2`
//! (with a `fusermount` fallback for unmount). It has no macOS/BSD code path and
//! is not designed to compile off Linux, so the whole module is gated to
//! `target_os = "linux"` in `raw/mod.rs`. Because of that outer gate, the code
//! below can use Linux-specific APIs unconditionally without per-item `#[cfg]`.

use curvine_config::FuseConf;
use curvine_io::{IOError, IOResult};
use curvine_sys::RawIO;
use log::{error, info};
use nix::unistd::{getgid, getuid};
use std::ffi::CString;
use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::path::Path;

use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};

use curvine_sys::open;

const FUSERMOUNT_BIN: &str = "fusermount";
const FUSERMOUNT3_BIN: &str = "fusermount3";

// Convert a filesystem path into a NUL-terminated C string without assuming the
// path is valid UTF-8. Unix paths are arbitrary byte sequences, so we read the
// raw bytes via `OsStrExt::as_bytes` instead of `Path::to_str().unwrap()`, which
// would panic on non-UTF-8 paths. An embedded NUL byte is reported as a normal
// error (via `NulError` -> `IOError`) rather than aborting the process.
fn path_to_cstring(mnt: &Path) -> IOResult<CString> {
    CString::new(mnt.as_os_str().as_bytes()).map_err(|e| {
        IOError::with_ctx(
            std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("mount path {:?} contains an embedded NUL byte: {}", mnt, e),
            ),
            format!("({}:{})", file!(), line!()),
        )
    })
}

// Check whether a mount option exists as a standalone key (not a substring).
// For example, "ro" should match token "ro" but NOT "rootmode=...".
fn has_mount_opt(options: &str, key: &str) -> bool {
    options.split(',').any(|token| {
        let token = token.trim();
        if token.is_empty() {
            return false;
        }
        let k = token.split_once('=').map(|(k, _)| k).unwrap_or(token);
        k == key
    })
}

pub fn options_to_flag(mount_option: &str) -> libc::c_ulong {
    let mut flags = 0;
    if has_mount_opt(mount_option, "ro") {
        flags |= libc::MS_RDONLY;
    }
    if has_mount_opt(mount_option, "nodev") {
        flags |= libc::MS_NODEV;
    }
    if has_mount_opt(mount_option, "nosuid") {
        flags |= libc::MS_NOSUID;
    }
    if has_mount_opt(mount_option, "noexec") {
        flags |= libc::MS_NOEXEC;
    }
    if has_mount_opt(mount_option, "noatime") {
        flags |= libc::MS_NOATIME;
    }
    if has_mount_opt(mount_option, "dirsync") {
        flags |= libc::MS_DIRSYNC;
    }
    if has_mount_opt(mount_option, "sync") {
        flags |= libc::MS_SYNCHRONOUS;
    }

    flags
}

pub fn fuse_mount_pure(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    if conf.auto_umount() {
        // TODO: handle auto umount
    }
    let res = fuse_mount_sys(mnt, conf);
    match res {
        Ok(fd) => Ok(fd),
        Err(e) => {
            error!("fuse mount sys failed; path {:?}, err {:?}", mnt, e);
            // Preserve the original mount error captured by `fuse_mount_sys`
            // (e.g. the real EACCES/ENOENT/EBUSY errno). Do NOT re-read
            // `std::io::Error::last_os_error()` here: intervening operations
            // such as logging can clobber `errno`, turning a specific mount
            // failure into a generic error and losing useful diagnostics.
            Err(IOError::with_ctx(
                e.into_raw(),
                format!("({}:{})", file!(), line!()),
            ))
        }
    }
}

fn fuse_mount_sys(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    let fuse_device_name = "/dev/fuse";
    let mountpoint_mode = File::open(mnt)?.metadata()?.permissions().mode();

    // Auto unmount requests must be sent to fusermount binary
    let path = CString::new(fuse_device_name).unwrap();
    let res = open(&path, libc::O_RDWR | libc::O_CLOEXEC);
    let fd = match res {
        Ok(fd) => fd,
        Err(e) => {
            error!("Open fuse device failed, {}, err {:?}", fuse_device_name, e);
            return Err(std::io::Error::from(ErrorKind::Other).into());
        }
    };
    // SAFETY: open returned a fresh descriptor whose ownership is transferred here.
    // Keep ownership until mount succeeds so every failure path closes /dev/fuse.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut flags = 0;
    let mut mount_options = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        fd.as_raw_fd(),
        mountpoint_mode,
        getuid(),
        getgid()
    );
    conf.set_fuse_opts(&mut mount_options);
    // Set the FUSE subtype so /proc/mounts reports `type fuse.curvinefs` instead of
    // the generic `type fuse` (same mechanism sshfs uses for `fuse.sshfs`). `c_type`
    // below stays "fuse" (the only registered kernel FUSE fs type); the Linux kernel
    // FUSE module parses `subtype=` from this mount data and sets sb->s_subtype, and
    // the VFS then reports the type as `fuse.<subtype>`. Hardcoded to match the source
    // name above (c_source = "curvinefs").
    mount_options.push_str(",subtype=curvinefs");
    flags |= options_to_flag(mount_options.as_str());
    info!("sys-mount options: {}; flags: 0x{:x}", mount_options, flags);

    // Default name is "/dev/fuse", then use the subtype, and lastly prefer the name
    let c_source = CString::new("curvinefs").unwrap();
    let c_mountpoint = path_to_cstring(mnt)?;

    let result = unsafe {
        let c_options = CString::new(mount_options.clone()).unwrap();
        let c_type = CString::new("fuse").unwrap();
        libc::mount(
            c_source.as_ptr(),
            c_mountpoint.as_ptr(),
            c_type.as_ptr(),
            flags,
            c_options.as_ptr() as *const libc::c_void,
        )
    };

    complete_mount(fd, result, mnt)
}

fn complete_mount(fd: OwnedFd, result: libc::c_int, mnt: &Path) -> IOResult<RawIO> {
    if result != 0 {
        let error = std::io::Error::last_os_error();
        error!(
            "Mount fuse failed, {} with result {}",
            mnt.display(),
            result
        );
        return Err(error.into());
    }
    info!("Mounted at {}", mnt.display());
    Ok(fd.into_raw_fd())
}

fn detect_fusermount_bin() -> String {
    for name in [
        FUSERMOUNT3_BIN.to_string(),
        FUSERMOUNT_BIN.to_string(),
        format!("/bin/{FUSERMOUNT3_BIN}"),
        format!("/bin/{FUSERMOUNT_BIN}"),
    ]
    .iter()
    {
        if Command::new(name).arg("-h").output().is_ok() {
            return name.to_string();
        }
    }
    // Default to fusermount3
    FUSERMOUNT3_BIN.to_string()
}

pub fn fuse_umount_pure(mnt: &Path) -> IOResult<()> {
    let c_mountpoint = path_to_cstring(mnt)?;
    let result = unsafe { libc::umount2(c_mountpoint.as_ptr(), 0) };

    if result == 0 {
        return Ok(());
    }

    // Capture the direct unmount errno immediately, before any other syscall
    // (e.g. spawning fusermount) can clobber `errno`.
    let direct_err = std::io::Error::last_os_error();
    info!(
        "direct umount2 failed for {:?}: {}; falling back to fusermount",
        mnt, direct_err
    );

    let mut builder = Command::new(detect_fusermount_bin());
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    builder.arg("-u").arg("-q").arg("-z").arg("--").arg(mnt);

    let output = match builder.output() {
        Ok(output) => output,
        Err(spawn_err) => {
            // Both the direct unmount and spawning the fallback failed; surface
            // the original unmount error together with the spawn failure.
            return Err(IOError::with_ctx(
                std::io::Error::new(
                    spawn_err.kind(),
                    format!(
                        "umount {:?} failed (direct: {}; fusermount spawn: {})",
                        mnt, direct_err, spawn_err
                    ),
                ),
                format!("({}:{})", file!(), line!()),
            ));
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        error!(
            "fusermount unmount failed for {:?}: status={}, stderr={}, stdout={}",
            mnt, output.status, stderr, stdout
        );
        return Err(IOError::with_ctx(
            std::io::Error::other(format!(
                "umount {:?} failed (direct: {}; fusermount exit {}: {})",
                mnt,
                direct_err,
                output.status,
                stderr.trim()
            )),
            format!("({}:{})", file!(), line!()),
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        info!("fusermount: {}", stdout);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{complete_mount, path_to_cstring};
    use std::ffi::OsStr;
    use std::fs::File;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::{AsRawFd, OwnedFd};
    use std::path::{Path, PathBuf};

    #[test]
    fn failed_mount_closes_fuse_fd() {
        let fd: OwnedFd = File::open("/dev/null").expect("open test fd").into();
        let raw_fd = fd.as_raw_fd();

        assert!(complete_mount(fd, -1, Path::new("/invalid-mount")).is_err());

        let result = unsafe { libc::fcntl(raw_fd, libc::F_GETFD) };
        assert_eq!(result, -1, "failed mount must close the owned FUSE fd");
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EBADF)
        );
    }

    #[test]
    fn successful_mount_transfers_fuse_fd() {
        let fd: OwnedFd = File::open("/dev/null").expect("open test fd").into();
        let raw_fd = fd.as_raw_fd();

        let returned_fd =
            complete_mount(fd, 0, Path::new("/test-mount")).expect("successful mount transfers fd");

        assert_eq!(returned_fd, raw_fd);
        assert_ne!(unsafe { libc::fcntl(returned_fd, libc::F_GETFD) }, -1);
        assert_eq!(unsafe { libc::close(returned_fd) }, 0);
    }

    #[test]
    fn path_to_cstring_accepts_valid_utf8() {
        let c = path_to_cstring(Path::new("/tmp/curvine")).expect("valid path");
        assert_eq!(c.as_bytes(), b"/tmp/curvine");
    }

    // A valid Unix mount path may contain non-UTF-8 bytes. This must NOT panic;
    // the raw bytes should be preserved via OsStrExt::as_bytes.
    #[test]
    fn path_to_cstring_accepts_non_utf8() {
        // 0xFF is not valid UTF-8, but is a legal byte in a Unix path.
        let raw = b"/tmp/\xff\xfemount";
        let os = OsStr::from_bytes(raw);
        let path = PathBuf::from(os);
        // Precondition: to_str() would return None (i.e. the old unwrap() would panic).
        assert!(path.to_str().is_none());

        let c = path_to_cstring(&path).expect("non-utf8 path must not panic and must convert");
        assert_eq!(c.as_bytes(), raw);
    }

    // An embedded NUL byte must be reported as a normal error, not a panic.
    #[test]
    fn path_to_cstring_rejects_embedded_nul() {
        let raw = b"/tmp/bad\0path";
        let os = OsStr::from_bytes(raw);
        let path = PathBuf::from(os);

        let err = path_to_cstring(&path).expect_err("embedded NUL must be an error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
