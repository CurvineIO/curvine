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

fn build_mount_options(
    fd: RawIO,
    mountpoint_mode: u32,
    conf: &FuseConf,
) -> IOResult<(String, libc::c_ulong)> {
    let mut mount_options = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        fd,
        mountpoint_mode,
        getuid(),
        getgid()
    );
    conf.set_fuse_opts(&mut mount_options)?;

    // Set the FUSE subtype so /proc/mounts reports `type fuse.curvinefs` instead of
    // the generic `type fuse` (same mechanism sshfs uses for `fuse.sshfs`). `c_type`
    // stays "fuse" (the only registered kernel FUSE fs type); the Linux kernel FUSE
    // module parses `subtype=` from this mount data and sets sb->s_subtype.
    mount_options.push_str(",subtype=curvinefs");

    // VFS options belong in the mount(2) flag argument, not in FUSE mount data.
    let mut vfs_options = conf.fuse_opts.join(",");
    if conf.readonly {
        if !vfs_options.is_empty() {
            vfs_options.push(',');
        }
        vfs_options.push_str("ro");
    }
    let flags = options_to_flag(&vfs_options);

    Ok((mount_options, flags))
}

pub fn fuse_mount_pure(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    let res = fuse_mount_sys(mnt, conf);
    match res {
        Ok(fd) => Ok(fd),
        Err(e) => {
            error!("fuse mount sys failed; path {:?}, err {}", mnt, e);
            Err(e)
        }
    }
}

fn fuse_mount_sys(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    let fuse_device_name = "/dev/fuse";
    let mountpoint_mode = mountpoint_mode(mnt)?;

    // Auto unmount requests must be sent to fusermount binary
    let path = CString::new(fuse_device_name).unwrap();
    let res = open(&path, libc::O_RDWR | libc::O_CLOEXEC);
    let fd = match res {
        Ok(fd) => fd,
        Err(e) => {
            let error = describe_fuse_device_error(e.into());
            error!("Open fuse device failed, {}: {}", fuse_device_name, error);
            return Err(error);
        }
    };
    // SAFETY: open returned a fresh descriptor whose ownership is transferred here.
    // Keep ownership until mount succeeds so every failure path closes /dev/fuse.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let (mount_options, flags) = build_mount_options(fd.as_raw_fd(), mountpoint_mode, conf)?;
    info!("sys-mount options: {}; flags: 0x{:x}", mount_options, flags);

    // Default name is "/dev/fuse", then use the subtype, and lastly prefer the name
    let c_source = CString::new("curvinefs").unwrap();
    let c_mountpoint = path_to_cstring(mnt)?;

    let result = unsafe {
        let c_options = CString::new(mount_options.as_str()).unwrap();
        let c_type = CString::new("fuse").unwrap();
        libc::mount(
            c_source.as_ptr(),
            c_mountpoint.as_ptr(),
            c_type.as_ptr(),
            flags,
            c_options.as_ptr() as *const libc::c_void,
        )
    };

    complete_mount(fd, result, mnt, &mount_options)
}

fn describe_mount_error(mnt: &Path, mount_options: &str, error: IOError) -> IOError {
    let raw_error = error.into_raw();
    let context = match raw_error.raw_os_error() {
        Some(libc::EINVAL) => format!(
            "FUSE mount request was rejected by the kernel (EINVAL). EINVAL alone does not identify the rejected argument. \
             Mount path: {}. Options: {}. Inspect `dmesg -T | tail -n 80`, then retry with a minimal `--options` list to isolate legacy-kernel incompatibilities.",
            mnt.display(),
            mount_options
        ),
        Some(libc::EACCES) | Some(libc::EPERM) => format!(
            "FUSE mount permission was denied by the kernel ({}). Mount path: {}. Options: {}. \
             Ensure /dev/fuse is available and the runtime is allowed to mount FUSE (for containers: CAP_SYS_ADMIN plus the platform's FUSE device and security-policy allowance).",
            raw_error,
            mnt.display(),
            mount_options
        ),
        Some(libc::EBUSY) => format!(
            "FUSE mount point {} is busy or already mounted (EBUSY). Stop or unmount the existing mount before retrying; use mount-table tooling rather than accessing an unhealthy mount path. Options: {}.",
            mnt.display(),
            mount_options
        ),
        Some(libc::ENOENT) => format!(
            "FUSE mount point {} disappeared before the kernel mounted it (ENOENT). Recreate the directory and verify the path remains available. Options: {}.",
            mnt.display(),
            mount_options
        ),
        Some(libc::ENOTDIR) => format!(
            "FUSE mount point {} is no longer a directory (ENOTDIR). Select an empty directory as --mnt-path. Options: {}.",
            mnt.display(),
            mount_options
        ),
        Some(libc::ENODEV) => format!(
            "FUSE mount failed because the kernel FUSE device/driver is unavailable (ENODEV). \
             Confirm /dev/fuse exists and the host has the fuse kernel module loaded. Mount path: {}. Options: {}.",
            mnt.display(),
            mount_options
        ),
        _ => format!(
            "FUSE mount syscall failed. Mount path: {}. Options: {}. \
             Inspect `dmesg -T | tail -n 80` for the kernel-side reason.",
            mnt.display(),
            mount_options
        ),
    };
    IOError::with_ctx(raw_error, context)
}

fn mountpoint_mode(mnt: &Path) -> IOResult<u32> {
    // Preserve the prior File::open permission check; this only adds diagnostics.
    let metadata =
        File::open(mnt).map_err(|error| describe_mountpoint_error("inspect", mnt, error.into()))?;
    let metadata = metadata
        .metadata()
        .map_err(|error| describe_mountpoint_error("inspect", mnt, error.into()))?;
    if !metadata.is_dir() {
        let error = IOError::from(std::io::Error::from_raw_os_error(libc::ENOTDIR));
        return Err(describe_mountpoint_error("inspect", mnt, error));
    }
    Ok(metadata.permissions().mode())
}

fn describe_mountpoint_error(stage: &str, mnt: &Path, error: IOError) -> IOError {
    let raw_error = error.into_raw();
    let context = match raw_error.raw_os_error() {
        Some(libc::ENOENT) => format!(
            "FUSE mount point {} does not exist. Create an empty directory and ensure the mounting user can access it.",
            mnt.display()
        ),
        Some(libc::ENOTDIR) => format!(
            "FUSE mount point {} is not a directory. Select an empty directory as --mnt-path.",
            mnt.display()
        ),
        Some(libc::EACCES) | Some(libc::EPERM) => format!(
            "Cannot access FUSE mount point {}. Check directory ownership, execute permission, and container volume policy.",
            mnt.display()
        ),
        _ => format!(
            "Cannot {stage} FUSE mount point {}. Check the path and its filesystem health.",
            mnt.display()
        ),
    };
    IOError::with_ctx(raw_error, context)
}

fn describe_fuse_device_error(error: IOError) -> IOError {
    let raw_error = error.into_raw();
    let context = match raw_error.raw_os_error() {
        Some(libc::ENOENT) | Some(libc::ENODEV) => {
            "FUSE device /dev/fuse is unavailable. Confirm the host has the fuse kernel module loaded and that the runtime receives the device assignment.".to_string()
        }
        Some(libc::EACCES) | Some(libc::EPERM) => {
            "Access to FUSE device /dev/fuse was denied. Check device permissions and, in containers, the FUSE device assignment and security policy.".to_string()
        }
        _ => "Unable to open FUSE device /dev/fuse. Check device availability and runtime permissions."
            .to_string(),
    };
    IOError::with_ctx(raw_error, context)
}

fn describe_unmount_error(mnt: &Path, error: IOError) -> IOError {
    let raw_error = error.into_raw();
    let context = match raw_error.raw_os_error() {
        Some(libc::EBUSY) => format!(
            "FUSE unmount of {} is busy (EBUSY). Stop processes using the mount, then retry the unmount; avoid probing an unhealthy mount path.",
            mnt.display()
        ),
        Some(libc::EINVAL) => format!(
            "FUSE unmount of {} was rejected because it is not a mounted filesystem (EINVAL). Inspect the mount table before retrying.",
            mnt.display()
        ),
        Some(libc::ENOENT) => format!(
            "FUSE unmount path {} no longer exists (ENOENT). Inspect the mount table for stale mount state.",
            mnt.display()
        ),
        Some(libc::EACCES) | Some(libc::EPERM) => format!(
            "FUSE unmount of {} was denied ({}). Check runtime mount permissions and container security policy.",
            mnt.display(),
            raw_error
        ),
        _ => format!(
            "FUSE unmount syscall failed for {}. Inspect `dmesg -T | tail -n 80` and the mount table for the kernel-side reason.",
            mnt.display()
        ),
    };
    IOError::with_ctx(raw_error, context)
}

fn complete_mount(
    fd: OwnedFd,
    result: libc::c_int,
    mnt: &Path,
    mount_options: &str,
) -> IOResult<RawIO> {
    if result != 0 {
        let error = IOError::from(std::io::Error::last_os_error());
        let error = describe_mount_error(mnt, mount_options, error);
        error!(
            "Mount fuse failed, {} with result {}: {}",
            mnt.display(),
            result,
            error
        );
        return Err(error);
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
    // Capture and contextualize the direct errno before probing the fallback.
    let direct_error = describe_unmount_error(mnt, IOError::from(std::io::Error::last_os_error()));
    let direct_message = direct_error.to_string();
    let direct_raw = direct_error.into_raw();
    let fusermount_bin = detect_fusermount_bin();
    info!(
        "direct umount2 failed for {:?}: {}; falling back to fusermount",
        mnt, direct_message
    );

    let mut builder = Command::new(&fusermount_bin);
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    builder.arg("-u").arg("-q").arg("-z").arg("--").arg(mnt);

    let output = match builder.output() {
        Ok(output) => output,
        Err(spawn_err) => {
            let message = format!(
                "{direct_message} FUSE unmount fallback {fusermount_bin} could not start: {spawn_err}"
            );
            error!("{}", message);
            return Err(IOError::with_ctx(direct_raw, message));
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let message = format!(
            "{direct_message} FUSE unmount fallback {fusermount_bin} failed with status {}. stdout: {} stderr: {}",
            output.status,
            stdout.trim(),
            stderr.trim()
        );
        error!("{}", message);
        return Err(IOError::with_ctx(direct_raw, message));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        info!("fusermount: {}", stdout);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_mount_options, complete_mount, describe_fuse_device_error, describe_mount_error,
        describe_mountpoint_error, describe_unmount_error, mountpoint_mode, options_to_flag,
        path_to_cstring,
    };
    use curvine_config::FuseConf;
    use curvine_io::IOError;
    use std::ffi::OsStr;
    use std::fs::{self, File};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::{AsRawFd, OwnedFd};
    use std::path::{Path, PathBuf};

    #[test]
    fn mount_builder_maps_all_supported_vfs_options_to_flags() {
        let mut conf = FuseConf {
            fuse_opts: vec!["ro,nodev,nosuid,noexec,noatime,dirsync,sync".to_string()],
            check_permission: false,
            ..Default::default()
        };
        conf.init().expect("supported options must initialize");

        let (mount_data, flags) =
            build_mount_options(10, 0o40755, &conf).expect("build mount arguments");

        assert_eq!(flags & libc::MS_RDONLY, libc::MS_RDONLY);
        assert_eq!(flags & libc::MS_NODEV, libc::MS_NODEV);
        assert_eq!(flags & libc::MS_NOSUID, libc::MS_NOSUID);
        assert_eq!(flags & libc::MS_NOEXEC, libc::MS_NOEXEC);
        assert_eq!(flags & libc::MS_NOATIME, libc::MS_NOATIME);
        assert_eq!(flags & libc::MS_DIRSYNC, libc::MS_DIRSYNC);
        assert_eq!(flags & libc::MS_SYNCHRONOUS, libc::MS_SYNCHRONOUS);
        assert!(!mount_data.split(',').any(|option| {
            matches!(
                option,
                "ro" | "nodev" | "nosuid" | "noexec" | "noatime" | "dirsync" | "sync"
            )
        }));
    }

    #[test]
    fn mount_builder_treats_positive_vfs_options_as_noops() {
        let conf = FuseConf {
            fuse_opts: vec!["rw,dev,suid,exec,atime,async".to_string()],
            check_permission: false,
            ..Default::default()
        };

        let (mount_data, flags) =
            build_mount_options(10, 0o40755, &conf).expect("build mount arguments");

        assert_eq!(flags, 0);
        assert!(!mount_data.split(',').any(|option| {
            matches!(option, "rw" | "dev" | "suid" | "exec" | "atime" | "async")
        }));
    }

    #[test]
    fn mount_builder_keeps_fuse_parameters_in_mount_data() {
        let mut conf = FuseConf {
            fuse_opts: vec!["allow_other,async,big_write".to_string()],
            check_permission: false,
            ..Default::default()
        };
        conf.init().expect("supported options must initialize");

        let (mount_data, flags) =
            build_mount_options(10, 0o40755, &conf).expect("build mount arguments");

        assert_eq!(flags, 0);
        assert!(mount_data.split(',').any(|item| item == "allow_other"));
        assert!(!mount_data
            .split(',')
            .any(|item| matches!(item, "async" | "big_write")));
        assert!(mount_data
            .split(',')
            .any(|item| item == "subtype=curvinefs"));
    }

    #[test]
    fn readonly_sets_vfs_flag_without_polluting_mount_data() {
        let conf = FuseConf {
            readonly: true,
            check_permission: false,
            ..Default::default()
        };

        let (mount_data, flags) =
            build_mount_options(10, 0o40755, &conf).expect("build mount arguments");

        assert_eq!(flags & libc::MS_RDONLY, libc::MS_RDONLY);
        assert!(!mount_data.split(',').any(|item| item == "ro"));
    }

    #[test]
    fn ro_flag_does_not_match_rootmode_parameter() {
        assert_eq!(options_to_flag("fd=10,rootmode=40755"), 0);
        assert_eq!(
            options_to_flag("fd=10,rootmode=40755,ro") & libc::MS_RDONLY,
            libc::MS_RDONLY
        );
    }

    #[test]
    fn failed_mount_closes_fuse_fd() {
        let fd: OwnedFd = File::open("/dev/null").expect("open test fd").into();
        let raw_fd = fd.as_raw_fd();

        assert!(complete_mount(fd, -1, Path::new("/invalid-mount"), "").is_err());

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

        let returned_fd = complete_mount(fd, 0, Path::new("/test-mount"), "")
            .expect("successful mount transfers fd");

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

    #[test]
    fn invalid_mount_options_error_preserves_errno_and_explains_next_steps() {
        let error = IOError::from(std::io::Error::from_raw_os_error(libc::EINVAL));
        let error = describe_mount_error(
            Path::new("/curvine-fuse"),
            "fd=10,rootmode=40755,async",
            error,
        );

        assert_eq!(error.raw_error().raw_os_error(), Some(libc::EINVAL));
        let message = error.to_string();
        assert!(message.contains("rejected by the kernel (EINVAL)"));
        assert!(message.contains("fd=10,rootmode=40755,async"));
        assert!(message.contains("dmesg -T | tail -n 80"));
        assert!(message.contains("minimal `--options` list"));
    }

    #[test]
    fn unavailable_fuse_device_error_preserves_errno_and_explains_requirement() {
        let error = IOError::from(std::io::Error::from_raw_os_error(libc::ENOENT));
        let error = describe_fuse_device_error(error);

        assert_eq!(error.raw_error().raw_os_error(), Some(libc::ENOENT));
        let message = error.to_string();
        assert!(message.contains("/dev/fuse is unavailable"));
        assert!(message.contains("fuse kernel module"));
        assert!(message.contains("device assignment"));
    }

    #[test]
    fn missing_mountpoint_error_preserves_errno_and_explains_requirement() {
        let error = IOError::from(std::io::Error::from_raw_os_error(libc::ENOENT));
        let error = describe_mountpoint_error("inspect", Path::new("/curvine-fuse"), error);

        assert_eq!(error.raw_error().raw_os_error(), Some(libc::ENOENT));
        let message = error.to_string();
        assert!(message.contains("mount point /curvine-fuse"));
        assert!(message.contains("does not exist"));
        assert!(message.contains("empty directory"));
    }

    #[test]
    fn file_mountpoint_is_rejected_before_mount() {
        let path = std::env::temp_dir().join(format!(
            "curvine-fuse-mountpoint-file-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after the Unix epoch")
                .as_nanos()
        ));
        File::create(&path).expect("create file mountpoint fixture");

        let error = mountpoint_mode(&path).expect_err("file must not be accepted as a mountpoint");
        let _ = fs::remove_file(&path);

        assert_eq!(error.raw_error().raw_os_error(), Some(libc::ENOTDIR));
        assert!(error.to_string().contains("is not a directory"));
    }

    #[test]
    fn busy_mountpoint_error_preserves_errno_and_explains_next_steps() {
        let error = IOError::from(std::io::Error::from_raw_os_error(libc::EBUSY));
        let error = describe_mount_error(Path::new("/curvine-fuse"), "fd=10", error);

        assert_eq!(error.raw_error().raw_os_error(), Some(libc::EBUSY));
        let message = error.to_string();
        assert!(message.contains("busy or already mounted"));
        assert!(message.contains("Stop or unmount"));
    }

    #[test]
    fn busy_unmount_error_preserves_errno_and_explains_next_steps() {
        let error = IOError::from(std::io::Error::from_raw_os_error(libc::EBUSY));
        let error = describe_unmount_error(Path::new("/curvine-fuse"), error);

        assert_eq!(error.raw_error().raw_os_error(), Some(libc::EBUSY));
        let message = error.to_string();
        assert!(message.contains("unmount of /curvine-fuse is busy"));
        assert!(message.contains("Stop processes using the mount"));
    }
}
