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

#![allow(unused)]

use crate::raw::fuse_abi::fuse_args;
// `fuse_mount_pure` / `fuse_umount_pure` live in the Linux-only `raw::fuse_pure`
// module (gated in `raw/mod.rs`). Import and use them only on Linux so this
// caller shares the same platform contract instead of failing to resolve the
// symbols off Linux.
#[cfg(target_os = "linux")]
use crate::raw::fuse_mount_pure;
#[cfg(target_os = "linux")]
use crate::raw::fuse_umount_pure;
use crate::{FuseUtils, FUSE_CLONE_FD_MIN_VERSION, UNIX_KERNEL_VERSION};
use curvine_config::FuseConf;
use curvine_io::{IOError, IOResult};
use curvine_sys as sys;
use curvine_sys::pipe::{AsyncFd, BorrowedFd, OwnedFd};
use curvine_sys::{CString, RawIO};
use log::{debug, error, info};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct FuseMnt {
    pub(crate) path: PathBuf,
    // Not an OwnedFd: in some cases the fd cannot be closed by Rust.
    pub(crate) fd: RawIO,
    pub(crate) clone_fds: Mutex<Vec<OwnedFd>>,
    auto_unmount: bool,
}

impl FuseMnt {
    pub fn new(path: PathBuf, conf: &FuseConf) -> IOResult<Self> {
        let fd = fuse_mount_pure(path.as_path(), conf)?;
        Self::from_fd(path, conf, fd)
    }

    pub fn from_fd(path: PathBuf, _conf: &FuseConf, fd: RawIO) -> IOResult<Self> {
        // Construct the RAII owner before fallible fd setup. If setup fails,
        // `mnt` is dropped and the mount is cleaned up instead of becoming stale.
        let mnt = Self {
            path,
            fd,
            clone_fds: Mutex::new(vec![]),
            auto_unmount: true,
        };
        sys::set_pipe_blocking(mnt.fd, false).map_err(|error| {
            describe_fuse_fd_error("set mounted FUSE fd nonblocking", mnt.fd, error.into())
        })?;
        info!("fuse mount success, path {:?}, fd {}", mnt.path, mnt.fd);
        Ok(mnt)
    }

    fn create_task_fd(&self, clone: bool) -> IOResult<BorrowedFd> {
        let kernel_version = *UNIX_KERNEL_VERSION;
        let clone_fd = if clone && kernel_version >= FUSE_CLONE_FD_MIN_VERSION {
            match FuseUtils::fuse_clone_fd(self.fd) {
                Ok(clone_fd) => {
                    debug!("Fuse clone fd, {} -> {}", self.fd, clone_fd);
                    clone_fd
                }

                Err(e) => {
                    error!(
                        "clone fd failed, will fall back to shared fd mode; kernel version: {}.{},\
                     source fd {}, cause: {}",
                        kernel_version.0, kernel_version.1, self.fd, e
                    );
                    sys::dup(self.fd).map_err(|error| {
                        describe_fuse_fd_error(
                            "duplicate FUSE fd after clone fallback",
                            self.fd,
                            error.into(),
                        )
                    })?
                }
            }
        } else {
            sys::dup(self.fd).map_err(|error| {
                describe_fuse_fd_error("duplicate FUSE fd", self.fd, error.into())
            })?
        };

        let new_fd = OwnedFd::new(clone_fd);
        new_fd.set_blocking(false).map_err(|error| {
            describe_fuse_fd_error("set task FUSE fd nonblocking", clone_fd, error.into())
        })?;

        let borrowed = new_fd.as_borrowed();
        // fd is recycled by FuseMnt and saved here.
        self.clone_fds.lock().unwrap().push(new_fd);

        Ok(borrowed)
    }

    // Get an async fd for reading and writing data.
    pub fn create_async_task_fd(&self, clone: bool) -> IOResult<Arc<AsyncFd>> {
        let fd = self.create_task_fd(clone)?;
        let raw_fd = fd.fd();
        let fd = Arc::new(AsyncFd::new(fd).map_err(|error| {
            describe_fuse_fd_error(
                "register task FUSE fd for asynchronous I/O",
                raw_fd,
                error.into(),
            )
        })?);
        Ok(fd)
    }

    pub fn auto_unmount(&mut self, auto_unmount: bool) {
        self.auto_unmount = auto_unmount;
    }
}

pub(super) fn describe_fuse_fd_error(stage: &str, fd: RawIO, error: IOError) -> IOError {
    let raw_error = error.into_raw();
    let remediation = match raw_error.raw_os_error() {
        Some(libc::EBADF) => "The FUSE fd is invalid or closed; check for an interrupted mount or stale restore state.",
        Some(libc::EMFILE) | Some(libc::ENFILE) => {
            "The process or system file-descriptor limit is exhausted; raise the relevant nofile limit."
        }
        Some(libc::EPERM) | Some(libc::EACCES) => {
            "The runtime lacks permission for this FUSE fd operation; check container security policy and device access."
        }
        Some(libc::EINVAL) => {
            "The kernel rejected this FUSE fd operation; verify kernel FUSE support and the mounted connection state."
        }
        _ => "Inspect the FUSE fd state and the kernel log for the underlying failure.",
    };
    IOError::with_ctx(
        raw_error,
        format!("FUSE {stage} failed for fd={fd}. {remediation}"),
    )
}

impl Drop for FuseMnt {
    fn drop(&mut self) {
        if self.auto_unmount {
            #[cfg(target_os = "linux")]
            match fuse_umount_pure(self.path.as_path()) {
                Ok(()) => info!("unmount path={:?}, fd={}", self.path, self.fd),
                Err(e) => error!(
                    "unmount failed path={:?}, fd={}, err={:?}",
                    self.path, self.fd, e
                ),
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{describe_fuse_fd_error, FuseMnt};
    use curvine_config::FuseConf;
    use std::path::PathBuf;

    fn missing_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "curvine-missing-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after the Unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn mount_failure_returns_error() {
        let conf = FuseConf::default();

        assert!(
            FuseMnt::new(missing_path("mount"), &conf).is_err(),
            "an invalid mount point must return an error instead of panicking"
        );
    }

    #[test]
    fn fd_setup_failure_returns_error() {
        let conf = FuseConf::default();

        assert!(
            FuseMnt::from_fd(missing_path("fd"), &conf, -1).is_err(),
            "an invalid FUSE fd must return an error instead of panicking"
        );
    }

    #[test]
    fn fuse_fd_error_preserves_errno_and_identifies_stage() {
        let error = curvine_io::IOError::from(std::io::Error::from_raw_os_error(libc::EBADF));
        let error = describe_fuse_fd_error("create nonblocking task fd", 42, error);

        assert_eq!(error.raw_error().raw_os_error(), Some(libc::EBADF));
        let message = error.to_string();
        assert!(message.contains("create nonblocking task fd"));
        assert!(message.contains("fd=42"));
        assert!(message.contains("invalid or closed"));
    }
}
