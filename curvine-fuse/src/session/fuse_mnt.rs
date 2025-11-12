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
use crate::raw::fuse_mount_pure;
use crate::raw::fuse_umount_pure;
use crate::{FuseUtils, RawSession, FUSE_CLONE_FD_MIN_VERSION, UNIX_KERNEL_VERSION};
use curvine_common::conf::FuseConf;
use log::{debug, error, info};
use orpc::io::IOResult;
use orpc::sys;
use orpc::sys::pipe::{AsyncFd, BorrowedFd, OwnedFd};
use orpc::sys::{CString, RawIO};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct FuseMnt {
    pub(crate) path: PathBuf,
    // OwnedFd is not used here, and in some cases fd cannot be turned off by rust.
    pub(crate) fd: RawIO,
    pub(crate) clone_fds: Mutex<Vec<OwnedFd>>,
}

impl FuseMnt {
    pub fn new(path: PathBuf, conf: &FuseConf) -> Self {
        let res = fuse_mount_pure(path.as_path(), conf);
        match res {
            Ok(fd) => {
                sys::set_pipe_blocking(fd, false).unwrap();
                info!("fuse mount success, path {:?}, fd {}", path, fd);
                Self {
                    path,
                    fd,
                    clone_fds: Mutex::new(vec![]),
                }
            }
            Err(e) => {
                error!("fuse mount failed, path {:?}, err {:?}", path, e);
                panic!("fuse mount failed");
            }
        }
    }

    fn crate_task_fd(&self, clone: bool) -> IOResult<BorrowedFd> {
        let clone_fd = if clone && *UNIX_KERNEL_VERSION >= FUSE_CLONE_FD_MIN_VERSION {
            match FuseUtils::fuse_clone_fd(self.fd) {
                Ok(clone_fd) => {
                    debug!("Fuse clone fd, {} -> {}", self.fd, clone_fd);
                    clone_fd
                }

                Err(e) => {
                    error!(
                        "clone fd failed, will fall back to shared fd mode; kernel version: {},\
                     source fd {}, cause: {}",
                        *UNIX_KERNEL_VERSION, self.fd, e
                    );
                    sys::dup(self.fd)?
                }
            }
        } else {
            sys::dup(self.fd)?
        };

        let new_fd = OwnedFd::new(clone_fd);
        new_fd.set_blocking(false)?;

        let borrowed = new_fd.as_borrowed();
        // fd is recycled by FuseMnt and saved here.
        self.clone_fds.lock().unwrap().push(new_fd);

        Ok(borrowed)
    }

    // Get 2 fds for asynchronous reading and writing data.
    pub fn create_async_task_fd(&self, clone: bool) -> IOResult<Arc<AsyncFd>> {
        let fd = self.crate_task_fd(clone)?;
        let fd = Arc::new(AsyncFd::new(fd)?);
        Ok(fd)
    }
}

impl Drop for FuseMnt {
    fn drop(&mut self) {
        fuse_umount_pure(self.path.as_path());
        info!("unmount {:?}", self.path)
    }
}
