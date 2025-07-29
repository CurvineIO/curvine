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

#![allow(unused_variables, unused, clippy::not_unsafe_ptr_arg_deref)]

use crate::raw::fuse_abi::fuse_args;
use crate::session::FuseMnt;
use crate::RawSession;
use libc::{c_char, c_int, c_void};
use orpc::io::IOResult;
use orpc::sys::RawIO;
use orpc::{err_box, err_io};
use std::ffi::CString;

// fuse 2.x version mount related functions.
#[cfg(all(target_os = "linux", feature = "fuse2"))]
extern "C" {
    pub fn fuse_mount_compat25(mountpoint: *const c_char, args: *const fuse_args) -> c_int;

    pub fn fuse_unmount_compat22(mountpoint: *const c_char);
}

// fuse 3.x version mount related functions.
#[cfg(all(target_os = "linux", feature = "fuse3"))]
extern "C" {
    pub fn fuse_session_new(
        args: *const fuse_args,
        op: *const c_void,
        op_size: libc::size_t,
        userdata: *mut c_void,
    ) -> *mut c_void;

    pub fn fuse_session_mount(se: *mut c_void, mountpoint: *const c_char) -> c_int;

    pub fn fuse_session_fd(se: *mut c_void) -> libc::c_int;

    pub fn fuse_session_unmount(se: *mut c_void);

    pub fn fuse_session_destroy(se: *mut c_void);
}

// fuse mount function, adapts to multiple versions through conditional compilation.
pub fn fuse_mount(
    mnt: *const c_char,
    args: *const fuse_args,
) -> IOResult<(RawIO, Option<RawSession>)> {
    #[cfg(all(target_os = "linux", feature = "fuse2"))]
    {
        let res = unsafe { fuse_mount_compat25(mnt, args) };
        let fd = err_io!(res)?;
        Ok((fd, None))
    }

    #[cfg(all(target_os = "linux", feature = "fuse3"))]
    {
        use std::ptr;
        let session = unsafe { fuse_session_new(args, ptr::null(), 0, ptr::null_mut()) };
        if session.is_null() {
            err_io!(-1)?;
        }

        let res = unsafe { fuse_session_mount(session, mnt) };
        err_io!(res)?;

        let res = unsafe { fuse_session_fd(session) };
        let fd = err_io!(res)?;
        Ok((fd, Some(session)))
    }

    #[cfg(not(target_os = "linux"))]
    {
        err_box!("Only supports linux system")
    }
}

pub fn fuse_unmount(mnt: &FuseMnt) {
    #[cfg(all(target_os = "linux", feature = "fuse2"))]
    {
        unsafe {
            fuse_unmount_compat22(mnt.path.as_ptr());
        }
    }

    #[cfg(all(target_os = "linux", feature = "fuse3"))]
    {
        if let Some(session) = &mnt.session {
            unsafe {
                fuse_session_unmount(*session);
                fuse_session_destroy(*session);
            }
        }
    }
}
