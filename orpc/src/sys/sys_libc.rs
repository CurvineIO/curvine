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

#![allow(unused, clippy::not_unsafe_ptr_arg_deref)]

use crate::err_box;
use crate::handler::RpcFrame;
use crate::io::IOResult;
use crate::sys::*;
use crate::{err_io, err_msg, try_err, try_option};
use std::ffi::{CStr, CString};
use std::fs;
use std::io::IoSlice;
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

// It defines the underlying functions of the operating system, which is an encapsulation of the libc library functions.
// The main supported systems are linux.

// Get the file descriptor of an io object (file, network), and for non-linux systems, an error is returned.
#[cfg(target_os = "linux")]
pub fn get_raw_io<T>(io: &T) -> IOResult<CInt>
where
    T: AsRawFd,
{
    err_io!(io.as_raw_fd())
}

#[cfg(not(target_os = "linux"))]
pub fn get_raw_io<T>(_: &T) -> IOResult<CInt> {
    err_box!("Unsupported os")
}

pub fn close(raw_io: RawIO) -> IOResult<()> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("Unsupported close raw id {}", raw_io)
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            err_io!(libc::close(raw_io))?;
        }
        Ok(())
    }
}

pub fn close_raw_io(raw_io: RawIO) -> IOResult<()> {
    close(raw_io)
}

// Linux sendfile function to send the number of files to the network.
// C function prototype:
//pub unsafe extern "C" fn sendfile(
//     out_fd: c_int,
//     in_fd: c_int,
//     offset: *mut off_t,
//     count: size_t
// ) -> ssize_t
pub fn send_file(fd_in: RawIO, fd_out: RawIO, off: Option<&mut i64>, len: usize) -> IOResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let off = match off {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };
        let res = unsafe { libc::sendfile(fd_out, fd_in, off, len as libc::size_t) };
        err_io!(res)
    }
}

pub async fn send_file_full(
    io_out: &RpcFrame,
    fd_in: RawIO,
    mut off: Option<i64>,
    len: usize,
) -> IOResult<()> {
    let fd_out = get_raw_io(io_out)?;
    let mut reaming = len;
    while reaming > 0 {
        let res = io_out
            .async_write(|| send_file(fd_in, fd_out, off.as_mut(), reaming))
            .await;

        match res {
            Ok(transferred) => {
                if transferred == 0 {
                    return err_box!("send_file return 0");
                }
                reaming -= transferred as usize;
            }

            Err(e) if e.is_would_block() => {
                continue;
            }

            Err(e) => return Err(e),
        }
    }

    Ok(())
}

// Linux splice function to copy data between 2 FDs.
// C function prototype:
// pub unsafe extern "C" fn splice(
//     fd_in: c_int,
//     off_in: *mut loff_t,
//     fd_out: c_int,
//     off_out: *mut loff_t,
//     len: size_t,
//     flags: c_uint
// ) -> size_t
pub fn splice(
    fd_in: RawIO,
    off_in: Option<&mut i64>,
    fd_out: RawIO,
    off_out: Option<&mut i64>,
    len: usize,
) -> IOResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let off_in = match off_in {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };
        let off_out = match off_out {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };

        let res = unsafe {
            libc::splice(
                fd_in,
                off_in,
                fd_out,
                off_out,
                len as libc::size_t,
                libc::SPLICE_F_NONBLOCK | libc::SPLICE_F_MOVE,
            )
        };
        err_io!(res)
    }
}

pub fn splice_out_full(
    fd_in: RawIO,
    mut off_in: Option<i64>,
    fd_out: RawIO,
    mut off_out: Option<i64>,
    len: usize,
) -> IOResult<()> {
    let mut reaming = len;
    while reaming > 0 {
        let transferred = splice(fd_in, off_in.as_mut(), fd_out, off_out.as_mut(), reaming)?;
        if transferred == 0 {
            return err_box!("unsupported operation");
        }
        reaming -= transferred as usize;
    }

    Ok(())
}

pub async fn splice_in_full(
    io_in: &RpcFrame,
    mut off_in: Option<i64>,
    fd_out: RawIO,
    mut off_out: Option<i64>,
    len: usize,
) -> IOResult<()> {
    let fd_in = get_raw_io(io_in)?;

    let mut reaming = len;
    while reaming > 0 {
        let write_res = io_in
            .async_read(|| splice(fd_in, off_in.as_mut(), fd_out, off_out.as_mut(), reaming))
            .await;

        match write_res {
            Ok(transferred) => reaming -= transferred as usize,

            Err(e) if e.is_would_block() => continue,

            Err(e) => return Err(e),
        }
    }

    Ok(())
}

// Return to whether the current pipeline is blocking mode
pub fn pipe_is_blocking(fd: RawIO) -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        true
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let status_flags = libc::fcntl(fd, libc::F_GETFL);
            status_flags & libc::O_NONBLOCK == 0
        }
    }
}

// Modify the pipeline blocking mode.
pub fn set_pipe_blocking(fd: RawIO, blocking: bool) -> IOResult<()> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let status_flags = err_io!(libc::fcntl(fd, libc::F_GETFL))?;
            let res = if blocking {
                libc::fcntl(fd, libc::F_SETFL, status_flags & !libc::O_NONBLOCK)
            } else {
                libc::fcntl(fd, libc::F_SETFL, status_flags | libc::O_NONBLOCK)
            };

            err_io!(res)?;
            Ok(())
        }
    }
}

pub fn pipe2(size: usize) -> IOResult<[RawIO; 2]> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut fds: [RawIO; 2] = [-1, -1];
            let res = libc::pipe2(
                fds.as_mut_ptr() as *mut libc::c_int,
                libc::O_CLOEXEC | libc::O_NONBLOCK,
            );
            err_io!(res)?;

            let set_buf_res = libc::fcntl(fds[1], libc::F_SETPIPE_SZ, size);
            err_io!(set_buf_res)?;

            let set_buf_res = libc::fcntl(fds[0], libc::F_SETPIPE_SZ, size);
            err_io!(set_buf_res)?;

            if (set_buf_res as usize) < size {
                return err_box!(
                    "Failed to set pipe size, expected: {}, actual: {}",
                    size,
                    set_buf_res
                );
            }

            Ok(fds)
        }
    }
}

// Operating system pre-read api.
//pub unsafe extern "C" fn posix_fadvise(
//     fd: c_int,
//     offset: off_t,
//     len: off_t,
//     advise: c_int
// ) -> c_int
pub fn read_ahead(file: &std::fs::File, off: i64, len: i64) -> IOResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(0)
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let fd = get_raw_io(file)?;
            let res = libc::posix_fadvise(
                fd,
                off as libc::off_t,
                len as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            );
            err_io!(res)
        }
    }
}

pub fn is_tmpfs(file_path: &str) -> IOResult<bool> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(false)
    }

    #[cfg(target_os = "linux")]
    {
        let path = match std::ffi::CString::new(file_path) {
            Err(e) => return err_box!("CString::new {}", e),
            Ok(v) => v,
        };

        unsafe {
            let mut stat: libc::statfs = std::mem::zeroed();
            err_io!(libc::statfs(path.as_ptr(), &mut stat))?;
            Ok(stat.f_type == libc::TMPFS_MAGIC)
        }
    }
}

pub fn thread_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_string()
}

pub fn read(fd: RawIO, buf: &mut [u8]) -> IOResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe {
            use libc::{self, c_void, size_t};
            libc::read(fd, buf.as_ptr() as *mut c_void, buf.len() as size_t)
        };

        err_io!(res)
    }
}

pub fn read_full(fd: RawIO, buf: &mut [u8]) -> IOResult<()> {
    let mut reaming = buf.len();
    let mut off = 0;

    while reaming > 0 {
        let read_len = read(fd, &mut buf[off..])? as usize;
        reaming -= read_len;
        off += read_len;
    }

    Ok(())
}

pub fn writev(fd: RawIO, bufs: &[IoSlice<'_>]) -> IOResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res =
            unsafe { libc::writev(fd, bufs.as_ptr() as *const libc::iovec, bufs.len() as CInt) };

        err_io!(res)
    }
}

pub fn get_uid() -> u32 {
    #[cfg(target_os = "linux")]
    {
        unsafe { libc::geteuid() }
    }

    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

pub fn get_gid() -> u32 {
    #[cfg(target_os = "linux")]
    {
        unsafe { libc::getegid() }
    }

    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

//pub unsafe extern "C" fn vmsplice(
//     fd: c_int,
//     iov: *const iovec,
//     nr_segs: size_t,
//     flags: c_uint
// ) -> ssize_t
pub fn vm_splice(fd: RawIO, iov: &[IoSlice<'_>]) -> IOResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe {
            libc::vmsplice(
                fd,
                iov.as_ptr() as *const libc::iovec,
                iov.len(),
                libc::SPLICE_F_NONBLOCK,
            )
        };

        err_io!(res)
    }
}

// pub unsafe extern "C" fn open(
//     path: *const c_char,
//     oflag: c_int,
//     ...
// ) -> c_int
pub fn open(path: &CString, flag: i32) -> IOResult<RawIO> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::open(path.as_ptr(), flag | libc::O_CLOEXEC) };
        err_io!(res)
    }
}

// pub unsafe extern "C" fn ioctl(
//     fd: c_int,
//     request: c_ulong,
//     ...
// ) -> c_int
pub fn ioctl(fd: RawIO, request: u64, arg: *mut libc::c_void) -> IOResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::ioctl(fd, request, arg) };
        err_io!(res)
    }
}

pub fn dup(fd: RawIO) -> IOResult<RawIO> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::dup(fd) };
        err_io!(res)
    }
}

// Get the page size.
// pub unsafe extern "C" fn sysconf(name: c_int) -> c_long
pub fn get_pagesize() -> IOResult<usize> {
    #[cfg(not(target_os = "linux"))]
    {
        err_box!("unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        err_io!(res)?;
        Ok(res as usize)
    }
}

pub fn get_device_id(path: &Path) -> u64 {
    if let Ok(metadata) = fs::metadata(path) {
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::fs::MetadataExt;
            metadata.st_dev()
        }
        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    } else {
        0
    }
}

/// Get UID by username using getpwnam system call
pub fn get_uid_by_name(username: &str) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        let c_username = match CString::new(username) {
            Ok(s) => s,
            Err(_) => return None,
        };

        unsafe {
            let passwd = libc::getpwnam(c_username.as_ptr());
            if passwd.is_null() {
                None
            } else {
                Some((*passwd).pw_uid)
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Get username by UID using getpwuid system call  
pub fn get_username_by_uid(uid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let passwd = libc::getpwuid(uid);
            if passwd.is_null() {
                None
            } else {
                let c_str = CStr::from_ptr((*passwd).pw_name);
                c_str.to_string_lossy().into_owned().into()
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Get GID by group name using getgrnam system call
pub fn get_gid_by_name(groupname: &str) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        let c_groupname = match CString::new(groupname) {
            Ok(s) => s,
            Err(_) => return None,
        };

        unsafe {
            let group = libc::getgrnam(c_groupname.as_ptr());
            if group.is_null() {
                None
            } else {
                Some((*group).gr_gid)
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Get group name by GID using getgrgid system call
pub fn get_groupname_by_gid(gid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let group = libc::getgrgid(gid);
            if group.is_null() {
                None
            } else {
                let c_str = CStr::from_ptr((*group).gr_name);
                c_str.to_string_lossy().into_owned().into()
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
