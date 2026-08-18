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

use crate::*;
use fs2::FileExt;
use std::ffi::{CStr, CString};
use std::fs;
use std::fs::Metadata;
use std::io::{ErrorKind, IoSlice};
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

// It defines the underlying functions of the operating system, which is an encapsulation of the libc library functions.
// The main supported systems are linux.

// Get the file descriptor of an io object (file, network), and for non-linux systems, an error is returned.
#[cfg(target_os = "linux")]
pub fn get_raw_io<T>(io: &T) -> SysResult<CInt>
where
    T: AsRawFd,
{
    Ok(io.as_raw_fd())
}

#[cfg(not(target_os = "linux"))]
pub fn get_raw_io<T>(_: &T) -> SysResult<CInt> {
    sys_error!(ErrorKind::Unsupported, "Unsupported os")
}

pub fn close(raw_io: RawIO) -> SysResult<()> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(
            ErrorKind::Unsupported,
            "Unsupported close raw id {}",
            raw_io
        )
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            sys_call!(libc::close(raw_io))?;
        }
        Ok(())
    }
}

pub fn close_raw_io(raw_io: RawIO) -> SysResult<()> {
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
pub fn send_file(
    fd_in: RawIO,
    fd_out: RawIO,
    off: Option<&mut i64>,
    len: usize,
) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let off = match off {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };
        let res = unsafe { libc::sendfile(fd_out, fd_in, off, len as libc::size_t) };
        sys_call!(res)
    }
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
) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
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
        sys_call!(res)
    }
}

pub fn splice_out_full(
    fd_in: RawIO,
    mut off_in: Option<i64>,
    fd_out: RawIO,
    mut off_out: Option<i64>,
    len: usize,
) -> SysResult<()> {
    let mut remaining = len;
    while remaining > 0 {
        let transferred = splice(fd_in, off_in.as_mut(), fd_out, off_out.as_mut(), remaining)?;
        if transferred == 0 {
            return sys_error!(ErrorKind::UnexpectedEof, "splice returned 0");
        }
        remaining -= transferred as usize;
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
pub fn set_pipe_blocking(fd: RawIO, blocking: bool) -> SysResult<()> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let status_flags = sys_call!(libc::fcntl(fd, libc::F_GETFL))?;
            let res = if blocking {
                libc::fcntl(fd, libc::F_SETFL, status_flags & !libc::O_NONBLOCK)
            } else {
                libc::fcntl(fd, libc::F_SETFL, status_flags | libc::O_NONBLOCK)
            };

            sys_call!(res)?;
            Ok(())
        }
    }
}

pub fn pipe2(size: usize) -> SysResult<[RawIO; 2]> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut fds: [RawIO; 2] = [-1, -1];
            let res = libc::pipe2(
                fds.as_mut_ptr() as *mut libc::c_int,
                libc::O_CLOEXEC | libc::O_NONBLOCK,
            );
            sys_call!(res)?;

            let set_buf_res = libc::fcntl(fds[1], libc::F_SETPIPE_SZ, size);
            sys_call!(set_buf_res)?;

            let set_buf_res = libc::fcntl(fds[0], libc::F_SETPIPE_SZ, size);
            sys_call!(set_buf_res)?;

            if (set_buf_res as usize) < size {
                return sys_error!(
                    ErrorKind::InvalidInput,
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
pub fn read_ahead(file: &std::fs::File, off: i64, len: i64) -> SysResult<CInt> {
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
            // posix_fadvise reports failure by returning a positive errno directly instead
            // of returning -1 and setting errno, so `sys_call!` must not be used here.
            if res == 0 {
                Ok(0)
            } else {
                Err(std::io::Error::from_raw_os_error(res))
            }
        }
    }
}

pub fn is_tmpfs(file_path: &str) -> SysResult<bool> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(false)
    }

    #[cfg(target_os = "linux")]
    {
        let path = match std::ffi::CString::new(file_path) {
            Err(e) => return sys_error!(ErrorKind::InvalidInput, "CString::new {}", e),
            Ok(v) => v,
        };

        unsafe {
            let mut stat: libc::statfs = std::mem::zeroed();
            sys_call!(libc::statfs(path.as_ptr(), &mut stat))?;
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

pub fn read(fd: RawIO, buf: &mut [u8]) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe {
            use libc::{self, c_void, size_t};
            libc::read(fd, buf.as_ptr() as *mut c_void, buf.len() as size_t)
        };

        sys_call!(res)
    }
}

pub fn read_full(fd: RawIO, buf: &mut [u8]) -> SysResult<()> {
    let mut remaining = buf.len();
    let mut off = 0;

    while remaining > 0 {
        let read_len = read(fd, &mut buf[off..])? as usize;
        remaining -= read_len;
        off += read_len;
    }

    Ok(())
}

/// pub unsafe extern "C" fn pread64(
///     fd: c_int,
///     buf: *mut c_void,
///     count: size_t,
///     offset: off64_t,
/// ) -> ssize_t
///
/// May return a short read. Use [`pread_full`] when `buf.len()` bytes are required.
pub fn pread(fd: RawIO, buf: &mut [u8], offset: u64) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe {
            libc::pread64(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len() as libc::size_t,
                offset as libc::off64_t,
            )
        };
        sys_call!(res)
    }
}

/// Loop [`pread`] until `buf` is filled or EOF/error. Does not move the fd offset.
pub fn pread_full(fd: RawIO, buf: &mut [u8], offset: u64) -> SysResult<()> {
    let mut remaining = buf.len();
    let mut buf_off = 0;
    let mut file_off = offset;

    while remaining > 0 {
        let read_len = pread(fd, &mut buf[buf_off..], file_off)? as usize;
        if read_len == 0 {
            return sys_error!(ErrorKind::UnexpectedEof, "pread returned 0");
        }
        remaining -= read_len;
        buf_off += read_len;
        file_off += read_len as u64;
    }

    Ok(())
}

/// pub unsafe extern "C" fn pwrite64(
///     fd: c_int,
///     buf: *const c_void,
///     count: size_t,
///     offset: off64_t,
/// ) -> ssize_t
///
/// May return a short write. Use [`pwrite_full`] when all bytes must be written.
pub fn pwrite(fd: RawIO, buf: &[u8], offset: u64) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe {
            libc::pwrite64(
                fd,
                buf.as_ptr() as *const libc::c_void,
                buf.len() as libc::size_t,
                offset as libc::off64_t,
            )
        };
        sys_call!(res)
    }
}

/// Loop [`pwrite`] until `buf` is fully written. Does not move the fd offset.
pub fn pwrite_full(fd: RawIO, buf: &[u8], offset: u64) -> SysResult<()> {
    let mut remaining = buf.len();
    let mut buf_off = 0;
    let mut file_off = offset;

    while remaining > 0 {
        let write_len = pwrite(fd, &buf[buf_off..], file_off)? as usize;
        if write_len == 0 {
            return sys_error!(ErrorKind::WriteZero, "pwrite returned 0");
        }
        remaining -= write_len;
        buf_off += write_len;
        file_off += write_len as u64;
    }

    Ok(())
}

pub fn writev(fd: RawIO, bufs: &[IoSlice<'_>]) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res =
            unsafe { libc::writev(fd, bufs.as_ptr() as *const libc::iovec, bufs.len() as CInt) };

        sys_call!(res)
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
pub fn vm_splice(fd: RawIO, iov: &[IoSlice<'_>]) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
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

        sys_call!(res)
    }
}

// pub unsafe extern "C" fn open(
//     path: *const c_char,
//     oflag: c_int,
//     ...
// ) -> c_int
pub fn open(path: &CString, flag: i32) -> SysResult<RawIO> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::open(path.as_ptr(), flag | libc::O_CLOEXEC) };
        sys_call!(res)
    }
}

// pub unsafe extern "C" fn ioctl(
//     fd: c_int,
//     request: c_ulong,
//     ...
// ) -> c_int
pub fn ioctl(fd: RawIO, request: u64, arg: *mut libc::c_void) -> SysResult<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::ioctl(fd, request, arg) };
        sys_call!(res)
    }
}

pub fn dup(fd: RawIO) -> SysResult<RawIO> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::dup(fd) };
        sys_call!(res)
    }
}

// Get the page size.
// pub unsafe extern "C" fn sysconf(name: c_int) -> c_long
pub fn get_pagesize() -> SysResult<usize> {
    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }

    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        sys_call!(res)?;
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

/// pub unsafe extern "C" fn ftruncate64(
//     fd: c_int,
//     length: off64_t,
// ) -> c_int
pub fn ftruncate(file: &fs::File, len: i64) -> SysResult<()> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let fd = get_raw_io(file)?;
            let result = libc::ftruncate64(fd, len as libc::off64_t);

            sys_call!(result)?;
            Ok(())
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        file.set_len(len as u64)?;
        Ok(())
    }
}

/// pub unsafe extern "C" fn fallocate64(
//     fd: c_int,
//     mode: c_int,
//     offset: off64_t,
//     len: off64_t,
// ) -> c_int
pub fn fallocate(file: &fs::File, off: i64, len: i64, mode: i32) -> SysResult<()> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let fd = get_raw_io(file)?;
            let result = libc::fallocate64(fd, mode, off as libc::off64_t, len as libc::off64_t);

            sys_call!(result)?;
            Ok(())
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        file.allocate(len as u64)?;
        Ok(())
    }
}

/// Get the actual size of the file
/// - If there are no holes in the file, returns the logical file size
/// - If there are holes, returns the actual disk space occupied
pub fn file_actual_size(metadata: Metadata) -> SysResult<u64> {
    // 4k
    // 1G
    let logical_size = metadata.len();

    #[cfg(target_os = "linux")]
    {
        use std::os::linux::fs::MetadataExt;
        let actual_size = metadata.st_blocks() * ST_BLOCK_SIZE;

        // If actual size is less than logical size, the file has holes
        // 1g > 4k
        if actual_size < logical_size {
            Ok(actual_size)
        } else {
            Ok(logical_size)
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        Ok(logical_size)
    }
}

pub fn fcntl_get(fd: RawIO) -> SysResult<CInt> {
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        sys_call!(res)
    }

    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }
}

pub fn fcntl_set(fd: RawIO, flags: CInt) -> SysResult<CInt> {
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::fcntl(fd, libc::F_SETFD, flags) };
        sys_call!(res)
    }

    #[cfg(not(target_os = "linux"))]
    {
        sys_error!(ErrorKind::Unsupported, "unsupported operation")
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use curvine_runtime::common::Utils;
    use std::fs::{remove_file, OpenOptions};
    use std::io::Seek;
    use std::os::unix::io::AsRawFd;

    fn open_temp() -> (String, std::fs::File) {
        let path = Utils::test_file();
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        (path, file)
    }

    #[test]
    fn pread_pwrite_roundtrip_does_not_move_offset() {
        let (path, mut file) = open_temp();
        let fd = file.as_raw_fd();

        pwrite_full(fd, b"hello world", 0).unwrap();
        assert_eq!(file.stream_position().unwrap(), 0);

        let mut buf = [0u8; 5];
        assert_eq!(pread(fd, &mut buf, 6).unwrap(), 5);
        assert_eq!(&buf, b"world");
        assert_eq!(file.stream_position().unwrap(), 0);

        pwrite_full(fd, b"RUST", 6).unwrap();
        let mut got = [0u8; 11];
        pread_full(fd, &mut got, 0).unwrap();
        assert_eq!(&got, b"hello RUSTd");
        assert_eq!(file.stream_position().unwrap(), 0);

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_may_return_short_at_eof() {
        let (path, file) = open_temp();
        let fd = file.as_raw_fd();
        pwrite_full(fd, b"abc", 0).unwrap();

        let mut buf = [0u8; 8];
        assert_eq!(pread(fd, &mut buf, 0).unwrap(), 3);
        assert_eq!(&buf[..3], b"abc");

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_full_reads_requested_length() {
        let (path, file) = open_temp();
        let fd = file.as_raw_fd();
        pwrite_full(fd, &[1u8; 4096], 0).unwrap();
        pwrite_full(fd, &[2u8; 100], 4096).unwrap();

        let mut a = [0u8; 4096];
        pread_full(fd, &mut a, 0).unwrap();
        assert!(a.iter().all(|&x| x == 1));

        let mut b = [0u8; 100];
        pread_full(fd, &mut b, 4096).unwrap();
        assert!(b.iter().all(|&x| x == 2));

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_full_eof_is_unexpected_eof() {
        let (path, file) = open_temp();
        let fd = file.as_raw_fd();
        pwrite_full(fd, b"abc", 0).unwrap();

        let mut buf = [0u8; 4];
        let err = pread_full(fd, &mut buf, 0).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);

        remove_file(&path).unwrap();
    }

    #[test]
    fn pread_full_empty_buf_succeeds() {
        let (path, file) = open_temp();
        let fd = file.as_raw_fd();
        pread_full(fd, &mut [], 0).unwrap();
        pwrite_full(fd, &[], 0).unwrap();
        remove_file(&path).unwrap();
    }
}
