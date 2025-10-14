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

use curvine_common::conf::FuseConf;
use log::{error, info};
use nix::unistd::{getgid, getuid};
use orpc::err_io;
use orpc::io::IOResult;
use orpc::sys::RawIO;
use std::ffi::CString;
use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};

use orpc::sys::open;

const FUSERMOUNT_BIN: &str = "fusermount";
const FUSERMOUNT3_BIN: &str = "fusermount3";

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

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "macos")]
pub fn options_to_flag(mount_option: &String) -> libc::c_long {
    let mut flags = 0;
    if has_mount_opt(mount_option, "ro") {
        flags |= libc::MNT_RDONLY;
    }
    if has_mount_opt(mount_option, "nodev") {
        flags |= libc::MNT_NODEV;
    }
    if has_mount_opt(mount_option, "nosuid") {
        flags |= libc::MNT_NOSUID;
    }
    if has_mount_opt(mount_option, "noexec") {
        flags |= libc::MNT_NOEXEC;
    }
    if has_mount_opt(mount_option, "noatime") {
        flags |= libc::MNT_NOATIME;
    }
    if has_mount_opt(mount_option, "dirsync") {
        flags |= libc::MNT_DIRSYNC;
    }
    if has_mount_opt(mount_option, "sync") {
        flags |= libc::MNT_SYNCHRONOUS;
    }

    return flags;
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
            err_io!(-1)
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
    let mut flags = 0;
    let mut mount_options = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        fd,
        mountpoint_mode,
        getuid(),
        getgid()
    );
    conf.set_fuse_opts(&mut mount_options);
    flags |= options_to_flag(mount_options.as_str());
    // Default name is "/dev/fuse", then use the subtype, and lastly prefer the name
    let c_source = CString::new("curvinefs").unwrap();
    let c_mountpoint = CString::new(mnt.to_str().unwrap()).unwrap();

    let result = unsafe {
        #[cfg(target_os = "linux")]
        {
            let c_options = CString::new(mount_options.clone()).unwrap();
            let c_type = CString::new("fuse").unwrap();
            libc::mount(
                c_source.as_ptr(),
                c_mountpoint.as_ptr(),
                c_type.as_ptr(),
                flags,
                c_options.as_ptr() as *const libc::c_void,
            )
        }
        #[cfg(target_os = "macos")]
        {
            let mut c_options = CString::new(mount_options).unwrap();
            libc::mount(
                c_source.as_ptr(),
                c_mountpoint.as_ptr(),
                flags,
                c_options.as_ptr() as *const libc::c_void,
            )
        }
    };

    if result != 0 {
        error!(
            "Mount fuse failed, {} with result {}",
            mnt.display(),
            result
        );
        return err_io!(-1);
    }
    info!("Mounted at {}", mnt.display());
    Ok(fd)
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

pub fn fuse_umount_pure(mnt: &Path) {
    let c_mountpoint = CString::new(mnt.to_str().unwrap()).unwrap();
    let result = unsafe {
        #[cfg(target_os = "linux")]
        {
            libc::umount2(c_mountpoint.as_ptr(), 0)
        }
        #[cfg(target_os = "macos")]
        {
            libc::umount(c_mountpoint.as_ptr())
        }
    };

    if result == 0 {
        return;
    }
    let mut builder = Command::new(detect_fusermount_bin());
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    builder.arg("-u").arg("-q").arg("-z").arg("--").arg(mnt);

    if let Ok(output) = builder.output() {
        info!("fusermount: {}", String::from_utf8_lossy(&output.stdout));
    }
}
