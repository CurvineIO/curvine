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

#![cfg(target_os = "linux")]

use curvine_config::FuseConf;
use curvine_fuse::raw::{fuse_mount_pure, fuse_umount_pure};
use curvine_fuse::FuseUtils;
use curvine_sys::pipe::PipeFd;
use std::path::{Path, PathBuf};

const CAP_SYS_ADMIN: u32 = 21;
const CAP_SYS_RESOURCE: u32 = 24;

struct MountGuard {
    path: PathBuf,
    fd: Option<std::os::fd::RawFd>,
}

impl MountGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, fd: None }
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        if self.fd.is_some() && mountpoint_present(&self.path) {
            let _ = fuse_umount_pure(&self.path);
        }
        if let Some(fd) = self.fd.take() {
            unsafe { libc::close(fd) };
        }
        let _ = std::fs::remove_dir(&self.path);
    }
}

fn mountpoint_present(path: &Path) -> bool {
    let target = path.to_string_lossy();
    std::fs::read_to_string("/proc/self/mountinfo")
        .expect("read mount table")
        .lines()
        .any(|line| line.split_whitespace().nth(4) == Some(target.as_ref()))
}

fn has_effective_capability(capability: u32) -> bool {
    let status = std::fs::read_to_string("/proc/self/status").expect("read process status");
    let cap_eff = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:\t"))
        .expect("find CapEff")
        .trim();
    let cap_eff = u64::from_str_radix(cap_eff, 16).expect("parse CapEff");
    cap_eff & (1_u64 << capability) != 0
}

#[test]
#[ignore = "requires CAP_SYS_ADMIN and an assigned /dev/fuse device"]
fn raw_mount_unmount_round_trip() {
    assert!(
        has_effective_capability(CAP_SYS_ADMIN),
        "test requires CAP_SYS_ADMIN"
    );

    let path = std::env::temp_dir().join(format!(
        "curvine-fuse-e2e-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&path).expect("create isolated mount point");
    let mut guard = MountGuard::new(path);

    let fd = fuse_mount_pure(&guard.path, &FuseConf::default())
        .expect("raw FUSE mount must succeed in the privileged E2E environment");
    guard.fd = Some(fd);
    assert!(
        mountpoint_present(&guard.path),
        "successful mount must appear in /proc/self/mountinfo"
    );

    let _ = fuse_umount_pure(&guard.path);
    assert!(
        !mountpoint_present(&guard.path),
        "FUSE unmount must remove the mount-table entry"
    );
}

#[test]
#[ignore = "requires /dev/fuse but must run without CAP_SYS_ADMIN"]
fn unprivileged_mount_reports_permission_remediation() {
    assert!(
        !has_effective_capability(CAP_SYS_ADMIN),
        "test requires CAP_SYS_ADMIN to be absent"
    );

    let path = std::env::temp_dir().join(format!(
        "curvine-fuse-denied-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&path).expect("create isolated mount point");
    let mut guard = MountGuard::new(path);

    let error = match fuse_mount_pure(&guard.path, &FuseConf::default()) {
        Ok(fd) => {
            guard.fd = Some(fd);
            panic!("an unprivileged process must not be allowed to mount FUSE");
        }
        Err(error) => error,
    };

    assert!(
        matches!(
            error.raw_error().raw_os_error(),
            Some(libc::EPERM) | Some(libc::EACCES)
        ),
        "expected EPERM or EACCES, got {error}"
    );
    let message = error.to_string();
    assert!(message.contains("CAP_SYS_ADMIN"), "message: {message}");
    assert!(message.contains("security-policy"), "message: {message}");
}

#[test]
#[ignore = "requires a restricted runtime with pipe-max-size below Curvine's FUSE buffer"]
fn restricted_splice_pipe_growth_returns_eperm() {
    assert!(
        !has_effective_capability(CAP_SYS_RESOURCE),
        "test requires CAP_SYS_RESOURCE to be absent"
    );

    let requested_size = FuseUtils::get_fuse_buf_size();
    let pipe_max_size = std::fs::read_to_string("/proc/sys/fs/pipe-max-size")
        .expect("read pipe-max-size")
        .trim()
        .parse::<usize>()
        .expect("parse pipe-max-size");
    assert!(
        requested_size > pipe_max_size,
        "test requires requested FUSE buffer {requested_size} > pipe-max-size {pipe_max_size}"
    );

    let error = match PipeFd::new(requested_size, false, false) {
        Ok(_) => panic!("restricted runtime unexpectedly grew pipe to {requested_size} bytes"),
        Err(error) => error,
    };
    assert_eq!(
        error.raw_os_error(),
        Some(libc::EPERM),
        "expected EPERM from F_SETPIPE_SZ, got {error}"
    );
}
