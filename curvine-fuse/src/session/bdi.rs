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

//! Linux-only helpers to override the FUSE mount BDI's read-ahead size.
//!
//! After the mount syscall completes successfully, sysfs exposes
//! `/sys/class/bdi/<major>:<minor>/read_ahead_kb` for the FUSE
//! superblock device. Writing the desired value here lets the FUSE kernel
//! issue larger read requests (e.g. 1 MiB) instead of the 128 KB default.
//!
//! Note: the user-facing config is named `fuse.max_readahead_kb` to mirror
//! the FUSE protocol field `max_readahead`. The kernel sysfs entry, however,
//! is `read_ahead_kb` (without the "max_" prefix), and that's what we write.

use std::path::Path;

#[cfg(target_os = "linux")]
use log::{info, warn};

#[cfg(target_os = "linux")]
fn bdi_path_from_majmin(majmin: &str) -> String {
    format!("/sys/class/bdi/{}/read_ahead_kb", majmin)
}

#[cfg(target_os = "linux")]
fn mountinfo_bdi_path(mnt_path: &Path) -> std::io::Result<Option<String>> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(mountinfo_bdi_path_from(&mountinfo, mnt_path))
}

#[cfg(target_os = "linux")]
fn mountinfo_bdi_path_from(mountinfo: &str, mnt_path: &Path) -> Option<String> {
    let target = mnt_path.to_string_lossy();

    for line in mountinfo.lines() {
        let mut fields = line.split_whitespace();
        let majmin = match fields.nth(2) {
            Some(v) => v,
            None => continue,
        };
        // mountinfo fields are: id parent major:minor root mount_point ...
        // We already consumed through major:minor, so skip root and read mount_point.
        let mount_point = match fields.nth(1) {
            Some(v) => v,
            None => continue,
        };
        if mount_point == target {
            return Some(bdi_path_from_majmin(majmin));
        }
    }

    None
}

/// Write `kb` into the BDI sysfs entry that backs `mnt_path`.
///
/// On any failure (mount path stat error, sysfs path missing, no write
/// permission, etc.) this function logs a warning and returns: the caller's
/// mount succeeds either way. On non-Linux platforms this is a no-op.
#[cfg(target_os = "linux")]
pub fn apply_max_readahead_kb(mnt_path: &Path, kb: u32) {
    let bdi_path = match mountinfo_bdi_path(mnt_path) {
        Ok(Some(path)) => path,
        Ok(None) => {
            warn!(
                "bdi max_readahead_kb skip: mountinfo entry for {} not found (mount continues)",
                mnt_path.display()
            );
            return;
        }
        Err(e) => {
            warn!(
                "bdi max_readahead_kb skip: read /proc/self/mountinfo failed: {} (mount continues)",
                e
            );
            return;
        }
    };
    // The BDI sysfs entry may not be immediately available after the mount
    // syscall. Retry a few times with a short delay to give the kernel time
    // to create /sys/class/bdi/<maj>:<min>/read_ahead_kb.
    let mut tries = 10;
    while tries > 0 {
        match std::fs::write(&bdi_path, kb.to_string()) {
            Ok(()) => {
                info!(
                    "bdi max_readahead_kb set: path={}, bdi={}, value={}",
                    mnt_path.display(),
                    bdi_path,
                    kb
                );
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tries -= 1;
                if tries > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
            }
            Err(e) => {
                warn!(
                    "bdi max_readahead_kb skip: write {} failed: {} (mount continues)",
                    bdi_path, e
                );
                return;
            }
        }
    }
    warn!(
        "bdi max_readahead_kb skip: {} not found after retries (mount continues)",
        bdi_path
    );
}

#[cfg(not(target_os = "linux"))]
pub fn apply_max_readahead_kb(_mnt_path: &Path, _kb: u32) {
    // sysfs / BDI is Linux-only; intentional no-op elsewhere.
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn bdi_path_from_majmin_formats_sysfs_path() {
        assert_eq!(
            bdi_path_from_majmin("8:1"),
            "/sys/class/bdi/8:1/read_ahead_kb"
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_matches_mount_point_field() {
        let mountinfo = "126 32 0:114 / /curvine-fuse rw,relatime shared:98 - fuse curvinefs rw,user_id=0,group_id=0,allow_other\n";
        assert_eq!(
            mountinfo_bdi_path_from(mountinfo, Path::new("/curvine-fuse")),
            Some("/sys/class/bdi/0:114/read_ahead_kb".to_string())
        );
    }

    #[test]
    fn apply_does_not_panic_on_missing_path() {
        // The function must remain best-effort: a non-existent mount path
        // should produce a warning, not a panic / propagated error.
        let bogus = PathBuf::from("/definitely/not/a/real/mount/point/curvine-bdi-test");
        apply_max_readahead_kb(&bogus, 1024);
    }
}
