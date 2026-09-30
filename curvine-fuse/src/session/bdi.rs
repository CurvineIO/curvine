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

//! Linux-only helpers for overriding the FUSE mount BDI read-ahead size by writing
//! `/sys/class/bdi/<major>:<minor>/read_ahead_kb` after mount.

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

    // The mount point can be shadowed: under Kubernetes the mount directory is
    // typically a hostPath bind mount (e.g. ext4) that the FUSE filesystem is
    // mounted over, so mountinfo holds several entries for the same path.
    // Taking the first match would resolve to the bind mount's block device —
    // whose BDI either does not exist (partition) or, worse, belongs to the
    // host disk. Only a fuse-fstype entry is ours; keep the last one (the
    // newest mount wins the path).
    let mut bdi_path = None;
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
        if mount_point != target {
            continue;
        }
        // A variable number of optional fields follows; per proc(5) the fstype
        // is the field right after the "-" separator.
        let mut rest = fields.skip_while(|f| *f != "-").skip(1);
        if matches!(rest.next(), Some(fstype) if fstype == "fuse" || fstype.starts_with("fuse.")) {
            bdi_path = Some(bdi_path_from_majmin(majmin));
        }
    }

    bdi_path
}

/// Write `kb` into the mount's BDI sysfs entry; failures only warn.
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
    // Retry briefly until the kernel creates the BDI sysfs entry.
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
                let hint = if e.kind() == std::io::ErrorKind::ReadOnlyFilesystem {
                    "; /sys is read-only here (typical inside containers) — mount a \
                     writable host /sys into the container or set read_ahead_kb from \
                     the host"
                } else {
                    ""
                };
                warn!(
                    "bdi max_readahead_kb skip: write {} failed: {}{} (mount continues)",
                    bdi_path, e, hint
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
    fn mountinfo_bdi_path_from_prefers_fuse_over_shadowing_bind_mount() {
        // Kubernetes hostPath pattern: the mount directory is a bind mount of a
        // block device partition, and the FUSE filesystem is mounted over it.
        // The partition entry comes first; resolving to it would target the
        // host disk's BDI instead of the FUSE one.
        let mountinfo = "\
            14896 14612 8:2 /curvinefs /mnt/curvinefs rw,relatime shared:1 - ext4 /dev/sda2 rw,stripe=64\n\
            7334 14896 0:481 / /mnt/curvinefs rw,relatime shared:3179 - fuse.curvinefs curvinefs rw,user_id=0,group_id=0\n";
        assert_eq!(
            mountinfo_bdi_path_from(mountinfo, Path::new("/mnt/curvinefs")),
            Some("/sys/class/bdi/0:481/read_ahead_kb".to_string())
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_ignores_non_fuse_entries() {
        // Only a fuse mount's BDI is ours to tune: a path that matches solely a
        // block-device mount must resolve to nothing rather than risk writing
        // the host disk's readahead.
        let mountinfo =
            "14896 14612 8:2 /curvinefs /mnt/curvinefs rw,relatime shared:1 - ext4 /dev/sda2 rw\n";
        assert_eq!(
            mountinfo_bdi_path_from(mountinfo, Path::new("/mnt/curvinefs")),
            None
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
