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

    // mountinfo order (and mount ID magnitude) does not describe stacking.
    // Keep every same-path entry until the top of the stack is identified:
    // filtering out foreign filesystems first could expose a hidden Curvine BDI.
    struct Mount<'a> {
        id: u64,
        parent: u64,
        majmin: &'a str,
        fstype: &'a str,
        source: &'a str,
    }

    let mut mounts = std::collections::HashMap::new();
    let mut parents = std::collections::HashSet::new();
    for line in mountinfo.lines() {
        let mut fields = line.split_whitespace();
        // mountinfo fields: id parent major:minor root mount_point ...
        let (id, parent, majmin) = (fields.next(), fields.next(), fields.next());
        if fields.nth(1) != Some(target.as_ref()) {
            continue;
        }

        // A malformed matching entry makes the stack unsafe to identify; do
        // not skip it and accidentally fall back to the mount underneath it.
        let id = id?.parse::<u64>().ok()?;
        let parent = parent?.parse::<u64>().ok()?;
        let mut rest = fields.skip_while(|f| *f != "-").skip(1);
        let mount = Mount {
            id,
            parent,
            majmin: majmin?,
            fstype: rest.next()?,
            source: rest.next()?,
        };
        if mounts.insert(id, mount).is_some() {
            return None;
        }
        // A namespace root may be its own parent; it does not hide itself.
        if parent != id {
            parents.insert(parent);
        }
    }

    // A stacked mount's parent is the previous mount at the same path. The
    // top-most entry is therefore the only one that is not another's parent.
    // See proc_pid_mountinfo(5). Multiple candidates are ambiguous: skip them.
    let mut tops = mounts.values().filter(|mnt| !parents.contains(&mnt.id));
    let top = tops.next()?;
    if tops.next().is_some() {
        return None;
    }

    // Require a single connected stack, even for malformed input containing a
    // disconnected cycle beside an otherwise unique top-most candidate.
    let mut current = top;
    let mut depth = 1;
    while current.parent != current.id {
        let Some(parent) = mounts.get(&current.parent) else {
            break;
        };
        depth += 1;
        if depth > mounts.len() {
            return None;
        }
        current = parent;
    }
    if depth != mounts.len() {
        return None;
    }

    // New mounts use subtype=curvinefs. Restoring a pre-subtype mount through
    // CURVINE_FUSE_STATE_PATH retains plain fuse, but its source is curvinefs.
    if top.fstype == "fuse.curvinefs" || (top.fstype == "fuse" && top.source == "curvinefs") {
        Some(bdi_path_from_majmin(top.majmin))
    } else {
        None
    }
}

/// Write `kb` into the mount's BDI sysfs entry; failures only warn.
#[cfg(target_os = "linux")]
pub fn apply_max_readahead_kb(mnt_path: &Path, kb: u32) {
    let bdi_path = match mountinfo_bdi_path(mnt_path) {
        Ok(Some(path)) => path,
        Ok(None) => {
            warn!(
                "bdi max_readahead_kb skip: no eligible top-most Curvine mountinfo entry for {} (mount absent, non-Curvine, or ambiguous; mount continues)",
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
                    "; /sys is read-only here (typical inside containers); set read_ahead_kb \
                     from the host or expose only the required writable BDI sysfs path \
                     with appropriate permissions. Kubernetes securityContext.privileged: true \
                     is another option if policy allows, but grants broad host privileges"
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
        // A path matching only a block-device mount must resolve to nothing
        // rather than risk writing the host disk's readahead.
        let mountinfo =
            "14896 14612 8:2 /curvinefs /mnt/curvinefs rw,relatime shared:1 - ext4 /dev/sda2 rw\n";
        assert_eq!(
            mountinfo_bdi_path_from(mountinfo, Path::new("/mnt/curvinefs")),
            None
        );
    }

    // Exercise every file order independently of the mount IDs. IDs can be
    // reused, so the newest mount deliberately has a smaller ID than its parent.
    fn assert_mount_orders(lines: &[&str], expected: Option<&str>) {
        fn visit(lines: &mut [&str], index: usize, expected: &Option<String>) {
            if index == lines.len() {
                let mountinfo = lines.join("\n");
                assert_eq!(
                    mountinfo_bdi_path_from(&mountinfo, Path::new("/mnt/curvinefs")),
                    *expected,
                    "mountinfo:\n{mountinfo}"
                );
                return;
            }
            for next in index..lines.len() {
                lines.swap(index, next);
                visit(lines, index + 1, expected);
                lines.swap(index, next);
            }
        }
        visit(&mut lines.to_vec(), 0, &expected.map(bdi_path_from_majmin));
    }

    #[test]
    fn mountinfo_bdi_path_from_selects_topmost_curvine_in_any_order() {
        assert_mount_orders(
            &[
                "90 1 8:2 /curvinefs /mnt/curvinefs rw - ext4 /dev/sda2 rw",
                "80 90 0:100 / /mnt/curvinefs rw shared:1 - fuse.curvinefs curvinefs rw",
                "20 80 0:200 / /mnt/curvinefs rw shared:2 master:3 - fuse.curvinefs curvinefs rw",
            ],
            Some("0:200"),
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_selects_curvine_over_foreign_fuse_in_any_order() {
        for fstype in ["fuse.sshfs", "fuse.lxcfs", "fuse"] {
            let lower = format!("80 1 0:100 / /mnt/curvinefs rw - {fstype} foreign rw");
            assert_mount_orders(
                &[
                    &lower,
                    "20 80 0:200 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                ],
                Some("0:200"),
            );
        }
    }

    #[test]
    fn mountinfo_bdi_path_from_does_not_fall_back_to_hidden_curvine() {
        for fstype in ["ext4", "fuse.sshfs", "fuse.lxcfs", "fuse", "fuseblk"] {
            let upper = format!("20 80 0:200 / /mnt/curvinefs rw - {fstype} foreign rw");
            assert_mount_orders(
                &[
                    "80 1 0:100 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                    &upper,
                ],
                None,
            );
        }
    }

    #[test]
    fn mountinfo_bdi_path_from_rejects_foreign_fuse_types_and_sources() {
        for (fstype, source) in [
            ("fuse.sshfs", "curvinefs"),
            ("fuse.lxcfs", "curvinefs"),
            ("fuse.curvinefs.extra", "curvinefs"),
            ("fuseblk", "curvinefs"),
            ("fuse", "foreign"),
        ] {
            let line = format!("20 1 0:200 / /mnt/curvinefs rw - {fstype} {source} rw");
            assert_mount_orders(&[&line], None);
        }
    }

    #[test]
    fn mountinfo_bdi_path_from_preserves_legacy_curvine_in_any_order() {
        assert_mount_orders(
            &[
                "80 1 8:2 /curvinefs /mnt/curvinefs rw - ext4 /dev/sda2 rw",
                "20 80 0:200 / /mnt/curvinefs rw - fuse curvinefs rw",
            ],
            Some("0:200"),
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_rejects_ambiguous_topmost_mounts() {
        assert_mount_orders(
            &[
                "90 1 8:2 /curvinefs /mnt/curvinefs rw - ext4 /dev/sda2 rw",
                "80 90 0:100 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                "20 90 0:200 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
            ],
            None,
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_handles_self_parent_namespace_root() {
        let root = "20 20 0:200 / / rw - fuse.curvinefs curvinefs rw";
        assert_eq!(
            mountinfo_bdi_path_from(root, Path::new("/")),
            Some(bdi_path_from_majmin("0:200")),
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_ignores_mounts_at_other_paths() {
        assert_mount_orders(
            &[
                "80 1 0:100 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                "20 80 8:2 / /mnt/curvinefs/child rw - ext4 /dev/sda2 rw",
            ],
            Some("0:100"),
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_rejects_malformed_matching_entry() {
        for upper in [
            "bad 80 0:200 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
            "20 bad 0:200 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
            "20 80 0:200 / /mnt/curvinefs rw",
            "20 80 0:200 / /mnt/curvinefs rw -",
            "20 80 0:200 / /mnt/curvinefs rw - fuse.curvinefs",
        ] {
            assert_mount_orders(
                &[
                    "80 1 0:100 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                    upper,
                ],
                None,
            );
        }
    }

    #[test]
    fn mountinfo_bdi_path_from_rejects_cycles() {
        assert_mount_orders(
            &[
                "80 20 0:100 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                "20 80 0:200 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
            ],
            None,
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_rejects_disconnected_cycle() {
        assert_mount_orders(
            &[
                "80 20 0:100 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                "20 80 0:200 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                "30 1 0:300 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
            ],
            None,
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_rejects_duplicate_mount_ids() {
        assert_mount_orders(
            &[
                "20 1 0:100 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
                "20 1 0:200 / /mnt/curvinefs rw - fuse.curvinefs curvinefs rw",
            ],
            None,
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_returns_none_for_absent_mount() {
        for mountinfo in [
            "",
            "malformed unrelated record",
            "20 1 0:200 / /elsewhere rw - fuse.curvinefs curvinefs rw",
        ] {
            assert_eq!(
                mountinfo_bdi_path_from(mountinfo, Path::new("/mnt/curvinefs")),
                None,
            );
        }
    }

    #[test]
    fn apply_does_not_panic_on_missing_path() {
        // The function must remain best-effort: a non-existent mount path
        // should produce a warning, not a panic / propagated error.
        let bogus = PathBuf::from("/definitely/not/a/real/mount/point/curvine-bdi-test");
        apply_max_readahead_kb(&bogus, 1024);
    }
}
