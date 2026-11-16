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

//! Equivalence checks for the status `mkdir` returns.
//!
//! `mkdir` used to project the newly committed directory into a `FileStatus`
//! through the generic read path. That projection is expensive (a live dir-node
//! view, xattr and policy clones, a child count that takes the child map) and it
//! runs inside the parent write lock. The replacement builds the status from the
//! node copy that the journal already owns, so these cases pin the contract the
//! replacement must keep: whatever `mkdir` returns must equal what a later
//! `file_status` on the same path returns, field by field, except that `atime`
//! may advance between the two reads.

use super::*;

/// Compares every field a fresh mkdir must agree with, skipping `atime` (the
/// second read can advance it) and `path`/`name` (asserted separately, because a
/// mismatch there means the wrong entry was projected).
fn assert_same_status(returned: &FileStatus, later: &FileStatus) {
    assert_eq!(returned.id, later.id, "id");
    assert_eq!(returned.path, later.path, "path");
    assert_eq!(returned.name, later.name, "name");
    assert_eq!(returned.is_dir, later.is_dir, "is_dir");
    assert_eq!(returned.mtime, later.mtime, "mtime");
    assert_eq!(returned.children_num, later.children_num, "children_num");
    assert_eq!(returned.is_complete, later.is_complete, "is_complete");
    assert_eq!(returned.len, later.len, "len");
    assert_eq!(returned.replicas, later.replicas, "replicas");
    assert_eq!(returned.block_size, later.block_size, "block_size");
    assert_eq!(returned.file_type, later.file_type, "file_type");
    assert_eq!(returned.x_attr, later.x_attr, "x_attr");
    assert_eq!(returned.storage_policy, later.storage_policy, "storage_policy");
    assert_eq!(returned.owner, later.owner, "owner");
    assert_eq!(returned.group, later.group, "group");
    assert_eq!(returned.mode, later.mode, "mode");
    assert_eq!(returned.nlink, later.nlink, "nlink");
    assert_eq!(returned.target, later.target, "target");
}

#[test]
fn mkdir_status_matches_a_later_file_status() {
    let fs = test_fs("mkdir-status-eq");

    let returned = fs.mkdir("/plain", false).expect("mkdir");
    let later = fs.file_status("/plain").expect("file_status");

    assert!(returned.is_dir);
    assert_eq!(returned.children_num, 0, "a fresh directory has no children");
    assert_eq!(returned.len, 0);
    assert_same_status(&returned, &later);
}

#[test]
fn mkdir_status_uses_the_setgid_parent_group_and_mode() {
    let fs = test_fs("mkdir-status-setgid");

    // A parent carrying setgid: children must inherit its group and gain setgid.
    let mut opts = MkdirOpts::with_create(false);
    opts.mode = 0o2775;
    opts.group = "inherited".to_string();
    fs.mkdir_with_opts("/sg", opts).expect("mkdir /sg");

    let returned = fs.mkdir("/sg/child", false).expect("mkdir child");
    let later = fs.file_status("/sg/child").expect("file_status");

    // The adjustment happens inside the create path, so the returned status has
    // to carry the adjusted group and mode, not the caller's original opts.
    assert_eq!(returned.group, "inherited", "setgid group inherited");
    assert_ne!(
        returned.mode & 0o2000,
        0,
        "setgid bit set on the returned mode"
    );
    assert_same_status(&returned, &later);
}

#[test]
fn mkdir_status_drops_the_internal_ctime_marker() {
    let fs = test_fs("mkdir-status-xattr");

    let mut opts = MkdirOpts::with_create(false);
    opts.x_attr
        .insert("user.tag".to_string(), b"value".to_vec());
    opts.x_attr.insert(
        INTERNAL_CTIME_XATTR.to_string(),
        b"stale".to_vec(),
    );
    let returned = fs
        .mkdir_with_opts("/tagged", opts)
        .expect("mkdir with xattr");
    let later = fs.file_status("/tagged").expect("file_status");

    assert_eq!(
        returned.x_attr.get("user.tag").map(|v| v.as_slice()),
        Some(&b"value"[..]),
        "the caller's xattr survives"
    );
    assert!(
        !returned.x_attr.contains_key(INTERNAL_CTIME_XATTR),
        "the internal ctime marker must never be returned"
    );
    assert_same_status(&returned, &later);
}

#[test]
fn mkdir_status_describes_the_target_of_a_multi_level_create() {
    let fs = test_fs("mkdir-status-multilevel");

    let returned = fs.mkdir("/a/b/c", true).expect("mkdir -p /a/b/c");
    let later = fs.file_status("/a/b/c").expect("file_status");

    assert_eq!(returned.path, "/a/b/c", "the target, not an intermediate");
    assert_eq!(returned.name, "c");
    assert_eq!(returned.children_num, 0);
    assert_same_status(&returned, &later);
}

#[test]
fn mkdir_status_reports_no_children_right_after_creation() {
    let fs = std::sync::Arc::new(test_fs("mkdir-status-child"));
    fs.mkdir("/p", true).expect("mkdir /p");

    let returned = fs.mkdir("/p/d", false).expect("mkdir /p/d");
    assert_eq!(
        returned.children_num, 0,
        "the returned status must not see children created afterwards"
    );

    // A child created right after the mkdir returns must not retroactively
    // appear in the status the mkdir already produced.
    let fs_child = fs.clone();
    let child = std::thread::spawn(move || {
        fs_child
            .create("/p/d/child", false)
            .expect("create under the new directory")
    });
    child.join().expect("child thread");
    assert_eq!(
        returned.children_num, 0,
        "the already returned status stays at zero children"
    );

    let later = fs.file_status("/p/d").expect("file_status");
    assert_eq!(later.children_num, 1, "the later read does see the child");
}
