// Copyright 2026 OPPO
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

//! Coverage for the read walks that borrow the root out of the held view
//! instead of cloning it (`resolve_edge_ids` and `plan_memory_read_walk`).
//!
//! The borrow is only sound while the view is held, so these cases pin down the
//! observable behaviour it must preserve: the root as a target, a child of the
//! root, a missing child, a deep path, a page listing of the root, and reads
//! after a restore replaced the tree.

use super::*;

/// A file directly under the root resolves through the borrowed root.
#[test]
fn a_child_of_the_root_is_resolved() {
    let fs = test_fs("root-borrow-child");
    fs.create("/f", false).unwrap();

    let status = fs.file_status("/f").unwrap();
    assert_eq!(status.name, "f");
    assert!(!status.is_dir);
}

/// A missing child of the root still reports FileNotFound: the borrowed root
/// must not turn a missing edge into a panic or a stale hit.
#[test]
fn a_missing_child_of_the_root_is_not_found() {
    let fs = test_fs("root-borrow-missing");

    let error = fs.file_status("/absent").unwrap_err();
    assert!(
        error.to_string().contains("not found") || error.to_string().contains("No such"),
        "unexpected error for a missing child: {error}"
    );
}

/// The root itself is still a valid target and still reported as a directory.
#[test]
fn the_root_is_still_a_directory_target() {
    let fs = test_fs("root-borrow-target");

    let status = fs.file_status("/").unwrap();
    assert!(status.is_dir, "the root must be reported as a directory");
}

/// A deeper path walks past the root edge and keeps every component's identity.
#[test]
fn a_deep_path_is_resolved_through_the_borrowed_root() {
    let fs = test_fs("root-borrow-deep");
    fs.mkdir("/a", true).unwrap();
    fs.mkdir("/a/b", true).unwrap();
    fs.create("/a/b/c", false).unwrap();

    let status = fs.file_status("/a/b/c").unwrap();
    assert_eq!(status.name, "c");
    assert_eq!(fs.file_status("/a/b").unwrap().name, "b");

    // A missing leaf under the same deep prefix is still reported as missing,
    // so the walk did not silently stop at an intermediate component.
    assert!(fs.file_status("/a/b/absent").is_err());
}

/// Listing the root builds its page from the borrowed root's children.
#[test]
fn listing_the_root_returns_its_children() {
    let fs = test_fs("root-borrow-page");
    fs.mkdir("/d", true).unwrap();
    fs.create("/f", false).unwrap();

    let names: Vec<String> = fs
        .list_options("/", ListOptions::default())
        .unwrap()
        .into_iter()
        .map(|status| status.name)
        .collect();

    assert!(names.contains(&"d".to_string()), "missing d in {names:?}");
    assert!(names.contains(&"f".to_string()), "missing f in {names:?}");
}

/// After a restore replaced the tree, reads must use the new root. A borrowed
/// root from a stale view would keep the pre-restore children visible.
#[test]
fn reads_after_a_restore_use_the_new_root() {
    let fs = test_fs("root-borrow-restore");
    fs.create("/kept", false).unwrap();
    fs.restore_from_rocksdb().unwrap();

    assert_eq!(fs.file_status("/kept").unwrap().name, "kept");

    // A child created after the restore must be visible through the new root.
    fs.create("/added", false).unwrap();
    assert_eq!(fs.file_status("/added").unwrap().name, "added");

    let names: Vec<String> = fs
        .list_options("/", ListOptions::default())
        .unwrap()
        .into_iter()
        .map(|status| status.name)
        .collect();
    assert!(names.contains(&"added".to_string()), "missing added: {names:?}");
}
