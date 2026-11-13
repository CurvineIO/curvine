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

//! Topology cases split out of `recursive_lock_set_test.rs` by
//! responsibility; reuse the parent fixture through `use super::*`.

use super::*;

#[test]
fn recursive_delete_keeps_a_subtree_that_moved_out_before_the_plan() {
    let fs = test_fs("recursive-moved-out");
    fs.mkdir("/a/d0/d1", true).unwrap();
    fs.create("/a/d0/d1/file", false).unwrap();
    fs.mkdir("/b", false).unwrap();
    let moved_id = fs.file_status("/a/d0/d1").unwrap().id;

    // The move completes before the delete plans: /a no longer owns d1, so d1
    // is out of scope and must survive untouched, keeping its inode id.
    fs.rename("/a/d0/d1", "/b/d1", RenameFlags::empty())
        .unwrap();
    fs.delete("/a", true).unwrap();

    let survivor = fs
        .file_status("/b/d1")
        .expect("the moved subtree must survive the delete");
    assert_eq!(survivor.id, moved_id, "the moved inode must keep its id");
    assert!(
        fs.file_status("/b/d1/file").is_ok(),
        "the moved subtree's child must survive too"
    );
    assert!(
        fs.file_status("/a").is_err(),
        "the source tree must be gone"
    );
}

#[test]
fn recursive_delete_does_not_orphan_a_child_present_before_the_plan() {
    let fs = test_fs("recursive-orphan");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/new", false).unwrap();
    let child_id = fs.file_status("/a/d0/new").unwrap().id;

    // The child is in the subtree when the plan runs: it must be covered, not
    // left behind as an orphan.
    fs.delete("/a", true).unwrap();

    assert!(
        fs.file_status("/a/d0/new").is_err(),
        "the child must be removed with its tree"
    );
    assert!(
        fs.file_status_by_id("/a/d0/new", Some(child_id)).is_err(),
        "no orphan inode may remain reachable by id"
    );
}

#[test]
fn recursive_delete_covers_a_node_moved_into_the_subtree_before_the_plan() {
    let fs = test_fs("recursive-moved-in");
    fs.mkdir("/a/d0", true).unwrap();
    fs.mkdir("/b", true).unwrap();
    fs.create("/b/x", false).unwrap();
    let moved_id = fs.file_status("/b/x").unwrap().id;

    // The node already lives inside the subtree when the plan runs.
    fs.rename("/b/x", "/a/d0/x", RenameFlags::empty()).unwrap();
    fs.delete("/a", true).unwrap();

    assert!(
        fs.file_status("/a/d0/x").is_err(),
        "a node inside the planned subtree must be removed with it"
    );
    assert!(
        fs.file_status_by_id("/a/d0/x", Some(moved_id)).is_err(),
        "the moved-in inode must not survive as an orphan"
    );
    assert!(
        fs.file_status("/b").is_ok(),
        "the empty source dir survives"
    );
}

/// Deleting one name of an externally hard-linked file must keep the inode and
/// its content reachable through the surviving name.
#[test]
fn recursive_delete_keeps_an_external_hardlink_alive() {
    let fs = test_fs("recursive-hardlink-external");
    fs.mkdir("/a", true).unwrap();
    fs.create("/a/file", false).unwrap();
    fs.link("/a/file".to_string(), "/survivor".to_string())
        .unwrap();
    let file_id = fs.file_status("/a/file").unwrap().id;

    fs.delete("/a", true).unwrap();

    let survivor = fs
        .file_status("/survivor")
        .expect("an external hard link must keep the inode alive");
    assert_eq!(
        survivor.id, file_id,
        "the surviving name must still resolve to the same inode"
    );
    assert!(
        fs.file_status("/a").is_err(),
        "the deleted subtree must be gone"
    );
}

/// Removing the last name drops the inode; removing one of two names does not.
#[test]
fn last_link_delete_drops_the_inode_only_at_the_end() {
    let fs = test_fs("recursive-last-link");
    fs.mkdir("/d", true).unwrap();
    fs.create("/d/f", false).unwrap();
    fs.link("/d/f".to_string(), "/d/g".to_string()).unwrap();
    let file_id = fs.file_status("/d/f").unwrap().id;

    fs.delete("/d/f", false).unwrap();
    let remaining = fs
        .file_status("/d/g")
        .expect("the second name must keep the inode");
    assert_eq!(remaining.id, file_id);

    fs.delete("/d/g", false).unwrap();
    assert!(
        fs.file_status_by_id("/d/g", Some(file_id)).is_err(),
        "removing the last name must drop the inode"
    );
    assert!(
        fs.file_status("/d").is_ok(),
        "the parent directory survives"
    );
}

#[path = "recursive_lock_set_window_test.rs"]
mod recursive_lock_set_window_test;
