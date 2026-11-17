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

//! Deterministic interleavings for the write-free `exists` walk (design S9.4).
//!
//! The reader parks at `exists-opt-leaf-<path>`: the walk has resolved the leaf
//! and the per-directory sequences have not been re-checked yet. The mutation
//! runs on the test thread inside that window; a sequence that missed it would
//! return the stale answer. No sleeps: arrival is a channel, release is explicit.

use super::exists_version_fastpath_test::{exists_parked, WAIT};
use super::*;
use crate::master::fs::master_filesystem::memory_read_gates;

const GATE_LEAF: &str = "exists-opt-leaf-";

fn parked_exists<F>(fs: &MasterFilesystem, path: &str, mutate: F) -> FsResult<bool>
where
    F: FnOnce(&MasterFilesystem),
{
    let _mode = memory_read_gates::hold_optimistic_mode(false);
    let _ = WAIT;
    exists_parked(fs, path, format!("{GATE_LEAF}{path}"), mutate)
}

#[test]
fn delete_inside_the_window_is_not_reported_present() {
    let fs = test_fs("exists-opt-delete");
    fs.mkdir("/op", true).unwrap();
    fs.create("/op/file", true).unwrap();
    let present = parked_exists(&fs, "/op/file", |w| {
        w.delete("/op/file", false).unwrap();
    })
    .unwrap();
    assert!(!present, "a delete that landed before the check must win");
}

#[test]
fn create_inside_the_window_is_reported_present() {
    let fs = test_fs("exists-opt-create");
    fs.mkdir("/op", true).unwrap();
    let present = parked_exists(&fs, "/op/new", |w| {
        w.create("/op/new", false).unwrap();
    })
    .unwrap();
    assert!(present, "a create that landed before the check must win");
}

#[test]
fn rename_across_parents_inside_the_window_follows_the_new_tree() {
    let fs = test_fs("exists-opt-rename");
    fs.mkdir("/op/a", true).unwrap();
    fs.mkdir("/op/b", true).unwrap();
    fs.create("/op/a/f", true).unwrap();
    let present = parked_exists(&fs, "/op/a/f", |w| {
        w.rename("/op/a/f", "/op/b/f", RenameFlags::empty())
            .unwrap();
    })
    .unwrap();
    assert!(
        !present,
        "the source name is gone once the rename published"
    );
    assert!(fs.exists("/op/b/f").unwrap());
}

#[test]
fn ancestor_removal_inside_the_window_hides_the_leaf() {
    let fs = test_fs("exists-opt-ancestor");
    fs.mkdir("/op/a", true).unwrap();
    fs.create("/op/a/f", true).unwrap();
    let present = parked_exists(&fs, "/op/a/f", |w| {
        w.delete("/op/a", true).unwrap();
    })
    .unwrap();
    assert!(!present, "removing an ancestor must invalidate the walk");
}

#[test]
fn same_name_replacement_inside_the_window_is_still_present() {
    let fs = test_fs("exists-opt-replace");
    fs.mkdir("/op", true).unwrap();
    fs.create("/op/f", true).unwrap();
    let present = parked_exists(&fs, "/op/f", |w| {
        w.delete("/op/f", false).unwrap();
        w.create("/op/f", false).unwrap();
    })
    .unwrap();
    assert!(present, "delete then create leaves the name present");
}

#[test]
fn a_stable_tree_answers_without_the_locked_path() {
    let fs = test_fs("exists-opt-stable");
    fs.mkdir("/op/d", true).unwrap();
    fs.create("/op/d/f", true).unwrap();
    assert!(fs.exists("/op/d/f").unwrap());
    assert!(fs.exists("/op/d").unwrap());
    assert!(!fs.exists("/op/d/missing").unwrap());
    assert!(!fs.exists("/op/d/f/x").unwrap());
    assert!(!fs.exists("/missing/x").unwrap());
}

#[test]
fn a_missing_row_behind_a_live_edge_surfaces_the_documented_error() {
    let fs = test_fs("exists-opt-dangling");
    fs.mkdir("/op", true).unwrap();
    let id = fs.create("/op/file", true).unwrap().id;
    {
        let view = fs.fs_dir.view().unwrap();
        let mut batch = view.store.new_batch();
        batch.delete_inode(id).unwrap();
        batch.commit().unwrap();
    }
    let error = fs.exists("/op/file").unwrap_err().to_string();
    assert!(
        error.contains("directory edge references missing inode"),
        "got {error}"
    );
}
