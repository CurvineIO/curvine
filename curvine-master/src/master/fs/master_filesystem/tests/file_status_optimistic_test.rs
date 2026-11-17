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

//! Deterministic interleavings for the write-free `file_status` walk (S9.4).
//!
//! The reader parks after the leaf and its row are resolved and before the
//! directory sequences are re-checked. Mutations run on the test thread in that
//! window; a stale answer would mean a sequence or the row check missed them.

use super::*;
use crate::master::fs::master_filesystem::memory_read_gates;
use std::time::Duration;

const GATE_LEAF: &str = "file-status-opt-leaf-";
const WAIT: Duration = Duration::from_secs(30);

fn parked_status<F>(fs: &MasterFilesystem, path: &str, mutate: F) -> FsResult<FileStatus>
where
    F: FnOnce(&MasterFilesystem),
{
    let _mode = memory_read_gates::hold_optimistic_mode(false);
    let (release, entered) = memory_read_gates::install_with_entered(&format!("{GATE_LEAF}{path}"));
    let reader_fs = fs.clone();
    let reader_path = path.to_string();
    let reader = std::thread::spawn(move || reader_fs.file_status(&reader_path));
    entered
        .recv_timeout(WAIT)
        .expect("the reader never reached the optimistic leaf gate");
    mutate(fs);
    release.send(()).ok();
    reader.join().expect("the reader panicked")
}

#[test]
fn delete_inside_the_window_is_not_found() {
    let fs = test_fs("fs-opt-delete");
    fs.mkdir("/op", true).unwrap();
    fs.create("/op/file", true).unwrap();
    let result = parked_status(&fs, "/op/file", |w| {
        w.delete("/op/file", false).unwrap();
    });
    assert!(
        result.is_err(),
        "a delete before the check must win: {result:?}"
    );
}

#[test]
fn rename_across_parents_inside_the_window_is_not_found_at_the_old_path() {
    let fs = test_fs("fs-opt-rename");
    fs.mkdir("/op/a", true).unwrap();
    fs.mkdir("/op/b", true).unwrap();
    let id = fs.create("/op/a/f", true).unwrap().id;
    let result = parked_status(&fs, "/op/a/f", |w| {
        w.rename("/op/a/f", "/op/b/f", RenameFlags::empty())
            .unwrap();
    });
    assert!(result.is_err(), "the old path is gone: {result:?}");
    assert_eq!(fs.file_status("/op/b/f").unwrap().id, id);
}

#[test]
fn same_name_replacement_inside_the_window_returns_the_new_file() {
    let fs = test_fs("fs-opt-replace");
    fs.mkdir("/op", true).unwrap();
    let old_id = fs.create("/op/f", true).unwrap().id;
    let status = parked_status(&fs, "/op/f", |w| {
        w.delete("/op/f", false).unwrap();
        w.create("/op/f", false).unwrap();
    })
    .unwrap();
    assert_ne!(
        status.id, old_id,
        "the replacement must not be reported as the old file"
    );
}

#[test]
fn ancestor_removal_inside_the_window_is_not_found() {
    let fs = test_fs("fs-opt-ancestor");
    fs.mkdir("/op/a", true).unwrap();
    fs.create("/op/a/f", true).unwrap();
    let result = parked_status(&fs, "/op/a/f", |w| {
        w.delete("/op/a", true).unwrap();
    });
    assert!(
        result.is_err(),
        "removing an ancestor must invalidate the walk: {result:?}"
    );
}

#[test]
fn stable_file_status_matches_the_locked_path() {
    let fs = test_fs("fs-opt-stable");
    fs.mkdir("/op/d", true).unwrap();
    let id = fs.create("/op/d/f", true).unwrap().id;
    let fast = fs.file_status("/op/d/f").unwrap();
    let locked = {
        let _mode = memory_read_gates::hold_optimistic_mode(true);
        fs.file_status("/op/d/f").unwrap()
    };
    assert_eq!(fast.id, id);
    assert_eq!(
        (
            fast.id,
            fast.len,
            fast.mtime,
            fast.is_dir,
            fast.name.clone()
        ),
        (
            locked.id,
            locked.len,
            locked.mtime,
            locked.is_dir,
            locked.name.clone()
        )
    );
    assert!(fs.file_status("/op/d/missing").is_err());
    assert!(fs.file_status("/missing/x").is_err());
}

#[test]
fn directory_targets_stay_on_the_locked_path_and_return_their_child_count() {
    let fs = test_fs("fs-opt-dir");
    fs.mkdir("/op/d", true).unwrap();
    fs.create("/op/d/f", true).unwrap();
    let status = fs.file_status("/op/d").unwrap();
    assert!(status.is_dir);
    assert_eq!(status.children_num, 1);
}
