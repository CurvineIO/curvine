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

//! Deterministic interleavings for the write-free `get_block_locations` walk
//! (S9.4). The reader parks after the row and its locations are built and
//! before the directory sequences are re-checked; mutations run on the test
//! thread in that window and a stale answer would mean validation missed them.

use super::*;
use crate::master::fs::master_filesystem::memory_read_gates;
use std::time::Duration;

const GATE_LEAF: &str = "gbl-opt-leaf-";
const WAIT: Duration = Duration::from_secs(30);

fn parked_locations<F>(fs: &MasterFilesystem, path: &str, mutate: F) -> FsResult<FileBlocks>
where
    F: FnOnce(&MasterFilesystem),
{
    let _mode = memory_read_gates::hold_optimistic_mode(false);
    let (release, entered) = memory_read_gates::install_with_entered(&format!("{GATE_LEAF}{path}"));
    let reader_fs = fs.clone();
    let reader_path = path.to_string();
    let reader = std::thread::spawn(move || reader_fs.get_block_locations(&reader_path));
    entered
        .recv_timeout(WAIT)
        .expect("the reader never reached the optimistic leaf gate");
    mutate(fs);
    release.send(()).ok();
    reader.join().expect("the reader panicked")
}

#[test]
fn delete_inside_the_window_is_not_found() {
    let fs = test_fs("gbl-opt-delete");
    fs.mkdir("/op", true).unwrap();
    fs.create("/op/file", true).unwrap();
    let result = parked_locations(&fs, "/op/file", |w| {
        w.delete("/op/file", false).unwrap();
    });
    assert!(
        result.is_err(),
        "a delete before the check must win: {result:?}"
    );
}

#[test]
fn rename_across_parents_inside_the_window_is_not_found_at_the_old_path() {
    let fs = test_fs("gbl-opt-rename");
    fs.mkdir("/op/a", true).unwrap();
    fs.mkdir("/op/b", true).unwrap();
    let id = fs.create("/op/a/f", true).unwrap().id;
    let result = parked_locations(&fs, "/op/a/f", |w| {
        w.rename("/op/a/f", "/op/b/f", RenameFlags::empty())
            .unwrap();
    });
    assert!(result.is_err(), "the old path is gone: {result:?}");
    assert_eq!(fs.get_block_locations("/op/b/f").unwrap().status.id, id);
}

#[test]
fn same_name_replacement_inside_the_window_returns_the_new_file() {
    let fs = test_fs("gbl-opt-replace");
    fs.mkdir("/op", true).unwrap();
    let old_id = fs.create("/op/f", true).unwrap().id;
    let blocks = parked_locations(&fs, "/op/f", |w| {
        w.delete("/op/f", false).unwrap();
        w.create("/op/f", false).unwrap();
    })
    .unwrap();
    assert_ne!(
        blocks.status.id, old_id,
        "the replacement must not be reported as the old file"
    );
}

#[test]
fn ancestor_removal_inside_the_window_is_not_found() {
    let fs = test_fs("gbl-opt-ancestor");
    fs.mkdir("/op/a", true).unwrap();
    fs.create("/op/a/f", true).unwrap();
    let result = parked_locations(&fs, "/op/a/f", |w| {
        w.delete("/op/a", true).unwrap();
    });
    assert!(
        result.is_err(),
        "removing an ancestor must invalidate the walk: {result:?}"
    );
}

#[test]
fn stable_answer_matches_the_locked_path() {
    let fs = test_fs("gbl-opt-stable");
    fs.mkdir("/op/d", true).unwrap();
    let id = fs.create("/op/d/f", true).unwrap().id;
    let fast = fs.get_block_locations("/op/d/f").unwrap();
    let locked = {
        let _mode = memory_read_gates::hold_optimistic_mode(true);
        fs.get_block_locations("/op/d/f").unwrap()
    };
    assert_eq!(fast.status.id, id);
    assert_eq!(fast.status.id, locked.status.id);
    assert_eq!(fast.block_locs.len(), locked.block_locs.len());
    assert!(fs.get_block_locations("/op/d/missing").is_err());
}

/// A file-row writer holds the target inode's write lock but does not touch any
/// directory sequence, so the walk's validation cannot see it. The read must
/// still wait for the writer rather than answer around it.
#[test]
fn a_held_inode_write_lock_blocks_the_read_until_it_is_released() {
    let fs = test_fs("gbl-opt-inode-writer");
    fs.mkdir("/op", true).unwrap();
    let id = fs.create("/op/file", true).unwrap().id;
    let held = fs.inode_locks.write_inode(id);
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(worker_fs.get_block_locations("/op/file")).unwrap();
    });
    let early = rx.recv_timeout(Duration::from_millis(200));
    drop(held);
    worker.join().unwrap();
    assert!(
        early.is_err(),
        "the read answered while a writer held the inode"
    );
    assert_eq!(rx.recv().unwrap().unwrap().status.id, id);
}

#[test]
fn a_held_inode_write_lock_blocks_file_status_until_it_is_released() {
    let fs = test_fs("fs-opt-inode-writer");
    fs.mkdir("/op", true).unwrap();
    let id = fs.create("/op/file", true).unwrap().id;
    let held = fs.inode_locks.write_inode(id);
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(worker_fs.file_status("/op/file")).unwrap();
    });
    let early = rx.recv_timeout(Duration::from_millis(200));
    drop(held);
    worker.join().unwrap();
    assert!(
        early.is_err(),
        "file_status answered while a writer held the inode"
    );
    assert_eq!(rx.recv().unwrap().unwrap().id, id);
}
