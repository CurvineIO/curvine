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

//! `expect_gen` conditional guard on the UFS mark.
//!
//! An asynchronous export reports completion by calling `set_attr(ufs_mtime)`.
//! Without a guard that report lands on whatever inode the source path resolves
//! to now, which may be a newer version or an unrelated file. These tests pin
//! the rule: the mark applies only when the inode still carries the version the
//! export claims to have read.

use super::*;

/// A completed file with one block and two replicas, so `gen()` has a non-zero
/// `last_block_id` and `is_complete()` holds.
fn completed_file(fs: &MasterFilesystem, path: &str, len: i64) -> FileGen {
    for worker_id in [100, 200] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);
    }

    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();

    let block = fs
        .add_block(
            path,
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    let block_id = block.block.id;
    fs.fs_dir
        .write()
        .add_block_location(block_id, BlockLocation::with_id(100))
        .unwrap();

    let commit = CommitBlock {
        block_id,
        block_len: len,
        locations: vec![BlockLocation::with_id(100)],
    };
    fs.complete_file(path, None, len, vec![commit], "", false, None)
        .unwrap();

    gen_of(fs, path)
}

fn gen_of(fs: &MasterFilesystem, path: &str) -> FileGen {
    let fs_dir = fs.fs_dir.read();
    let inp = InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store).unwrap();
    inp.get_last_inode()
        .expect("inode exists")
        .as_ref()
        .gen()
        .expect("gen is defined for files")
}

fn state_of(fs: &MasterFilesystem, path: &str) -> StorageState {
    fs.file_status(path).unwrap().storage_policy.state
}

fn ufs_mtime_of(fs: &MasterFilesystem, path: &str) -> i64 {
    fs.file_status(path).unwrap().storage_policy.ufs_mtime
}

fn mark(fs: &MasterFilesystem, path: &str, gen: FileGen, mtime: i64) {
    let opts = SetAttrOptsBuilder::new()
        .ufs_mtime(mtime)
        .expect_gen(gen)
        .build();
    fs.set_attr(path, opts).unwrap();
}

#[test]
fn matching_gen_marks_the_file_as_both() {
    let fs = test_fs("expect-gen-match");
    let gen = completed_file(&fs, "/match.log", 4096);

    mark(&fs, "/match.log", gen, 777);

    assert_eq!(state_of(&fs, "/match.log"), StorageState::Both);
    assert_eq!(ufs_mtime_of(&fs, "/match.log"), 777);
}

#[test]
fn a_newer_version_is_not_marked() {
    let fs = test_fs("expect-gen-newer");
    let stale = completed_file(&fs, "/newer.log", 4096);

    // truncate changes `len`, so the recorded generation is superseded.
    fs.resize("/newer.log", FileAllocOpts::with_truncate(2048))
        .unwrap();
    assert_ne!(gen_of(&fs, "/newer.log"), stale);

    mark(&fs, "/newer.log", stale, 777);

    assert_eq!(state_of(&fs, "/newer.log"), StorageState::Cv);
    assert_eq!(ufs_mtime_of(&fs, "/newer.log"), 0);
}

#[test]
fn a_recycled_path_is_not_marked() {
    let fs = test_fs("expect-gen-recycled");
    let stale = completed_file(&fs, "/recycled.log", 4096);

    fs.delete("/recycled.log", false).unwrap();
    let fresh = completed_file(&fs, "/recycled.log", 4096);

    // Same path, same length, possibly the same millisecond: only the inode id
    // distinguishes them, which is why `FileGen` carries it.
    assert_ne!(fresh.inode_id, stale.inode_id);
    mark(&fs, "/recycled.log", stale, 777);

    assert_eq!(state_of(&fs, "/recycled.log"), StorageState::Cv);
    assert_eq!(ufs_mtime_of(&fs, "/recycled.log"), 0);
}

#[test]
fn a_freed_file_is_not_flipped_back_to_both() {
    let fs = test_fs("expect-gen-freed");
    let gen = completed_file(&fs, "/freed.log", 4096);

    // Mark unconditionally first so `free()` accepts the file, then release the
    // Curvine copy: state goes to `Ufs` with no local data behind it.
    fs.set_attr(
        "/freed.log",
        SetAttrOptsBuilder::new()
            .ufs_mtime(gen.mtime + 1_000)
            .ttl_action(TtlAction::Free)
            .build(),
    )
    .unwrap();
    fs.free("/freed.log", false).unwrap();
    assert_eq!(state_of(&fs, "/freed.log"), StorageState::Ufs);

    mark(&fs, "/freed.log", gen, 777);

    assert_eq!(
        state_of(&fs, "/freed.log"),
        StorageState::Ufs,
        "save_ufs would resurrect a released file as `Both`"
    );
    assert_eq!(ufs_mtime_of(&fs, "/freed.log"), gen.mtime + 1_000);
}

#[test]
fn a_file_still_being_written_is_not_marked() {
    let fs = test_fs("expect-gen-writing");
    for worker_id in [100, 200] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);
    }
    fs.create_with_opts(
        "/writing.log",
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();

    let gen = gen_of(&fs, "/writing.log");
    mark(&fs, "/writing.log", gen, 777);

    assert_eq!(state_of(&fs, "/writing.log"), StorageState::Cv);
    assert_eq!(ufs_mtime_of(&fs, "/writing.log"), 0);
}

#[test]
fn a_directory_is_never_marked() {
    let fs = test_fs("expect-gen-dir");
    fs.mkdir("/adir", true).unwrap();

    let opts = SetAttrOptsBuilder::new()
        .ufs_mtime(777)
        .expect_gen(FileGen::new(1, 0, 0, 0))
        .build();
    fs.set_attr("/adir", opts).unwrap();

    assert_eq!(state_of(&fs, "/adir"), StorageState::Cv);
    assert_eq!(ufs_mtime_of(&fs, "/adir"), 0);
}

#[test]
fn without_expect_gen_the_mark_is_unconditional() {
    // Reports without a generation (sync path, user exports) keep the original
    // behaviour: the object's mtime is recorded whatever its age, so a UFS clock
    // behind the master never silently drops a mark.
    let fs = test_fs("expect-gen-unconditional");
    let gen = completed_file(&fs, "/plain.log", 4096);

    fs.set_attr(
        "/plain.log",
        SetAttrOptsBuilder::new()
            .ufs_mtime(gen.mtime - 60_000)
            .build(),
    )
    .unwrap();

    assert_eq!(state_of(&fs, "/plain.log"), StorageState::Both);
    assert_eq!(ufs_mtime_of(&fs, "/plain.log"), gen.mtime - 60_000);
}
