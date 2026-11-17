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

//! Same-parent creates run in parallel: the parent is read-locked and the
//! per-name shard lock inside `DirectoryChildren` serialises only equal names.
//!
//! What the parent write lock used to guarantee, and what these cases pin:
//!  * two creates of the SAME name: exactly one wins;
//!  * creates of different names: every one lands, none is lost or doubled;
//!  * the parent's persisted mtime never goes backwards, even though creates
//!    now race to update it;
//!  * a create racing a delete/list of the same parent stays exclusive or
//!    consistent, because those still take the parent write lock / validate by
//!    version.

use super::*;
use std::sync::{Arc, Barrier};

/// Directories below the shard threshold keep one table, so a test about
/// parallelism has to start above it.
fn big_parent(fs: &MasterFilesystem, dir: &str) {
    fs.mkdir(dir, true).unwrap();
    // `min_sharded_children()` is the promotion threshold; stay well above it.
    for i in 0..400 {
        fs.create(format!("{dir}/seed-{i}"), false).unwrap();
    }
}

#[test]
fn concurrent_creates_of_distinct_names_all_land() {
    let fs = Arc::new(test_fs("sp-create-distinct"));
    big_parent(&fs, "/sp1");
    const THREADS: usize = 8;
    const EACH: usize = 200;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.create(format!("/sp1/t{t}-{i}"), false).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let names: std::collections::BTreeSet<String> = fs
        .list_status("/sp1")
        .unwrap()
        .into_iter()
        .map(|status| status.name)
        .collect();
    assert_eq!(
        names.len(),
        400 + THREADS * EACH,
        "a create was lost or doubled"
    );
    assert_eq!(
        fs.file_status("/sp1").unwrap().children_num as usize,
        400 + THREADS * EACH,
        "the parent's child count disagrees with the listing"
    );
}

#[test]
fn concurrent_creates_of_one_name_have_exactly_one_winner() {
    let fs = Arc::new(test_fs("sp-create-same-name"));
    big_parent(&fs, "/sp2");
    const THREADS: usize = 8;
    for round in 0..50 {
        let path = format!("/sp2/contended-{round}");
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.create_with_opts(
                        &path,
                        CreateFileOpts::with_create(false),
                        OpenFlags::new_create().set_exclusive(true),
                    )
                    .map(|_| ())
                    .map_err(|e| e.to_string())
                })
            })
            .collect();
        let results: Vec<Result<(), String>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wins = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            wins, 1,
            "round {round}: {wins} creators of {path} succeeded: {results:?}"
        );
    }
}

#[test]
fn the_parent_mtime_never_goes_backwards_under_concurrent_creates() {
    let fs = Arc::new(test_fs("sp-create-mtime"));
    big_parent(&fs, "/sp3");
    let before = fs.file_status("/sp3").unwrap().mtime;
    const THREADS: usize = 8;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                let mut last = 0;
                for i in 0..300 {
                    fs.create(format!("/sp3/t{t}-{i}"), false).unwrap();
                    let seen = fs.file_status("/sp3").unwrap().mtime;
                    assert!(seen >= last, "mtime went from {last} to {seen}");
                    last = seen;
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert!(fs.file_status("/sp3").unwrap().mtime >= before);
}

#[test]
fn a_create_and_a_delete_of_the_same_parent_do_not_corrupt_the_listing() {
    let fs = Arc::new(test_fs("sp-create-delete"));
    big_parent(&fs, "/sp4");
    let creator = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..400 {
                fs.create(format!("/sp4/new-{i}"), false).unwrap();
            }
        })
    };
    let deleter = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..400 {
                fs.delete(format!("/sp4/seed-{i}"), false).unwrap();
            }
        })
    };
    creator.join().unwrap();
    deleter.join().unwrap();
    let list = fs.list_status("/sp4").unwrap();
    assert_eq!(list.len(), 400, "400 seeds deleted, 400 created");
    assert_eq!(fs.file_status("/sp4").unwrap().children_num, 400);
}

/// A file child under a non-root parent no longer rewrites the parent's inode row;
/// its mtime travels in the dir-attrs record only. A restore rebuilds the tree
/// from the store, so the newer mtime and the child count must come back from
/// the record, not from a stale row.
#[test]
fn a_parent_mtime_set_by_creates_survives_a_restore_from_the_store() {
    let fs = test_fs("sp-create-restore");
    fs.mkdir("/sp5", true).unwrap();
    let before = fs.file_status("/sp5").unwrap().mtime;
    // Wait out the clock tick so the creates carry a strictly newer mtime.
    std::thread::sleep(std::time::Duration::from_millis(5));
    for i in 0..20 {
        fs.create(format!("/sp5/f{i}"), false).unwrap();
    }
    let live = fs.file_status("/sp5").unwrap();
    assert!(
        live.mtime > before,
        "the creates must have advanced the parent mtime"
    );
    assert_eq!(live.children_num, 20);

    fs.restore_from_rocksdb().unwrap();

    let restored = fs.file_status("/sp5").unwrap();
    assert_eq!(
        restored.mtime, live.mtime,
        "the parent mtime was lost by the restore"
    );
    assert_eq!(restored.children_num, 20);
    assert_eq!(fs.list_status("/sp5").unwrap().len(), 20);
}

/// With the parent only read-locked, concurrent creates must not read-modify-write
/// the parent's inode row: a late writer holding an older mtime could carry it
/// back over a newer one. The row is read straight from the store, bypassing the
/// dir-attrs overlay that hides this from `file_status`.
#[test]
fn concurrent_creates_leave_the_parent_inode_row_untouched() {
    let fs = Arc::new(test_fs("sp-create-row"));
    big_parent(&fs, "/sp6");
    let id = fs.file_status("/sp6").unwrap().id;
    let row = |fs: &MasterFilesystem| {
        fs.fs_dir
            .view()
            .unwrap()
            .store_ref()
            .store()
            .get_inode(id)
            .unwrap()
            .unwrap()
            .mtime()
    };
    let before = row(&fs);
    std::thread::sleep(std::time::Duration::from_millis(5));
    const THREADS: usize = 8;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    fs.create(format!("/sp6/t{t}-{i}"), false).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(
        row(&fs),
        before,
        "a file create rewrote the parent row it only holds a read lock on"
    );
    // The live value still advanced: it comes from the dir-attrs record.
    assert!(fs.file_status("/sp6").unwrap().mtime > before);
}
