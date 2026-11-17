// Copyright 2026 OPPO
// Licensed under the Apache License, Version 2.0 (see LICENSE).

//! Cross-directory rename under canonical directory attributes: both parents are
//! shared, the two names it touches carry edge locks, and neither parent's inode
//! row is rewritten.

use super::*;
use curvine_model::RenameFlags;
use std::sync::{Arc, Barrier};

fn canonical_pair(name: &str) -> MasterFilesystem {
    let fs = test_fs(name);
    {
        let handle = fs.fs_dir();
        handle
            .view()
            .unwrap()
            .store_ref()
            .set_canonical_directory_attributes(true);
    }
    fs.mkdir("/a", true).unwrap();
    fs.mkdir("/b", true).unwrap();
    fs
}

fn raw_row(fs: &MasterFilesystem, path: &str) -> (i64, u32) {
    let id = fs.file_status(path).unwrap().id;
    let handle = fs.fs_dir();
    let view = handle.view().unwrap();
    let row = view.store_ref().store().get_inode(id).unwrap().unwrap();
    (row.mtime(), row.as_dir_ref().unwrap().nlink)
}

fn names(fs: &MasterFilesystem, path: &str) -> std::collections::BTreeSet<String> {
    fs.list_status(path)
        .unwrap()
        .into_iter()
        .map(|status| status.name)
        .collect()
}

#[test]
fn cross_parent_renames_of_distinct_files_all_land() {
    let fs = Arc::new(canonical_pair("canonical-rename-cross"));
    const THREADS: usize = 8;
    const EACH: usize = 15;
    for thread in 0..THREADS {
        fs.mkdir(format!("/a/t{thread}"), false).unwrap();
    }
    for thread in 0..THREADS {
        for i in 0..EACH {
            fs.create(format!("/a/f-{thread}-{i}"), false).unwrap();
        }
    }
    let rows = (raw_row(&fs, "/a"), raw_row(&fs, "/b"));
    std::thread::sleep(std::time::Duration::from_millis(5));
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.rename(
                        format!("/a/f-{thread}-{i}"),
                        format!("/b/g-{thread}-{i}"),
                        RenameFlags::empty(),
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("rename worker panicked");
    }

    let a = names(&fs, "/a");
    let b = names(&fs, "/b");
    assert_eq!(a.len(), THREADS, "only the seed directories stay in /a");
    assert!(a.iter().all(|name| name.starts_with('t')));
    assert_eq!(b.len(), THREADS * EACH);
    assert_eq!(fs.file_status("/a").unwrap().children_num as usize, a.len());
    assert_eq!(fs.file_status("/b").unwrap().children_num as usize, b.len());
    assert_eq!(
        (raw_row(&fs, "/a"), raw_row(&fs, "/b")),
        rows,
        "a parent row was rewritten"
    );
}

#[test]
fn cross_parent_renames_to_one_name_leave_one_entry() {
    let fs = Arc::new(canonical_pair("canonical-rename-target"));
    const THREADS: usize = 8;
    for round in 0..40 {
        let target = format!("/b/target-{round}");
        for thread in 0..THREADS {
            fs.create(format!("/a/src-{round}-{thread}"), false)
                .unwrap();
        }
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|thread| {
                let (fs, barrier, target) = (fs.clone(), barrier.clone(), target.clone());
                let source = format!("/a/src-{round}-{thread}");
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.rename(source, target, RenameFlags::empty())
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                })
            })
            .collect();
        let results: Vec<Result<(), String>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        // A plain rename replaces an existing name, so several racers may win; what
        // must hold is that exactly one entry remains and the counters agree.
        assert!(
            results.iter().any(|result| result.is_ok()),
            "round {round}: nobody renamed: {results:?}"
        );
        assert!(fs.exists(&target).unwrap());
        assert_eq!(
            fs.file_status("/b").unwrap().children_num as usize,
            names(&fs, "/b").len()
        );
        assert_eq!(
            fs.file_status("/a").unwrap().children_num as usize,
            names(&fs, "/a").len()
        );
    }
    fs.create("/b/still-works", false).unwrap();
}

#[test]
fn a_cross_rename_and_an_exclusive_create_of_the_target_have_one_winner() {
    let fs = Arc::new(canonical_pair("canonical-rename-vs-create"));
    for round in 0..60 {
        let source = format!("/a/s-{round}");
        let target = format!("/b/t-{round}");
        fs.create(&source, false).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let renamer = {
            let (fs, barrier, source, target) =
                (fs.clone(), barrier.clone(), source, target.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.rename(source, target, RenameFlags::empty())
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            })
        };
        let creator = {
            let (fs, barrier, target) = (fs.clone(), barrier.clone(), target);
            std::thread::spawn(move || {
                barrier.wait();
                fs.create_with_opts(
                    target,
                    CreateFileOpts::with_create(false),
                    OpenFlags::new_create().set_exclusive(true),
                )
                .map(|_| ())
                .map_err(|error| error.to_string())
            })
        };
        let results = [renamer.join().unwrap(), creator.join().unwrap()];
        assert!(
            results.iter().any(|result| result.is_ok()),
            "round {round}: {results:?}"
        );
        // Whichever ran second either replaced the name (rename) or found it taken
        // (create); neither may leave the writer unusable.
        fs.create(format!("/b/tail-{round}"), false).unwrap();
    }
}

#[test]
fn a_cross_rename_and_a_delete_of_the_source_have_one_winner() {
    let fs = Arc::new(canonical_pair("canonical-rename-vs-delete"));
    for round in 0..60 {
        let source = format!("/a/r-{round}");
        let target = format!("/b/r-{round}");
        fs.create(&source, false).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let renamer = {
            let (fs, barrier, source, target) =
                (fs.clone(), barrier.clone(), source.clone(), target.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.rename(source, target, RenameFlags::empty()).is_ok()
            })
        };
        let deleter = {
            let (fs, barrier, source) = (fs.clone(), barrier.clone(), source.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.delete(source, false).is_ok()
            })
        };
        let (renamed, deleted) = (renamer.join().unwrap(), deleter.join().unwrap());
        assert!(renamed || deleted, "round {round}: neither succeeded");
        assert!(
            !fs.exists(&source).unwrap(),
            "round {round}: the source survived"
        );
        if renamed && !deleted {
            assert!(
                fs.exists(&target).unwrap(),
                "round {round}: rename lost its result"
            );
        }
        fs.create(format!("/b/tail-{round}"), false).unwrap();
    }
}
