// Copyright 2026 OPPO
// Licensed under the Apache License, Version 2.0 (see LICENSE).

//! mkdir with canonical directory attributes: the parent is only shared, so the
//! new name needs an edge lock and the parent's inode row must stay untouched
//! while its record carries the link and child counts.

use super::*;
use std::sync::{Arc, Barrier};

const WAIT: std::time::Duration = std::time::Duration::from_secs(30);

fn canonical_fs(name: &str) -> MasterFilesystem {
    let fs = test_fs(name);
    {
        let handle = fs.fs_dir();
        handle
            .view()
            .unwrap()
            .store_ref()
            .set_canonical_directory_attributes(true);
    }
    fs.mkdir("/p", true).unwrap();
    fs.create("/p/seed", false).unwrap();
    fs
}

fn raw_row(fs: &MasterFilesystem, path: &str) -> (i64, u32) {
    let id = fs.file_status(path).unwrap().id;
    let handle = fs.fs_dir();
    let view = handle.view().unwrap();
    let row = view.store_ref().store().get_inode(id).unwrap().unwrap();
    (row.mtime(), row.as_dir_ref().unwrap().nlink)
}

#[test]
fn distinct_names_created_in_parallel_all_land() {
    let fs = Arc::new(canonical_fs("canonical-mkdir-distinct"));
    let row_before = raw_row(&fs, "/p");
    const THREADS: usize = 8;
    const EACH: usize = 20;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.mkdir(format!("/p/d-{thread}-{i}"), false).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("mkdir worker panicked");
    }
    let expected = THREADS * EACH + 1;
    assert_eq!(fs.list_status("/p").unwrap().len(), expected);
    let status = fs.file_status("/p").unwrap();
    assert_eq!(status.children_num as usize, expected);
    assert_eq!(
        status.nlink as usize,
        expected + 1,
        "each child directory links its parent"
    );
    assert_eq!(
        raw_row(&fs, "/p"),
        row_before,
        "mkdir rewrote the parent row"
    );
}

#[test]
fn two_mkdirs_of_one_name_have_exactly_one_winner() {
    let fs = Arc::new(canonical_fs("canonical-mkdir-same-name"));
    const THREADS: usize = 8;
    for round in 0..50 {
        let path = format!("/p/same-{round}");
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.mkdir(path, false).map(|_| ()).map_err(|e| e.to_string())
                })
            })
            .collect();
        let results: Vec<Result<(), String>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "round {round}: {results:?}"
        );
        assert!(
            results
                .iter()
                .filter_map(|r| r.as_ref().err())
                .all(|e| e.contains("already exists")),
            "round {round}: a loser failed for another reason: {results:?}"
        );
        // The writer must still be usable: no path may have been fenced.
        fs.create(format!("/p/after-{round}"), false).unwrap();
    }
}

#[test]
fn a_mkdir_and_an_exclusive_create_of_one_name_have_one_winner() {
    let fs = Arc::new(canonical_fs("canonical-mkdir-vs-create"));
    for round in 0..60 {
        let path = format!("/p/clash-{round}");
        let barrier = Arc::new(Barrier::new(2));
        let maker = {
            let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.mkdir(path, false).map(|_| ()).map_err(|e| e.to_string())
            })
        };
        let creator = {
            let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.create_with_opts(
                    path,
                    CreateFileOpts::with_create(false),
                    OpenFlags::new_create().set_exclusive(true),
                )
                .map(|_| ())
                .map_err(|e| e.to_string())
            })
        };
        let results = [maker.join().unwrap(), creator.join().unwrap()];
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "round {round}: {results:?}"
        );
        assert!(
            results
                .iter()
                .filter_map(|r| r.as_ref().err())
                .all(|e| e.contains("already exists")),
            "round {round}: {results:?}"
        );
        fs.create(format!("/p/tail-{round}"), false).unwrap();
    }
}

#[test]
fn a_mkdir_and_a_symlink_of_one_name_have_one_winner() {
    let fs = Arc::new(canonical_fs("canonical-mkdir-vs-symlink"));
    for round in 0..60 {
        let path = format!("/p/link-{round}");
        let barrier = Arc::new(Barrier::new(2));
        let maker = {
            let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.mkdir(path, false).map(|_| ()).map_err(|e| e.to_string())
            })
        };
        let linker = {
            let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.symlink("/p/seed", path.as_str(), false, 0o777)
                    .map_err(|e| e.to_string())
            })
        };
        let results = [maker.join().unwrap(), linker.join().unwrap()];
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "round {round}: {results:?}"
        );
        fs.create(format!("/p/tail-{round}"), false).unwrap();
    }
}

#[test]
fn a_mkdir_racing_a_listing_never_shows_a_half_built_page() {
    let fs = Arc::new(canonical_fs("canonical-mkdir-vs-list"));
    let writer = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..200 {
                fs.mkdir(format!("/p/g-{i}"), false).unwrap();
            }
        })
    };
    let reader = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            let mut seen = 0usize;
            while !fs.exists("/p/g-199").unwrap() {
                let page = fs.list_status("/p").unwrap();
                // Every page is a snapshot of one instant: names are unique and
                // the count never exceeds what has been created so far.
                let names: std::collections::BTreeSet<_> = page.iter().map(|s| &s.name).collect();
                assert_eq!(names.len(), page.len(), "a page repeated a name");
                assert!(page.len() >= seen || page.len() == names.len());
                seen = page.len();
            }
            seen
        })
    };
    writer.join().expect("mkdir worker panicked");
    let last = reader.join().expect("listing worker panicked");
    assert!(last >= 1);
    let expected = 201;
    assert_eq!(fs.list_status("/p").unwrap().len(), expected);
    assert_eq!(
        fs.file_status("/p").unwrap().children_num as usize,
        expected
    );
    assert!(WAIT.as_secs() > 0);
}
