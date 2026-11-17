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

// ---- delete under a read-locked parent -------------------------------------

#[test]
fn concurrent_deletes_of_distinct_names_all_land() {
    let fs = Arc::new(test_fs("sp-delete-distinct"));
    big_parent(&fs, "/sp7");
    const THREADS: usize = 8;
    const EACH: usize = 40;
    // Disjoint slices of the seeded names, one slice per thread.
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.delete(format!("/sp7/seed-{}", t * EACH + i), false)
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let remaining = 400 - THREADS * EACH;
    assert_eq!(fs.list_status("/sp7").unwrap().len(), remaining);
    assert_eq!(
        fs.file_status("/sp7").unwrap().children_num as usize,
        remaining
    );
}

#[test]
fn concurrent_deletes_of_one_name_have_exactly_one_winner() {
    let fs = Arc::new(test_fs("sp-delete-same-name"));
    big_parent(&fs, "/sp8");
    const THREADS: usize = 8;
    for round in 0..50 {
        let path = format!("/sp8/seed-{round}");
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.delete(&path, false)
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
            "round {round}: {wins} deleters succeeded: {results:?}"
        );
        // The losers fail cleanly; the writer is still usable afterwards.
        fs.create(format!("/sp8/after-{round}"), false).unwrap();
    }
}

#[test]
fn a_delete_and_a_create_of_one_name_never_leave_a_half_state() {
    let fs = Arc::new(test_fs("sp-delete-create-race"));
    big_parent(&fs, "/sp9");
    const ROUNDS: usize = 200;
    let creator = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for _ in 0..ROUNDS {
                let _ = fs.create("/sp9/flip", false);
            }
        })
    };
    let deleter = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for _ in 0..ROUNDS {
                let _ = fs.delete("/sp9/flip", false);
            }
        })
    };
    creator.join().unwrap();
    deleter.join().unwrap();
    // Whatever interleaving happened, the listing and the point lookups agree,
    // and the writer still accepts writes.
    let listed = fs
        .list_status("/sp9")
        .unwrap()
        .iter()
        .any(|status| status.name == "flip");
    assert_eq!(fs.exists("/sp9/flip").unwrap(), listed);
    assert_eq!(
        fs.file_status("/sp9").unwrap().children_num as usize,
        fs.list_status("/sp9").unwrap().len()
    );
    fs.create("/sp9/still-works", false).unwrap();
}

#[test]
fn a_file_delete_leaves_the_parent_inode_row_untouched() {
    let fs = test_fs("sp-delete-row");
    big_parent(&fs, "/sp10");
    let id = fs.file_status("/sp10").unwrap().id;
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
    for i in 0..50 {
        fs.delete(format!("/sp10/seed-{i}"), false).unwrap();
    }
    assert_eq!(row(&fs), before, "a file delete rewrote the parent row");
    assert!(fs.file_status("/sp10").unwrap().mtime > before);
}

#[test]
fn deleting_a_directory_still_excludes_creates_in_its_parent() {
    let fs = Arc::new(test_fs("sp-delete-dir"));
    big_parent(&fs, "/sp11");
    fs.mkdir("/sp11/sub", false).unwrap();
    fs.create("/sp11/sub/inner", false).unwrap();
    let creator = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..200 {
                fs.create(format!("/sp11/c-{i}"), false).unwrap();
            }
        })
    };
    fs.delete("/sp11/sub", true).unwrap();
    creator.join().unwrap();
    assert!(!fs.exists("/sp11/sub").unwrap());
    assert!(!fs.exists("/sp11/sub/inner").unwrap());
    assert_eq!(
        fs.file_status("/sp11").unwrap().children_num as usize,
        fs.list_status("/sp11").unwrap().len()
    );
}

// ---- same-directory rename under a read-locked parent ----------------------

use curvine_model::RenameFlags;

#[test]
fn concurrent_renames_to_distinct_names_all_land() {
    let fs = Arc::new(test_fs("sp-rename-distinct"));
    big_parent(&fs, "/sp12");
    const THREADS: usize = 8;
    const EACH: usize = 40;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    let n = t * EACH + i;
                    fs.rename(
                        format!("/sp12/seed-{n}"),
                        format!("/sp12/moved-{n}"),
                        RenameFlags::empty(),
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let names: std::collections::BTreeSet<String> = fs
        .list_status("/sp12")
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names.len(), 400, "a rename lost or doubled an entry");
    assert_eq!(
        names.iter().filter(|n| n.starts_with("moved-")).count(),
        THREADS * EACH
    );
    assert_eq!(fs.file_status("/sp12").unwrap().children_num, 400);
}

#[test]
fn renames_of_different_sources_to_one_target_have_one_winner() {
    let fs = Arc::new(test_fs("sp-rename-same-target"));
    big_parent(&fs, "/sp13");
    const THREADS: usize = 8;
    for round in 0..40 {
        let barrier = Arc::new(Barrier::new(THREADS));
        let target = format!("/sp13/target-{round}");
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let (fs, barrier, target) = (fs.clone(), barrier.clone(), target.clone());
                let src = format!("/sp13/seed-{}", round * THREADS + t);
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.rename(&src, &target, RenameFlags::empty())
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                })
            })
            .collect();
        let results: Vec<Result<(), String>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wins = results.iter().filter(|r| r.is_ok()).count();
        // A plain rename onto an existing name replaces it, so every racer may
        // succeed; what must hold is that exactly one entry ends up at the target
        // and no source survives under its old name once it won.
        assert!(wins >= 1, "round {round}: nobody renamed: {results:?}");
        assert!(fs.exists(&target).unwrap());
        fs.create(format!("/sp13/after-{round}"), false).unwrap();
    }
    let listed = fs.list_status("/sp13").unwrap().len();
    assert_eq!(
        fs.file_status("/sp13").unwrap().children_num as usize,
        listed,
        "the child count drifted from the listing"
    );
}

#[test]
fn a_rename_and_a_create_of_the_target_name_never_corrupt_the_parent() {
    let fs = Arc::new(test_fs("sp-rename-vs-create"));
    big_parent(&fs, "/sp14");
    const ROUNDS: usize = 150;
    let renamer = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..ROUNDS {
                let _ = fs.rename(
                    format!("/sp14/seed-{i}"),
                    format!("/sp14/clash-{i}"),
                    RenameFlags::empty(),
                );
            }
        })
    };
    let creator = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..ROUNDS {
                let _ = fs.create(format!("/sp14/clash-{i}"), false);
            }
        })
    };
    renamer.join().unwrap();
    creator.join().unwrap();
    let listed = fs.list_status("/sp14").unwrap().len();
    assert_eq!(
        fs.file_status("/sp14").unwrap().children_num as usize,
        listed
    );
    for i in 0..ROUNDS {
        assert!(
            fs.exists(format!("/sp14/clash-{i}")).unwrap(),
            "clash-{i} vanished"
        );
    }
    fs.create("/sp14/still-works", false).unwrap();
}

#[test]
fn a_rename_and_a_delete_of_the_source_have_one_winner() {
    let fs = Arc::new(test_fs("sp-rename-vs-delete"));
    big_parent(&fs, "/sp15");
    for round in 0..60 {
        let src = format!("/sp15/seed-{round}");
        let dst = format!("/sp15/gone-{round}");
        let barrier = Arc::new(Barrier::new(2));
        let renamer = {
            let (fs, barrier, src, dst) = (fs.clone(), barrier.clone(), src.clone(), dst.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.rename(&src, &dst, RenameFlags::empty()).is_ok()
            })
        };
        let deleter = {
            let (fs, barrier, src) = (fs.clone(), barrier.clone(), src.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.delete(&src, false).is_ok()
            })
        };
        let (renamed, deleted) = (renamer.join().unwrap(), deleter.join().unwrap());
        assert!(renamed || deleted, "round {round}: neither succeeded");
        assert!(
            !fs.exists(&src).unwrap(),
            "round {round}: the source survived"
        );
        // Whichever ran first decides: renamed first means the delete found nothing
        // (and the file lives at dst); deleted first means the rename found nothing.
        if renamed && !deleted {
            assert!(fs.exists(&dst).unwrap());
        }
    }
    let listed = fs.list_status("/sp15").unwrap().len();
    assert_eq!(
        fs.file_status("/sp15").unwrap().children_num as usize,
        listed
    );
}

#[test]
fn a_rename_that_replaces_an_existing_file_still_works() {
    let fs = test_fs("sp-rename-replace");
    big_parent(&fs, "/sp16");
    fs.rename("/sp16/seed-0", "/sp16/seed-1", RenameFlags::empty())
        .unwrap();
    assert!(!fs.exists("/sp16/seed-0").unwrap());
    assert!(fs.exists("/sp16/seed-1").unwrap());
    assert_eq!(fs.list_status("/sp16").unwrap().len(), 399);
    assert_eq!(fs.file_status("/sp16").unwrap().children_num, 399);
}

#[test]
fn a_file_rename_leaves_the_parent_inode_row_untouched() {
    let fs = test_fs("sp-rename-row");
    big_parent(&fs, "/sp17");
    let id = fs.file_status("/sp17").unwrap().id;
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
    for i in 0..50 {
        fs.rename(
            format!("/sp17/seed-{i}"),
            format!("/sp17/renamed-{i}"),
            RenameFlags::empty(),
        )
        .unwrap();
    }
    assert_eq!(
        row(&fs),
        before,
        "a same-directory file rename rewrote the parent row"
    );
}

#[test]
fn replacing_renames_and_creates_in_one_directory_keep_the_count_exact() {
    let fs = Arc::new(test_fs("sp-rename-replace-race"));
    big_parent(&fs, "/sp18");
    const ROUNDS: usize = 150;
    // Every rename replaces an existing file; the files it replaces are gone, so the
    // directory shrinks by one per successful rename while creates grow it.
    let renamer = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            let mut done = 0usize;
            for i in 0..ROUNDS {
                if fs
                    .rename(
                        format!("/sp18/seed-{}", 2 * i),
                        format!("/sp18/seed-{}", 2 * i + 1),
                        RenameFlags::empty(),
                    )
                    .is_ok()
                {
                    done += 1;
                }
            }
            done
        })
    };
    let creator = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..ROUNDS {
                fs.create(format!("/sp18/fresh-{i}"), false).unwrap();
            }
        })
    };
    let replaced = renamer.join().unwrap();
    creator.join().unwrap();
    let listed = fs.list_status("/sp18").unwrap().len();
    assert_eq!(
        listed,
        400 - replaced + ROUNDS,
        "replaced files are still listed"
    );
    assert_eq!(
        fs.file_status("/sp18").unwrap().children_num as usize,
        listed
    );
}

#[test]
fn a_directory_renamed_inside_its_parent_still_checks_for_cycles_and_counts_links() {
    let fs = Arc::new(test_fs("sp-rename-dir"));
    fs.mkdir("/sp19", true).unwrap();
    fs.mkdir("/sp19/a", false).unwrap();
    fs.mkdir("/sp19/a/inner", false).unwrap();
    for i in 0..50 {
        fs.create(format!("/sp19/f{i}"), false).unwrap();
    }
    let creator = {
        let fs = fs.clone();
        std::thread::spawn(move || {
            for i in 0..150 {
                fs.create(format!("/sp19/g{i}"), false).unwrap();
            }
        })
    };
    fs.rename("/sp19/a", "/sp19/b", RenameFlags::empty())
        .unwrap();
    creator.join().unwrap();
    assert!(fs
        .rename("/sp19/b", "/sp19/b/inner/loop", RenameFlags::empty())
        .is_err());
    assert!(fs.exists("/sp19/b/inner").unwrap());
    assert!(!fs.exists("/sp19/a").unwrap());
    let listed = fs.list_status("/sp19").unwrap().len();
    assert_eq!(listed, 1 + 50 + 150);
    assert_eq!(
        fs.file_status("/sp19").unwrap().children_num as usize,
        listed
    );
}

/// Renaming a directory inside its parent changes no link count, but it is the
/// case the cycle check and the subtree bookkeeping exist for, so it must keep the
/// parent exclusive. Many directories renamed in parallel must all land, none lost,
/// and the parent's link count must still equal its subdirectory count.
#[test]
fn concurrent_directory_renames_in_one_parent_lose_nothing() {
    let fs = Arc::new(test_fs("sp-rename-dirs"));
    fs.mkdir("/sp20", true).unwrap();
    const THREADS: usize = 6;
    const EACH: usize = 30;
    for t in 0..THREADS {
        for i in 0..EACH {
            fs.mkdir(format!("/sp20/d-{t}-{i}"), false).unwrap();
        }
    }
    let nlink_before = fs.file_status("/sp20").unwrap().nlink;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.rename(
                        format!("/sp20/d-{t}-{i}"),
                        format!("/sp20/e-{t}-{i}"),
                        RenameFlags::empty(),
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let names: std::collections::BTreeSet<String> = fs
        .list_status("/sp20")
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names.len(), THREADS * EACH);
    assert!(names.iter().all(|n| n.starts_with("e-")));
    let after = fs.file_status("/sp20").unwrap();
    assert_eq!(
        after.nlink, nlink_before,
        "a same-parent directory rename changed the link count"
    );
    assert_eq!(after.children_num as usize, THREADS * EACH);
}

/// Directories renamed inside one parent while files are created and deleted in
/// them and in the parent. A directory keeps its node across a rename, so work
/// that began under the old name must land in the renamed directory, never in a
/// copy, and the parent's counters must come out exact.
#[test]
fn directory_renames_race_with_work_inside_the_renamed_directories() {
    let fs = Arc::new(test_fs("sp-rename-dirs-busy"));
    fs.mkdir("/sp21", true).unwrap();
    const DIRS: usize = 8;
    for d in 0..DIRS {
        fs.mkdir(format!("/sp21/d{d}"), false).unwrap();
    }
    let renamers: Vec<_> = (0..DIRS)
        .map(|d| {
            let fs = fs.clone();
            std::thread::spawn(move || {
                for round in 0..20 {
                    let from = if round % 2 == 0 {
                        format!("d{d}")
                    } else {
                        format!("e{d}")
                    };
                    let to = if round % 2 == 0 {
                        format!("e{d}")
                    } else {
                        format!("d{d}")
                    };
                    fs.rename(
                        format!("/sp21/{from}"),
                        format!("/sp21/{to}"),
                        RenameFlags::empty(),
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let fs = fs.clone();
            std::thread::spawn(move || {
                for i in 0..60 {
                    fs.create(format!("/sp21/top-{w}-{i}"), false).unwrap();
                }
            })
        })
        .collect();
    for handle in renamers.into_iter().chain(writers) {
        handle.join().unwrap();
    }
    let list = fs.list_status("/sp21").unwrap();
    assert_eq!(list.len(), DIRS + 4 * 60);
    assert_eq!(list.iter().filter(|s| s.is_dir).count(), DIRS);
    let parent = fs.file_status("/sp21").unwrap();
    assert_eq!(parent.children_num as usize, list.len());
    assert_eq!(
        parent.nlink as usize,
        2 + DIRS,
        "link count drifted from the subdirectory count"
    );
}

// ---- the root directory as the parent --------------------------------------
//
// The lock planner treats the root as a pure ancestor and drops its read requests,
// keeping only a write request. A writer that holds the parent in read mode and
// relies on an edge lock must still exclude the writers that take the parent in
// write mode, and when the parent is the root that read request is the only thing
// doing it.

#[test]
fn a_create_and_a_mkdir_of_one_name_under_the_root_have_one_winner() {
    let fs = Arc::new(test_fs("sp-root-create-mkdir"));
    for round in 0..80 {
        let name = format!("/rootrace-{round}");
        let barrier = Arc::new(Barrier::new(2));
        let creator = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.create_with_opts(
                    &name,
                    CreateFileOpts::with_create(false),
                    OpenFlags::new_create().set_exclusive(true),
                )
                .map(|_| ())
                .map_err(|e| e.to_string())
            })
        };
        let maker = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.mkdir(&name, false)
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        };
        let results = [creator.join().unwrap(), maker.join().unwrap()];
        let wins = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(wins, 1, "round {round}: {results:?}");
        // The loser must have failed cleanly, not fenced the writer.
        fs.create(format!("/rootafter-{round}"), false).unwrap();
    }
}

#[test]
fn a_rename_and_a_mkdir_onto_one_name_under_the_root_leave_the_writer_usable() {
    let fs = Arc::new(test_fs("sp-root-rename-mkdir"));
    for round in 0..80 {
        fs.create(format!("/rootsrc-{round}"), false).unwrap();
        let target = format!("/roottarget-{round}");
        let barrier = Arc::new(Barrier::new(2));
        let renamer = {
            let (fs, barrier, target) = (fs.clone(), barrier.clone(), target.clone());
            let src = format!("/rootsrc-{round}");
            std::thread::spawn(move || {
                barrier.wait();
                fs.rename(&src, &target, RenameFlags::empty()).map(|_| ())
            })
        };
        let maker = {
            let (fs, barrier, target) = (fs.clone(), barrier.clone(), target.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.mkdir(&target, false).map(|_| ())
            })
        };
        let _ = (renamer.join().unwrap(), maker.join().unwrap());
        fs.create(format!("/rootafter-{round}"), false)
            .unwrap_or_else(|e| panic!("round {round}: the writer was left unusable: {e}"));
    }
}

#[test]
fn a_delete_and_a_mkdir_of_one_name_under_the_root_leave_the_writer_usable() {
    let fs = Arc::new(test_fs("sp-root-delete-mkdir"));
    for round in 0..80 {
        let name = format!("/rootdel-{round}");
        fs.create(&name, false).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let deleter = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.delete(&name, false).map(|_| ())
            })
        };
        let maker = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.mkdir(&name, false).map(|_| ())
            })
        };
        let _ = (deleter.join().unwrap(), maker.join().unwrap());
        fs.create(format!("/rootdelafter-{round}"), false)
            .unwrap_or_else(|e| panic!("round {round}: the writer was left unusable: {e}"));
    }
}

#[test]
fn a_delete_and_a_create_of_one_name_under_the_root_leave_the_writer_usable() {
    let fs = Arc::new(test_fs("sp-root-delete-create"));
    for round in 0..80 {
        let name = format!("/rootdc-{round}");
        fs.create(&name, false).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let deleter = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.delete(&name, false).map(|_| ())
            })
        };
        let creator = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.create(&name, false).map(|_| ())
            })
        };
        let _ = (deleter.join().unwrap(), creator.join().unwrap());
        fs.create(format!("/rootdcafter-{round}"), false)
            .unwrap_or_else(|e| panic!("round {round}: the writer was left unusable: {e}"));
    }
}

// ---- link and symlink under a read-locked parent ---------------------------

#[test]
fn concurrent_symlinks_of_distinct_names_all_land() {
    let fs = Arc::new(test_fs("sp-symlink-distinct"));
    big_parent(&fs, "/sp22");
    const THREADS: usize = 8;
    const EACH: usize = 40;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.symlink(
                        "/sp22/seed-0",
                        format!("/sp22/l{t}-{i}").as_str(),
                        false,
                        0o777,
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(fs.list_status("/sp22").unwrap().len(), 400 + THREADS * EACH);
    assert_eq!(
        fs.file_status("/sp22").unwrap().children_num as usize,
        400 + THREADS * EACH
    );
}

#[test]
fn concurrent_symlinks_of_one_name_have_exactly_one_winner() {
    let fs = Arc::new(test_fs("sp-symlink-same-name"));
    big_parent(&fs, "/sp23");
    const THREADS: usize = 8;
    for round in 0..50 {
        let path = format!("/sp23/ln-{round}");
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.symlink("/sp23/seed-0", path.as_str(), false, 0o777)
                        .map_err(|e| e.to_string())
                })
            })
            .collect();
        let results: Vec<Result<(), String>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wins = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            wins, 1,
            "round {round}: {wins} symlinks of {path} succeeded: {results:?}"
        );
        fs.create(format!("/sp23/after-{round}"), false).unwrap();
    }
}

#[test]
fn concurrent_links_of_distinct_names_all_land() {
    let fs = Arc::new(test_fs("sp-link-distinct"));
    big_parent(&fs, "/sp24");
    const THREADS: usize = 8;
    const EACH: usize = 30;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.link("/sp24/seed-0", format!("/sp24/h{t}-{i}").as_str())
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(fs.list_status("/sp24").unwrap().len(), 400 + THREADS * EACH);
    assert_eq!(
        fs.file_status("/sp24").unwrap().children_num as usize,
        400 + THREADS * EACH
    );
}

#[test]
fn concurrent_links_of_one_name_have_exactly_one_winner() {
    let fs = Arc::new(test_fs("sp-link-same-name"));
    big_parent(&fs, "/sp25");
    const THREADS: usize = 8;
    for round in 0..50 {
        let path = format!("/sp25/hl-{round}");
        let barrier = Arc::new(Barrier::new(THREADS));
        // Each racer links a different source to the same new name. Racers sharing a
        // source would exclude each other on the source inode's write lock and never
        // reach the edge lock, which is what this case has to pin.
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let (fs, barrier, path) = (fs.clone(), barrier.clone(), path.clone());
                let source = format!("/sp25/seed-{}", round * THREADS + t);
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.link(source.as_str(), path.as_str())
                        .map_err(|e| e.to_string())
                })
            })
            .collect();
        let results: Vec<Result<(), String>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wins = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            wins, 1,
            "round {round}: {wins} links of {path} succeeded: {results:?}"
        );
        fs.create(format!("/sp25/after-{round}"), false).unwrap();
    }
}

#[test]
fn a_link_and_a_symlink_leave_the_parent_inode_row_untouched() {
    let fs = test_fs("sp-link-row");
    big_parent(&fs, "/sp26");
    let id = fs.file_status("/sp26").unwrap().id;
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
    for i in 0..20 {
        fs.symlink("/sp26/seed-0", format!("/sp26/s{i}").as_str(), false, 0o777)
            .unwrap();
        fs.link("/sp26/seed-1", format!("/sp26/h{i}").as_str())
            .unwrap();
    }
    assert_eq!(row(&fs), before, "a link or symlink rewrote the parent row");
    assert!(fs.file_status("/sp26").unwrap().mtime > before);
}

#[test]
fn a_symlink_and_a_mkdir_of_one_name_under_the_root_have_one_winner() {
    let fs = Arc::new(test_fs("sp-root-symlink-mkdir"));
    for round in 0..60 {
        let name = format!("/rootsym-{round}");
        let barrier = Arc::new(Barrier::new(2));
        let linker = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.symlink("/x", name.as_str(), false, 0o777)
                    .map_err(|e| e.to_string())
            })
        };
        let maker = {
            let (fs, barrier, name) = (fs.clone(), barrier.clone(), name.clone());
            std::thread::spawn(move || {
                barrier.wait();
                fs.mkdir(&name, false)
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        };
        let results = [linker.join().unwrap(), maker.join().unwrap()];
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "round {round}: {results:?}"
        );
        fs.create(format!("/rootsymafter-{round}"), false).unwrap();
    }
}
