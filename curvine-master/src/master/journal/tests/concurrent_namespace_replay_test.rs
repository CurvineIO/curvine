// Copyright 2026 OPPO
// Licensed under the Apache License, Version 2.0 (see LICENSE).

//! Replay journal entries enqueued by concurrent namespace mutations.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use crate::master::fs::MasterFilesystem;
use crate::master::journal::{JournalBatch, JournalEntry, JournalSystem};
use crate::Master;
use curvine_config::ClusterConf;
use curvine_error::FsResult;
use curvine_model::{CreateFileOpts, OpenFlags, RenameFlags};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_runtime::common::Utils;
use raft::eraftpb::Entry;

fn conf(name: &str) -> ClusterConf {
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = true;
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 0;
    conf.change_test_meta_dir(format!("concurrent-replay-{name}-{}", Utils::rand_str(8)));
    conf
}

fn entries(
    fs: &MasterFilesystem,
    path: &str,
) -> BTreeMap<String, (i64, u32, bool, Option<String>)> {
    fs.list_status(path)
        .unwrap()
        .into_iter()
        .map(|status| {
            (
                status.name,
                (status.id, status.nlink, status.is_dir, status.target),
            )
        })
        .collect()
}

/// Reads a directory's inode row directly, bypassing the overlay that a normal
/// read applies from the attribute record.
fn raw_directory_row(fs: &MasterFilesystem, path: &str) -> FsResult<(i64, u32)> {
    let id = fs.file_status(path)?.id;
    let handle = fs.fs_dir();
    let view = handle.view()?;
    let inode =
        view.store_ref().store().get_inode(id)?.ok_or_else(|| {
            curvine_error::FsError::common(format!("missing inode row for {path}"))
        })?;
    Ok((inode.mtime(), inode.as_dir_ref()?.nlink))
}

/// Compares the directory state that has to converge, without depending on which
/// record generation a replica encoded: whole-store hashes change when the
/// process-wide format flag flips, which other tests are allowed to do.
fn assert_same_directory_state(
    primary: &MasterFilesystem,
    replica: &MasterFilesystem,
    path: &str,
) -> FsResult<()> {
    let expected = primary.file_status(path)?;
    let actual = replica.file_status(path)?;
    assert_eq!(actual.mtime, expected.mtime, "{path}: mtime");
    assert_eq!(actual.nlink, expected.nlink, "{path}: nlink");
    assert_eq!(
        actual.children_num, expected.children_num,
        "{path}: child count"
    );
    assert_eq!(
        raw_directory_row(replica, path)?,
        raw_directory_row(primary, path)?,
        "{path}: inode row"
    );
    Ok(())
}

fn replay(target: &JournalSystem, journal: Vec<JournalEntry>) -> FsResult<()> {
    // Raft serializes records before applying them on a replica. The test queue
    // can retain Arc-backed directory nodes shared with the source instance.
    let loader = target.journal_loader();
    let index = loader.get_fsm_state().applied.index + 1;
    let mut batch = JournalBatch::new(index);
    for entry in journal {
        batch.push(entry);
    }
    let entry = Entry {
        index,
        term: 1,
        data: batch.encode()?,
        ..Default::default()
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime
        .block_on(loader.apply(true, ApplyMsg::new_entry(entry)))
        .unwrap();
    assert_eq!(loader.get_fsm_state().applied.index, index);
    Ok(())
}

#[test]
fn concurrent_namespace_mutations_replay_to_the_same_state() -> FsResult<()> {
    Master::init_test_metrics();
    let source = JournalSystem::from_conf(&conf("source"))?;
    let target = JournalSystem::from_conf(&conf("target"))?;
    let fs = Arc::new(source.fs());
    fs.mkdir("/parent", false)?;
    fs.mkdir("/other", false)?;
    // 2,048 is the maximum runtime promotion threshold; one extra insertion
    // triggers promotion even on hosts with many available CPUs.
    const SEED: usize = 2_049;
    for i in 0..SEED {
        fs.create(format!("/parent/seed-{i}"), false)?;
    }
    let handle = fs.fs_dir();
    let view = handle.view()?;
    let parent = crate::master::meta::inode::InodePath::resolve(
        view.root_node(),
        "/parent",
        view.store_ref(),
    )?;
    assert!(
        parent
            .get_last_dir_node()
            .unwrap()
            .children
            .is_sharded_for_test(),
        "fixture must exercise the sharded child index"
    );
    let setup = view.take_entries();
    drop(view);
    assert_eq!(setup.len(), 2 + SEED);
    replay(&target, setup)?;

    const THREADS: usize = 8;
    const EACH: usize = 20;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    let original = format!("/parent/f-{thread}-{i}");
                    let directory = if i % 2 == 0 { "/parent" } else { "/other" };
                    let renamed = format!("{directory}/r-{thread}-{i}");
                    let hard = format!("/parent/h-{thread}-{i}");
                    let symbolic = format!("/parent/s-{thread}-{i}");
                    fs.create(&original, false).unwrap();
                    fs.rename(&original, &renamed, RenameFlags::empty())
                        .unwrap();
                    fs.link(&renamed, &hard).unwrap();
                    fs.symlink(&renamed, &symbolic, false, 0o777).unwrap();
                    if i % 2 == 0 {
                        fs.delete(&hard, false).unwrap();
                    }
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("namespace worker panicked");
    }

    let source_parent = entries(&fs, "/parent");
    let source_other = entries(&fs, "/other");
    assert_eq!(
        source_parent.len() + source_other.len(),
        SEED + THREADS * EACH * 5 / 2
    );
    assert_eq!(
        fs.file_status("/parent")?.children_num as usize,
        source_parent.len()
    );
    assert_eq!(
        fs.file_status("/other")?.children_num as usize,
        source_other.len()
    );

    let journal = fs.fs_dir().view()?.take_entries();
    assert_eq!(journal.len(), THREADS * EACH * 4 + THREADS * EACH / 2);
    replay(&target, journal)?;
    let target_fs = target.fs();
    assert_eq!(entries(&target_fs, "/parent"), source_parent);
    assert_eq!(entries(&target_fs, "/other"), source_other);
    for path in ["/parent", "/other"] {
        assert_same_directory_state(&fs, &target_fs, path)?;
    }
    assert!(
        target_fs.fs_dir().view()?.take_entries().is_empty(),
        "replay echoed journal entries"
    );
    Ok(())
}

#[test]
fn concurrent_mkdir_replays_parent_link_and_child_counts() -> FsResult<()> {
    Master::init_test_metrics();
    let source = JournalSystem::from_conf(&conf("mkdir-source"))?;
    let target = JournalSystem::from_conf(&conf("mkdir-target"))?;
    let fs = Arc::new(source.fs());
    fs.mkdir("/parent", false)?;
    replay(&target, fs.fs_dir().view()?.take_entries())?;

    const THREADS: usize = 8;
    const EACH: usize = 12;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.mkdir(format!("/parent/d-{thread}-{i}"), false).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("mkdir worker panicked");
    }
    let records = fs.fs_dir().view()?.take_entries();
    assert_eq!(records.len(), THREADS * EACH);
    replay(&target, records)?;
    assert_eq!(entries(&target.fs(), "/parent"), entries(&fs, "/parent"));
    let primary = fs.file_status("/parent")?;
    let replica = target.fs().file_status("/parent")?;
    assert_eq!(primary.children_num as usize, THREADS * EACH);
    assert_eq!(replica.children_num, primary.children_num);
    assert_eq!(primary.nlink as usize, 2 + THREADS * EACH);
    assert_eq!(replica.nlink, primary.nlink);
    assert_eq!(replica.mtime, primary.mtime);
    assert_same_directory_state(&fs, &target.fs(), "/parent")?;
    Ok(())
}

#[test]
fn competing_creates_replay_only_the_winning_entry() -> FsResult<()> {
    Master::init_test_metrics();
    let source = JournalSystem::from_conf(&conf("contended-source"))?;
    let target = JournalSystem::from_conf(&conf("contended-target"))?;
    let fs = Arc::new(source.fs());
    fs.mkdir("/parent", false)?;
    replay(&target, fs.fs_dir().view()?.take_entries())?;

    const THREADS: usize = 8;
    const ROUNDS: usize = 20;
    for round in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let (fs, barrier) = (fs.clone(), barrier.clone());
                std::thread::spawn(move || {
                    let path = format!("/parent/same-{round}");
                    barrier.wait();
                    fs.create_with_opts(
                        path,
                        CreateFileOpts::with_create(false),
                        OpenFlags::new_create().set_exclusive(true),
                    )
                    .map(|_| ())
                    .map_err(|error| error.to_string())
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "round {round}: {results:?}"
        );
        assert!(
            results
                .iter()
                .filter_map(|result| result.as_ref().err())
                .all(|error| error.contains("already exists")),
            "round {round}: unexpected error: {results:?}"
        );
    }
    let journal = fs.fs_dir().view()?.take_entries();
    assert_eq!(
        journal.len(),
        ROUNDS,
        "losing creates must not produce journal records"
    );
    replay(&target, journal)?;
    assert_eq!(entries(&target.fs(), "/parent"), entries(&fs, "/parent"));
    assert_eq!(
        target.fs().file_status("/parent")?.children_num as usize,
        ROUNDS
    );
    Ok(())
}

#[test]
fn concurrent_mkdir_keeps_the_record_authoritative() -> FsResult<()> {
    Master::init_test_metrics();
    let source = JournalSystem::from_conf(&conf("record-source"))?;
    let target = JournalSystem::from_conf(&conf("record-target"))?;
    let fs = Arc::new(source.fs());
    fs.mkdir("/parent", false)?;
    fs.create("/parent/seed", false)?;
    replay(&target, fs.fs_dir().view()?.take_entries())?;

    // The row exists from before the gate was exercised; a directory child under
    // mkdir must not rewrite it again.
    let row_before = raw_directory_row(&fs, "/parent")?;
    let status_before = fs.file_status("/parent")?;
    std::thread::sleep(std::time::Duration::from_millis(5));

    const THREADS: usize = 6;
    const EACH: usize = 8;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let (fs, barrier) = (fs.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..EACH {
                    fs.mkdir(format!("/parent/d-{thread}-{i}"), false).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("mkdir worker panicked");
    }

    let children = THREADS * EACH;
    let live = fs.file_status("/parent")?;
    assert_eq!(live.children_num as usize, children + 1);
    assert_eq!(
        live.nlink as usize,
        2 + children,
        "link count lives in the record"
    );
    assert!(
        live.mtime > status_before.mtime,
        "the record must carry the newer mtime"
    );
    assert_eq!(
        raw_directory_row(&fs, "/parent")?,
        row_before,
        "mkdir must not rewrite the parent's inode row"
    );
    assert_eq!(fs.list_status("/parent")?.len(), children + 1);

    let records = fs.fs_dir().view()?.take_entries();
    assert_eq!(records.len(), children);
    replay(&target, records)?;
    assert_same_directory_state(&fs, &target.fs(), "/parent")?;
    assert_eq!(entries(&target.fs(), "/parent"), entries(&fs, "/parent"));
    Ok(())
}

#[test]
fn conflicting_rename_and_delete_replay_in_enqueued_order() -> FsResult<()> {
    Master::init_test_metrics();
    let source = JournalSystem::from_conf(&conf("rename-delete-source"))?;
    let target = JournalSystem::from_conf(&conf("rename-delete-target"))?;
    let fs = Arc::new(source.fs());
    fs.mkdir("/parent", false)?;
    fs.mkdir("/other", false)?;
    replay(&target, fs.fs_dir().view()?.take_entries())?;

    for round in 0..20 {
        let src = format!("/parent/race-{round}");
        let dst = format!("/other/race-{round}");
        fs.create(&src, false)?;
        let barrier = Arc::new(Barrier::new(2));
        let renamer = {
            let (fs, barrier, src, dst) = (fs.clone(), barrier.clone(), src.clone(), dst);
            std::thread::spawn(move || {
                barrier.wait();
                fs.rename(src, dst, RenameFlags::empty()).is_ok()
            })
        };
        let deleter = {
            let (fs, barrier, src) = (fs.clone(), barrier.clone(), src);
            std::thread::spawn(move || {
                barrier.wait();
                fs.delete(src, false).is_ok()
            })
        };
        assert_ne!(
            renamer.join().unwrap(),
            deleter.join().unwrap(),
            "round {round}"
        );
        let records = fs.fs_dir().view()?.take_entries();
        assert_eq!(records.len(), 2, "round {round}: create and one winner");
        replay(&target, records)?;
        for path in ["/parent", "/other"] {
            assert_eq!(
                entries(&target.fs(), path),
                entries(&fs, path),
                "round {round}: {path}"
            );
        }
    }
    Ok(())
}
