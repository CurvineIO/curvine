// Copyright 2026 OPPO
// Licensed under the Apache License, Version 2.0 (see LICENSE).

//! The canonical-attributes gate decides which copy of a directory's attributes
//! is the truth, so it has to travel with the data: a process restart must not
//! silently put the stale inode row back in charge.

use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;

fn conf(name: &str) -> ClusterConf {
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("{name}-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("{name}-journal-{}", Utils::rand_str(6)));
    conf
}

fn reopen(base: &ClusterConf) -> MasterFilesystem {
    let mut conf = base.clone();
    // Same directories, not formatted: this is a restart of the same master.
    conf.format_master = false;
    JournalSystem::fs_only_for_test(&conf).unwrap()
}

#[test]
fn a_restart_keeps_the_gate_and_the_link_count() {
    Master::init_test_metrics();
    let base = conf("canonical-restart");
    let nlink_after_gate;
    {
        let fs = JournalSystem::fs_only_for_test(&base).unwrap();
        fs.enable_canonical_directory_attributes().unwrap();
        fs.mkdir("/parent", false).unwrap();
        for i in 0..5 {
            fs.mkdir(format!("/parent/d{i}"), false).unwrap();
        }
        nlink_after_gate = fs.file_status("/parent").unwrap().nlink;
        assert_eq!(nlink_after_gate, 2 + 5);
        // Dropping the filesystem releases the store; the data stays on disk.
    }

    let fs = reopen(&base);
    assert!(
        fs.canonical_directory_attributes_enabled(),
        "the gate must be read back from the store, not reset to its default"
    );
    assert_eq!(fs.file_status("/parent").unwrap().nlink, nlink_after_gate);
    fs.mkdir("/parent/after-restart", false).unwrap();
    assert_eq!(
        fs.file_status("/parent").unwrap().nlink,
        nlink_after_gate + 1,
        "a mkdir after the restart read-modify-wrote a stale row over the record"
    );
    assert_eq!(fs.list_status("/parent").unwrap().len(), 6);
}

#[test]
fn a_store_that_never_enabled_the_gate_stays_without_it() {
    Master::init_test_metrics();
    let base = conf("canonical-restart-off");
    {
        let fs = JournalSystem::fs_only_for_test(&base).unwrap();
        fs.mkdir("/parent", false).unwrap();
        fs.mkdir("/parent/d0", false).unwrap();
        assert!(!fs.canonical_directory_attributes_enabled());
    }
    let fs = reopen(&base);
    assert!(!fs.canonical_directory_attributes_enabled());
    fs.mkdir("/parent/d1", false).unwrap();
    assert_eq!(fs.file_status("/parent").unwrap().nlink, 4);
}

#[test]
fn enabling_the_gate_on_a_populated_store_keeps_every_directory_exact() {
    Master::init_test_metrics();
    let base = conf("canonical-enable-populated");
    let fs = JournalSystem::fs_only_for_test(&base).unwrap();
    // Written with the gate off: the row and the record are kept in step.
    fs.mkdir("/a", false).unwrap();
    fs.mkdir("/a/b", false).unwrap();
    for i in 0..4 {
        fs.mkdir(format!("/a/b/c{i}"), false).unwrap();
        fs.create(format!("/a/b/f{i}"), false).unwrap();
    }
    let before: Vec<_> = ["/", "/a", "/a/b", "/a/b/c0"]
        .iter()
        .map(|p| {
            let s = fs.file_status(p).unwrap();
            (s.nlink, s.children_num, s.mtime)
        })
        .collect();

    fs.enable_canonical_directory_attributes().unwrap();
    assert!(fs.canonical_directory_attributes_enabled());
    let after: Vec<_> = ["/", "/a", "/a/b", "/a/b/c0"]
        .iter()
        .map(|p| {
            let s = fs.file_status(p).unwrap();
            (s.nlink, s.children_num, s.mtime)
        })
        .collect();
    assert_eq!(
        after, before,
        "enabling the gate changed what a read returns"
    );

    // From now on only the record moves, and a restart agrees with the live view.
    fs.mkdir("/a/b/new", false).unwrap();
    let live = fs.file_status("/a/b").unwrap();
    assert_eq!(live.nlink, 2 + 4 + 1);
    drop(fs);
    let fs = reopen(&base);
    assert!(fs.canonical_directory_attributes_enabled());
    let restored = fs.file_status("/a/b").unwrap();
    assert_eq!(
        (restored.nlink, restored.children_num),
        (live.nlink, live.children_num)
    );
    assert_eq!(fs.list_status("/a/b").unwrap().len(), 4 + 4 + 1);
}
