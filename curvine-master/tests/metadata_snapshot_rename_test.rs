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

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::inode::{InodePath, ROOT_INODE_ID};
use curvine_master::master::Master;
use curvine_model::RenameFlags;
use curvine_runtime::common::Utils;

#[test]
fn opposite_cross_directory_renames_complete_and_restore() {
    use std::sync::{mpsc, Arc, Barrier};
    use std::time::Duration;

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("opposite-rename-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.mkdir("/a", true).unwrap();
    fs.mkdir("/b", true).unwrap();
    let start = Arc::new(Barrier::new(9));
    let (tx, rx) = mpsc::channel();
    let mut workers = Vec::new();
    let mut expected = Vec::new();
    for index in 0..8 {
        let (home, other) = if index % 2 == 0 {
            ("a", "b")
        } else {
            ("b", "a")
        };
        let source = format!("/{home}/file-{index}");
        let destination = format!("/{other}/file-{index}");
        expected.push((
            source.clone(),
            destination.clone(),
            fs.create(&source, false).unwrap().id,
        ));
        let worker_fs = fs.clone();
        let worker_start = start.clone();
        let done = tx.clone();
        workers.push(std::thread::spawn(move || {
            worker_start.wait();
            for _ in 0..40 {
                worker_fs
                    .rename(&source, &destination, RenameFlags::empty())
                    .unwrap();
                worker_fs
                    .rename(&destination, &source, RenameFlags::empty())
                    .unwrap();
            }
            done.send(()).unwrap();
        }));
    }
    start.wait();
    for _ in 0..8 {
        rx.recv_timeout(Duration::from_secs(15))
            .expect("cross-directory rename did not finish");
    }
    for worker in workers {
        worker.join().unwrap();
    }
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        for (path, other, id) in &expected {
            assert_eq!(fs.file_status(path).unwrap().id, *id);
            assert!(!fs.exists(other).unwrap());
        }
    }
}

#[test]
fn inode_path_rejects_cycles_and_missing_ancestors() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("inode-parent-corrupt-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let file = fs.create("/dir/file", true).unwrap();
    let dir = fs.fs_dir.write();
    let mut inode = dir.store_ref().get_inode(file.id, None).unwrap().unwrap();
    inode.set_parent_id(file.id);
    let mut batch = dir.store_ref().new_batch();
    batch.write_inode(&inode).unwrap();
    batch.commit().unwrap();
    let error = dir.get_inode_path(file.id).unwrap_err();
    assert!(error.to_string().contains("Cycle detected"));

    inode.set_parent_id(i64::MAX);
    let mut batch = dir.store_ref().new_batch();
    batch.write_inode(&inode).unwrap();
    batch.commit().unwrap();
    let error = dir.get_inode_path(file.id).unwrap_err();
    assert!(error.to_string().contains("missing ancestor"));
}

#[test]
fn inode_path_does_not_mix_parent_generations_during_rename() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("inode-parent-snapshot-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let file = fs.create("/a/x/file", true).unwrap();
    fs.mkdir("/b/y", true).unwrap();
    let worker_fs = fs.clone();
    let start = std::sync::Arc::new(std::sync::Barrier::new(2));
    let worker_start = start.clone();
    let worker = std::thread::spawn(move || {
        worker_start.wait();
        for _ in 0..100 {
            worker_fs
                .rename("/a/x/file", "/b/y/file", RenameFlags::empty())
                .unwrap();
            worker_fs.rename("/a", "/c", RenameFlags::empty()).unwrap();
            worker_fs.rename("/c", "/a", RenameFlags::empty()).unwrap();
            worker_fs
                .rename("/b/y/file", "/a/x/file", RenameFlags::empty())
                .unwrap();
        }
    });
    start.wait();
    for _ in 0..400 {
        let dir = fs.fs_dir.read();
        let path = dir.get_inode_path(file.id).unwrap();
        assert!(
            path == "/a/x/file" || path == "/b/y/file",
            "mixed path: {path}"
        );
    }
    worker.join().unwrap();
}

#[test]
fn snapshot_path_rejects_dangling_edge() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-dangling-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    {
        let dir = fs.fs_dir.write();
        let mut batch = dir.store_ref().new_batch();
        batch
            .add_child(ROOT_INODE_ID, "dangling", ROOT_INODE_ID + 999)
            .unwrap();
        batch.commit().unwrap();
    }
    let lease = fs.fs_dir.read().begin_snapshot_scan().unwrap();
    let snapshot = lease.snapshot_reader().unwrap();
    let error = match snapshot.resolve_path("/dangling") {
        Ok(_) => panic!("dangling edge must fail closed"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("references missing inode"));
}

#[test]
fn snapshot_sees_consistent_edges_and_inodes_across_replace_rename() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-rename-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let source = fs.create("/left/source", true).unwrap();
    let target = fs.create("/right/target", true).unwrap();
    fs.link("/right/target", "/right/alias").unwrap();
    let left = fs.file_status("/left").unwrap().id;
    let right = fs.file_status("/right").unwrap().id;

    let lease = fs.fs_dir.read().begin_snapshot_scan().unwrap();
    let before = lease.snapshot_reader().unwrap();
    {
        let dir = fs.fs_dir.read();
        for path in [
            "/",
            "/left/source",
            "/right/alias",
            "/left/missing",
            "/left/missing/child",
            "/right/alias/child",
        ] {
            let memory = InodePath::resolve(dir.root_ptr(), path, dir.store_ref()).unwrap();
            let snapshot = before.resolve_path(path).unwrap();
            assert_eq!(snapshot.is_full(), memory.is_full(), "{path}");
            assert_eq!(
                snapshot.parent().map(|inode| inode.id()),
                memory.get_inode(-2).map(|inode| inode.id()),
                "{path}"
            );
            let expected: Vec<_> = (0..snapshot.components().len())
                .filter_map(|i| memory.get_inode(i as i32).map(|inode| inode.id()))
                .collect();
            let actual: Vec<_> = snapshot.inodes().iter().map(|inode| inode.id()).collect();
            assert_eq!(actual, expected, "{path}");
        }
    }
    let root = before.resolve_path("/").unwrap();
    assert!(root.is_full());
    assert_eq!(root.target().unwrap().id(), ROOT_INODE_ID);
    assert!(root.parent().is_none());
    assert!(before.resolve_path("relative/path").is_err());
    assert!(before.resolve_path("/left/").is_err());
    let partial = before.resolve_path("/left/missing/child").unwrap();
    assert!(!partial.is_full());
    assert!(partial.target().is_none());
    assert!(partial.parent().is_none());
    assert_eq!(partial.inodes().last().unwrap().id(), left);
    let no_parent = before.resolve_path("/left/missing").unwrap();
    assert_eq!(no_parent.parent().unwrap().id(), left);
    let alias = before.resolve_path("/right/alias").unwrap();
    assert!(alias.is_full());
    assert_eq!(alias.target().unwrap().id(), target.id);
    assert_eq!(alias.target().unwrap().name(), "alias");
    assert!(!before.resolve_path("/right/alias/child").unwrap().is_full());
    fs.rename("/left/source", "/right/target", RenameFlags::empty())
        .unwrap();
    let after = lease.snapshot_reader().unwrap();
    assert_eq!(
        before
            .resolve_path("/right/target")
            .unwrap()
            .target()
            .unwrap()
            .id(),
        target.id
    );
    assert_eq!(
        after
            .resolve_path("/right/target")
            .unwrap()
            .target()
            .unwrap()
            .id(),
        source.id
    );
    assert!(!after.resolve_path("/left/source").unwrap().is_full());
    for (path, unchanged) in [
        ("/", true),
        ("/left/source", false),
        ("/right/target", false),
        ("/right/alias", true),
        ("/left/missing/child", true),
    ] {
        assert_eq!(
            before
                .resolve_path(path)
                .unwrap()
                .same_resolution(&after.resolve_path(path).unwrap()),
            unchanged,
            "{path}"
        );
    }
    assert!(!before
        .resolve_path("/right/alias")
        .unwrap()
        .same_resolution(&before.resolve_path("/right/target").unwrap()));

    assert_eq!(
        before.get_child_id(left, "source").unwrap(),
        Some(source.id)
    );
    assert_eq!(
        before.get_child_id(right, "target").unwrap(),
        Some(target.id)
    );
    assert_eq!(
        before.get_child_id(right, "alias").unwrap(),
        Some(target.id)
    );
    assert_eq!(
        before
            .get_inode(target.id)
            .unwrap()
            .unwrap()
            .nlink()
            .unwrap(),
        2
    );
    assert_eq!(after.get_child_id(left, "source").unwrap(), None);
    assert_eq!(
        after.get_child_id(right, "target").unwrap(),
        Some(source.id)
    );
    assert_eq!(after.get_child_id(right, "alias").unwrap(), Some(target.id));
    assert_eq!(
        after
            .get_inode(target.id)
            .unwrap()
            .unwrap()
            .nlink()
            .unwrap(),
        1
    );
}
