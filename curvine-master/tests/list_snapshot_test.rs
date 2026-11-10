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
use curvine_master::master::fs::DeleteMode;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_model::{ListOptions, RenameFlags};
use curvine_runtime::common::Utils;
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn listing_matches_live_status_fields() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("list-fields-{}", Utils::rand_str(8)));
    conf.journal.enable = false;
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.mkdir("/parent/dir/nested", true).unwrap();
    fs.create("/parent/dir/file", false).unwrap();
    fs.create("/parent/file", false).unwrap();
    fs.link("/parent/file", "/parent/alias").unwrap();
    fs.set_attr(
        "/parent/dir",
        curvine_model::SetAttrOpts {
            mode: Some(0o700),
            ..Default::default()
        },
    )
    .unwrap();

    for path in ["/", "/parent", "/parent/dir", "/parent/file"] {
        let expected_status = {
            let dir = fs.fs_dir.write();
            let inp = curvine_master::master::meta::inode::InodePath::resolve(
                dir.root_node(),
                path,
                dir.store_ref(),
            )
            .unwrap();
            dir.file_status(&inp).unwrap()
        };
        assert_eq!(
            serde_json::to_value(fs.file_status(path).unwrap()).unwrap(),
            serde_json::to_value(expected_status).unwrap(),
            "status path: {path}"
        );
        for opts in [ListOptions::default(), ListOptions::with_limit(1)] {
            let expected = {
                let dir = fs.fs_dir.write();
                let inp = curvine_master::master::meta::inode::InodePath::resolve(
                    dir.root_node(),
                    path,
                    dir.store_ref(),
                )
                .unwrap();
                dir.list_options(&inp, &opts).unwrap()
            };
            let actual = fs.list_options(path, opts).unwrap();
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap(),
                "path: {path}"
            );
        }
    }
}

#[test]
fn paginated_listing_uses_shared_lock_and_preserves_cursors() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("list-page-snapshot-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    for name in ["a", "aa", "b", "中"] {
        fs.create(format!("/dir/{name}"), true).unwrap();
    }
    let options = ListOptions {
        limit: Some(2),
        start_after: Some("a".into()),
    };
    let guard = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (tx, rx) = mpsc::channel();
    let worker =
        std::thread::spawn(move || tx.send(worker_fs.list_options("/dir", options)).unwrap());
    let result = rx.recv_timeout(Duration::from_secs(5));
    drop(guard);
    let page = result
        .expect("paginated listing blocked on shared namespace reader")
        .unwrap();
    worker.join().unwrap();
    assert_eq!(
        page.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        ["aa", "b"]
    );
    let page = fs
        .list_options(
            "/dir",
            ListOptions {
                limit: None,
                start_after: Some("ab".into()),
            },
        )
        .unwrap();
    assert_eq!(
        page.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        ["b", "中"]
    );
    assert!(fs
        .list_options("/dir", ListOptions::with_limit(0))
        .unwrap()
        .is_empty());
    assert!(fs
        .list_options(
            "/dir/a",
            ListOptions {
                limit: None,
                start_after: Some("a".into())
            }
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        fs.list_options("/dir/a", ListOptions::with_limit(1))
            .unwrap()
            .len(),
        1
    );
    for cursor in [None, Some("a"), Some("ab"), Some("中"), Some("终")] {
        for limit in [None, Some(0), Some(1), Some(10)] {
            let opts = ListOptions {
                limit,
                start_after: cursor.map(str::to_owned),
            };
            let expected = {
                let dir = fs.fs_dir.write();
                let inp = curvine_master::master::meta::inode::InodePath::resolve(
                    dir.root_node(),
                    "/dir",
                    dir.store_ref(),
                )
                .unwrap();
                dir.list_options(&inp, &opts).unwrap()
            };
            let actual = fs.list_options("/dir", opts).unwrap();
            assert_eq!(
                actual.iter().map(|s| (&s.name, s.id)).collect::<Vec<_>>(),
                expected.iter().map(|s| (&s.name, s.id)).collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn paginated_trash_listing_skips_pending_before_applying_limit() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("list-trash-page-{}", Utils::rand_str(8)));
    conf.master.trash_enabled = true;
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.create("/visible", false).unwrap();
    assert!(
        fs.delete_with_mode("/visible", false, DeleteMode::Default)
            .unwrap()
            .trashed
    );
    fs.create("/visible2", false).unwrap();
    assert!(
        fs.delete_with_mode("/visible2", false, DeleteMode::Default)
            .unwrap()
            .trashed
    );
    fs.create("/pending", false).unwrap();
    assert!(
        fs.delete_with_mode("/pending", false, DeleteMode::AsyncPurge)
            .unwrap()
            .purge_pending
    );
    let expected = fs.list_status("/.Trash").unwrap();
    assert_eq!(expected.len(), 2);
    assert!(fs
        .list_options("/.Trash", ListOptions::with_limit(0))
        .unwrap()
        .is_empty());
    let page = fs
        .list_options("/.Trash", ListOptions::with_limit(1))
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].name, expected[0].name);
    let next = fs
        .list_options(
            "/.Trash",
            ListOptions {
                limit: Some(1),
                start_after: Some(page[0].name.clone()),
            },
        )
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].name, expected[1].name);
    assert!(fs
        .list_options(
            "/.Trash",
            ListOptions {
                limit: Some(1),
                start_after: Some(next[0].name.clone())
            }
        )
        .unwrap()
        .is_empty());
}

#[test]
fn listing_uses_owned_snapshot_with_shared_namespace_guard() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("list-snapshot-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let file = fs.create("/dir/file", true).unwrap();
    fs.link("/dir/file", "/dir/alias").unwrap();
    fs.mkdir("/dir/sub", false).unwrap();
    fs.create("/dir/sub/child", false).unwrap();
    let dir_id = fs.file_status("/dir").unwrap().id;
    let lease = fs.fs_dir.read().begin_snapshot_scan().unwrap();
    let snapshot = lease.snapshot_reader().unwrap();
    fs.rename("/dir/file", "/dir/moved", RenameFlags::empty())
        .unwrap();
    let inodes = snapshot.get_inodes(&[i64::MAX, file.id, file.id]).unwrap();
    assert!(inodes[0].is_none());
    assert_eq!(inodes[1].as_ref().unwrap().id(), file.id);
    assert_eq!(inodes[1].as_ref().unwrap().name(), "file");
    assert_eq!(inodes[2].as_ref().unwrap().id(), file.id);
    let old: Vec<_> = snapshot
        .children(dir_id)
        .unwrap()
        .iter()
        .map(|child| child.name().to_string())
        .collect();
    assert_eq!(old, ["alias", "file", "sub"]);
    let guard = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || tx.send(worker_fs.list_status("/dir")).unwrap());
    let result = rx.recv_timeout(Duration::from_secs(5));
    drop(guard);
    let statuses = result
        .expect("listing blocked on a shared namespace reader")
        .unwrap();
    worker.join().unwrap();
    assert_eq!(
        statuses.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        ["alias", "moved", "sub"]
    );
    assert_eq!(statuses[0].id, statuses[1].id);
    assert_eq!(statuses[2].children_num, 1);
    assert_eq!(statuses[2].path, "/dir/sub");
    assert!(fs.list_status("/dir/missing").is_err());
    assert_eq!(fs.list_status("/dir/moved").unwrap()[0].name, "moved");
    {
        let dir = fs.fs_dir.write();
        let mut batch = dir.store_ref().new_batch();
        batch.add_child(dir_id, "00-dangling", i64::MAX).unwrap();
        batch.commit().unwrap();
    }
    assert_eq!(fs.list_status("/dir").unwrap().len(), 3);
    let page = fs.list_options("/dir", ListOptions::with_limit(1)).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].name, "alias");
}

#[test]
fn listing_is_consistent_while_directory_is_recreated() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("list-recreate-{}", Utils::rand_str(8)));
    conf.journal.enable = false;
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.mkdir("/parent", false).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let worker_fs = fs.clone();
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || {
        worker_barrier.wait();
        for _ in 0..200 {
            worker_fs.mkdir("/parent/dir", false).unwrap();
            worker_fs.create("/parent/dir/file", false).unwrap();
            worker_fs.delete("/parent/dir", true).unwrap();
        }
    });
    barrier.wait();
    for _ in 0..1000 {
        match fs.list_status("/parent/dir") {
            Ok(statuses) => {
                assert!(statuses.len() <= 1);
                if let Some(status) = statuses.first() {
                    assert_eq!(status.name, "file");
                    assert_eq!(status.path, "/parent/dir/file");
                }
            }
            Err(curvine_error::FsError::FileNotFound(_)) => {}
            Err(error) => panic!("unexpected listing error: {error}"),
        }
    }
    worker.join().unwrap();
}

#[test]
fn listing_never_mixes_edges_during_rename() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("list-rename-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let file = fs.create("/dir/a", true).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let worker_fs = fs.clone();
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || {
        worker_barrier.wait();
        for _ in 0..200 {
            worker_fs
                .rename("/dir/a", "/dir/b", RenameFlags::empty())
                .unwrap();
            worker_fs
                .rename("/dir/b", "/dir/a", RenameFlags::empty())
                .unwrap();
        }
    });
    barrier.wait();
    for _ in 0..400 {
        for entries in [
            fs.list_status("/dir").unwrap(),
            fs.list_options("/dir", ListOptions::with_limit(1)).unwrap(),
        ] {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].id, file.id);
            assert!(entries[0].name == "a" || entries[0].name == "b");
            assert_eq!(entries[0].path, format!("/dir/{}", entries[0].name));
        }
    }
    worker.join().unwrap();
}
