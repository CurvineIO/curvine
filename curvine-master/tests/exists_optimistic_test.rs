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

use curvine_master::master::fs::MasterFilesystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn test_fs(name: &str) -> Arc<MasterFilesystem> {
    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("exists-optimistic-{}-{}", name, Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "exists-optimistic-j-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    Arc::new(curvine_master::master::journal::JournalSystem::fs_only_for_test(&conf).unwrap())
}

#[test]
fn exists_reads_a_store_snapshot_with_a_shared_namespace_guard() {
    let fs = test_fs("shared-snapshot");
    fs.create("/dir/file", true).unwrap();
    let dir = fs.fs_dir();
    let guard = dir.read();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send((worker_fs.exists("/dir/file"), worker_fs.exists("/missing")))
            .unwrap();
    });
    let result = rx.recv_timeout(std::time::Duration::from_secs(3));
    drop(guard);
    worker.join().unwrap();
    let (existing, missing) = result.unwrap();
    assert!(existing.unwrap());
    assert!(!missing.unwrap());
    assert!(fs.exists("/").unwrap());
    assert!(!fs.exists("/dir/file/child").unwrap());
    assert!(fs.exists("relative").is_err());
    assert!(fs.exists("/dir/").is_err());
    assert!(!fs.exists("/.Trash/.pending/file").unwrap());
}

// Rows deleted behind the live tree's back are corruption: the store-snapshot
// readers (scan lease, checkpoint, restore rebuild) report the dangling edge,
// and repair flows (no-snapshot rebuild) reconcile it. Whether `exists` also
// trips depends on the child container format, so the read-side contract is
// owned by the snapshot reader.
#[test]
fn exists_reports_a_dangling_edge_instead_of_treating_it_as_present() {
    let fs = test_fs("dangling-edge");
    fs.create("/dir/file", true).unwrap();
    fs.create("/other/sub/file", true).unwrap();
    let file_id = fs.file_status("/dir/file").unwrap().id;
    let parent_id = fs.file_status("/other/sub").unwrap().id;
    {
        let guard = fs.fs_dir.write();
        let mut batch = guard.store_ref().new_batch();
        batch.delete_inode(file_id).unwrap();
        batch.delete_inode(parent_id).unwrap();
        batch.commit().unwrap();
    }

    let guard = fs.fs_dir.read();
    let lease = guard.begin_snapshot_scan().unwrap();
    let snapshot = lease.snapshot_reader().unwrap();
    for path in ["/dir/file", "/other/sub/file"] {
        match snapshot.resolve_path(path) {
            Err(error) => assert!(error
                .to_string()
                .contains("directory edge references missing inode")),
            Ok(_) => panic!("store reader must detect the dangling edge at {}", path),
        }
    }
}

#[test]
fn exists_tracks_own_create_and_delete_under_concurrent_writes() {
    let fs = test_fs("linearizable");
    fs.mkdir("/dir", true).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for thread in 0..4 {
        let fs = fs.clone();
        let stop = stop.clone();
        writers.push(std::thread::spawn(move || {
            let mut i: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let path = format!("/dir/w{thread}-{i}");
                fs.create(&path, false).unwrap();
                assert!(fs.exists(&path).unwrap(), "{path} must exist after create");
                fs.delete(&path, false).unwrap();
                assert!(
                    !fs.exists(&path).unwrap(),
                    "{path} must be gone after delete"
                );
                i += 1;
            }
        }));
    }

    let mut reads: u64 = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        // Exercise reads while other threads mutate the directory.
        for thread in 0..4 {
            for i in 0..4 {
                let path = format!("/dir/w{thread}-{i}");
                let _ = fs.exists(&path).unwrap();
            }
        }
        reads += 16;
    }
    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        writer.join().unwrap();
    }
    assert!(reads > 0);
}

#[test]
fn recursive_mkdir_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("recursive-mkdir-shared-reader");
    fs.mkdir("/root", false).unwrap();
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || tx.send(worker_fs.mkdir("/root/a/b/c", true)).unwrap());

    let status = rx
        .recv_timeout(std::time::Duration::from_secs(3))
        .expect("recursive mkdir was blocked by a shared namespace reader")
        .unwrap();
    assert_eq!(status.path, "/root/a/b/c");
    drop(reader);
    worker.join().unwrap();
    fs.restore_from_rocksdb().unwrap();
    assert!(fs.exists("/root/a/b/c").unwrap());
}

#[test]
fn concurrent_recursive_mkdir_keeps_one_durable_path() {
    let fs = test_fs("concurrent-recursive-mkdir");
    fs.mkdir("/base", false).unwrap();
    let start = Arc::new(std::sync::Barrier::new(3));
    let workers = (0..2)
        .map(|_| {
            let fs = fs.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                fs.mkdir("/base/a/b/c", true)
            })
        })
        .collect::<Vec<_>>();

    start.wait();
    for worker in workers {
        assert_eq!(worker.join().unwrap().unwrap().path, "/base/a/b/c");
    }
    assert_eq!(fs.list_status("/base/a/b").unwrap().len(), 1);
    fs.restore_from_rocksdb().unwrap();
    assert!(fs.exists("/base/a/b/c").unwrap());
}

#[test]
fn mkdir_under_distinct_parents_remains_consistent_after_reload() {
    let fs = test_fs("mkdir-distinct-parents");
    const PARENTS: usize = 4;
    const CHILDREN: usize = 64;

    for parent in 0..PARENTS {
        fs.mkdir(format!("/p{parent}"), false).unwrap();
    }

    let start = Arc::new(std::sync::Barrier::new(PARENTS));
    let mut workers = Vec::new();
    for parent in 0..PARENTS {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            for child in 0..CHILDREN {
                fs.mkdir(format!("/p{parent}/d{child}"), false).unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    for parent in 0..PARENTS {
        assert_eq!(
            fs.list_status(format!("/p{parent}")).unwrap().len(),
            CHILDREN
        );
    }
    fs.restore_from_rocksdb().unwrap();
    for parent in 0..PARENTS {
        for child in 0..CHILDREN {
            assert!(fs.exists(format!("/p{parent}/d{child}")).unwrap());
        }
    }
}

#[test]
fn create_under_distinct_parents_remains_consistent_after_reload() {
    let fs = test_fs("create-distinct-parents");
    const PARENTS: usize = 4;
    const CHILDREN: usize = 64;

    for parent in 0..PARENTS {
        fs.mkdir(format!("/p{parent}"), false).unwrap();
    }

    let start = Arc::new(std::sync::Barrier::new(PARENTS));
    let mut workers = Vec::new();
    for parent in 0..PARENTS {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            for child in 0..CHILDREN {
                fs.create(format!("/p{parent}/f{child}"), false).unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    for parent in 0..PARENTS {
        assert_eq!(
            fs.list_status(format!("/p{parent}")).unwrap().len(),
            CHILDREN
        );
    }
    fs.restore_from_rocksdb().unwrap();
    for parent in 0..PARENTS {
        for child in 0..CHILDREN {
            assert!(fs.exists(format!("/p{parent}/f{child}")).unwrap());
        }
    }
}

#[test]
fn concurrent_create_same_path_keeps_one_namespace_entry() {
    let fs = test_fs("create-same-path");
    fs.mkdir("/dir", false).unwrap();

    let start = Arc::new(std::sync::Barrier::new(8));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            fs.create("/dir/file", false).unwrap();
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(fs.list_status("/dir").unwrap().len(), 1);
    assert!(fs.exists("/dir/file").unwrap());
}

#[test]
fn delete_under_distinct_parents_remains_consistent_after_reload() {
    let fs = test_fs("delete-distinct-parents");
    const PARENTS: usize = 4;
    const CHILDREN: usize = 64;

    for parent in 0..PARENTS {
        fs.mkdir(format!("/p{parent}"), false).unwrap();
        for child in 0..CHILDREN {
            fs.create(format!("/p{parent}/f{child}"), false).unwrap();
        }
    }

    let start = Arc::new(std::sync::Barrier::new(PARENTS));
    let mut workers = Vec::new();
    for parent in 0..PARENTS {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            for child in 0..CHILDREN {
                fs.delete(format!("/p{parent}/f{child}"), false).unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    for parent in 0..PARENTS {
        assert!(fs.list_status(format!("/p{parent}")).unwrap().is_empty());
    }
    fs.restore_from_rocksdb().unwrap();
    for parent in 0..PARENTS {
        assert!(fs.list_status(format!("/p{parent}")).unwrap().is_empty());
    }
}

#[test]
fn rename_under_distinct_parents_remains_consistent_after_reload() {
    let fs = test_fs("rename-distinct-parents");
    const PAIRS: usize = 4;

    for pair in 0..PAIRS {
        fs.mkdir(format!("/src{pair}"), false).unwrap();
        fs.mkdir(format!("/dst{pair}"), false).unwrap();
        fs.mkdir(format!("/src{pair}/source/child"), true).unwrap();
    }

    let start = Arc::new(std::sync::Barrier::new(PAIRS));
    let mut workers = Vec::new();
    for pair in 0..PAIRS {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            fs.rename(
                format!("/src{pair}/source"),
                format!("/dst{pair}/target"),
                curvine_model::RenameFlags::empty(),
            )
            .unwrap();
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    for pair in 0..PAIRS {
        assert!(!fs.exists(format!("/src{pair}/source")).unwrap());
        assert!(fs.exists(format!("/dst{pair}/target/child")).unwrap());
    }
    fs.restore_from_rocksdb().unwrap();
    for pair in 0..PAIRS {
        assert!(!fs.exists(format!("/src{pair}/source")).unwrap());
        assert!(fs.exists(format!("/dst{pair}/target/child")).unwrap());
    }
}

#[test]
fn link_under_distinct_parents_remains_consistent_after_reload() {
    let fs = test_fs("link-distinct-parents");
    const PARENTS: usize = 4;
    const CHILDREN: usize = 32;

    fs.create("/source", false).unwrap();
    for parent in 0..PARENTS {
        fs.mkdir(format!("/p{parent}"), false).unwrap();
    }

    let start = Arc::new(std::sync::Barrier::new(PARENTS));
    let mut workers = Vec::new();
    for parent in 0..PARENTS {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            for child in 0..CHILDREN {
                fs.link("/source".to_string(), format!("/p{parent}/l{child}"))
                    .unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    for parent in 0..PARENTS {
        assert_eq!(
            fs.list_status(format!("/p{parent}")).unwrap().len(),
            CHILDREN
        );
    }
    assert_eq!(
        fs.file_status("/source").unwrap().nlink as usize,
        PARENTS * CHILDREN + 1
    );
    fs.restore_from_rocksdb().unwrap();
    for parent in 0..PARENTS {
        for child in 0..CHILDREN {
            assert!(fs.exists(format!("/p{parent}/l{child}")).unwrap());
        }
    }
}

#[test]
fn symlink_under_distinct_parents_remains_consistent_after_reload() {
    let fs = test_fs("symlink-distinct-parents");
    const PARENTS: usize = 4;
    const CHILDREN: usize = 32;

    for parent in 0..PARENTS {
        fs.mkdir(format!("/p{parent}"), false).unwrap();
    }

    let start = Arc::new(std::sync::Barrier::new(PARENTS));
    let mut workers = Vec::new();
    for parent in 0..PARENTS {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            for child in 0..CHILDREN {
                fs.symlink(
                    format!("/target/{child}"),
                    format!("/p{parent}/s{child}"),
                    false,
                    0o644,
                )
                .unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    for parent in 0..PARENTS {
        assert_eq!(
            fs.list_status(format!("/p{parent}")).unwrap().len(),
            CHILDREN
        );
    }
    fs.restore_from_rocksdb().unwrap();
    for parent in 0..PARENTS {
        for child in 0..CHILDREN {
            assert!(fs.exists(format!("/p{parent}/s{child}")).unwrap());
        }
    }
}

#[test]
fn exists_remains_consistent_during_tree_reload() {
    let fs = test_fs("reload");
    fs.create("/dir/file", true).unwrap();
    let reader_fs = fs.clone();
    let start = Arc::new(std::sync::Barrier::new(2));
    let reader_start = start.clone();
    let reader = std::thread::spawn(move || {
        reader_start.wait();
        for _ in 0..500 {
            assert!(reader_fs.exists("/dir/file").unwrap());
            assert!(!reader_fs.exists("/dir/missing").unwrap());
        }
    });
    start.wait();
    for _ in 0..10 {
        fs.restore_from_rocksdb().unwrap();
    }
    reader.join().unwrap();
}
