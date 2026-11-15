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
use curvine_master::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use curvine_master::Master;
use curvine_model::RenameFlags;
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
fn exists_waits_for_file_deletion_publication() {
    use std::sync::mpsc;
    use std::time::Duration;

    let fs = test_fs("exists-delete-publication");
    let id = fs.create("/dir/file", true).unwrap().id;
    let guard = fs.fs_dir.view().unwrap();
    let inode = guard.store_ref().get_inode(id, None).unwrap();
    let mut batch = guard.store_ref().new_batch();
    batch.delete_inode(id).unwrap();
    batch.commit().unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx.send(worker_fs.exists("/dir/file")).unwrap();
    });
    started_rx.recv().unwrap();
    let early = result_rx.recv_timeout(Duration::from_millis(100));
    let mut batch = guard.store_ref().new_batch();
    batch.write_inode(&inode.unwrap()).unwrap();
    batch.commit().unwrap();
    drop(guard);
    let result = match early {
        Ok(result) => {
            worker.join().unwrap();
            panic!("exists observed an unpublished deletion: {result:?}");
        }
        Err(_) => result_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
    };
    worker.join().unwrap();
    assert!(result.unwrap());
}

#[test]
fn exists_reads_a_store_snapshot_with_a_shared_namespace_guard() {
    let fs = test_fs("shared-snapshot");
    fs.create("/dir/file", true).unwrap();
    let dir = fs.fs_dir.clone();
    let guard = dir.view().unwrap();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send((
            worker_fs.exists("/dir/file"),
            worker_fs.exists("/missing"),
            worker_fs.exists("/dir/"),
        ))
        .unwrap();
    });
    let result = rx.recv_timeout(std::time::Duration::from_secs(3));
    drop(guard);
    worker.join().unwrap();
    let (existing, missing, invalid) = result.unwrap();
    assert!(existing.unwrap());
    assert!(!missing.unwrap());
    assert!(invalid.is_err());
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
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let mut batch = guard.store_ref().new_batch();
        batch.delete_inode(file_id).unwrap();
        batch.delete_inode(parent_id).unwrap();
        batch.commit().unwrap();
    }

    let guard = fs.fs_dir.view().unwrap();
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
    let reader = fs.fs_dir.view().unwrap();
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
fn rename_noop_rejects_dangling_file_inode() {
    let fs = test_fs("rename-dangling-inode");
    let file_id = fs.create("/dir/file", true).unwrap().id;
    fs.link("/dir/file", "/dir/alias").unwrap();
    {
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let mut batch = guard.store_ref().new_batch();
        batch.delete_inode(file_id).unwrap();
        batch.commit().unwrap();
    }
    assert!(fs
        .rename("/dir/file", "/dir/file", RenameFlags::empty())
        .is_err());
    assert!(fs
        .rename("/dir/file", "/dir/alias", RenameFlags::empty())
        .is_err());
}

#[test]
fn delete_rejects_dangling_file_inode_after_lock_planning() {
    let fs = test_fs("delete-dangling-inode");
    let file_id = fs.create("/dir/file", true).unwrap().id;
    {
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let mut batch = guard.store_ref().new_batch();
        batch.delete_inode(file_id).unwrap();
        batch.commit().unwrap();
    }
    let error = fs.delete("/dir/file", false).unwrap_err();
    assert!(
        error.to_string().contains("Failed to load inode"),
        "{error}"
    );
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
fn reopen_existing_file_and_reject_invalid_paths() {
    use curvine_model::{CreateFileOpts, OpenFlags};

    let fs = test_fs("reopen-existing");
    fs.mkdir("/dir", false).unwrap();
    let original = fs.create("/dir/file", false).unwrap();
    let mut opts = CreateFileOpts::with_create(false);
    opts.client_name = "reopen-client".into();
    for _ in 0..2 {
        fs.open_file("/dir/file", opts.clone(), OpenFlags::WRONLY)
            .unwrap();
        fs.close_file("/dir/file", "reopen-client").unwrap();
    }
    for path in ["/dir/missing", "/dir", "/.Trash/.pending/hidden"] {
        assert!(fs.open_file(path, opts.clone(), OpenFlags::WRONLY).is_err());
    }
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/dir/file").unwrap().id, original.id);
}

#[test]
fn reopen_rejects_restore_admission_without_changing_writer_state() {
    use curvine_model::{CreateFileOpts, OpenFlags};

    let fs = test_fs("reopen-restore-admission");
    fs.create("/file", false).unwrap();
    fs.close_file("/file", "").unwrap();
    assert!(fs.file_status("/file").unwrap().is_complete);
    let restore = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();
    let result = fs.open_file(
        "/file",
        CreateFileOpts::with_create(false),
        OpenFlags::WRONLY,
    );
    assert!(result.unwrap_err().to_string().contains("snapshot restore"));
    drop(restore);
    assert!(fs.file_status("/file").unwrap().is_complete);
}

#[test]
fn truncate_open_and_create_overwrite_keep_inode_identity() {
    use curvine_model::{CreateFileOpts, OpenFlags};

    let fs = test_fs("truncate-open");
    let original = fs.create("/file", false).unwrap();
    let opts = CreateFileOpts::with_create(false);
    fs.open_file("/file", opts.clone(), OpenFlags::WRONLY | OpenFlags::TRUNC)
        .unwrap();
    fs.create_with_opts(
        "/file",
        opts,
        OpenFlags::WRONLY | OpenFlags::CREAT | OpenFlags::TRUNC,
    )
    .unwrap();
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let current = fs.file_status("/file").unwrap();
        assert_eq!((current.id, current.len), (original.id, 0));
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn reopen_commit_failure_does_not_publish_writer_state() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use curvine_model::{CreateFileOpts, OpenFlags};

    let fs = test_fs("reopen-commit-failure");
    let file = fs.create("/file", false).unwrap();
    fs.close_file("/file", "").unwrap();
    assert!(fs.file_status("/file").unwrap().is_complete);

    let runtime = FaultRuntime::process();
    let rule_id = format!("reopen-failure-{}", file.id);
    let rule = FaultRuleBuilder::named("master.metadata.writer.before_commit")
        .matches("inode_id", file.id)
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected reopen failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.open_file(
        "/file",
        CreateFileOpts::with_create(false),
        OpenFlags::WRONLY,
    );
    runtime.remove(&rule_id).unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected reopen failure"));
    assert!(fs.file_status("/file").unwrap().is_complete);
    fs.restore_from_rocksdb().unwrap();
    assert!(fs.file_status("/file").unwrap().is_complete);
}

#[test]
fn link_fallbacks_preserve_existing_entries() {
    let fs = test_fs("link-fallbacks");
    fs.create("/source", false).unwrap();
    fs.mkdir("/dir", false).unwrap();
    fs.link("/source", "/dir/existing").unwrap();
    assert!(fs.link("/source", "/dir/existing").is_err());
    assert!(fs.link("/missing", "/dir/alias").is_err());
    assert!(fs
        .symlink("/target", "/dir/existing", false, 0o777)
        .is_err());
    assert!(fs
        .symlink("/target", "/missing/alias", false, 0o777)
        .is_err());
    assert!(fs.link("/source", "/.Trash/.pending/hidden").is_err());
    assert!(fs
        .symlink("/target", "/.Trash/.pending/hidden", false, 0o777)
        .is_err());

    fs.link("/source", "/missing/alias").unwrap();
    let original = fs.file_status("/source").unwrap();
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let source = fs.file_status("/source").unwrap();
        let existing = fs.file_status("/dir/existing").unwrap();
        let nested = fs.file_status("/missing/alias").unwrap();
        assert_eq!((source.id, source.nlink), (original.id, original.nlink));
        assert_eq!(existing.id, original.id);
        assert_eq!(nested.id, original.id);
        assert!(!fs.exists("/dir/alias").unwrap());
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
fn symlink_rejects_restore_admission_without_publishing_entry() {
    let fs = test_fs("symlink-restore-admission");
    fs.mkdir("/dir", false).unwrap();
    let restore = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();
    let error = fs
        .symlink("/target", "/dir/link", false, 0o777)
        .unwrap_err();
    assert!(error.to_string().contains("snapshot restore"));
    drop(restore);
    assert!(!fs.exists("/dir/link").unwrap());
    fs.symlink("/target", "/dir/link", false, 0o777).unwrap();
    fs.restore_from_rocksdb().unwrap();
    assert!(fs.exists("/dir/link").unwrap());
}

#[test]
fn symlink_rejects_file_parent_and_survives_restore() {
    let fs = test_fs("symlink-file-parent");
    fs.create("/file", false).unwrap();
    assert!(fs.symlink("/target", "/file/link", false, 0o777).is_err());
    fs.mkdir("/dir", false).unwrap();
    fs.symlink("/target", "/dir/link", false, 0o777).unwrap();
    let link = fs.file_status("/dir/link").unwrap();
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/dir/link").unwrap().id, link.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn forced_symlink_store_failure_preserves_old_link() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("symlink-force-failure");
    fs.mkdir("/dir", false).unwrap();
    fs.symlink("/first", "/dir/link", false, 0o777).unwrap();
    let old_id = fs.file_status("/dir/link").unwrap().id;
    fs.link("/dir/link", "/dir/alias").unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = format!("symlink-force-{}", Utils::rand_str(12));
    let rule = FaultRuleBuilder::named("master.metadata.symlink.before_commit")
        .matches("child_name", "link")
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected symlink failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.symlink("/second", "/dir/link", true, 0o777);
    runtime.remove(&rule_id).unwrap();
    assert!(result.is_err());
    assert_eq!(fs.file_status("/dir/link").unwrap().id, old_id);
    assert_eq!(fs.file_status("/dir/alias").unwrap().nlink, 2);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/dir/link").unwrap().id, old_id);
    assert_eq!(fs.file_status("/dir/alias").unwrap().nlink, 2);
}

#[test]
fn forced_symlink_rejects_regular_file_target() {
    let fs = test_fs("symlink-force-regular-file");
    fs.mkdir("/dir", false).unwrap();
    fs.create("/dir/link", false).unwrap();
    assert!(fs.symlink("/second", "/dir/link", true, 0o777).is_err());
    assert_eq!(
        fs.file_status("/dir/link").unwrap().file_type,
        curvine_model::FileType::File
    );
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(
        fs.file_status("/dir/link").unwrap().file_type,
        curvine_model::FileType::File
    );
}

#[test]
fn forced_symlink_replacement_preserves_other_hard_links() {
    let fs = test_fs("symlink-force-hard-link");
    fs.mkdir("/dir", false).unwrap();
    fs.symlink("/first", "/dir/link", false, 0o777).unwrap();
    let old_id = fs.file_status("/dir/link").unwrap().id;
    fs.link("/dir/link", "/dir/alias").unwrap();
    fs.symlink("/second", "/dir/link", true, 0o777).unwrap();
    let new_id = fs.file_status("/dir/link").unwrap().id;
    assert_ne!(old_id, new_id);
    assert_eq!(fs.file_status("/dir/alias").unwrap().id, old_id);
    assert_eq!(fs.file_status("/dir/alias").unwrap().nlink, 1);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/dir/alias").unwrap().id, old_id);
    assert_eq!(fs.file_status("/dir/link").unwrap().id, new_id);
    fs.delete("/dir/link", false).unwrap();
    fs.delete("/dir/alias", false).unwrap();
    assert!(fs.file_status_by_id("/dir/link", Some(new_id)).is_err());
    assert!(fs.file_status_by_id("/dir/alias", Some(old_id)).is_err());
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

/// `rename` advances the namespace epoch that `exists_memory` validates across
/// its lock window. A reader running concurrently must therefore either observe
/// the pre-rename name or the post-rename one — and whenever it reports "yes",
/// the path must still resolve to the same inode (never a stale or a different
/// one). Mid-rename disagreement is allowed; a wrong identity is not.
#[test]
fn exists_never_reports_a_renamed_path_under_the_wrong_inode() {
    let fs = test_fs("exists-rename-epoch");
    fs.mkdir("/dir", true).unwrap();
    let created = fs.create("/dir/src", false).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let renamer = {
        let fs = fs.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            // A single renamer keeps the pair consistent: the inode lives under
            // exactly one of the two names between iterations.
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                fs.rename("/dir/src", "/dir/dst", RenameFlags::empty())
                    .unwrap();
                fs.rename("/dir/dst", "/dir/src", RenameFlags::empty())
                    .unwrap();
            }
        })
    };

    for _ in 0..1000 {
        for path in ["/dir/src", "/dir/dst"] {
            if fs.exists(path).unwrap() {
                // The rename may land between the two calls, so a lookup failure
                // here is expected; a *successful* lookup must be this inode.
                if let Ok(status) = fs.file_status(path) {
                    assert_eq!(
                        status.id, created.id,
                        "{path} must resolve to the created inode, not a stale one"
                    );
                }
            }
        }
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    renamer.join().unwrap();

    // Sequential contract after the concurrent phase: exactly one name holds it.
    let (src, dst) = (
        fs.exists("/dir/src").unwrap(),
        fs.exists("/dir/dst").unwrap(),
    );
    assert!(
        src != dst,
        "after quiescing, exactly one name must hold the inode (src={src}, dst={dst})"
    );
}
