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

use super::*;

#[cfg(feature = "fault-injection")]
#[test]
fn file_status_lock_window_excludes_rename_writers() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let fs = test_fs("file-status-lock-window");
    fs.mkdir("/p/a", true).unwrap();
    fs.mkdir("/q", true).unwrap();
    let before = fs.file_status("/p/a").unwrap();
    let runtime = FaultRuntime::process();
    let entry_id = "file-status-lock-entry";
    let arrival_id = "file-status-lock-arrival";
    runtime
        .configure(
            entry_id,
            FaultRuleBuilder::named("master.metadata.file_status.locked_build")
                .matches("path", "/p/a")
                .unwrap()
                .record()
                .unwrap(),
        )
        .unwrap();
    runtime
        .configure(
            arrival_id,
            FaultRuleBuilder::named("master.metadata.rename_planned")
                .matches("path", "/p/a")
                .unwrap()
                .record()
                .unwrap(),
        )
        .unwrap();
    let release = crate::master::fs::master_filesystem::memory_read_gates::install(
        "master.metadata.file_status.locked_build",
    );
    let reader_fs = fs.clone();
    let reader = std::thread::spawn(move || reader_fs.file_status("/p/a"));

    fn confirm(runtime: &FaultRuntime, id: &str, deadline: Instant) -> bool {
        while Instant::now() < deadline {
            if runtime
                .rule(id)
                .ok()
                .and_then(|rule| rule)
                .is_some_and(|rule| rule.executions > 0)
            {
                return true;
            }
            std::thread::yield_now();
        }
        false
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    if !confirm(&runtime, entry_id, deadline) {
        failures.push("reader never reached the locked build window".to_string());
    }
    let writer = if failures.is_empty() {
        let (done, done_rx) = mpsc::channel::<()>();
        let writer_fs = fs.clone();
        let handle = std::thread::spawn(move || {
            writer_fs
                .rename("/p/a", "/q/a", RenameFlags::empty())
                .unwrap();
            done.send(()).unwrap();
        });
        if !confirm(&runtime, arrival_id, deadline) {
            failures.push("rename writer never reached its lock plan".to_string());
        }
        let blocked = {
            std::thread::sleep(Duration::from_millis(50));
            done_rx.try_recv().is_err()
        };
        if !blocked {
            failures
                .push("rename completed while the locked read window held inode locks".to_string());
        }
        Some((handle, done_rx))
    } else {
        None
    };

    // Release, join and clean up before asserting so failures never leak
    // parked readers or fault rules.
    drop(release);
    let status = reader.join().unwrap().unwrap();
    if let Some((handle, done_rx)) = writer {
        handle.join().unwrap();
        drop(done_rx);
    }
    runtime.remove(entry_id).unwrap();
    runtime.remove(arrival_id).unwrap();

    assert!(failures.is_empty(), "{:?}", failures);
    assert_eq!(status.id, before.id);
    assert_eq!(status.mode, before.mode);
    assert!(fs.file_status("/p/a").is_err());
    assert_eq!(fs.file_status("/q/a").unwrap().id, before.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn list_options_lock_window_excludes_rename_writers() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let fs = test_fs("list-options-lock-window");
    fs.mkdir("/p", true).unwrap();
    fs.mkdir("/q", true).unwrap();
    fs.create("/p/f", false).unwrap();
    let before = fs.file_status("/p/f").unwrap();
    let runtime = FaultRuntime::process();
    let entry_id = "list-options-lock-entry";
    let arrival_id = "list-options-lock-arrival";
    runtime
        .configure(
            entry_id,
            FaultRuleBuilder::named("master.metadata.list_options.locked_build")
                .matches("path", "/p/f")
                .unwrap()
                .record()
                .unwrap(),
        )
        .unwrap();
    runtime
        .configure(
            arrival_id,
            FaultRuleBuilder::named("master.metadata.rename_planned")
                .matches("path", "/p/f")
                .unwrap()
                .record()
                .unwrap(),
        )
        .unwrap();
    let release = crate::master::fs::master_filesystem::memory_read_gates::install(
        "master.metadata.list_options.locked_build",
    );
    let reader_fs = fs.clone();
    let reader = std::thread::spawn(move || reader_fs.list_options("/p/f", ListOptions::default()));

    fn confirm(runtime: &FaultRuntime, id: &str, deadline: Instant) -> bool {
        while Instant::now() < deadline {
            if runtime
                .rule(id)
                .ok()
                .and_then(|rule| rule)
                .is_some_and(|rule| rule.executions > 0)
            {
                return true;
            }
            std::thread::yield_now();
        }
        false
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    if !confirm(&runtime, entry_id, deadline) {
        failures.push("reader never reached the locked build window".to_string());
    }
    let writer = if failures.is_empty() {
        let (done, done_rx) = mpsc::channel::<()>();
        let writer_fs = fs.clone();
        let handle = std::thread::spawn(move || {
            writer_fs
                .rename("/p/f", "/q/f", RenameFlags::empty())
                .unwrap();
            done.send(()).unwrap();
        });
        if !confirm(&runtime, arrival_id, deadline) {
            failures.push("rename writer never reached its lock plan".to_string());
        }
        let blocked = {
            std::thread::sleep(Duration::from_millis(50));
            done_rx.try_recv().is_err()
        };
        if !blocked {
            failures
                .push("rename completed while the locked read window held inode locks".to_string());
        }
        Some((handle, done_rx))
    } else {
        None
    };

    drop(release);
    let page = reader.join().unwrap().unwrap();
    if let Some((handle, done_rx)) = writer {
        handle.join().unwrap();
        drop(done_rx);
    }
    runtime.remove(entry_id).unwrap();
    runtime.remove(arrival_id).unwrap();

    assert!(failures.is_empty(), "{:?}", failures);
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].name, "f");
    assert_eq!(page[0].id, before.id);
    assert!(fs.list_options("/p/f", ListOptions::default()).is_err());
    assert_eq!(fs.file_status("/q/f").unwrap().id, before.id);
}

#[test]
fn moved_directory_ctime_matches_live_attributes() {
    let fs = test_fs("moved-directory-ctime");
    fs.mkdir("/left/a", true).unwrap();
    fs.mkdir("/right", true).unwrap();
    let a = {
        let dir = fs.fs_dir.read();
        let a = InodePath::resolve(dir.root_node(), "/left/a", dir.store_ref()).unwrap();
        a.get_last_dir_node().unwrap()
    };
    let a_before = a.directory_attributes().ctime;
    std::thread::sleep(std::time::Duration::from_millis(5));
    fs.rename("/left/a", "/right/b", RenameFlags::empty())
        .unwrap();
    let a_after = a.directory_attributes().ctime;
    assert!(a_after > a_before);
    assert_eq!(a_after, fs.file_status("/right/b").unwrap().ctime());
    std::thread::sleep(std::time::Duration::from_millis(5));
    fs.rename("/right/b", "/left/c", RenameFlags::empty())
        .unwrap();
    assert!(a.directory_attributes().ctime > a_after);
    assert_eq!(
        a.directory_attributes().ctime,
        fs.file_status("/left/c").unwrap().ctime()
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn rename_reloads_inode_after_a_hard_link_is_added() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("rename-reload-hardlink");
    let original = fs.create("/a/file", true).unwrap();
    for path in ["/b", "/c", "/d"] {
        fs.mkdir(path, false).unwrap();
    }
    fs.link("/a/file", "/b/alias").unwrap();
    let parent = fs.file_status("/a").unwrap().id;
    let runtime = FaultRuntime::process();
    let rule_id = "rename-reload-hardlink";
    let rule = FaultRuleBuilder::named("master.metadata.rename_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(parent);
    let worker_fs = fs.clone();
    let worker =
        std::thread::spawn(move || worker_fs.rename("/a/file", "/d/moved", RenameFlags::empty()));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let link_result = if planned {
        Some(fs.link("/b/alias", "/c/alias"))
    } else {
        None
    };
    drop(held);
    let rename_result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "rename did not reach lock planning");
    link_result.unwrap().unwrap();
    rename_result.unwrap();
    fs.delete("/b/alias", false).unwrap();
    fs.delete("/c/alias", false).unwrap();
    assert_eq!(fs.file_status("/d/moved").unwrap().id, original.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/d/moved").unwrap().id, original.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn rename_replans_after_source_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("rename-replaced-parent");
    let old_file = fs.create("/a/file", true).unwrap();
    fs.mkdir("/d", false).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "rename-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.rename_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old_file.id);
    let worker_fs = fs.clone();
    let worker =
        std::thread::spawn(move || worker_fs.rename("/a/file", "/d/moved", RenameFlags::empty()));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let new_file = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "rename did not reach lock planning");
    let new_file = new_file.unwrap();
    assert_ne!(old_file.id, new_file.id);
    result.unwrap();
    assert_eq!(fs.file_status("/d/moved").unwrap().id, new_file.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/d/moved").unwrap().id, new_file.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn rename_commit_failure_keeps_parent_metadata_and_tree() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("rename-commit-failure");
    let name = format!("source-{}", curvine_runtime::common::Utils::rand_str(10));
    let source_path = format!("/left/{name}");
    fs.mkdir(&format!("{source_path}/child"), true).unwrap();
    fs.mkdir("/right", false).unwrap();
    for path in ["/left", "/right"] {
        fs.set_attr(
            path,
            SetAttrOpts {
                mtime: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let before_left = fs.file_status("/left").unwrap();
    let before_right = fs.file_status("/right").unwrap();
    let before_source = fs.file_status(&source_path).unwrap();
    let source_node = {
        let dir = fs.fs_dir.read();
        InodePath::resolve(dir.root_node(), &source_path, dir.store_ref())
            .unwrap()
            .get_last_dir_node()
            .unwrap()
    };
    let before_attrs = source_node.directory_attributes();
    let runtime = FaultRuntime::process();
    let rule_id = format!("rename-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.rename.before_commit")
        .matches("source_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected rename commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.rename(source_path.as_str(), "/right/target", RenameFlags::empty());
    runtime.remove(&rule_id).unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected rename commit failure"));
    assert_eq!(source_node.directory_attributes(), before_attrs);
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let left = fs.file_status("/left").unwrap();
        let right = fs.file_status("/right").unwrap();
        assert_eq!(
            (left.mtime, left.nlink),
            (before_left.mtime, before_left.nlink)
        );
        assert_eq!(
            (right.mtime, right.nlink),
            (before_right.mtime, before_right.nlink)
        );
        assert_eq!(fs.file_status(&source_path).unwrap().id, before_source.id);
        assert_eq!(
            fs.file_status(&source_path).unwrap().ctime(),
            before_source.ctime()
        );
        assert!(fs.exists(&format!("{source_path}/child")).unwrap());
        assert!(!fs.exists("/right/target").unwrap());
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn same_parent_rename_commit_failure_preserves_tree_and_reloads() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("same-parent-rename-commit-failure");
    fs.mkdir("/parent/source/child", true).unwrap();
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    let parent_before = fs.file_status("/parent").unwrap();
    let source_before = fs.file_status("/parent/source").unwrap();

    let runtime = FaultRuntime::process();
    let rule_id = "same-parent-rename-failure";
    let rule = FaultRuleBuilder::named("master.metadata.rename.before_commit")
        .matches("source_name", "source")
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected same-parent rename failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.rename("/parent/source", "/parent/target", RenameFlags::empty());
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected same-parent rename failure"));
    assert_eq!(
        fs.file_status("/parent").unwrap().mtime,
        parent_before.mtime
    );
    assert_eq!(
        fs.file_status("/parent/source").unwrap().id,
        source_before.id
    );
    assert!(fs.exists("/parent/source/child").unwrap());
    assert!(!fs.exists("/parent/target").unwrap());

    fs.restore_from_rocksdb().unwrap();
    assert_eq!(
        fs.file_status("/parent/source").unwrap().id,
        source_before.id
    );
    assert!(fs.exists("/parent/source/child").unwrap());

    fs.rename("/parent/source", "/parent/target", RenameFlags::empty())
        .unwrap();
    assert_eq!(
        fs.file_status("/parent/target").unwrap().id,
        source_before.id
    );
    assert!(fs.exists("/parent/target/child").unwrap());
    assert!(!fs.exists("/parent/source").unwrap());

    fs.restore_from_rocksdb().unwrap();
    assert_eq!(
        fs.file_status("/parent/target").unwrap().id,
        source_before.id
    );
    assert!(fs.exists("/parent/target/child").unwrap());
}

#[cfg(feature = "fault-injection")]
#[test]
fn exchange_commit_failure_preserves_parents_and_subtree() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("exchange-commit-failure");
    let name = format!("source-{}", curvine_runtime::common::Utils::rand_str(10));
    let source = format!("/left/{name}");
    fs.mkdir(&format!("{source}/child"), true).unwrap();
    fs.create("/right/target", true).unwrap();
    for path in ["/left", "/right"] {
        fs.set_attr(
            path,
            SetAttrOpts {
                mtime: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let left_before = fs.file_status("/left").unwrap();
    let right_before = fs.file_status("/right").unwrap();
    let source_before = fs.file_status(&source).unwrap();
    let target_before = fs.file_status("/right/target").unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = format!("exchange-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.exchange.before_commit")
        .matches("source_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected exchange commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.rename(source.as_str(), "/right/target", RenameFlags::EXCHANGE);
    runtime.remove(&rule_id).unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected exchange commit failure"));
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let left = fs.file_status("/left").unwrap();
        let right = fs.file_status("/right").unwrap();
        assert_eq!(
            (left.mtime, left.nlink),
            (left_before.mtime, left_before.nlink)
        );
        assert_eq!(
            (right.mtime, right.nlink),
            (right_before.mtime, right_before.nlink)
        );
        assert_eq!(fs.file_status(&source).unwrap().id, source_before.id);
        assert!(fs.exists(&format!("{source}/child")).unwrap());
        assert_eq!(
            fs.file_status("/right/target").unwrap().id,
            target_before.id
        );
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn replace_rename_commit_failure_preserves_targets() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    for directory in [false, true] {
        let fs = test_fs("replace-rename-commit-failure");
        let name = format!("source-{}", curvine_runtime::common::Utils::rand_str(10));
        let source = format!("/left/{name}");
        if directory {
            fs.mkdir(&format!("{source}/child"), true).unwrap();
            fs.mkdir("/right/target", true).unwrap();
        } else {
            fs.create(&source, true).unwrap();
            fs.create("/right/target", true).unwrap();
            fs.link("/right/target", "/right/alias").unwrap();
        }
        let source_before = fs.file_status(&source).unwrap();
        let target_before = fs.file_status("/right/target").unwrap();
        let left_before = fs.file_status("/left").unwrap();
        let right_before = fs.file_status("/right").unwrap();
        let runtime = FaultRuntime::process();
        let rule_id = format!("replace-failure-{name}");
        let rule = FaultRuleBuilder::named("master.metadata.rename.before_commit")
            .matches("source_name", name.as_str())
            .unwrap()
            .times(1)
            .unwrap()
            .return_error("injected replace commit failure")
            .unwrap();
        runtime.configure(&rule_id, rule).unwrap();
        let result = fs.rename(source.as_str(), "/right/target", RenameFlags::empty());
        runtime.remove(&rule_id).unwrap();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("injected replace commit failure"));
        for restored in [false, true] {
            if restored {
                fs.restore_from_rocksdb().unwrap();
            }
            assert_eq!(fs.file_status(&source).unwrap().id, source_before.id);
            let target = fs.file_status("/right/target").unwrap();
            assert_eq!(
                (target.id, target.nlink),
                (target_before.id, target_before.nlink)
            );
            let left = fs.file_status("/left").unwrap();
            let right = fs.file_status("/right").unwrap();
            assert_eq!(
                (left.mtime, left.nlink),
                (left_before.mtime, left_before.nlink)
            );
            assert_eq!(
                (right.mtime, right.nlink),
                (right_before.mtime, right_before.nlink)
            );
            if directory {
                assert!(fs.exists(&format!("{source}/child")).unwrap());
            } else {
                assert_eq!(fs.file_status("/right/alias").unwrap().id, target_before.id);
            }
        }
    }
}

#[test]
fn exchange_preserves_parent_links_and_subtrees_after_restore() {
    for same_parent in [false, true] {
        for source_dir in [false, true] {
            for target_dir in [false, true] {
                let fs = test_fs("exchange-parent-links");
                let source = "/left/source";
                let target = if same_parent {
                    "/left/target"
                } else {
                    "/right/target"
                };
                if source_dir {
                    fs.mkdir(&format!("{source}/child"), true).unwrap();
                } else {
                    fs.create(source, true).unwrap();
                }
                if target_dir {
                    fs.mkdir(&format!("{target}/child"), true).unwrap();
                } else {
                    fs.create(target, true).unwrap();
                }
                let source_id = fs.file_status(source).unwrap().id;
                let target_id = fs.file_status(target).unwrap().id;
                let left_before = fs.file_status("/left").unwrap().nlink;
                let right_before = (!same_parent).then(|| fs.file_status("/right").unwrap().nlink);
                fs.rename(source, target, RenameFlags::EXCHANGE).unwrap();
                let delta = i32::from(target_dir) - i32::from(source_dir);
                for restored in [false, true] {
                    if restored {
                        fs.restore_from_rocksdb().unwrap();
                    }
                    assert_eq!(fs.file_status(source).unwrap().id, target_id);
                    assert_eq!(fs.file_status(target).unwrap().id, source_id);
                    assert_eq!(
                        fs.file_status("/left").unwrap().nlink as i32,
                        left_before as i32 + if same_parent { 0 } else { delta }
                    );
                    if let Some(before) = right_before {
                        assert_eq!(
                            fs.file_status("/right").unwrap().nlink as i32,
                            before as i32 - delta
                        );
                    }
                    if source_dir {
                        assert!(fs.exists(&format!("{target}/child")).unwrap());
                    }
                    if target_dir {
                        assert!(fs.exists(&format!("{source}/child")).unwrap());
                    }
                }
            }
        }
    }
}

#[test]
fn rename_uses_source_edge_when_directory_metadata_name_is_stale() {
    let fs = test_fs("rename-stale-directory-name");
    fs.mkdir("/source/child", true).unwrap();
    let source = fs.file_status("/source").unwrap();
    {
        let dir = fs.fs_dir.write();
        let path = InodePath::resolve(dir.root_node(), "/source", &dir.store).unwrap();
        let mut inode = path.get_last_inode().unwrap();
        inode.change_name("stale-name".to_string());
    }
    fs.rename("/source", "/target", RenameFlags::empty())
        .unwrap();
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        assert!(!fs.exists("/source").unwrap());
        assert_eq!(fs.file_status("/target").unwrap().id, source.id);
        assert!(fs.exists("/target/child").unwrap());
    }
}

#[test]
fn rename_rejects_long_packed_name_before_mutation() {
    let fs = test_fs("rename-long-packed-name");
    let original = fs.create("/source", true).unwrap();
    for name in ["x".repeat(256), "界".repeat(86)] {
        let destination = format!("/{name}");
        assert!(fs
            .rename("/source", &destination, RenameFlags::empty())
            .is_err());
        assert_eq!(fs.file_status("/source").unwrap().id, original.id);
        assert!(!fs.exists(&destination).unwrap());
    }
    let dir = fs.fs_dir.read();
    let (_, restored) = dir.store.create_tree().unwrap();
    let root = restored.children_handle().unwrap();
    assert_eq!(root.get_child_edge("source").unwrap().id(), original.id);
    assert!(root.get_child_edge(&"x".repeat(256)).is_none());
    assert!(root.get_child_edge(&"界".repeat(86)).is_none());
}

#[test]
fn oversized_child_does_not_modify_parent() {
    let fs = test_fs("oversized-child-no-parent-mutation");
    fs.mkdir("/parent", false).unwrap();
    let source = fs.create("/source", false).unwrap();
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    let destination = format!("/parent/{}", "x".repeat(256));
    assert!(fs.create(&destination, false).is_err());
    assert_eq!(fs.file_status("/parent").unwrap().mtime, 1);
    assert!(fs.symlink("/source", &destination, false, 0o777).is_err());
    assert_eq!(fs.file_status("/parent").unwrap().mtime, 1);
    assert!(fs.link("/source", &destination).is_err());
    assert_eq!(fs.file_status("/parent").unwrap().mtime, 1);
    assert_eq!(fs.file_status("/source").unwrap().id, source.id);
    assert!(!fs.exists(&destination).unwrap());
}

#[test]
fn rename_between_hard_links_is_a_noop() {
    let fs = test_fs("rename-same-inode");
    fs.create("/source", false).unwrap();
    fs.link("/source", "/target").unwrap();
    let before = fs.file_status("/source").unwrap();
    assert_eq!(before.nlink, 2);
    assert!(fs
        .rename("/source", "/target", RenameFlags::NO_REPLACE)
        .is_err());
    assert!(fs
        .rename("/source", "/target", RenameFlags::empty())
        .unwrap());
    let source = fs.file_status("/source").unwrap();
    let target = fs.file_status("/target").unwrap();
    assert_eq!((source.id, source.nlink), (before.id, 2));
    assert_eq!((target.id, target.nlink), (before.id, 2));
    let dir = fs.fs_dir.read();
    let (_, root) = dir.store.create_tree().unwrap();
    let root = root.children_handle().unwrap();
    assert_eq!(root.get_child_edge("source").unwrap().id(), before.id);
    assert_eq!(root.get_child_edge("target").unwrap().id(), before.id);
    assert_eq!(
        dir.store
            .get_inode(before.id, None)
            .unwrap()
            .unwrap()
            .nlink()
            .unwrap(),
        2
    );
}

#[test]
fn replace_rename_returns_replaced_file_blocks() {
    let fs = test_fs("replace-rename-blocks");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);
    fs.create("/source", false).unwrap();
    fs.create_with_opts(
        "/target",
        CreateFileOptsBuilder::new()
            .create_parent(false)
            .replicas(1)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let block = fs
        .add_block(
            "/target",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    let block_id = block.block.id;
    let location = BlockLocation::with_id(100);
    fs.fs_dir
        .write()
        .add_block_location(block_id, location.clone())
        .unwrap();
    fs.complete_file(
        "/target",
        None,
        4096,
        vec![CommitBlock {
            block_id,
            block_len: 4096,
            locations: vec![location],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    let dir = fs.fs_dir.write();
    let source = InodePath::resolve(dir.root_node(), "/source", &dir.store).unwrap();
    let target = InodePath::resolve(dir.root_node(), "/target", &dir.store).unwrap();
    let deleted = dir
        .rename(&source, &target, RenameFlags::empty())
        .unwrap()
        .unwrap();
    assert_eq!(deleted.inodes, 1);
    assert_eq!(deleted.bytes, 4096);
    assert_eq!(deleted.blocks.get(&block_id).unwrap().len(), 1);
}

#[test]
fn rename_over_hard_link_preserves_other_link_after_restore() {
    let fs = test_fs("rename-over-hard-link-restore");
    let source = fs.create("/source", false).unwrap();
    let target = fs.create("/target", false).unwrap();
    fs.link("/target", "/alias").unwrap();
    assert_eq!(fs.file_status("/alias").unwrap().nlink, 2);
    fs.rename("/source", "/target", RenameFlags::empty())
        .unwrap();

    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        assert!(!fs.exists("/source").unwrap());
        assert_eq!(fs.file_status("/target").unwrap().id, source.id);
        let alias = fs.file_status("/alias").unwrap();
        assert_eq!((alias.id, alias.nlink), (target.id, 1));
    }
}

#[test]
fn rename_over_sibling_directory_decrements_parent_link_once() {
    let fs = test_fs("rename-over-sibling-directory");
    fs.mkdir("/parent/source/child", true).unwrap();
    fs.mkdir("/parent/target", true).unwrap();
    let parent = fs.file_status("/parent").unwrap();
    let source = fs.file_status("/parent/source").unwrap();
    fs.rename("/parent/source", "/parent/target", RenameFlags::empty())
        .unwrap();
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        assert_eq!(fs.file_status("/parent").unwrap().nlink, parent.nlink - 1);
        assert_eq!(fs.file_status("/parent/target").unwrap().id, source.id);
        assert!(fs.exists("/parent/target/child").unwrap());
        assert!(!fs.exists("/parent/source").unwrap());
    }
}

#[test]
fn rename_over_empty_directory_preserves_parent_links_after_restore() {
    let fs = test_fs("rename-over-directory-restore");
    fs.mkdir("/left/src/child", true).unwrap();
    fs.mkdir("/right/dst", true).unwrap();
    let source = fs.file_status("/left/src").unwrap();
    let child = fs.file_status("/left/src/child").unwrap();
    let left_before = fs.file_status("/left").unwrap().nlink;
    let right_before = fs.file_status("/right").unwrap().nlink;
    fs.rename("/left/src", "/right/dst", RenameFlags::empty())
        .unwrap();

    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        assert!(!fs.exists("/left/src").unwrap());
        assert_eq!(fs.file_status("/right/dst").unwrap().id, source.id);
        assert_eq!(fs.file_status("/right/dst/child").unwrap().id, child.id);
        assert_eq!(fs.file_status("/left").unwrap().nlink, left_before - 1);
        assert_eq!(fs.file_status("/right").unwrap().nlink, right_before);
    }
}

#[test]
fn ttl_does_not_delete_replacement_of_hard_link_path() {
    let fs = test_fs("ttl-hard-link-replaced-path");
    let mut opts = CreateFileOpts::with_create(false);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1,
        ttl_action: TtlAction::Delete,
        ..Default::default()
    };
    let target = fs
        .create_with_opts("/target", opts, OpenFlags::new_create())
        .unwrap();
    fs.complete_file("/target", Some(target.id), 0, vec![], "", false, None)
        .unwrap();
    fs.link("/target", "/alias").unwrap();
    let source = fs.create("/source", false).unwrap();
    fs.rename("/source", "/target", RenameFlags::empty())
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let executor = crate::master::meta::inode::ttl::InodeTtlExecutor::with_managers(fs.clone());
    let (handled, _, _) = executor.execute_by_id(target.id).unwrap();
    assert!(!handled);
    assert_eq!(fs.file_status("/target").unwrap().id, source.id);
    assert_eq!(fs.file_status("/alias").unwrap().id, target.id);
}

#[test]
fn ttl_free_does_not_release_replacement_of_hard_link_path() {
    let fs = test_fs("ttl-free-hard-link-replaced-path");
    let mut opts = CreateFileOpts::with_create(false);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1,
        ttl_action: TtlAction::Free,
        ..Default::default()
    };
    let target = fs
        .create_with_opts("/target", opts, OpenFlags::new_create())
        .unwrap();
    fs.complete_file("/target", Some(target.id), 0, vec![], "", false, None)
        .unwrap();
    fs.link("/target", "/alias").unwrap();
    let source = fs.create("/source", false).unwrap();
    fs.rename("/source", "/target", RenameFlags::empty())
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let executor = crate::master::meta::inode::ttl::InodeTtlExecutor::with_managers(fs.clone());
    let (handled, _, _) = executor.execute_by_id(target.id).unwrap();
    assert!(!handled);
    assert_eq!(fs.file_status("/target").unwrap().id, source.id);
    assert_eq!(fs.file_status("/alias").unwrap().id, target.id);
}
