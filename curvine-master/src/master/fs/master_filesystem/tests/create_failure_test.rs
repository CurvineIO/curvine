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

#[cfg(feature = "fault-injection")]
use super::*;

#[cfg(feature = "fault-injection")]
#[test]
fn link_replans_after_source_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("link-replaced-parent");
    let old_file = fs.create("/a/file", true).unwrap();
    fs.mkdir("/d", false).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "link-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.link_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old_file.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.link("/a/file", "/d/alias"));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let new_file = if planned {
        // Plain sharded identity change: no exclusive window, no recursive
        // delete — the parked worker is the only in-flight writer.
        fs.rename("/a", "/a-moved", RenameFlags::empty()).unwrap();
        fs.mkdir("/a", false).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    fs.delete("/a-moved", true).unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "link did not reach lock planning");
    let new_file = new_file.unwrap();
    assert_ne!(old_file.id, new_file.id);
    // The worker resolves /a/file after the driver's identity change, so the
    // alias points at the replacement inode (the by-path contract).
    result.unwrap();
    assert_eq!(fs.file_status("/d/alias").unwrap().id, new_file.id);
    fs.restore_from_rocksdb().unwrap();
    // Restore re-assigns inode ids: only existence and file-ness survive.
    let alias = fs.file_status("/d/alias");
    assert!(alias.is_ok());
    assert!(alias.unwrap().is_dir == false);
}

#[cfg(feature = "fault-injection")]
#[test]
fn create_commit_failure_does_not_publish_to_the_namespace_tree() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("create-commit-failure");
    fs.mkdir("/parent", false).unwrap();
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    let before = fs.file_status("/parent").unwrap();
    let name = format!("file-{}", curvine_runtime::common::Utils::rand_str(10));
    let path = format!("/parent/{name}");

    let runtime = FaultRuntime::process();
    let rule_id = format!("create-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.add.before_commit")
        .matches("child_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected create commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.create(&path, false);
    runtime.remove(&rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected create commit failure"));
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let parent = fs.file_status("/parent").unwrap();
        assert_eq!((parent.mtime, parent.nlink), (before.mtime, before.nlink));
        assert!(!fs.exists(&path).unwrap());
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn mkdir_commit_failure_does_not_publish_to_the_namespace_tree() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("mkdir-commit-failure");
    fs.mkdir("/parent", false).unwrap();
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    let before = fs.file_status("/parent").unwrap();
    let name = format!("dir-{}", curvine_runtime::common::Utils::rand_str(10));
    let path = format!("/parent/{name}");

    let runtime = FaultRuntime::process();
    let rule_id = format!("mkdir-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.add.before_commit")
        .matches("child_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected mkdir commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.mkdir(&path, false);
    runtime.remove(&rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected mkdir commit failure"));
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let parent = fs.file_status("/parent").unwrap();
        assert_eq!((parent.mtime, parent.nlink), (before.mtime, before.nlink));
        assert!(!fs.exists(&path).unwrap());
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn symlink_commit_failure_does_not_publish_to_the_namespace_tree() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("symlink-commit-failure");
    fs.mkdir("/parent", false).unwrap();
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    let before = fs.file_status("/parent").unwrap();
    let name = format!("link-{}", curvine_runtime::common::Utils::rand_str(10));
    let path = format!("/parent/{name}");

    let runtime = FaultRuntime::process();
    let rule_id = format!("symlink-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.symlink.before_commit")
        .matches("child_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected symlink commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.symlink("/target".to_string(), path.clone(), false, 0o644);
    runtime.remove(&rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected symlink commit failure"));
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let parent = fs.file_status("/parent").unwrap();
        assert_eq!((parent.mtime, parent.nlink), (before.mtime, before.nlink));
        assert!(!fs.exists(&path).unwrap());
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn link_commit_failure_does_not_publish_to_the_namespace_tree() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("link-commit-failure");
    fs.create("/source", false).unwrap();
    fs.mkdir("/parent", false).unwrap();
    let before_source = fs.file_status("/source").unwrap();
    let before_parent = fs.file_status("/parent").unwrap();
    let name = format!("entry-{}", curvine_runtime::common::Utils::rand_str(10));
    let path = format!("/parent/{name}");

    let runtime = FaultRuntime::process();
    let rule_id = format!("link-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.link.before_commit")
        .matches("child_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected link commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.link("/source".to_string(), path.clone());
    runtime.remove(&rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected link commit failure"));
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let source = fs.file_status("/source").unwrap();
        let parent = fs.file_status("/parent").unwrap();
        assert_eq!(source.nlink, before_source.nlink);
        assert_eq!(
            (parent.mtime, parent.nlink),
            (before_parent.mtime, before_parent.nlink)
        );
        assert!(!fs.exists(&path).unwrap());
    }
}
