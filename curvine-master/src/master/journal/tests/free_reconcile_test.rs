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

//! Object verification used before an fs_mode release.

use crate::master::common_ancestor;
use crate::master::fs::FsModeSnapshot;
use crate::master::journal::free_reconcile::{object_holds, FreeReconciler};
use crate::master::journal::JournalSystem;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_model::{
    FileGen, FileStatus, MountOptions, SetAttrOptsBuilder, StorageState, TtlAction, WriteType,
    CV_GEN_METADATA_KEY,
};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use std::collections::HashMap;
use std::sync::Arc;

fn snapshot() -> FsModeSnapshot {
    FsModeSnapshot {
        path: "/mnt/a".to_string(),
        gen: FileGen::new(7, 10, 99, 12),
        state: StorageState::Both,
        ufs_mtime: 5_000,
        quiescent: true,
    }
}

fn object(len: i64, mtime: i64, cv_gen: Option<&str>) -> FileStatus {
    let mut x_attr = HashMap::new();
    if let Some(gen) = cv_gen {
        x_attr.insert(CV_GEN_METADATA_KEY.to_string(), gen.as_bytes().to_vec());
    }
    FileStatus {
        len,
        mtime,
        x_attr,
        ..Default::default()
    }
}

#[test]
fn object_store_requires_the_exact_generation() {
    let snap = snapshot();
    assert!(object_holds(
        &object(10, 1, Some("7:10:99:12")),
        &snap,
        true
    ));
    // Another version that happens to share inode, length and mtime.
    assert!(!object_holds(
        &object(10, 1, Some("7:10:99:13")),
        &snap,
        true
    ));
    // No generation: never trusted, even with a matching mtime.
    assert!(!object_holds(&object(10, 5_000, None), &snap, true));
    assert!(!object_holds(
        &object(11, 1, Some("7:10:99:12")),
        &snap,
        true
    ));
}

#[test]
fn rename_capable_backends_compare_the_recorded_mtime() {
    let snap = snapshot();
    assert!(object_holds(&object(10, 5_000, None), &snap, false));
    assert!(!object_holds(&object(10, 5_001, None), &snap, false));
    assert!(!object_holds(&object(9, 5_000, None), &snap, false));
}

#[test]
fn a_directory_never_holds_a_file_version() {
    let mut dir = object(10, 5_000, Some("7:10:99:12"));
    dir.is_dir = true;
    assert!(!object_holds(&dir, &snapshot(), true));
    assert!(!object_holds(&dir, &snapshot(), false));
}

#[test]
fn reconciliation_uses_and_releases_its_own_runtime() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("free-runtime-{}", Utils::rand_str(8)));
    let js = JournalSystem::from_conf(&conf).unwrap();
    let manager = js.job_manager();
    let manager_count = Arc::strong_count(&manager);
    let reconciler = FreeReconciler::start(manager.clone());
    assert_eq!(Arc::strong_count(&manager), manager_count);
    let runtime = Arc::downgrade(&reconciler.runtime);
    let task = reconciler
        .runtime
        .spawn(async { std::thread::current().name().unwrap().to_owned() });
    let name = AsyncRuntime::single().block_on(task).unwrap();
    assert_eq!(name, "free-reconcile");
    drop(reconciler);
    assert!(runtime.upgrade().is_none(), "runtime must not form a cycle");
    drop(manager);
    js.shutdown();
}

#[test]
fn cleanup_removes_only_files_without_cv_metadata() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.transfer.enabled = true;
    conf.change_test_meta_dir(format!("free-cleanup-{}", Utils::rand_str(8)));
    let js = JournalSystem::from_conf(&conf).unwrap();
    let root = std::env::temp_dir().join(format!("curvine-free-cleanup-{}", Utils::uuid()));
    std::fs::create_dir_all(root.join("orphan-dir")).unwrap();
    let mount = MountOptions::builder()
        .write_type(WriteType::FsMode)
        .build();
    js.mount_manager()
        .mount(None, "/mnt", &format!("file://{}/", root.display()), &mount)
        .unwrap();
    let fs = js.fs();
    fs.create("/mnt/cv-file", true).unwrap();
    fs.complete_file("/mnt/cv-file", None, 0, vec![], "", false, None)
        .unwrap();
    fs.set_attr(
        "/mnt/cv-file",
        SetAttrOptsBuilder::new()
            .ufs_mtime(1)
            .ttl_action(TtlAction::Free)
            .build(),
    )
    .unwrap();
    let status = fs.file_status("/mnt/cv-file").unwrap();
    let snapshot = fs.fs_mode_snapshot(status.id).unwrap().unwrap();
    assert!(fs
        .release_verified(status.id, &snapshot.path, snapshot.gen, snapshot.ufs_mtime)
        .unwrap());
    assert_eq!(
        fs.file_status("/mnt/cv-file").unwrap().storage_policy.state,
        StorageState::Ufs
    );
    std::fs::write(root.join("cv-file"), b"keep").unwrap();
    std::fs::write(root.join("orphan"), b"remove").unwrap();
    std::fs::write(root.join("orphan-dir/child"), b"remove").unwrap();

    let reconciler = FreeReconciler::start(js.job_manager());
    AsyncRuntime::single()
        .block_on(reconciler.cleanup_ufs_only("/mnt"))
        .unwrap();

    assert!(
        root.join("cv-file").exists(),
        "metadata survives Free and protects its UFS file"
    );
    assert!(!root.join("orphan").exists());
    assert!(!root.join("orphan-dir").exists());

    // An actual recursive Free also schedules the cleanup on the shared queue,
    // even when every CV file in the directory has already been released.
    let queued_orphan = root.join("queued-orphan");
    std::fs::write(&queued_orphan, b"remove").unwrap();
    fs.free("/mnt", true).unwrap();
    for _ in 0..100 {
        if !queued_orphan.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(!queued_orphan.exists());
    assert!(root.join("cv-file").exists());
    js.shutdown();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn common_ancestor_of_export_paths() {
    let one = ["s3://bkt/mnt/a"];
    assert_eq!(common_ancestor(one.into_iter()), "s3://bkt/mnt/a");
    let two = ["s3://bkt/mnt/x/a", "s3://bkt/mnt/y/b"];
    assert_eq!(common_ancestor(two.into_iter()), "s3://bkt/mnt");
    let cv = ["/mnt/a", "/other/b"];
    assert_eq!(common_ancestor(cv.into_iter()), "/");
}
