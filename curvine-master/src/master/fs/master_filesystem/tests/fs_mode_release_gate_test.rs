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

//! The fs_mode release gate on `MasterFilesystem::free_or_defer`, shared by TTL
//! Free and `cv free`, and the verified release that reconciliation uses.
//!
//! fs_mode reads a released file from UFS by its current path, so under
//! transfer mode nothing may be released before its UFS object is verified.

use super::*;
use crate::master::journal::FsModeReleaser;

#[derive(Default)]
struct RecordingReleaser {
    /// Paths under this prefix are fs_mode.
    fs_mode_prefix: String,
    /// Mount points reported by `contains_fs_mode_mount`.
    mounts: Vec<String>,
    deferred: std::sync::Mutex<Vec<i64>>,
    cleanups: std::sync::Mutex<Vec<String>>,
    fail_cleanup: bool,
}

impl RecordingReleaser {
    fn deferring() -> Self {
        Self {
            fs_mode_prefix: "/".to_string(),
            ..Default::default()
        }
    }

    fn with_mount(mount: &str) -> Self {
        Self {
            fs_mode_prefix: mount.to_string(),
            mounts: vec![mount.to_string()],
            ..Default::default()
        }
    }

    fn recorded(&self) -> Vec<i64> {
        self.deferred.lock().expect("lock").clone()
    }
}

impl FsModeReleaser for RecordingReleaser {
    fn is_fs_mode(&self, path: &str) -> bool {
        !self.fs_mode_prefix.is_empty() && path.starts_with(&self.fs_mode_prefix)
    }

    fn contains_fs_mode_mount(&self, path: &str) -> bool {
        let prefix = format!("{}/", path.trim_end_matches('/'));
        self.mounts.iter().any(|m| m.starts_with(&prefix))
    }

    fn defer_release(&self, inode_id: i64) {
        self.deferred.lock().expect("lock").push(inode_id);
    }

    fn defer_cleanup(&self, path: &str) -> FsResult<()> {
        if self.fail_cleanup {
            return Err(FsError::common("cleanup queue full"));
        }
        self.cleanups.lock().expect("lock").push(path.to_owned());
        Ok(())
    }
}

/// A completed file marked `Both`, i.e. eligible for a release.
fn releasable_file(fs: &MasterFilesystem, path: &str) -> i64 {
    for worker_id in [100, 200] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);
    }

    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();

    let block = fs
        .add_block(
            path,
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    let block_id = block.block.id;
    fs.fs_dir
        .write()
        .add_block_location(block_id, BlockLocation::with_id(100))
        .unwrap();

    let commit = CommitBlock {
        block_id,
        block_len: 4096,
        locations: vec![BlockLocation::with_id(100)],
    };
    fs.complete_file(path, None, 4096, vec![commit], "", false, None)
        .unwrap();

    let mtime = fs.file_status(path).unwrap().mtime;
    fs.set_attr(
        path,
        SetAttrOptsBuilder::new()
            .ufs_mtime(mtime + 1_000)
            .ttl_action(TtlAction::Free)
            .build(),
    )
    .unwrap();

    let status = fs.file_status(path).unwrap();
    assert_eq!(status.storage_policy.state, StorageState::Both);
    status.id
}

#[test]
fn a_deferred_free_releases_nothing_and_records_the_candidate() {
    let fs = test_fs("fs-mode-free-gate-defer");
    let inode_id = releasable_file(&fs, "/deferred.log");

    let releaser = Arc::new(RecordingReleaser::deferring());
    fs.set_fs_mode_releaser(releaser.clone());

    let freed = fs.free("/deferred.log", false).unwrap();
    assert_eq!(
        freed.inodes, 0,
        "nothing may be released before verification"
    );

    assert_eq!(releaser.recorded(), vec![inode_id]);
    assert_eq!(
        fs.file_status("/deferred.log")
            .unwrap()
            .storage_policy
            .state,
        StorageState::Both,
        "the Curvine copy must survive a deferred release"
    );
}

#[test]
fn deferred_free_rejects_missing_and_root_paths() {
    let fs = test_fs("fs-mode-free-gate-invalid");
    fs.set_fs_mode_releaser(Arc::new(RecordingReleaser::deferring()));
    assert!(fs.free("/missing", false).is_err());
    assert!(fs.free("/", true).is_err());
}

#[test]
fn a_recursive_deferred_free_records_every_file_under_the_directory() {
    let fs = test_fs("fs-mode-free-gate-recursive");
    let first = releasable_file(&fs, "/tree/a.log");
    let second = releasable_file(&fs, "/tree/b.log");

    let releaser = Arc::new(RecordingReleaser::deferring());
    fs.set_fs_mode_releaser(releaser.clone());

    fs.free("/tree", true).unwrap();

    let mut recorded = releaser.recorded();
    recorded.sort_unstable();
    let mut expected = vec![first, second];
    expected.sort_unstable();
    assert_eq!(recorded, expected);
    assert_eq!(*releaser.cleanups.lock().unwrap(), vec!["/tree"]);
}

#[test]
fn an_empty_recursive_directory_is_scheduled_for_cleanup() {
    let fs = test_fs("fs-mode-free-empty-directory");
    fs.mkdir("/empty", true).unwrap();
    let releaser = Arc::new(RecordingReleaser::deferring());
    fs.set_fs_mode_releaser(releaser.clone());

    fs.free("/empty", true).unwrap();
    assert_eq!(*releaser.cleanups.lock().unwrap(), vec!["/empty"]);
}

#[test]
fn recursive_free_reports_cleanup_enqueue_failure() {
    let fs = test_fs("fs-mode-free-cleanup-queue-full");
    fs.mkdir("/empty", true).unwrap();
    let releaser = RecordingReleaser {
        fs_mode_prefix: "/".to_owned(),
        fail_cleanup: true,
        ..Default::default()
    };
    fs.set_fs_mode_releaser(Arc::new(releaser));
    assert!(fs.free("/empty", true).is_err());
}

#[test]
fn a_release_without_the_gate_still_frees_directly() {
    // Transfer disabled: no gate installed, legacy behavior must be unchanged.
    let fs = test_fs("fs-mode-free-gate-off");
    releasable_file(&fs, "/direct.log");

    let freed = fs.free("/direct.log", false).unwrap();

    assert_eq!(freed.inodes, 1);
    assert_eq!(
        fs.file_status("/direct.log").unwrap().storage_policy.state,
        StorageState::Ufs
    );
}

#[test]
fn a_recursive_free_above_an_fs_mode_mount_is_refused() {
    let fs = test_fs("fs-mode-free-gate-parent");
    releasable_file(&fs, "/top/mnt/a.log");

    fs.set_fs_mode_releaser(Arc::new(RecordingReleaser::with_mount("/top/mnt")));

    assert!(
        fs.free("/top", true).is_err(),
        "would release unverified files"
    );
    assert_eq!(
        fs.file_status("/top/mnt/a.log")
            .unwrap()
            .storage_policy
            .state,
        StorageState::Both
    );
}

#[test]
fn a_deferred_free_reports_no_pending_once_released() {
    let fs = test_fs("fs-mode-free-gate-released");
    let inode_id = releasable_file(&fs, "/done.log");
    let snapshot = fs.fs_mode_snapshot(inode_id).unwrap().unwrap();
    assert!(fs
        .release_verified(inode_id, &snapshot.path, snapshot.gen, snapshot.ufs_mtime)
        .unwrap());

    fs.set_fs_mode_releaser(Arc::new(RecordingReleaser::deferring()));
    // Nothing left holding data: the TTL executor treats this as done.
    assert!(matches!(
        fs.free_or_defer("/done.log", true).unwrap(),
        FreeOutcome::Deferred { pending: 0 }
    ));
}

#[test]
fn a_verified_release_frees_exactly_the_verified_version() {
    let fs = test_fs("fs-mode-release-verified");
    let inode_id = releasable_file(&fs, "/verified.log");
    let snapshot = fs.fs_mode_snapshot(inode_id).unwrap().unwrap();
    assert_eq!(snapshot.path, "/verified.log");
    assert!(snapshot.quiescent);

    // The gate stays installed: a verified release must not loop back into it.
    fs.set_fs_mode_releaser(Arc::new(RecordingReleaser::deferring()));
    assert!(fs
        .release_verified(inode_id, &snapshot.path, snapshot.gen, snapshot.ufs_mtime)
        .unwrap());
    assert_eq!(
        fs.file_status("/verified.log")
            .unwrap()
            .storage_policy
            .state,
        StorageState::Ufs
    );
}

#[test]
fn a_verified_release_refuses_a_renamed_file() {
    let fs = test_fs("fs-mode-release-renamed");
    let inode_id = releasable_file(&fs, "/before.log");
    let snapshot = fs.fs_mode_snapshot(inode_id).unwrap().unwrap();
    fs.rename("/before.log", "/after.log", RenameFlags::empty())
        .unwrap();
    assert!(!fs
        .release_verified(inode_id, &snapshot.path, snapshot.gen, snapshot.ufs_mtime)
        .unwrap());
    assert_eq!(
        fs.file_status("/after.log").unwrap().storage_policy.state,
        StorageState::Both
    );
}

#[test]
fn a_verified_release_refuses_a_changed_file() {
    let fs = test_fs("fs-mode-release-changed");
    let inode_id = releasable_file(&fs, "/changed.log");
    let snapshot = fs.fs_mode_snapshot(inode_id).unwrap().unwrap();

    // A newer mark after verification: the observation is stale.
    fs.set_attr(
        "/changed.log",
        SetAttrOptsBuilder::new()
            .ufs_mtime(snapshot.ufs_mtime + 1)
            .build(),
    )
    .unwrap();
    assert!(!fs
        .release_verified(inode_id, &snapshot.path, snapshot.gen, snapshot.ufs_mtime)
        .unwrap());

    // Another file renamed onto the path: the inode id no longer resolves there.
    let other = releasable_file(&fs, "/other.log");
    fs.delete("/changed.log", false).unwrap();
    fs.rename("/other.log", "/changed.log", RenameFlags::empty())
        .unwrap();
    assert!(!fs
        .release_verified(inode_id, &snapshot.path, snapshot.gen, snapshot.ufs_mtime)
        .unwrap());
    assert_eq!(
        fs.fs_mode_snapshot(other).unwrap().unwrap().state,
        StorageState::Both,
        "the renamed-in file was never verified"
    );
}
